package introspector

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	ingestpb "github.com/ethp2p/instrument/pb/ingest"
	"google.golang.org/protobuf/proto"
)

const ingestProtocolVersion uint32 = 2

// IngestListener accepts inbound connections from probes, performs a
// ClientHello/ServerHello handshake, then streams Envelope events into the
// Processor. Each unique source_id gets at most one active session; a
// reconnecting probe preempts the previous connection.
type IngestListener struct {
	ctx       context.Context
	processor *Processor
	registry  *SourceRegistry
	storage   *Storage
	mu        sync.Mutex
	sessions  map[string]*activeSession
}

type activeSession struct {
	cancel func()
	conn   net.Conn
}

func NewIngestListener(processor *Processor, registry *SourceRegistry, storage *Storage) *IngestListener {
	return &IngestListener{
		processor: processor,
		registry:  registry,
		storage:   storage,
		sessions:  make(map[string]*activeSession),
	}
}

// ListenAndServe binds to address and accepts probe connections until ctx is
// cancelled. Address is interpreted as a Unix socket path if it contains '/',
// otherwise as a TCP address.
func (l *IngestListener) ListenAndServe(ctx context.Context, address string) error {
	l.ctx = ctx

	network := "tcp"
	if strings.Contains(address, "/") {
		network = "unix"
		os.Remove(address)
	}

	ln, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go l.handleConnection(conn)
	}
}

func (l *IngestListener) handleConnection(conn net.Conn) {
	hello, err := ReadClientHello(conn)
	if err != nil {
		conn.Close()
		return
	}

	if hello.ProtocolVersion != ingestProtocolVersion {
		conn.Close()
		return
	}

	sourceID := DeriveSourceID(hello.PeerId)

	l.mu.Lock()
	if old, ok := l.sessions[sourceID]; ok {
		old.cancel()
		old.conn.Close()
	}
	ctx, cancel := context.WithCancel(l.ctx)
	l.sessions[sourceID] = &activeSession{cancel: cancel, conn: conn}
	l.mu.Unlock()

	defer func() {
		l.registry.SetConnected(sourceID, false)
		l.mu.Lock()
		if sess, ok := l.sessions[sourceID]; ok && sess.conn == conn {
			delete(l.sessions, sourceID)
		}
		l.mu.Unlock()
		conn.Close()
		cancel()
	}()

	err = WriteServerHello(conn, &ingestpb.ServerHello{
		ProtocolVersion: ingestProtocolVersion,
		SourceId:        sourceID,
	})
	if err != nil {
		return
	}

	info := SourceInfo{
		SourceID:    sourceID,
		PeerID:      hello.PeerId,
		ClientName:  hello.ClientName,
		BootID:      hello.BootId,
		StartedAtNs: hello.StartedAtNs,
		ConnectedAt: time.Now(),
		Connected:   true,
	}
	l.registry.Register(info)
	if err := l.storage.WriteSourceMeta(info); err != nil {
		log.Printf("ingest: failed to persist source meta for %s: %v", sourceID, err)
	}

	l.processor.ResetSourceAliases(sourceID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		env, err := ReadEnvelope(conn)
		if err != nil {
			return
		}
		l.processor.ApplyForSource(sourceID, env)
	}
}

type UnixIngestClient struct {
	socketPath string
	processor  *Processor
	sourceID   string
}

func NewUnixIngestClient(socketPath string, processor *Processor, sourceID string) *UnixIngestClient {
	return &UnixIngestClient{
		socketPath: socketPath,
		processor:  processor,
		sourceID:   sourceID,
	}
}

func (c *UnixIngestClient) Run() error {
	return c.RunContext(context.Background())
}

func (c *UnixIngestClient) RunContext(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := c.runOnce()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, os.ErrNotExist) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (c *UnixIngestClient) runOnce() error {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return err
	}
	defer conn.Close()

	for {
		var event ingestpb.Envelope
		if err := readDelimited(conn, &event, 4<<20); err != nil {
			return err
		}
		c.processor.ApplyForSource(c.sourceID, &event)
	}
}

func readDelimited(r io.Reader, msg proto.Message, maxSize int) error {
	var length uint64
	var shift uint
	for {
		var b [1]byte
		if _, err := r.Read(b[:]); err != nil {
			return err
		}
		length |= uint64(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return errors.New("varint overflow")
		}
	}

	if int(length) > maxSize {
		return errors.New("message too large")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return proto.Unmarshal(data, msg)
}

func writeDelimited(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(data)))
	if _, err := w.Write(buf[:n]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}
