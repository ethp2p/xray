package backend

import (
	"context"
	"log"
	"net"
	"os"
	"sync"
	"time"

	ingestpb "github.com/ethp2p/xray/proto/ingest"
	"github.com/ethp2p/xray/wire"
)

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

	network := wire.InferNetwork(address)
	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			log.Printf("warning: could not remove stale socket %s: %v", address, err)
		}
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
	hello, err := wire.ReadClientHello(conn)
	if err != nil {
		conn.Close()
		return
	}

	if hello.ProtocolVersion != wire.IngestProtocolVersion {
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

	err = wire.WriteServerHello(conn, &ingestpb.ServerHello{
		ProtocolVersion: wire.IngestProtocolVersion,
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
	if l.storage != nil {
		if err := l.storage.WriteSourceMeta(info); err != nil {
			log.Printf("ingest: failed to persist source meta for %s: %v", sourceID, err)
		}
	}

	l.processor.ResetSourceAliases(sourceID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		env, err := wire.ReadEnvelope(conn)
		if err != nil {
			return
		}
		l.processor.ApplyForSource(sourceID, env)
	}
}

