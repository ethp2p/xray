package instrument

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ingestpb "github.com/ethp2p/instrument/pb/ingest"
	"google.golang.org/protobuf/proto"
)

const ingestProtocolVersion = 2

type ingestSnapshot struct {
	strings     []string
	peers       []*ingestpb.PeerUpsert
	connections []*ingestpb.ConnectionUpsert
	streams     []*ingestpb.StreamUpsert
}

// SinkIngest dials the backend's ingest listener and streams the private
// ingest protocol. On connect it performs a ClientHello/ServerHello handshake,
// sends a full state snapshot, then forwards live events. It reconnects
// automatically if the connection drops.
type SinkIngest struct {
	mu sync.Mutex

	emitter *Emitter

	address     string
	localPeerID []byte
	clientName  string
	bootID      []byte
	startedAtNs int64
	waitForAttach bool
	sourceID    string // assigned by backend

	conn           net.Conn
	sendCh         chan *ingestpb.Envelope
	done           chan struct{}
	attached       chan struct{}
	attachedClosed atomic.Bool
	closed         atomic.Bool
	nextSeq        atomic.Uint64
}

// NewSinkIngest creates a sink that dials the backend ingest listener.
// The goroutine connects immediately and retries on failure.
func NewSinkIngest(address string, emitter *Emitter, clientName string, localPeerID []byte, waitForAttach bool) *SinkIngest {
	sink := &SinkIngest{
		emitter:       emitter,
		address:       address,
		localPeerID:   append([]byte(nil), localPeerID...),
		clientName:    clientName,
		bootID:        []byte(time.Now().UTC().Format(time.RFC3339Nano)),
		startedAtNs:   time.Now().UnixNano(),
		waitForAttach: waitForAttach,
		sendCh:        make(chan *ingestpb.Envelope, 1024),
		done:          make(chan struct{}),
		attached:      make(chan struct{}),
	}

	go sink.connectLoop()

	return sink
}

func (s *SinkIngest) WaitForAttach() error {
	select {
	case <-s.attached:
		return nil
	case <-s.done:
		return errors.New("ingest sink closed before attach")
	}
}

func (s *SinkIngest) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	close(s.done)

	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	return nil
}

// connectLoop dials the backend, performs the handshake, sends the snapshot,
// then runs the write pump. On any error it waits briefly and retries.
func (s *SinkIngest) connectLoop() {
	for {
		select {
		case <-s.done:
			return
		default:
		}

		if err := s.connect(); err != nil {
			select {
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}

		// Drain stale events queued during disconnect.
		s.drainSendCh()

		if err := s.sendSnapshot(); err != nil {
			s.closeConn()
			continue
		}

		if s.attachedClosed.CompareAndSwap(false, true) {
			close(s.attached)
		}

		s.writePump()
		s.closeConn()
	}
}

func (s *SinkIngest) connect() error {
	network := "tcp"
	if strings.Contains(s.address, "/") {
		network = "unix"
	}

	conn, err := net.DialTimeout(network, s.address, 5*time.Second)
	if err != nil {
		return err
	}

	hello := &ingestpb.ClientHello{
		ProtocolVersion: ingestProtocolVersion,
		PeerId:          s.localPeerID,
		ClientName:      s.clientName,
		BootId:          s.bootID,
		StartedAtNs:     s.startedAtNs,
	}
	if err := writeTypedMessage(conn, wireClientHelloByte, hello); err != nil {
		conn.Close()
		return err
	}

	var serverHello ingestpb.ServerHello
	if err := readTypedMessage(conn, wireServerHelloByte, &serverHello); err != nil {
		conn.Close()
		return err
	}

	s.mu.Lock()
	s.conn = conn
	s.sourceID = serverHello.SourceId
	s.mu.Unlock()

	return nil
}

func (s *SinkIngest) sendSnapshot() error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return errors.New("no connection")
	}

	s.nextSeq.Store(0)

	if err := writeTypedEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_SnapshotStart{
		SnapshotStart: &ingestpb.SnapshotStart{},
	})); err != nil {
		return err
	}

	snap := s.emitter.ingestSnapshot()
	for id, value := range snap.strings {
		if err := writeTypedEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_StringDef{
			StringDef: &ingestpb.StringDef{Id: uint32(id), Value: value},
		})); err != nil {
			return err
		}
	}
	for _, peer := range snap.peers {
		if err := writeTypedEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_PeerUpsert{
			PeerUpsert: peer,
		})); err != nil {
			return err
		}
	}
	for _, connection := range snap.connections {
		if err := writeTypedEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_ConnectionUpsert{
			ConnectionUpsert: connection,
		})); err != nil {
			return err
		}
	}
	for _, stream := range snap.streams {
		if err := writeTypedEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_StreamUpsert{
			StreamUpsert: stream,
		})); err != nil {
			return err
		}
	}

	return writeTypedEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_SnapshotEnd{
		SnapshotEnd: &ingestpb.SnapshotEnd{},
	}))
}

func (s *SinkIngest) writePump() {
	for {
		select {
		case event, ok := <-s.sendCh:
			if !ok {
				return
			}
			s.mu.Lock()
			conn := s.conn
			s.mu.Unlock()
			if conn == nil {
				return
			}
			if err := writeTypedEnvelope(conn, event); err != nil {
				return
			}
		case <-s.done:
			return
		}
	}
}

func (s *SinkIngest) closeConn() {
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.mu.Unlock()
}

func (s *SinkIngest) drainSendCh() {
	for {
		select {
		case <-s.sendCh:
		default:
			return
		}
	}
}

func (s *SinkIngest) offer(event *ingestpb.Envelope) {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- event:
	default:
	}
}

func (s *SinkIngest) EmitStringDef(id uint32, value string) {
	s.offer(s.nextEnvelope(&ingestpb.Envelope_StringDef{
		StringDef: &ingestpb.StringDef{Id: id, Value: value},
	}))
}

func (s *SinkIngest) EmitPeerUpsert(alias uint64, peerID []byte) {
	s.offer(s.nextEnvelope(&ingestpb.Envelope_PeerUpsert{
		PeerUpsert: &ingestpb.PeerUpsert{
			PeerAlias: alias,
			PeerId:    append([]byte(nil), peerID...),
		},
	}))
}

func (s *SinkIngest) EmitConnectionUpsert(connection *ingestpb.ConnectionUpsert) {
	s.offer(s.nextEnvelope(&ingestpb.Envelope_ConnectionUpsert{
		ConnectionUpsert: connection,
	}))
}

func (s *SinkIngest) EmitConnectionClosed(connAlias uint64, closedAtNs int64) {
	s.offer(s.nextEnvelope(&ingestpb.Envelope_ConnectionClosed{
		ConnectionClosed: &ingestpb.ConnectionClosed{
			ConnAlias:  connAlias,
			ClosedAtNs: closedAtNs,
		},
	}))
}

func (s *SinkIngest) EmitStreamUpsert(stream *ingestpb.StreamUpsert) {
	s.offer(s.nextEnvelope(&ingestpb.Envelope_StreamUpsert{
		StreamUpsert: stream,
	}))
}

func (s *SinkIngest) EmitStreamClosed(streamAlias uint64, closedAtNs int64, reason ingestpb.CloseReason) {
	s.offer(s.nextEnvelope(&ingestpb.Envelope_StreamClosed{
		StreamClosed: &ingestpb.StreamClosed{
			StreamAlias: streamAlias,
			ClosedAtNs:  closedAtNs,
			Reason:      reason,
		},
	}))
}

func (s *SinkIngest) EmitStreamChunk(streamAlias uint64, dir Direction, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	s.offer(s.nextEnvelope(&ingestpb.Envelope_StreamChunk{
		StreamChunk: &ingestpb.StreamChunk{
			StreamAlias: streamAlias,
			Direction:   directionToIngest(dir),
			Data:        cp,
		},
	}))
}

func (s *SinkIngest) nextEnvelope(payload any) *ingestpb.Envelope {
	event := &ingestpb.Envelope{
		Seq:          s.nextSeq.Add(1) - 1,
		ObservedAtNs: time.Now().UnixNano(),
	}
	switch value := payload.(type) {
	case *ingestpb.Envelope_SnapshotStart:
		event.Payload = value
	case *ingestpb.Envelope_SnapshotEnd:
		event.Payload = value
	case *ingestpb.Envelope_StringDef:
		event.Payload = value
	case *ingestpb.Envelope_PeerUpsert:
		event.Payload = value
	case *ingestpb.Envelope_ConnectionUpsert:
		event.Payload = value
	case *ingestpb.Envelope_ConnectionClosed:
		event.Payload = value
	case *ingestpb.Envelope_StreamUpsert:
		event.Payload = value
	case *ingestpb.Envelope_StreamClosed:
		event.Payload = value
	case *ingestpb.Envelope_StreamChunk:
		event.Payload = value
	default:
		panic("unsupported ingest envelope payload")
	}
	return event
}

func directionToIngest(d Direction) ingestpb.Direction {
	switch d {
	case DirectionIn:
		return ingestpb.Direction_DIRECTION_IN
	case DirectionOut:
		return ingestpb.Direction_DIRECTION_OUT
	default:
		return ingestpb.Direction_DIRECTION_UNKNOWN
	}
}

// Wire format: type discriminator byte + varint-delimited protobuf.
// Duplicated from introspector/codec.go to avoid circular dependency
// (instrument package cannot import introspector).
const (
	wireClientHelloByte byte = 0x01
	wireServerHelloByte byte = 0x02
	wireEnvelopeByte    byte = 0x03
)

func writeTypedMessage(w io.Writer, typ byte, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := w.Write([]byte{typ}); err != nil {
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

func readTypedMessage(r io.Reader, expectedType byte, msg proto.Message) error {
	var typBuf [1]byte
	if _, err := io.ReadFull(r, typBuf[:]); err != nil {
		return err
	}
	if typBuf[0] != expectedType {
		return fmt.Errorf("unexpected message type: got 0x%02x, want 0x%02x", typBuf[0], expectedType)
	}

	var length uint64
	var shift uint
	for {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
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

	const maxMessageSize = 4 << 20
	if length > maxMessageSize {
		return fmt.Errorf("message too large: %d bytes", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return proto.Unmarshal(data, msg)
}

func writeTypedEnvelope(w io.Writer, event *ingestpb.Envelope) error {
	return writeTypedMessage(w, wireEnvelopeByte, event)
}
