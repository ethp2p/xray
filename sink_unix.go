package instrument

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	ingestpb "github.com/ethp2p/instrument/pb/ingest"
	"google.golang.org/protobuf/proto"
)

const ingestProtocolVersion = 1

type ingestSnapshot struct {
	strings     []string
	peers       []*ingestpb.PeerUpsert
	connections []*ingestpb.ConnectionUpsert
	streams     []*ingestpb.StreamUpsert
}

// SinkUnix streams the private ingest protocol over a Unix domain socket.
type SinkUnix struct {
	mu sync.Mutex

	emitter *Emitter

	path           string
	sourceID       string
	localPeerID    []byte
	bootID         []byte
	startedAtNs    int64
	waitForAttach  bool
	listener       net.Listener
	clients        map[*unixClient]struct{}
	done           chan struct{}
	attached       chan struct{}
	attachedClosed atomic.Bool
	closed         atomic.Bool
	nextSeq        atomic.Uint64
}

type unixClient struct {
	sink   *SinkUnix
	conn   net.Conn
	sendCh chan *ingestpb.Envelope
	closed atomic.Bool
}

// NewSinkUnix creates a Unix socket sink for the ingest protocol.
func NewSinkUnix(path string, emitter *Emitter, sourceID string, localPeerID []byte, waitForAttach bool) (*SinkUnix, error) {
	if path == "" {
		return nil, errors.New("unix socket path is required")
	}

	if err := removeStaleSocket(path); err != nil {
		return nil, err
	}

	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}

	sink := &SinkUnix{
		emitter:       emitter,
		path:          path,
		sourceID:      sourceID,
		localPeerID:   append([]byte(nil), localPeerID...),
		bootID:        []byte(time.Now().UTC().Format(time.RFC3339Nano)),
		startedAtNs:   time.Now().UnixNano(),
		waitForAttach: waitForAttach,
		listener:      listener,
		clients:       make(map[*unixClient]struct{}),
		done:          make(chan struct{}),
		attached:      make(chan struct{}),
	}

	go sink.acceptLoop()

	return sink, nil
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errors.New("unix socket path already exists and is not a socket")
	}
	return os.Remove(path)
}

func (s *SinkUnix) WaitForAttach() error {
	select {
	case <-s.attached:
		return nil
	case <-s.done:
		return errors.New("unix socket sink closed before attach")
	}
}

func (s *SinkUnix) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	close(s.done)
	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.mu.Lock()
	clients := make([]*unixClient, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.Unlock()

	for _, client := range clients {
		client.close()
	}

	_ = os.Remove(s.path)
	return nil
}

func (s *SinkUnix) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				continue
			}
		}

		client := &unixClient{
			sink:   s,
			conn:   conn,
			sendCh: make(chan *ingestpb.Envelope, 1024),
		}

		if err := s.sendInitialSync(conn); err != nil {
			_ = conn.Close()
			continue
		}

		s.mu.Lock()
		s.clients[client] = struct{}{}
		s.mu.Unlock()

		if s.attachedClosed.CompareAndSwap(false, true) {
			close(s.attached)
		}

		go client.writePump()
	}
}

func (s *SinkUnix) sendInitialSync(conn net.Conn) error {
	if err := writeServerHello(conn, &ingestpb.ServerHello{
		ProtocolVersion: ingestProtocolVersion,
		SourceId:        s.sourceID,
	}); err != nil {
		return err
	}

	if err := writeEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_SnapshotStart{
		SnapshotStart: &ingestpb.SnapshotStart{},
	})); err != nil {
		return err
	}

	snap := s.emitter.ingestSnapshot()
	for id, value := range snap.strings {
		if err := writeEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_StringDef{
			StringDef: &ingestpb.StringDef{Id: uint32(id), Value: value},
		})); err != nil {
			return err
		}
	}
	for _, peer := range snap.peers {
		if err := writeEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_PeerUpsert{
			PeerUpsert: peer,
		})); err != nil {
			return err
		}
	}
	for _, connection := range snap.connections {
		if err := writeEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_ConnectionUpsert{
			ConnectionUpsert: connection,
		})); err != nil {
			return err
		}
	}
	for _, stream := range snap.streams {
		if err := writeEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_StreamUpsert{
			StreamUpsert: stream,
		})); err != nil {
			return err
		}
	}

	return writeEnvelope(conn, s.nextEnvelope(&ingestpb.Envelope_SnapshotEnd{
		SnapshotEnd: &ingestpb.SnapshotEnd{},
	}))
}

func (s *SinkUnix) EmitStringDef(id uint32, value string) {
	s.broadcast(s.nextEnvelope(&ingestpb.Envelope_StringDef{
		StringDef: &ingestpb.StringDef{Id: id, Value: value},
	}))
}

func (s *SinkUnix) EmitPeerUpsert(alias uint64, peerID []byte) {
	s.broadcast(s.nextEnvelope(&ingestpb.Envelope_PeerUpsert{
		PeerUpsert: &ingestpb.PeerUpsert{
			PeerAlias: alias,
			PeerId:    append([]byte(nil), peerID...),
		},
	}))
}

func (s *SinkUnix) EmitConnectionUpsert(connection *ingestpb.ConnectionUpsert) {
	s.broadcast(s.nextEnvelope(&ingestpb.Envelope_ConnectionUpsert{
		ConnectionUpsert: connection,
	}))
}

func (s *SinkUnix) EmitConnectionClosed(connAlias uint64, closedAtNs int64) {
	s.broadcast(s.nextEnvelope(&ingestpb.Envelope_ConnectionClosed{
		ConnectionClosed: &ingestpb.ConnectionClosed{
			ConnAlias:  connAlias,
			ClosedAtNs: closedAtNs,
		},
	}))
}

func (s *SinkUnix) EmitStreamUpsert(stream *ingestpb.StreamUpsert) {
	s.broadcast(s.nextEnvelope(&ingestpb.Envelope_StreamUpsert{
		StreamUpsert: stream,
	}))
}

func (s *SinkUnix) EmitStreamClosed(streamAlias uint64, closedAtNs int64, reason ingestpb.CloseReason) {
	s.broadcast(s.nextEnvelope(&ingestpb.Envelope_StreamClosed{
		StreamClosed: &ingestpb.StreamClosed{
			StreamAlias: streamAlias,
			ClosedAtNs:  closedAtNs,
			Reason:      reason,
		},
	}))
}

func (s *SinkUnix) EmitStreamChunk(streamAlias uint64, dir Direction, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	s.broadcast(s.nextEnvelope(&ingestpb.Envelope_StreamChunk{
		StreamChunk: &ingestpb.StreamChunk{
			StreamAlias: streamAlias,
			Direction:   directionToIngest(dir),
			Data:        cp,
		},
	}))
}

func (s *SinkUnix) nextEnvelope(payload any) *ingestpb.Envelope {
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

func (s *SinkUnix) broadcast(event *ingestpb.Envelope) {
	s.mu.Lock()
	clients := make([]*unixClient, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.Unlock()

	for _, client := range clients {
		client.offer(event)
	}
}

func (s *SinkUnix) removeClient(client *unixClient) {
	s.mu.Lock()
	delete(s.clients, client)
	s.mu.Unlock()
}

func (c *unixClient) offer(event *ingestpb.Envelope) {
	if c.closed.Load() {
		return
	}
	select {
	case c.sendCh <- event:
	default:
		c.close()
	}
}

func (c *unixClient) writePump() {
	defer c.close()
	for {
		select {
		case event, ok := <-c.sendCh:
			if !ok {
				return
			}
			if err := writeEnvelope(c.conn, event); err != nil {
				return
			}
		case <-c.sink.done:
			return
		}
	}
}

func (c *unixClient) close() {
	if c.closed.Swap(true) {
		return
	}
	c.sink.removeClient(c)
	close(c.sendCh)
	_ = c.conn.Close()
}

const (
	wireServerHelloByte byte = 0x02
)

// writeServerHello sends a standalone ServerHello using the type-discriminated
// wire format: [0x02][varint len][proto bytes].
func writeServerHello(w net.Conn, msg *ingestpb.ServerHello) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := w.Write([]byte{wireServerHelloByte}); err != nil {
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

func writeEnvelope(w net.Conn, event *ingestpb.Envelope) error {
	data, err := proto.Marshal(event)
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
