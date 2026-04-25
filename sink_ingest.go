package xray

import (
	"crypto/rand"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
	"github.com/ethp2p/xray/proto/wiretap/wire"
)

// SinkIngest dials the backend's ingest listener and streams envelopes. On
// connect it performs a ClientHello/ServerHello handshake and replays the
// emitter's current state as a snapshot before forwarding live envelopes.
// Reconnects automatically if the connection drops.
type SinkIngest struct {
	mu sync.Mutex

	emitter *Emitter

	address       string
	localPeerID   []byte
	clientName    string
	bootID        []byte
	startedAtNs   int64
	waitForAttach bool
	sourceID      string

	conn           net.Conn
	sendCh         chan *wiretappb.Envelope
	done           chan struct{}
	attached       chan struct{}
	attachedClosed atomic.Bool
	closed         atomic.Bool
}

var _ Sink = (*SinkIngest)(nil)

// NewSinkIngest creates a sink that dials the backend ingest listener and
// streams envelopes. The connect goroutine starts immediately and retries on
// failure.
func NewSinkIngest(address string, emitter *Emitter, clientName string, localPeerID []byte, waitForAttach bool) *SinkIngest {
	sink := &SinkIngest{
		emitter:       emitter,
		address:       address,
		localPeerID:   append([]byte(nil), localPeerID...),
		clientName:    clientName,
		bootID:        randomBootID(),
		startedAtNs:   time.Now().UnixNano(),
		waitForAttach: waitForAttach,
		sendCh:        make(chan *wiretappb.Envelope, 1024),
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

// Write enqueues an envelope for transmission. Drops silently if the buffer is
// full; the emitter's ring buffer remains the source of truth for catch-up.
func (s *SinkIngest) Write(env *wiretappb.Envelope) bool {
	if s.closed.Load() {
		return false
	}
	select {
	case s.sendCh <- env:
		return true
	default:
		return false
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
	conn, err := net.DialTimeout(wire.InferNetwork(s.address), s.address, 5*time.Second)
	if err != nil {
		return err
	}

	hello := &wiretappb.ClientHello{
		ProtocolVersion: wire.IngestProtocolVersion,
		PeerId:          s.localPeerID,
		ClientName:      s.clientName,
		BootId:          s.bootID,
		StartedAtNs:     s.startedAtNs,
	}
	if err := wire.WriteClientHello(conn, hello); err != nil {
		conn.Close()
		return err
	}

	serverHello, err := wire.ReadServerHello(conn)
	if err != nil {
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

	for _, env := range snapshotEnvelopes(s.emitter.snapshot()) {
		if err := wire.WriteEnvelope(conn, env); err != nil {
			return err
		}
	}
	return nil
}

func (s *SinkIngest) writePump() {
	for {
		select {
		case env, ok := <-s.sendCh:
			if !ok {
				return
			}
			s.mu.Lock()
			conn := s.conn
			s.mu.Unlock()
			if conn == nil {
				return
			}
			if err := wire.WriteEnvelope(conn, env); err != nil {
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

// randomBootID returns 16 random bytes that change on each process start.
// The backend uses bootID to detect probe restarts; an opaque random ID is
// safer than a timestamp string (no parse format, no ns-collision risk).
func randomBootID() []byte {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		// crypto/rand should never fail on a healthy system; fall back to a
		// time-derived value so the probe still has *some* unique identifier.
		nano := time.Now().UnixNano()
		for i := range id {
			id[i] = byte(nano >> (i % 8 * 8))
		}
	}
	return id
}
