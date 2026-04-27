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

// SinkIngest dials the backend's ingest listener and streams envelopes.
// After the ClientHello/ServerHello handshake the server reports the highest
// envelope seq it has applied; if non-zero and still in the emitter's ring,
// the missing range is replayed directly from the ring so a transient
// disconnect loses no events. Otherwise — fresh attach, probe restart, or a
// disconnect long enough that the ring rolled past the cursor — a full
// snapshot is sent and the server resets its per-source state. Reconnects
// automatically if the connection drops.
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

// Write enqueues an envelope for transmission. If sendCh is full, the envelope
// is not enqueued here, but it has already been recorded in the emitter's ring
// buffer — on reconnect, the emitter is replayed from the server's last-acked
// seq, so any envelope dropped here is delivered then.
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

		lastAcked, err := s.connect()
		if err != nil {
			select {
			case <-s.done:
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}

		if err := s.sendCatchup(lastAcked); err != nil {
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

func (s *SinkIngest) connect() (uint64, error) {
	conn, err := net.DialTimeout(wire.InferNetwork(s.address), s.address, 5*time.Second)
	if err != nil {
		return 0, err
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
		return 0, err
	}

	serverHello, err := wire.ReadServerHello(conn)
	if err != nil {
		conn.Close()
		return 0, err
	}

	s.mu.Lock()
	s.conn = conn
	s.sourceID = serverHello.SourceId
	s.mu.Unlock()

	return serverHello.LastAckedSeq, nil
}

// sendCatchup brings the server up to date after the handshake. If the server
// is fresh (last_acked == 0) or the emitter's ring no longer covers
// [last_acked+1, current], a full snapshot is sent — its SnapshotStart marker
// also resets server-side state. Otherwise the missing range is replayed
// directly from the ring; envelopes still queued in sendCh that overlap with
// the replay are deduped server-side via Envelope.Seq.
func (s *SinkIngest) sendCatchup(lastAcked uint64) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return errors.New("no connection")
	}

	if lastAcked > 0 {
		replay, ok := s.emitter.EventsFromSeq(lastAcked + 1)
		if ok {
			for _, env := range replay {
				if err := wire.WriteEnvelope(conn, env); err != nil {
					return err
				}
			}
			return nil
		}
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
