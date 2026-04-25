package xray

import (
	"sync"
	"sync/atomic"
	"time"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
)

// DefaultRingBufferSize is the default number of envelopes to keep in the ring buffer.
const DefaultRingBufferSize = 65536

// Emitter is the event pipeline. It assigns sequence numbers, maintains a ring
// buffer for catch-up, and fans out envelopes to registered sinks.
type Emitter struct {
	mu sync.Mutex

	nextSeq uint64

	buffer    []*wiretappb.Envelope
	bufferIdx int
	bufferLen int

	sinks []Sink

	closed bool

	// Collaborators set after construction.
	strings *stringInterner
	net     *wrappedNetwork

	nextPeerAlias atomic.Uint64
	nextConnID    atomic.Uint32
	nextStreamID  atomic.Uint32
}

// NewEmitter creates a new Emitter with the given ring buffer size.
func NewEmitter(bufferSize int) *Emitter {
	if bufferSize <= 0 {
		bufferSize = DefaultRingBufferSize
	}
	return &Emitter{
		buffer: make([]*wiretappb.Envelope, bufferSize),
	}
}

// NextPeerAlias returns the next peer alias.
func (e *Emitter) NextPeerAlias() uint64 {
	return e.nextPeerAlias.Add(1) - 1
}

// NextConnID returns the next connection ID.
func (e *Emitter) NextConnID() uint32 {
	return e.nextConnID.Add(1) - 1
}

// NextStreamID returns the next stream ID.
func (e *Emitter) NextStreamID() uint32 {
	return e.nextStreamID.Add(1) - 1
}

// Emit assigns a sequence number and observed-at timestamp, adds the envelope
// to the ring buffer, and fans out to all sinks. Sink writes happen after the
// lock is released so a slow sink (e.g. SinkFile doing disk I/O) cannot stall
// other producers calling Emit.
func (e *Emitter) Emit(env *wiretappb.Envelope) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	env.Seq = e.nextSeq
	e.nextSeq++
	env.ObservedAtNs = time.Now().UnixNano()
	e.addToBufferLocked(env)
	sinks := e.snapshotSinks()
	e.mu.Unlock()

	for _, sink := range sinks {
		sink.Write(env)
	}
}

func (e *Emitter) addToBufferLocked(env *wiretappb.Envelope) {
	e.buffer[e.bufferIdx] = env
	e.bufferIdx = (e.bufferIdx + 1) % len(e.buffer)
	if e.bufferLen < len(e.buffer) {
		e.bufferLen++
	}
}

// snapshot is the probe's current state, replayed by SinkFile and SinkIngest
// as a sequence of upsert envelopes when a fresh consumer attaches. Internal:
// the wiretappb types are transport mechanics and should not leak through the
// public SDK surface.
type snapshot struct {
	strings     []string
	peers       []*wiretappb.PeerUpsert
	connections []*wiretappb.ConnectionUpsert
	streams     []*wiretappb.StreamUpsert
}

func (e *Emitter) snapshot() snapshot {
	snap := snapshot{
		strings: e.strings.snapshot(),
	}
	if e.net != nil {
		snap.peers, snap.connections, snap.streams = e.net.snapshot()
	}
	return snap
}

// AddSink registers a sink to receive envelopes.
func (e *Emitter) AddSink(sink Sink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sinks = append(e.sinks, sink)
}

// RemoveSink unregisters a sink.
func (e *Emitter) RemoveSink(sink Sink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, s := range e.sinks {
		if s == sink {
			e.sinks = append(e.sinks[:i], e.sinks[i+1:]...)
			return
		}
	}
}

// Sinks returns all registered sinks.
func (e *Emitter) Sinks() []Sink {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotSinks()
}

func (e *Emitter) snapshotSinks() []Sink {
	result := make([]Sink, len(e.sinks))
	copy(result, e.sinks)
	return result
}

// SetClosed sets the closed flag to stop accepting new envelopes.
func (e *Emitter) SetClosed(closed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = closed
}
