package instrument

import (
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/ethp2p/instrument/pb"
)

// DefaultRingBufferSize is the default number of events to keep in the ring buffer.
const DefaultRingBufferSize = 65536

// Emitter is the event pipeline. It assigns sequence numbers, maintains a ring
// buffer for catch-up, and fans out events to registered sinks.
type Emitter struct {
	mu sync.Mutex

	nextSeq uint64

	buffer    []*pb.TraceEvent
	bufferIdx int
	bufferLen int

	sinks []Sink

	closed bool

	// Collaborators set after construction.
	strings    *StringInterner
	net        *wrappedNetwork
	ingestSink *SinkIngest

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
		buffer: make([]*pb.TraceEvent, bufferSize),
	}
}

// NextConnID returns the next connection ID.
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

// Emit assigns a sequence number and timestamp, adds the event to the ring
// buffer, and fans out to all sinks.
func (e *Emitter) Emit(event *pb.TraceEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return
	}

	event.Seq = e.nextSeq
	e.nextSeq++
	event.TimestampNs = time.Now().UnixNano()

	e.addToBufferLocked(event)

	sinks := e.snapshotSinks()
	fanOut(sinks, event)
}

func (e *Emitter) addToBufferLocked(event *pb.TraceEvent) {
	e.buffer[e.bufferIdx] = event
	e.bufferIdx = (e.bufferIdx + 1) % len(e.buffer)
	if e.bufferLen < len(e.buffer) {
		e.bufferLen++
	}
}

// EmitTraffic emits a traffic event with the given parameters.
func (e *Emitter) EmitTraffic(streamID uint32, dir Direction, bytes int, tags []Tag) {
	pbDir := directionToPb(dir)

	var pbTags []*pb.Tag
	if len(tags) > 0 {
		pbTags = make([]*pb.Tag, len(tags))
		for i, t := range tags {
			nameID := e.strings.Intern(t.Name)
			valueIDs := make([]uint32, len(t.Values))
			for j, v := range t.Values {
				valueIDs[j] = e.strings.Intern(v)
			}
			pbTags[i] = &pb.Tag{
				NameId:   nameID,
				ValueIds: valueIDs,
			}
		}
	}

	e.Emit(&pb.TraceEvent{
		Event: &pb.TraceEvent_Traffic{
			Traffic: &pb.Traffic{
				StreamId:  streamID,
				Direction: pbDir,
				Bytes:     uint32(bytes),
				Tags:      pbTags,
			},
		},
	})
}

// Snapshot returns a snapshot of the current state (strings, connections, streams).
func (e *Emitter) Snapshot() *pb.Snapshot {
	snap := &pb.Snapshot{
		Strings: e.strings.snapshot(),
	}
	if e.net != nil {
		snap.Connections, snap.Streams = e.net.snapshot()
	}
	return snap
}

func (e *Emitter) ingestSnapshot() ingestSnapshot {
	snap := ingestSnapshot{
		strings: e.strings.snapshot(),
	}
	if e.net != nil {
		snap.peers, snap.connections, snap.streams = e.net.ingestSnapshot()
	}
	return snap
}

// EventsFromSeq returns events starting from the given sequence number.
// Returns nil if seq is no longer in the buffer (client too far behind).
func (e *Emitter) EventsFromSeq(seq uint64) []*pb.TraceEvent {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.bufferLen == 0 {
		return nil
	}

	oldestIdx := (e.bufferIdx - e.bufferLen + len(e.buffer)) % len(e.buffer)
	oldestSeq := e.buffer[oldestIdx].Seq
	if seq < oldestSeq {
		return nil
	}

	offset := int(seq - oldestSeq)
	if offset >= e.bufferLen {
		return []*pb.TraceEvent{}
	}

	startIdx := (oldestIdx + offset) % len(e.buffer)
	count := e.bufferLen - offset

	result := make([]*pb.TraceEvent, count)
	for i := 0; i < count; i++ {
		result[i] = e.buffer[(startIdx+i)%len(e.buffer)]
	}
	return result
}

// AddSink registers a sink to receive events.
func (e *Emitter) AddSink(sink Sink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sinks = append(e.sinks, sink)
}

// RemoveSink unregisters a sink from receiving events.
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

func fanOut(sinks []Sink, event *pb.TraceEvent) {
	for _, sink := range sinks {
		sink.Write(event)
	}
}

// Strings returns the string interner.
func (e *Emitter) Strings() *StringInterner {
	return e.strings
}

// SetClosed sets the closed flag to stop accepting new events.
func (e *Emitter) SetClosed(closed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = closed
}
