package xray

import (
	"encoding/binary"
	"os"
	"sync"
	"time"

	pb "github.com/ethp2p/xray/proto"
	"google.golang.org/protobuf/proto"
)

// SinkFile writes trace events to disk as length-delimited protobuf.
type SinkFile struct {
	mu sync.Mutex

	file    *os.File
	emitter *Emitter

	snapshotTicker *time.Ticker

	closed bool
	done   chan struct{}
}

var _ Sink = (*SinkFile)(nil)

// NewSinkFile creates a new SinkFile that writes to the given path.
func NewSinkFile(path string, emitter *Emitter, opts ...SinkFileOption) (*SinkFile, error) {
	cfg := &sinkFileConfig{
		snapshotInterval: 5 * time.Minute,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	s := &SinkFile{
		file:    file,
		emitter: emitter,
		done:    make(chan struct{}),
	}

	snap := emitter.Snapshot()
	event := &pb.TraceEvent{
		Seq:         0,
		TimestampNs: time.Now().UnixNano(),
		Event:       &pb.TraceEvent_Snapshot{Snapshot: snap},
	}
	if err := s.writeEvent(event); err != nil {
		file.Close()
		return nil, err
	}

	if cfg.snapshotInterval > 0 {
		s.snapshotTicker = time.NewTicker(cfg.snapshotInterval)
		go s.snapshotLoop()
	}

	return s, nil
}

// Write sends an event to the sink.
func (s *SinkFile) Write(event *pb.TraceEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	return s.writeEvent(event) == nil
}

func (s *SinkFile) writeEvent(event *pb.TraceEvent) error {
	data, err := proto.Marshal(event)
	if err != nil {
		return err
	}

	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(data)))
	if _, err := s.file.Write(buf[:n]); err != nil {
		return err
	}

	_, err = s.file.Write(data)
	return err
}

// Close stops the sink and closes the underlying file.
func (s *SinkFile) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	s.mu.Unlock()

	if s.snapshotTicker != nil {
		s.snapshotTicker.Stop()
	}

	return s.file.Close()
}

func (s *SinkFile) snapshotLoop() {
	for {
		select {
		case <-s.done:
			return
		case <-s.snapshotTicker.C:
			s.mu.Lock()
			if !s.closed {
				snap := s.emitter.Snapshot()
				event := &pb.TraceEvent{
					TimestampNs: time.Now().UnixNano(),
					Event:       &pb.TraceEvent_Snapshot{Snapshot: snap},
				}
				s.writeEvent(event)
			}
			s.mu.Unlock()
		}
	}
}
