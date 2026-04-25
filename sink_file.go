package xray

import (
	"encoding/binary"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
)

// SinkFile writes envelopes to disk as length-delimited protobuf, prefixed with
// a snapshot of the current state so the file can be replayed standalone.
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

	if err := s.writeSnapshotLocked(); err != nil {
		file.Close()
		return nil, err
	}

	if cfg.snapshotInterval > 0 {
		s.snapshotTicker = time.NewTicker(cfg.snapshotInterval)
		go s.snapshotLoop()
	}

	return s, nil
}

// Write delivers an envelope to the file.
func (s *SinkFile) Write(env *wiretappb.Envelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	return s.writeEnvelopeLocked(env) == nil
}

func (s *SinkFile) writeEnvelopeLocked(env *wiretappb.Envelope) error {
	data, err := proto.Marshal(env)
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

func (s *SinkFile) writeSnapshotLocked() error {
	snap := s.emitter.snapshot()
	envs := snapshotEnvelopes(snap)
	for _, env := range envs {
		if err := s.writeEnvelopeLocked(env); err != nil {
			return err
		}
	}
	return nil
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
				_ = s.writeSnapshotLocked()
			}
			s.mu.Unlock()
		}
	}
}

// snapshotEnvelopes converts a snapshot into a sequence of envelopes wrapped
// between SnapshotStart and SnapshotEnd markers. Used by both SinkFile and
// SinkIngest to bring a fresh consumer up to date.
func snapshotEnvelopes(snap snapshot) []*wiretappb.Envelope {
	envs := make([]*wiretappb.Envelope, 0, 2+len(snap.strings)+len(snap.peers)+len(snap.connections)+len(snap.streams))
	envs = append(envs, &wiretappb.Envelope{
		Payload: &wiretappb.Envelope_SnapshotStart{SnapshotStart: &wiretappb.SnapshotStart{}},
	})
	for id, value := range snap.strings {
		envs = append(envs, &wiretappb.Envelope{
			Payload: &wiretappb.Envelope_StringDef{
				StringDef: &wiretappb.StringDef{Id: uint32(id), Value: value},
			},
		})
	}
	for _, p := range snap.peers {
		envs = append(envs, &wiretappb.Envelope{
			Payload: &wiretappb.Envelope_PeerUpsert{PeerUpsert: p},
		})
	}
	for _, c := range snap.connections {
		envs = append(envs, &wiretappb.Envelope{
			Payload: &wiretappb.Envelope_ConnectionUpsert{ConnectionUpsert: c},
		})
	}
	for _, st := range snap.streams {
		envs = append(envs, &wiretappb.Envelope{
			Payload: &wiretappb.Envelope_StreamUpsert{StreamUpsert: st},
		})
	}
	envs = append(envs, &wiretappb.Envelope{
		Payload: &wiretappb.Envelope_SnapshotEnd{SnapshotEnd: &wiretappb.SnapshotEnd{}},
	})
	return envs
}
