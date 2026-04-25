package xray

import (
	"context"
	"sync"
)

type decodeChunk struct {
	stream *wrappedStream
	dir    Direction
	data   []byte
}

// decodeWorker processes decode chunks from all streams in a single goroutine,
// keeping proto unmarshalling entirely off the hot path.
type decodeWorker struct {
	ch     chan decodeChunk
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newDecodeWorker() *decodeWorker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &decodeWorker{
		ch:     make(chan decodeChunk, 4096),
		cancel: cancel,
	}
	w.wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case chunk := <-w.ch:
				w.process(chunk)
			}
		}
	})
	return w
}

func (w *decodeWorker) process(chunk decodeChunk) {
	s := chunk.stream
	if s.failed {
		return
	}

	emit := func(wireBytes int, tags []Tag, parsed any) {
		msg := DecodedMessage{
			StreamID:  s.streamID,
			ConnID:    s.wconn.connID,
			Protocol:  s.protocol,
			Direction: chunk.dir,
			WireBytes: wireBytes,
			Tags:      tags,
			Parsed:    parsed,
		}
		for _, fn := range s.handlers {
			fn(msg)
		}
	}

	var err error
	switch chunk.dir {
	case DirectionIn:
		err = s.decoder.ObserveRead(chunk.data, emit)
	case DirectionOut:
		err = s.decoder.ObserveWrite(chunk.data, emit)
	}
	if err != nil {
		s.decoder.Reset()
		s.failed = true
	}
}

// sendShared enqueues data for async decoding without copying. The caller must
// guarantee the slice is not mutated after this call (e.g. it was just allocated
// for the matching envelope and is not reused). Drops silently if the channel
// is full.
func (w *decodeWorker) sendShared(s *wrappedStream, dir Direction, data []byte) {
	select {
	case w.ch <- decodeChunk{stream: s, dir: dir, data: data}:
	default:
	}
}

func (w *decodeWorker) stop() {
	w.cancel()
	w.wg.Wait()
}
