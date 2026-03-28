package probe

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
		s.emitter.EmitTraffic(s.streamID, chunk.dir, len(chunk.data), nil)
		return
	}

	emit := func(wireBytes int, tags []Tag, parsed any) {
		s.emitter.EmitTraffic(s.streamID, chunk.dir, wireBytes, tags)
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
		// Emit raw bytes for the failed chunk — the decoder returned before
		// calling emit, so these bytes would otherwise go unaccounted.
		s.emitter.EmitTraffic(s.streamID, chunk.dir, len(chunk.data), nil)
		s.decoder.Reset()
		s.failed = true
	}
}

// send copies data and enqueues it for async decoding. Drops silently if the
// channel is full — bytes are already counted on the hot path.
func (w *decodeWorker) send(s *wrappedStream, dir Direction, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case w.ch <- decodeChunk{stream: s, dir: dir, data: cp}:
	default:
	}
}

func (w *decodeWorker) stop() {
	w.cancel()
	w.wg.Wait()
}
