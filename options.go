package instrument

import "time"

type decoderEntry struct {
	match       func(string) bool
	decoderCtor func() StreamDecoder
}

type config struct {
	ringBufferSize int
	sinkFilePath   string
	sinkFileOpts   []SinkFileOption
	unixSocketPath string
	waitForAttach  bool
	decoders       []decoderEntry
	onMessage      []OnMessageFactory
	sinks          []Sink
}

// Option configures an instrumented host.
type Option func(*config)

// WithRingBufferSize sets the size of the event ring buffer.
// Events are dropped when the buffer is full and sinks cannot keep up.
func WithRingBufferSize(size int) Option {
	return func(c *config) {
		c.ringBufferSize = size
	}
}

// WithDecoder registers a streaming protocol decoder.
// match returns true for protocol IDs this decoder handles; decoderCtor
// returns a fresh stateful decoder for each matched stream.
// Multiple decoders can be registered; the first matching decoder wins.
func WithDecoder(match func(string) bool, decoderCtor func() StreamDecoder) Option {
	return func(c *config) {
		c.decoders = append(c.decoders, decoderEntry{match: match, decoderCtor: decoderCtor})
	}
}

// WithSinkFile enables file sink for writing traces to disk.
// The file will contain length-delimited protobuf messages.
func WithSinkFile(path string, opts ...SinkFileOption) Option {
	return func(c *config) {
		c.sinkFilePath = path
		c.sinkFileOpts = opts
	}
}

// WithUnixSocket enables the private ingest socket on the given Unix domain socket path.
func WithUnixSocket(path string) Option {
	return func(c *config) {
		c.unixSocketPath = path
	}
}

// WithWaitForAttach blocks startup until an ingest client attaches to the Unix socket.
func WithWaitForAttach() Option {
	return func(c *config) {
		c.waitForAttach = true
	}
}

// WithSink adds a custom sink to receive trace events.
// Multiple sinks can be registered; each receives all events.
func WithSink(sink Sink) Option {
	return func(c *config) {
		c.sinks = append(c.sinks, sink)
	}
}

// WithOnMessage registers a factory that creates per-stream message handlers.
// Handlers receive decoded messages off the hot path in per-stream goroutines.
func WithOnMessage(factory OnMessageFactory) Option {
	return func(c *config) {
		c.onMessage = append(c.onMessage, factory)
	}
}

type sinkFileConfig struct {
	snapshotInterval time.Duration
}

// SinkFileOption configures the file sink.
type SinkFileOption func(*sinkFileConfig)

// SnapshotInterval sets how often periodic snapshots are written.
func SnapshotInterval(d time.Duration) SinkFileOption {
	return func(c *sinkFileConfig) {
		c.snapshotInterval = d
	}
}
