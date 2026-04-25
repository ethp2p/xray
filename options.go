package xray

import "time"

type decoderEntry struct {
	match       func(string) bool
	decoderCtor func() StreamDecoder
}

type config struct {
	ringBufferSize int
	sinkFilePath   string
	sinkFileOpts   []SinkFileOption
	ingestAddr     string
	clientName     string
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

// WithIngestAddr sets the address of the backend ingest listener to dial.
// If the address contains '/' it is treated as a Unix domain socket path,
// otherwise as a TCP address.
func WithIngestAddr(addr string) Option {
	return func(c *config) {
		c.ingestAddr = addr
	}
}

// WithClientName sets the client name sent in the ClientHello handshake.
func WithClientName(name string) Option {
	return func(c *config) {
		c.clientName = name
	}
}

// WithWaitForAttach blocks startup until the probe connects to the backend.
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
