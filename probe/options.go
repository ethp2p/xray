package probe

import "time"

type config struct {
	ringBufferSize int
	sinkFilePath   string
	sinkFileOpts   []SinkFileOption
	ingestAddr     string
	clientName     string
	waitForAttach  bool
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
// Intended for tests and advanced embeddings; not part of the root compat shim.
func WithSink(sink Sink) Option {
	return func(c *config) {
		c.sinks = append(c.sinks, sink)
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
