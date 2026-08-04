package xray

import (
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	"github.com/ethp2p/xray/probe"
)

// Types — aliases preserve identity (xray.Host === probe.Host).
type (
	Host           = probe.Host
	Option         = probe.Option
	SinkFileOption = probe.SinkFileOption
)

// Wrap creates an instrumented host that records traffic and connection events.
func Wrap(h host.Host, opts ...Option) (*Host, error) {
	return probe.Wrap(h, opts...)
}

// Wiretap is retained for ethp2p/prysm @ 1fcc706ce.
//
// Deprecated: use Wrap.
func Wiretap(h host.Host, opts ...Option) (*Host, error) {
	return Wrap(h, opts...)
}

func WithIngestAddr(addr string) Option { return probe.WithIngestAddr(addr) }
func WithClientName(name string) Option { return probe.WithClientName(name) }
func WithWaitForAttach() Option         { return probe.WithWaitForAttach() }
func WithSinkFile(path string, opts ...SinkFileOption) Option {
	return probe.WithSinkFile(path, opts...)
}
func SnapshotInterval(d time.Duration) SinkFileOption { return probe.SnapshotInterval(d) }
func WithRingBufferSize(size int) Option              { return probe.WithRingBufferSize(size) }
