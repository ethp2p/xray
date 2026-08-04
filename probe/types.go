package probe

import xraypb "github.com/ethp2p/xray/proto/xray"

// Sink receives envelopes from the emitter.
type Sink interface {
	// Write delivers an envelope to the sink.
	// Returns false if the sink cannot accept (backpressure).
	Write(env *xraypb.Envelope) bool

	// Close shuts down the sink.
	Close() error
}

// direction is the probe-local traffic direction used when emitting chunks.
type direction int

const (
	directionUnknown direction = iota
	directionIn
	directionOut
)
