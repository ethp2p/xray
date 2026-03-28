package probe

import pb "github.com/ethp2p/wiretap/proto"

// Sink receives trace events from the collector.
type Sink interface {
	// Write sends an event to the sink.
	// Returns false if sink cannot accept (backpressure).
	Write(event *pb.TraceEvent) bool

	// Close shuts down the sink.
	Close() error
}
