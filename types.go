package xray

import ingestpb "github.com/ethp2p/xray/proto/ingest"

// Sink receives envelopes from the emitter.
type Sink interface {
	// Write delivers an envelope to the sink.
	// Returns false if the sink cannot accept (backpressure).
	Write(env *ingestpb.Envelope) bool

	// Close shuts down the sink.
	Close() error
}

// Direction represents the direction of traffic flow.
type Direction int

const (
	DirectionUnknown Direction = iota
	DirectionIn
	DirectionOut
)

// Tag represents a key with one or more values for message categorization.
type Tag struct {
	Name   string
	Values []string
}

// DecodedMessage is produced by the async decode pipeline for each
// complete protocol message. Handlers receive these off the hot path.
type DecodedMessage struct {
	StreamID  uint32
	ConnID    uint32
	Protocol  string
	Direction Direction
	WireBytes int
	Tags      []Tag
	Parsed    any
}

// OnMessage processes a single decoded message. Called from a per-stream
// goroutine; never concurrent for the same stream, but multiple streams
// may call different OnMessage instances concurrently.
type OnMessage func(msg DecodedMessage)

// OnMessageFactory creates a per-stream OnMessage handler. Called once when
// a decoder matches the stream's protocol. Return nil to skip this stream.
type OnMessageFactory func(streamID uint32, protocol string) OnMessage

// EmitFunc is called once per decoded message.
// wireBytes is the number of bytes attributable to the message on the stream
// (including any framing bytes). parsed carries the decoder's native object
// (e.g. *pubsubpb.RPC) for downstream handlers.
type EmitFunc func(wireBytes int, tags []Tag, parsed any)

// StreamDecoder is a stateful decoder bound to one stream.
//
// ObserveRead and ObserveWrite may be called concurrently (read/write goroutines),
// but instrumentation may assume at most one goroutine calls each method at a time.
type StreamDecoder interface {
	// ObserveRead consumes bytes read from the stream (remote -> local).
	// Implementations must not retain data after returning.
	ObserveRead(data []byte, emit EmitFunc) error

	// ObserveWrite consumes bytes written to the stream (local -> remote).
	// Implementations must not retain data after returning.
	ObserveWrite(data []byte, emit EmitFunc) error

	// BufferedRead returns bytes buffered waiting for a full message.
	//
	// This is used on stream close/reset to account for bytes that were read
	// but never completed a full framed message (e.g., stream closed mid-message).
	BufferedRead() int

	// BufferedWrite returns bytes buffered waiting for a full message.
	//
	// This is used on stream close/reset to account for bytes that were written
	// but never completed a full framed message (e.g., stream closed mid-message).
	BufferedWrite() int

	// Reset clears internal buffers (called on stream close/reset).
	Reset()
}
