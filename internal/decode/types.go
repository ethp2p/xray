// Package decode holds stream-decode types shared by gossipsub framing and the
// backend processor. The probe SDK does not depend on this package.
package decode

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

// DecodedMessage is produced by a decode pipeline for each complete protocol message.
type DecodedMessage struct {
	StreamID  uint32
	ConnID    uint32
	Protocol  string
	Direction Direction
	WireBytes int
	Tags      []Tag
	Parsed    any
}

// OnMessage processes a single decoded message.
type OnMessage func(msg DecodedMessage)

// OnMessageFactory creates a per-stream OnMessage handler.
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
	BufferedRead() int

	// BufferedWrite returns bytes buffered waiting for a full message.
	BufferedWrite() int

	// Reset clears internal buffers (called on stream close/reset).
	Reset()
}
