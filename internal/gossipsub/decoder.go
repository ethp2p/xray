package gossipsub

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/gogo/protobuf/proto"

	"github.com/ethp2p/xray/internal/decode"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// DefaultMaxRPCSize matches pubsub.DefaultMaxMessageSize (1 MiB).
const DefaultMaxRPCSize = 1 << 20

// MessageDecoder processes an application-level payload within a gossipsub publish message.
// It receives the raw *pb.Message and may mutate tags to add or replace entries.
type MessageDecoder func(msg *pb.Message, tags map[string][]string)

// Decoder selects GossipSub/FloodSub streams and creates decoder instances.
type Decoder struct {
	// MaxRPCSize bounds buffered memory per direction per stream.
	// If 0, DefaultMaxRPCSize is used.
	MaxRPCSize int

	// MessageDecoder is called for each published message before emitting.
	// Nil means no application-level decoding.
	MessageDecoder MessageDecoder
}

// Match returns true for meshsub and floodsub protocols.
func (d Decoder) Match(protocol string) bool {
	return strings.HasPrefix(protocol, "/meshsub/") ||
		strings.HasPrefix(protocol, "/floodsub/")
}

// New returns a per-stream decoder instance.
func (d Decoder) New() decode.StreamDecoder {
	max := d.MaxRPCSize
	if max == 0 {
		max = DefaultMaxRPCSize
	}
	return &instance{
		in:         newVarintRPC(max),
		out:        newVarintRPC(max),
		msgDecoder: d.MessageDecoder,
	}
}

type instance struct {
	in         *bufferingRPC
	out        *bufferingRPC
	msgDecoder MessageDecoder
}

func (i *instance) ObserveRead(data []byte, emit decode.EmitFunc) error {
	return i.in.observe(i.msgDecoder, data, emit)
}

func (i *instance) ObserveWrite(data []byte, emit decode.EmitFunc) error {
	return i.out.observe(i.msgDecoder, data, emit)
}

func (i *instance) BufferedRead() int  { return i.in.buffered() }
func (i *instance) BufferedWrite() int { return i.out.buffered() }
func (i *instance) Reset()             { i.in.reset(); i.out.reset() }

// bufferingRPC incrementally parses varint-length-prefixed pubsub RPC frames.
// It buffers partial frames internally and emits one observation per action within each RPC.
// Not safe for concurrent use — callers must ensure serial access (decodeWorker does).
type bufferingRPC struct {
	max int
	buf []byte
}

func newVarintRPC(max int) *bufferingRPC {
	return &bufferingRPC{max: max}
}

func (v *bufferingRPC) buffered() int {
	return len(v.buf)
}

func (v *bufferingRPC) reset() {
	v.buf = v.buf[:0]
}

func (v *bufferingRPC) observe(msgDecoder MessageDecoder, data []byte, emit decode.EmitFunc) error {
	v.buf = append(v.buf, data...)

	for {
		if len(v.buf) == 0 {
			break
		}

		msgLen, headerLen := binary.Uvarint(v.buf)
		if headerLen == 0 {
			break
		}
		if headerLen < 0 {
			return errors.New("varint overflow")
		}

		if msgLen > uint64(v.max) {
			return errors.New("message too large")
		}

		totalLen := headerLen + int(msgLen)
		if len(v.buf) < totalLen {
			break
		}

		payload := v.buf[headerLen:totalLen]

		var rpc pb.RPC
		if err := proto.Unmarshal(payload, &rpc); err != nil {
			return err
		}

		innerBytes := emitActions(&rpc, msgDecoder, emit)

		// Framing bytes: varint prefix + proto field encoding overhead in the RPC envelope.
		if framing := totalLen - innerBytes; framing > 0 {
			emit(framing, []decode.Tag{{Name: decode.TagFraming}}, nil)
		}

		v.buf = v.buf[totalLen:]
	}

	return nil
}

// emitActions atomises a single RPC into one emit per logical action and returns
// the total number of bytes attributed to inner messages.
func emitActions(rpc *pb.RPC, msgDecoder MessageDecoder, emit decode.EmitFunc) int {
	inner := 0

	for _, sub := range rpc.Subscriptions {
		kind := "UNSUBSCRIBE"
		if sub.GetSubscribe() {
			kind = "SUBSCRIBE"
		}
		inner += emitTopicControl(sub.Size(), kind, sub.Topicid, sub, emit)
	}

	for _, msg := range rpc.Publish {
		size := msg.Size()
		inner += size
		tags := map[string][]string{
			decode.TagMessageKind: {"PUBLISH"},
		}
		if msg.Topic != nil {
			tags[decode.TagTopic] = []string{*msg.Topic}
		}
		if msgDecoder != nil {
			msgDecoder(msg, tags)
		}
		emit(size, tagsFromMap(tags), msg)
	}

	if ctrl := rpc.Control; ctrl != nil {
		for _, ihave := range ctrl.Ihave {
			inner += emitTopicControl(ihave.Size(), "IHAVE", ihave.TopicID, ihave, emit)
		}
		for _, iwant := range ctrl.Iwant {
			size := iwant.Size()
			inner += size
			emit(size, []decode.Tag{{Name: decode.TagMessageKind, Values: []string{"IWANT"}}}, iwant)
		}
		for _, graft := range ctrl.Graft {
			inner += emitTopicControl(graft.Size(), "GRAFT", graft.TopicID, graft, emit)
		}
		for _, prune := range ctrl.Prune {
			inner += emitTopicControl(prune.Size(), "PRUNE", prune.TopicID, prune, emit)
		}
		for _, idw := range ctrl.Idontwant {
			size := idw.Size()
			inner += size
			emit(size, []decode.Tag{{Name: decode.TagMessageKind, Values: []string{"IDONTWANT"}}}, idw)
		}
	}

	return inner
}

// emitTopicControl emits a control action with a message_kind tag and optional topic tag.
// Returns the size for accumulation into inner bytes. Builds the tag slice directly
// without an intermediate map since control messages are not mutated before emit.
func emitTopicControl(size int, kind string, topicID *string, parsed any, emit decode.EmitFunc) int {
	tags := make([]decode.Tag, 1, 2)
	tags[0] = decode.Tag{Name: decode.TagMessageKind, Values: []string{kind}}
	if topicID != nil {
		tags = append(tags, decode.Tag{Name: decode.TagTopic, Values: []string{*topicID}})
	}
	emit(size, tags, parsed)
	return size
}

func tagsFromMap(m map[string][]string) []decode.Tag {
	tags := make([]decode.Tag, 0, len(m))
	for name, values := range m {
		tags = append(tags, decode.Tag{Name: name, Values: values})
	}
	return tags
}
