package eth

import (
	"strings"
	"time"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/ethp2p/xray"
)

// SlotTracker returns a message handler factory that tracks slot transitions
// and detects cross-slot activity on GossipSub streams.
func SlotTracker(clock SlotClock, onBoundary func(prev, current uint64)) xray.OnMessageFactory {
	return func(streamID uint32, protocol string) xray.OnMessage {
		if !strings.HasPrefix(protocol, "/meshsub/") &&
			!strings.HasPrefix(protocol, "/floodsub/") {
			return nil
		}

		var prevSlot uint64
		var initialized bool

		return func(msg xray.DecodedMessage) {
			ref, ok := clock.At(time.Now())
			if !ok {
				return
			}
			if initialized && ref.Slot != prevSlot && onBoundary != nil {
				onBoundary(prevSlot, ref.Slot)
			}
			prevSlot = ref.Slot
			initialized = true

			rpc, ok := msg.Parsed.(*pubsubpb.RPC)
			if !ok {
				return
			}
			for _, pub := range rpc.Publish {
				if pub.Topic == nil {
					continue
				}
				if decodedSlot, ok := DecodeSlot(
					normalizeTopic(*pub.Topic), pub.Data,
				); ok && decodedSlot != ref.Slot {
					// Slot leakage: message for slot X arrived in slot Y.
					_ = decodedSlot
				}
			}
		}
	}
}
