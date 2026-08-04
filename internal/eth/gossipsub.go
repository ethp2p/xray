package eth

import (
	"strconv"
	"strings"

	"github.com/ethp2p/xray/internal/decode"
	gossipsub "github.com/ethp2p/xray/internal/gossipsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// GossipSubDecoder returns a gossipsub.Decoder configured for Ethereum networks.
// It normalizes Ethereum topic names and decodes SSZ slot numbers from published messages.
func GossipSubDecoder() gossipsub.Decoder {
	return gossipsub.Decoder{MessageDecoder: decodeMessage}
}

func decodeMessage(msg *pubsubpb.Message, tags map[string][]string) {
	if msg.Topic == nil {
		return
	}
	norm := normalizeTopic(*msg.Topic)
	tags[decode.TagTopic] = []string{norm}

	slot, slotOk, meta := DecodePayload(norm, msg.Data)
	if slotOk {
		tags[decode.TagDecodedSlot] = append(tags[decode.TagDecodedSlot], strconv.FormatUint(slot, 10))
		tags[decode.TagDecodedFrom] = []string{"gossipsub_publish"}
	}
	if meta.HasProposer {
		tags[decode.TagProposerIndex] = []string{strconv.FormatUint(meta.ProposerIndex, 10)}
	}
	if meta.HasAttestations {
		tags[decode.TagAttestationCount] = []string{strconv.Itoa(meta.AttestationCount)}
	}
	if meta.HasCommitments {
		tags[decode.TagBlobCommitments] = []string{strconv.Itoa(meta.BlobCommitments)}
	}
	if meta.HasTxCount {
		tags[decode.TagTxCount] = []string{strconv.Itoa(meta.TxCount)}
	}
	if meta.HasSidecarIndex {
		tags[decode.TagSidecarIndex] = []string{strconv.FormatUint(meta.SidecarIndex, 10)}
	}
}

func normalizeTopic(topic string) string {
	// /eth2/<forkDigest>/<topicName>/ssz_snappy -> <topicName>
	if strings.HasPrefix(topic, "/eth2/") {
		parts := strings.Split(topic, "/")
		if len(parts) >= 5 && parts[3] != "" {
			return parts[3]
		}
	}
	return topic
}
