package eth

import (
	"strconv"
	"strings"

	gossipsub "github.com/ethp2p/instrument/libp2p/gossipsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

const (
	TagDecodedSlot = "eth.payload.slot"
	TagDecodedFrom = "eth.payload.from"

	TagProposerIndex    = "eth.proposer_index"
	TagAttestationCount = "eth.attestation_count"
	TagBlobCommitments  = "eth.blob_commitments"
	TagTxCount          = "eth.tx_count"
	TagSidecarIndex     = "eth.sidecar_index"

	// Re-exported from gossipsub for consumers who import only this package.
	TagTopic       = gossipsub.TagTopic
	TagMessageKind = gossipsub.TagMessageKind
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
	tags[TagTopic] = []string{norm}

	slot, slotOk, meta := DecodePayload(norm, msg.Data)
	if slotOk {
		tags[TagDecodedSlot] = append(tags[TagDecodedSlot], strconv.FormatUint(slot, 10))
		tags[TagDecodedFrom] = []string{"gossipsub_publish"}
	}
	if meta.HasProposer {
		tags[TagProposerIndex] = []string{strconv.FormatUint(meta.ProposerIndex, 10)}
	}
	if meta.HasAttestations {
		tags[TagAttestationCount] = []string{strconv.Itoa(meta.AttestationCount)}
	}
	if meta.HasCommitments {
		tags[TagBlobCommitments] = []string{strconv.Itoa(meta.BlobCommitments)}
	}
	if meta.HasTxCount {
		tags[TagTxCount] = []string{strconv.Itoa(meta.TxCount)}
	}
	if meta.HasSidecarIndex {
		tags[TagSidecarIndex] = []string{strconv.FormatUint(meta.SidecarIndex, 10)}
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
