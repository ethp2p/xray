package eth

import (
	"encoding/binary"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/golang/snappy"
)

// Cached decode result for avoiding repeated Snappy decompression of
// the same gossip payload received from multiple peers.
type cachedPayload struct {
	slot   uint64
	slotOk bool
	meta   SSZMeta
}

const decodeCacheMax = 256

var (
	decodeCacheMu sync.Mutex
	decodeCache   = make(map[uint64]cachedPayload, decodeCacheMax)
)

// payloadKey computes a fast fingerprint from the compressed data.
// Same content from different peers produces identical Snappy output.
func payloadKey(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

// Offsets derived from Fulu consensus-spec SSZ container layouts.
//
//	beacon_block (SignedBeaconBlock):         [msg_off:4][sig:96] | BeaconBlock.slot@0
//	beacon_aggregate_and_proof:               [msg_off:4][sig:96] | [agg_idx:8][agg_off:4][sel_proof:96] | Attestation[agg_bits_off:4][AttData.slot@0]
//	beacon_attestation (Electra Attestation): [agg_bits_off:4][AttData.slot@0]
//	proposer_slashing:                        all-fixed; header1.message.slot@0
//	attester_slashing (Electra):              [att1_off:4][att2_off:4] | IndexedAtt[indices_off:4][AttData.slot@0]
//	sync_committee (SyncCommitteeMessage):    all-fixed; slot is first field
//	sync_committee_contribution_and_proof:    [msg:ContribAndProof][sig:96]; within ContribAndProof: [agg_idx:8][SyncContrib.slot@0]
//	data_column_sidecar:                      [index:8][col_off:4][commits_off:4][proofs_off:4] | SignedBlockHeader.message.slot@0
//	blob_sidecar:                             all-fixed; [index:8][blob:131072][commit:48][proof:48] | header.slot@0
var topicSlotOffset = map[string]int{
	"beacon_block":                          100,
	"beacon_aggregate_and_proof":            212,
	"beacon_attestation":                    4,
	"proposer_slashing":                     0,
	"attester_slashing":                     12,
	"sync_committee":                        0,
	"sync_committee_contribution_and_proof": 8,
	"data_column_sidecar":                   20,
	"blob_sidecar":                          131176,
}

// Subnet-parameterized topic prefixes whose trailing _N should be stripped.
var subnetPrefixes = []string{
	"beacon_attestation_",
	"sync_committee_",
	"data_column_sidecar_",
	"blob_sidecar_",
}

// DecodeSlot reads the slot number from a Snappy-compressed SSZ gossip
// payload. normalizedTopic is the output of normalizeTopic (e.g.
// "beacon_block", "beacon_attestation_3"). Returns (0, false) when the
// topic is unknown or the payload is too short.
func DecodeSlot(normalizedTopic string, compressedData []byte) (uint64, bool) {
	base := stripSubnetID(normalizedTopic)
	offset, ok := topicSlotOffset[base]
	if !ok {
		return 0, false
	}

	data, err := snappy.Decode(nil, compressedData)
	if err != nil {
		data = compressedData
	}

	end := offset + 8
	if len(data) < end {
		return 0, false
	}
	return binary.LittleEndian.Uint64(data[offset:end]), true
}

// DecodePayload decompresses a gossip payload once and extracts both
// the slot number and SSZ metadata. Results are cached by content hash
// to avoid re-decompressing the same payload from multiple peers.
func DecodePayload(normalizedTopic string, compressedData []byte) (slot uint64, slotOk bool, meta SSZMeta) {
	key := payloadKey(compressedData)

	decodeCacheMu.Lock()
	if cached, ok := decodeCache[key]; ok {
		decodeCacheMu.Unlock()
		return cached.slot, cached.slotOk, cached.meta
	}
	decodeCacheMu.Unlock()

	data, err := snappy.Decode(nil, compressedData)
	if err != nil {
		data = compressedData
	}

	base := stripSubnetID(normalizedTopic)
	offset, ok := topicSlotOffset[base]
	if ok {
		end := offset + 8
		if len(data) >= end {
			slot = binary.LittleEndian.Uint64(data[offset:end])
			slotOk = true
		}
	}

	meta = DecodeMeta(normalizedTopic, data)

	decodeCacheMu.Lock()
	if len(decodeCache) >= decodeCacheMax {
		// Evict all on overflow (simple, infrequent)
		decodeCache = make(map[uint64]cachedPayload, decodeCacheMax)
	}
	decodeCache[key] = cachedPayload{slot: slot, slotOk: slotOk, meta: meta}
	decodeCacheMu.Unlock()

	return
}

// stripSubnetID removes the trailing _N from subnet-parameterized topics.
// "beacon_attestation_3" -> "beacon_attestation"
// "beacon_block" -> "beacon_block" (unchanged)
func stripSubnetID(topic string) string {
	for _, prefix := range subnetPrefixes {
		if strings.HasPrefix(topic, prefix) {
			suffix := topic[len(prefix):]
			if len(suffix) > 0 && isDigits(suffix) {
				return topic[:len(prefix)-1]
			}
		}
	}
	return topic
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
