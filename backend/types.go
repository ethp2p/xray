package backend

type BleedDistEntry struct {
	BytesIn  uint64 `json:"bytes_in"`
	BytesOut uint64 `json:"bytes_out"`
}

type SlotMeta struct {
	ProposerIndex    *uint64 `json:"proposer_index,omitempty"`
	AttestationCount *int    `json:"attestation_count,omitempty"`
	BlobCommitments  *int    `json:"blob_commitments,omitempty"`
	TxCount          *int    `json:"tx_count,omitempty"`
	BleedBytesIn     *uint64 `json:"bleed_bytes_in,omitempty"`
	BleedBytesOut    *uint64 `json:"bleed_bytes_out,omitempty"`
}

type SlotSummary struct {
	Slot          uint64   `json:"slot"`
	Epoch         uint64   `json:"epoch"`
	SlotStartNs   int64    `json:"slot_start_ns"`
	SlotEndNs     int64    `json:"slot_end_ns"`
	BytesIn       uint64   `json:"bytes_in"`
	BytesOut      uint64   `json:"bytes_out"`
	Finalized     bool     `json:"finalized"`
	LastUpdatedNs int64    `json:"last_updated_ns"`
	Meta          SlotMeta `json:"meta"`
}

type SlotBucketPoint struct {
	OffsetMs  int64             `json:"offset_ms"`
	BytesIn   uint64            `json:"bytes_in"`
	BytesOut  uint64            `json:"bytes_out"`
	Breakdown []BucketBreakdown `json:"breakdown,omitempty"`
}

type BucketBreakdown struct {
	Protocol        string                      `json:"protocol"`
	Topic           string                      `json:"topic"`
	MessageKind     string                      `json:"message_kind"`
	BytesIn         uint64                      `json:"bytes_in"`
	BytesOut        uint64                      `json:"bytes_out"`
	MsgCount        uint64                      `json:"msg_count"`
	BleedBytesIn    uint64                      `json:"bleed_bytes_in"`
	BleedBytesOut   uint64                      `json:"bleed_bytes_out"`
	BleedByDistance map[string]BleedDistEntry   `json:"bleed_by_distance,omitempty"`
}

type SlotBreakdown struct {
	Protocol        string                      `json:"protocol"`
	Topic           string                      `json:"topic"`
	MessageKind     string                      `json:"message_kind"`
	BytesIn         uint64                      `json:"bytes_in"`
	BytesOut        uint64                      `json:"bytes_out"`
	MsgCount        uint64                      `json:"msg_count"`
	BleedBytesIn    uint64                      `json:"bleed_bytes_in"`
	BleedBytesOut   uint64                      `json:"bleed_bytes_out"`
	BleedByDistance map[string]BleedDistEntry   `json:"bleed_by_distance,omitempty"`
}

type SlotDetail struct {
	Summary   SlotSummary       `json:"summary"`
	Buckets   []SlotBucketPoint `json:"buckets"`
	Breakdown []SlotBreakdown   `json:"breakdown"`
}

type wsMessage struct {
	Type      string        `json:"type"`
	SourceID  string        `json:"source_id,omitempty"`
	Slot      *SlotSummary  `json:"slot,omitempty"`
	Slots     []SlotSummary `json:"slots,omitempty"`
	Current   uint64        `json:"current_slot,omitempty"`
	PeerCount *int          `json:"peer_count,omitempty"`
}

type FinalizedSlot struct {
	SourceID string
	Detail   SlotDetail
}

