// Package api defines the JSON contract between the xray backend and any
// consumer of its REST/WebSocket surface (today: the Solid.js dashboard).
//
// These types are the source of truth: tygo regenerates the matching
// TypeScript declarations into dashboard/src/api.gen.ts as part of the
// dashboard build, so drift between Go and TS is caught at compile time.
package api

// BleedDistEntry counts bytes attributed to a particular bleed distance bucket
// ("1", "2", "3", "4+") for a slot or per-bucket breakdown row.
type BleedDistEntry struct {
	BytesIn  uint64 `json:"bytes_in"`
	BytesOut uint64 `json:"bytes_out"`
}

// SlotMeta carries per-slot metadata extracted from beacon block / attestation
// payloads. All fields are optional because the slot may not have observed a
// full block yet.
type SlotMeta struct {
	ProposerIndex    *uint64 `json:"proposer_index,omitempty"`
	AttestationCount *int    `json:"attestation_count,omitempty"`
	BlobCommitments  *int    `json:"blob_commitments,omitempty"`
	TxCount          *int    `json:"tx_count,omitempty"`
	BleedBytesIn     *uint64 `json:"bleed_bytes_in,omitempty"`
	BleedBytesOut    *uint64 `json:"bleed_bytes_out,omitempty"`
}

// SlotSummary is the headline view of a slot: totals, timing, and finalization.
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

// SlotBucketPoint is one 100ms bucket of traffic within a slot.
type SlotBucketPoint struct {
	OffsetMs  int64             `json:"offset_ms"`
	BytesIn   uint64            `json:"bytes_in"`
	BytesOut  uint64            `json:"bytes_out"`
	Breakdown []BucketBreakdown `json:"breakdown,omitempty"`
}

// BucketBreakdown attributes bytes within a single time bucket to a specific
// (protocol, topic, message_kind) tuple.
type BucketBreakdown struct {
	Protocol        string                    `json:"protocol"`
	Topic           string                    `json:"topic"`
	MessageKind     string                    `json:"message_kind"`
	BytesIn         uint64                    `json:"bytes_in"`
	BytesOut        uint64                    `json:"bytes_out"`
	MsgCount        uint64                    `json:"msg_count"`
	BleedBytesIn    uint64                    `json:"bleed_bytes_in"`
	BleedBytesOut   uint64                    `json:"bleed_bytes_out"`
	BleedByDistance map[string]BleedDistEntry `json:"bleed_by_distance,omitempty"`
}

// SlotBreakdown is the slot-level aggregate analogue of BucketBreakdown.
type SlotBreakdown struct {
	Protocol        string                    `json:"protocol"`
	Topic           string                    `json:"topic"`
	MessageKind     string                    `json:"message_kind"`
	BytesIn         uint64                    `json:"bytes_in"`
	BytesOut        uint64                    `json:"bytes_out"`
	MsgCount        uint64                    `json:"msg_count"`
	BleedBytesIn    uint64                    `json:"bleed_bytes_in"`
	BleedBytesOut   uint64                    `json:"bleed_bytes_out"`
	BleedByDistance map[string]BleedDistEntry `json:"bleed_by_distance,omitempty"`
}

// SlotDetail is the full per-slot payload returned by GET /api/slots/{slot}.
type SlotDetail struct {
	Summary   SlotSummary       `json:"summary"`
	Buckets   []SlotBucketPoint `json:"buckets"`
	Breakdown []SlotBreakdown   `json:"breakdown"`
}

// ConnState describes a single libp2p connection a peer holds with the source.
type ConnState struct {
	RemoteAddr string `json:"remote_addr"`
	LocalAddr  string `json:"local_addr,omitempty"`
	Direction  string `json:"direction"`
	Transport  string `json:"transport,omitempty"`
	Security   string `json:"security,omitempty"`
	Muxer      string `json:"muxer,omitempty"`
	OpenedAtNs int64  `json:"opened_at_ns"`
}

// PeerSummary is one row in the response to GET /api/peers.
type PeerSummary struct {
	PeerID      string      `json:"peer_id"`
	Connections []ConnState `json:"connections"`
	FirstSeenNs int64       `json:"first_seen_ns"`
	LastSeenNs  int64       `json:"last_seen_ns"`
}

// WsMessage is the union of WebSocket payloads pushed by the server.
//
// The Type field discriminates: "snapshot" carries Current and PeerCount;
// "slot_update" carries a single Slot; "slot_batch" carries Slots and Current.
type WsMessage struct {
	Type      string        `json:"type"`
	SourceID  string        `json:"source_id,omitempty"`
	Slot      *SlotSummary  `json:"slot,omitempty"`
	Slots     []SlotSummary `json:"slots,omitempty"`
	Current   uint64        `json:"current_slot,omitempty"`
	PeerCount *int          `json:"peer_count,omitempty"`
}
