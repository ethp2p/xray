package processor

import "github.com/ethp2p/xray/api"

// Re-export public API types so internal code can refer to them by short
// names. The canonical definitions live in package api.
type (
	BleedDistEntry  = api.BleedDistEntry
	SlotMeta        = api.SlotMeta
	SlotSummary     = api.SlotSummary
	SlotBucketPoint = api.SlotBucketPoint
	BucketBreakdown = api.BucketBreakdown
	SlotBreakdown   = api.SlotBreakdown
	SlotDetail      = api.SlotDetail
)

// FinalizedSlot bridges the processor's finalize callback to the storage
// goroutine in cmd/xray. It is not part of the public API.
type FinalizedSlot struct {
	SourceID string
	Detail   SlotDetail
}

// wsMessage is the internal representation matching api.WsMessage. We keep it
// here so the server package can build it without importing api directly.
type wsMessage = api.WsMessage
