package decode

// Tag names attached to decoded messages by the built-in decoders.
const (
	TagTopic       = "topic"
	TagMessageKind = "message_kind"
	TagFraming     = "_framing"

	TagDecodedSlot = "eth.payload.slot"
	TagDecodedFrom = "eth.payload.from"

	TagProposerIndex    = "eth.proposer_index"
	TagAttestationCount = "eth.attestation_count"
	TagBlobCommitments  = "eth.blob_commitments"
	TagTxCount          = "eth.tx_count"
	TagSidecarIndex     = "eth.sidecar_index"
)
