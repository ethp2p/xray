package xray

// Tag names attached to DecodedMessage by the built-in decoders. Defined here
// so external callers can switch on tag values without reaching into the
// internal decoder packages.
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
