package eth

import (
	"encoding/binary"

	_ "github.com/ferranbt/fastssz"
)

// SSZ field offsets verified against Fulu consensus-spec container layouts.
//
// SignedBeaconBlock: [msg_off:4][sig:96] | BeaconBlock@100
//   BeaconBlock: [slot:8][proposer_index:8][parent_root:32][state_root:32][body_off:4]
//   BeaconBlockBody fixed region (396 bytes):
//     [randao:96][eth1:72][graffiti:32]
//     [prop_slash_off:4][att_slash_off:4][attestations_off:4][deposits_off:4][vol_exits_off:4]
//     [sync_agg:160]
//     [exec_payload_off:4][bls_changes_off:4][blob_kzg_off:4][exec_requests_off:4]
//   ExecutionPayload fixed region (528 bytes, Deneb+):
//     [parent_hash:32][fee_recipient:20][state_root:32][receipts_root:32]
//     [logs_bloom:256][prev_randao:32]
//     [block_num:8][gas_limit:8][gas_used:8][timestamp:8]
//     [extra_data_off:4][base_fee:32][block_hash:32]
//     [transactions_off:4][withdrawals_off:4]
//     [blob_gas_used:8][excess_blob_gas:8]
//
// BlobSidecar:       [index:8][blob:131072][commit:48][proof:48][header...]
// DataColumnSidecar: [index:8][col_off:4][commits_off:4][proofs_off:4][header...]

// SSZMeta holds extracted metadata from an SSZ gossip payload.
type SSZMeta struct {
	ProposerIndex    uint64
	AttestationCount int
	BlobCommitments  int
	TxCount          int
	SidecarIndex     uint64
	HasProposer      bool
	HasAttestations  bool
	HasCommitments   bool
	HasTxCount       bool
	HasSidecarIndex  bool
}

// DecodeMeta extracts metadata fields from a decompressed SSZ payload
// based on the normalized topic name.
func DecodeMeta(normalizedTopic string, data []byte) SSZMeta {
	base := stripSubnetID(normalizedTopic)
	var m SSZMeta

	switch base {
	case "beacon_block":
		m = decodeBeaconBlockMeta(data)
	case "blob_sidecar":
		if len(data) >= 8 {
			m.SidecarIndex = binary.LittleEndian.Uint64(data[0:8])
			m.HasSidecarIndex = true
		}
	case "data_column_sidecar":
		if len(data) >= 8 {
			m.SidecarIndex = binary.LittleEndian.Uint64(data[0:8])
			m.HasSidecarIndex = true
		}
	}
	return m
}

func decodeBeaconBlockMeta(data []byte) SSZMeta {
	var m SSZMeta

	// SignedBeaconBlock: [msg_offset:4][sig:96] | BeaconBlock at offset 100
	const blockBase = 100

	// Need at least blockBase + 84 for proposer_index and body_offset
	if len(data) < blockBase+84 {
		return m
	}

	m.ProposerIndex = binary.LittleEndian.Uint64(data[blockBase+8 : blockBase+16])
	m.HasProposer = true

	// body_offset at BeaconBlock relative offset 80
	bodyOff := binary.LittleEndian.Uint32(data[blockBase+80 : blockBase+84])
	bodyBase := blockBase + int(bodyOff)

	// BeaconBlockBody (Fulu): execution_requests_offset ends at bodyBase+396
	if len(data) < bodyBase+396 {
		return m
	}

	// Attestation count: variable-size list between attestations_offset and deposits_offset
	attOff := int(binary.LittleEndian.Uint32(data[bodyBase+208 : bodyBase+212]))
	depOff := int(binary.LittleEndian.Uint32(data[bodyBase+212 : bodyBase+216]))
	attStart := bodyBase + attOff
	attEnd := bodyBase + depOff
	if attEnd > attStart && attStart+4 <= len(data) {
		firstOff := int(binary.LittleEndian.Uint32(data[attStart : attStart+4]))
		if firstOff > 0 && firstOff%4 == 0 {
			m.AttestationCount = firstOff / 4
			m.HasAttestations = true
		}
	} else if attEnd == attStart {
		m.AttestationCount = 0
		m.HasAttestations = true
	}

	// Blob KZG commitment count: fixed-size (48 bytes each) between blob_kzg and execution_requests offsets
	blobKzgOff := int(binary.LittleEndian.Uint32(data[bodyBase+388 : bodyBase+392]))
	execReqOff := int(binary.LittleEndian.Uint32(data[bodyBase+392 : bodyBase+396]))
	regionSize := execReqOff - blobKzgOff
	if regionSize >= 0 {
		m.BlobCommitments = regionSize / 48
		m.HasCommitments = true
	}

	// Transaction count inside ExecutionPayload
	epOff := int(binary.LittleEndian.Uint32(data[bodyBase+380 : bodyBase+384]))
	epBase := bodyBase + epOff
	if epBase+512 <= len(data) {
		txOff := int(binary.LittleEndian.Uint32(data[epBase+504 : epBase+508]))
		wdOff := int(binary.LittleEndian.Uint32(data[epBase+508 : epBase+512]))
		txStart := epBase + txOff
		txEnd := epBase + wdOff
		if txEnd > txStart && txStart+4 <= len(data) {
			firstOff := int(binary.LittleEndian.Uint32(data[txStart : txStart+4]))
			if firstOff > 0 && firstOff%4 == 0 {
				m.TxCount = firstOff / 4
				m.HasTxCount = true
			}
		} else if txEnd == txStart {
			m.TxCount = 0
			m.HasTxCount = true
		}
	}

	return m
}
