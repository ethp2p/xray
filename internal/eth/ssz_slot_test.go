package eth

import (
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
)

func putSlotAt(buf []byte, offset int, slot uint64) {
	binary.LittleEndian.PutUint64(buf[offset:], slot)
}

func compressAndDecode(t *testing.T, topic string, raw []byte, wantSlot uint64) {
	t.Helper()
	compressed := snappy.Encode(nil, raw)
	slot, ok := DecodeSlot(topic, compressed)
	if !ok {
		t.Fatalf("DecodeSlot(%q) returned ok=false", topic)
	}
	if slot != wantSlot {
		t.Fatalf("DecodeSlot(%q) = %d, want %d", topic, slot, wantSlot)
	}
}

func TestDecodeSlot_BeaconBlock(t *testing.T) {
	data := make([]byte, 200)
	putSlotAt(data, 100, 42)
	compressAndDecode(t, "beacon_block", data, 42)
}

func TestDecodeSlot_AggregateAndProof(t *testing.T) {
	data := make([]byte, 300)
	putSlotAt(data, 212, 7777)
	compressAndDecode(t, "beacon_aggregate_and_proof", data, 7777)
}

func TestDecodeSlot_Attestation(t *testing.T) {
	data := make([]byte, 240)
	putSlotAt(data, 4, 99)
	compressAndDecode(t, "beacon_attestation_5", data, 99)
}

func TestDecodeSlot_AttestationSubnet0(t *testing.T) {
	data := make([]byte, 240)
	putSlotAt(data, 4, 101)
	compressAndDecode(t, "beacon_attestation_0", data, 101)
}

func TestDecodeSlot_ProposerSlashing(t *testing.T) {
	data := make([]byte, 100)
	putSlotAt(data, 0, 500)
	compressAndDecode(t, "proposer_slashing", data, 500)
}

func TestDecodeSlot_AttesterSlashing(t *testing.T) {
	data := make([]byte, 100)
	putSlotAt(data, 12, 600)
	compressAndDecode(t, "attester_slashing", data, 600)
}

func TestDecodeSlot_SyncCommittee(t *testing.T) {
	data := make([]byte, 100)
	putSlotAt(data, 0, 12345)
	compressAndDecode(t, "sync_committee_2", data, 12345)
}

func TestDecodeSlot_SyncCommitteeContributionAndProof(t *testing.T) {
	data := make([]byte, 200)
	putSlotAt(data, 8, 54321)
	compressAndDecode(t, "sync_committee_contribution_and_proof", data, 54321)
}

func TestDecodeSlot_DataColumnSidecar(t *testing.T) {
	data := make([]byte, 100)
	putSlotAt(data, 20, 999)
	compressAndDecode(t, "data_column_sidecar_64", data, 999)
}

func TestDecodeSlot_BlobSidecar(t *testing.T) {
	data := make([]byte, 131184)
	putSlotAt(data, 131176, 8080)
	compressAndDecode(t, "blob_sidecar_3", data, 8080)
}

func TestDecodeSlot_RawFallback(t *testing.T) {
	// Uncompressed SSZ should still decode (fallback path)
	data := make([]byte, 200)
	putSlotAt(data, 100, 77)
	slot, ok := DecodeSlot("beacon_block", data)
	if !ok {
		t.Fatal("raw fallback returned ok=false")
	}
	if slot != 77 {
		t.Fatalf("raw fallback slot = %d, want 77", slot)
	}
}

func TestDecodeSlot_UnknownTopic(t *testing.T) {
	data := make([]byte, 100)
	compressed := snappy.Encode(nil, data)
	_, ok := DecodeSlot("voluntary_exit", compressed)
	if ok {
		t.Fatal("expected ok=false for unknown topic")
	}
}

func TestDecodeSlot_TooShort(t *testing.T) {
	data := make([]byte, 10)
	compressed := snappy.Encode(nil, data)
	_, ok := DecodeSlot("beacon_block", compressed)
	if ok {
		t.Fatal("expected ok=false for payload shorter than offset+8")
	}
}

func TestDecodeSlot_EmptyPayload(t *testing.T) {
	_, ok := DecodeSlot("beacon_block", nil)
	if ok {
		t.Fatal("expected ok=false for nil payload")
	}
}

func TestStripSubnetID(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"beacon_attestation_0", "beacon_attestation"},
		{"beacon_attestation_63", "beacon_attestation"},
		{"sync_committee_3", "sync_committee"},
		{"data_column_sidecar_127", "data_column_sidecar"},
		{"blob_sidecar_5", "blob_sidecar"},
		{"beacon_block", "beacon_block"},
		{"sync_committee_contribution_and_proof", "sync_committee_contribution_and_proof"},
		{"beacon_attestation_", "beacon_attestation_"},
		{"beacon_attestation_abc", "beacon_attestation_abc"},
	}
	for _, tt := range tests {
		got := stripSubnetID(tt.input)
		if got != tt.want {
			t.Errorf("stripSubnetID(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
