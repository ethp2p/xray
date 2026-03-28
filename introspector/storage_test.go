package introspector

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStorageWriteAndReadSlot(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	detail := SlotDetail{
		Summary:   SlotSummary{Slot: 100, Epoch: 3, BytesIn: 500, BytesOut: 300},
		Breakdown: []SlotBreakdown{{Protocol: "meshsub", BytesIn: 500}},
	}
	if err := s.WriteSlot("src1", detail); err != nil {
		t.Fatal(err)
	}
	raw, err := s.ReadSlot("src1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty slot data")
	}
	if !bytes.Contains(raw, []byte(`"slot":100`)) {
		t.Fatal("slot data should contain slot number")
	}
}

func TestStorageEpochIndex(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range []uint64{96, 97, 98} {
		detail := SlotDetail{
			Summary: SlotSummary{Slot: slot, Epoch: slot / 32, BytesIn: uint64(slot * 10)},
		}
		if err := s.WriteSlot("src1", detail); err != nil {
			t.Fatal(err)
		}
	}
	summaries := s.ListSummaries("src1", 10)
	if len(summaries) != 3 {
		t.Fatalf("expected 3 summaries, got %d", len(summaries))
	}
	if summaries[0].Slot != 98 || summaries[2].Slot != 96 {
		t.Fatal("summaries should be sorted by slot descending")
	}
}

func TestStorageDuplicateSlotSkipped(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	detail := SlotDetail{
		Summary: SlotSummary{Slot: 100, Epoch: 3},
	}
	if err := s.WriteSlot("src1", detail); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteSlot("src1", detail); err != nil {
		t.Fatal(err)
	}
	summaries := s.ListSummaries("src1", 10)
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary (deduped), got %d", len(summaries))
	}
}

func TestStorageTruncatedJSONLRecovery(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	detail := SlotDetail{
		Summary: SlotSummary{Slot: 100, Epoch: 3, BytesIn: 500},
	}
	if err := s.WriteSlot("src1", detail); err != nil {
		t.Fatal(err)
	}
	epochFile := filepath.Join(dir, "src1", "index", "3.jsonl")
	f, err := os.OpenFile(epochFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"slot":101,"epoch":3,"bytes_in":60`)
	f.Close()

	s2, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.LoadSummaryIndex("src1"); err != nil {
		t.Fatal(err)
	}
	summaries := s2.ListSummaries("src1", 10)
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary after recovery, got %d", len(summaries))
	}
	if summaries[0].Slot != 100 {
		t.Fatalf("expected slot 100, got %d", summaries[0].Slot)
	}
}

func TestStorageSourceMetadata(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	info := SourceInfo{
		SourceID:   "src1",
		PeerID:     []byte("peer123"),
		ClientName: "prysm/v5.2.0",
		Connected:  true,
	}
	if err := s.WriteSourceMeta(info); err != nil {
		t.Fatal(err)
	}

	s2, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	metas, err := s2.LoadSourceMetas()
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 source meta, got %d", len(metas))
	}
	if metas[0].ClientName != "prysm/v5.2.0" {
		t.Fatalf("wrong client name: %s", metas[0].ClientName)
	}
}

func TestStorageSearchSlots(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for slot := uint64(90); slot <= 110; slot++ {
		detail := SlotDetail{
			Summary: SlotSummary{Slot: slot, Epoch: slot / 32},
		}
		if err := s.WriteSlot("src1", detail); err != nil {
			t.Fatal(err)
		}
	}
	results := s.SearchSlots("src1", 95, 105, 100)
	if len(results) != 11 {
		t.Fatalf("expected 11 results (95-105), got %d", len(results))
	}
	if results[0].Slot != 105 {
		t.Fatalf("expected first result slot 105, got %d", results[0].Slot)
	}
}

func TestStoragePrune(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	genesisUnix := int64(1606824023)
	secondsPerSlot := uint64(12)
	slotsPerEpoch := uint64(32)

	// Compute a "current" slot relative to real genesis
	now := time.Now().Unix()
	currentSlot := uint64(now-genesisUnix) / secondsPerSlot

	// Write one old slot (should be pruned) and one recent slot (should survive)
	oldSlot := currentSlot - 400_000 // ~55 days ago
	recentSlot := currentSlot - 100  // ~20 minutes ago

	for _, slot := range []uint64{oldSlot, recentSlot} {
		detail := SlotDetail{
			Summary: SlotSummary{Slot: slot, Epoch: slot / slotsPerEpoch},
		}
		if err := s.WriteSlot("src1", detail); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Prune(30, slotsPerEpoch, secondsPerSlot, genesisUnix); err != nil {
		t.Fatal(err)
	}

	summaries := s.ListSummaries("src1", 100)
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary after prune, got %d", len(summaries))
	}
	if summaries[0].Slot != recentSlot {
		t.Fatalf("expected surviving slot %d, got %d", recentSlot, summaries[0].Slot)
	}
}
