package storage

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStorageWriteAndReadSlot(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
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

	db, err := sql.Open("sqlite3", filepath.Join(dir, "xray.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var detailType, detailSlot string
	err = db.QueryRow(`
		SELECT typeof(detail), json_extract(detail, '$.summary.slot')
		FROM slots
		WHERE source_id = ? AND slot = ?
	`, "src1", 100).Scan(&detailType, &detailSlot)
	if err != nil {
		t.Fatal(err)
	}
	if detailType != "blob" {
		t.Fatalf("expected JSONB detail to be stored as blob, got %s", detailType)
	}
	if detailSlot != "100" {
		t.Fatalf("expected JSONB detail to be queryable, got slot %s", detailSlot)
	}

	var columnType string
	err = db.QueryRow(`
		SELECT type
		FROM pragma_table_info('slots')
		WHERE name = 'detail'
	`).Scan(&columnType)
	if err != nil {
		t.Fatal(err)
	}
	if columnType != "JSONB" {
		t.Fatalf("expected detail column type JSONB, got %s", columnType)
	}
}

func TestStorageListSummariesSorted(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, slot := range []uint64{96, 97, 98} {
		detail := SlotDetail{
			Summary: SlotSummary{Slot: slot, Epoch: slot / 32, BytesIn: uint64(slot * 10)},
		}
		if err := s.WriteSlot("src1", detail); err != nil {
			t.Fatal(err)
		}
	}
	summaries, err := s.ListSummaries("src1", 10)
	if err != nil {
		t.Fatal(err)
	}
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
	defer s.Close()
	detail := SlotDetail{
		Summary: SlotSummary{Slot: 100, Epoch: 3},
	}
	if err := s.WriteSlot("src1", detail); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteSlot("src1", detail); err != nil {
		t.Fatal(err)
	}
	summaries, err := s.ListSummaries("src1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary (deduped), got %d", len(summaries))
	}
}

func TestStorageMigratesLegacySlotFiles(t *testing.T) {
	dir := t.TempDir()
	detail := SlotDetail{
		Summary: SlotSummary{Slot: 100, Epoch: 3, BytesIn: 500},
	}
	if err := os.MkdirAll(filepath.Join(dir, "src1", "slots"), 0755); err != nil {
		t.Fatal(err)
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "src1", "slots", "100.json"), detailJSON, 0644); err != nil {
		t.Fatal(err)
	}
	s2, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	summaries, err := s2.ListSummaries("src1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary after recovery, got %d", len(summaries))
	}
	if summaries[0].Slot != 100 {
		t.Fatalf("expected slot 100, got %d", summaries[0].Slot)
	}
	if _, err := os.Stat(filepath.Join(dir, "src1", "slots")); !os.IsNotExist(err) {
		t.Fatal("legacy slots dir should be removed after migration")
	}
}

func TestStorageLegacyMigrationQuarantinesMalformedSlotFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "src1", "slots"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "src1", "slots", "100.json"), []byte(`{"summary":`), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := os.Stat(filepath.Join(dir, ".legacy-json-migrated")); err != nil {
		t.Fatal("migration marker should be written after quarantining malformed legacy data")
	}
	if _, err := os.Stat(filepath.Join(dir, "legacy-failed", "src1", "slots", "100.json")); err != nil {
		t.Fatalf("expected malformed legacy slot to be quarantined: %v", err)
	}
	summaries, err := s.ListSummaries("src1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("expected no migrated summaries from malformed legacy data, got %d", len(summaries))
	}
}

func TestStorageSourceMetadata(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
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
	defer s2.Close()
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
	defer s.Close()
	for slot := uint64(90); slot <= 110; slot++ {
		detail := SlotDetail{
			Summary: SlotSummary{Slot: slot, Epoch: slot / 32},
		}
		if err := s.WriteSlot("src1", detail); err != nil {
			t.Fatal(err)
		}
	}
	results, err := s.SearchSlots("src1", 95, 105, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 11 {
		t.Fatalf("expected 11 results (95-105), got %d", len(results))
	}
	if results[0].Slot != 105 {
		t.Fatalf("expected first result slot 105, got %d", results[0].Slot)
	}
}

func TestStorageWriteThenSearch(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	detail := SlotDetail{
		Summary: SlotSummary{Slot: 100, Epoch: 3, BytesIn: 500, BytesOut: 300},
	}
	if err := s.WriteSlot("src1", detail); err != nil {
		t.Fatal(err)
	}
	results, err := s.SearchSlots("src1", 99, 101, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 search result, got %d", len(results))
	}
	if results[0].Slot != 100 {
		t.Fatalf("expected slot 100, got %d", results[0].Slot)
	}
}

func TestStorageRejectsSlotsOutsideSQLiteIntegerRange(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.WriteSlot("src1", SlotDetail{Summary: SlotSummary{Slot: uint64(1 << 63)}})
	if err == nil {
		t.Fatal("expected slot outside sqlite integer range to fail")
	}
}

func TestStoragePrune(t *testing.T) {
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

	summaries, err := s.ListSummaries("src1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary after prune, got %d", len(summaries))
	}
	if summaries[0].Slot != recentSlot {
		t.Fatalf("expected surviving slot %d, got %d", recentSlot, summaries[0].Slot)
	}
}
