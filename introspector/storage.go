package introspector

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Storage struct {
	dataDir string
	mu      sync.Mutex
	indices map[string][]SlotSummary // sourceID -> sorted by slot descending
}

func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	return &Storage{
		dataDir: dataDir,
		indices: make(map[string][]SlotSummary),
	}, nil
}

// WriteSlot persists a finalized slot to disk and updates the in-memory index.
// Slot JSON is written atomically via temp-file + rename. The epoch JSONL index
// is append-only with per-slot dedup.
func (s *Storage) WriteSlot(sourceID string, detail SlotDetail) error {
	slotsDir := filepath.Join(s.dataDir, sourceID, "slots")
	if err := os.MkdirAll(slotsDir, 0755); err != nil {
		return fmt.Errorf("create slots dir: %w", err)
	}

	data, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal slot detail: %w", err)
	}

	// Atomic write: temp file then rename
	slotFile := filepath.Join(slotsDir, fmt.Sprintf("%d.json", detail.Summary.Slot))
	tmpFile := slotFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("write temp slot file: %w", err)
	}
	if err := os.Rename(tmpFile, slotFile); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("rename slot file: %w", err)
	}

	epoch := detail.Summary.Slot / 32
	indexDir := filepath.Join(s.dataDir, sourceID, "index")
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	epochFile := filepath.Join(indexDir, fmt.Sprintf("%d.jsonl", epoch))

	if slotExistsInEpochFile(epochFile, detail.Summary.Slot) {
		return nil
	}

	summaryLine, err := json.Marshal(detail.Summary)
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}

	f, err := os.OpenFile(epochFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open epoch file: %w", err)
	}
	_, writeErr := fmt.Fprintf(f, "%s\n", summaryLine)
	closeErr := f.Close()
	if writeErr != nil {
		return fmt.Errorf("write epoch line: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close epoch file: %w", closeErr)
	}

	s.mu.Lock()
	s.insertSummaryLocked(sourceID, detail.Summary)
	s.mu.Unlock()

	return nil
}

// slotExistsInEpochFile checks whether a slot is already recorded in the JSONL.
func slotExistsInEpochFile(path string, slot uint64) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	needle := fmt.Sprintf(`"slot":%d`, slot)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), needle) {
			var summary SlotSummary
			if json.Unmarshal(scanner.Bytes(), &summary) == nil && summary.Slot == slot {
				return true
			}
		}
	}
	return false
}

// insertSummaryLocked inserts a summary maintaining descending slot order.
// Caller must hold s.mu.
func (s *Storage) insertSummaryLocked(sourceID string, summary SlotSummary) {
	idx := s.indices[sourceID]

	// Dedup: check if slot already in index
	for _, existing := range idx {
		if existing.Slot == summary.Slot {
			return
		}
	}

	// Binary search for insertion point (descending order)
	pos := sort.Search(len(idx), func(i int) bool {
		return idx[i].Slot < summary.Slot
	})
	idx = append(idx, SlotSummary{})
	copy(idx[pos+1:], idx[pos:])
	idx[pos] = summary
	s.indices[sourceID] = idx
}

func (s *Storage) ReadSlot(sourceID string, slot uint64) ([]byte, error) {
	path := filepath.Join(s.dataDir, sourceID, "slots", fmt.Sprintf("%d.json", slot))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read slot %d: %w", slot, err)
	}
	return data, nil
}

func (s *Storage) ListSummaries(sourceID string, limit int) []SlotSummary {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.indices[sourceID]
	if len(idx) == 0 {
		return nil
	}
	if limit > len(idx) {
		limit = len(idx)
	}
	out := make([]SlotSummary, limit)
	copy(out, idx[:limit])
	return out
}

// SearchSlots returns summaries in [fromSlot, toSlot] sorted descending, up to limit.
func (s *Storage) SearchSlots(sourceID string, fromSlot, toSlot uint64, limit int) []SlotSummary {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.indices[sourceID]
	if len(idx) == 0 {
		return nil
	}

	// Index is sorted descending. Find the first entry <= toSlot.
	start := sort.Search(len(idx), func(i int) bool {
		return idx[i].Slot <= toSlot
	})

	var out []SlotSummary
	for i := start; i < len(idx) && len(out) < limit; i++ {
		if idx[i].Slot < fromSlot {
			break
		}
		out = append(out, idx[i])
	}
	return out
}

func (s *Storage) WriteSourceMeta(info SourceInfo) error {
	dir := filepath.Join(s.dataDir, info.SourceID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create source dir: %w", err)
	}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal source info: %w", err)
	}
	path := filepath.Join(dir, "source.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write temp source meta: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename source meta: %w", err)
	}
	return nil
}

func (s *Storage) LoadSourceMetas() ([]SourceInfo, error) {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}
	var metas []SourceInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(s.dataDir, entry.Name(), "source.json")
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var info SourceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		metas = append(metas, info)
	}
	return metas, nil
}

// LoadSummaryIndex reads all epoch JSONL files for a source into memory.
// Truncated lines are silently discarded (crash recovery).
func (s *Storage) LoadSummaryIndex(sourceID string) error {
	indexDir := filepath.Join(s.dataDir, sourceID, "index")
	entries, err := os.ReadDir(indexDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read index dir: %w", err)
	}

	var all []SlotSummary
	seen := make(map[uint64]bool)

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(indexDir, entry.Name())
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var summary SlotSummary
			if err := json.Unmarshal(line, &summary); err != nil {
				// Truncated or corrupt line; skip (crash recovery)
				continue
			}
			if seen[summary.Slot] {
				continue
			}
			seen[summary.Slot] = true
			all = append(all, summary)
		}
		f.Close()
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Slot > all[j].Slot
	})

	s.mu.Lock()
	s.indices[sourceID] = all
	s.mu.Unlock()

	return nil
}

// Prune removes slot files and epoch index files older than the retention window,
// and trims the corresponding in-memory index entries.
func (s *Storage) Prune(retentionDays int, slotsPerEpoch, secondsPerSlot uint64) error {
	if retentionDays <= 0 {
		return nil
	}

	retentionSeconds := uint64(retentionDays) * 24 * 3600
	retentionSlots := retentionSeconds / secondsPerSlot
	now := uint64(time.Now().Unix())
	// Approximate current slot (genesis = 0 simplification; caller sets secondsPerSlot)
	currentSlot := now / secondsPerSlot
	var cutoffSlot uint64
	if currentSlot > retentionSlots {
		cutoffSlot = currentSlot - retentionSlots
	}
	cutoffEpoch := cutoffSlot / slotsPerEpoch

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	for _, sourceEntry := range entries {
		if !sourceEntry.IsDir() {
			continue
		}
		sourceID := sourceEntry.Name()

		// Prune slot files
		slotsDir := filepath.Join(s.dataDir, sourceID, "slots")
		slotEntries, err := os.ReadDir(slotsDir)
		if err == nil {
			for _, se := range slotEntries {
				name := se.Name()
				if !strings.HasSuffix(name, ".json") {
					continue
				}
				slotStr := strings.TrimSuffix(name, ".json")
				slot, err := strconv.ParseUint(slotStr, 10, 64)
				if err != nil {
					continue
				}
				if slot < cutoffSlot {
					os.Remove(filepath.Join(slotsDir, name))
				}
			}
		}

		// Prune epoch index files
		indexDir := filepath.Join(s.dataDir, sourceID, "index")
		indexEntries, err := os.ReadDir(indexDir)
		if err == nil {
			for _, ie := range indexEntries {
				name := ie.Name()
				if !strings.HasSuffix(name, ".jsonl") {
					continue
				}
				epochStr := strings.TrimSuffix(name, ".jsonl")
				epoch, err := strconv.ParseUint(epochStr, 10, 64)
				if err != nil {
					continue
				}
				if epoch < cutoffEpoch {
					os.Remove(filepath.Join(indexDir, name))
				}
			}
		}

		// Trim in-memory index
		s.mu.Lock()
		idx := s.indices[sourceID]
		if len(idx) > 0 {
			// Find first entry below cutoff (index is descending)
			cutPos := sort.Search(len(idx), func(i int) bool {
				return idx[i].Slot < cutoffSlot
			})
			if cutPos < len(idx) {
				s.indices[sourceID] = idx[:cutPos]
			}
		}
		s.mu.Unlock()
	}

	return nil
}
