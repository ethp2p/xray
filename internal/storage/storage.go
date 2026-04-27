package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethp2p/xray/api"
	"github.com/ethp2p/xray/internal/sources"
	_ "github.com/mattn/go-sqlite3"
)

var ErrSlotNotFound = errors.New("slot not found")

// Re-export public DTOs locally so the storage code reads naturally.
type (
	SlotSummary     = api.SlotSummary
	SlotDetail      = api.SlotDetail
	SlotBreakdown   = api.SlotBreakdown
	SlotBucketPoint = api.SlotBucketPoint
	SourceInfo      = sources.SourceInfo
)

type Storage struct {
	dataDir string
	db      *sql.DB
}

func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "xray.db")
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL", dbPath))
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	s := &Storage{dataDir: dataDir, db: db}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.migrateLegacyFiles(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) initSchema() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS sources (
			source_id TEXT PRIMARY KEY,
			info JSONB NOT NULL
		);
		CREATE TABLE IF NOT EXISTS slots (
			source_id TEXT NOT NULL,
			slot INTEGER NOT NULL,
			summary JSONB NOT NULL,
			detail JSONB NOT NULL,
			PRIMARY KEY (source_id, slot)
		);
		CREATE INDEX IF NOT EXISTS slots_source_slot_desc
			ON slots (source_id, slot DESC);
	`)
	if err != nil {
		return fmt.Errorf("initialize sqlite schema: %w", err)
	}
	return nil
}

func (s *Storage) migrateLegacyFiles() error {
	markerPath := filepath.Join(s.dataDir, ".legacy-json-migrated")
	if _, err := os.Stat(markerPath); err == nil {
		return nil
	}

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return fmt.Errorf("read data dir for legacy migration: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "legacy-failed" {
			continue
		}
		sourceID := entry.Name()
		if err := s.migrateLegacySource(sourceID); err != nil {
			return err
		}
	}
	if err := os.WriteFile(markerPath, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0644); err != nil {
		return fmt.Errorf("write legacy migration marker: %w", err)
	}
	return nil
}

func (s *Storage) migrateLegacySource(sourceID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin legacy migration for %s: %w", sourceID, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	sourcePath := filepath.Join(s.dataDir, sourceID, "source.json")
	if data, err := os.ReadFile(sourcePath); err == nil {
		var info SourceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			if err := s.quarantineLegacyFile(sourceID, sourcePath); err != nil {
				return err
			}
		} else if err := writeSourceMeta(tx, info); err != nil {
			return fmt.Errorf("migrate source %s: %w", sourceID, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read legacy source %s: %w", sourcePath, err)
	}

	slotsDir := filepath.Join(s.dataDir, sourceID, "slots")
	entries, err := os.ReadDir(slotsDir)
	if err != nil {
		if os.IsNotExist(err) {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit legacy migration for %s: %w", sourceID, err)
			}
			committed = true
			if err := s.removeLegacySourceFiles(sourceID); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("read legacy slots for %s: %w", sourceID, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(slotsDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read legacy slot %s: %w", path, err)
		}
		var detail SlotDetail
		if err := json.Unmarshal(data, &detail); err != nil {
			if err := s.quarantineLegacyFile(sourceID, path); err != nil {
				return err
			}
			continue
		}
		if err := writeSlot(tx, sourceID, detail); err != nil {
			return fmt.Errorf("migrate slot %s/%s: %w", sourceID, entry.Name(), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit legacy migration for %s: %w", sourceID, err)
	}
	committed = true
	if err := s.removeLegacySourceFiles(sourceID); err != nil {
		return err
	}
	return nil
}

func (s *Storage) quarantineLegacyFile(sourceID string, path string) error {
	rel, err := filepath.Rel(filepath.Join(s.dataDir, sourceID), path)
	if err != nil {
		return fmt.Errorf("resolve legacy quarantine path %s: %w", path, err)
	}
	dst := filepath.Join(s.dataDir, "legacy-failed", sourceID, rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("create legacy quarantine dir: %w", err)
	}
	if err := os.Rename(path, dst); err != nil {
		return fmt.Errorf("quarantine legacy file %s: %w", path, err)
	}
	return nil
}

func (s *Storage) removeLegacySourceFiles(sourceID string) error {
	paths := []string{
		filepath.Join(s.dataDir, sourceID, "source.json"),
		filepath.Join(s.dataDir, sourceID, "slots"),
		filepath.Join(s.dataDir, sourceID, "index"),
	}
	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove legacy path %s: %w", path, err)
		}
	}
	return nil
}

// WriteSlot persists a finalized slot to SQLite. Slot detail and summary are
// stored as SQLite JSONB, with scalar columns kept for indexed lookup.
func (s *Storage) WriteSlot(sourceID string, detail SlotDetail) error {
	return writeSlot(s.db, sourceID, detail)
}

type sqlExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func writeSlot(exec sqlExecutor, sourceID string, detail SlotDetail) error {
	slot, err := int64Slot(detail.Summary.Slot)
	if err != nil {
		return err
	}
	summaryJSON, err := json.Marshal(detail.Summary)
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal slot detail: %w", err)
	}
	_, err = exec.Exec(`
		INSERT OR IGNORE INTO slots (source_id, slot, summary, detail)
		VALUES (?, ?, jsonb(?), jsonb(?))
	`, sourceID, slot, string(summaryJSON), string(detailJSON))
	if err != nil {
		return fmt.Errorf("write slot %d: %w", detail.Summary.Slot, err)
	}
	return nil
}

func int64Slot(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("slot value %d exceeds sqlite integer range", value)
	}
	return int64(value), nil
}

func (s *Storage) ReadSlot(sourceID string, slot uint64) ([]byte, error) {
	slotValue, err := int64Slot(slot)
	if err != nil {
		return nil, err
	}
	var data []byte
	err = s.db.QueryRow(`
		SELECT json(detail)
		FROM slots
		WHERE source_id = ? AND slot = ?
	`, sourceID, slotValue).Scan(&data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSlotNotFound
		}
		return nil, fmt.Errorf("read slot %d: %w", slot, err)
	}
	return data, nil
}

func (s *Storage) ListSummaries(sourceID string, limit int) ([]SlotSummary, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.Query(`
		SELECT json(summary)
		FROM slots
		WHERE source_id = ?
		ORDER BY slot DESC
		LIMIT ?
	`, sourceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list summaries for %s: %w", sourceID, err)
	}
	defer rows.Close()

	return scanSummaries(rows)
}

// SearchSlots returns summaries in [fromSlot, toSlot] sorted descending, up to limit.
func (s *Storage) SearchSlots(sourceID string, fromSlot, toSlot uint64, limit int) ([]SlotSummary, error) {
	if limit <= 0 {
		return nil, nil
	}
	fromValue, err := int64Slot(fromSlot)
	if err != nil {
		return nil, err
	}
	toValue, err := int64Slot(toSlot)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`
		SELECT json(summary)
		FROM slots
		WHERE source_id = ? AND slot >= ? AND slot <= ?
		ORDER BY slot DESC
		LIMIT ?
	`, sourceID, fromValue, toValue, limit)
	if err != nil {
		return nil, fmt.Errorf("search slots for %s: %w", sourceID, err)
	}
	defer rows.Close()

	return scanSummaries(rows)
}

func scanSummaries(rows *sql.Rows) ([]SlotSummary, error) {
	var out []SlotSummary
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan summary: %w", err)
		}
		var summary SlotSummary
		if err := json.Unmarshal(data, &summary); err != nil {
			return nil, fmt.Errorf("decode summary: %w", err)
		}
		out = append(out, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate summaries: %w", err)
	}
	return out, nil
}

func (s *Storage) WriteSourceMeta(info SourceInfo) error {
	return writeSourceMeta(s.db, info)
}

func writeSourceMeta(exec sqlExecutor, info SourceInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal source info: %w", err)
	}
	_, err = exec.Exec(`
		INSERT INTO sources (source_id, info)
		VALUES (?, jsonb(?))
		ON CONFLICT(source_id) DO UPDATE SET info = excluded.info
	`, info.SourceID, string(data))
	if err != nil {
		return fmt.Errorf("write source meta: %w", err)
	}
	return nil
}

func (s *Storage) LoadSourceMetas() ([]SourceInfo, error) {
	rows, err := s.db.Query(`
		SELECT json(info)
		FROM sources
		ORDER BY source_id
	`)
	if err != nil {
		return nil, fmt.Errorf("load source metas: %w", err)
	}
	defer rows.Close()

	var metas []SourceInfo
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan source meta: %w", err)
		}
		var info SourceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("decode source meta: %w", err)
		}
		metas = append(metas, info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source metas: %w", err)
	}
	return metas, nil
}

// Prune removes slots older than the retention window.
func (s *Storage) Prune(retentionDays int, slotsPerEpoch, secondsPerSlot uint64, genesisUnix int64) error {
	if retentionDays <= 0 {
		return nil
	}

	retentionSeconds := uint64(retentionDays) * 24 * 3600
	retentionSlots := retentionSeconds / secondsPerSlot
	elapsed := uint64(time.Now().Unix()) - uint64(genesisUnix)
	currentSlot := elapsed / secondsPerSlot
	var cutoffSlot uint64
	if currentSlot > retentionSlots {
		cutoffSlot = currentSlot - retentionSlots
	}
	cutoffValue, err := int64Slot(cutoffSlot)
	if err != nil {
		return err
	}
	for {
		result, err := s.db.Exec(`
			DELETE FROM slots
			WHERE rowid IN (
				SELECT rowid
				FROM slots
				WHERE slot < ?
				LIMIT 1000
			)
		`, cutoffValue)
		if err != nil {
			return fmt.Errorf("prune slots: %w", err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read pruned row count: %w", err)
		}
		if deleted == 0 {
			return nil
		}
	}
}
