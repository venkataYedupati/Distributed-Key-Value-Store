package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var ErrNotFound = errors.New("key not found")

type Entry struct {
	Key       string     `json:"key"`
	Value     string     `json:"value"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Deleted   bool       `json:"deleted"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type Engine struct {
	mu                 sync.RWMutex
	db                 *leveldb.DB
	dataDir            string
	snapshotDir        string
	snapshotThreshold  int
	writeCount         atomic.Uint64
}

func NewEngine(dataDir string, snapshotThreshold int) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	snapshotDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dataDir, "kv.db")
	db, err := leveldb.OpenFile(dbPath, &opt.Options{ErrorIfMissing: false})
	if err != nil {
		return nil, err
	}
	engine := &Engine{db: db, dataDir: dataDir, snapshotDir: snapshotDir, snapshotThreshold: snapshotThreshold}
	if err := engine.restoreLatestSnapshot(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return engine, nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

func (e *Engine) Set(key, value string, ttl time.Duration) error {
	return e.storeEntry(Entry{Key: key, Value: value, UpdatedAt: time.Now().UTC()}, ttl)
}

func (e *Engine) Delete(key string) error {
	entry := Entry{Key: key, Deleted: true, UpdatedAt: time.Now().UTC()}
	return e.putEntry(entry)
}

func (e *Engine) Get(key string) (Entry, error) {
	data, err := e.db.Get([]byte(key), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	var entry Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return Entry{}, err
	}
	if entry.Deleted {
		return Entry{}, ErrNotFound
	}
	if entry.ExpiresAt != nil && time.Now().UTC().After(*entry.ExpiresAt) {
		_ = e.db.Delete([]byte(key), nil)
		return Entry{}, ErrNotFound
	}
	return entry, nil
}

func (e *Engine) Keys() ([]string, error) {
	iter := e.db.NewIterator(nil, nil)
	defer iter.Release()
	keys := make([]string, 0)
	for iter.Next() {
		var entry Entry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			continue
		}
		if entry.Deleted {
			continue
		}
		if entry.ExpiresAt != nil && time.Now().UTC().After(*entry.ExpiresAt) {
			continue
		}
		keys = append(keys, string(iter.Key()))
	}
	return keys, iter.Error()
}

func (e *Engine) Stats() map[string]any {
	keys, _ := e.Keys()
	return map[string]any{
		"keys":     len(keys),
		"data_dir": e.dataDir,
		"writes":   e.writeCount.Load(),
	}
}

func (e *Engine) storeEntry(entry Entry, ttl time.Duration) error {
	if ttl > 0 {
		expiresAt := time.Now().UTC().Add(ttl)
		entry.ExpiresAt = &expiresAt
	}
	return e.putEntry(entry)
}

func (e *Engine) putEntry(entry Entry) error {
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err := e.db.Put([]byte(entry.Key), payload, nil); err != nil {
		return err
	}
	if e.snapshotThreshold > 0 && e.writeCount.Add(1)%uint64(e.snapshotThreshold) == 0 {
		if err := e.createSnapshot(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) ApplyBatch(entries []Entry) error {
	batch := new(leveldb.Batch)
	for _, entry := range entries {
		payload, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		batch.Put([]byte(entry.Key), payload)
	}
	if err := e.db.Write(batch, nil); err != nil {
		return err
	}
	if e.snapshotThreshold > 0 && e.writeCount.Add(uint64(len(entries)))%uint64(e.snapshotThreshold) == 0 {
		return e.createSnapshot()
	}
	return nil
}

func (e *Engine) storeFromRecord(record SnapshotRecord) Entry {
	entry := Entry{Key: record.Key, Value: record.Value, Deleted: record.Deleted, UpdatedAt: record.UpdatedAt}
	if record.ExpiresAt != nil {
		expires := record.ExpiresAt.UTC()
		entry.ExpiresAt = &expires
	}
	return entry
}

func (e *Engine) restoreEntry(entry Entry) error {
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return e.db.Put([]byte(entry.Key), payload, nil)
}

func (e *Engine) ApplyRecords(records []SnapshotRecord) error {
	for _, record := range records {
		if err := e.restoreEntry(e.storeFromRecord(record)); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) maybeCleanupExpired(key string, entry Entry) {
	if entry.ExpiresAt != nil && time.Now().UTC().After(*entry.ExpiresAt) {
		_ = e.db.Delete([]byte(key), nil)
	}
}

func (e *Engine) Dump() ([]SnapshotRecord, error) {
	iter := e.db.NewIterator(nil, nil)
	defer iter.Release()
	records := make([]SnapshotRecord, 0)
	for iter.Next() {
		var entry Entry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			continue
		}
		if entry.Deleted {
			continue
		}
		if entry.ExpiresAt != nil && time.Now().UTC().After(*entry.ExpiresAt) {
			continue
		}
		records = append(records, SnapshotRecord{Key: entry.Key, Value: entry.Value, ExpiresAt: entry.ExpiresAt, Deleted: entry.Deleted, UpdatedAt: entry.UpdatedAt})
	}
	return records, iter.Error()
}

func (e *Engine) DebugString() string {
	return fmt.Sprintf("engine[dataDir=%s]", e.dataDir)
}
