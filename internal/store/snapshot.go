package store

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type StoreEntry struct {
	Value     string `json:"value"`
	ExpiresAt int64  `json:"expires_at"`
}

type SnapshotRecord struct {
	Key       string     `json:"key"`
	Value     string     `json:"value"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Deleted   bool       `json:"deleted"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type SnapshotFile struct {
	CreatedAt time.Time        `json:"created_at"`
	Records   []SnapshotRecord `json:"records"`
}

func (e *Engine) TakeSnapshot(lastIncludedIndex int64, lastIncludedTerm int64) ([]byte, error) {
	entries := make(map[string]StoreEntry)
	iter := e.db.NewIterator(nil, nil)
	defer iter.Release()
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
		expiresAt := int64(0)
		if entry.ExpiresAt != nil {
			expiresAt = entry.ExpiresAt.UnixNano()
		}
		entries[string(iter.Key())] = StoreEntry{Value: entry.Value, ExpiresAt: expiresAt}
	}
	_ = lastIncludedIndex
	_ = lastIncludedTerm
	payload, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(payload); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *Engine) ApplySnapshot(data []byte, lastIncludedIndex int64, lastIncludedTerm int64) error {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer reader.Close()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	entries := make(map[string]StoreEntry)
	if err := json.Unmarshal(decoded, &entries); err != nil {
		return err
	}
	keys, err := e.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := e.db.Delete([]byte(key), nil); err != nil {
			return err
		}
	}
	batch := new(leveldb.Batch)
	for key, entry := range entries {
		state := Entry{Key: key, Value: entry.Value, UpdatedAt: time.Now().UTC()}
		if entry.ExpiresAt > 0 {
			expires := time.Unix(0, entry.ExpiresAt).UTC()
			state.ExpiresAt = &expires
		}
		payload, err := json.Marshal(state)
		if err != nil {
			return err
		}
		batch.Put([]byte(key), payload)
	}
	if err := e.db.Write(batch, nil); err != nil {
		return err
	}
	_ = lastIncludedIndex
	_ = lastIncludedTerm
	return nil
}

func (e *Engine) createSnapshot() error {
	data, err := e.TakeSnapshot(0, 0)
	if err != nil {
		return err
	}
	fileName := filepath.Join(e.snapshotDir, fmt.Sprintf("snapshot-%d.json.gz", time.Now().UTC().UnixNano()))
	return os.WriteFile(fileName, data, 0o644)
}

func (e *Engine) restoreLatestSnapshot() error {
	entries, err := os.ReadDir(e.snapshotDir)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	latest := entries[len(entries)-1]
	path := filepath.Join(e.snapshotDir, latest.Name())
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return e.ApplySnapshot(data, 0, 0)
}
