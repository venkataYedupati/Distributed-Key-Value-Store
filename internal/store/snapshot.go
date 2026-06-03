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
	CreatedAt         time.Time             `json:"created_at"`
	LastIncludedIndex int64                 `json:"last_included_index"`
	LastIncludedTerm  int64                 `json:"last_included_term"`
	Entries           map[string]StoreEntry `json:"entries"`
	Records           []SnapshotRecord      `json:"records,omitempty"`
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
	snapshot := SnapshotFile{
		CreatedAt:         time.Now().UTC(),
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Entries:           entries,
	}
	payload, err := json.Marshal(snapshot)
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
	entries, snapshotIndex, snapshotTerm, err := decodeSnapshot(decoded)
	if err != nil {
		return err
	}
	if lastIncludedIndex == 0 {
		lastIncludedIndex = snapshotIndex
	}
	if lastIncludedTerm == 0 {
		lastIncludedTerm = snapshotTerm
	}

	batch := new(leveldb.Batch)
	e.mu.Lock()
	defer e.mu.Unlock()

	iter := e.db.NewIterator(nil, nil)
	for iter.Next() {
		batch.Delete(append([]byte(nil), iter.Key()...))
	}
	if err := iter.Error(); err != nil {
		iter.Release()
		return err
	}
	iter.Release()

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
	e.snapshotIndex = lastIncludedIndex
	e.snapshotTerm = lastIncludedTerm
	return nil
}

func decodeSnapshot(decoded []byte) (map[string]StoreEntry, int64, int64, error) {
	var snapshot SnapshotFile
	if err := json.Unmarshal(decoded, &snapshot); err == nil && snapshot.Entries != nil {
		return snapshot.Entries, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, nil
	}

	legacy := make(map[string]StoreEntry)
	if err := json.Unmarshal(decoded, &legacy); err != nil {
		return nil, 0, 0, err
	}
	return legacy, 0, 0, nil
}

func (e *Engine) createSnapshot() error {
	snapshotIndex, snapshotTerm := e.SnapshotPosition()
	data, err := e.TakeSnapshot(snapshotIndex, snapshotTerm)
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
