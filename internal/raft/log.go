package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LogEntry struct {
	Index int64  `json:"index"`
	Term  int64  `json:"term"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
	TTL   int64  `json:"ttl"`
}

type LogStore struct {
	mu        sync.RWMutex
	db        *leveldb.DB
	dataDir   string
	lastIndex int64
	lastTerm  int64
	count     int64
}

const (
	metaTermKey    = "meta:term"
	metaVotedForKey = "meta:voted_for"
	logPrefix      = "log:"
)

func NewLogStore(dataDir string) (*LogStore, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	db, err := leveldb.OpenFile(filepath.Join(dataDir, "raft-log.db"), &opt.Options{ErrorIfMissing: false})
	if err != nil {
		return nil, err
	}
	store := &LogStore{db: db, dataDir: dataDir}
	if err := store.reindex(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *LogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *LogStore) Append(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err := s.db.Put([]byte(logKey(entry.Index)), payload, nil); err != nil {
		return err
	}
	if entry.Index > s.lastIndex {
		s.lastIndex = entry.Index
		s.lastTerm = entry.Term
	}
	s.count++
	return nil
}

func (s *LogStore) Get(index int64) (LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.db.Get([]byte(logKey(index)), nil)
	if err != nil {
		return LogEntry{}, err
	}
	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return LogEntry{}, err
	}
	return entry, nil
}

func (s *LogStore) GetRange(from, to int64) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if to < from {
		return []LogEntry{}, nil
	}
	iter := s.db.NewIterator(util.BytesPrefix([]byte(logPrefix)), nil)
	defer iter.Release()
	entries := make([]LogEntry, 0, to-from+1)
	for iter.Seek([]byte(logKey(from))); iter.Valid(); iter.Next() {
		index := indexFromKey(string(iter.Key()))
		if index > to {
			break
		}
		var entry LogEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, iter.Error()
}

func (s *LogStore) DeleteFrom(index int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	iter := s.db.NewIterator(util.BytesPrefix([]byte(logPrefix)), nil)
	defer iter.Release()
	for iter.Seek([]byte(logKey(index))); iter.Valid(); iter.Next() {
		if err := s.db.Delete(append([]byte(nil), iter.Key()...), nil); err != nil {
			return err
		}
	}
	return s.reindexLocked()
}

func (s *LogStore) DeleteThrough(index int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	iter := s.db.NewIterator(util.BytesPrefix([]byte(logPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		entryIndex := indexFromKey(string(iter.Key()))
		if entryIndex > index {
			continue
		}
		if err := s.db.Delete(append([]byte(nil), iter.Key()...), nil); err != nil {
			return err
		}
	}
	return s.reindexLocked()
}

func (s *LogStore) LastIndex() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex
}

func (s *LogStore) LastTerm() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastTerm
}

func (s *LogStore) Count() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

func (s *LogStore) SearchLastTerm(term int64) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	iter := s.db.NewIterator(util.BytesPrefix([]byte(logPrefix)), nil)
	defer iter.Release()
	var found int64 = -1
	for iter.Next() {
		var entry LogEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			continue
		}
		if entry.Term == term {
			found = entry.Index
		}
	}
	return found, found >= 0
}

func (s *LogStore) SaveTerm(term int64) error {
	return s.db.Put([]byte(metaTermKey), []byte(strconv.FormatInt(term, 10)), nil)
}

func (s *LogStore) SaveVotedFor(votedFor string) error {
	return s.db.Put([]byte(metaVotedForKey), []byte(votedFor), nil)
}

func (s *LogStore) LoadTerm() (int64, error) {
	data, err := s.db.Get([]byte(metaTermKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	term, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, err
	}
	return term, nil
}

func (s *LogStore) LoadVotedFor() (string, error) {
	data, err := s.db.Get([]byte(metaVotedForKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

func (s *LogStore) reindex() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reindexLocked()
}

func (s *LogStore) reindexLocked() error {
	iter := s.db.NewIterator(util.BytesPrefix([]byte(logPrefix)), nil)
	defer iter.Release()
	entries := make([]LogEntry, 0)
	for iter.Next() {
		var entry LogEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Index < entries[j].Index })
	s.lastIndex = 0
	s.lastTerm = 0
	s.count = int64(len(entries))
	if len(entries) > 0 {
		s.lastIndex = entries[len(entries)-1].Index
		s.lastTerm = entries[len(entries)-1].Term
	}
	return iter.Error()
}

func logKey(index int64) string {
	return fmt.Sprintf("log:%020d", index)
}

func indexFromKey(key string) int64 {
	parts := strings.Split(key, ":")
	if len(parts) == 0 {
		return 0
	}
	value, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return value
}
