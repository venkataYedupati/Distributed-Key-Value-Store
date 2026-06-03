package raft

import (
	"fmt"
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/hash"
	"distributed-kv-store/internal/store"
	"github.com/stretchr/testify/require"
)

func TestApplyEntryOnlyStoresReplicaKeys(t *testing.T) {
	ring := testRing()
	key := keyExcludedFromReplicaSet(t, ring, "node1")

	dir := t.TempDir()
	engine, err := store.NewEngine(filepath.Join(dir, "node1"), 10)
	require.NoError(t, err)
	defer engine.Close()

	node := NewNode(config.Config{NodeID: "node1", DataDir: filepath.Join(dir, "raft")}, engine, ring, nil)
	defer node.Close()

	require.NoError(t, node.applyEntry(LogEntry{Op: "set", Key: key, Value: "value"}))
	_, err = engine.Get(key)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestApplyEntryStoresLocalReplicaKeys(t *testing.T) {
	ring := testRing()
	key := keyIncludedInReplicaSet(t, ring, "node1")

	dir := t.TempDir()
	engine, err := store.NewEngine(filepath.Join(dir, "node1"), 10)
	require.NoError(t, err)
	defer engine.Close()

	node := NewNode(config.Config{NodeID: "node1", DataDir: filepath.Join(dir, "raft")}, engine, ring, nil)
	defer node.Close()

	require.NoError(t, node.applyEntry(LogEntry{Op: "set", Key: key, Value: "value"}))
	entry, err := engine.Get(key)
	require.NoError(t, err)
	require.Equal(t, "value", entry.Value)
}

func testRing() *hash.Ring {
	ring := hash.NewRing(25, 3)
	for _, id := range []string{"node1", "node2", "node3", "node4", "node5"} {
		ring.AddNode(hash.PhysicalNode{ID: id})
	}
	return ring
}

func keyExcludedFromReplicaSet(t *testing.T, ring *hash.Ring, nodeID string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("excluded-%d", i)
		if !replicaSetContains(ring.GetReplicaSet(key), nodeID) {
			return key
		}
	}
	t.Fatalf("could not find key excluded from replica set")
	return ""
}

func keyIncludedInReplicaSet(t *testing.T, ring *hash.Ring, nodeID string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("included-%d", i)
		if replicaSetContains(ring.GetReplicaSet(key), nodeID) {
			return key
		}
	}
	t.Fatalf("could not find key included in replica set")
	return ""
}

func replicaSetContains(replicas []hash.PhysicalNode, nodeID string) bool {
	for _, replica := range replicas {
		if replica.ID == nodeID {
			return true
		}
	}
	return false
}
