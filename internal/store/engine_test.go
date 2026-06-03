package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEngineTTLExpires(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewEngine(filepath.Join(dir, "store"), 10)
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.Set("k", "v", 20*time.Millisecond))
	time.Sleep(40 * time.Millisecond)
	_, err = engine.Get("k")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestEngineSetGetDelete(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewEngine(filepath.Join(dir, "store"), 10)
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.Set("k", "v", 0))
	entry, err := engine.Get("k")
	require.NoError(t, err)
	require.Equal(t, "v", entry.Value)
	require.NoError(t, engine.Delete("k"))
	_, err = engine.Get("k")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestSnapshotRestoresRaftPosition(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewEngine(filepath.Join(dir, "source"), 10)
	require.NoError(t, err)
	require.NoError(t, engine.Set("k", "v", 0))

	data, err := engine.TakeSnapshot(42, 7)
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	restored, err := NewEngine(filepath.Join(dir, "restored"), 10)
	require.NoError(t, err)
	defer restored.Close()

	require.NoError(t, restored.ApplySnapshot(data, 0, 0))
	index, term := restored.SnapshotPosition()
	require.Equal(t, int64(42), index)
	require.Equal(t, int64(7), term)

	entry, err := restored.Get("k")
	require.NoError(t, err)
	require.Equal(t, "v", entry.Value)
}
