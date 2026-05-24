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
