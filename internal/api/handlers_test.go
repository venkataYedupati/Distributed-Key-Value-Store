package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTTLAcceptsDurationString(t *testing.T) {
	raw, err := json.Marshal("30s")
	require.NoError(t, err)

	ttl, err := parseTTL(raw)
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, ttl)
}

func TestParseTTLAcceptsNumericSeconds(t *testing.T) {
	raw, err := json.Marshal(60)
	require.NoError(t, err)

	ttl, err := parseTTL(raw)
	require.NoError(t, err)
	require.Equal(t, time.Minute, ttl)
}
