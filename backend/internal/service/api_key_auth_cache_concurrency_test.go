package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

/*
 * A warm cache must enforce the same cap as a fresh database lookup.
 */
func TestAPIKeyAuthSnapshotConcurrencyLimit(t *testing.T) {
	svc := &APIKeyService{}
	key := &APIKey{ID: 1, UserID: 2, Key: "test-key", Status: StatusActive, ConcurrencyLimit: 3, User: &User{ID: 2, Status: StatusActive}}
	payload, err := json.Marshal(&APIKeyAuthCacheEntry{Snapshot: svc.snapshotFromAPIKey(context.Background(), key)})
	require.NoError(t, err)
	var cached APIKeyAuthCacheEntry
	require.NoError(t, json.Unmarshal(payload, &cached))
	restored, used, err := svc.applyAuthCacheEntry(key.Key, &cached)
	require.NoError(t, err)
	require.True(t, used)
	require.Equal(t, 3, restored.ConcurrencyLimit)
	cached.Snapshot.Version = 25
	_, used, err = svc.applyAuthCacheEntry(key.Key, &cached)
	require.NoError(t, err)
	require.False(t, used, "pre-limit snapshots must not silently remove protection")
}
