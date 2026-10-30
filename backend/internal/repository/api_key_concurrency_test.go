package repository

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

/*
 * Concurrent clients must share one hard cap, independent of proxy instances.
 */
func TestAPIKeyConcurrencyAtomicAdmission(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	caches := []*concurrencyCache{
		NewConcurrencyCache(client, 15, 900).(*concurrencyCache),
		NewConcurrencyCache(client, 15, 900).(*concurrencyCache),
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := caches[i%2].AcquireAPIKeySlot(context.Background(), 1, 3, fmt.Sprint(i))
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				accepted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, 3, accepted.Load())
	counts, err := caches[0].GetAPIKeyConcurrencyBatch(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, 3, counts[1])
	ok, err := caches[1].AcquireAPIKeySlot(context.Background(), 2, 1, "other-key")
	require.NoError(t, err)
	require.True(t, ok, "another key must keep its own capacity")
}

/*
 * Released, refreshed and crashed requests must not permanently consume capacity.
 */
func TestAPIKeyConcurrencyLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewConcurrencyCache(client, 15, 900).(*concurrencyCache)
	ctx := context.Background()
	ok, err := cache.AcquireAPIKeySlot(ctx, 1, 1, "first")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, cache.ReleaseAPIKeySlot(ctx, 1, "first"))
	ok, err = cache.AcquireAPIKeySlot(ctx, 1, 1, "second")
	require.NoError(t, err)
	require.True(t, ok)
	server.FastForward(10 * time.Minute)
	ok, err = cache.RefreshAPIKeySlot(ctx, 1, "second")
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, server.TTL(apiKeySlotKey(1)), 14*time.Minute)
	server.FastForward(16 * time.Minute)
	ok, err = cache.AcquireAPIKeySlot(ctx, 1, 1, "after-crash")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.RefreshAPIKeySlot(ctx, 1, "second")
	require.NoError(t, err)
	require.False(t, ok, "expired owners must not resurrect slots")
}

/*
 * Live sessions and ordinary requests must consume the same key budget.
 */
func TestAPIKeyConcurrencyIncludesLiveSessions(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewConcurrencyCache(client, 15, 900).(*concurrencyCache)
	ctx := service.WithAPIKeyConcurrencyLimit(context.Background(), 3, 1)
	ok, err := cache.AcquireLiveLease(ctx, 1, 10, 2, 10, 3, "live", false)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.AcquireAPIKeySlot(ctx, 3, 1, "regular")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = cache.AcquireLiveLease(ctx, 1, 10, 2, 10, 3, "second-live", false)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, cache.ReleaseLiveLease(ctx, 1, 2, 3, "live"))
	ok, err = cache.AcquireAPIKeySlot(ctx, 3, 1, "regular")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cache.AcquireLiveLease(ctx, 1, 10, 2, 10, 3, "replacement", true)
	require.NoError(t, err)
	require.True(t, ok, "handoff must not double-count the admitted request")
}

/*
 * Issuing more keys must never enlarge a user's aggregate concurrency budget.
 */
func TestAPIKeyConcurrencyMultipleKeysShareUserCap(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewConcurrencyCache(client, 15, 900)
	svc := service.NewConcurrencyService(cache)
	ctxA := service.WithAPIKeyConcurrencyLimit(context.Background(), 1, 3)
	ctxB := service.WithAPIKeyConcurrencyLimit(context.Background(), 2, 3)
	first, err := svc.AcquireUserSlot(ctxA, 7, 1)
	require.NoError(t, err)
	require.True(t, first.Acquired)
	t.Cleanup(first.ReleaseFunc)
	second, err := svc.AcquireUserSlot(ctxB, 7, 1)
	require.NoError(t, err)
	require.False(t, second.Acquired)
	first.ReleaseFunc()
	second, err = svc.AcquireUserSlot(ctxB, 7, 1)
	require.NoError(t, err)
	require.True(t, second.Acquired)
	second.ReleaseFunc()
}
