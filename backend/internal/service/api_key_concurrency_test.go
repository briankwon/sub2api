//go:build unit

package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type limitedKeyCache struct {
	refreshLost bool
	refreshErr  error
	stubConcurrencyCacheForTest
	keyAllowed  bool
	keyErr      error
	keyReleased atomic.Int32
}

func (c *limitedKeyCache) AcquireAPIKeySlot(context.Context, int64, int, string) (bool, error) {
	return c.keyAllowed, c.keyErr
}
func (c *limitedKeyCache) RefreshAPIKeySlot(context.Context, int64, string) (bool, error) {
	return !c.refreshLost, c.refreshErr
}
func (c *limitedKeyCache) ReleaseAPIKeySlot(context.Context, int64, string) error {
	c.keyReleased.Add(1)
	return nil
}

/*
 * Missing or zero policy preserves standard-mode behavior even with key caps stored.
 */
func TestAPIKeyConcurrencyPolicyCompatibility(t *testing.T) {
	for _, limit := range []int{0, 2} {
		cache := &limitedKeyCache{keyAllowed: false}
		svc := NewConcurrencyService(cache)
		ctx := context.Background()
		if limit == 0 {
			ctx = WithAPIKeyConcurrencyLimit(ctx, 1, 0)
		}
		result, err := svc.AcquireUserSlot(ctx, 1, 0)
		require.NoError(t, err)
		require.True(t, result.Acquired)
		result.ReleaseFunc()
		require.Zero(t, cache.keyReleased.Load())
	}
}

/*
 * Key saturation and cache failure must reject before any upstream work.
 */
func TestAPIKeyConcurrencyRejectsAtAdmission(t *testing.T) {
	ctx := WithAPIKeyConcurrencyLimit(context.Background(), 1, 1)
	cache := &limitedKeyCache{}
	svc := NewConcurrencyService(cache)
	_, err := svc.AcquireUserSlot(ctx, 1, 0)
	require.ErrorIs(t, err, ErrAPIKeyConcurrencyLimitExceeded)
	cache.keyErr = errors.New("redis unavailable")
	_, err = svc.AcquireUserSlot(ctx, 1, 0)
	require.ErrorContains(t, err, "redis unavailable")
}

/*
 * Extra keys cannot bypass the existing user cap, and failed admission releases key capacity.
 */
func TestAPIKeyConcurrencyPreservesUserLimit(t *testing.T) {
	cache := &limitedKeyCache{keyAllowed: true}
	svc := NewConcurrencyService(cache)
	ctx := WithAPIKeyConcurrencyLimit(context.Background(), 1, 3)
	result, err := svc.AcquireUserSlot(ctx, 2, 1)
	require.NoError(t, err)
	require.False(t, result.Acquired)
	require.EqualValues(t, 1, cache.keyReleased.Load())
	cache.acquireErr = errors.New("user cache unavailable")
	_, err = svc.AcquireUserSlot(ctx, 2, 1)
	require.Error(t, err)
	require.EqualValues(t, 2, cache.keyReleased.Load())
}

/*
 * Cancellation and normal cleanup may race; both must release exactly once.
 */
func TestAPIKeyConcurrencyCancellationAndNoDoubleTracking(t *testing.T) {
	cache := &limitedKeyCache{keyAllowed: true}
	svc := NewConcurrencyService(cache)
	base, cancel := context.WithCancel(context.Background())
	ctx := WithAPIKeyConcurrencyLimit(base, 1, 1)
	result, err := svc.AcquireUserSlot(ctx, 1, 0)
	require.NoError(t, err)
	require.True(t, result.Acquired)
	svc.TrackAPIKeySlot(ctx, 1)()
	require.Empty(t, cache.trackedAPIKeyIDs)
	cancel()
	require.Eventually(t, func() bool { return cache.keyReleased.Load() == 1 }, time.Second, time.Millisecond)
	result.ReleaseFunc()
	result.ReleaseFunc()
	require.EqualValues(t, 1, cache.keyReleased.Load())
}

/*
 * Leaving the user wait queue must not release an accepted request's key slot.
 */
func TestAPIKeyConcurrencyAdmissionTimeoutDoesNotEndRequest(t *testing.T) {
	cache := &limitedKeyCache{keyAllowed: true}
	svc := NewConcurrencyService(cache)
	requestCtx := WithAPIKeyConcurrencyLimit(context.Background(), 1, 1)
	attemptCtx, cancelAttempt := context.WithCancel(requestCtx)
	result, err := svc.AcquireUserSlot(attemptCtx, 7, 0)
	require.NoError(t, err)
	require.True(t, result.Acquired)
	cancelAttempt()
	require.Never(t, func() bool { return cache.keyReleased.Load() > 0 }, 20*time.Millisecond, time.Millisecond)
	result.ReleaseFunc()
	require.EqualValues(t, 1, cache.keyReleased.Load())
}

/*
 * A lost renewal must stop upstream work rather than admit unprotected requests.
 */
func TestAPIKeyConcurrencyLostLeaseCancelsRequest(t *testing.T) {
	for _, redisFailure := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			cache := &limitedKeyCache{keyAllowed: true, refreshLost: !redisFailure}
			if redisFailure {
				cache.refreshErr = errors.New("redis offline")
			}
			svc := NewConcurrencyService(cache)
			ctx := WithAPIKeyConcurrencyLimit(context.Background(), 1, 1)
			result, err := svc.AcquireUserSlot(ctx, 7, 0)
			require.NoError(t, err)
			time.Sleep(apiKeySlotRefreshInterval + time.Second)
			synctest.Wait()
			require.Error(t, ctx.Err())
			require.ErrorContains(t, context.Cause(ctx), "lease lost")
			require.EqualValues(t, 1, cache.keyReleased.Load())
			result.ReleaseFunc()
			require.EqualValues(t, 1, cache.keyReleased.Load())
		})
	}
}

type concurrencyEditRepo struct {
	quotaBaseAPIKeyRepoStub
	key    *APIKey
	fields APIKeyUpdateFields
}

func (r *concurrencyEditRepo) GetByID(context.Context, int64) (*APIKey, error) {
	clone := *r.key
	return &clone, nil
}
func (r *concurrencyEditRepo) Update(_ context.Context, key *APIKey, fields APIKeyUpdateFields) error {
	r.key = key
	r.fields = fields
	return nil
}

/*
 * Editing only the cap must invalidate auth caches without overwriting live usage.
 */
func TestAPIKeyConcurrencyEditPreservesUsageAndInvalidatesAuth(t *testing.T) {
	repo := &concurrencyEditRepo{key: &APIKey{ID: 1, UserID: 7, Key: "edit-key", Status: StatusActive, ConcurrencyLimit: 5, QuotaUsed: 8, Usage1d: 4}}
	cache := &quotaStateCacheStub{}
	svc := &APIKeyService{apiKeyRepo: repo, cache: cache}
	limit := 2
	updated, err := svc.Update(context.Background(), 1, 7, UpdateAPIKeyRequest{ConcurrencyLimit: &limit})
	require.NoError(t, err)
	require.Equal(t, 2, updated.ConcurrencyLimit)
	require.Equal(t, APIKeyUpdateFields{ConcurrencyLimit: true}, repo.fields)
	require.EqualValues(t, 8, updated.QuotaUsed)
	require.EqualValues(t, 4, updated.Usage1d)
	require.Contains(t, cache.deleteAuthKeys, svc.authCacheKey("edit-key"))
	limit = 0
	updated, err = svc.Update(context.Background(), 1, 7, UpdateAPIKeyRequest{ConcurrencyLimit: &limit})
	require.NoError(t, err)
	require.Zero(t, updated.ConcurrencyLimit)
}

type uncertainKeyAdmissionCache struct {
	limitedKeyCache
	cancel          context.CancelFunc
	acquiredID      string
	reclaimedID     string
	deadlinePresent bool
	cleanupErr      error
}

func (c *uncertainKeyAdmissionCache) AcquireAPIKeySlot(ctx context.Context, _ int64, _ int, id string) (bool, error) {
	c.acquiredID = id
	_, c.deadlinePresent = ctx.Deadline()
	c.cancel()
	return false, context.DeadlineExceeded
}
func (c *uncertainKeyAdmissionCache) ReleaseAPIKeySlot(ctx context.Context, _ int64, id string) error {
	c.reclaimedID = id
	c.cleanupErr = ctx.Err()
	return nil
}

/*
 * An uncertain admission may already own capacity. Reclaim that exact member
 * using an independent live context, even when the request has been canceled.
 */
func TestAPIKeyConcurrencyUncertainAdmissionReclaimsOwnSlot(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := &uncertainKeyAdmissionCache{cancel: cancel}
	svc := NewConcurrencyService(cache)
	ctx := WithAPIKeyConcurrencyLimit(parent, 1, 1)
	_, err := svc.AcquireUserSlot(ctx, 7, 3)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, cache.deadlinePresent)
	require.NotEmpty(t, cache.acquiredID)
	require.Equal(t, cache.acquiredID, cache.reclaimedID)
	require.NoError(t, cache.cleanupErr, "canceled request contexts must not prevent cleanup")
}
