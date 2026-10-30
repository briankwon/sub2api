package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

/*
 * ErrAPIKeyConcurrencyLimitExceeded identifies immediate key-level rejection.
 */
var ErrAPIKeyConcurrencyLimitExceeded = errors.New("API key concurrency limit exceeded")

/*
 * apiKeyConcurrencyPolicy is installed by simple-mode authentication only.
 */
type apiKeyConcurrencyPolicy struct {
	/*
	 * The key slot is already owned by the surrounding HTTP admission scope.
	 */
	reserved bool
	/*
	 * The request/session lifetime, independent of short admission deadlines.
	 */
	lifetime context.Context
	/*
	 * Cancels upstream work if distributed protection is lost.
	 */
	cancel context.CancelCauseFunc
	/*
	 * Authenticated API key identity; it never comes from a request payload.
	 */
	keyID int64
	/*
	 * Maximum active requests for this key, in addition to user/account caps.
	 */
	limit int
}
type apiKeyConcurrencyPolicyKey struct{}

/*
 * WithAPIKeyConcurrencyLimit binds a trusted simple-mode policy to a request.
 * Authentication calls this only in simple mode. Zero leaves normal tracking intact.
 */
func WithAPIKeyConcurrencyLimit(ctx context.Context, keyID int64, limit int) context.Context {
	if keyID <= 0 || limit < 0 {
		return ctx
	}
	/*
	 * A refreshed WS limit must retain the real connection cancellation owner.
	 * Zero deliberately replaces an old positive limit instead of inheriting it.
	 */
	if previous, ok := ctx.Value(apiKeyConcurrencyPolicyKey{}).(apiKeyConcurrencyPolicy); ok {
		previous.keyID = keyID
		previous.limit = limit
		previous.reserved = false
		return context.WithValue(ctx, apiKeyConcurrencyPolicyKey{}, previous)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	return context.WithValue(ctx, apiKeyConcurrencyPolicyKey{}, apiKeyConcurrencyPolicy{keyID: keyID, limit: limit, cancel: cancel, lifetime: ctx})
}

/*
 * APIKeyConcurrencyLimit returns a trusted request policy, or zero outside simple mode.
 */
func APIKeyConcurrencyLimit(ctx context.Context, keyID int64) int {
	if ctx == nil {
		return 0
	}
	policy, ok := ctx.Value(apiKeyConcurrencyPolicyKey{}).(apiKeyConcurrencyPolicy)
	if !ok || policy.keyID != keyID {
		return 0
	}
	return policy.limit
}

const apiKeySlotRefreshInterval = 30 * time.Second

type apiKeySlotLimiter interface {
	RefreshAPIKeySlot(context.Context, int64, string) (bool, error)
	AcquireAPIKeySlot(context.Context, int64, int, string) (bool, error)
	ReleaseAPIKeySlot(context.Context, int64, string) error
}

/*
 * AcquireUserSlot applies a Key cap to non-queued admissions, such as WS turns.
 * An HTTP scope reserves the Key once before entering the shared user queue.
 */
func (s *ConcurrencyService) AcquireUserSlot(ctx context.Context, userID int64, maxConcurrency int) (*AcquireResult, error) {
	policy, limited := ctx.Value(apiKeyConcurrencyPolicyKey{}).(apiKeyConcurrencyPolicy)
	if !limited || policy.limit <= 0 || policy.reserved {
		return s.acquireUserSlot(ctx, userID, maxConcurrency)
	}
	reservedCtx, releaseKey, err := s.ReserveAPIKeySlot(ctx)
	if err != nil {
		return nil, err
	}
	result, err := s.acquireUserSlot(reservedCtx, userID, maxConcurrency)
	if err != nil || !result.Acquired {
		releaseKey()
		return result, err
	}
	releaseUser := result.ReleaseFunc
	release := sync.OnceFunc(func() {
		releaseUser()
		releaseKey()
	})
	stop := context.AfterFunc(policy.lifetime, release)
	result.ReleaseFunc = func() {
		stop()
		release()
	}
	return result, nil
}

/*
 * ReserveAPIKeySlot holds one Key admission across user/account waiting and
 * forwarding. The returned context prevents repeated user attempts from taking
 * duplicate Key slots and is valid only for the lifetime of the returned lease.
 */
func (s *ConcurrencyService) ReserveAPIKeySlot(ctx context.Context) (context.Context, func(), error) {
	policy, limited := ctx.Value(apiKeyConcurrencyPolicyKey{}).(apiKeyConcurrencyPolicy)
	if !limited || policy.limit <= 0 || policy.reserved {
		return ctx, func() {}, nil
	}
	cache, ok := s.cache.(apiKeySlotLimiter)
	if !ok {
		return ctx, nil, errors.New("API key concurrency cache unavailable")
	}
	requestID := generateRequestID()
	acquireCtx, cancel := context.WithTimeout(ctx, apiKeySlotTrackTimeout)
	acquired, err := cache.AcquireAPIKeySlot(acquireCtx, policy.keyID, policy.limit, requestID)
	cancel()
	if err != nil {
		/*
		 * The script may have committed before its reply was lost. Reclaim only
		 * this request's member, even when its parent context is already canceled.
		 */
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), apiKeySlotTrackTimeout)
		if cleanupErr := cache.ReleaseAPIKeySlot(cleanupCtx, policy.keyID, requestID); cleanupErr != nil {
			logger.LegacyPrintf("service.concurrency", "Failed to reclaim uncertain API key %d slot: %v", policy.keyID, cleanupErr)
		}
		cleanupCancel()
		return ctx, nil, fmt.Errorf("acquire API key slot: %w", err)
	}
	if !acquired {
		return ctx, nil, ErrAPIKeyConcurrencyLimitExceeded
	}
	releaseKey := sync.OnceFunc(func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), apiKeySlotTrackTimeout)
		defer cancel()
		if err := cache.ReleaseAPIKeySlot(releaseCtx, policy.keyID, requestID); err != nil {
			logger.LegacyPrintf("service.concurrency", "Failed to release API key %d slot: %v", policy.keyID, err)
		}
	})
	stopRefresh := make(chan struct{})
	refreshDone := make(chan struct{})
	var lifecycleMu sync.Mutex
	released := false
	release := sync.OnceFunc(func() {
		lifecycleMu.Lock()
		released = true
		close(stopRefresh)
		lifecycleMu.Unlock()
		releaseKey()
	})
	/*
	 * Long generations renew their slots; lost protection cancels upstream work.
	 */
	go func() {
		defer close(refreshDone)
		ticker := time.NewTicker(apiKeySlotRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopRefresh:
				return
			case <-ticker.C:
				refreshCtx, cancel := context.WithTimeout(context.Background(), apiKeySlotTrackTimeout)
				owned, err := cache.RefreshAPIKeySlot(refreshCtx, policy.keyID, requestID)
				cancel()
				if err != nil || !owned {
					lifecycleMu.Lock()
					if released {
						lifecycleMu.Unlock()
						return
					}
					policy.cancel(errors.New("API key concurrency lease lost"))
					lifecycleMu.Unlock()
					release()
					return
				}
			}
		}
	}()
	/*
	 * Admission timeouts may end after a queued request is accepted.
	 */
	stop := context.AfterFunc(policy.lifetime, release)
	policy.reserved = true
	reservedCtx := context.WithValue(ctx, apiKeyConcurrencyPolicyKey{}, policy)
	return reservedCtx, func() {
		stop()
		release()
		<-refreshDone
	}, nil
}
