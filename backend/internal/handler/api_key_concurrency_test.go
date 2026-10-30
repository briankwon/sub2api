package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type queuedKeyAdmission struct {
	release func()
	err     error
}

/*
 * A waiting client consumes its own Key capacity, so it cannot fill the shared
 * user's queue with arbitrarily many requests. Both cancel and successful
 * admission must retain the right ownership and release capacity exactly once.
 */
func TestAPIKeyConcurrencyCoversUserQueue(t *testing.T) {
	for _, cancelWhileWaiting := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel=%t", cancelWhileWaiting), func(t *testing.T) {
			cache := testutil.NewRedisConcurrencyCache(t)
			svc := service.NewConcurrencyService(cache)
			helper := NewConcurrencyHelper(svc, SSEPingFormatNone, time.Second)
			userSlot, err := svc.AcquireUserSlot(context.Background(), 7, 1)
			require.NoError(t, err)
			require.True(t, userSlot.Acquired)
			t.Cleanup(userSlot.ReleaseFunc)
			parent, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			requestCtx := service.WithAPIKeyConcurrencyLimit(parent, 11, 1)
			c, _ := newHelperTestContext(http.MethodPost, "/v1/messages")
			c.Request = c.Request.WithContext(requestCtx)
			c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 11})
			outcome := make(chan queuedKeyAdmission, 1)
			go func() {
				started := false
				release, err := helper.acquireUserSlotWithWaitTimeout(c, 7, 1, 2*time.Second, false, &started)
				outcome <- queuedKeyAdmission{release: release, err: err}
			}()
			require.Eventually(t, func() bool {
				counts, err := svc.GetAPIKeyConcurrencyBatch(context.Background(), []int64{11})
				return err == nil && counts[11] == 1
			}, time.Second, time.Millisecond)
			second, _ := newHelperTestContext(http.MethodPost, "/v1/messages")
			second.Request = second.Request.WithContext(service.WithAPIKeyConcurrencyLimit(context.Background(), 11, 1))
			second.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 11})
			started := false
			_, err = helper.AcquireUserSlotWithWait(second, 7, 1, false, &started)
			require.ErrorIs(t, err, service.ErrAPIKeyConcurrencyLimitExceeded)
			require.False(t, second.Writer.Written(), "Key rejection must precede stream headers")
			if cancelWhileWaiting {
				cancel()
			} else {
				userSlot.ReleaseFunc()
			}
			var result queuedKeyAdmission
			select {
			case result = <-outcome:
			case <-time.After(3 * time.Second):
				t.Fatal("queued request did not finish")
			}
			if cancelWhileWaiting {
				require.ErrorIs(t, result.err, context.Canceled)
			} else {
				require.NoError(t, result.err)
				counts, err := svc.GetAPIKeyConcurrencyBatch(context.Background(), []int64{11})
				require.NoError(t, err)
				require.Equal(t, 1, counts[11], "leaving the wait timeout scope must not release the request")
				result.release()
				result.release()
			}
			require.Eventually(t, func() bool {
				counts, err := svc.GetAPIKeyConcurrencyBatch(context.Background(), []int64{11})
				return err == nil && counts[11] == 0
			}, time.Second, time.Millisecond)
		})
	}
}

type wsConcurrencyPolicyRepo struct {
	service.APIKeyRepository
	key   *service.APIKey
	limit atomic.Int64
	fail  atomic.Bool
}

func (r *wsConcurrencyPolicyRepo) GetByKeyForAuth(context.Context, string) (*service.APIKey, error) {
	if r.fail.Load() {
		return nil, errors.New("temporary auth store failure")
	}
	key := *r.key
	key.ConcurrencyLimit = int(r.limit.Load())
	return &key, nil
}

/*
 * Existing WS connections apply new caps on the next response.create turn,
 * including clearing a positive cap. Rejected turns never reach upstream.
 */
func TestAPIKeyConcurrencyWebSocketRefreshesBetweenTurns(t *testing.T) {
	for _, tc := range []struct {
		name         string
		snapshot     int
		initial      int
		next         int
		refreshFails bool
		blocked      bool
	}{
		{name: "enable limit", snapshot: 0, initial: 0, next: 1, blocked: true},
		{name: "lower limit", snapshot: 3, initial: 3, next: 1, blocked: true},
		{name: "clear limit", snapshot: 1, initial: 1, next: 0, blocked: false},
		{name: "failed refresh retains last successful limit", snapshot: 0, initial: 1, next: 0, refreshFails: true, blocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var upstreamRequests atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
				if err != nil {
					return
				}
				defer conn.CloseNow()
				for {
					if _, _, err = conn.Read(r.Context()); err != nil {
						return
					}
					number := upstreamRequests.Add(1)
					payload := fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_key_limit_%d","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":1}}}`, number)
					if err = conn.Write(r.Context(), coderws.MessageText, []byte(payload)); err != nil {
						return
					}
				}
			}))
			defer upstream.Close()
			svc := service.NewConcurrencyService(testutil.NewRedisConcurrencyCache(t))
			var repo *wsConcurrencyPolicyRepo
			harness := newOpenAIWSPassthroughHandlerHarnessConfigured(t, upstream.URL, func(h *OpenAIGatewayHandler, key *service.APIKey) {
				key.ConcurrencyLimit = tc.snapshot
				repo = &wsConcurrencyPolicyRepo{key: key}
				repo.limit.Store(int64(tc.initial))
				h.cfg = &config.Config{RunMode: config.RunModeSimple}
				h.apiKeyService = service.NewAPIKeyService(repo, nil, nil, nil, nil, nil, &config.Config{})
				h.concurrencyHelper = NewConcurrencyHelper(svc, SSEPingFormatNone, time.Second)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request := []byte(`{"type":"response.create","model":"gpt-5.1","input":"hello"}`)
			require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, request))
			_, response, err := harness.clientConn.Read(ctx)
			require.NoError(t, err)
			require.Contains(t, string(response), "response.completed")
			require.Eventually(t, func() bool {
				counts, err := svc.GetAPIKeyConcurrencyBatch(ctx, []int64{harness.apiKey.ID})
				return err == nil && counts[harness.apiKey.ID] == 0
			}, time.Second, time.Millisecond, "idle WS sessions must not occupy a turn slot")
			holderCtx := service.WithAPIKeyConcurrencyLimit(ctx, harness.apiKey.ID, 1)
			_, releaseHolder, err := svc.ReserveAPIKeySlot(holderCtx)
			require.NoError(t, err)
			defer releaseHolder()
			repo.limit.Store(int64(tc.next))
			repo.fail.Store(tc.refreshFails)
			require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, request))
			_, response, err = harness.clientConn.Read(ctx)
			if tc.blocked {
				require.Equal(t, coderws.StatusTryAgainLater, coderws.CloseStatus(err))
				require.ErrorContains(t, err, "api_key_concurrency_limit")
				require.EqualValues(t, 1, upstreamRequests.Load())
			} else {
				require.NoError(t, err)
				require.Contains(t, string(response), "response.completed")
				require.EqualValues(t, 2, upstreamRequests.Load())
			}
		})
	}
}
