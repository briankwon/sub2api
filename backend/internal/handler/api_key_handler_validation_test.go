//go:build unit

package handler

import (
	"math"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/stretchr/testify/require"
)

func TestValidateAPIKeyCreateRequest(t *testing.T) {
	zero, large, negative, nan, inf := 0.0, 1e100, -1.0, math.NaN(), math.Inf(1)
	positiveDays, zeroDays, negativeDays := 1, 0, -1
	require.NoError(t, validateAPIKeyCreateRequest(CreateAPIKeyRequest{}))
	require.NoError(t, validateAPIKeyCreateRequest(CreateAPIKeyRequest{Quota: &zero, RateLimit5h: &large, ExpiresInDays: &positiveDays}))

	for _, req := range []CreateAPIKeyRequest{
		{Quota: &negative},
		{Quota: &nan},
		{RateLimit5h: &inf},
		{RateLimit1d: &negative},
		{RateLimit7d: &negative},
		{ExpiresInDays: &zeroDays},
		{ExpiresInDays: &negativeDays},
	} {
		require.Error(t, validateAPIKeyCreateRequest(req))
	}
}

func TestValidateAPIKeyUpdateRequest(t *testing.T) {
	zero, large, negative, nan, inf := 0.0, 1e100, -1.0, math.NaN(), math.Inf(-1)
	require.NoError(t, validateAPIKeyUpdateRequest(UpdateAPIKeyRequest{Quota: &zero, RateLimit7d: &large}))

	for _, req := range []UpdateAPIKeyRequest{
		{Quota: &negative},
		{RateLimit5h: &nan},
		{RateLimit1d: &inf},
		{RateLimit7d: &negative},
	} {
		require.Error(t, validateAPIKeyUpdateRequest(req))
	}
}

/* Both HTTP operations reject limits the INTEGER column cannot store. */
func TestValidateAPIKeyConcurrencyLimit(t *testing.T) {
	for _, limit := range []int{0, 1, service.APIKeyMaxConcurrency} {
		require.NoError(t, validateAPIKeyCreateRequest(CreateAPIKeyRequest{ConcurrencyLimit: limit}))
		require.NoError(t, validateAPIKeyUpdateRequest(UpdateAPIKeyRequest{ConcurrencyLimit: &limit}))
	}
	for _, limit := range []int{-1, service.APIKeyMaxConcurrency + 1} {
		require.Error(t, validateAPIKeyCreateRequest(CreateAPIKeyRequest{ConcurrencyLimit: limit}))
		require.Error(t, validateAPIKeyUpdateRequest(UpdateAPIKeyRequest{ConcurrencyLimit: &limit}))
	}
}
