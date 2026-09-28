// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nodeparity

import (
	"context"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/stretchr/testify/require"
)

// cacheWithEpochInfo returns a cache holding one epoch_info row, so these
// tests exercise compareTotalActiveStake's cache-first path without any
// network call.
func cacheWithEpochInfo(
	t *testing.T, network string, epoch uint64, activeStake string,
) *koiosparity.Cache {
	t.Helper()
	cache, err := koiosparity.OpenCache(
		filepath.Join(t.TempDir(), "cache.db"),
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	require.NoError(t, cache.UpsertEpochInfo(koiosparity.KoiosEpochInfo{
		Network:     network,
		Epoch:       epoch,
		ActiveStake: activeStake,
		FetchedAt:   time.Now().UTC(),
	}))
	return cache
}

// TestCompareTotalActiveStakeDetectsADroppedPool is the point of dingo#4321:
// the per-pool loop iterates only the pools Dingo reports, so a pool Dingo
// lost entirely is never looked up and the epoch reports a clean match.
// Summing what Dingo did report and comparing against Koios's epoch-wide
// active_stake catches the omission.
func TestCompareTotalActiveStakeDetectsADroppedPool(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995

	// Koios's epoch total covers three pools; Dingo reports only two.
	cache := cacheWithEpochInfo(t, network, epoch, "600")
	reported := []poolStake{
		{bech32: "pool1aaa", stake: 100},
		{bech32: "pool1bbb", stake: 200},
		// pool1ccc, stake 300, dropped by Dingo.
	}

	got := compareTotalActiveStake(
		context.Background(), nil, cache, network, epoch, reported,
	)
	require.NotNil(t, got, "a dropped pool carrying stake must be reported")
	require.Equal(t, ReasonTotalActiveStakeMismatch, got.Reason)
	require.Empty(t, got.PoolIDBech32,
		"the total check knows a pool is missing, not which one")
	require.Equal(t, uint64(300), got.DingoStake)
	require.Equal(t, "600", got.KoiosStake)
	require.Equal(t, int64(-300), got.DiffLovelace,
		"the shortfall equals the dropped pool's stake")
}

// TestCompareTotalActiveStakeQuietWhenTotalsAgree pins the other half: this
// check must not fire on a healthy epoch. Measured across 1,321 cached
// Preview epochs (2-1322), Koios's own per-pool active_stake values sum to
// its epoch_info active_stake exactly, with no rounding slack anywhere, so
// any tolerance here would only hide a real divergence.
func TestCompareTotalActiveStakeQuietWhenTotalsAgree(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995

	cache := cacheWithEpochInfo(t, network, epoch, "600")
	reported := []poolStake{
		{bech32: "pool1aaa", stake: 100},
		{bech32: "pool1bbb", stake: 200},
		{bech32: "pool1ccc", stake: 300},
	}

	require.Nil(t, compareTotalActiveStake(
		context.Background(), nil, cache, network, epoch, reported,
	), "agreeing totals must not be reported as a divergence")
}

// TestCompareTotalActiveStakeStaysQuietOnAnUnusableKoiosValue pins that a
// fault on Koios's side is not reported as a Dingo divergence. The per-pool
// path marks that case KoiosFault; here there is no pool to attribute it to,
// so the check stays silent rather than inventing one.
func TestCompareTotalActiveStakeStaysQuietOnAnUnusableKoiosValue(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995
	reported := []poolStake{{bech32: "pool1aaa", stake: 100}}

	cache := cacheWithEpochInfo(t, network, epoch, "not-a-number")
	require.Nil(t, compareTotalActiveStake(
		context.Background(), nil, cache, network, epoch, reported,
	), "an unusable Koios value is not a Dingo divergence")
}

// TestCompareTotalActiveStakeWorksWithoutACache pins that --koios-cache-path
// is genuinely optional for this check. A cacheless run fetches epoch_info
// live instead, at the cost of one request per epoch; it must not silently
// skip the comparison, which would leave a no-cache run blind to exactly the
// missing-pool case this exists to catch.
func TestCompareTotalActiveStakeWorksWithoutACache(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995

	koiosURL, reqCount := countingKoiosServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"epoch_no":995,"active_stake":"600"}]`))
		})
	koios, err := NewKoiosClient(network, "", koiosURL, true, true)
	require.NoError(t, err)

	reported := []poolStake{
		{bech32: "pool1aaa", stake: 100},
		{bech32: "pool1bbb", stake: 200},
		// 300 missing.
	}

	got := compareTotalActiveStake(
		context.Background(), koios, nil, network, epoch, reported,
	)
	require.NotNil(t, got,
		"a nil cache must still compare, not silently skip the check")
	require.Equal(t, ReasonTotalActiveStakeMismatch, got.Reason)
	require.Equal(t, int64(-300), got.DiffLovelace)
	require.Equal(t, int32(1), reqCount.Load(),
		"exactly one epoch_info request when there is no cache to read")
}
