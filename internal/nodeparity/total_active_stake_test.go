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

	got, err := compareTotalActiveStake(
		context.Background(), nil, cache, network, epoch, reported,
	)
	require.NoError(t, err)
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

	got, err := compareTotalActiveStake(
		context.Background(), nil, cache, network, epoch, reported,
	)
	require.NoError(t, err)
	require.Nil(t, got, "agreeing totals must not be reported as a divergence")
}

// TestCompareTotalActiveStakeFlagsAnUnusableKoiosValueAsAFault pins that a
// fault on Koios's side is reported, but not as a Dingo divergence. Staying
// silent would be worse than either: a caller cannot tell a check that found
// nothing from one that never ran, which is the blind spot #4321 is about.
// KoiosFault is how the per-pool path draws that line, and callers already
// exclude it from the divergence count.
func TestCompareTotalActiveStakeFlagsAnUnusableKoiosValueAsAFault(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995
	reported := []poolStake{{bech32: "pool1aaa", stake: 100}}

	cache := cacheWithEpochInfo(t, network, epoch, "not-a-number")
	got, err := compareTotalActiveStake(
		context.Background(), nil, cache, network, epoch, reported,
	)
	require.NoError(t, err, "a Koios data fault is not a check failure")
	require.NotNil(t, got, "an unusable Koios value must still be reported")
	require.True(t, got.KoiosFault,
		"an unusable Koios value is not a Dingo divergence")
	require.Equal(t, "unparseable koios active_stake value", got.Reason)
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

	got, err := compareTotalActiveStake(
		context.Background(), koios, nil, network, epoch, reported,
	)
	require.NoError(t, err)
	require.NotNil(t, got,
		"a nil cache must still compare, not silently skip the check")
	require.Equal(t, ReasonTotalActiveStakeMismatch, got.Reason)
	require.Equal(t, int64(-300), got.DiffLovelace)
	require.Equal(t, int32(1), reqCount.Load(),
		"exactly one epoch_info request when there is no cache to read")
}

// TestCompareTotalActiveStakeReturnsAFetchError pins that an epoch_info fetch
// failure is propagated, not swallowed into a clean result. A nil mismatch
// and a nil error mean "compared, and they agree"; returning that for a
// comparison that never ran would recreate #4321's blind spot in a new place
// -- CheckStakeDistribution would report the epoch verified having skipped
// the only check able to see a wholly missing pool.
func TestCompareTotalActiveStakeReturnsAFetchError(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995

	koiosURL, _ := countingKoiosServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})
	koios, err := NewKoiosClient(network, "", koiosURL, true, true)
	require.NoError(t, err)

	got, err := compareTotalActiveStake(
		context.Background(), koios, nil, network, epoch,
		[]poolStake{{bech32: "pool1aaa", stake: 100}},
	)
	require.Error(t, err, "a failed epoch_info fetch must not read as a match")
	require.Nil(t, got)
}

// TestCompareTotalActiveStakeFlagsAMissingKoiosTotal covers the other
// unrunnable case: Koios answers, but with no active_stake for the epoch.
// That is a missing reference rather than a transport failure, so it is a
// KoiosFault mismatch rather than an error -- still visible, still excluded
// from the divergence count.
func TestCompareTotalActiveStakeFlagsAMissingKoiosTotal(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995

	koiosURL, _ := countingKoiosServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"epoch_no":995,"active_stake":null}]`))
		})
	koios, err := NewKoiosClient(network, "", koiosURL, true, true)
	require.NoError(t, err)

	got, err := compareTotalActiveStake(
		context.Background(), koios, nil, network, epoch,
		[]poolStake{{bech32: "pool1aaa", stake: 100}},
	)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, ReasonNoKoiosActiveStake, got.Reason)
	require.True(t, got.KoiosFault)
	require.Equal(t, uint64(100), got.DingoStake)
}

// TestCompareTotalActiveStakeDoesNotClobberCachedEpochInfo is the reason this
// check reads the cache but never writes it. koios_epoch_info rows are shared
// with dingo's in-process koios-parity observer, and UpsertEpochInfo rewrites
// every column on conflict -- so caching active_stake alone would blank
// EpochEndTime, which internal/koiosparity reads to size the grace window
// that keeps a lagged, empty account-reward result retryable. A zero there
// silently accepts an empty result as complete, in a different process.
//
// The row here is the shape that actually triggers it: fully populated by the
// observer, but with no active_stake yet, so this check does go to Koios.
func TestCompareTotalActiveStakeDoesNotClobberCachedEpochInfo(t *testing.T) {
	t.Parallel()
	const network = "preview"
	const epoch = 995
	endTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	cache, err := koiosparity.OpenCache(
		filepath.Join(t.TempDir(), "cache.db"),
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	require.NoError(t, cache.UpsertEpochInfo(koiosparity.KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  "",
		TotalRewards: "777",
		EpochEndTime: endTime,
		FetchedAt:    time.Now().UTC(),
	}))

	koiosURL, _ := countingKoiosServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"epoch_no":995,"active_stake":"600"}]`))
		})
	koios, err := NewKoiosClient(network, "", koiosURL, true, true)
	require.NoError(t, err)

	_, err = compareTotalActiveStake(
		context.Background(), koios, cache, network, epoch,
		[]poolStake{{bech32: "pool1aaa", stake: 600}},
	)
	require.NoError(t, err)

	after, err := cache.GetEpochInfo(network, epoch)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Equal(t, endTime.UTC(), after.EpochEndTime.UTC(),
		"the observer's epoch end time must survive this check")
	require.Equal(t, "777", after.TotalRewards,
		"the observer's reward columns must survive this check")
}
