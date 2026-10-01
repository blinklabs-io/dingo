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
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/stretchr/testify/require"
)

const totalTestEpoch = uint64(995)

// --- compareTotalActiveStake: the comparison itself -------------------------
//
// It now takes Koios's already-fetched total and fetch error rather than
// doing its own lookup, so CheckStakeDistribution can read epoch_info once
// and use it both for the not-published guard and for this (dingo#4820).
// These cases are therefore pure arithmetic and need no server.

// TestCompareTotalActiveStakeDetectsADroppedPool is the point of dingo#4321:
// the per-pool loop iterates only the pools Dingo reports, so a pool Dingo
// lost entirely is never looked up and the epoch reports a clean match.
// Summing what Dingo did report and comparing against Koios's epoch-wide
// active_stake catches the omission.
func TestCompareTotalActiveStakeDetectsADroppedPool(t *testing.T) {
	t.Parallel()
	reported := []poolStake{
		{bech32: "pool1aaa", stake: 100},
		{bech32: "pool1bbb", stake: 200},
		// pool1ccc, stake 300, dropped by Dingo.
	}
	got := compareTotalActiveStake("600", nil, totalTestEpoch, reported)
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
	reported := []poolStake{
		{bech32: "pool1aaa", stake: 100},
		{bech32: "pool1bbb", stake: 200},
		{bech32: "pool1ccc", stake: 300},
	}
	require.Nil(t,
		compareTotalActiveStake("600", nil, totalTestEpoch, reported),
		"agreeing totals must not be reported as a divergence")
}

// TestCompareTotalActiveStakeReportsADingoSurplus pins the other direction.
// Raised in review on #4781: changing the final guard to `diff >= 0` left
// every test in this package green, so the total check could silently stop
// reporting Dingo over-reporting. A surplus means Dingo reports stake Koios's
// epoch total does not account for, which is what a duplicated or phantom
// pool looks like.
func TestCompareTotalActiveStakeReportsADingoSurplus(t *testing.T) {
	t.Parallel()
	reported := []poolStake{
		{bech32: "pool1aaa", stake: 100},
		{bech32: "pool1bbb", stake: 200},
		{bech32: "pool1ccc", stake: 300},
		{bech32: "pool1ddd", stake: 150}, // Koios's total covers only 600.
	}
	got := compareTotalActiveStake("600", nil, totalTestEpoch, reported)
	require.NotNil(t, got,
		"a dingo surplus must be reported, not only a shortfall")
	require.Equal(t, ReasonTotalActiveStakeMismatch, got.Reason)
	require.Equal(t, int64(150), got.DiffLovelace)
	require.False(t, got.KoiosFault,
		"a surplus is a real divergence, not a koios-side fault")
}

// TestCompareTotalActiveStakeFlagsAnUnusableKoiosValueAsAFault pins that a
// fault on Koios's side is reported, but not as a Dingo divergence. Staying
// silent would be worse than either: a caller cannot tell a check that found
// nothing from one that never ran.
func TestCompareTotalActiveStakeFlagsAnUnusableKoiosValueAsAFault(t *testing.T) {
	t.Parallel()
	got := compareTotalActiveStake("not-a-number", nil, totalTestEpoch,
		[]poolStake{{bech32: "pool1aaa", stake: 100}})
	require.NotNil(t, got, "an unusable Koios value must still be reported")
	require.True(t, got.KoiosFault,
		"an unusable Koios value is not a Dingo divergence")
	require.Equal(t, "unparseable koios active_stake value", got.Reason)
}

// TestCompareTotalActiveStakeFlagsAFetchFailure pins that an epoch_info fetch
// failure is reported, not swallowed into a clean result.
//
// It is a KoiosFault mismatch rather than an error on purpose. Reviewed on
// #4781: an error return discards the per-pool mismatches
// CheckStakeDistribution has already collected, so an outage of this one
// endpoint would hide a pool divergence the per-pool half had found.
func TestCompareTotalActiveStakeFlagsAFetchFailure(t *testing.T) {
	t.Parallel()
	got := compareTotalActiveStake("", errors.New("koios down"),
		totalTestEpoch, []poolStake{{bech32: "pool1aaa", stake: 100}})
	require.NotNil(t, got,
		"a failed epoch_info fetch must not read as a match")
	require.True(t, got.KoiosFault,
		"an unreachable reference is not a dingo divergence")
	require.Contains(t, got.Reason, ReasonKoiosEpochInfoUnavailable)
	require.Equal(t, uint64(100), got.DingoStake)
}

// TestCompareTotalActiveStakeFlagsAMissingKoiosTotal covers the other
// unrunnable case: Koios answers, but with no active_stake for an epoch that
// should have one.
func TestCompareTotalActiveStakeFlagsAMissingKoiosTotal(t *testing.T) {
	t.Parallel()
	got := compareTotalActiveStake("", nil, totalTestEpoch,
		[]poolStake{{bech32: "pool1aaa", stake: 100}})
	require.NotNil(t, got)
	require.Equal(t, ReasonNoKoiosActiveStake, got.Reason)
	require.True(t, got.KoiosFault)
	require.Equal(t, uint64(100), got.DingoStake)
}

// TestCompareTotalActiveStakeSkipsPreStakingEpochs walks the boundary
// koiosparity.IsPreStakingEpoch draws, from both sides.
//
// Koios returns active_stake=null for epochs 0 and 1 permanently and
// correctly: the "go" stake snapshot an epoch's active stake is computed
// from is captured two epochs earlier, so it does not exist until epoch 2.
// from-genesis starts at genesis, so folding them into
// ReasonNoKoiosActiveStake would make the first two stake checks of every
// healthy replay report a fault. Above the boundary the same null means
// something is wrong upstream and must still be reported.
func TestCompareTotalActiveStakeSkipsPreStakingEpochs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		epoch     uint64
		wantFault bool
	}{
		{"epoch 0 predates any stake snapshot", 0, false},
		{"epoch 1 is the last pre-staking epoch", 1, false},
		{"epoch 2 is the first epoch that must have one", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := compareTotalActiveStake("", nil, tc.epoch,
				[]poolStake{{bech32: "pool1aaa", stake: 100}})
			if !tc.wantFault {
				require.Nil(t, got,
					"a pre-staking epoch has nothing to compare and must "+
						"not be reported as a Koios fault")
				return
			}
			require.NotNil(t, got,
				"above the pre-staking boundary a missing active_stake is a "+
					"real reference failure and must stay visible")
			require.Equal(t, ReasonNoKoiosActiveStake, got.Reason)
			require.True(t, got.KoiosFault)
		})
	}
}

// --- koiosActiveStakeForEpoch: the fetch half -------------------------------

func openEpochInfoCache(
	t *testing.T, network string, epoch uint64,
	activeStake string, endTime time.Time,
) *koiosparity.Cache {
	t.Helper()
	cache, err := koiosparity.OpenCache(
		filepath.Join(t.TempDir(), "cache.db"),
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	require.NoError(t, cache.UpsertEpochInfo(koiosparity.KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  activeStake,
		EpochEndTime: endTime,
		TotalRewards: "777",
		FetchedAt:    time.Now().UTC(),
	}))
	return cache
}

// TestKoiosActiveStakeForEpochWorksWithoutACache pins that
// --koios-cache-path is genuinely optional: a cacheless run fetches
// epoch_info live instead, at the cost of one request per epoch.
func TestKoiosActiveStakeForEpochWorksWithoutACache(t *testing.T) {
	t.Parallel()
	koiosURL, reqCount := countingKoiosServer(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w,
				`[{"epoch_no":%s,"active_stake":"600","end_time":1700000000}]`,
				r.URL.Query().Get("_epoch_no"))
		})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	total, endTime, err := koiosActiveStakeForEpoch(
		context.Background(), koios, nil, "preview", totalTestEpoch,
	)
	require.NoError(t, err)
	require.Equal(t, "600", total,
		"a nil cache must still fetch, not silently skip")
	require.False(t, endTime.IsZero(),
		"the end time must come back too, or the not-published guard "+
			"cannot tell lag from a divergence")
	require.Equal(t, int32(1), reqCount.Load(),
		"exactly one epoch_info request when there is no cache to read")
}

// TestKoiosActiveStakeForEpochDoesNotClobberCachedEpochInfo is the reason
// this reads the cache but never writes it. koios_epoch_info rows are shared
// with dingo's in-process koios-parity observer, and UpsertEpochInfo
// rewrites every column on conflict -- so caching active_stake alone would
// blank EpochEndTime, which internal/koiosparity reads to size the grace
// window that keeps a lagged, empty account-reward result retryable.
func TestKoiosActiveStakeForEpochDoesNotClobberCachedEpochInfo(t *testing.T) {
	t.Parallel()
	const network = "preview"
	endTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	// Fully populated by the observer, but with no active_stake yet, so this
	// call does go to Koios.
	cache := openEpochInfoCache(t, network, totalTestEpoch, "", endTime)

	koiosURL, _ := countingKoiosServer(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w,
				`[{"epoch_no":%s,"active_stake":"600"}]`,
				r.URL.Query().Get("_epoch_no"))
		})
	koios, err := NewKoiosClient(network, "", koiosURL, true, true)
	require.NoError(t, err)

	_, _, err = koiosActiveStakeForEpoch(
		context.Background(), koios, cache, network, totalTestEpoch,
	)
	require.NoError(t, err)

	after, err := cache.GetEpochInfo(network, totalTestEpoch)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Equal(t, endTime.UTC(), after.EpochEndTime.UTC(),
		"the observer's epoch end time must survive this check")
	require.Equal(t, "777", after.TotalRewards,
		"the observer's reward columns must survive this check")
}
