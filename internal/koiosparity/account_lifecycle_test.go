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

package koiosparity

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAccountLifecycleMismatchesReportsZeroReward proves dingo #3099's
// zero-reward-confirmed reporting: an address Koios answered for with no
// reward rows is reported via CategoryAcctZeroReward — a dimension #3097's
// merged CompareAccountEpoch structurally cannot see (it only ever compares
// keys present in at least one side's row map).
func TestAccountLifecycleMismatchesReportsZeroReward(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now()
	// addr1 earns a reward (1 staged row); addr2 is confirmed checked with
	// zero reward rows (present in addressesInChunk, no matching row).
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview",
		500,
		"chunkA",
		[]KoiosAccountRewards{
			{StakeAddress: "addr1", RewardType: "member", Earned: "1000"},
		},
		[]string{"addr1", "addr2"},
		now,
	))

	mismatches := accountLifecycleMismatches(cache, "preview", 500, now)
	require.Len(t, mismatches, 1)
	require.Equal(t, "addr2", mismatches[0].StakeAddress)
	require.Equal(t, CategoryAcctZeroReward, mismatches[0].Category)
	require.Equal(t, "0", mismatches[0].KoiosValue)
}

// TestAccountLifecycleMismatchesReportsNewlyRegisteredAndDeregistered proves
// the epoch-over-epoch universe diff: an address present in the current
// epoch's checked set but not the previous epoch's is newly registered; the
// reverse is deregistered.
func TestAccountLifecycleMismatchesReportsNewlyRegisteredAndDeregistered(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now()
	// Previous epoch (499): addrOld, addrBoth checked; coverage complete.
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview", 499, "chunkA", nil, []string{"addrOld", "addrBoth"}, now,
	))
	require.NoError(t, cache.CommitAccountRewardsForEpoch(
		"preview", 499, nil, 2, true, now,
	))

	// Current epoch (500): addrBoth, addrNew checked.
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview", 500, "chunkA", nil, []string{"addrBoth", "addrNew"}, now,
	))

	mismatches := accountLifecycleMismatches(cache, "preview", 500, now)

	var sawNew, sawDeregistered bool
	for _, m := range mismatches {
		switch m.Category {
		case CategoryAcctNewlyRegistered:
			require.Equal(t, "addrNew", m.StakeAddress)
			sawNew = true
		case CategoryAcctDeregistered:
			require.Equal(t, "addrOld", m.StakeAddress)
			sawDeregistered = true
		case CategoryAcctZeroReward:
			// Both addresses have zero reward rows here (nil rows passed
			// above) — expected alongside the lifecycle categories, not
			// asserted on further in this test.
		default:
			t.Fatalf("unexpected category %q", m.Category)
		}
	}
	require.True(t, sawNew, "addrNew must be reported as newly registered")
	require.True(t, sawDeregistered, "addrOld must be reported as deregistered")
}

// TestAccountLifecycleMismatchesDisablesLifecycleReportWhenPreviousEpochIncomplete
// proves the previous epoch's universe is only trusted once its own coverage
// is complete — an absent or incomplete previous fetch must disable the
// newly-registered/deregistered report entirely rather than let every
// current address look newly registered.
func TestAccountLifecycleMismatchesDisablesLifecycleReportWhenPreviousEpochIncomplete(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now()
	// Current epoch (500) has data; epoch 499 was never fetched at all.
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview",
		500,
		"chunkA",
		[]KoiosAccountRewards{
			{StakeAddress: "addrNew", RewardType: "member", Earned: "1000"},
		},
		[]string{"addrNew"},
		now,
	))

	mismatches := accountLifecycleMismatches(cache, "preview", 500, now)
	for _, m := range mismatches {
		require.NotEqual(
			t,
			CategoryAcctNewlyRegistered,
			m.Category,
			"no previous-epoch coverage means the lifecycle report must be disabled entirely",
		)
		require.NotEqual(t, CategoryAcctDeregistered, m.Category)
	}
}

// TestAccountLifecycleMismatchesEpochZeroSkipsLifecycleReport proves epoch 0
// (no possible previous epoch) never attempts the lifecycle diff.
func TestAccountLifecycleMismatchesEpochZeroSkipsLifecycleReport(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	mismatches := accountLifecycleMismatches(cache, "preview", 0, time.Now())
	require.Empty(t, mismatches)
}

// TestDetermineStatusAccountLifecycleCategoriesAreInformational proves the
// three dingo #3099 categories never affect Status: alone they must PASS,
// and alongside a genuine FAIL-triggering mismatch they must not mask or
// alter that FAIL.
func TestDetermineStatusAccountLifecycleCategoriesAreInformational(
	t *testing.T,
) {
	now := time.Now()
	onlyInformational := []CheckMismatch{
		{Category: CategoryAcctZeroReward, CheckedAt: now},
		{Category: CategoryAcctNewlyRegistered, CheckedAt: now},
		{Category: CategoryAcctDeregistered, CheckedAt: now},
	}
	require.Equal(t, StatusPass, DetermineStatus(onlyInformational))

	withRealFailure := append(
		append([]CheckMismatch{}, onlyInformational...),
		CheckMismatch{Category: CategoryValueMismatch, CheckedAt: now},
	)
	require.Equal(t, StatusFail, DetermineStatus(withRealFailure))
}
