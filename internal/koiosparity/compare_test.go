// Copyright 2025 Blink Labs Software
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCompareEpochAggregatesIgnoresEpochInfoFeesAndRewards is an integrated
// test using real, distinct preview epoch 10 values for /epoch_info and
// /totals (see KoiosTotalsResp's doc comment) to guard against reintroducing
// the bug where CompareEpochAggregates compared reward_ada_pots.Fees/Rewards
// (the AdaPots pot values) against /epoch_info.fees/total_rewards (raw
// block/tx accounting) — a different quantity that only /totals.fees (checked
// by CompareEpochTotals) correctly matches against. dingo.Fees equals the
// /totals figure and deliberately does NOT equal /epoch_info.fees;
// CompareEpochAggregates must report zero mismatches for total_active_stake
// alone regardless. koios.Reward/dingo.TotalRewards are real live values that
// happen to differ hugely (see CompareEpochTotals's doc comment on why
// totals.reward isn't compared at all) — included to confirm neither function
// reacts to that divergence.
func TestCompareEpochAggregatesIgnoresEpochInfoFeesAndRewards(t *testing.T) {
	now := time.Now()
	koiosEpochInfo := &KoiosEpochInfo{
		ActiveStake:  "100",
		Fees:         "597144524", // /epoch_info.fees: tx fees collected this epoch
		TotalRewards: "13101661554",
	}
	koiosTotals := &KoiosTotals{
		Fees:   "484716590", // /totals.fees: the AdaPots fee-pot balance
		Reward: "500000000", // /totals.reward: not compared — see CompareEpochTotals
	}
	dingo := &DingoEpochData{
		TotalActiveStake:     "100",
		Fees:                 "484716590", // reward_ada_pots.Fees — matches /totals, not /epoch_info
		TotalRewards:         "999999999", // reward_ada_pots.Rewards — not compared against either Koios field
		RewardAdaPotsPresent: true,
	}

	require.Empty(t, CompareEpochAggregates("preview", 10, koiosEpochInfo, dingo, nil, now, 0),
		"epoch_info.fees/total_rewards must not be compared against reward_ada_pots")
	require.Empty(t, CompareEpochTotals("preview", 10, koiosTotals, dingo, now),
		"reward_ada_pots.Fees must match /totals.fees; totals.reward must not be compared at all")
}

func TestCompareEpochTotals(t *testing.T) {
	now := time.Now()
	koios := &KoiosTotals{
		Treasury: "6931231163186226",
		Reserves: "7792082362166766",
		Fees:     "1245791321",
		Reward:   "292608261256804",
	}
	dingo := &DingoEpochData{
		Fees:                 "1245791321",
		TotalRewards:         "1", // arbitrary — must never affect the result; totals.reward isn't compared
		Treasury:             "6931231163186226",
		Reserves:             "7792082362166766",
		RewardAdaPotsPresent: true,
	}
	require.Empty(t, CompareEpochTotals("preview", 1367, koios, dingo, now))

	// totals.fees is the fee-pot value (reward_ada_pots.Fees), a different
	// quantity from epoch_info.fees compared by CompareEpochAggregates —
	// verify a totals-only fees divergence is reported as totals_fees, not
	// epoch_fees.
	dingo.Fees = "1245791322"
	ms := CompareEpochTotals("preview", 1367, koios, dingo, now)
	require.Len(t, ms, 1)
	require.Equal(t, "totals_fees", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
	dingo.Fees = "1245791321"

	dingo.Treasury = "0"
	ms = CompareEpochTotals("preview", 1367, koios, dingo, now)
	require.Len(t, ms, 1)
	require.Equal(t, "totals_treasury", ms[0].Field)
	dingo.Treasury = "6931231163186226"

	// A missing /totals cache row (not yet fetched) skips comparison rather
	// than flagging anything.
	require.Empty(t, CompareEpochTotals("preview", 1367, nil, dingo, now))

	// A missing Dingo row is left to CompareEpochAggregates' "epoch_summary"
	// report; CompareEpochTotals must not duplicate it under a second field.
	require.Empty(t, CompareEpochTotals("preview", 1367, koios, nil, now))
}

// TestCompareEpochTotalsMissingRewardAdaPots guards against the false-PASS
// bug where a ready epoch_summary row with no corresponding reward_ada_pots
// row (RewardAdaPotsPresent == false, all pot fields left as their zero value
// "") was indistinguishable from "nothing to compare" because the field-level
// "!= \"\"" guards suppressed every comparison. A missing pot row combined
// with a cached Koios /totals row must be reported explicitly instead of
// silently passing.
func TestCompareEpochTotalsMissingRewardAdaPots(t *testing.T) {
	now := time.Now()
	koios := &KoiosTotals{
		Treasury: "6931231163186226",
		Reserves: "7792082362166766",
		Fees:     "1245791321",
	}
	dingo := &DingoEpochData{
		TotalActiveStake:     "100",
		RewardAdaPotsPresent: false, // epoch_summary ready, reward_ada_pots absent
	}

	ms := CompareEpochTotals("preview", 1367, koios, dingo, now)
	require.Len(t, ms, 1)
	require.Equal(t, "reward_ada_pots", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}

// TestCompareEpochTotalsRewardIsNeverCompared guards against reintroducing
// the bug this was fixed for: comparing Koios's /totals.reward (a
// monotonically increasing, 2-epoch-lagged cumulative accumulator — verified
// against live preview data: totals.reward jumps from 500000000 at epoch 11
// to 13601661554 at epoch 12, a delta that exactly equals
// epoch_info.total_rewards for epoch 10, not 11 or 12) against Dingo's
// reward_ada_pots.Rewards, which is a fresh single-epoch flow value with no
// stored cumulative counterpart. Since this checker does not compute
// cross-epoch aggregates on Dingo's behalf, totals_reward must never appear
// in the mismatch output, no matter how far apart the two values are.
func TestCompareEpochTotalsRewardIsNeverCompared(t *testing.T) {
	now := time.Now()
	koios := &KoiosTotals{Reward: "13601661554"}
	dingo := &DingoEpochData{
		TotalRewards:         "21543976446", // epoch 12's own single-epoch flow
		RewardAdaPotsPresent: true,
	}

	ms := CompareEpochTotals("preview", 12, koios, dingo, now)
	require.Empty(t, ms, "totals_reward has no Dingo counterpart and must never be reported")
}

func TestComparePoolEpochFixedCostAndMargin(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "1000",
		BlockCnt:    2,
		Delegators:  3,
		FixedCost:   "340000000",
		Margin:      "0.1",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:   true,
		DelegatedStake: "1000",
		ParamsPresent:  true,
		BlocksProduced: 2,
		DelegatorCount: 3,
		FixedCost:      "340000000",
		Margin:         "1/10",
	}
	require.Empty(t, ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{}))

	dingo.FixedCost = "340000001"
	ms := ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "fixed_cost", ms[0].Field)

	dingo.FixedCost = "340000000"
	dingo.Margin = "1/5"
	ms = ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "margin", ms[0].Field)
}

// TestComparePoolEpochParamsNotPresent guards against a false PASS when
// reward_pool_input hasn't been captured yet at the "param epoch" (K+1):
// blocks_produced/fixed_cost/margin must never be silently skipped in a way
// that lets the epoch read as PASS. Within the grace window this is
// reference_lag (ERROR); past it, dingo_db_missing (ERROR) — never PASS and
// never a spurious value_mismatch against zeroed fields.
func TestComparePoolEpochParamsNotPresent(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "1000",
		BlockCnt:    2,
		Delegators:  3,
		FixedCost:   "340000000",
		Margin:      "0.1",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:   true,
		DelegatedStake: "1000",
		DelegatorCount: 3,
		ParamsPresent:  false,
	}

	// Historical (outside grace, or no grace configured): dingo_db_missing.
	ms := ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "reward_pool_input_params", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))

	// Recent (epoch closed within the grace window): reference_lag, not PASS.
	recentClose := now.Add(-time.Hour)
	ms = ComparePoolEpoch("preview", 5, koios, dingo, now, 24, recentClose)
	require.Len(t, ms, 1)
	require.Equal(t, "reward_pool_input_params", ms[0].Field)
	require.Equal(t, CategoryReferenceLag, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}

// TestComparePoolEpochStakeNotPresent guards against a false PASS/
// value_mismatch when reward_pool_input hasn't been captured yet at the
// "stake epoch" (K-1) — e.g. a freshly registered pool whose param-epoch
// row exists but whose stake-epoch row hasn't landed yet. Before StakePresent
// existed, GetPoolEpochDataMap's zero-value stub (DelegatedStake=""/
// DelegatorCount=0) would compare directly against Koios's real figures here
// and produce a false value_mismatch instead of the correct reference_lag/
// dingo_db_missing classification.
func TestComparePoolEpochStakeNotPresent(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "5000000",
		BlockCnt:    4,
		Delegators:  7,
		FixedCost:   "340000000",
		Margin:      "0.1",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:   false,
		ParamsPresent:  true,
		BlocksProduced: 4,
		FixedCost:      "340000000",
		Margin:         "1/10",
	}

	// Historical (outside grace, or no grace configured): dingo_db_missing,
	// never a value_mismatch against the zero-value stub.
	ms := ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "reward_pool_input_stake", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
	for _, m := range ms {
		require.NotEqual(t, CategoryValueMismatch, m.Category)
	}

	// Recent (epoch closed within the grace window): reference_lag, not PASS
	// and not a value_mismatch.
	recentClose := now.Add(-time.Hour)
	ms = ComparePoolEpoch("preview", 5, koios, dingo, now, 24, recentClose)
	require.Len(t, ms, 1)
	require.Equal(t, "reward_pool_input_stake", ms[0].Field)
	require.Equal(t, CategoryReferenceLag, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}

func TestComparePoolEpochMemberRewards(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1test",
		ActiveStake:   "1000",
		BlockCnt:      2,
		Delegators:    3,
		FixedCost:     "340000000",
		Margin:        "0.1",
		MemberRewards: "123456789",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:        true,
		DelegatedStake:      "1000",
		ParamsPresent:       true,
		BlocksProduced:      2,
		DelegatorCount:      3,
		FixedCost:           "340000000",
		Margin:              "1/10",
		MemberRewardPresent: true,
		MemberRewardTotal:   "123456789",
	}
	require.Empty(t, ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{}))

	// Reward calculation not yet finished for this pool/epoch: Dingo has no
	// reward_pool_output row. This must NEVER read as a comparison pass —
	// see TestComparePoolEpochMemberRewardsNotPresent for the recent
	// (reference_lag) vs historical (dingo_db_missing) split this guards.
	dingo.MemberRewardPresent = false
	dingo.MemberRewardTotal = ""
	ms := ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1, "a missing reward_pool_output row must never be silently skipped")
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
	dingo.MemberRewardPresent = true

	dingo.MemberRewardTotal = "1"
	ms = ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
}

// TestComparePoolEpochMemberRewardsNotPresent proves a missing
// reward_pool_output row can never yield PASS regardless of how recently the
// epoch closed — only the mismatch category differs (reference_lag for a
// recent epoch that may simply not be computed yet vs dingo_db_missing for a
// long-settled one), per the reviewer finding that this condition must not be
// conflated with "nothing to compare".
func TestComparePoolEpochMemberRewardsNotPresent(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1test",
		ActiveStake:   "1000",
		BlockCnt:      2,
		Delegators:    3,
		MemberRewards: "123456789",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:        true,
		DelegatedStake:      "1000",
		DelegatorCount:      3,
		ParamsPresent:       true,
		BlocksProduced:      2,
		MemberRewardPresent: false,
	}

	// Historical: outside the grace window (or no grace configured) — a
	// genuine gap in Dingo's own computation.
	ms := ComparePoolEpoch("preview", 5, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.NotEqual(t, StatusPass, DetermineStatus(ms))

	// Recent: epoch closed within the grace window — may simply not be
	// computed yet, but still not a pass.
	recentClose := now.Add(-time.Hour)
	ms = ComparePoolEpoch("preview", 5, koios, dingo, now, 24, recentClose)
	require.Len(t, ms, 1)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryReferenceLag, ms[0].Category)
	require.NotEqual(t, StatusPass, DetermineStatus(ms))
}
