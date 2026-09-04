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

	require.Empty(
		t,
		CompareEpochAggregates(
			"preview",
			10,
			koiosEpochInfo,
			dingo,
			nil,
			now,
			0,
		),
		"epoch_info.fees/total_rewards must not be compared against reward_ada_pots",
	)
	require.Empty(
		t,
		CompareEpochTotals("preview", 10, koiosTotals, dingo, now),
		"reward_ada_pots.Fees must match /totals.fees; totals.reward must not be compared at all",
	)
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

	// A missing /totals cache row (e.g. cached before totals fetching was
	// added, or a --skip-fetch run) must be reported explicitly, not skipped —
	// see TestCompareEpochTotalsMissingKoiosRow below for the full regression.
	ms = CompareEpochTotals("preview", 1367, nil, dingo, now)
	require.Len(t, ms, 1)
	require.Equal(t, "koios_totals", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)

	// A missing Dingo row is left to CompareEpochAggregates' "epoch_summary"
	// report; CompareEpochTotals must not duplicate it under a second field.
	require.Empty(t, CompareEpochTotals("preview", 1367, koios, nil, now))
}

// TestCompareEpochTotalsMissingKoiosRow guards against the false-PASS bug
// where a missing /totals cache row (koiosTotals == nil) was silently
// skipped instead of flagged. This happens for caches created before totals
// fetching was added, and for --skip-fetch runs against a cache that never
// fetched /totals — in both cases treasury/reserves/fees would never
// actually be validated, yet the epoch could still report PASS. A missing
// reference row must always surface as an explicit, non-PASS result.
func TestCompareEpochTotalsMissingKoiosRow(t *testing.T) {
	now := time.Now()
	dingo := &DingoEpochData{
		Treasury:             "6931231163186226",
		Reserves:             "7792082362166766",
		Fees:                 "1245791321",
		RewardAdaPotsPresent: true,
	}

	ms := CompareEpochTotals("preview", 1367, nil, dingo, now)
	require.Len(t, ms, 1)
	require.Equal(t, "koios_totals", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms),
		"a missing Koios /totals reference row must never resolve to PASS")
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
	require.Empty(
		t,
		ms,
		"totals_reward has no Dingo counterpart and must never be reported",
	)
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
	require.Empty(
		t,
		ComparePoolEpoch(
			"preview",
			5,
			koios,
			dingo,
			now,
			0,
			time.Time{},
			false,
		),
	)

	dingo.FixedCost = "340000001"
	ms := ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "fixed_cost", ms[0].Field)

	dingo.FixedCost = "340000000"
	dingo.Margin = "1/5"
	ms = ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "margin", ms[0].Field)
}

// TestComparePoolEpochEmptyDingoSideIsFlagged guards against reintroducing an
// asymmetry between the fixed_cost and margin guards: once StakePresent is
// true (the reward_pool_input row at the stake epoch genuinely exists — the
// "not ready yet" case is already handled by the outer StakePresent check),
// an unexpectedly empty dingoPool.FixedCost/Margin means a corrupted/partial
// row, not a legitimate skip condition, and must be reported as a
// value_mismatch like any other divergence rather than silently passed over.
// Both fields are read at the stake epoch (dingo #3484), so StakePresent, not
// ParamsPresent, is the flag that governs them.
func TestComparePoolEpochEmptyDingoSideIsFlagged(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "1000",
		BlockCnt:    2,
		Delegators:  3,
		FixedCost:   "340000000",
		Margin:      "0.1",
	}
	baseline := &DingoPoolEpochData{
		StakePresent:   true,
		DelegatedStake: "1000",
		ParamsPresent:  true,
		BlocksProduced: 2,
		DelegatorCount: 3,
		FixedCost:      "340000000",
		Margin:         "1/10",
	}

	dingo := *baseline
	dingo.FixedCost = ""
	ms := ComparePoolEpoch(
		"preview",
		5,
		koios,
		&dingo,
		now,
		0,
		time.Time{},
		false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "fixed_cost", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)

	dingo = *baseline
	dingo.Margin = ""
	ms = ComparePoolEpoch(
		"preview",
		5,
		koios,
		&dingo,
		now,
		0,
		time.Time{},
		false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "margin", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
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
	// FixedCost/Margin are stake-epoch fields (dingo #3484), so they are
	// present here and match Koios: the only thing missing is the
	// param-epoch row, and blocks_produced is the only field it still owns.
	dingo := &DingoPoolEpochData{
		StakePresent:   true,
		DelegatedStake: "1000",
		DelegatorCount: 3,
		FixedCost:      "340000000",
		Margin:         "1/10",
		ParamsPresent:  false,
	}

	// Historical (outside grace, or no grace configured): dingo_db_missing.
	ms := ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "reward_pool_input_params", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))

	// Recent (epoch closed within the grace window): reference_lag, not PASS.
	recentClose := now.Add(-time.Hour)
	ms = ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		24,
		recentClose,
		false,
	)
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
	ms := ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		false,
	)
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
	ms = ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		24,
		recentClose,
		false,
	)
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
	require.Empty(
		t,
		ComparePoolEpoch(
			"preview",
			5,
			koios,
			dingo,
			now,
			0,
			time.Time{},
			false,
		),
	)

	// Reward calculation not yet finished for this pool/epoch: Dingo has no
	// reward_pool_output row. This must NEVER read as a comparison pass —
	// see TestComparePoolEpochMemberRewardsNotPresent for the recent
	// (reference_lag) vs historical (dingo_db_missing) split this guards.
	dingo.MemberRewardPresent = false
	dingo.MemberRewardTotal = ""
	ms := ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		false,
	)
	require.Len(
		t,
		ms,
		1,
		"a missing reward_pool_output row must never be silently skipped",
	)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
	dingo.MemberRewardPresent = true

	dingo.MemberRewardTotal = "1"
	ms = ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		false,
	)
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
	ms := ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.NotEqual(t, StatusPass, DetermineStatus(ms))

	// Recent: epoch closed within the grace window — may simply not be
	// computed yet, but still not a pass.
	recentClose := now.Add(-time.Hour)
	ms = ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		24,
		recentClose,
		false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryReferenceLag, ms[0].Category)
	require.NotEqual(t, StatusPass, DetermineStatus(ms))
}

func TestCompareAccountEpochExactMatch(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "member", Earned: "1000000"},
	}
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1a", RewardType: "member", Amount: "1000000"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, dingo, now, 0, time.Time{})
	require.Empty(t, ms)
	require.Equal(t, StatusPass, DetermineStatus(ms))
}

func TestCompareAccountEpochZeroRewardBothSidesPasses(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "member", Earned: "0"},
	}
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1a", RewardType: "member", Amount: "0"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, dingo, now, 0, time.Time{})
	require.Empty(t, ms)
}

func TestCompareAccountEpochMissingFromDingo(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "member", Earned: "1000000"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, nil, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, CategoryAcctOnlyKoios, ms[0].Category)
	require.Equal(t, "stake1a", ms[0].StakeAddress)
	require.NotEqual(t, StatusPass, DetermineStatus(ms))
}

func TestCompareAccountEpochMissingFromKoios(t *testing.T) {
	now := time.Now()
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1a", RewardType: "member", Amount: "1000000"},
	}
	ms := CompareAccountEpoch("preview", 100, nil, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, CategoryAcctOnlyDingo, ms[0].Category)
	require.Equal(t, StatusFail, DetermineStatus(ms))
}

func TestCompareAccountEpochMissingFromDingoWithinGraceIsReferenceLag(
	t *testing.T,
) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "member", Earned: "1000000"},
	}
	recentClose := now.Add(-time.Hour)
	ms := CompareAccountEpoch("preview", 100, koios, nil, now, 24, recentClose)
	require.Len(t, ms, 1)
	require.Equal(t, CategoryReferenceLag, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}

// TestCompareAccountEpochMissingFromKoiosWithinGraceIsReferenceLag mirrors
// TestCompareAccountEpochMissingFromDingoWithinGraceIsReferenceLag for the
// symmetric direction: an account Dingo has already committed a reward for,
// but that Koios hasn't published /account_reward_history for yet, within
// graceHours of epochEndTime, must be reported as CategoryReferenceLag
// (StatusError) rather than CategoryAcctOnlyDingo (StatusFail) — Koios can
// lag in publishing account rewards for a just-closed epoch the same way it
// can lag on any other endpoint.
func TestCompareAccountEpochMissingFromKoiosWithinGraceIsReferenceLag(
	t *testing.T,
) {
	now := time.Now()
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1a", RewardType: "member", Amount: "1000000"},
	}
	recentClose := now.Add(-time.Hour)
	ms := CompareAccountEpoch("preview", 100, nil, dingo, now, 24, recentClose)
	require.Len(t, ms, 1)
	require.Equal(t, CategoryReferenceLag, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}

func TestCompareAccountEpochDuplicateInKoios(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "member", Earned: "1000000"},
		{StakeAddress: "stake1a", RewardType: "member", Earned: "1000000"},
	}
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1a", RewardType: "member", Amount: "1000000"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, CategoryAcctDuplicate, ms[0].Category)
	require.Equal(t, StatusFail, DetermineStatus(ms))
}

func TestCompareAccountEpochDuplicateInDingo(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "member", Earned: "1000000"},
	}
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1a", RewardType: "member", Amount: "1000000"},
		{StakeAddress: "stake1a", RewardType: "member", Amount: "1000000"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, CategoryAcctDuplicate, ms[0].Category)
	require.Equal(t, StatusFail, DetermineStatus(ms))
}

// TestCompareAccountEpochMemberAndLeaderIndependent proves an account with
// both a member and a leader row in the same epoch (a pool owner delegating
// to their own pool) is checked independently per reward type, not merged or
// summed — a mismatch on one type must not be masked by a match on the other.
func TestCompareAccountEpochMemberAndLeaderIndependent(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1owner", RewardType: "member", Earned: "1000000"},
		{StakeAddress: "stake1owner", RewardType: "leader", Earned: "5000000"},
	}
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1owner", RewardType: "member", Amount: "1000000"},
		// Leader amount differs by 1 lovelace.
		{StakeAddress: "stake1owner", RewardType: "leader", Amount: "5000001"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
	require.Equal(t, "5000001", ms[0].DingoValue)
	require.Equal(t, "5000000", ms[0].KoiosValue)
}

// TestCompareAccountEpochAmountMismatchByOneLovelace proves no tolerance:
// even a 1-lovelace difference is a real mismatch.
func TestCompareAccountEpochAmountMismatchByOneLovelace(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "member", Earned: "1000000"},
	}
	dingo := []DingoAccountReward{
		{StakeAddress: "stake1a", RewardType: "member", Amount: "1000001"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, dingo, now, 0, time.Time{})
	require.Len(t, ms, 1)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
	require.Equal(t, "account_reward_amount", ms[0].Field)
}

// TestCompareAccountEpochOutOfScopeRewardTypesFiltered proves treasury/
// reserves/refund Koios reward rows never surface as acct_only_koios, since
// Dingo's reward_account_output does not currently produce those types.
func TestCompareAccountEpochOutOfScopeRewardTypesFiltered(t *testing.T) {
	now := time.Now()
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake1a", RewardType: "treasury", Earned: "1000000"},
		{StakeAddress: "stake1a", RewardType: "reserves", Earned: "1000000"},
		{StakeAddress: "stake1a", RewardType: "refund", Earned: "1000000"},
	}
	ms := CompareAccountEpoch("preview", 100, koios, nil, now, 0, time.Time{})
	require.Empty(t, ms)
}

func TestLovelaceEqual(t *testing.T) {
	require.True(t, lovelaceEqual("1000000", "1000000"))
	require.True(t, lovelaceEqual("0", "0"))
	require.False(t, lovelaceEqual("1000000", "1000001"))
	require.False(t, lovelaceEqual("not-a-number", "1000000"))
	require.False(t, lovelaceEqual("1000000", "not-a-number"))

	// Numerically equal but textually different (leading zeros) must go
	// through the big.Int Cmp() path, not a string-equality short-circuit —
	// exercises the branch every other "equal" case above skips since they
	// use byte-identical strings.
	require.True(t, lovelaceEqual("01000000", "1000000"))
	require.True(t, lovelaceEqual("0", "00"))

	// A malformed or negative value must never compare equal to itself:
	// lovelace amounts are never negative, and an identical-string fast
	// path would otherwise report two invalid values as "equal" without
	// ever validating them.
	require.False(t, lovelaceEqual("not-a-number", "not-a-number"))
	require.False(t, lovelaceEqual("-5", "-5"))
	require.False(t, lovelaceEqual("-5", "5"))
}

// TestComparePoolEpochDepartedPoolIsInformational covers a pool that was in
// epoch K's stake basis but is absent from the committed K+1 snapshot: it left
// the pool set.
//
// blocks_produced for epoch K lives on the K+1 reward_pool_input row, which
// therefore never exists, so that one field cannot be compared. Both sides
// agree the pool departed — Koios has no pool_history row at K+1's reporting
// epoch either — so this is a documented gap in coverage, not a divergence,
// and it must not escalate to ERROR and halt a strict-mode node (dingo #3485).
func TestComparePoolEpochDepartedPoolIsInformational(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "1000",
		BlockCnt:    15,
		Delegators:  3,
		FixedCost:   "340000000",
		Margin:      "0.1",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:   true,
		DelegatedStake: "1000",
		DelegatorCount: 3,
		FixedCost:      "340000000",
		Margin:         "1/10",
		ParamsPresent:  false,
	}

	// The K+1 snapshot is committed, so the absent row means the pool left
	// the set rather than the snapshot being unwritten.
	ms := ComparePoolEpoch(
		"preview",
		5,
		koios,
		dingo,
		now,
		0,
		time.Time{},
		true,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "reward_pool_input_params", ms[0].Field)
	require.Equal(t, CategoryPoolDeparted, ms[0].Category)
	require.Equal(
		t,
		StatusPass,
		DetermineStatus(ms),
		"a departed pool must not fail or error the epoch",
	)

	// Same shape inside the grace window: still a departure, not lag.
	ms = ComparePoolEpoch(
		"preview", 5, koios, dingo, now, 24, now.Add(-time.Hour), true,
	)
	require.Len(t, ms, 1)
	require.Equal(t, CategoryPoolDeparted, ms[0].Category)
	require.Equal(t, StatusPass, DetermineStatus(ms))
}

// TestComparePoolEpochUncapturedParamEpochStillErrors is the negative case: an
// absent K+1 row with no committed K+1 snapshot is a genuine gap in Dingo's
// own computation and must keep escalating, so the departure classification
// cannot be widened into suppressing real missing data.
func TestComparePoolEpochUncapturedParamEpochStillErrors(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "1000",
		BlockCnt:    15,
		Delegators:  3,
		FixedCost:   "340000000",
		Margin:      "0.1",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:   true,
		DelegatedStake: "1000",
		DelegatorCount: 3,
		FixedCost:      "340000000",
		Margin:         "1/10",
		ParamsPresent:  false,
	}

	ms := ComparePoolEpoch(
		"preview", 5, koios, dingo, now, 0, time.Time{}, false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}

// TestComparePoolEpochDepartedRequiresStakeEpochRow proves the departure
// classification is anchored to the pool actually having been in epoch K's
// stake basis. A pool absent from both reads is not a departure and keeps its
// existing treatment.
func TestComparePoolEpochDepartedRequiresStakeEpochRow(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:  "pool1test",
		ActiveStake: "1000",
		BlockCnt:    15,
		Delegators:  3,
	}
	dingo := &DingoPoolEpochData{
		StakePresent:  false,
		ParamsPresent: false,
	}

	ms := ComparePoolEpoch(
		"preview", 5, koios, dingo, now, 0, time.Time{}, true,
	)
	for _, m := range ms {
		require.NotEqual(
			t,
			CategoryPoolDeparted,
			m.Category,
			"a pool with no stake-epoch row never departed epoch 5's basis",
		)
	}
	require.Equal(t, StatusError, DetermineStatus(ms))
}

// TestComparePoolEpochMemberRewardsExcludesUnspendable pins the quantity
// member_rewards is compared on. Koios reports the rewards members actually
// received; reward_pool_output.member_reward_total sums every member reward the
// calculation produced, spendable or not. A pool with an unspendable member
// reward — one computed for a credential the ledger correctly never credits —
// used to fail against a node that was right (dingo #3797).
func TestComparePoolEpochMemberRewardsExcludesUnspendable(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1test",
		ActiveStake:   "1000",
		BlockCnt:      2,
		Delegators:    3,
		FixedCost:     "340000000",
		Margin:        "0.1",
		MemberRewards: "327005332",
	}
	// The shape observed on Preview epoch 18: six spendable member rewards
	// summing to Koios's figure, plus one unspendable 71328 the pool total
	// still carries.
	dingo := &DingoPoolEpochData{
		StakePresent:                 true,
		DelegatedStake:               "1000",
		ParamsPresent:                true,
		BlocksProduced:               2,
		DelegatorCount:               3,
		FixedCost:                    "340000000",
		Margin:                       "1/10",
		MemberRewardPresent:          true,
		MemberRewardTotal:            "327076660",
		SpendableMemberRewardPresent: true,
		SpendableMemberRewardTotal:   "327005332",
	}
	require.Empty(t, ComparePoolEpoch(
		"preview", 5, koios, dingo, now, 0, time.Time{}, false,
	), "an unspendable member reward is not a divergence")

	// A real disagreement in the spendable sum still fails, and reports the
	// spendable figure rather than the pool total.
	dingo.SpendableMemberRewardTotal = "327005333"
	ms := ComparePoolEpoch(
		"preview", 5, koios, dingo, now, 0, time.Time{}, false,
	)
	require.Len(t, ms, 1)
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryValueMismatch, ms[0].Category)
	require.Equal(t, "327005333", ms[0].DingoValue)
}

// TestComparePoolEpochMemberRewardsWithoutAccountOutputs covers a node whose
// reward_account_output rows for the epoch are gone — cleanupOldSnapshots
// retains them without bound only in api storage mode.
//
// Falling back to reward_pool_output.member_reward_total is sound exactly when
// the row says nothing was withheld, because the pool's member total is then
// its spendable member total by construction. When something was withheld the
// two provably differ, so the field is reported as a missing row rather than
// compared on a basis that would fail a correct ledger.
func TestComparePoolEpochMemberRewardsWithoutAccountOutputs(t *testing.T) {
	now := time.Now()
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1test",
		ActiveStake:   "1000",
		BlockCnt:      2,
		Delegators:    3,
		FixedCost:     "340000000",
		Margin:        "0.1",
		MemberRewards: "327005332",
	}
	dingo := &DingoPoolEpochData{
		StakePresent:                 true,
		DelegatedStake:               "1000",
		ParamsPresent:                true,
		BlocksProduced:               2,
		DelegatorCount:               3,
		FixedCost:                    "340000000",
		Margin:                       "1/10",
		MemberRewardPresent:          true,
		MemberRewardTotal:            "327005332",
		SpendableMemberRewardPresent: false,
		PoolUnspendable:              0,
	}
	require.Empty(t, ComparePoolEpoch(
		"preview", 5, koios, dingo, now, 0, time.Time{}, false,
	), "with nothing withheld the pool total is the spendable total")

	// The same missing rows, but the pool withheld something, so the pool
	// total is known to overstate what members received.
	dingo.PoolUnspendable = 71328
	dingo.MemberRewardTotal = "327076660"
	ms := ComparePoolEpoch(
		"preview", 5, koios, dingo, now, 0, time.Time{}, false,
	)
	require.Len(t, ms, 1,
		"an unformable comparison must not read as a pass")
	require.Equal(t, "member_rewards", ms[0].Field)
	require.Equal(t, CategoryDBMissing, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}
