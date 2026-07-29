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
	"fmt"
	"math/big"
	"strconv"
	"time"
)

// Mismatch categories.
const (
	CategoryValueMismatch = "value_mismatch"
	CategoryPoolOnlyDingo = "pool_only_dingo"
	CategoryPoolOnlyKoios = "pool_only_koios"
	CategoryReferenceLag  = "reference_lag"
	CategoryDBError       = "dingo_db_error"   // DB query returned an unexpected error
	CategoryDBMissing     = "dingo_db_missing" // expected DB row is absent
)

// Epoch check status values.
const (
	StatusPass  = "PASS"
	StatusFail  = "FAIL"
	StatusError = "ERROR"
)

// EpochCompareResult holds the comparison outcome for one epoch.
type EpochCompareResult struct {
	Network        string
	Epoch          uint64
	Status         string
	Mismatches     []CheckMismatch
	DingoPoolCount int
	KoiosPoolCount int
	OnlyDingo      []string
	OnlyKoios      []string
}

// CompareEpochAggregates compares epoch-level fields from Dingo's database
// against the Koios /epoch_info reference row for that epoch.
// Only total_active_stake is compared here — koios.Fees and koios.TotalRewards
// (/epoch_info.fees and /epoch_info.total_rewards) are raw block/tx accounting
// quantities that have no corresponding Dingo aggregate; see the comment below
// and KoiosTotalsResp's doc comment. The AdaPots pot values Dingo does track
// (reward_ada_pots.Fees/Rewards) are compared against their correct Koios
// counterpart, /totals, in CompareEpochTotals instead.
// dingoEpoch may be nil when the epoch_summary row is absent (not yet computed).
// fetchErr is set when the DB query itself failed.
// graceHours: if the Koios row was fetched within this many hours and Dingo's
// row is missing, emit reference_lag (ERROR) instead of dingo_db_missing (ERROR)
// so operators don't mistake an in-progress sync for a real discrepancy.
func CompareEpochAggregates(
	network string,
	epoch uint64,
	koios *KoiosEpochInfo,
	dingoEpoch *DingoEpochData,
	fetchErr error,
	now time.Time,
	graceHours int,
) []CheckMismatch {
	var out []CheckMismatch

	if fetchErr != nil {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "epoch_summary",
			DingoValue: fmt.Sprintf("error: %v", fetchErr),
			KoiosValue: "",
			Category:   CategoryDBError,
			CheckedAt:  now,
		})
		return out
	}

	if dingoEpoch == nil {
		cat := CategoryDBMissing
		if graceHours > 0 && koios != nil && !koios.EpochEndTime.IsZero() &&
			now.Sub(koios.EpochEndTime) < time.Duration(graceHours)*time.Hour {
			cat = CategoryReferenceLag
		}
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "epoch_summary",
			DingoValue: "",
			KoiosValue: "present",
			Category:   cat,
			CheckedAt:  now,
		})
		return out
	}

	// total_active_stake
	if dingoEpoch.TotalActiveStake != koios.ActiveStake {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "total_active_stake",
			DingoValue: dingoEpoch.TotalActiveStake,
			KoiosValue: koios.ActiveStake,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// koios.Fees (/epoch_info.fees) and koios.TotalRewards (/epoch_info.total_rewards)
	// are deliberately NOT compared here. Both are raw block/tx accounting
	// quantities (the sum of transaction fees for txs included in that epoch's
	// blocks, and total rewards earned in the epoch) — Dingo has no matching
	// aggregate; reward_ada_pots.Fees/Rewards are AdaPots *pot* values (balances
	// at the epoch boundary), a different quantity entirely (see
	// KoiosTotalsResp's doc comment). reward_ada_pots is compared against its
	// correct Koios counterpart, /totals.fees and /totals.reward, in
	// CompareEpochTotals instead.

	return out
}

// CompareEpochTotals compares Koios /totals fields against Dingo's database
// for the given epoch.
//
// This is independent of CompareEpochAggregates, which compares /epoch_info
// fields. /totals and /epoch_info share a "fees" field name (and "reward" /
// "total_rewards" name similarly) that are NOT the same quantity — see the
// KoiosTotalsResp doc comment for the distinction, confirmed empirically
// against a live preview node. Field names below are prefixed "totals_" so a
// mismatch report never conflates a /totals discrepancy with an /epoch_info
// one, even though both may ultimately compare against the same Dingo
// reward_ada_pots column.
//
// totals.fees and totals.treasury/reserves are single point-in-time pot
// balances, matching what Dingo already stores per epoch (Fees is that
// epoch's fee-pot snapshot; Treasury/Reserves are running ledger balances
// Dingo already carries forward — see ledger/reward_calculation.go's
// NetworkState updates), so those three are compared directly. totals.reward
// is different in kind — a cumulative accumulator, not a snapshot — and is
// not compared at all; see the comment after the totals_fees check below.
//
// koiosTotals is nil when no /totals row has been cached for this epoch yet
// (e.g. cached before totals fetching was added) — comparison is skipped
// rather than flagged, since that is a reference-data gap, not a Dingo/Koios
// disagreement. dingoEpoch is nil when epoch_summary isn't available yet;
// CompareEpochAggregates already reports that condition once (as
// "epoch_summary"), so this function skips silently rather than duplicating
// the same root cause under a second field name.
//
// dingoEpoch.RewardAdaPotsPresent == false is a distinct condition from
// dingoEpoch == nil: epoch_summary is ready but reward_ada_pots (treasury/
// reserves/fees) never got written for this epoch — see
// DingoEpochData.RewardAdaPotsPresent's doc comment for how this happens
// (bootstrap import). Unlike a merely-empty value string, this is reported
// explicitly rather than having the "!= \"\"" value guards below silently
// treat "missing" the same as "nothing to compare".
func CompareEpochTotals(
	network string,
	epoch uint64,
	koiosTotals *KoiosTotals,
	dingoEpoch *DingoEpochData,
	now time.Time,
) []CheckMismatch {
	if koiosTotals == nil || dingoEpoch == nil {
		return nil
	}

	if !dingoEpoch.RewardAdaPotsPresent {
		return []CheckMismatch{{
			Network:    network,
			Epoch:      epoch,
			Field:      "reward_ada_pots",
			DingoValue: "",
			KoiosValue: "present",
			Category:   CategoryDBMissing,
			CheckedAt:  now,
		}}
	}

	var out []CheckMismatch

	// totals_treasury
	if dingoEpoch.Treasury != koiosTotals.Treasury {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "totals_treasury",
			DingoValue: dingoEpoch.Treasury,
			KoiosValue: koiosTotals.Treasury,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// totals_reserves
	if dingoEpoch.Reserves != koiosTotals.Reserves {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "totals_reserves",
			DingoValue: dingoEpoch.Reserves,
			KoiosValue: koiosTotals.Reserves,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// totals_fees — reward_ada_pots.Fees vs Koios totals.fees (the fee-pot
	// value), independent of the totals.fees vs epoch_info.fees comparison in
	// CompareEpochAggregates.
	if dingoEpoch.Fees != koiosTotals.Fees {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "totals_fees",
			DingoValue: dingoEpoch.Fees,
			KoiosValue: koiosTotals.Fees,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// totals.reward is deliberately NOT compared: verified against live preview
	// data, it is a monotonically increasing cumulative accumulator (flat
	// through epoch 11, then totals.reward(12) - totals.reward(11) =
	// 13101661554 — an exact match to epoch_info.total_rewards for epoch 10,
	// not 11 or 12, i.e. a 2-epoch-lagged running sum), not a per-epoch pot
	// snapshot the way totals.treasury/reserves/fees are. Dingo has no stored
	// aggregate matching that cumulative quantity — reward_ada_pots.Rewards is
	// a fresh per-epoch flow value, overwritten every epoch (see
	// DingoEpochData.TotalRewards). This checker does not derive one on
	// Dingo's behalf by summing across epochs itself: a missing aggregate is a
	// Dingo data-model gap to fix at the source (see epoch10-koios-parity-issue.md,
	// Finding 3), not something for the parity checker to compute and paper over.

	return out
}

// ComparePoolEpoch compares per-pool reward-input fields from Dingo's database
// against the Koios reference row for (pool, epoch).
// dingoPool is nil when the pool has no reward_pool_input row for this epoch.
// epochEndTime is the actual epoch close time (from KoiosEpochInfo.EpochEndTime);
// zero means unknown. graceHours: if the epoch closed within this many hours and
// Dingo has no reward_pool_input row, emit reference_lag instead of pool_only_koios.
func ComparePoolEpoch(
	network string,
	epoch uint64,
	koiosPool *KoiosPoolEpoch,
	dingoPool *DingoPoolEpochData,
	now time.Time,
	graceHours int,
	epochEndTime time.Time,
) []CheckMismatch {
	var out []CheckMismatch

	if dingoPool == nil {
		// Pool known to Koios but has no reward_pool_input row in Dingo.
		// Within the grace window the absence may mean Dingo hasn't finished
		// computing rewards for this epoch yet — flag as reference_lag, not FAIL.
		cat := CategoryPoolOnlyKoios
		if graceHours > 0 && !epochEndTime.IsZero() &&
			now.Sub(epochEndTime) < time.Duration(graceHours)*time.Hour {
			cat = CategoryReferenceLag
		}
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "pool_presence",
			DingoValue: "",
			KoiosValue: "present",
			Category:   cat,
			CheckedAt:  now,
		})
		return out
	}

	// delegated_stake
	if dingoPool.DelegatedStake != koiosPool.ActiveStake {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "delegated_stake",
			DingoValue: dingoPool.DelegatedStake,
			KoiosValue: koiosPool.ActiveStake,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// delegator_count
	dingoDelegStr := strconv.FormatUint(dingoPool.DelegatorCount, 10)
	koiosDelegStr := strconv.Itoa(koiosPool.Delegators)
	if dingoDelegStr != koiosDelegStr {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "delegator_count",
			DingoValue: dingoDelegStr,
			KoiosValue: koiosDelegStr,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// blocks_produced
	dingoBlockStr := strconv.FormatUint(dingoPool.BlocksProduced, 10)
	koiosBlockStr := strconv.Itoa(koiosPool.BlockCnt)
	if dingoBlockStr != koiosBlockStr {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "blocks_produced",
			DingoValue: dingoBlockStr,
			KoiosValue: koiosBlockStr,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// fixed_cost — reward_pool_input.cost vs Koios pool_history.fixed_cost.
	if koiosPool.FixedCost != "" && dingoPool.FixedCost != koiosPool.FixedCost {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "fixed_cost",
			DingoValue: dingoPool.FixedCost,
			KoiosValue: koiosPool.FixedCost,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// margin — compare as rationals so Koios "0.1" matches Dingo "1/10".
	if koiosPool.Margin != "" && dingoPool.Margin != "" && !rationalsEqual(dingoPool.Margin, koiosPool.Margin) {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "margin",
			DingoValue: dingoPool.Margin,
			KoiosValue: koiosPool.Margin,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// member_rewards — reward_pool_output.member_reward_total vs Koios
	// pool_history.member_rewards. Both are a direct sum of per-delegator
	// reward amounts (excluding the pool operator's own leader/margin cut),
	// so, unlike pool_fees/deleg_rewards below, this is safe to compare 1:1.
	//
	// pool_fees and deleg_rewards are intentionally NOT compared against
	// Dingo's LeaderReward/TotalReward: Koios's grest.get_pool_history_data_bulk
	// recomputes pool_fees from fixed_cost+margin alone
	// (https://github.com/cardano-community/koios-artifacts, files/grest/rpc/
	// 00_cached_tables/pool_history_cache.sql), which omits the pledge/owner-
	// stake bonus term the Shelley ledger spec folds into the true leader
	// reward. That recomputed value systematically diverges from
	// RewardPoolOutput.LeaderReward for any pool with owner stake, and
	// deleg_rewards is derived from that same approximation plus a ROUND()
	// on each side, so their difference isn't even guaranteed to cancel out
	// to the exact lovelace. Comparing either would produce mismatches that
	// reflect Koios's own reporting approximation, not a real Dingo bug.
	if koiosPool.MemberRewards != "" && dingoPool.MemberRewardTotal != "" &&
		dingoPool.MemberRewardTotal != koiosPool.MemberRewards {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "member_rewards",
			DingoValue: dingoPool.MemberRewardTotal,
			KoiosValue: koiosPool.MemberRewards,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	return out
}

// rationalsEqual reports whether two numeric strings represent the same
// rational (e.g. "0.1" and "1/10"). Returns false if either fails to parse.
func rationalsEqual(a, b string) bool {
	var ra, rb big.Rat
	if _, ok := ra.SetString(a); !ok {
		return false
	}
	if _, ok := rb.SetString(b); !ok {
		return false
	}
	return ra.Cmp(&rb) == 0
}

// DetermineStatus returns PASS, FAIL, or ERROR from a list of mismatches.
//
//   - FAIL: any value_mismatch, pool_only_dingo, or pool_only_koios entry.
//   - ERROR: only DB-level failures (dingo_db_error, dingo_db_missing) or
//     reference_lag (Koios data may be incomplete for a recent epoch).
//   - PASS: no mismatches.
func DetermineStatus(mismatches []CheckMismatch) string {
	if len(mismatches) == 0 {
		return StatusPass
	}
	hasError := false
	for _, m := range mismatches {
		switch m.Category {
		case CategoryDBError, CategoryDBMissing, CategoryReferenceLag:
			hasError = true
		default:
			return StatusFail
		}
	}
	if hasError {
		return StatusError
	}
	return StatusPass
}
