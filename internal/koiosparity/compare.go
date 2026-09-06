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
	"slices"
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
	// CategoryPoolDeparted marks a pool that was in epoch K's stake basis but
	// is absent from the captured K+1 snapshot -- it left the pool set. Its
	// epoch-K block count lives on the K+1 reward_pool_input row, which
	// therefore never exists, so blocks_produced cannot be compared for that
	// one epoch. Purely informational, like the account lifecycle categories
	// below: both sides agree the pool departed, so it is a documented gap in
	// coverage rather than a divergence (dingo #3485).
	CategoryPoolDeparted = "pool_departed"

	// CategoryAcctOnlyDingo/CategoryAcctOnlyKoios mirror
	// CategoryPoolOnlyDingo/CategoryPoolOnlyKoios but at per-account
	// granularity (#3097) — a stake account (and specific reward type; see
	// CompareAccountEpoch) with a reward row on only one side.
	CategoryAcctOnlyDingo = "acct_only_dingo"
	CategoryAcctOnlyKoios = "acct_only_koios"
	// CategoryAcctDuplicate marks a genuine duplicate (stake_address,
	// reward_type) row within a single side (Koios reference data or
	// Dingo's committed reward_account_output) for one epoch — a
	// data-integrity problem in whichever side produced it, never folded
	// into CategoryValueMismatch.
	CategoryAcctDuplicate = "acct_duplicate"
	// CategoryAcctCoverageIncomplete marks an epoch whose Koios account-reward
	// fetch (FetchAccountRewardsForEpoch) never completed successfully across
	// every chunk of the requested address universe — see
	// KoiosAccountCoverage. Treated as ERROR (see DetermineStatus), never as
	// "nothing to compare": an incomplete reference set must never let an
	// epoch read as PASS just because every row that *was* fetched happened
	// to match.
	CategoryAcctCoverageIncomplete = "acct_coverage_incomplete"

	// CategoryAcctZeroReward/CategoryAcctNewlyRegistered/CategoryAcctDeregistered
	// (dingo #3099) report the three account dimensions #3097's merged
	// comparison structurally cannot: CompareAccountEpoch only ever compares
	// keys present in at least one side's row map, so an address absent from
	// both (a confirmed-zero-reward account, meaning Koios returned no rows
	// for it at all — distinct from Koios returning a row whose amount is
	// zero, which it does emit and which CategoryAcctZeroRewardRow covers)
	// never enters that comparison at all, and #3097's
	// address universe is a single flat list reused across every epoch in one
	// run, with no per-epoch persisted snapshot to diff for lifecycle
	// changes. All three are purely informational — descriptive state, not a
	// Dingo-vs-Koios discrepancy — and must never affect Status (see
	// DetermineStatus's dedicated no-op case for these three).
	CategoryAcctZeroReward = "acct_zero_reward"
	// CategoryAcctZeroRewardRow marks a reward row worth zero that exists on
	// only one side. It corrects the premise stated above: Koios does emit a
	// row for a zero reward — Preview publishes zero-earned leader rows —
	// while Dingo writes no reward_account_output row at all in that case.
	// Nothing is credited either way, so the two agree about every lovelace
	// and the one-sided row is a representational difference, not a
	// divergence. Purely informational, and reported rather than dropped so
	// the difference stays visible.
	CategoryAcctZeroRewardRow = "acct_zero_reward_row"
	// CategoryAcctNewlyRegistered marks a stake address present in this
	// stake epoch's Dingo-committed reward_account_output universe but
	// absent from the previous stake epoch's — see
	// check.go's accountLifecycleMismatches/dingoRewardAddressSet.
	CategoryAcctNewlyRegistered = "acct_newly_registered"
	// CategoryAcctDeregistered marks a stake address present in the previous
	// stake epoch's reward_account_output universe but absent from this
	// epoch's.
	CategoryAcctDeregistered = "acct_deregistered"
)

// AllCategories is every mismatch category above, in one place.
//
// severityOf classifies exactly these values, and the tests that guard the
// classification iterate this slice rather than restating it. A hand-written
// second copy is how a category comes to be classified by DetermineStatus and
// missed by CountSignificant (or the reverse) with every test still green, so
// the list exists once and TestAllCategoriesCoversEveryConstant pins it to the
// const block above.
var AllCategories = []string{
	CategoryValueMismatch,
	CategoryPoolOnlyDingo,
	CategoryPoolOnlyKoios,
	CategoryReferenceLag,
	CategoryDBError,
	CategoryDBMissing,
	CategoryPoolDeparted,
	CategoryAcctOnlyDingo,
	CategoryAcctOnlyKoios,
	CategoryAcctDuplicate,
	CategoryAcctCoverageIncomplete,
	CategoryAcctZeroReward,
	CategoryAcctZeroRewardRow,
	CategoryAcctNewlyRegistered,
	CategoryAcctDeregistered,
}

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
// and KoiosTotalsResp's doc comment. Dingo's reward_ada_pots.Fees is compared
// against its correct /totals.fees counterpart in CompareEpochTotals;
// reward_ada_pots.Rewards has no matching Koios per-epoch field.
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
	// aggregate; reward_ada_pots.Fees/Rewards are different quantities (see
	// KoiosTotalsResp's doc comment). reward_ada_pots.Fees is compared against
	// /totals.fees in CompareEpochTotals; Rewards remains explicitly
	// unsupported.

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
// — e.g. a cache created before totals fetching was added, or a --skip-fetch
// run against a cache that never fetched it. This is reported explicitly (as
// a "koios_totals" / CategoryDBMissing mismatch) rather than skipped: a
// missing reference row must never silently produce a PASS that in fact never
// validated treasury/reserves/fees at all. dingoEpoch is nil when
// epoch_summary isn't available yet; CompareEpochAggregates already reports
// that condition once (as "epoch_summary"), so this function still skips
// silently in that specific case rather than duplicating the same root cause
// under a second field name — the missing-dingoEpoch check runs first so a
// concurrently-missing koiosTotals doesn't also get double-reported.
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
	if dingoEpoch == nil {
		return nil
	}

	if koiosTotals == nil {
		return []CheckMismatch{{
			Network:    network,
			Epoch:      epoch,
			Field:      "koios_totals",
			DingoValue: "present",
			KoiosValue: "",
			Category:   CategoryDBMissing,
			CheckedAt:  now,
		}}
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
// departedAtParamEpoch reports whether this pool provably left the pool set by
// K+1 — either because its own retirement certificate had taken effect by the
// K+1 boundary and no later registration cancelled it, or because it is absent
// from a K+1 pool set already established as complete. Both are per-pool
// facts, which is the distinction that matters: epoch_summary.SnapshotReady is
// not one. The snapshot writer commits the epoch summary on every transition
// regardless of reward-input availability, and deliberately omits a degraded
// active pool from reward_pool_input while keeping it in the pool set. Those
// are missing input rather than departure, and an epoch-level flag would
// downgrade both. False whenever neither route could establish departure,
// which keeps the stricter classification (dingo #3485, #3925). See
// poolDepartedAtParamEpoch in check.go.
func ComparePoolEpoch(
	network string,
	epoch uint64,
	koiosPool *KoiosPoolEpoch,
	dingoPool *DingoPoolEpochData,
	now time.Time,
	graceHours int,
	epochEndTime time.Time,
	departedAtParamEpoch bool,
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

	// delegated_stake/delegator_count both come from reward_pool_input at the
	// "stake epoch" (K-1) — see DingoPoolEpochData's doc comment. That row
	// not existing yet is never a silent pass: a pool present only in the
	// param-epoch or output query (e.g. a freshly registered pool whose
	// stake-epoch row hasn't landed yet) would otherwise carry zero-value
	// DelegatedStake/DelegatorCount that compare as a real (and wrong) value
	// against Koios's actual figures. Within the grace window this may
	// simply not be captured yet (reference_lag); past it, it's a genuine
	// gap in Dingo's own computation (dingo_db_missing).
	if !dingoPool.StakePresent {
		cat := CategoryDBMissing
		if graceHours > 0 && !epochEndTime.IsZero() &&
			now.Sub(epochEndTime) < time.Duration(graceHours)*time.Hour {
			cat = CategoryReferenceLag
		}
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "reward_pool_input_stake",
			DingoValue: "",
			KoiosValue: "present",
			Category:   cat,
			CheckedAt:  now,
		})
	} else {
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

		// fixed_cost — reward_pool_input.cost vs Koios
		// pool_history.fixed_cost. A mark snapshot records the pool
		// parameters as of its own boundary, and those are the ones in force
		// for the epoch that snapshot is the basis for, so cost and margin
		// align with the stake epoch (K-1) rather than with blocks_produced
		// at the param epoch (dingo #3484).
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
		// Only guard on the Koios side being non-empty, matching fixed_cost
		// above: an empty dingoPool.Margin here means a corrupted/partial row
		// despite StakePresent being true (the "not ready yet" case is
		// already handled by the outer StakePresent check), so it must be
		// flagged as a mismatch, not silently skipped. rationalsEqual returns
		// false (not a panic) when given an empty string.
		if koiosPool.Margin != "" && !rationalsEqual(dingoPool.Margin, koiosPool.Margin) {
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
	}

	// blocks_produced comes from the reward_pool_input "param epoch" (K+1)
	// row — see DingoPoolEpochData's doc comment. That row
	// not existing yet is never a silent pass: within the grace window it may
	// simply not be captured yet (reference_lag); past it, it's a genuine gap
	// in Dingo's own computation (dingo_db_missing).
	if !dingoPool.ParamsPresent {
		cat := CategoryDBMissing
		switch {
		case dingoPool.StakePresent && departedAtParamEpoch:
			// The pool was in this epoch's stake basis and is absent from the
			// K+1 pool set itself, so it left the set. Its epoch-K block
			// count is stamped onto the K+1 row that will never be written,
			// so blocks_produced is not comparable for this one epoch.
			// Recorded rather than skipped, so the gap is visible, but
			// informational: both sides agree the pool departed. A pool still
			// in the K+1 pool set whose reward-input row is absent is missing
			// input, not a departure, and falls through to the cases below.
			cat = CategoryPoolDeparted
		case graceHours > 0 && !epochEndTime.IsZero() &&
			now.Sub(epochEndTime) < time.Duration(graceHours)*time.Hour:
			cat = CategoryReferenceLag
		}
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "reward_pool_input_params",
			DingoValue: "",
			KoiosValue: "present",
			Category:   cat,
			CheckedAt:  now,
		})
	} else {
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
	}

	// member_rewards — Dingo's spendable member reward sum vs Koios
	// pool_history.member_rewards. Both are a direct sum of the per-delegator
	// reward amounts actually credited (excluding the pool operator's own
	// leader/margin cut), so, unlike pool_fees/deleg_rewards below, this is
	// safe to compare 1:1.
	//
	// reward_pool_output.member_reward_total is deliberately not the Dingo
	// side of this comparison. It sums every member reward the calculation
	// produced, spendable or not, so it exceeds Koios's figure by exactly the
	// pool's unspendable member rewards — amounts computed for a credential
	// the ledger correctly never credits. Comparing it reported a
	// value_mismatch for any pool holding one, against a node that was right
	// (dingo #3797). The row's own unspendable column is not a usable
	// correction either, since it accumulates unspendable leader rewards too.
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
	//
	// A missing reward_pool_output row (MemberRewardPresent == false) is
	// never treated as "nothing to compare" when Koios has a value: within
	// the grace window it may simply not be computed yet (reference_lag,
	// ERROR); past it, it's a genuine gap in Dingo's own computation
	// (dingo_db_missing, ERROR). Neither case can produce a PASS.
	if koiosPool.MemberRewards != "" {
		switch {
		case !dingoPool.MemberRewardPresent,
			!dingoPool.SpendableMemberRewardPresent &&
				dingoPool.PoolUnspendable > 0:
			// Either the reward calculation has not produced a
			// reward_pool_output row for this pool/epoch, or it has and the
			// per-account rows the comparable sum is formed from are gone
			// while the row says something was withheld — so the two
			// quantities provably differ and the comparison cannot be formed.
			// Neither may read as a pass: within the grace window it may
			// simply not be computed yet (reference_lag, ERROR); past it, it
			// is a genuine gap in what Dingo can answer (dingo_db_missing,
			// ERROR).
			cat := CategoryDBMissing
			if graceHours > 0 && !epochEndTime.IsZero() &&
				now.Sub(epochEndTime) < time.Duration(graceHours)*time.Hour {
				cat = CategoryReferenceLag
			}
			out = append(out, CheckMismatch{
				Network:    network,
				Epoch:      epoch,
				PoolBech32: koiosPool.PoolBech32,
				Field:      "member_rewards",
				DingoValue: "",
				KoiosValue: koiosPool.MemberRewards,
				Category:   cat,
				CheckedAt:  now,
			})
		default:
			// Prefer the per-account sum. Falling back to the pool total is
			// only sound because PoolUnspendable is zero on this branch:
			// nothing was withheld, so the pool's member total is its
			// spendable member total by construction.
			dingoValue := dingoPool.SpendableMemberRewardTotal
			if !dingoPool.SpendableMemberRewardPresent {
				dingoValue = dingoPool.MemberRewardTotal
			}
			if dingoValue != koiosPool.MemberRewards {
				out = append(out, CheckMismatch{
					Network:    network,
					Epoch:      epoch,
					PoolBech32: koiosPool.PoolBech32,
					Field:      "member_rewards",
					DingoValue: dingoValue,
					KoiosValue: koiosPool.MemberRewards,
					Category:   CategoryValueMismatch,
					CheckedAt:  now,
				})
			}
		}
	}

	return out
}

// koiosAccountRewardTypesOutOfScope lists Koios /account_reward_history
// "type" enum values that Dingo's reward_account_output does not currently
// produce (MIR/refund distribution mechanisms — treasury/reserves MIR
// transfers and protocol-parameter-change refunds), per this issue's (#3097)
// explicit scope note. Rows of these types are filtered out of the
// comparison entirely — deliberately, not silently: they never contribute a
// koios-only mismatch, so a real Dingo gap in ordinary member/leader reward
// accounting is never masked by a flood of "missing" rows for a mechanism
// Dingo was never expected to track in the first place. If Dingo's ledger
// package ever starts tracking any of these (see
// ledger/reward_calculation.go/ledger/rewards/rewards.go), this map (and
// ARCHITECTURE.md's Koios Parity Tracker section) is the first place to
// update.
var koiosAccountRewardTypesOutOfScope = map[string]bool{
	"treasury": true,
	"reserves": true,
	"refund":   true,
}

// DingoAccountReward is a Dingo reward_account_output row reduced to the
// fields CompareAccountEpoch needs, with StakingKey/CredentialTag already
// resolved to a bech32 stake address by the caller (checkEpoch, via
// StakeAddressFromCredential) — CompareAccountEpoch itself never needs a
// network ID and never touches raw credential bytes.
type DingoAccountReward struct {
	StakeAddress string
	RewardType   string
	Amount       string // lovelace decimal string
}

// accountRewardKey identifies one (stake_address, reward_type) reward row
// within a single epoch — the granularity #3097 compares at, since one
// account can legitimately carry both a member and a leader row in the same
// epoch (a pool owner delegating to their own pool).
type accountRewardKey struct {
	address string
	rtype   string
}

// CompareAccountEpoch compares every Koios /account_reward_history reference
// row against Dingo's committed reward_account_output rows for one epoch,
// exactly — integer lovelace, no rounding/sampling/tolerance — per #3097's
// acceptance criteria. See ARCHITECTURE.md's Koios Parity Tracker
// "Per-account exact parity (#3097)" subsection for the full design.
//
// koiosRows/dingoRows are each scanned for internal duplicates first: the
// same (stake_address, reward_type) key appearing more than once within one
// side is a data-integrity problem in whichever side produced it (not a
// value disagreement), reported once per duplicate occurrence via
// CategoryAcctDuplicate. The first occurrence of a duplicated key is still
// kept for the union comparison below, so a real value mismatch on that key
// is not masked by the duplicate report.
//
// The union of the two (deduplicated) keysets is then walked: present only
// in Koios -> CategoryAcctOnlyKoios, present only in Dingo ->
// CategoryAcctOnlyDingo, present on both sides with differing amounts ->
// CategoryValueMismatch (compared as exact integers via lovelaceEqual, never
// as floats/rationals), present on both sides with equal amounts -> no
// mismatch. A zero-reward account present identically on both sides is
// therefore a pass, exactly like any other equal-amount case.
//
// Two things reclassify a one-sided row before it is emitted, in this order:
//
//   - The row is worth zero. The two sides then agree on what was credited
//     (nothing) and disagree only about whether to store a row saying so, so
//     it is CategoryAcctZeroRewardRow — informational — rather than either
//     acct_only_* category. This takes precedence over the grace window
//     below, because a zero row is not a value the other side can still
//     publish later.
//   - The check is inside graceHours of epochEndTime. Koios can lag in
//     publishing /account_reward_history for a just-closed epoch, and Dingo
//     can commit ahead of it, so a nonzero one-sided row is
//     CategoryReferenceLag until the window closes — mirroring
//     ComparePoolEpoch's identical pattern.
//
// graceHours/epochEndTime/now/network/epoch all mirror ComparePoolEpoch's
// identical parameters and meaning.
func CompareAccountEpoch(
	network string,
	epoch uint64,
	koiosRows []KoiosAccountRewards,
	dingoRows []DingoAccountReward,
	now time.Time,
	graceHours int,
	epochEndTime time.Time,
) []CheckMismatch {
	var out []CheckMismatch

	koiosByKey := make(map[accountRewardKey]KoiosAccountRewards, len(koiosRows))
	koiosSeen := make(map[accountRewardKey]int, len(koiosRows))
	for _, r := range koiosRows {
		if koiosAccountRewardTypesOutOfScope[r.RewardType] {
			continue
		}
		k := accountRewardKey{r.StakeAddress, r.RewardType}
		koiosSeen[k]++
		if koiosSeen[k] > 1 {
			out = append(out, CheckMismatch{
				Network:      network,
				Epoch:        epoch,
				StakeAddress: r.StakeAddress,
				Field:        "account_reward_duplicate",
				DingoValue:   "",
				KoiosValue: fmt.Sprintf(
					"reward_type=%s amount=%s duplicated in koios reference data (occurrence %d)",
					r.RewardType,
					r.Earned,
					koiosSeen[k],
				),
				Category:  CategoryAcctDuplicate,
				CheckedAt: now,
			})
			continue
		}
		koiosByKey[k] = r
	}

	dingoByKey := make(map[accountRewardKey]DingoAccountReward, len(dingoRows))
	dingoSeen := make(map[accountRewardKey]int, len(dingoRows))
	for _, r := range dingoRows {
		k := accountRewardKey{r.StakeAddress, r.RewardType}
		dingoSeen[k]++
		if dingoSeen[k] > 1 {
			out = append(out, CheckMismatch{
				Network:      network,
				Epoch:        epoch,
				StakeAddress: r.StakeAddress,
				Field:        "account_reward_duplicate",
				DingoValue: fmt.Sprintf(
					"reward_type=%s amount=%s duplicated in dingo committed state (occurrence %d)",
					r.RewardType,
					r.Amount,
					dingoSeen[k],
				),
				KoiosValue: "",
				Category:   CategoryAcctDuplicate,
				CheckedAt:  now,
			})
			continue
		}
		dingoByKey[k] = r
	}

	allKeys := make([]accountRewardKey, 0, len(koiosByKey)+len(dingoByKey))
	seenKey := make(map[accountRewardKey]bool, len(koiosByKey)+len(dingoByKey))
	for k := range koiosByKey {
		if !seenKey[k] {
			seenKey[k] = true
			allKeys = append(allKeys, k)
		}
	}
	for k := range dingoByKey {
		if !seenKey[k] {
			seenKey[k] = true
			allKeys = append(allKeys, k)
		}
	}
	// Deterministic ordering: report output/tests should not depend on Go's
	// randomised map iteration order.
	slices.SortFunc(allKeys, func(a, b accountRewardKey) int {
		if a.address != b.address {
			if a.address < b.address {
				return -1
			}
			return 1
		}
		switch {
		case a.rtype < b.rtype:
			return -1
		case a.rtype > b.rtype:
			return 1
		default:
			return 0
		}
	})

	for _, k := range allKeys {
		kr, koiosOK := koiosByKey[k]
		dr, dingoOK := dingoByKey[k]
		switch {
		case koiosOK && !dingoOK:
			cat := CategoryAcctOnlyKoios
			switch {
			case isZeroRewardAmount(kr.Earned):
				// Both sides credited nothing; see
				// CategoryAcctZeroRewardRow.
				cat = CategoryAcctZeroRewardRow
			case graceHours > 0 && !epochEndTime.IsZero() &&
				now.Sub(epochEndTime) < time.Duration(graceHours)*time.Hour:
				cat = CategoryReferenceLag
			}
			out = append(out, CheckMismatch{
				Network:      network,
				Epoch:        epoch,
				StakeAddress: k.address,
				Field:        "account_reward_presence",
				DingoValue:   "",
				KoiosValue: fmt.Sprintf(
					"%s (type=%s)",
					kr.Earned,
					kr.RewardType,
				),
				Category:  cat,
				CheckedAt: now,
			})
		case dingoOK && !koiosOK:
			// Symmetric with the koiosOK && !dingoOK case above: Koios can
			// lag in publishing /account_reward_history for a just-closed
			// epoch the same way it can lag on any other endpoint, so an
			// account Dingo has already committed a reward for but Koios
			// hasn't published yet within graceHours is reference lag, not
			// a real acct_only_dingo discrepancy.
			cat := CategoryAcctOnlyDingo
			switch {
			case isZeroRewardAmount(dr.Amount):
				// Symmetric with the koiosOK && !dingoOK case above.
				cat = CategoryAcctZeroRewardRow
			case graceHours > 0 && !epochEndTime.IsZero() &&
				now.Sub(epochEndTime) < time.Duration(graceHours)*time.Hour:
				cat = CategoryReferenceLag
			}
			out = append(out, CheckMismatch{
				Network:      network,
				Epoch:        epoch,
				StakeAddress: k.address,
				Field:        "account_reward_presence",
				DingoValue: fmt.Sprintf(
					"%s (type=%s)",
					dr.Amount,
					dr.RewardType,
				),
				KoiosValue: "",
				Category:   cat,
				CheckedAt:  now,
			})
		default:
			if !lovelaceEqual(dr.Amount, kr.Earned) {
				out = append(out, CheckMismatch{
					Network:      network,
					Epoch:        epoch,
					StakeAddress: k.address,
					Field:        "account_reward_amount",
					DingoValue:   dr.Amount,
					KoiosValue:   kr.Earned,
					Category:     CategoryValueMismatch,
					CheckedAt:    now,
				})
			}
		}
	}

	return out
}

// lovelaceEqual reports whether a and b represent the same non-negative
// integer lovelace amount, parsed exactly via big.Int — never as a float or
// rational — so #3097's "no rounding, sampling, or tolerance" requirement
// holds even for values exceeding float64's exact-integer range. An
// unparsable value on either side compares unequal (never silently equal),
// so a corrupt/malformed amount always surfaces as a mismatch rather than a
// false pass.
func lovelaceEqual(a, b string) bool {
	// Both values are parsed and validated (well-formed base-10 integer,
	// non-negative — lovelace amounts are never negative) before any
	// equality check, including the a == b case: a naive fast-path
	// string-equality short-circuit would report two identical malformed or
	// negative strings as "equal" without ever validating them, letting
	// CompareAccountEpoch pass on invalid account data.
	x, ok := parseLovelace(a)
	if !ok {
		return false
	}
	y, ok := parseLovelace(b)
	if !ok {
		return false
	}
	return x.Cmp(y) == 0
}

// parseLovelace is the single definition of a well-formed lovelace amount:
// one or more ASCII digits, nothing else. No sign, no surrounding whitespace,
// no separators.
//
// It exists so that lovelaceEqual and isZeroRewardAmount cannot disagree about
// what a string means. They read the same field from the same two sides, and a
// string one of them accepts while the other rejects gets two verdicts from the
// same input: with a divergent parse, " 0" was agreement when the row was
// one-sided and value_mismatch when both sides had one, and "+0" was the
// reverse. Whether a given malformed spelling ought to be tolerated is a
// separate question from whether the two paths answer it the same way; this
// answers the second, strictly, so a malformed amount is always reported and
// never waived.
//
// Deliberately stricter than big.Int.SetString alone, which accepts a leading
// sign: "-0" parses to zero with a non-negative sign, so a sign check does not
// exclude it, and a negative lovelace amount is malformed data rather than a
// zero reward. Leading zeros are accepted ("00" is zero) because the two sides
// format independently and a value's spelling is not the comparison's business.
func parseLovelace(s string) (*big.Int, bool) {
	if s == "" {
		return nil, false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return nil, false
		}
	}
	var v big.Int
	if _, ok := v.SetString(s, 10); !ok {
		return nil, false
	}
	return &v, true
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

// mismatchSeverity classifies one mismatch category. DetermineStatus and
// CountSignificant both read it, so a category added to one can never be
// forgotten by the other — the failure the split invited was a count that did
// not agree with the status it accompanied.
type mismatchSeverity int

const (
	// severityInformational describes state rather than disagreement. These
	// must never turn an otherwise-clean epoch into ERROR or FAIL, and must
	// never be counted as a reason for one.
	severityInformational mismatchSeverity = iota
	// severityError means the comparison could not be trusted, not that it
	// disagreed.
	severityError
	// severityFail is a real Dingo/Koios disagreement.
	severityFail
)

func severityOf(category string) mismatchSeverity {
	switch category {
	case CategoryDBError,
		CategoryDBMissing,
		CategoryReferenceLag,
		CategoryAcctCoverageIncomplete:
		return severityError
	case CategoryAcctZeroReward,
		CategoryAcctZeroRewardRow,
		CategoryAcctNewlyRegistered,
		CategoryAcctDeregistered,
		CategoryPoolDeparted:
		// Purely informational — see these categories' doc comments.
		return severityInformational
	default:
		return severityFail
	}
}

// CountSignificant returns how many mismatches drove the status DetermineStatus
// reports — every mismatch that is not purely informational.
//
// A caller reporting a failure should use this rather than len(mismatches).
// An epoch can hold many informational rows and still pass, so including them
// points the reader at rows that are by definition never the reason: Preview
// epoch 198 failed on three account mismatches and reported twelve, eight of
// which were pool departures that DetermineStatus ignores by design.
func CountSignificant(mismatches []CheckMismatch) int {
	n := 0
	for _, m := range mismatches {
		if severityOf(m.Category) != severityInformational {
			n++
		}
	}
	return n
}

// isZeroRewardAmount reports whether a lovelace decimal string is zero.
//
// Parsed rather than compared to "0": the two sides format independently, and
// a reward that is genuinely zero must be recognised as zero however it is
// spelled, so "00" is zero. An unparseable amount is not zero — it is a real
// value the comparison must keep reporting rather than quietly waive, so "",
// "abc", " 0" and "-0" all stay one-sided rows and keep failing the epoch.
//
// The parse is parseLovelace, the same one lovelaceEqual uses, so a string
// cannot be zero here and malformed there.
func isZeroRewardAmount(amount string) bool {
	v, ok := parseLovelace(amount)
	return ok && v.Sign() == 0
}

// DetermineStatus returns PASS, FAIL, or ERROR from a list of mismatches.
//
//   - FAIL: any value_mismatch, pool_only_dingo, pool_only_koios,
//     acct_only_dingo, acct_only_koios, or acct_duplicate entry — a strict
//     run cannot report PASS while any of these are present (#3097's
//     acceptance criteria: a single missing, extra, duplicate, or differing
//     account fails the whole epoch).
//   - ERROR: only DB-level failures (dingo_db_error, dingo_db_missing),
//     reference_lag (Koios data may be incomplete for a recent epoch), or
//     acct_coverage_incomplete (the Koios account-reward fetch for this
//     epoch never completed across every chunk, so there is no complete
//     reference set to compare against yet).
//   - PASS: no mismatches.
func DetermineStatus(mismatches []CheckMismatch) string {
	hasError := false
	for _, m := range mismatches {
		switch severityOf(m.Category) {
		case severityFail:
			return StatusFail
		case severityError:
			hasError = true
		case severityInformational:
		}
	}
	if hasError {
		return StatusError
	}
	return StatusPass
}
