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
// against the Koios reference row for that epoch.
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

	// fees — only compared when Dingo has a reward_ada_pots row.
	if dingoEpoch.Fees != "" && dingoEpoch.Fees != koios.Fees {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "epoch_fees",
			DingoValue: dingoEpoch.Fees,
			KoiosValue: koios.Fees,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// pool_count
	dingoPoolCnt := strconv.FormatUint(dingoEpoch.PoolCount, 10)
	koiosPoolCnt := strconv.Itoa(koios.PoolCnt)
	if dingoPoolCnt != koiosPoolCnt {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "pool_count",
			DingoValue: dingoPoolCnt,
			KoiosValue: koiosPoolCnt,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// delegator_count
	dingoDelegCnt := strconv.FormatUint(dingoEpoch.DelegatorCount, 10)
	koiosDelegCnt := strconv.Itoa(koios.DelegatorCnt)
	if dingoDelegCnt != koiosDelegCnt {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "delegator_count",
			DingoValue: dingoDelegCnt,
			KoiosValue: koiosDelegCnt,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

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

	return out
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
