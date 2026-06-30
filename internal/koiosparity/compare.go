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
	"errors"
	"fmt"
	"time"
)

// Mismatch categories.
const (
	CategoryValueMismatch   = "value_mismatch"
	CategoryPoolOnlyDingo   = "pool_only_dingo"
	CategoryPoolOnlyKoios   = "pool_only_koios"
	CategoryReferenceLag    = "reference_lag"
	CategoryDingoAPIMissing = "dingo_api_missing"
	CategoryDingoAPIError   = "dingo_api_error"
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

// CompareEpochAggregates compares epoch-level fields from Dingo vs Koios.
// It records ErrAPINotFound → CategoryDingoAPIMissing, 5xx → CategoryDingoAPIError.
func CompareEpochAggregates(
	network string,
	epoch uint64,
	koios *KoiosEpochInfo,
	dingoEpoch *DingoEpochResp,
	fetchErr error,
	now time.Time,
) []CheckMismatch {
	var out []CheckMismatch

	if fetchErr != nil {
		cat := CategoryDingoAPIError
		if errors.Is(fetchErr, ErrAPINotFound) {
			cat = CategoryDingoAPIMissing
		}
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "epoch_info",
			DingoValue: fmt.Sprintf("error: %v", fetchErr),
			KoiosValue: "",
			Category:   cat,
			CheckedAt:  now,
		})
		return out
	}

	// active_stake
	if dingoEpoch.ActiveStake != nil {
		if *dingoEpoch.ActiveStake != koios.ActiveStake {
			out = append(out, CheckMismatch{
				Network:    network,
				Epoch:      epoch,
				Field:      "total_active_stake",
				DingoValue: *dingoEpoch.ActiveStake,
				KoiosValue: koios.ActiveStake,
				Category:   CategoryValueMismatch,
				CheckedAt:  now,
			})
		}
	}

	// fees
	if dingoEpoch.Fees != "" && koios.Fees != "" {
		if dingoEpoch.Fees != koios.Fees {
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
	}

	return out
}

// ComparePoolEpoch compares per-pool fields from Dingo vs Koios for one pool.
// dingoItem may be nil (pool not in Dingo) or fetchErr non-nil (API failure).
func ComparePoolEpoch(
	network string,
	epoch uint64,
	koiosPool *KoiosPoolEpoch,
	dingoItem *DingoPoolHistoryItem,
	fetchErr error,
	now time.Time,
) []CheckMismatch {
	var out []CheckMismatch

	if fetchErr != nil {
		cat := CategoryDingoAPIError
		if errors.Is(fetchErr, ErrAPINotFound) {
			cat = CategoryDingoAPIMissing
		}
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "pool_history",
			DingoValue: fmt.Sprintf("error: %v", fetchErr),
			KoiosValue: "",
			Category:   cat,
			CheckedAt:  now,
		})
		return out
	}

	if dingoItem == nil {
		// Pool known to Koios but absent from Dingo.
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "pool_presence",
			DingoValue: "",
			KoiosValue: "present",
			Category:   CategoryPoolOnlyKoios,
			CheckedAt:  now,
		})
		return out
	}

	// delegated_stake
	if dingoItem.ActiveStake != koiosPool.ActiveStake {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "delegated_stake",
			DingoValue: dingoItem.ActiveStake,
			KoiosValue: koiosPool.ActiveStake,
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// delegator_count
	if dingoItem.Delegators != koiosPool.Delegators {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "delegator_count",
			DingoValue: fmt.Sprintf("%d", dingoItem.Delegators),
			KoiosValue: fmt.Sprintf("%d", koiosPool.Delegators),
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	// blocks_produced
	if dingoItem.Blocks != koiosPool.BlockCnt {
		out = append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			PoolBech32: koiosPool.PoolBech32,
			Field:      "blocks_produced",
			DingoValue: fmt.Sprintf("%d", dingoItem.Blocks),
			KoiosValue: fmt.Sprintf("%d", koiosPool.BlockCnt),
			Category:   CategoryValueMismatch,
			CheckedAt:  now,
		})
	}

	return out
}

// DetermineStatus returns PASS, FAIL, or ERROR from a list of mismatches.
//
//   - FAIL: any value_mismatch, pool_only_dingo, or pool_only_koios entry.
//   - ERROR: only API-level failures (dingo_api_missing, dingo_api_error) or
//     reference_lag (Koios data may be incomplete for a recent epoch).
//   - PASS: no mismatches.
func DetermineStatus(mismatches []CheckMismatch) string {
	if len(mismatches) == 0 {
		return StatusPass
	}
	hasError := false
	for _, m := range mismatches {
		switch m.Category {
		case CategoryDingoAPIError, CategoryDingoAPIMissing, CategoryReferenceLag:
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
