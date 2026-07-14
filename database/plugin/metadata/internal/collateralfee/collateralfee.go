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

// Package collateralfee computes the fee-pot contribution of phase-2-invalid
// transactions. The Alonzo/Babbage UTXOS rule consumes the collateral inputs
// of an isValid=false transaction (minus its collateral return output) into
// the fee pot, and that pot is snapshotted as feeSS for the delayed reward
// calculation, so omitting it diverges every downstream reward value.
package collateralfee

import (
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// PendingAmountFunc resolves the lovelace amount of a UTxO produced earlier
// in the current ingest batch but not yet flushed to the utxo table. It
// returns false when the reference is not an in-flight producer.
type PendingAmountFunc func(txId []byte, outputIdx uint32) (uint64, bool)

// ForTransaction returns the lovelace amount an isValid=false transaction
// adds to the fee pot: consumed collateral inputs minus the collateral
// return output. Valid transactions return 0.
//
// The declared total-collateral field is used when present because the
// Babbage UTXO rule requires it to equal the consumed balance; otherwise
// the collateral inputs are resolved from the utxo table (including
// already-spent rows) with pending as a fallback for same-batch producers.
//
// The boolean reports whether every collateral input was resolved. On false
// the returned amount is a lower bound: unresolved inputs count as zero,
// matching the node's lenient consumed-UTxO recovery below the Mithril
// trust boundary.
func ForTransaction(
	db *gorm.DB,
	tx lcommon.Transaction,
	pending PendingAmountFunc,
) (uint64, bool, error) {
	if tx.IsValid() {
		return 0, true, nil
	}
	if tc := tx.TotalCollateral(); tc != nil && tc.Sign() > 0 {
		if !tc.IsUint64() {
			return 0, false, fmt.Errorf(
				"total collateral %s exceeds uint64 range", tc.String(),
			)
		}
		return tc.Uint64(), true, nil
	}
	collateral := tx.Collateral()
	if len(collateral) == 0 {
		return 0, true, nil
	}
	var sum uint64
	resolvedAll := true
	seen := make(map[string]struct{}, len(collateral))
	for _, input := range collateral {
		inputTxId := input.Id()
		key := fmt.Sprintf("%x:%d", inputTxId[:], input.Index())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		amount, ok, err := utxoAmount(db, inputTxId[:], input.Index())
		if err != nil {
			return 0, false, fmt.Errorf(
				"resolve collateral input %s: %w", input.String(), err,
			)
		}
		if !ok && pending != nil {
			amount, ok = pending(inputTxId[:], input.Index())
		}
		if !ok {
			resolvedAll = false
			continue
		}
		if amount > math.MaxUint64-sum {
			return 0, false, errors.New("collateral input sum overflow")
		}
		sum += amount
	}
	if ret := tx.CollateralReturn(); ret != nil {
		retAmount := ret.Amount()
		if retAmount != nil && retAmount.Sign() > 0 {
			if !retAmount.IsUint64() || retAmount.Uint64() > sum {
				return 0, false, nil
			}
			sum -= retAmount.Uint64()
		}
	}
	return sum, resolvedAll, nil
}

func utxoAmount(
	db *gorm.DB,
	txId []byte,
	outputIdx uint32,
) (uint64, bool, error) {
	var row struct{ Amount types.Uint64 }
	result := db.Model(&models.Utxo{}).
		Select("amount").
		Where("tx_id = ? AND output_idx = ?", txId, outputIdx).
		Take(&row)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return 0, false, nil
		}
		return 0, false, result.Error
	}
	return uint64(row.Amount), true, nil
}
