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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// TxSizeForFee computes the transaction size used in
// the Cardano fee formula. See eras.TxSizeForFee.
func TxSizeForFee(tx lcommon.Transaction) uint64 {
	return eras.TxSizeForFee(tx)
}

// TxBodySize is a deprecated alias for TxSizeForFee.
func TxBodySize(tx lcommon.Transaction) uint64 {
	return eras.TxSizeForFee(tx)
}

// ValidateTxSize checks that the transaction size does
// not exceed the protocol parameter maximum.
func ValidateTxSize(
	tx lcommon.Transaction,
	maxTxSize uint,
) error {
	return eras.ValidateTxSize(tx, maxTxSize)
}

// ValidateTxExUnits checks that total execution units
// do not exceed the protocol parameter per-transaction
// limits.
func ValidateTxExUnits(
	totalExUnits lcommon.ExUnits,
	maxTxExUnits lcommon.ExUnits,
) error {
	return eras.ValidateTxExUnits(
		totalExUnits,
		maxTxExUnits,
	)
}

// CalculateMinFee computes the minimum fee for a
// transaction using the Cardano fee formula:
//
//	fee = (minFeeA * txSize) + minFeeB + scriptFee
//
// where:
//
//	scriptFee = ceil(pricesMem * exUnits.Memory)
//	          + ceil(pricesSteps * exUnits.Steps)
//
// All arithmetic uses big.Int to match the Haskell
// reference implementation. Overflow is impossible
// with Cardano protocol parameters.
func CalculateMinFee(
	txSize uint64,
	exUnits lcommon.ExUnits,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) uint64 {
	return eras.CalculateMinFee(
		txSize,
		exUnits,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)
}

// DeclaredExUnits returns the total execution units
// declared across all redeemers in a transaction's
// witness set. Returns an error if the summation
// would overflow int64.
func DeclaredExUnits(
	tx lcommon.Transaction,
) (lcommon.ExUnits, error) {
	return eras.DeclaredExUnits(tx)
}

// ValidateTxFee checks that the fee declared in the
// transaction body is at least the calculated minimum
// fee, including both the base fee component and the
// script execution fee component.
func ValidateTxFee(
	tx lcommon.Transaction,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) error {
	return eras.ValidateTxFee(
		tx,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)
}

// IsCompatibleEra checks if a transaction era is valid
// for the current ledger era. Cardano allows transactions
// from the current era and (current era - 1).
func IsCompatibleEra(txEraId, ledgerEraId uint) bool {
	return eras.IsCompatibleEra(txEraId, ledgerEraId)
}

// ValidateTxEra checks that a transaction's era is
// compatible with the current ledger era. Returns an
// error if the transaction era is not the current era
// or the immediately previous era.
func ValidateTxEra(
	tx lcommon.Transaction,
	ledgerEraId uint,
) error {
	txEraId := uint(tx.Type()) // #nosec G115 -- era IDs are non-negative
	if !eras.IsCompatibleEra(txEraId, ledgerEraId) {
		txEra := eras.GetEraById(txEraId)
		ledgerEra := eras.GetEraById(ledgerEraId)
		txEraName := fmt.Sprintf("unknown(%d)", txEraId)
		ledgerEraName := fmt.Sprintf("unknown(%d)", ledgerEraId)
		if txEra != nil {
			txEraName = txEra.Name
		}
		if ledgerEra != nil {
			ledgerEraName = ledgerEra.Name
		}
		return fmt.Errorf(
			"transaction era %s is not compatible with ledger era %s",
			txEraName,
			ledgerEraName,
		)
	}
	return nil
}
