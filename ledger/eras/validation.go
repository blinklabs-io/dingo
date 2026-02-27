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

package eras

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrExUnitsOverflow is returned when ExUnits
// summation would overflow int64.
var ErrExUnitsOverflow = errors.New(
	"execution units overflow int64",
)

// SafeAddExUnits adds two ExUnits values with
// overflow detection. Returns an error if either
// the Memory or Steps sum would exceed
// math.MaxInt64.
func SafeAddExUnits(
	a, b lcommon.ExUnits,
) (lcommon.ExUnits, error) {
	if a.Memory < 0 || b.Memory < 0 {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: negative memory %d, %d",
			ErrExUnitsOverflow,
			a.Memory,
			b.Memory,
		)
	}
	if a.Steps < 0 || b.Steps < 0 {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: negative steps %d, %d",
			ErrExUnitsOverflow,
			a.Steps,
			b.Steps,
		)
	}
	if a.Memory > 0 && b.Memory > math.MaxInt64-a.Memory {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: memory %d + %d",
			ErrExUnitsOverflow,
			a.Memory,
			b.Memory,
		)
	}
	if a.Steps > 0 && b.Steps > math.MaxInt64-a.Steps {
		return lcommon.ExUnits{}, fmt.Errorf(
			"%w: steps %d + %d",
			ErrExUnitsOverflow,
			a.Steps,
			b.Steps,
		)
	}
	return lcommon.ExUnits{
		Memory: a.Memory + b.Memory,
		Steps:  a.Steps + b.Steps,
	}, nil
}

// txTypeAlonzo is the first era whose on-wire CBOR
// includes the 1-byte IsValid boolean field.
const txTypeAlonzo = 4

// TxSizeForFee computes the transaction size used in the
// Cardano fee formula. Per the Haskell ledger's
// toCBORForSizeComputation, this is the 3-element CBOR
// encoding [body, witnesses, auxiliary_data] — the
// IsValid boolean is excluded for backward compatibility
// with Mary-era transaction sizes. Since both 0x83 and
// 0x84 are 1-byte CBOR headers, the difference from the
// on-wire 4-element format is exactly the 1-byte IsValid
// field. Pre-Alonzo transactions (Byron through Mary) do
// not contain an IsValid byte, so their full CBOR length
// is the fee-relevant size.
func TxSizeForFee(tx lcommon.Transaction) uint64 {
	fullSize := uint64(len(tx.Cbor()))
	if fullSize > 0 && tx.Type() >= txTypeAlonzo {
		return fullSize - 1
	}
	return fullSize
}

// ValidateTxSize checks that the transaction size does
// not exceed the protocol parameter maximum.
func ValidateTxSize(
	tx lcommon.Transaction,
	maxTxSize uint,
) error {
	size := TxSizeForFee(tx)
	if size > uint64(maxTxSize) {
		return fmt.Errorf(
			"transaction size %d exceeds maximum %d",
			size,
			maxTxSize,
		)
	}
	return nil
}

// ValidateTxExUnits checks that total execution units
// do not exceed the protocol parameter per-transaction
// limits.
func ValidateTxExUnits(
	totalExUnits lcommon.ExUnits,
	maxTxExUnits lcommon.ExUnits,
) error {
	if totalExUnits.Memory > maxTxExUnits.Memory {
		return fmt.Errorf(
			"transaction memory %d exceeds maximum %d",
			totalExUnits.Memory,
			maxTxExUnits.Memory,
		)
	}
	if totalExUnits.Steps > maxTxExUnits.Steps {
		return fmt.Errorf(
			"transaction steps %d exceeds maximum %d",
			totalExUnits.Steps,
			maxTxExUnits.Steps,
		)
	}
	return nil
}

// CalculateMinFee computes the minimum fee for a
// transaction using the Cardano fee formula:
//
//	fee = (minFeeA * txSize) + minFeeB + scriptFee
//
// where (per Alonzo spec txscriptfee):
//
//	scriptFee = ceil(pricesMem * mem + pricesSteps * steps)
//
// Note: a single ceiling is applied over the sum of
// both components, NOT ceil of each added together.
// Script fee arithmetic uses big.Rat to prevent
// overflow and preserve exact rational arithmetic.
func CalculateMinFee(
	txSize uint64,
	exUnits lcommon.ExUnits,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) uint64 {
	// baseFee = minFeeA * txSize + minFeeB
	a := uint64(minFeeA)
	b := uint64(minFeeB)
	var baseFee uint64
	if a != 0 && txSize > (math.MaxUint64-b)/a {
		baseFee = math.MaxUint64
	} else {
		baseFee = a*txSize + b
	}

	var scriptFee uint64
	if pricesMem != nil && pricesSteps != nil {
		// Compute exact rational sum:
		// sum = pricesMem * mem + pricesSteps * steps
		memCost := new(big.Rat).Mul(
			pricesMem,
			new(big.Rat).SetInt64(exUnits.Memory),
		)
		stepCost := new(big.Rat).Mul(
			pricesSteps,
			new(big.Rat).SetInt64(exUnits.Steps),
		)
		sum := new(big.Rat).Add(memCost, stepCost)
		// ceil(sum) using integer arithmetic
		num := sum.Num()
		denom := sum.Denom()
		q, r := new(big.Int).DivMod(
			num,
			denom,
			new(big.Int),
		)
		if r.Sign() > 0 {
			q.Add(q, big.NewInt(1))
		}
		if !q.IsUint64() {
			scriptFee = math.MaxUint64
		} else {
			scriptFee = q.Uint64()
		}
	}

	total := baseFee + scriptFee
	if total < baseFee {
		return math.MaxUint64
	}
	return total
}

// DeclaredExUnits returns the total execution units
// declared across all redeemers in a transaction's
// witness set. These are the budgets the transaction
// builder committed to (not the evaluated actuals).
// Returns an error if the summation would overflow
// int64.
func DeclaredExUnits(
	tx lcommon.Transaction,
) (lcommon.ExUnits, error) {
	var total lcommon.ExUnits
	wits := tx.Witnesses()
	if wits == nil {
		return total, nil
	}
	redeemers := wits.Redeemers()
	if redeemers == nil {
		return total, nil
	}
	for _, val := range redeemers.Iter() {
		var err error
		total, err = SafeAddExUnits(total, val.ExUnits)
		if err != nil {
			return lcommon.ExUnits{}, fmt.Errorf(
				"summing redeemer execution units: %w",
				err,
			)
		}
	}
	return total, nil
}

// ValidateTxFee checks that the fee declared in the
// transaction body is at least the calculated minimum
// fee, including both the base fee component and the
// script execution fee component.
//
// The minimum fee formula (Alonzo+ eras), computed by
// CalculateMinFee:
//
//	minFee = (minFeeA * txSize) + minFeeB
//	       + ceil(pricesMem * totalMem
//	            + pricesSteps * totalSteps)
//
// Note: a single ceiling is applied over the combined
// script cost sum, not per-component.
//
// Returns nil if the declared fee is sufficient.
func ValidateTxFee(
	tx lcommon.Transaction,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) error {
	txSize := TxSizeForFee(tx)
	declaredEU, err := DeclaredExUnits(tx)
	if err != nil {
		return fmt.Errorf(
			"calculating declared execution units: %w",
			err,
		)
	}
	minFee := CalculateMinFee(
		txSize,
		declaredEU,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)
	txFee := tx.Fee()
	if txFee == nil {
		txFee = new(big.Int)
	}
	minFeeBig := new(big.Int).SetUint64(minFee)
	if txFee.Cmp(minFeeBig) >= 0 {
		return nil
	}
	return fmt.Errorf(
		"transaction fee %d is less than the calculated "+
			"minimum fee %d",
		txFee,
		minFeeBig,
	)
}
