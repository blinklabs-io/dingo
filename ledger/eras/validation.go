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

// errFeeOverflow is returned when fee arithmetic
// overflows uint64.
var errFeeOverflow = errors.New("fee calculation overflow")

// errZeroDenominator is returned when CeilMul receives
// a zero denominator.
var errZeroDenominator = errors.New(
	"CeilMul: zero denominator",
)

// TxBodySize returns the CBOR-serialized size of a
// transaction in bytes.
func TxBodySize(tx lcommon.Transaction) uint64 {
	return uint64(len(tx.Cbor()))
}

// ValidateTxSize checks that the transaction size does
// not exceed the protocol parameter maximum.
func ValidateTxSize(
	tx lcommon.Transaction,
	maxTxSize uint,
) error {
	size := TxBodySize(tx)
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

// CeilMul computes ceil(rat * value) using integer
// arithmetic. The rational is represented as num/denom.
// This avoids floating-point imprecision. Returns an
// error if denom is zero.
func CeilMul(
	num, denom, value *big.Int,
) (uint64, error) {
	if denom.Sign() == 0 {
		return 0, errZeroDenominator
	}
	// product = num * value
	product := new(big.Int).Mul(num, value)
	// ceil(product / denom) = (product + denom - 1) / denom
	one := big.NewInt(1)
	denomMinusOne := new(big.Int).Sub(denom, one)
	product.Add(product, denomMinusOne)
	product.Div(product, denom)
	if !product.IsUint64() {
		return 0, errFeeOverflow
	}
	return product.Uint64(), nil
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
// All arithmetic uses integer math (big.Int) to match
// the Haskell reference implementation. Returns an error
// if any intermediate result overflows uint64.
func CalculateMinFee(
	txSize uint64,
	exUnits lcommon.ExUnits,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) (uint64, error) {
	// baseFee = minFeeA * txSize + minFeeB
	a := uint64(minFeeA)
	product := a * txSize
	if txSize != 0 && product/txSize != a {
		return 0, fmt.Errorf(
			"%w: minFeeA * txSize", errFeeOverflow,
		)
	}
	baseFee := product + uint64(minFeeB)
	if baseFee < product {
		return 0, fmt.Errorf(
			"%w: baseFee + minFeeB", errFeeOverflow,
		)
	}

	var scriptFee uint64
	if pricesMem != nil && pricesSteps != nil {
		memFee, err := CeilMul(
			pricesMem.Num(),
			pricesMem.Denom(),
			big.NewInt(exUnits.Memory),
		)
		if err != nil {
			return 0, fmt.Errorf("memFee: %w", err)
		}
		stepFee, err := CeilMul(
			pricesSteps.Num(),
			pricesSteps.Denom(),
			big.NewInt(exUnits.Steps),
		)
		if err != nil {
			return 0, fmt.Errorf("stepFee: %w", err)
		}
		scriptFee = memFee + stepFee
		if scriptFee < memFee {
			return 0, fmt.Errorf(
				"%w: memFee + stepFee",
				errFeeOverflow,
			)
		}
	}

	total := baseFee + scriptFee
	if total < baseFee {
		return 0, fmt.Errorf(
			"%w: baseFee + scriptFee", errFeeOverflow,
		)
	}
	return total, nil
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
// The minimum fee formula (Alonzo+ eras):
//
//	minFee = (minFeeA * txSize) + minFeeB
//	       + ceil(pricesMem * totalMem)
//	       + ceil(pricesSteps * totalSteps)
//
// Returns nil if the declared fee is sufficient.
func ValidateTxFee(
	tx lcommon.Transaction,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) error {
	txSize := TxBodySize(tx)
	declaredEU, err := DeclaredExUnits(tx)
	if err != nil {
		return fmt.Errorf(
			"calculating declared execution units: %w",
			err,
		)
	}
	minFee, err := CalculateMinFee(
		txSize,
		declaredEU,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)
	if err != nil {
		return fmt.Errorf(
			"calculating minimum fee: %w", err,
		)
	}
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
