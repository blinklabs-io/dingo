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
	"fmt"
	"math/big"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
// This avoids floating-point imprecision.
func CeilMul(num, denom, value *big.Int) uint64 {
	// product = num * value
	product := new(big.Int).Mul(num, value)
	// ceil(product / denom) = (product + denom - 1) / denom
	one := big.NewInt(1)
	denomMinusOne := new(big.Int).Sub(denom, one)
	product.Add(product, denomMinusOne)
	product.Div(product, denom)
	return product.Uint64()
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
// the Haskell reference implementation.
func CalculateMinFee(
	txSize uint64,
	exUnits lcommon.ExUnits,
	minFeeA uint,
	minFeeB uint,
	pricesMem *big.Rat,
	pricesSteps *big.Rat,
) uint64 {
	baseFee := uint64(minFeeA)*txSize + uint64(minFeeB)
	var scriptFee uint64
	if pricesMem != nil && pricesSteps != nil {
		memFee := CeilMul(
			pricesMem.Num(),
			pricesMem.Denom(),
			big.NewInt(exUnits.Memory),
		)
		stepFee := CeilMul(
			pricesSteps.Num(),
			pricesSteps.Denom(),
			big.NewInt(exUnits.Steps),
		)
		scriptFee = memFee + stepFee
	}
	return baseFee + scriptFee
}
