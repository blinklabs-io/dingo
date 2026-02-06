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
	"math/big"

	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// TxBodySize returns the CBOR-serialized size of a
// transaction in bytes.
func TxBodySize(tx lcommon.Transaction) uint64 {
	return eras.TxBodySize(tx)
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
	return eras.CalculateMinFee(
		txSize,
		exUnits,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)
}
