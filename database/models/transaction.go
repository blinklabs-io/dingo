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

package models

import "github.com/blinklabs-io/dingo/database/types"

// Transaction represents a transaction record
type Transaction struct {
	// CollateralReturn uses a separate FK (CollateralReturnForTxID) to
	// distinguish it from regular Outputs, which use TransactionID.
	CollateralReturn *Utxo
	PlutusData       []PlutusData
	Certificates     []Certificate
	Outputs          []Utxo
	Hash             []byte
	Collateral       []Utxo
	BlockHash        []byte
	KeyWitnesses     []KeyWitness
	WitnessScripts   []WitnessScripts
	Inputs           []Utxo
	Redeemers        []Redeemer
	ReferenceInputs  []Utxo
	Metadata         []byte
	Slot             uint64
	Type             int
	ID               uint
	Fee              types.Uint64
	// CollateralFee is the lovelace consumed into the fee pot by a
	// phase-2-invalid transaction (collateral inputs minus collateral
	// return, per the Alonzo/Babbage UTXOS rule). Zero for valid
	// transactions; Fee keeps the declared body fee in both cases.
	CollateralFee types.Uint64
	TTL           types.Uint64
	BlockIndex    uint32
	Valid         bool
}

func (Transaction) TableName() string {
	return "transaction"
}
