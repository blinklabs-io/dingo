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
	// CollateralReturn uses a separate FK (CollateralReturnForTxID) to distinguish
	// from regular Outputs which use TransactionID. This allows GORM to load each
	// association correctly without ambiguity.
	CollateralReturn *Utxo            `gorm:"foreignKey:CollateralReturnForTxID;references:ID;constraint:OnDelete:CASCADE"`
	PlutusData       []PlutusData     `gorm:"foreignKey:TransactionID;references:ID;constraint:OnDelete:CASCADE"`
	Certificates     []Certificate    `gorm:"foreignKey:TransactionID;references:ID;constraint:OnDelete:CASCADE"`
	Outputs          []Utxo           `gorm:"foreignKey:TransactionID;references:ID;constraint:OnDelete:CASCADE"`
	Hash             []byte           `gorm:"uniqueIndex;size:32"`
	Collateral       []Utxo           `gorm:"foreignKey:CollateralByTxId;references:Hash"`
	BlockHash        []byte           `gorm:"index;size:32"`
	KeyWitnesses     []KeyWitness     `gorm:"foreignKey:TransactionID;references:ID;constraint:OnDelete:CASCADE"`
	WitnessScripts   []WitnessScripts `gorm:"foreignKey:TransactionID;references:ID;constraint:OnDelete:CASCADE"`
	Inputs           []Utxo           `gorm:"foreignKey:SpentAtTxId;references:Hash"`
	Redeemers        []Redeemer       `gorm:"foreignKey:TransactionID;references:ID;constraint:OnDelete:CASCADE"`
	ReferenceInputs  []Utxo           `gorm:"foreignKey:ReferencedByTxId;references:Hash"`
	Metadata         []byte
	Slot             uint64 `gorm:"index"`
	Type             int
	ID               uint `gorm:"primaryKey"`
	Fee              types.Uint64
	TTL              types.Uint64
	BlockIndex       uint32
	Valid            bool
}

func (Transaction) TableName() string {
	return "transaction"
}
