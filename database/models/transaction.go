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
	Hash             []byte `gorm:"uniqueIndex"`
	BlockHash        []byte `gorm:"index"`
	Inputs           []Utxo `gorm:"foreignKey:SpentAtTxId;references:Hash"`
	Outputs          []Utxo `gorm:"foreignKey:TransactionID;references:ID"`
	ReferenceInputs  []Utxo `gorm:"foreignKey:ReferencedByTxId;references:Hash"`
	Collateral       []Utxo `gorm:"foreignKey:CollateralByTxId;references:Hash"`
	CollateralReturn *Utxo  `gorm:"foreignKey:TransactionID;references:ID"`
	ID               uint   `gorm:"primaryKey"`
	Type             int
	BlockIndex       uint32
	Metadata         []byte
	Fee              types.Uint64
	TTL              types.Uint64
	Valid            bool
}

func (Transaction) TableName() string {
	return "transaction"
}
