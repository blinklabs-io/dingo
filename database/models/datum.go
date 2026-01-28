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

type Datum struct {
	Hash      []byte `gorm:"index;not null;unique;size:64"`
	RawDatum  []byte `gorm:"not null"`
	ID        uint   `gorm:"primarykey"`
	AddedSlot uint64 `gorm:"index;not null"`
}

func (Datum) TableName() string {
	return "datum"
}

// PlutusData represents a Plutus data value in the witness set
type PlutusData struct {
	Transaction   *Transaction `gorm:"foreignKey:TransactionID"`
	Data          []byte
	ID            uint `gorm:"primaryKey"`
	TransactionID uint `gorm:"index"`
}

func (PlutusData) TableName() string {
	return "plutus_data"
}
