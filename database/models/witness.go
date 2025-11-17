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

// WitnessType represents the type of witness
type WitnessType uint8

const (
	WitnessTypeVkey      WitnessType = 0
	WitnessTypeBootstrap WitnessType = 1
)

// Witness represents a witness entry (Vkey or Bootstrap)
type Witness struct {
	ID            uint   `gorm:"primaryKey"`
	TransactionID uint   `gorm:"index"`
	Type          uint8  `gorm:"index"` // WitnessType (0=Vkey, 1=Bootstrap)
	Vkey          []byte `gorm:"type:bytea"`
	Signature     []byte `gorm:"type:bytea"`
	PublicKey     []byte `gorm:"type:bytea"` // For Bootstrap
	ChainCode     []byte `gorm:"type:bytea"` // For Bootstrap
	Attributes    []byte `gorm:"type:bytea"` // For Bootstrap
	Transaction   *Transaction
}

func (Witness) TableName() string {
	return "witness"
}
