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

const (
	// KeyWitnessTypeVkey represents a Vkey witness
	KeyWitnessTypeVkey uint8 = 0
	// KeyWitnessTypeBootstrap represents a Bootstrap witness
	KeyWitnessTypeBootstrap uint8 = 1
)

// KeyWitness represents a key witness entry (Vkey or Bootstrap)
// Type: KeyWitnessTypeVkey = VkeyWitness, KeyWitnessTypeBootstrap = BootstrapWitness
type KeyWitness struct {
	ID            uint   `gorm:"primaryKey"`
	TransactionID uint   `gorm:"index"`
	Type          uint8  `gorm:"index"` // See KeyWitnessType* constants
	Vkey          []byte // Vkey witness key
	Signature     []byte // Witness signature
	PublicKey     []byte // For Bootstrap witness
	ChainCode     []byte // For Bootstrap witness
	Attributes    []byte // For Bootstrap witness
	Transaction   *Transaction
}

func (KeyWitness) TableName() string {
	return "key_witness"
}
