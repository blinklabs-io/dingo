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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package models

// GenesisDelegation records Shelley genesis-key delegation certificates.
// Header verification uses the latest row for a genesis key before a block slot
// to validate overlay-slot genesis delegate headers against active ledger state.
type GenesisDelegation struct {
	ID                  uint   `gorm:"primarykey"`
	GenesisHash         []byte `gorm:"index:idx_genesis_delegation_lookup,priority:1;size:28;not null"`
	GenesisDelegateHash []byte `gorm:"index;size:28;not null"`
	VrfKeyHash          []byte `gorm:"size:32;not null"`
	AddedSlot           uint64 `gorm:"index:idx_genesis_delegation_lookup,priority:2;index;not null"`
	BlockIndex          uint32 `gorm:"index:idx_genesis_delegation_lookup,priority:3;not null"`
	CertIndex           uint   `gorm:"index:idx_genesis_delegation_lookup,priority:4;not null"`
	CertificateID       uint   `gorm:"uniqueIndex"`
}

func (GenesisDelegation) TableName() string {
	return "genesis_delegation"
}
