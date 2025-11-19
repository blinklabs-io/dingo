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

import (
	"github.com/blinklabs-io/dingo/database/types"
)

// GenesisKeyDelegation represents a genesis key delegation certificate
type GenesisKeyDelegation struct {
	GenesisHash         []byte       `gorm:"index;uniqueIndex:uniq_genesis_delegation"`
	GenesisDelegateHash []byte       `gorm:"uniqueIndex:uniq_genesis_delegation"`
	VrfKeyHash          []byte       `gorm:"uniqueIndex:uniq_genesis_delegation"`
	ID                  uint         `gorm:"primaryKey"`
	CertificateID       uint         `gorm:"uniqueIndex:uniq_genesis_delegation_cert"`
	AddedSlot           types.Uint64 `gorm:"index"`
}

// TableName returns the database table name for the GenesisKeyDelegation model.
func (GenesisKeyDelegation) TableName() string {
	return "genesis_key_delegations"
}
