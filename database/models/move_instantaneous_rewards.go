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

// MoveInstantaneousRewards represents a move instantaneous rewards certificate
type MoveInstantaneousRewards struct {
	RewardData    []byte       // JSON-encoded rewards map
	ID            uint         `gorm:"primaryKey"`
	CertificateID uint         `gorm:"uniqueIndex:uniq_move_instantaneous_rewards_cert"`
	Source        uint         `gorm:"index"` // 0=reserves, 1=treasury
	OtherPot      types.Uint64 // Amount moved from other pot
	AddedSlot     types.Uint64 `gorm:"index"`
}

// TableName returns the database table name for the MoveInstantaneousRewards model.
func (MoveInstantaneousRewards) TableName() string {
	return "move_instantaneous_rewards"
}
