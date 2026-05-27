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

import "github.com/blinklabs-io/dingo/database/types"

// RewardAdaPots captures the reward-related ADA pots at an epoch boundary.
type RewardAdaPots struct {
	ID           uint         `gorm:"primarykey"`
	Epoch        uint64       `gorm:"uniqueIndex;not null"`
	Treasury     types.Uint64 `gorm:"not null"`
	Reserves     types.Uint64 `gorm:"not null"`
	Fees         types.Uint64 `gorm:"not null"`
	Rewards      types.Uint64 `gorm:"not null"`
	CapturedSlot uint64       `gorm:"index;not null"`
}

func (RewardAdaPots) TableName() string {
	return "reward_ada_pots"
}

// RewardSnapshot captures reward-calculation snapshot metadata for an epoch.
type RewardSnapshot struct {
	ID               uint         `gorm:"primarykey"`
	Epoch            uint64       `gorm:"uniqueIndex:idx_reward_snapshot_epoch_type,priority:1;not null"`
	SnapshotType     string       `gorm:"type:varchar(4);uniqueIndex:idx_reward_snapshot_epoch_type,priority:2;not null"`
	TotalActiveStake types.Uint64 `gorm:"not null"`
	TotalPoolCount   uint64       `gorm:"not null"`
	TotalDelegators  uint64       `gorm:"not null"`
	CapturedSlot     uint64       `gorm:"index;not null"`
	BoundarySlot     uint64       `gorm:"index;not null"`
	EpochNonce       []byte       `gorm:"size:32"`
	ProtocolVersion  uint         `gorm:"not null"`
}

func (RewardSnapshot) TableName() string {
	return "reward_snapshot"
}

// RewardPoolInput captures per-pool inputs needed by reward calculation.
type RewardPoolInput struct {
	Margin             *types.Rat
	PoolKeyHash        []byte `gorm:"uniqueIndex:idx_reward_pool_input_epoch_pool,priority:2;size:28;not null"`
	BlocksProduced     *uint64
	TotalBlocksInEpoch *uint64
	ID                 uint         `gorm:"primarykey"`
	Epoch              uint64       `gorm:"uniqueIndex:idx_reward_pool_input_epoch_pool,priority:1;not null"`
	Pledge             types.Uint64 `gorm:"not null"`
	DelegatedStake     types.Uint64 `gorm:"not null"`
	Cost               types.Uint64 `gorm:"not null"`
	DelegatorCount     uint64       `gorm:"not null"`
	CapturedSlot       uint64       `gorm:"index;not null"`
	BoundarySlot       uint64       `gorm:"index;not null"`
}

func (RewardPoolInput) TableName() string {
	return "reward_pool_input"
}
