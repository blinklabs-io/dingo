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

// PoolStakeSnapshot captures aggregated pool stake at an epoch boundary.
// Used for leader election in Ouroboros Praos consensus.
type PoolStakeSnapshot struct {
	ID             uint         `gorm:"primarykey"`
	Epoch          uint64       `gorm:"index:idx_pool_stake_epoch_pool,priority:1;not null"`
	SnapshotType   string       `gorm:"type:varchar(4);index:idx_pool_stake_epoch_pool,priority:2;not null"` // "mark", "set", "go"
	PoolKeyHash    []byte       `gorm:"index:idx_pool_stake_epoch_pool,priority:3;size:28"`
	TotalStake     types.Uint64 `gorm:"not null"`
	DelegatorCount uint64       `gorm:"not null"`
	CapturedSlot   uint64       `gorm:"not null"`
}

// TableName returns the table name
func (PoolStakeSnapshot) TableName() string {
	return "pool_stake_snapshot"
}

// EpochSummary captures network-wide aggregate statistics at epoch boundary.
type EpochSummary struct {
	ID               uint         `gorm:"primarykey"`
	Epoch            uint64       `gorm:"uniqueIndex;not null"`
	TotalActiveStake types.Uint64 `gorm:"not null"`
	TotalPoolCount   uint64       `gorm:"not null"`
	TotalDelegators  uint64       `gorm:"not null"`
	EpochNonce       []byte       `gorm:"size:32"`
	BoundarySlot     uint64       `gorm:"not null"`
	SnapshotReady    bool         `gorm:"not null;default:false"`
}

// TableName returns the table name
func (EpochSummary) TableName() string {
	return "epoch_summary"
}
