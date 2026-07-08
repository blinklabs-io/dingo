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
	Margin                     *types.Rat
	PoolKeyHash                []byte `gorm:"uniqueIndex:idx_reward_pool_input_epoch_pool,priority:2;size:28;not null"`
	RewardAccount              []byte `gorm:"size:28"`
	BlocksProduced             *uint64
	TotalBlocksInEpoch         *uint64
	ID                         uint         `gorm:"primarykey"`
	Epoch                      uint64       `gorm:"uniqueIndex:idx_reward_pool_input_epoch_pool,priority:1;not null"`
	Pledge                     types.Uint64 `gorm:"not null"`
	DelegatedStake             types.Uint64 `gorm:"not null"`
	OwnerStake                 types.Uint64 `gorm:"not null"`
	Cost                       types.Uint64 `gorm:"not null"`
	DelegatorCount             uint64       `gorm:"not null"`
	RewardAccountCredentialTag uint8        `gorm:"not null;default:0"`
	CapturedSlot               uint64       `gorm:"index;not null"`
	BoundarySlot               uint64       `gorm:"index;not null"`
}

func (RewardPoolInput) TableName() string {
	return "reward_pool_input"
}

// RewardStakeInput captures per-credential stake at the reward snapshot.
type RewardStakeInput struct {
	PoolKeyHash   []byte       `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:2;size:28;not null"`
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:4;size:28;not null"`
	ID            uint         `gorm:"primarykey"`
	Epoch         uint64       `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:1;not null"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_stake_input_epoch_pool_cred,priority:3;not null;default:0"`
	Stake         types.Uint64 `gorm:"not null"`
	Owner         bool         `gorm:"not null;default:false"`
	Registered    bool         `gorm:"not null"`
	CapturedSlot  uint64       `gorm:"index;not null"`
	BoundarySlot  uint64       `gorm:"index;not null"`
}

func (RewardStakeInput) TableName() string {
	return "reward_stake_input"
}

// RewardLiveStake is the live per-stake-credential aggregate used to make
// epoch-boundary reward snapshots cheap. UtxoStake and RewardStake are stored
// separately so rollback/account-reward repair can refresh only the affected
// credential while TotalStake is directly queryable by the snapshot copier.
type RewardLiveStake struct {
	PoolKeyHash   []byte       `gorm:"index:idx_reward_live_stake_pool,priority:1;size:28"`
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_live_stake_cred,priority:2;size:28;not null"`
	ID            uint         `gorm:"primarykey"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_live_stake_cred,priority:1;not null;default:0;index:idx_reward_live_stake_pool,priority:2"`
	UtxoStake     types.Uint64 `gorm:"not null"`
	RewardStake   types.Uint64 `gorm:"not null"`
	TotalStake    types.Uint64 `gorm:"not null;index:idx_reward_live_stake_pool,priority:3"`
	Registered    bool         `gorm:"not null;index:idx_reward_live_stake_pool,priority:4"`
	// PoolDelegation* pins the certificate order for PoolKeyHash. A stake
	// credential only contributes to a pool's reward snapshot when this
	// delegation is at least as recent as the pool's current registration.
	PoolDelegationSlot       uint64 `gorm:"index:idx_reward_live_stake_pool,priority:5;not null;default:0"`
	PoolDelegationBlockIndex uint64 `gorm:"not null;default:0"`
	PoolDelegationCertIndex  uint32 `gorm:"not null;default:0"`
	UpdatedSlot              uint64 `gorm:"index;not null"`
}

func (RewardLiveStake) TableName() string {
	return "reward_live_stake"
}

// RewardPoolOutput captures per-pool reward calculation output for an epoch.
type RewardPoolOutput struct {
	ApparentPerformance *types.Rat
	PoolKeyHash         []byte       `gorm:"uniqueIndex:idx_reward_pool_output_epoch_pool,priority:2;size:28;not null"`
	ID                  uint         `gorm:"primarykey"`
	Epoch               uint64       `gorm:"uniqueIndex:idx_reward_pool_output_epoch_pool,priority:1;not null"`
	OptimalReward       types.Uint64 `gorm:"not null"`
	TotalReward         types.Uint64 `gorm:"not null"`
	LeaderReward        types.Uint64 `gorm:"not null"`
	MemberRewardTotal   types.Uint64 `gorm:"not null"`
	OwnerStake          types.Uint64 `gorm:"not null"`
	Undistributed       types.Uint64 `gorm:"not null"`
	Unspendable         types.Uint64 `gorm:"not null"`
	CapturedSlot        uint64       `gorm:"index;not null"`
	BoundarySlot        uint64       `gorm:"index;not null"`
}

func (RewardPoolOutput) TableName() string {
	return "reward_pool_output"
}

// RewardAccountOutput captures per-account reward calculation output.
type RewardAccountOutput struct {
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:3;size:28;not null"`
	PoolKeyHash   []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:4;size:28;not null"`
	RewardType    string       `gorm:"type:varchar(16);uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:5;not null"`
	ID            uint         `gorm:"primarykey"`
	Epoch         uint64       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:1;not null"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:2;not null;default:0"`
	Amount        types.Uint64 `gorm:"not null"`
	Spendable     bool         `gorm:"not null"`
	CapturedSlot  uint64       `gorm:"index;not null"`
	BoundarySlot  uint64       `gorm:"index;not null"`
}

func (RewardAccountOutput) TableName() string {
	return "reward_account_output"
}
