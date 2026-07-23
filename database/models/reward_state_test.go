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

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// legacyRewardPoolInput is the reward_pool_input schema from before
// owner_stake and reward-account identity were persisted.
type legacyRewardPoolInput struct {
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

type legacyRewardLiveStake struct {
	PoolKeyHash []byte       `gorm:"index:idx_reward_live_stake_pool,priority:1;size:28"`
	TotalStake  types.Uint64 `gorm:"index:idx_reward_live_stake_pool,priority:2;not null"`
	ID          uint         `gorm:"primarykey"`
}

func (legacyRewardLiveStake) TableName() string {
	return "reward_live_stake"
}

func (legacyRewardPoolInput) TableName() string {
	return "reward_pool_input"
}

func TestRewardPoolInputMigrationDefaultsOwnerStake(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, db.AutoMigrate(&legacyRewardPoolInput{}))
	require.NoError(t, db.Create(&legacyRewardPoolInput{
		PoolKeyHash:    make([]byte, 28),
		Epoch:          1,
		Pledge:         1,
		DelegatedStake: 2,
		Cost:           3,
		CapturedSlot:   4,
		BoundarySlot:   5,
	}).Error)

	require.NoError(t, db.AutoMigrate(&RewardPoolInput{}))
	var migrated RewardPoolInput
	require.NoError(t, db.First(&migrated).Error)
	require.Zero(t, uint64(migrated.OwnerStake))
	require.Equal(t, uint64(1), migrated.Epoch)
	require.Equal(t, uint64(1), uint64(migrated.Pledge))
	require.Equal(t, uint64(2), uint64(migrated.DelegatedStake))
	require.Equal(t, uint64(3), uint64(migrated.Cost))
	require.Equal(t, uint64(4), migrated.CapturedSlot)
	require.Equal(t, uint64(5), migrated.BoundarySlot)
}

func TestMigrateRewardLiveStakePoolIndex(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, db.AutoMigrate(&legacyRewardLiveStake{}))
	require.True(
		t,
		db.Migrator().HasIndex(
			&legacyRewardLiveStake{},
			"idx_reward_live_stake_pool",
		),
	)

	require.NoError(t, MigrateRewardLiveStakePoolIndex(db, nil))
	require.False(
		t,
		db.Migrator().HasIndex(
			&legacyRewardLiveStake{},
			"idx_reward_live_stake_pool",
		),
	)
	require.NoError(t, MigrateRewardLiveStakePoolIndex(db, nil))
}

// noIndexRewardLiveStake mirrors RewardLiveStake but without the unique
// credential index, so a test can seed duplicate credential rows that the
// enforced schema would reject.
type noIndexRewardLiveStake struct {
	PoolKeyHash              []byte `gorm:"size:28"`
	StakingKey               []byte `gorm:"size:28;not null"`
	ID                       uint   `gorm:"primarykey"`
	CredentialTag            uint8  `gorm:"not null;default:0"`
	UtxoStake                types.Uint64
	RewardStake              types.Uint64
	TotalStake               types.Uint64
	Registered               bool
	PoolDelegationSlot       uint64 `gorm:"not null;default:0"`
	PoolDelegationBlockIndex uint64 `gorm:"not null;default:0"`
	PoolDelegationCertIndex  uint32 `gorm:"not null;default:0"`
	UpdatedSlot              uint64 `gorm:"not null;default:0"`
}

func (noIndexRewardLiveStake) TableName() string {
	return "reward_live_stake"
}

func TestDedupeRewardLiveStake(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, db.AutoMigrate(&noIndexRewardLiveStake{}))

	stakingKey := make([]byte, 28)
	stakingKey[0] = 0x31
	other := make([]byte, 28)
	other[0] = 0x32

	// Two rows for the same credential (the corruption), plus a distinct one.
	require.NoError(t, db.Create(&noIndexRewardLiveStake{
		CredentialTag: 0, StakingKey: stakingKey, TotalStake: 40, Registered: true,
	}).Error)
	require.NoError(t, db.Create(&noIndexRewardLiveStake{
		CredentialTag: 0, StakingKey: stakingKey, TotalStake: 70, Registered: true,
	}).Error)
	require.NoError(t, db.Create(&noIndexRewardLiveStake{
		CredentialTag: 0, StakingKey: other, TotalStake: 30, Registered: true,
	}).Error)

	require.NoError(t, DedupeRewardLiveStake(db, nil))

	// The duplicate credential collapses to the highest-id row (70); the
	// distinct credential is untouched.
	var rows []noIndexRewardLiveStake
	require.NoError(t, db.Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	byKey := map[string]uint64{}
	for _, r := range rows {
		byKey[string(r.StakingKey)] = uint64(r.TotalStake)
	}
	require.Equal(t, uint64(70), byKey[string(stakingKey)])
	require.Equal(t, uint64(30), byKey[string(other)])

	// After dedup, the enforced schema's unique index installs successfully.
	require.NoError(t, db.AutoMigrate(&RewardLiveStake{}))
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardLiveStake{},
			"idx_reward_live_stake_cred",
		),
	)

	// Idempotent: a second run with no duplicates is a no-op.
	require.NoError(t, DedupeRewardLiveStake(db, nil))
}
