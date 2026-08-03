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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// legacyRewardAccountOutputSpendableNoGuarded is the reward_account_output
// schema exactly as dingo #1875's follow-up (#3015) shipped it:
// idx_reward_account_output_credential_spendable leads with (credential_tag,
// staking_key, spendable) but there is no guarded column or index at all.
// This is the shape TestMigrateRewardAccountOutputAddsSpendableGuardedIndex
// upgrades from, exercising the database an operator who already deployed
// the #3015 spendable filter has on disk before the dingo #3021 guarded
// column.
type legacyRewardAccountOutputSpendableNoGuarded struct {
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:3;size:28;not null;index:idx_reward_account_output_credential_spendable,priority:2"`
	PoolKeyHash   []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:4;size:28;not null;index:idx_reward_account_output_credential_spendable,priority:5"`
	RewardType    string       `gorm:"type:varchar(16);uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:5;not null;index:idx_reward_account_output_credential_spendable,priority:6"`
	ID            uint         `gorm:"primarykey"`
	Epoch         uint64       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:1;not null;index:idx_reward_account_output_credential_spendable,priority:4"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:2;not null;default:0;index:idx_reward_account_output_credential_spendable,priority:1"`
	Amount        types.Uint64 `gorm:"not null"`
	Spendable     bool         `gorm:"not null;index:idx_reward_account_output_credential_spendable,priority:3"`
	CapturedSlot  uint64       `gorm:"index;not null"`
	BoundarySlot  uint64       `gorm:"index;not null"`
}

func (legacyRewardAccountOutputSpendableNoGuarded) TableName() string {
	return "reward_account_output"
}

// TestMigrateRewardAccountOutputAddsSpendableGuardedIndex pins the dingo
// #3021 upgrade path: a database already running the spendable-aware index
// #3015 shipped gains the new idx_reward_account_output_credential_spendable_guarded
// index from a plain AutoMigrate, with every pre-existing row intact and its
// new Guarded column defaulting to false (the column did not exist before
// this migration, so every row it upgrades predates the CIP-0163 guard
// entirely).
//
// Unlike MigrateRewardAccountOutputCredentialIndex (which drops a superseded
// index after AutoMigrate creates its replacement),
// idx_reward_account_output_credential_spendable_guarded needs no drop step
// and no accompanying Migrate* helper: it is an entirely new, non-unique
// secondary index name that AutoMigrate creates directly with CREATE INDEX on
// any prior schema shape, the same way idx_reward_account_output_credential_spendable
// itself needed none when it was first introduced
// (TestMigrateRewardAccountOutputCredentialIndex). The older
// idx_reward_account_output_credential_spendable index is deliberately left
// declared on RewardAccountOutput and still exists afterward: dropping it
// would need the same after-AutoMigrate migration pattern, but its continued
// existence after a plain AutoMigrate is exactly what
// TestMigrateRewardAccountOutputCredentialIndex and
// TestMigrateRewardAccountOutputCredentialSpendableIndex already pin, so this
// change does not touch it.
func TestMigrateRewardAccountOutputAddsSpendableGuardedIndex(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(
		t,
		db.AutoMigrate(&legacyRewardAccountOutputSpendableNoGuarded{}),
	)
	require.True(
		t,
		db.Migrator().HasIndex(
			&legacyRewardAccountOutputSpendableNoGuarded{},
			"idx_reward_account_output_credential_spendable",
		),
	)
	require.False(
		t,
		db.Migrator().HasIndex(
			&legacyRewardAccountOutputSpendableNoGuarded{},
			"idx_reward_account_output_credential_spendable_guarded",
		),
	)

	spendableKey := make([]byte, 28)
	spendableKey[0] = 0x61
	otherKey := make([]byte, 28)
	otherKey[0] = 0x62
	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x63
	require.NoError(t, db.Create(&legacyRewardAccountOutputSpendableNoGuarded{
		Epoch: 20, CredentialTag: 0, StakingKey: spendableKey,
		PoolKeyHash: poolKeyHash, RewardType: "member", Amount: 555,
		Spendable: true, CapturedSlot: 30, BoundarySlot: 31,
	}).Error)
	require.NoError(t, db.Create(&legacyRewardAccountOutputSpendableNoGuarded{
		Epoch: 20, CredentialTag: 0, StakingKey: otherKey,
		PoolKeyHash: poolKeyHash, RewardType: "leader", Amount: 777,
		Spendable: true, CapturedSlot: 30, BoundarySlot: 31,
	}).Error)

	require.NoError(t, db.AutoMigrate(&RewardAccountOutput{}))

	// The new guarded-aware index now exists...
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_credential_spendable_guarded",
		),
	)
	// ...alongside the pre-existing spendable-only index, which this change
	// leaves declared and does not drop or rename.
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_credential_spendable",
		),
	)

	var rows []RewardAccountOutput
	require.NoError(t, db.Order("staking_key ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	require.Equal(t, spendableKey, rows[0].StakingKey)
	require.Equal(t, uint64(555), uint64(rows[0].Amount))
	require.Equal(t, otherKey, rows[1].StakingKey)
	require.Equal(t, uint64(777), uint64(rows[1].Amount))
	for _, row := range rows {
		assert.True(t, row.Spendable)
		assert.False(
			t, row.Guarded,
			"pre-existing rows must default guarded=false: the column did not exist before this migration",
		)
	}

	// Idempotent: running AutoMigrate again against an already-migrated
	// database is a no-op.
	require.NoError(t, db.AutoMigrate(&RewardAccountOutput{}))
}
