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

// legacyRewardAccountOutput is the reward_account_output schema from before
// dingo #1875 added any credential-leading index at all (it predates both
// idx_reward_account_output_credential and its later replacement,
// idx_reward_account_output_credential_spendable; see
// legacyRewardAccountOutputCredentialNoSpendable below for the intermediate
// shape). Every other tag is identical to the current RewardAccountOutput, so
// migrating from this shape to the current one exercises exactly what an
// operator upgrading a database that predates dingo #1875 entirely sees.
type legacyRewardAccountOutput struct {
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

func (legacyRewardAccountOutput) TableName() string {
	return "reward_account_output"
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
	require.NoError(t, db.AutoMigrate(&RewardLiveStake{}))
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardLiveStake{},
			"idx_reward_live_stake_pool_order",
		),
	)
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
	// Capture the lowest-ID row because that is the row GORM First selects in
	// RefreshLiveStakeAggregate before the unique index exists.
	refreshed := noIndexRewardLiveStake{
		CredentialTag: 0, StakingKey: stakingKey, TotalStake: 40, Registered: true,
	}
	require.NoError(t, db.Create(&refreshed).Error)
	require.NoError(t, db.Create(&noIndexRewardLiveStake{
		CredentialTag: 0, StakingKey: stakingKey, TotalStake: 70, Registered: true,
	}).Error)
	require.NoError(t, db.Create(&noIndexRewardLiveStake{
		CredentialTag: 0, StakingKey: other, TotalStake: 30, Registered: true,
	}).Error)

	// Mirror an incremental refresh after duplicates already exist. The
	// lowest-ID row becomes canonical while the higher-ID duplicate remains
	// stale.
	require.NoError(t, db.Model(&refreshed).Updates(map[string]any{
		"total_stake":  types.Uint64(90),
		"updated_slot": uint64(100),
	}).Error)

	require.NoError(t, DedupeRewardLiveStake(db, nil))

	// Dedup preserves the row updated by RefreshLiveStakeAggregate; keeping the
	// highest-ID row here would retain stale total_stake 70 indefinitely
	// because the backfill check only looks for missing credential keys.
	var rows []noIndexRewardLiveStake
	require.NoError(t, db.Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	byKey := map[string]noIndexRewardLiveStake{}
	for _, r := range rows {
		byKey[string(r.StakingKey)] = r
	}
	require.Equal(t, refreshed.ID, byKey[string(stakingKey)].ID)
	require.Equal(t, uint64(90), uint64(byKey[string(stakingKey)].TotalStake))
	require.Equal(t, uint64(100), byKey[string(stakingKey)].UpdatedSlot)
	require.Equal(t, uint64(30), uint64(byKey[string(other)].TotalStake))

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

// TestMigrateRewardAccountOutputCredentialIndex pins that AutoMigrate adds
// idx_reward_account_output_credential_spendable to a database that predates
// dingo #1875 entirely (an in-place upgrade), not only to a freshly created
// one, and that pre-existing rows survive untouched. Unlike
// idx_reward_live_stake_pool_order (MigrateRewardLiveStakePoolIndex) this
// index needs no pre-migration repair step to be created: it is a new,
// non-unique secondary index over columns whose types are unchanged, so
// AutoMigrate creates it directly with CREATE INDEX rather than rebuilding
// the table. See TestMigrateRewardAccountOutputCredentialSpendableIndex for
// the narrower upgrade from the index shape dingo #1875 originally shipped
// (credential-leading but without spendable), which does need the
// post-AutoMigrate drop step.
func TestMigrateRewardAccountOutputCredentialIndex(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, db.AutoMigrate(&legacyRewardAccountOutput{}))
	require.False(
		t,
		db.Migrator().HasIndex(
			&legacyRewardAccountOutput{},
			"idx_reward_account_output_credential_spendable",
		),
	)

	stakingKey := make([]byte, 28)
	stakingKey[0] = 0x41
	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x42
	require.NoError(t, db.Create(&legacyRewardAccountOutput{
		Epoch:         7,
		CredentialTag: 0,
		StakingKey:    stakingKey,
		PoolKeyHash:   poolKeyHash,
		RewardType:    "member",
		Amount:        123,
		Spendable:     true,
		CapturedSlot:  10,
		BoundarySlot:  11,
	}).Error)

	require.NoError(t, db.AutoMigrate(&RewardAccountOutput{}))

	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_credential_spendable",
		),
	)
	// The pre-existing unique index survives alongside the new one.
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_epoch_cred_pool_type",
		),
	)

	var migrated RewardAccountOutput
	require.NoError(t, db.First(&migrated).Error)
	require.Equal(t, uint64(7), migrated.Epoch)
	require.Equal(t, stakingKey, migrated.StakingKey)
	require.Equal(t, poolKeyHash, migrated.PoolKeyHash)
	require.Equal(t, "member", migrated.RewardType)
	require.Equal(t, uint64(123), uint64(migrated.Amount))

	// Idempotent: migrating an already-migrated database is a no-op.
	require.NoError(t, db.AutoMigrate(&RewardAccountOutput{}))
}

// legacyRewardAccountOutputCredentialNoSpendable is the reward_account_output
// schema as dingo #1875 originally shipped it: idx_reward_account_output_credential
// leads with (credential_tag, staking_key) but has no spendable column, so a
// query filtering on spendable degrades that index's seek into a seek over
// every one of the credential's rows plus a per-row filter (the "Finding 1"
// fix). This is the shape TestMigrateRewardAccountOutputCredentialSpendableIndex
// upgrades from, exercising the exact database an operator who already
// deployed dingo #1875 has on disk.
type legacyRewardAccountOutputCredentialNoSpendable struct {
	StakingKey    []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:3;size:28;not null;index:idx_reward_account_output_credential,priority:2"`
	PoolKeyHash   []byte       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:4;size:28;not null;index:idx_reward_account_output_credential,priority:4"`
	RewardType    string       `gorm:"type:varchar(16);uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:5;not null;index:idx_reward_account_output_credential,priority:5"`
	ID            uint         `gorm:"primarykey"`
	Epoch         uint64       `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:1;not null;index:idx_reward_account_output_credential,priority:3"`
	CredentialTag uint8        `gorm:"uniqueIndex:idx_reward_account_output_epoch_cred_pool_type,priority:2;not null;default:0;index:idx_reward_account_output_credential,priority:1"`
	Amount        types.Uint64 `gorm:"not null"`
	Spendable     bool         `gorm:"not null"`
	CapturedSlot  uint64       `gorm:"index;not null"`
	BoundarySlot  uint64       `gorm:"index;not null"`
}

func (legacyRewardAccountOutputCredentialNoSpendable) TableName() string {
	return "reward_account_output"
}

// TestMigrateRewardAccountOutputCredentialSpendableIndex pins the upgrade
// path for the Finding 1 fix: a database already running the
// credential-leading index dingo #1875 originally shipped (no spendable
// column) ends up with exactly the new idx_reward_account_output_credential_spendable
// index and none of the superseded idx_reward_account_output_credential,
// with every pre-existing row (both spendable and non-spendable) intact.
//
// This is the "prove the upgrade path works on a populated database" case:
// AutoMigrate alone only adds the new index (GORM does not alter or drop an
// existing index it no longer sees declared), so without
// MigrateRewardAccountOutputCredentialIndex a migrated database would carry
// both indexes forever. The migration runs after AutoMigrate specifically so
// the replacement always exists before the old one is removed.
func TestMigrateRewardAccountOutputCredentialSpendableIndex(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(
		t,
		db.AutoMigrate(&legacyRewardAccountOutputCredentialNoSpendable{}),
	)
	require.True(
		t,
		db.Migrator().HasIndex(
			&legacyRewardAccountOutputCredentialNoSpendable{},
			"idx_reward_account_output_credential",
		),
	)
	require.False(
		t,
		db.Migrator().HasIndex(
			&legacyRewardAccountOutputCredentialNoSpendable{},
			"idx_reward_account_output_credential_spendable",
		),
	)

	spendableKey := make([]byte, 28)
	spendableKey[0] = 0x51
	unspendableKey := make([]byte, 28)
	unspendableKey[0] = 0x52
	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x53
	require.NoError(t, db.Create(&legacyRewardAccountOutputCredentialNoSpendable{
		Epoch: 9, CredentialTag: 0, StakingKey: spendableKey,
		PoolKeyHash: poolKeyHash, RewardType: "member", Amount: 111,
		Spendable: true, CapturedSlot: 20, BoundarySlot: 21,
	}).Error)
	require.NoError(t, db.Create(&legacyRewardAccountOutputCredentialNoSpendable{
		Epoch: 9, CredentialTag: 0, StakingKey: unspendableKey,
		PoolKeyHash: poolKeyHash, RewardType: "member", Amount: 222,
		Spendable: false, CapturedSlot: 20, BoundarySlot: 21,
	}).Error)

	// AutoMigrate on its own only adds the replacement index; it does not
	// drop the superseded one, since GORM never alters/removes an existing
	// index that the current struct simply no longer declares.
	require.NoError(t, db.AutoMigrate(&RewardAccountOutput{}))
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_credential_spendable",
		),
	)
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_credential",
		),
		"superseded index must still be present before the migration helper runs",
	)

	require.NoError(t, MigrateRewardAccountOutputCredentialIndex(db, nil))

	require.False(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_credential",
		),
		"superseded index must be dropped once the replacement exists",
	)
	require.True(
		t,
		db.Migrator().HasIndex(
			&RewardAccountOutput{},
			"idx_reward_account_output_credential_spendable",
		),
	)

	// Pre-existing rows of both spendable states survive the migration.
	var rows []RewardAccountOutput
	require.NoError(t, db.Order("staking_key ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	require.Equal(t, spendableKey, rows[0].StakingKey)
	require.True(t, rows[0].Spendable)
	require.Equal(t, uint64(111), uint64(rows[0].Amount))
	require.Equal(t, unspendableKey, rows[1].StakingKey)
	require.False(t, rows[1].Spendable)
	require.Equal(t, uint64(222), uint64(rows[1].Amount))

	// Idempotent: running the migration again against an already-migrated
	// database (superseded index already gone) is a no-op.
	require.NoError(t, MigrateRewardAccountOutputCredentialIndex(db, nil))
}
