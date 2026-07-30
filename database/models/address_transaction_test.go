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

	"github.com/stretchr/testify/require"
)

// legacyAddressTransaction mirrors the AddressTransaction schema from
// before idx_addr_tx_stake_position existed: idx_addr_tx_staking covered
// only (credential_tag, staking_key), and slot/tx_index were not part of
// any index the account-transactions range query could use for ordering.
type legacyAddressTransaction struct {
	ID            uint   `gorm:"primaryKey"`
	PaymentKey    []byte `gorm:"index:idx_addr_tx_payment;size:28"`
	StakingKey    []byte `gorm:"index:idx_addr_tx_staking,priority:2;size:28"`
	CredentialTag uint8  `gorm:"not null;default:0;index:idx_addr_tx_staking,priority:1"`
	TransactionID uint   `gorm:"index"`
	Slot          uint64 `gorm:"index"`
	TxIndex       uint32
}

func (legacyAddressTransaction) TableName() string {
	return "address_transaction"
}

// TestMigrateAddressTransactionStakePositionIndex verifies the upgrade
// path on a populated, pre-existing table, called in the same order
// production call sites (postgres/sqlite/mysql database.go) now use:
// AutoMigrate creates the new idx_addr_tx_stake_position index on its own
// (it is a new name) BEFORE the migration helper runs, and the helper
// then drops the now-redundant idx_addr_tx_staking, without losing any
// rows.
func TestMigrateAddressTransactionStakePositionIndex(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, db.AutoMigrate(&legacyAddressTransaction{}))
	require.True(
		t,
		db.Migrator().HasIndex(
			&legacyAddressTransaction{}, "idx_addr_tx_staking",
		),
	)
	require.False(
		t,
		db.Migrator().HasIndex(
			&AddressTransaction{}, "idx_addr_tx_stake_position",
		),
	)

	// Seed a row on the legacy schema so the migration exercises a
	// populated table, not just an empty one.
	require.NoError(t, db.Create(&legacyAddressTransaction{
		PaymentKey:    make([]byte, 28),
		StakingKey:    make([]byte, 28),
		CredentialTag: 0,
		TransactionID: 1,
		Slot:          10,
		TxIndex:       0,
	}).Error)

	// AutoMigrate (creating idx_addr_tx_stake_position) MUST run before
	// the cleanup helper: see MigrateAddressTransactionStakePositionIndex's
	// doc comment and TestMigrateAddressTransactionStakePositionIndex_
	// ReplacementMissing below for what happens if a caller gets this
	// order wrong.
	require.NoError(t, db.AutoMigrate(&AddressTransaction{}))
	require.NoError(t, MigrateAddressTransactionStakePositionIndex(db, nil))

	require.True(
		t,
		db.Migrator().HasIndex(
			&AddressTransaction{}, "idx_addr_tx_stake_position",
		),
	)
	require.False(
		t,
		db.Migrator().HasIndex(&AddressTransaction{}, "idx_addr_tx_staking"),
	)

	// The pre-migration row must survive the index swap.
	var count int64
	require.NoError(t, db.Model(&AddressTransaction{}).Count(&count).Error)
	require.Equal(t, int64(1), count)

	// Idempotent: running it again against the now-migrated schema is a
	// no-op, matching the reward_state precedent
	// (TestMigrateRewardLiveStakePoolIndex).
	require.NoError(t, MigrateAddressTransactionStakePositionIndex(db, nil))
}

func TestMigrateAddressTransactionStakePositionIndex_NoTable(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, MigrateAddressTransactionStakePositionIndex(db, nil))
}

// TestMigrateAddressTransactionStakePositionIndex_ReplacementMissing is the
// regression test for the drop-before-create ordering bug (CodeRabbit
// review, dingo PR #3016): if idx_addr_tx_stake_position does not exist
// yet — i.e. a caller invokes this helper before AutoMigrate has created
// it, or a prior AutoMigrate run failed partway through — the helper must
// leave idx_addr_tx_staking in place rather than dropping the only index
// that still covers credential lookups.
func TestMigrateAddressTransactionStakePositionIndex_ReplacementMissing(t *testing.T) {
	db := openMemoryDB(t)
	require.NoError(t, db.AutoMigrate(&legacyAddressTransaction{}))
	require.True(
		t,
		db.Migrator().HasIndex(
			&legacyAddressTransaction{}, "idx_addr_tx_staking",
		),
	)
	require.False(
		t,
		db.Migrator().HasIndex(
			&AddressTransaction{}, "idx_addr_tx_stake_position",
		),
	)

	// Deliberately do NOT call AutoMigrate(&AddressTransaction{}) first, so
	// idx_addr_tx_stake_position never gets created.
	require.NoError(t, MigrateAddressTransactionStakePositionIndex(db, nil))

	require.True(
		t,
		db.Migrator().HasIndex(
			&legacyAddressTransaction{}, "idx_addr_tx_staking",
		),
		"legacy index must survive when its replacement was never created",
	)
}
