//go:build dingo_db_integration

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

package sqlstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func TestPostgresRewardJournalUpgradeIntegration(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	exerciseRewardJournalUpgrade(
		t,
		"pgx",
		dsn,
		PostgresDialect(),
		schema,
		registry,
	)
}

func TestMySQLRewardJournalUpgradeIntegration(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	exerciseRewardJournalUpgrade(
		t,
		"mysql",
		dsn,
		MySQLDialect(),
		database,
		registry,
	)
}

// exerciseRewardJournalUpgrade migrates a database to the version before the
// journal move, seeds every class of legacy row, runs the move in small
// batches, and then drives the store's journal paths on the result.
func exerciseRewardJournalUpgrade(
	t *testing.T,
	driver, dsn string,
	dialect Dialect,
	lockNamespace string,
	registry []migrations.Migration,
) {
	t.Helper()
	db, err := OpenDB(driver, dsn, dialect.Name(), false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	index := -1
	for i, migration := range registry {
		if migration.Name == "reward-journal-tables" {
			index = i
		}
	}
	require.Positive(t, index)
	registry[index].BatchSize = 3
	runTo := func(versions []migrations.Migration) {
		t.Helper()
		runner := migrations.Runner{
			DB:       db,
			Dialect:  dialect.Name(),
			Registry: versions,
			Locker:   integrationMigrationLocker(dialect.Name(), lockNamespace),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:index])

	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.ExecContext(ctx, dialect.Rebind(query), args...)
		require.NoError(t, err, query)
	}
	tx := dialect.QuoteIdentifier("transaction")
	keys := make([][]byte, 4)
	for i := range keys {
		keys[i] = bytes.Repeat([]byte{byte(0x10 + i)}, 28)
		exec(`INSERT INTO account (staking_key, credential_tag, added_slot,
created_slot, active, reward) VALUES (?, 0, 1, 1, TRUE, '100')`, keys[i])
	}
	exec(`INSERT INTO epoch (epoch_id, start_slot) VALUES (7, 700)`)
	hash := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }
	legacy := func(key []byte, h []byte, amount string, previous any, slot int64, withdrawal, post bool) {
		exec(`INSERT INTO account_reward_delta (staking_key, credential_tag,
tx_hash, amount, previous_reward, added_slot, withdrawal, post_snapshot)
VALUES (?, 0, ?, ?, ?, ?, ?, ?)`, key, h, amount, previous, slot, withdrawal, post)
	}
	for i, key := range keys {
		legacy(key, hash(byte(0x20+i)), "10", nil, 700, false, false)
	}
	legacy(keys[0], bytes.Repeat([]byte{0xc1}, 28), "7", nil, 700, false, true)
	legacy(
		keys[1],
		[]byte("dingo:mir:\x00\x00\x00\x00\x00\x00\x00\x00"),
		"5",
		nil,
		710,
		false,
		false,
	)
	for i := range 2 {
		h := hash(byte(0x30 + i))
		exec(
			`INSERT INTO `+tx+` (hash, slot, block_index) VALUES (?, ?, 0)`,
			h,
			720+i,
		)
		legacy(keys[i], h, "4", "50", int64(720+i), true, false)
	}
	runTo(registry[:index+1])

	count := func(query string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&n))
		return n
	}
	require.Equal(t, 4, count(`SELECT COUNT(*) FROM reward_credit`))
	require.Equal(t, 1, count(`SELECT COUNT(*) FROM reward_credit_slot`))
	require.Equal(t, 2, count(`SELECT COUNT(*) FROM reward_event_credit`))
	require.Equal(t, 2, count(`SELECT COUNT(*) FROM reward_withdrawal`))
	_, err = db.ExecContext(ctx, `SELECT COUNT(*) FROM account_reward_delta`)
	require.Error(t, err, "the legacy table is dropped")

	var store *Store
	switch dialect.Name() {
	case "postgres":
		store = newIntegrationSQLStore(t, "pgx", dsn, "postgres", lockNamespace)
	default:
		store = newIntegrationSQLStore(t, "mysql", dsn, "mysql", lockNamespace)
	}
	sums, err := store.GetAccountSumsByCredential(0, keys[0], nil)
	require.NoError(t, err)
	require.Equal(t, uint64(4), sums.WithdrawalsSum)
	history, err := store.GetAccountWithdrawalHistoryByCredential(
		0,
		keys[1],
		10,
		0,
		"desc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, uint64(721), history[0].TxSlot)
	events, err := store.GetPostSnapshotRewardCreditEvents(700, nil)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, bytes.Repeat([]byte{0xc1}, 28), events[0].TxHash)

	// New writes: an event credit, a stake-reward credit applied twice, and
	// a rollback across all of them.
	require.NoError(
		t,
		store.AddAccountRewardByCredential(0, keys[2], 9, 800, hash(0x40), nil),
	)
	require.NoError(
		t,
		store.AddAccountRewardByCredential(0, keys[2], 9, 800, hash(0x40), nil),
	)
	credit := models.AccountRewardCredit{
		StakingKey: keys[3], Amount: 6, Slot: 900, OutputID: 77,
	}
	for range 2 {
		require.NoError(
			t,
			store.AddAccountRewardsByCredential(
				[]models.AccountRewardCredit{credit},
				nil,
			),
		)
	}
	account, err := store.GetAccountByCredential(0, keys[2], false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(109), uint64(account.Reward))
	account, err = store.GetAccountByCredential(0, keys[3], false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(106), uint64(account.Reward))
	atSlot, err := store.GetAccountsByCredentialAtSlot(
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(0, keys[3]),
		},
		850,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(100),
		uint64(
			atSlot[models.NewStakeCredentialRef(0, keys[3]).MapKey()].Reward,
		),
	)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(750, nil))
	for i, want := range []uint64{100, 100, 100, 100} {
		account, err := store.GetAccountByCredential(0, keys[i], false, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			want,
			uint64(account.Reward),
			"account %d after rollback",
			i,
		)
	}
	require.Equal(
		t,
		2,
		count(`SELECT COUNT(*) FROM reward_withdrawal`),
		"withdrawals below the slot stay",
	)
	require.Equal(t, 4, count(`SELECT COUNT(*) FROM reward_credit`))
}
