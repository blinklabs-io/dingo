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

package migrations_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const rewardJournalVersion = 57

// execAll runs statements and fails the test on the first error.
func execAll(t *testing.T, db *sql.DB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		_, err := db.ExecContext(context.Background(), statement)
		require.NoError(t, err, statement)
	}
}

func hexBlob(b []byte) string { return fmt.Sprintf("x'%x'", b) }

// seedLegacyRewardJournal fills a v55 database with every class of row the
// legacy journal holds and returns how many of each it wrote.
func seedLegacyRewardJournal(
	t *testing.T,
	db *sql.DB,
) (credits, events, withdrawals int) {
	t.Helper()
	pool := bytes.Repeat([]byte{0x71}, 28)
	for i := 1; i <= 6; i++ {
		key := bytes.Repeat([]byte{byte(i)}, 28)
		execAll(t, db, fmt.Sprintf(`INSERT INTO account
(staking_key, credential_tag, added_slot, created_slot, active, reward)
VALUES (%s, %d, 1, 1, TRUE, '%d')`, hexBlob(key), i%2, i*100))
	}
	// Epochs 5..8 start at slots 500..800. The round at 700 (snapshot 5) was
	// credited through the deferred path and has a reward_credit_round row;
	// the rounds at 500 and 600 were applied eagerly and have none.
	for e := 5; e <= 8; e++ {
		execAll(t, db, fmt.Sprintf(
			`INSERT INTO epoch (epoch_id, start_slot) VALUES (%d, %d)`,
			e,
			e*100,
		))
	}
	execAll(
		t,
		db,
		`INSERT INTO reward_credit_round (snapshot_epoch, boundary_slot) VALUES (5, 700)`,
	)
	// Outputs: the eager rounds' (snapshot 3 at 500, snapshot 4 at 600) were
	// never marked folded; the deferred round's partly are.
	for _, round := range []struct{ epoch, slot int }{{3, 500}, {4, 600}, {5, 700}} {
		for i := 1; i <= 6; i++ {
			key := bytes.Repeat([]byte{byte(i)}, 28)
			folded := round.epoch == 5 && i <= 3
			execAll(t, db, fmt.Sprintf(`INSERT INTO reward_account_output
(staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount,
 spendable, guarded, captured_slot, boundary_slot, folded)
VALUES (%s, %s, 'member', %d, %d, '10', TRUE, FALSE, %d, %d, %v)`,
				hexBlob(key), hexBlob(pool), round.epoch, i%2,
				round.slot-1, round.slot, folded))
		}
	}
	journal := func(i int, hash []byte, amount, previous string, slot int,
		withdrawal, post bool,
	) {
		key := bytes.Repeat([]byte{byte(i)}, 28)
		prev := "NULL"
		if previous != "" {
			prev = "'" + previous + "'"
		}
		execAll(t, db, fmt.Sprintf(
			`INSERT INTO account_reward_delta
(staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot,
 withdrawal, post_snapshot) VALUES (%s, %d, %s, '%s', %s, %d, %v, %v)`,
			hexBlob(
				key,
			),
			i%2,
			hexBlob(hash),
			amount,
			prev,
			slot,
			withdrawal,
			post,
		))
	}
	hash32 := func(seed int) []byte {
		return bytes.Repeat([]byte{byte(0xa0 + seed)}, 32)
	}
	n := 0
	// Round credits at the deferred round and at the two eager boundaries.
	for _, slot := range []int{500, 600, 700} {
		for i := 1; i <= 6; i++ {
			n++
			journal(i, hash32(n%200), fmt.Sprint(10+i), "", slot, false, false)
			credits++
		}
	}
	// Event credits: pool-retirement refund, a MIR repeated at two slots, and
	// a governance credit, each marked post-snapshot except the MIR.
	journal(1, bytes.Repeat([]byte{0xc1}, 28), "7", "", 600, false, true)
	journal(
		2,
		[]byte("dingo:mir:\x00\x00\x00\x00\x00\x00\x00\x00"),
		"5",
		"",
		600,
		false,
		false,
	)
	journal(
		2,
		[]byte("dingo:mir:\x00\x00\x00\x00\x00\x00\x00\x00"),
		"9",
		"",
		700,
		false,
		false,
	)
	journal(3, bytes.Repeat([]byte{0xd1}, 32), "11", "", 700, false, true)
	journal(4, hash32(150), "13", "", 650, false, false)
	events = 5
	// Withdrawals: owned by transaction rows; one with a NULL previous.
	for i := 1; i <= 4; i++ {
		txHash := bytes.Repeat([]byte{byte(0xe0 + i)}, 32)
		execAll(t, db, fmt.Sprintf(
			`INSERT INTO "transaction" (hash, slot, block_index) VALUES (%s, %d, 0)`,
			hexBlob(txHash),
			640+i,
		))
		previous := fmt.Sprint(20 * i)
		if i == 4 {
			previous = ""
		}
		journal(i, txHash, fmt.Sprint(i), previous, 640+i, true, false)
		withdrawals++
	}
	return credits, events, withdrawals
}

// rewardJournalDigest lists the migrated rows with account keys in place of
// ids, so databases that migrated through different batch splits compare equal.
func rewardJournalDigest(t *testing.T, db *sql.DB) string {
	t.Helper()
	var lines []string
	collect := func(label, query string) {
		rows, err := db.QueryContext(context.Background(), query)
		require.NoError(t, err, query)
		defer rows.Close()
		for rows.Next() {
			var a, b, c, d, e sql.NullString
			require.NoError(t, rows.Scan(&a, &b, &c, &d, &e))
			lines = append(lines, strings.Join([]string{
				label, a.String, b.String, c.String, d.String, e.String,
			}, "|"))
		}
		require.NoError(t, rows.Err())
	}
	collect(
		"credit",
		`SELECT c.boundary_slot, hex(a.staking_key), c.output_id, c.amount, ''
FROM reward_credit c JOIN account a ON a.id = c.account_id`,
	)
	collect(
		"slot",
		`SELECT boundary_slot, '', '', '', '' FROM reward_credit_slot`,
	)
	collect(
		"event",
		`SELECT hex(s.hash), hex(a.staking_key), e.slot, e.amount, e.post_snapshot
FROM reward_event_credit e JOIN account a ON a.id = e.account_id
JOIN reward_source s ON s.id = e.source_id`,
	)
	collect(
		"withdrawal",
		`SELECT hex(t.hash), hex(a.staking_key), w.slot, w.amount, w.previous_reward
FROM reward_withdrawal w JOIN account a ON a.id = w.account_id
JOIN "transaction" t ON t.id = w.transaction_id`,
	)
	collect(
		"round",
		`SELECT snapshot_epoch, boundary_slot, '', '', '' FROM reward_credit_round`,
	)
	collect(
		"folded",
		`SELECT epoch, COUNT(*), '', '', '' FROM reward_account_output
WHERE folded GROUP BY epoch`,
	)
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// expectedMigratedRewardRows lists the rows seedLegacyRewardJournal's journal
// must migrate to, by the first byte of the account's staking key.
func expectedMigratedRewardRows() []string {
	var rows []string
	for _, slot := range []int{500, 600, 700} {
		for i := 1; i <= 6; i++ {
			rows = append(rows, fmt.Sprintf("credit|%02X|%d|%d", i, slot, 10+i))
		}
	}
	rows = append(rows,
		"event|01|600|7|1",
		"event|02|600|5|0",
		"event|04|650|13|0",
		"event|02|700|9|0",
		"event|03|700|11|1",
	)
	for i := 1; i <= 4; i++ {
		previous := 20 * i
		if i == 4 {
			previous = 0
		}
		rows = append(rows,
			fmt.Sprintf("withdrawal|%02X|%d|%d|%d", i, 640+i, i, previous))
	}
	return rows
}

func migratedRewardRows(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
SELECT 'credit|' || hex(substr(a.staking_key, 1, 1)) || '|' || c.boundary_slot
    || '|' || c.amount, 0, c.boundary_slot, a.id
FROM reward_credit c JOIN account a ON a.id = c.account_id
UNION ALL
SELECT 'event|' || hex(substr(a.staking_key, 1, 1)) || '|' || e.slot || '|'
    || e.amount || '|' || e.post_snapshot, 1, e.slot, a.id
FROM reward_event_credit e JOIN account a ON a.id = e.account_id
UNION ALL
SELECT 'withdrawal|' || hex(substr(a.staking_key, 1, 1)) || '|' || w.slot
    || '|' || w.amount || '|' || COALESCE(w.previous_reward, '0'), 2, w.slot,
    a.id
FROM reward_withdrawal w JOIN account a ON a.id = w.account_id
ORDER BY 2, 3, 4`)
	require.NoError(t, err)
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var line string
		var kind, slot, account int64
		require.NoError(t, rows.Scan(&line, &kind, &slot, &account))
		ret = append(ret, line)
	}
	require.NoError(t, rows.Err())
	return ret
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		name,
	).Scan(&n))
	return n == 1
}

func count(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), query).Scan(&n))
	return n
}

func TestRewardJournalMigrationMovesEveryLegacyRow(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name string
		prep []string
	}{
		{name: "main shape"},
		{name: "indexes already dropped", prep: []string{
			`DROP INDEX idx_account_reward_delta_tx_hash`,
			`DROP INDEX idx_account_reward_delta_withdrawal`,
		}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			db, runTo := migratedPrefixDB(t, rewardJournalVersion-1)
			execAll(t, db, shape.prep...)
			credits, events, withdrawals := seedLegacyRewardJournal(t, db)
			registry, err := migrations.SQLiteRegistry()
			require.NoError(t, err)
			runTo(registry)

			require.False(t, tableExists(t, db, "account_reward_delta"))
			require.Equal(
				t,
				credits,
				count(t, db, `SELECT COUNT(*) FROM reward_credit`),
			)
			require.Equal(
				t,
				events,
				count(t, db, `SELECT COUNT(*) FROM reward_event_credit`),
			)
			require.Equal(
				t,
				withdrawals,
				count(t, db, `SELECT COUNT(*) FROM reward_withdrawal`),
			)
			// The MIR source is shared by its two slots.
			require.Equal(
				t,
				events-1,
				count(t, db, `SELECT COUNT(*) FROM reward_source`),
			)
			// Credits keep their slots: one entry per credited boundary, the
			// round table and the outputs' folded flags untouched.
			require.Equal(
				t,
				3,
				count(t, db, `SELECT COUNT(*) FROM reward_credit_slot`),
			)
			require.Equal(
				t,
				1,
				count(t, db, `SELECT COUNT(*) FROM reward_credit_round`),
			)
			require.Equal(
				t,
				3,
				count(
					t,
					db,
					`SELECT COUNT(*) FROM reward_account_output WHERE folded`,
				),
			)
			require.Equal(
				t,
				1,
				count(t, db, `SELECT COUNT(*) FROM reward_event_credit
WHERE post_snapshot AND slot = 700`),
			)
			require.Equal(
				t,
				1,
				count(t, db, `SELECT COUNT(*) FROM reward_withdrawal
WHERE amount = '4' AND previous_reward = '0'`),
				"a NULL previous reward reads as zero",
			)
			require.Equal(
				t,
				expectedMigratedRewardRows(),
				migratedRewardRows(t, db),
				"each legacy row keeps its account, amount and slot",
			)

			// A second run finds nothing to do.
			digest := rewardJournalDigest(t, db)
			runTo(registry)
			require.Equal(t, digest, rewardJournalDigest(t, db))
		})
	}
}

func TestRewardJournalMigrationResumesAfterInterruption(t *testing.T) {
	t.Parallel()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	reference, referenceRun := migratedPrefixDB(t, rewardJournalVersion-1)
	seedLegacyRewardJournal(t, reference)
	referenceRun(registry)
	want := rewardJournalDigest(t, reference)

	counter, counterRun := migratedPrefixDB(t, rewardJournalVersion-1)
	seedLegacyRewardJournal(t, counter)
	counted := append([]migrations.Migration(nil), registry...)
	counted[rewardJournalVersion-1].BatchSize = 3
	countedBackfill := counted[rewardJournalVersion-1].Backfill
	calls := 0
	counted[rewardJournalVersion-1].Backfill = func(ctx context.Context, batch migrations.Batch) (migrations.BatchResult, error) {
		calls++
		return countedBackfill(ctx, batch)
	}
	counterRun(counted)
	require.Equal(t, want, rewardJournalDigest(t, counter))
	require.Greater(t, calls, 2)

	errInterrupted := errors.New("interrupted")
	// Crash after the nth batch commits, for every n before the last: each
	// restart resumes from the committed cursor and ends in the same state.
	for crashAfter := 1; crashAfter < calls; crashAfter++ {
		t.Run(fmt.Sprintf("after batch %d", crashAfter), func(t *testing.T) {
			t.Parallel()
			db, runTo := migratedPrefixDB(t, rewardJournalVersion-1)
			seedLegacyRewardJournal(t, db)
			interrupted := append([]migrations.Migration(nil), registry...)
			migration := interrupted[rewardJournalVersion-1]
			migration.BatchSize = 3
			inner := migration.Backfill
			calls := 0
			migration.Backfill = func(ctx context.Context, batch migrations.Batch) (migrations.BatchResult, error) {
				if calls >= crashAfter {
					return migrations.BatchResult{}, errInterrupted
				}
				calls++
				return inner(ctx, batch)
			}
			interrupted[rewardJournalVersion-1] = migration
			runner := migrations.Runner{
				DB: db, Dialect: "sqlite", Registry: interrupted,
				Locker: migrations.NewProcessLocker(),
			}
			require.ErrorIs(t, runner.Run(context.Background()), errInterrupted)
			require.True(t, tableExists(t, db, "account_reward_delta"),
				"the legacy table survives an interrupted backfill")
			resumed := append([]migrations.Migration(nil), registry...)
			resumed[rewardJournalVersion-1].BatchSize = 3
			runTo(resumed)
			require.False(t, tableExists(t, db, "account_reward_delta"))
			require.Equal(t, want, rewardJournalDigest(t, db))
		})
	}
}

func TestRewardJournalMigrationRefusesRowsItCannotMove(t *testing.T) {
	t.Parallel()
	db, _ := migratedPrefixDB(t, rewardJournalVersion-1)
	seedLegacyRewardJournal(t, db)
	// A withdrawal whose transaction row is gone cannot be keyed by
	// transaction id; the migration must stop with the legacy table intact
	// rather than drop it.
	execAll(t, db, `DELETE FROM "transaction" WHERE hash = `+
		hexBlob(bytes.Repeat([]byte{0xe2}, 32)))
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB: db, Dialect: "sqlite", Registry: registry,
		Locker: migrations.NewProcessLocker(),
	}
	err = runner.Run(context.Background())
	require.ErrorContains(t, err, "digest mismatch")
	require.True(t, tableExists(t, db, "account_reward_delta"))
}
