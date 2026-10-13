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

package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestParseAddColumnStatement covers the parser the per-dialect
// already-applied classifiers use to confirm a replayed expand statement.
// Quoting differs per dialect, and a statement that is not an ADD COLUMN must
// not be claimed, or an unrelated "already exists" failure would be swallowed.
func TestParseAddColumnStatement(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		statement  string
		table      string
		column     string
		definition string
		ok         bool
	}{
		{
			name:       "backtick quoted",
			statement:  "ALTER TABLE `committee_member` ADD COLUMN `term_start_slot_set` boolean NOT NULL DEFAULT false",
			table:      "committee_member",
			column:     "term_start_slot_set",
			definition: "boolean NOT NULL DEFAULT false",
			ok:         true,
		},
		{
			name:       "double quoted",
			statement:  `ALTER TABLE "committee_member" ADD COLUMN "cold_credential_tag" BIGINT NOT NULL DEFAULT 0`,
			table:      "committee_member",
			column:     "cold_credential_tag",
			definition: "BIGINT NOT NULL DEFAULT 0",
			ok:         true,
		},
		{
			name:       "unquoted with trailing semicolon",
			statement:  "ALTER TABLE account_import_baseline ADD COLUMN deposit_amount text;",
			table:      "account_import_baseline",
			column:     "deposit_amount",
			definition: "text",
			ok:         true,
		},
		{
			name:       "lowercase keywords",
			statement:  "alter table `t` add column `c` integer",
			table:      "t",
			column:     "c",
			definition: "integer",
			ok:         true,
		},
		{
			// A same-named index is a different object; claiming it would let
			// a genuine duplicate-index failure be treated as applied.
			name:      "create index is not an add column",
			statement: "CREATE INDEX `idx_a` ON `t`(`c`)",
			ok:        false,
		},
		{
			name:      "drop column is not an add column",
			statement: "ALTER TABLE `t` DROP COLUMN `c`",
			ok:        false,
		},
		{
			name:      "truncated add column",
			statement: "ALTER TABLE `t` ADD COLUMN",
			ok:        false,
		},
		{
			name:      "empty statement",
			statement: "",
			ok:        false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table, column, definition, ok := parseAddColumnStatement(
				test.statement,
			)
			require.Equal(t, test.ok, ok)
			if !test.ok {
				return
			}
			require.Equal(t, test.table, table)
			require.Equal(t, test.column, column)
			require.Equal(t, test.definition, definition)
		})
	}
}

// The committee term-start backfill is data driven, so its SQL never passes
// through the migration DDL translator. A dialect that does not take ? has to
// see its own placeholders, which is why Batch carries the rebinder.
func TestCommitteeTermStartBackfillUsesDialectPlaceholders(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := SQLiteRegistry()
	require.NoError(t, err)

	var rebound []string
	runner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   NewFileLocker(databasePath + ".migrate.lock"),
		Rebind: func(query string) string {
			if strings.Contains(query, "committee_member") {
				rebound = append(rebound, query)
			}
			return query
		},
	}
	require.NoError(t, runner.Run(context.Background()))

	require.NotEmpty(
		t,
		rebound,
		"the committee term-start backfill must route its SQL through Batch.Rebind",
	)
	for _, query := range rebound {
		require.Contains(
			t,
			query,
			"?",
			"the backfill must hand the rebinder ? placeholders",
		)
	}
}

// Batch.Rebind is never nil, so a backfill can call it unconditionally.
func TestBackfillBatchRebindDefaultsToIdentity(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	runner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   NewFileLocker(databasePath + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))
}

// TestBackfillRebindFallsBackToDialectRebinder proves that a Runner built
// without an explicit Rebind still rewrites placeholders for a dialect that
// rejects `?`. An identity fallback would hand `?` straight to PostgreSQL and
// fail the committee term-start backfill.
func TestBackfillRebindFallsBackToDialectRebinder(t *testing.T) {
	postgres := Runner{Dialect: "postgres"}
	require.Equal(
		t,
		"SELECT id FROM t WHERE a = $1 AND b = $2",
		postgres.backfillRebind()("SELECT id FROM t WHERE a = ? AND b = ?"),
	)

	// A dialect that takes ? directly is unchanged.
	sqlite := Runner{Dialect: "sqlite"}
	require.Equal(
		t,
		"SELECT id FROM t WHERE a = ?",
		sqlite.backfillRebind()("SELECT id FROM t WHERE a = ?"),
	)

	// An explicit override still wins over the dialect rebinder.
	explicit := Runner{
		Dialect: "postgres",
		Rebind:  func(string) string { return "override" },
	}
	require.Equal(t, "override", explicit.backfillRebind()("anything"))
}

func TestLeiosSnapshotRegistrationEpochBackfill(t *testing.T) {
	t.Parallel()

	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for _, statement := range []string{
		`CREATE TABLE pool_stake_snapshot (
			id INTEGER PRIMARY KEY, epoch INTEGER, pool_key_hash BLOB,
			leios_key_public BLOB, leios_key_possession_proof BLOB,
			leios_key_registration_epoch INTEGER, captured_slot INTEGER
		)`,
		`CREATE TABLE pool_registration (
			id INTEGER PRIMARY KEY, pool_key_hash BLOB, added_slot INTEGER,
			leios_key_public BLOB, leios_key_possession_proof BLOB,
			leios_key_registration_age_unknown BOOLEAN,
			leios_key_registration_epoch INTEGER
		)`,
		`CREATE TABLE epoch (
			id INTEGER PRIMARY KEY, epoch_id INTEGER, start_slot INTEGER,
			length_in_slots INTEGER
		)`,
		`INSERT INTO epoch VALUES (1, 5, 0, 100), (2, 6, 100, 100)`,
		`INSERT INTO pool_registration VALUES
			(1, X'01', 100, X'11', X'21', FALSE, NULL),
			(2, X'02', 999, X'12', X'22', FALSE, 4),
			(3, X'03', 50, X'13', X'23', TRUE, NULL)`,
		`INSERT INTO pool_stake_snapshot VALUES
			(1, 7, X'01', X'11', X'21', NULL, 199),
			(2, 5, X'02', X'12', X'22', NULL, 100),
			(3, 7, X'03', X'13', X'23', NULL, 199),
			(4, 7, X'04', X'14', X'24', NULL, 199)`,
	} {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
	for _, statement := range registry[28].SQL["sqlite"].Expand {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		id   int
		want sql.NullInt64
	}{
		{id: 1, want: sql.NullInt64{Int64: 7, Valid: true}},
		{id: 2, want: sql.NullInt64{Int64: 4, Valid: true}},
		{id: 3, want: sql.NullInt64{}},
		{id: 4, want: sql.NullInt64{}},
	} {
		var got sql.NullInt64
		err := db.QueryRow(
			`SELECT leios_key_registration_epoch
			 FROM pool_stake_snapshot WHERE id = ?`,
			tc.id,
		).Scan(&got)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "snapshot row %d", tc.id)
	}
}

func TestRewardCreditRoundBackfillMovesLegacyState(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	locker := NewFileLocker(databasePath + ".migrate.lock")
	legacyRunner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry[:30],
		Locker:   locker,
	}
	require.NoError(t, legacyRunner.Run(context.Background()))

	want := make([]models.RewardCreditRound, 1_005)
	for i := range want {
		want[i] = models.RewardCreditRound{
			SnapshotEpoch: uint64(i),
			BoundarySlot:  uint64(i*10 + 5),
		}
	}
	raw, err := json.Marshal(want)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)`,
		models.PendingRewardCreditRoundsKey,
		string(raw),
	)
	require.NoError(t, err)

	runner := Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   locker,
	}
	require.NoError(t, runner.Run(context.Background()))

	rows, err := db.Query(
		`SELECT snapshot_epoch, boundary_slot FROM reward_credit_round ORDER BY snapshot_epoch`,
	)
	require.NoError(t, err)
	defer rows.Close()
	var got []models.RewardCreditRound
	for rows.Next() {
		var epoch, slot int64
		require.NoError(t, rows.Scan(&epoch, &slot))
		got = append(got, models.RewardCreditRound{
			SnapshotEpoch: uint64(epoch),
			BoundarySlot:  uint64(slot),
		})
	}
	require.NoError(t, rows.Err())
	require.Equal(t, want, got)
	var legacyCount int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM sync_state WHERE sync_key = ?`,
		models.PendingRewardCreditRoundsKey,
	).Scan(&legacyCount)
	require.NoError(t, err)
	require.Zero(t, legacyCount, "the legacy state key should be consumed")
}
