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
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// TestGovernanceVoteTombstoneBackfill covers v66: a vote that is soft-deleted
// gets one deletion entry in the vote journal, and a live vote gets none.
func TestGovernanceVoteTombstoneBackfill(t *testing.T) {
	t.Parallel()

	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, databasePath, 65)
	db, err := sql.Open(
		"sqlite",
		"file:"+databasePath+
			"?_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	_, err = db.Exec(
		"INSERT INTO governance_proposal (tx_hash, action_index, " +
			"action_type, proposed_epoch, expires_epoch, deposit, " +
			"added_slot) VALUES (X'aa', 0, 6, 1, 10, 0, 90)",
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO governance_vote (id, proposal_id, voter_type, " +
			"voter_credential_tag, voter_credential, vote, added_slot, " +
			"deleted_slot) VALUES (1, 1, 1, 0, X'01', 1, 150, 200), " +
			"(2, 1, 1, 0, X'02', 0, 150, NULL)",
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO governance_vote_history (vote_id, transition_slot, " +
			"vote) VALUES (1, 150, 1), (2, 150, 0)",
	)
	require.NoError(t, err)

	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker: migrations.NewFileLocker(
			databasePath + ".migrate.lock",
		),
	}
	require.NoError(t, runner.Run(context.Background()))
	require.NoError(t, runner.Run(context.Background()))

	rows, err := db.Query(
		"SELECT vote_id, transition_slot, vote FROM " +
			"governance_vote_history WHERE deleted = TRUE",
	)
	require.NoError(t, err)
	defer rows.Close()
	var got [][3]int
	for rows.Next() {
		var row [3]int
		require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
		got = append(got, row)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, [][3]int{{1, 200, 1}}, got)
}
