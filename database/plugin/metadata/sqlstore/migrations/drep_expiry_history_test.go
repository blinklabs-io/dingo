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

// TestDrepExpiryHistoryBackfillDatesCurrentExpiry covers the v37 seed row
// each existing DRep gets: its current expiry, dated at the latest event that
// could have set it.
func TestDrepExpiryHistoryBackfillDatesCurrentExpiry(t *testing.T) {
	t.Parallel()

	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open(
		"sqlite",
		"file:"+databasePath+
			"?_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 39)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	runTo(registry[:36])
	voted := []byte{0x01}
	updated := []byte{0x02}
	untouched := []byte{0x03}
	for _, row := range []struct {
		credential []byte
		addedSlot  uint64
		activity   uint64
		expiry     uint64
	}{
		{voted, 100, 10, 30},
		{updated, 200, 3, 23},
		{untouched, 50, 1, 21},
	} {
		_, err = db.Exec(
			"INSERT INTO drep (credential_tag, credential, added_slot, "+
				"last_activity_epoch, expiry_epoch, active) "+
				"VALUES (0, ?, ?, ?, ?, TRUE)",
			row.credential, row.addedSlot, row.activity, row.expiry,
		)
		require.NoError(t, err)
	}
	_, err = db.Exec(
		"INSERT INTO governance_proposal (tx_hash, action_index, " +
			"action_type, proposed_epoch, expires_epoch, deposit, " +
			"added_slot) VALUES (X'aa', 0, 6, 1, 10, 0, 90)",
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO governance_vote (proposal_id, voter_type, "+
			"voter_credential_tag, voter_credential, vote, added_slot, "+
			"vote_updated_slot) VALUES (1, 1, 0, ?, 1, 400, 500)",
		voted,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO update_drep (credential_tag, credential, added_slot) "+
			"VALUES (0, ?, 300)",
		updated,
	)
	require.NoError(t, err)

	runTo(registry)

	for _, want := range []struct {
		credential []byte
		slot       uint64
		activity   uint64
		expiry     uint64
	}{
		{voted, 500, 10, 30},
		{updated, 300, 3, 23},
		{untouched, 50, 1, 21},
	} {
		var slot, activity, expiry uint64
		require.NoError(t, db.QueryRow(
			"SELECT added_slot, last_activity_epoch, expiry_epoch "+
				"FROM drep_expiry_history "+
				"WHERE credential_tag = 0 AND credential = ?",
			want.credential,
		).Scan(&slot, &activity, &expiry))
		require.Equal(t, want.slot, slot)
		require.Equal(t, want.activity, activity)
		require.Equal(t, want.expiry, expiry)
	}
}
