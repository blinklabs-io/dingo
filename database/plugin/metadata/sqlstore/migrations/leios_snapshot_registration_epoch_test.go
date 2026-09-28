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
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

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
