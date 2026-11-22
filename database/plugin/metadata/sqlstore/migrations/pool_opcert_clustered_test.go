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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const poolOpCertClusteredVersion = 80

// legacyPoolOpCert is one by-value pool_opcert_sequence row. A nil Slot or
// Pool leaves the column NULL, as the nullable legacy columns allow.
type legacyPoolOpCert struct {
	Pool     []byte
	Slot     *int64
	Sequence int64
}

var poolOpCertRows = []legacyPoolOpCert{
	{credentialKey(0xc1), int64Ptr(10), 1},
	{credentialKey(0xc2), int64Ptr(11), 1},
	{credentialKey(0xc1), int64Ptr(20), 2},
	{credentialKey(0xc3), int64Ptr(21), 7},
	{credentialKey(0xc2), int64Ptr(30), 1},
	{credentialKey(0xc1), int64Ptr(40), 2},
}

func poolOpCertDB(t *testing.T, rows []legacyPoolOpCert) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, poolOpCertClusteredVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// A credential the dictionary already holds for another purpose is reused.
	_, err = db.Exec(`INSERT INTO credential (hash) VALUES (?)`,
		credentialKey(0xc1))
	require.NoError(t, err)
	for _, row := range rows {
		var slot any
		if row.Slot != nil {
			slot = *row.Slot
		}
		_, err := db.Exec(`
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, ?)`, row.Pool, slot, row.Sequence)
		require.NoError(t, err)
	}
	return db, path
}

func runPoolOpCertClustered(
	t *testing.T,
	db *sql.DB,
	path string,
	batchSize int,
) error {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[poolOpCertClusteredVersion-1]
	require.Equal(t, "pool-opcert-clustered", migration.Name)
	require.NotNil(t, migration.Backfill)
	if batchSize > 0 {
		migration.BatchSize = batchSize
	}
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
	return runner.Run(context.Background())
}

func poolOpCertContents(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT c.hash, o.slot, o.sequence FROM pool_opcert_sequence o
JOIN credential c ON c.id = o.pool_key_id ORDER BY o.slot`)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var pool []byte
		var slot, sequence int64
		require.NoError(t, rows.Scan(&pool, &slot, &sequence))
		out = append(out, fmt.Sprintf("%x/%d/%d", pool, slot, sequence))
	}
	require.NoError(t, rows.Err())
	return out
}

// TestPoolOpCertClusteredKeepsRowsAndHoldsEachPoolOnce upgrades databases with
// rows spread over several batches and checks every keyed row survives with its
// pool, slot and counter, each pool key is one dictionary row, the table is
// keyed by (pool credential id, slot) and the by-value column and its indexes
// are gone.
func TestPoolOpCertClusteredKeepsRowsAndHoldsEachPoolOnce(t *testing.T) {
	t.Parallel()
	var want []string
	for _, row := range poolOpCertRows {
		want = append(
			want,
			fmt.Sprintf("%x/%d/%d", row.Pool, *row.Slot, row.Sequence),
		)
	}
	for _, batchSize := range []int{0, 1, 2, 4, 6, 7} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()
			db, path := poolOpCertDB(t, poolOpCertRows)
			require.NoError(t, runPoolOpCertClustered(t, db, path, batchSize))

			require.Equal(t, want, poolOpCertContents(t, db))
			var pools int
			require.NoError(t, db.QueryRow(
				`SELECT COUNT(*) FROM credential`).Scan(&pools))
			require.Equal(t, 3, pools, "one dictionary row per pool key")

			require.Equal(t,
				[]string{"pool_key_id", "slot", "sequence"},
				tableColumns(t, db, "pool_opcert_sequence"))
			require.Equal(t, []string{
				"idx_pool_opcert_sequence_pool_sequence",
				"idx_pool_opcert_sequence_slot",
			}, tableIndexes(t, db, "pool_opcert_sequence"))
			var primary []string
			pk, err := db.Query(`
SELECT name FROM pragma_table_info('pool_opcert_sequence')
WHERE pk > 0 ORDER BY pk`)
			require.NoError(t, err)
			for pk.Next() {
				var name string
				require.NoError(t, pk.Scan(&name))
				primary = append(primary, name)
			}
			require.NoError(t, pk.Err())
			require.NoError(t, pk.Close())
			require.Equal(t, []string{"pool_key_id", "slot"}, primary)
			var withoutRowID int
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE name = 'pool_opcert_sequence' AND sql LIKE '%WITHOUT ROWID%'`,
			).Scan(&withoutRowID))
			require.Equal(t, 1, withoutRowID, "the table is clustered")
			var copies int
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE name = 'pool_opcert_sequence_clustered'`).Scan(&copies))
			require.Zero(t, copies)
		})
	}
}

// TestPoolOpCertClusteredDropsRowsWithoutPoolOrSlot pins that a legacy row that
// cannot be keyed is dropped and every legacy row is accounted for.
func TestPoolOpCertClusteredDropsRowsWithoutPoolOrSlot(t *testing.T) {
	t.Parallel()
	rows := append([]legacyPoolOpCert{
		{credentialKey(0xc4), nil, 3},
		{nil, int64Ptr(50), 3},
		{[]byte{}, int64Ptr(51), 3},
	}, poolOpCertRows...)
	db, path := poolOpCertDB(t, rows)
	require.NoError(t, runPoolOpCertClustered(t, db, path, 2))
	var count int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM pool_opcert_sequence`).Scan(&count))
	require.Equal(t, len(poolOpCertRows), count)
}

func TestPoolOpCertClusteredOnEmptyTable(t *testing.T) {
	t.Parallel()
	db, path := poolOpCertDB(t, nil)
	require.NoError(t, runPoolOpCertClustered(t, db, path, 0))
	require.Contains(
		t,
		tableColumns(t, db, "pool_opcert_sequence"),
		"pool_key_id",
	)
}

// TestPoolOpCertClusteredRefusesAnInconsistentCopy pins the row-count
// reconciliation: a copy holding a row the backfill did not copy must not
// replace the legacy table.
func TestPoolOpCertClusteredRefusesAnInconsistentCopy(t *testing.T) {
	t.Parallel()
	db, path := poolOpCertDB(t, poolOpCertRows)
	_, err := db.Exec(`
CREATE TABLE pool_opcert_sequence_clustered (pool_key_id integer NOT NULL,
 slot integer NOT NULL, sequence integer, PRIMARY KEY (pool_key_id, slot))
 WITHOUT ROWID`)
	require.NoError(t, err)
	_, err = db.Exec(`
INSERT INTO pool_opcert_sequence_clustered VALUES (999, 999, 1)`)
	require.NoError(t, err)
	err = runPoolOpCertClustered(t, db, path, 0)
	require.ErrorContains(t, err, "rebuild is incomplete")
	require.Contains(t, tableColumns(t, db, "pool_opcert_sequence"),
		"pool_key_hash", "the legacy table stays")
}
