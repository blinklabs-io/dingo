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
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// The account history queries must not depend on sqlite_stat1 cardinality
// for "transaction". After a Mithril bootstrap the statistics are recorded
// while the table is nearly empty and stay stale until the next epoch
// rollover; a plan that scans "transaction" per matching certificate then
// costs time proportional to the transactions applied since startup.
func TestAccountHistoryPlansWithStaleTransactionStats(
	t *testing.T,
) {
	t.Parallel()
	db, err := OpenDB(
		"sqlite",
		filepath.Join(t.TempDir(), "metadata.sqlite"),
		"sqlite",
		false,
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	key := make([]byte, 28)
	key[0] = 1
	_, err = store.writeDB.Exec(
		`INSERT INTO "transaction" (id, slot, block_index) VALUES (1, 1, 0)`,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO certs (id, transaction_id, slot, cert_index) VALUES (1, 1, 1, 0)`,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO stake_registration (staking_key, credential_tag, certificate_id, added_slot, deposit_amount)
VALUES (?, 0, 1, 1, '2000000')`,
		key,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec("ANALYZE")
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
WITH digits(d) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9))
INSERT INTO "transaction" (id, slot, block_index)
SELECT n + 2, n + 2, 0
FROM (
    SELECT d0.d + 10*d1.d + 100*d2.d + 1000*d3.d + 10000*d4.d AS n
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
    CROSS JOIN digits d4
)
`)
	require.NoError(t, err)
	var stat string
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT stat FROM sqlite_stat1 WHERE tbl = 'transaction' AND idx = 'idx_transaction_hash'`,
	).Scan(&stat))
	require.Equal(t, "1 1", stat, "statistics must still be stale")

	queries := []struct {
		name  string
		build func(uint8, []byte) (string, []any)
	}{
		{"registration", accountRegistrationHistoryQuery},
		{"delegation", accountDelegationHistoryQuery},
	}
	for _, q := range queries {
		name := q.name
		query, args := q.build(0, key)
		rows, err := store.writeDB.QueryContext(
			context.Background(),
			"EXPLAIN QUERY PLAN "+query,
			args...,
		)
		require.NoError(t, err)
		var details []string
		for rows.Next() {
			var id, parent, notUsed int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
			details = append(details, detail)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		plan := strings.Join(details, "\n")
		require.NotContains(t, plan, "SCAN tx", "%s plan:\n%s", name, plan)
		require.Contains(
			t,
			plan,
			"SEARCH tx USING INTEGER PRIMARY KEY",
			"%s plan:\n%s",
			name,
			plan,
		)
	}
}
