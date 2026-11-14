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

func poolCurrentStateRows(t *testing.T, db *sql.DB) []string {
	t.Helper()
	var out []string
	collect := func(query string, columns int) {
		rows, err := db.Query(query)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			v := make([]int64, columns)
			dest := make([]any, columns)
			for i := range v {
				dest[i] = &v[i]
			}
			require.NoError(t, rows.Scan(dest...))
			out = append(out, fmt.Sprint(v))
		}
		require.NoError(t, rows.Err())
	}
	collect(`
SELECT pool_id, horizon_slot
FROM pool_current_state ORDER BY pool_id`, 2)
	collect(`
SELECT pool_id, kind, event_rank, event_id, effective_slot, added_slot,
       synthetic, block_index, cert_index, registration_index, epoch
FROM pool_current_event ORDER BY pool_id, kind, event_rank`, 11)
	return out
}

// TestPoolCurrentStateBackfillsUpgradedDatabase upgrades a v64 database whose
// pools carry an import baseline with a tombstone, a registration after it, a
// retirement-only pool, same-slot registrations ordered by block index, and a
// closure context that moves a registration's effective slot, and requires
// the rows the ranking selects (every event while the epoch table is empty),
// a pool without history to get no row, and a replay of the migration to leave the rows unchanged.
func TestPoolCurrentStateBackfillsUpgradedDatabase(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, 64)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.Exec(query, args...)
		require.NoError(t, err, query)
	}
	for id := 1; id <= 4; id++ {
		hash := make([]byte, 28)
		hash[0] = byte(id)
		exec(
			`INSERT INTO pool (id, pool_key_hash, pledge, cost, latest_op_cert_sequence)
VALUES (?, ?, '0', '0', 0)`,
			id,
			hash,
		)
	}
	hash := func(id int) []byte {
		h := make([]byte, 28)
		h[0] = byte(id)
		return h
	}
	cert := func(id, slot, blockIndex, certIndex int) {
		txHash := make([]byte, 32)
		txHash[0] = byte(id)
		exec(`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`, id, txHash)
		exec(`INSERT INTO applied_transaction (tx_id, slot, block_index, valid)
VALUES (?, ?, ?, 1)`, id, slot, blockIndex)
		exec(
			`INSERT INTO certs (id, transaction_id, certificate_id, slot, cert_index, cert_type)
VALUES (?, ?, 0, ?, ?, 3)`,
			id,
			id,
			slot,
			certIndex,
		)
	}
	reg := func(id, pool, certificate, slot int) {
		exec(
			`INSERT INTO pool_registration (id, pool_key_hash, certificate_id, pool_id, added_slot, pledge, cost)
VALUES (?, ?, ?, ?, ?, '0', '0')`,
			id,
			hash(pool),
			certificate,
			pool,
			slot,
		)
	}
	ret := func(id, pool, certificate, epoch, slot int) {
		exec(
			`INSERT INTO pool_retirement (id, pool_key_hash, certificate_id, pool_id, epoch, added_slot)
VALUES (?, ?, ?, ?, ?, ?)`,
			id,
			hash(pool),
			certificate,
			pool,
			epoch,
			slot,
		)
	}
	// Pool 1: import baseline and tombstone, then a registration after them.
	reg(1, 1, 0, 100)
	ret(1, 1, 0, 9, 100)
	cert(11, 300, 2, 1)
	reg(2, 1, 11, 300)
	// Pool 2: a retirement and no registration.
	cert(12, 400, 0, 0)
	ret(2, 2, 12, 5, 400)
	// Pool 3: two registrations in one slot; the first sits at slot 700 by
	// its closure context and wins over the later block index.
	cert(13, 500, 1, 0)
	cert(14, 500, 3, 0)
	reg(3, 3, 13, 500)
	reg(4, 3, 14, 500)
	exec(
		`INSERT INTO leios_transaction_context (transaction_id, slot) VALUES (13, 700)`,
	)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func() {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	run()

	want := []string{
		"[1 0]",
		"[2 0]",
		"[3 0]",
		"[1 0 0 2 300 300 0 2 1 0 0]",
		"[1 0 1 1 100 100 0 0 0 0 0]",
		"[1 1 0 1 100 100 1 0 0 0 9]",
		"[2 1 0 2 400 400 0 0 0 0 5]",
		"[3 0 0 3 700 500 0 1 0 0 0]",
		"[3 0 1 4 500 500 0 3 0 0 0]",
	}
	require.Equal(t, want, poolCurrentStateRows(t, db))

	exec(`UPDATE schema_migrations
SET phase = 'expand', dirty = true, completed_at = NULL WHERE version = 66`)
	run()
	require.Equal(t, want, poolCurrentStateRows(t, db))
}
