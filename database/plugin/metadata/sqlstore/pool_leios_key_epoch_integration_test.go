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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The registration epoch reads run on the certificate write path, so they
// must hold on the PostgreSQL and MySQL dialects as well as SQLite.
func TestPostgresPoolLeiosKeyEpochWritePath(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	testPoolLeiosKeyEpochWritePath(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLPoolLeiosKeyEpochWritePath(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	testPoolLeiosKeyEpochWritePath(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

func testPoolLeiosKeyEpochWritePath(t *testing.T, store *Store) {
	t.Helper()
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xe5)
	staged := depositHeldPoolKey(0xe6)

	// Reaped at the boundary into 5, registered afresh in 5 and re-registered
	// twice in the same epoch, then once after the boundary.
	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 3_100, 0, depositHeldRetirement(pool, 5), 0)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_200, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 5_300, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)
	writeDepositHeldCert(t, store, 7_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	require.Equal(
		t,
		[]int64{2, 5, 6, 5, 8},
		leiosKeyEpochsByDialect(t, store, pool),
	)

	// A snapshot's staged key is adopted at the boundary after the import.
	importLeiosKeyPoolStaged(t, store, staged, 4_050, 0x11, 2, 0x12)
	writeDepositHeldCert(t, store, 6_150, 0, leiosKeyEpochCert(staged, 0x12, 0x22), 0)
	epochs := leiosKeyEpochsByDialect(t, store, staged)
	require.Equal(t, int64(5), epochs[len(epochs)-1])
}

// leiosKeyEpochsByDialect is leiosKeyEpochsInOrder through the store's
// dialect-translating queryer.
func leiosKeyEpochsByDialect(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
) []int64 {
	t.Helper()
	db, ctx, err := store.dbFromTxn(nil)
	require.NoError(t, err)
	rows, err := db.QueryContext(ctx, `
SELECT leios_key_registration_epoch FROM pool_registration
WHERE pool_key_hash = ? ORDER BY id`, pool.Bytes())
	require.NoError(t, err)
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var epoch *int64
		require.NoError(t, rows.Scan(&epoch))
		if epoch == nil {
			out = append(out, -1)
			continue
		}
		out = append(out, *epoch)
	}
	require.NoError(t, rows.Err())
	return out
}
