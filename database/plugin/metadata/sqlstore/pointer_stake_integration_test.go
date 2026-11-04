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

	"github.com/stretchr/testify/require"
)

func TestPostgresPointerStakeInputsSQLExecutes(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	testPointerStakeInputsSQLExecutes(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLPointerStakeInputsSQLExecutes(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	testPointerStakeInputsSQLExecutes(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

// testPointerStakeInputsSQLExecutes runs the credential-scoped overlay query
// on a server dialect. The scope joins are the only SQL the overlay adds to
// activeDelegationSQL, so a spelling one dialect rejects fails here before
// any data is involved.
func testPointerStakeInputsSQLExecutes(t *testing.T, store *Store) {
	t.Helper()
	db, ctx, err := store.readDBFromTxn(nil)
	require.NoError(t, err)
	for _, expiryEpoch := range []uint64{0, 5} {
		query, args, err := pointerStakeInputsSQL(3_000, expiryEpoch)
		require.NoError(t, err)
		rows, err := db.QueryContext(ctx, query, args...)
		require.NoError(t, err, "expiry %d", expiryEpoch)
		for rows.Next() {
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
}
