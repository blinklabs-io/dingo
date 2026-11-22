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
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// newRecordingAPIStore is newRecordingSQLiteStore in API storage mode and
// with the default (eager) hot-statement configuration.
func newRecordingAPIStore(t *testing.T) (*Store, *statementRecorder) {
	t.Helper()
	recorder := &statementRecorder{}
	dsn := fmt.Sprintf(
		"file:sqlstore_prepared_%d?mode=memory&cache=shared",
		testStoreSequence.Add(1),
	)
	seedMigratedSQLiteDSN(t, dsn)
	db := sql.OpenDB(recordingConnector{dsn: dsn, recorder: recorder})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		StorageMode:     types.StorageModeAPI,
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, recorder
}

func countStatements(statements []string, match func(string) bool) int {
	n := 0
	for _, query := range statements {
		if match(query) {
			n++
		}
	}
	return n
}

// TestRowBatchInsertsReusePreparedStatements flushes identically shaped
// batches twice in one write transaction on the default configuration
// (hot statements prepared eagerly). Each batch INSERT text must reach the
// driver once, as a prepare, and the applied transaction insert must not reach
// it at all because Start already prepared it; any repeat means the statement
// was parsed again.
func TestRowBatchInsertsReusePreparedStatements(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingAPIStore(t)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewBatchAccumulator()
	defer acc.Reset()

	mark := len(recorder.snapshot())
	for round := range 2 {
		for i := range 3 {
			seed := byte(1 + round*3 + i)
			tx, point := witnessTx(t, seed, 2)
			require.NoError(t, store.SetTransactionBatchedHistorical(
				tx, point, 0, nil, true, true, acc, txn, 0,
			))
		}
		require.NoError(t, store.FlushBatch(acc, txn))
	}
	issued := recorder.snapshot()[mark:]

	seen := make(map[string]int)
	for _, query := range issued {
		if strings.HasPrefix(query, "-- name: Insert") &&
			strings.Contains(query, "Batch ") {
			seen[query]++
		}
	}
	require.NotEmpty(t, seen, "no row batch INSERT was issued")
	for query, n := range seen {
		require.Equal(
			t, 1, n,
			"batch statement parsed %d times across two flushes: %s", n, query,
		)
	}
	require.Zero(t, countStatements(issued, func(q string) bool {
		return q == appliedTransactionInsertSQL
	}), "applied transaction insert must use the cached statement")
}

// TestAssetHoldingBatchInsertsReusePreparedStatements flushes two
// same-sized holding batches in one write transaction. The identity and
// holding INSERT texts must each be parsed once.
func TestAssetHoldingBatchInsertsReusePreparedStatements(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingAPIStore(t)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)

	mark := len(recorder.snapshot())
	for round := range 2 {
		var rows rowBatch
		for i := range 3 {
			rows.add(
				assetHoldingShape,
				int64(round*3+i+1),
				[]byte{byte(round*3 + i)},
				[]byte("n"),
				validString("1"),
			)
		}
		require.NoError(t, rows.flush(
			ctx, db, store, store.dialect.ParameterLimit(),
		))
	}
	seen := make(map[string]int)
	for _, query := range recorder.snapshot()[mark:] {
		if strings.HasPrefix(query, "-- name: InsertAssetHoldingBatch ") ||
			strings.HasPrefix(query, "-- name: InsertAssetIdentityBatch ") {
			seen[query]++
		}
	}
	require.Len(t, seen, 2, "identity and holding inserts must both run")
	for query, n := range seen {
		require.Equal(
			t, 1, n, "statement parsed %d times: %s", n, query,
		)
	}
}
