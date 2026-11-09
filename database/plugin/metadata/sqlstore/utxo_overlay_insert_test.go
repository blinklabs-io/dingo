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

//nolint:gosec // SQL INTEGER mappings preserve the unsigned domain API.
package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newOverlayInsertStore(
	tb testing.TB,
	tracing bool,
	rows int,
) (*Store, *prometheus.Registry) {
	tb.Helper()
	db, err := OpenDB("sqlite", fmt.Sprintf(
		"file:insert_width_%d?mode=memory&cache=shared&_pragma=foreign_keys(1)",
		testStoreSequence.Add(1)), "sqlite", tracing)
	require.NoError(tb, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	metrics := prometheus.NewRegistry()
	store, err := New(Config{
		WriteDB: db, Dialect: SQLiteDialect(), Migrations: registry,
		MigrationLocker: migrations.NewProcessLocker(), PromRegistry: metrics,
		SQLiteOverlayInsertRows: rows,
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	_, err = db.Exec(
		`INSERT INTO "transaction" (id,hash,slot,valid,fee) VALUES (1,?,1,TRUE,'0')`,
		bytes.Repeat([]byte{1}, 32),
	)
	require.NoError(tb, err)
	return store, metrics
}

func overlayInsertOutputs(start, count int) []*overlayOutput {
	outputs := make([]*overlayOutput, count)
	parent := uint(1)
	for i := range count {
		id := start + i
		model := utxoForBenchmarkIteration(uint64(id))
		model.TransactionID = &parent
		// One producer owns each output index once.
		model.OutputIdx = uint32(id)
		outputs[i] = &overlayOutput{id: int64(id), model: *model}
	}
	return outputs
}

func overlayInsertStatementCount(
	tb testing.TB,
	metrics *prometheus.Registry,
) uint64 {
	tb.Helper()
	families, err := metrics.Gather()
	require.NoError(tb, err)
	for _, family := range families {
		if family.GetName() != "dingo_database_sql_query_duration_seconds" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "query" &&
					label.GetValue() == "BatchOverlayInsertUtxos" {
					return metric.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

func insertOverlayFixture(store *Store, outputs []*overlayOutput) error {
	return store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			var overlay utxoOverlay
			return store.insertOverlayUtxos(ctx, db, &overlay, outputs)
		},
	)
}

func TestSQLiteOverlayInsertReducesStatementsWithSameRows(t *testing.T) {
	t.Parallel()
	for _, tracing := range []bool{false, true} {
		t.Run(fmt.Sprint(tracing), func(t *testing.T) {
			t.Parallel()
			states := make([]string, 2)
			for i, limit := range []int{0, 2047} {
				store, metrics := newOverlayInsertStore(t, tracing, limit)
				require.NoError(
					t,
					insertOverlayFixture(store, overlayInsertOutputs(1, 8192)),
				)
				count := overlayInsertStatementCount(t, metrics)
				if limit == 0 {
					require.Equal(t, uint64(133), count)
				} else {
					require.Equal(t, uint64(5), count)
				}
				states[i] = overlayStoreState(t, store)
				var fkViolation any
				require.ErrorIs(
					t,
					store.writeDB.QueryRow("PRAGMA foreign_key_check").
						Scan(&fkViolation),
					sql.ErrNoRows,
				)
			}
			require.Equal(t, states[0], states[1])
		})
	}
}

func TestSQLiteOverlayInsertRespectsRuntimeLimit(t *testing.T) {
	t.Parallel()
	store, metrics := newOverlayInsertStore(t, false, 2047)
	limitSQLiteVariableNumber(t, store.writeDB, 999)
	require.NoError(
		t,
		insertOverlayFixture(store, overlayInsertOutputs(1, 128)),
	)
	require.Equal(t, uint64(3), overlayInsertStatementCount(t, metrics))
	limitSQLiteVariableNumber(t, store.writeDB, 32766)
	require.NoError(
		t,
		insertOverlayFixture(store, overlayInsertOutputs(129, 128)),
	)
	require.Equal(t, uint64(4), overlayInsertStatementCount(t, metrics))
}

func TestSQLiteOverlayInsertPreservesTriggerBoundaries(t *testing.T) {
	t.Parallel()
	for _, temporary := range []bool{false, true} {
		t.Run(fmt.Sprint(temporary), func(t *testing.T) {
			t.Parallel()
			store, metrics := newOverlayInsertStore(t, false, 2047)
			_, err := store.writeDB.Exec(
				"CREATE TABLE output_audit (utxo_id INTEGER, prior_changes INTEGER)",
			)
			require.NoError(t, err)
			keyword := ""
			if temporary {
				keyword = "TEMP "
			}
			_, err = store.writeDB.Exec(
				"CREATE " + keyword + `TRIGGER observe_width AFTER INSERT ON main.utxo BEGIN
INSERT INTO output_audit VALUES (new.id, changes()); END`,
			)
			require.NoError(t, err)
			_, err = store.writeDB.Exec(
				"INSERT INTO output_audit VALUES (-1,0)",
			)
			require.NoError(t, err)
			require.NoError(
				t,
				insertOverlayFixture(store, overlayInsertOutputs(1, 128)),
			)
			require.Equal(t, uint64(3), overlayInsertStatementCount(t, metrics))
			rows, err := store.writeDB.Query(
				"SELECT utxo_id,prior_changes FROM output_audit WHERE utxo_id>0 ORDER BY utxo_id",
			)
			require.NoError(t, err)
			seen := 0
			for i := 1; rows.Next(); i++ {
				seen++
				var id, changes int
				require.NoError(t, rows.Scan(&id, &changes))
				require.Equal(t, i, id)
				want := 62
				if i <= 62 {
					want = 1
				}
				require.Equal(t, want, changes)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, 128, seen)
		})
	}
}

func TestSQLiteOverlayInsertRechecksTemporarySchema(t *testing.T) {
	t.Parallel()
	store, metrics := newOverlayInsertStore(t, false, 2047)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var overlay utxoOverlay
	require.NoError(
		t,
		store.insertOverlayUtxos(
			ctx,
			db,
			&overlay,
			overlayInsertOutputs(1, 128),
		),
	)
	require.Equal(t, uint64(1), overlayInsertStatementCount(t, metrics))
	_, err = db.ExecContext(
		ctx,
		`CREATE TEMP TRIGGER observe_width AFTER INSERT ON main.utxo BEGIN SELECT new.id; END`,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		store.insertOverlayUtxos(
			ctx,
			db,
			&overlay,
			overlayInsertOutputs(129, 128),
		),
	)
	require.Equal(t, uint64(4), overlayInsertStatementCount(t, metrics))
	_, err = db.ExecContext(ctx, "DROP TRIGGER temp.observe_width")
	require.NoError(t, err)
	require.NoError(
		t,
		store.insertOverlayUtxos(
			ctx,
			db,
			&overlay,
			overlayInsertOutputs(257, 128),
		),
	)
	require.Equal(t, uint64(5), overlayInsertStatementCount(t, metrics))
	_, err = db.ExecContext(
		ctx,
		"CREATE TEMP TABLE UTXO AS SELECT * FROM main.utxo WHERE 0",
	)
	require.NoError(t, err)
	// The duplicate probe names the imported-row index, which the copy lacks.
	_, err = db.ExecContext(
		ctx,
		"CREATE INDEX temp.idx_utxo_imported_ref ON UTXO(tx_id, output_idx)",
	)
	require.NoError(t, err)
	require.NoError(
		t,
		store.insertOverlayUtxos(
			ctx,
			db,
			&overlay,
			overlayInsertOutputs(385, 128),
		),
	)
	require.Equal(t, uint64(8), overlayInsertStatementCount(t, metrics))
	require.NoError(t, txn.Rollback())
}

// An imported output blocks a produced output only at its own output index;
// a sibling output of the same transaction hash does not.
func TestSQLiteOverlayInsertConflictsOnlyWithTheExactImportedOutput(t *testing.T) {
	t.Parallel()
	for _, sameIndex := range []bool{false, true} {
		t.Run(fmt.Sprint(sameIndex), func(t *testing.T) {
			t.Parallel()
			store, _ := newOverlayInsertStore(t, false, 0)
			outputs := overlayInsertOutputs(10, 2)
			importedIdx := outputs[0].model.OutputIdx + 100
			if sameIndex {
				importedIdx = outputs[0].model.OutputIdx
			}
			require.NoError(t, store.ImportUtxos([]models.Utxo{{
				TxId:      outputs[0].model.TxId,
				OutputIdx: importedIdx,
				Amount:    types.Uint64(1),
			}}, nil))
			err := insertOverlayFixture(store, outputs)
			var rows int
			require.NoError(t, store.writeDB.QueryRow(
				"SELECT COUNT(*) FROM utxo",
			).Scan(&rows))
			if sameIndex {
				require.ErrorIs(t, err, types.ErrBatchUtxoOverlayConflict)
				require.Equal(t, 1, rows, "the batch must not be written")
				return
			}
			require.NoError(t, err)
			require.Equal(t, 3, rows)
		})
	}
}

func TestSQLiteOverlayInsertKeepsConflictAndCancellationRollback(t *testing.T) {
	t.Parallel()
	store, _ := newOverlayInsertStore(t, false, 2047)
	outputs := overlayInsertOutputs(1, 4096)
	outputs[3000].model.TxId = outputs[0].model.TxId
	outputs[3000].model.OutputIdx = outputs[0].model.OutputIdx
	err := insertOverlayFixture(store, outputs)
	require.ErrorIs(t, err, types.ErrBatchUtxoOverlayConflict)
	var count int
	require.NoError(
		t,
		store.writeDB.QueryRow("SELECT COUNT(*) FROM utxo").Scan(&count),
	)
	require.Zero(t, count)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, _, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var overlay utxoOverlay
	err = store.insertOverlayUtxos(
		ctx,
		db,
		&overlay,
		overlayInsertOutputs(1, 128),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, txn.Rollback())
	require.NoError(
		t,
		store.writeDB.QueryRow("SELECT COUNT(*) FROM utxo").Scan(&count),
	)
	require.Zero(t, count)
}

func BenchmarkSQLiteOverlayInsertWidth(b *testing.B) {
	for _, count := range []int{63, 256, 2047, 8192} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			store, _ := newOverlayInsertStore(b, false, 2047)
			outputs := overlayInsertOutputs(1, count)
			b.ReportAllocs()
			for b.Loop() {
				txn := store.Transaction(b.Context())
				db, ctx, err := store.dbFromTxn(txn)
				if err != nil {
					b.Fatal(err)
				}
				var overlay utxoOverlay
				if err := store.insertOverlayUtxos(ctx, db, &overlay, outputs); err != nil {
					_ = txn.Rollback()
					b.Fatal(err)
				}
				if err := txn.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
			var rows int
			require.NoError(
				b,
				store.writeDB.QueryRow("SELECT COUNT(*) FROM utxo").Scan(&rows),
			)
			require.Zero(b, rows)
		})
	}
}

func TestSQLiteOverlayInsertOptInAndBound(t *testing.T) {
	t.Parallel()
	store, _ := newOverlayInsertStore(t, false, 0)
	for _, limit := range []int{-1, 2048} {
		_, err := New(
			Config{
				WriteDB:                 store.writeDB,
				Dialect:                 SQLiteDialect(),
				SQLiteOverlayInsertRows: limit,
			},
		)
		require.Error(t, err)
	}
	for _, dialect := range []Dialect{PostgresDialect(), MySQLDialect()} {
		_, err := New(Config{
			WriteDB: store.writeDB, Dialect: dialect,
			SQLiteOverlayInsertRows: 2047,
		})
		require.ErrorContains(t, err, "wider overlay inserts require SQLite")
	}
	rows, err := store.overlayInsertRows(t.Context(), store.writeDB, 8192)
	require.NoError(t, err)
	require.Equal(t, 62, rows)
	store.sqliteOverlayInsertRows = 2047
	rows, err = store.overlayInsertRows(t.Context(), store.writeDB, 8192)
	require.NoError(t, err)
	require.Equal(
		t,
		62,
		rows,
		"a pool does not pin the probe and insert to one connection",
	)
	for _, ctxErr := range []error{context.Canceled, context.DeadlineExceeded} {
		rows, err = overlayInsertFallback(
			t.Context(),
			62,
			fmt.Errorf("probe: %w", ctxErr),
		)
		require.Equal(t, 62, rows)
		require.ErrorIs(t, err, ctxErr)
	}
	rows, err = overlayInsertFallback(
		t.Context(),
		62,
		errors.New("unsupported probe"),
	)
	require.NoError(t, err)
	require.Equal(t, 62, rows)
}
