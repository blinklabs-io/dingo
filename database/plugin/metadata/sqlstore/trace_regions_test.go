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
	"testing"

	"github.com/blinklabs-io/dingo/internal/tracing/tracetest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const traceTestQuery = "-- name: TraceTestStatement :exec\nINSERT INTO t VALUES (1)"

// Not t.Parallel: only one execution trace can run per process.
func TestStatementAndPhaseRegionsAreRecorded(t *testing.T) {
	ctx := context.Background()
	trace := tracetest.Capture(t, func() {
		statementRegion(ctx, traceTestQuery)()
		p := startPhases(ctx, "dingo.sqlstore.tx.first")
		p.next(ctx, "dingo.sqlstore.tx.second")
		p.end()
	})

	tracetest.RequireRegions(
		t,
		trace,
		"dingo.sql.TraceTestStatement",
		"dingo.sqlstore.tx.first",
		"dingo.sqlstore.tx.second",
	)
}

// Not t.Parallel: only one execution trace can run per process.
func TestSetTransactionRecordsPhaseRegions(t *testing.T) {
	store := newMigratedSQLiteStore(t)
	tx, point, _, _ := assetOutputTx(t, 0x61)

	var err error
	trace := tracetest.Capture(t, func() {
		err = store.SetTransaction(tx, point, 0, nil, true, nil, 0)
	})
	require.NoError(t, err)

	tracetest.RequireRegionOrder(
		t,
		trace,
		"dingo.sqlstore.tx.prepare",
		"dingo.sqlstore.tx.identity",
		"dingo.sqlstore.tx.produced",
		"dingo.sqlstore.tx.api_details",
		"dingo.sqlstore.tx.spend",
		"dingo.sqlstore.tx.supply_stake",
	)
}

// Not t.Parallel: testing.AllocsPerRun measures the whole process.
func TestRegionsDoNotAllocateWhenTracingIsOff(t *testing.T) {
	tracetest.SkipIfTracing(t)
	ctx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		statementRegion(ctx, traceTestQuery)()
		p := startPhases(ctx, "dingo.sqlstore.tx.first")
		p.next(ctx, "dingo.sqlstore.tx.second")
		p.end()
	})
	require.Zero(t, allocs)
}

// The store has no PromRegistry, so no countingQueryer wraps its queryer and
// every region here comes from a prepared-statement cache hit.
//
// Not t.Parallel: only one execution trace can run per process.
func TestCachedStatementsRecordStatementRegions(t *testing.T) {
	store, _ := newRecordingAPIStore(t)
	require.Nil(t, store.sqlOperations)
	require.Nil(t, store.sqlQueryDuration)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var rows rowBatch
	rows.add(
		assetHoldingShape,
		int64(1),
		[]byte{1},
		[]byte("n"),
		validString("1"),
	)

	var execErr, rowErr, rowsErr, flushErr error
	trace := tracetest.Capture(t, func() {
		_, execErr = store.execCached(
			ctx, db, committeeHotAuthorizationDeleteIDQuery, int64(1),
		)
		var id int64
		rowErr = store.queryRowCached(
			ctx, db, transactionIdentityIDSQL, []byte{0},
		).Scan(&id)
		var queryRows *sql.Rows
		queryRows, rowsErr = store.queryRowsCached(
			ctx, db, poolOpCertSequenceBySlotSQL, []byte{0}, int64(0),
		)
		if rowsErr == nil {
			rowsErr = queryRows.Close()
		}
		flushErr = rows.flush(ctx, db, store, store.dialect.ParameterLimit())
	})
	require.NoError(t, execErr)
	require.ErrorIs(t, rowErr, sql.ErrNoRows)
	require.NoError(t, rowsErr)
	require.NoError(t, flushErr)

	tracetest.RequireRegions(
		t,
		trace,
		"dingo.sql.DeleteCommitteeHotAuthorizationByID",
		"dingo.sql.GetTransactionIdentityID",
		"dingo.sql.GetPoolOpCertSequenceBySlot",
		"dingo.sql.InsertAssetIdentityBatch",
		"dingo.sql.InsertAssetHoldingBatch",
	)
}

// Not t.Parallel: only one execution trace can run per process.
func TestInstrumentedQueryerRecordsStatementRegions(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_trace_region_statements_total"},
		[]string{"op"},
	)
	q := countingQueryer{queryer: db, counter: counter}
	ctx := context.Background()

	var execErr, queryErr, rowErr error
	trace := tracetest.Capture(t, func() {
		_, execErr = q.ExecContext(ctx, "-- name: TraceExecStatement :exec\nSELECT 1")
		var rows *sql.Rows
		rows, queryErr = q.QueryContext(ctx, "-- name: TraceQueryStatement :many\nSELECT 1")
		if queryErr == nil {
			queryErr = rows.Close()
		}
		var one int
		rowErr = q.QueryRowContext(ctx, "-- name: TraceRowStatement :one\nSELECT 1").
			Scan(&one)
	})
	require.NoError(t, execErr)
	require.NoError(t, queryErr)
	require.NoError(t, rowErr)

	tracetest.RequireRegions(
		t,
		trace,
		"dingo.sql.TraceExecStatement",
		"dingo.sql.TraceQueryStatement",
		"dingo.sql.TraceRowStatement",
	)
}
