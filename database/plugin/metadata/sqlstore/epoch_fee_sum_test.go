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
	"math"
	"testing"

	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/coinsum"
	"github.com/stretchr/testify/require"
)

func newEpochFeeStore(t testing.TB) *Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	store, err := New(Config{WriteDB: db, Dialect: SQLiteDialect()})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, err = db.Exec(
		`CREATE TABLE "transaction" (id INTEGER PRIMARY KEY, slot INTEGER, valid BOOLEAN, fee TEXT, collateral_fee TEXT);
CREATE INDEX fee_slot ON "transaction"(slot);
CREATE TABLE leios_transaction_context (transaction_id INTEGER PRIMARY KEY, slot INTEGER);
CREATE INDEX fee_context_slot ON leios_transaction_context(slot);`,
	)
	require.NoError(t, err)
	return store
}

func TestEpochFeeSumPreservesExecutionSlotAndDomain(t *testing.T) {
	t.Parallel()
	store := newEpochFeeStore(t)
	_, err := store.writeDB.Exec(`INSERT INTO "transaction" VALUES
(1,10,TRUE,'5','bad'), (2,11,FALSE,'bad','7'),
(3,90,TRUE,'13','bad'), (4,11,TRUE,'17','bad'),
(5,80,FALSE,'bad','29'), (6,11,TRUE,NULL,'bad'),
(7,11,TRUE,'','bad'), (8,11,TRUE,X'','bad');
INSERT INTO leios_transaction_context VALUES (3,11), (4,90), (5,12);`)
	require.NoError(t, err)
	got, err := store.SumTransactionFeesInSlotRange(10, 12, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(54), got)
	got, err = store.SumTransactionFeesInSlotRange(12, 10, nil)
	require.NoError(t, err)
	require.Zero(t, got)
	_, err = store.SumTransactionFeesInSlotRange(0, math.MaxUint64, nil)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	txn := store.ReadTransaction(ctx)
	t.Cleanup(func() { _ = txn.Rollback() })
	_, err = store.SumTransactionFeesInSlotRange(10, 12, txn)
	require.ErrorIs(t, err, context.Canceled)
}

func TestEpochFeeSumRejectsInvalidRowsAndOverflow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		values []any
		valid  bool
	}{
		{"max", []any{"18446744073709551615"}, true},
		{"blob", []any{[]byte("7"), "5"}, true},
		{"nullable", []any{nil, "", []byte{}}, true},
		{"overflow", []any{"18446744073709551615", "1"}, false},
		{"oversized", []any{"18446744073709551616"}, false},
		{"negative cancellation", []any{"-1", "1"}, false},
		{"malformed", []any{"1x"}, false},
		{"fractional decimal", []any{"1.5"}, false},
		{"leading sign", []any{"+1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newEpochFeeStore(t)
			for i, value := range tc.values {
				_, err := store.writeDB.Exec(
					`INSERT INTO "transaction" VALUES (?,10,TRUE,?,'0')`,
					i+1,
					value,
				)
				require.NoError(t, err)
			}
			got, err := store.SumTransactionFeesInSlotRange(10, 10, nil)
			baseline, baselineErr := sumUint64Rows(t.Context(), store.writeDB,
				`SELECT fee FROM "transaction" WHERE slot=10`)
			if !tc.valid {
				require.Error(t, baselineErr)
				require.Error(t, err)
				return
			}
			require.NoError(t, baselineErr)
			require.NoError(t, err)
			require.Equal(t, baseline, got)
		})
	}
}

func BenchmarkEpochFeeSum(b *testing.B) {
	for _, count := range []int{1_000, 100_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			store := newEpochFeeStore(b)
			txn, err := store.writeDB.BeginTx(b.Context(), nil)
			require.NoError(b, err)
			_, err = txn.Exec(
				`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO "transaction" SELECT i,i,TRUE,'1234','0' FROM n`,
				count,
			)
			require.NoError(b, err)
			_, err = txn.Exec(
				`INSERT INTO leios_transaction_context SELECT id,slot FROM "transaction" WHERE id%5=0`,
			)
			require.NoError(b, err)
			require.NoError(b, txn.Commit())
			want := uint64(count) * 1234
			b.ReportAllocs()
			for b.Loop() {
				got, err := store.SumTransactionFeesInSlotRange(
					0,
					uint64(count),
					nil,
				)
				if err != nil || got != want {
					b.Fatalf("fee sum = %d, %v; want %d", got, err, want)
				}
			}
		})
	}
}

func TestEpochFeeSumUsesCallerTransaction(t *testing.T) {
	t.Parallel()
	store := newEpochFeeStore(t)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	_, err = db.ExecContext(
		ctx,
		`INSERT INTO "transaction" VALUES (1,10,TRUE,'13','0')`,
	)
	require.NoError(t, err)
	got, err := store.SumTransactionFeesInSlotRange(10, 10, txn)
	require.NoError(t, err)
	require.Equal(t, uint64(13), got)
	require.NoError(t, txn.Rollback())
	got, err = store.SumTransactionFeesInSlotRange(10, 10, nil)
	require.NoError(t, err)
	require.Zero(t, got)
}
