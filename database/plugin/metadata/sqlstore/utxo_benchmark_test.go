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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
)

func BenchmarkGetUtxoPreparedStatementCache(b *testing.B) {
	for _, rowCount := range []int{1_000, 50_000} {
		b.Run(fmt.Sprintf("rows=%d", rowCount), func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			seedUtxoLookupRows(b, store, rowCount)
			txID := benchmarkUtxoTxID(rowCount / 2)

			b.Run("generated-one-shot", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					utxo, err := getUtxoGeneratedOneShot(
						store,
						txID,
						0,
					)
					if err != nil {
						b.Fatal(err)
					}
					if utxo == nil {
						b.Fatal("generated query returned no UTxO")
					}
				}
				b.ReportMetric(float64(rowCount), "utxos")
			})

			b.Run("prepared-cache", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					utxo, err := store.GetUtxo(txID, 0, nil)
					if err != nil {
						b.Fatal(err)
					}
					if utxo == nil {
						b.Fatal("cached query returned no UTxO")
					}
				}
				b.ReportMetric(float64(rowCount), "utxos")
			})
		})
	}
}

func seedUtxoLookupRows(tb testing.TB, store *Store, count int) {
	tb.Helper()
	tx, err := store.writeDB.BeginTx(context.Background(), nil)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = tx.Rollback() })
	stmt, err := tx.PrepareContext(
		context.Background(),
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount) "+
			"VALUES (?, 0, ?, 0, '1000000')",
	)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()
	for i := range count {
		if _, err := stmt.ExecContext(
			context.Background(),
			benchmarkUtxoTxID(i),
			int64(i+1),
		); err != nil {
			tb.Fatalf("seed UTxO %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

func benchmarkUtxoTxID(index int) []byte {
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], uint64(index+1)) // #nosec G115 -- benchmark index is non-negative.
	return txID
}

func getUtxoGeneratedOneShot(
	store *Store,
	txID []byte,
	index uint32,
) (*models.Utxo, error) {
	db, ctx, err := store.readDBFromTxn(nil)
	if err != nil {
		return nil, err
	}
	row, err := store.operationalQueries(db).GetLiveUtxo(
		ctx,
		sqlitequery.GetLiveUtxoParams{
			TxID: txID,
			OutputIdx: sql.NullInt64{
				Int64: int64(index),
				Valid: true,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	utxo, err := utxoFromSQLite(row)
	if err != nil {
		return nil, err
	}
	if err := store.loadUtxoAssets(ctx, db, []*models.Utxo{utxo}); err != nil {
		return nil, err
	}
	return utxo, nil
}
