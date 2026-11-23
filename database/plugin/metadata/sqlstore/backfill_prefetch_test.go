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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func newBackfillPrefetchStore(tb testing.TB) *Store {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "metadata.sqlite")
	writeDB, err := OpenDB(
		"sqlite",
		"file:"+path+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)",
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	seedMigratedSQLite(tb, writeDB)
	readDB, err := OpenDB(
		"sqlite",
		"file:"+path+"?mode=ro&_pragma=foreign_keys(1)",
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	readDB.SetMaxOpenConns(5)
	readDB.SetMaxIdleConns(5)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:         writeDB,
		ReadDB:          readDB,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return store
}

func TestPrefetchBackfillInputsReadsBesideOpenWriter(t *testing.T) {
	t.Parallel()

	store := newBackfillPrefetchStore(t)
	seedUtxoLookupRows(t, store, 20)
	refs := make([]models.UtxoId, 20)
	for i := range refs {
		refs[i] = models.UtxoId{Hash: benchmarkUtxoTxID(i)}
	}
	writeTxn := store.Transaction(t.Context())
	defer writeTxn.Rollback() //nolint:errcheck
	require.NoError(t, store.PrefetchBackfillInputs(t.Context(), refs))
}

func BenchmarkPrefetchBackfillInputs(b *testing.B) {
	for _, refCount := range []int{40, 400} {
		b.Run(fmt.Sprintf("refs=%d", refCount), func(b *testing.B) {
			store := newBackfillPrefetchStore(b)
			seedUtxoLookupRows(b, store, 10_000)
			refs := make([]models.UtxoId, refCount)
			for i := range refs {
				refs[i] = models.UtxoId{
					Hash: benchmarkUtxoTxID(i * 10_000 / refCount),
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := store.PrefetchBackfillInputs(b.Context(), refs); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(refCount), "refs/op")
		})
	}
}
