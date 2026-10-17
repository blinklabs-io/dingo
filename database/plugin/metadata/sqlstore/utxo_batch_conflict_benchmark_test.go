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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
)

func BenchmarkInsertUtxoBatchConflicts(b *testing.B) {
	const batchSize = utxoInsertBatchSize
	store := newMigratedSQLiteStore(b)
	snapshotUtxos := make([]models.Utxo, batchSize)
	batch := make([]*models.Utxo, batchSize)
	for i := range snapshotUtxos {
		utxo := utxoForBenchmarkIteration(uint64(i + 100))
		snapshotUtxos[i] = *utxo
		batch[i] = utxo
	}
	if err := store.ImportUtxos(snapshotUtxos, nil); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := store.withWriteTransaction(nil,
			func(db queryer, ctx context.Context) error {
				_, err := store.insertUtxoModelsChecked(ctx, db, batch, true, nil)
				return err
			},
		); err != nil {
			b.Fatal(err)
		}
	}
}
