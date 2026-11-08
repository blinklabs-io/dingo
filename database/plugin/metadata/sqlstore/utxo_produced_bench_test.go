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

// BenchmarkGetUtxoProduced reads live outputs that have a producing
// transaction row, the shape block apply creates, by (hash, index).
func BenchmarkGetUtxoProduced(b *testing.B) {
	store := newMigratedSQLiteStore(b)
	const txs = 25_000
	_, err := store.writeDB.Exec(`
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ` + "25000" + `)
INSERT INTO "transaction" (hash, slot, block_index) SELECT randomblob(32), i, 0 FROM n`)
	require.NoError(b, err)
	_, err = store.writeDB.Exec(`
INSERT INTO utxo (transaction_id, tx_id, output_idx, credential_tag, added_slot, deleted_slot, amount)
SELECT t.id, t.hash, o.i, 0, t.slot, 0, '1'
FROM "transaction" t, (SELECT 0 AS i UNION ALL SELECT 1) o`)
	require.NoError(b, err)
	hashes := make([][]byte, 0, txs)
	rows, err := store.writeDB.Query(`SELECT hash FROM "transaction" ORDER BY id`)
	require.NoError(b, err)
	for rows.Next() {
		var h []byte
		require.NoError(b, rows.Scan(&h))
		hashes = append(hashes, h)
	}
	require.NoError(b, rows.Close())
	b.ResetTimer()
	i := 0
	for b.Loop() {
		got, err := store.GetUtxo(hashes[(i*7919)%txs], uint32(i%2), nil) //nolint:gosec
		if err != nil || got == nil {
			b.Fatal(err, got)
		}
		i++
	}
}
