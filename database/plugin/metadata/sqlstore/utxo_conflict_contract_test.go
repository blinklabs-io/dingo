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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// requireCreateUtxoConflictContract pins the error CreateUtxo returns for an
// output that is already stored, whichever dialect enforces the uniqueness: an
// output produced twice, a produced output over an imported row, and an
// imported row over a produced output.
func requireCreateUtxoConflictContract(t *testing.T, store *Store) {
	t.Helper()
	importedHash, producedHash := bytes32(0xa1), bytes32(0xa2)
	addr := []byte("addr_test1contract")
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: importedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	}}, nil))
	table := store.dialect.QuoteIdentifier("transaction")
	_, err := store.writeDB.Exec(
		store.dialect.Rebind(
			"INSERT INTO "+table+" (hash, slot) VALUES (?, 5)",
		),
		producedHash,
	)
	require.NoError(t, err)
	var producer int64
	require.NoError(t, store.writeDB.QueryRow(
		store.dialect.Rebind("SELECT id FROM "+table+" WHERE hash = ?"),
		producedHash,
	).Scan(&producer))
	id := uint(producer)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	}))

	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "produced twice")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: importedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "produced over imported")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "imported over produced")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: importedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "imported twice")
}

func TestSQLiteCreateUtxoConflictContract(t *testing.T) {
	t.Parallel()
	requireCreateUtxoConflictContract(t, newMigratedSQLiteStore(t))
}
