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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestReservedReadTxnSkipsWriteStatementCache runs hot-statement reads inside
// a transaction begun on a reserved read-pool connection of a file-backed
// store. The write pool prepared those statements, and database/sql refuses
// to derive a transaction-scoped statement from another pool, so the reads
// must take the uncached path.
func TestReservedReadTxnSkipsWriteStatementCache(t *testing.T) {
	t.Parallel()
	store, _, _, err := openSQLStore(
		Config{DataDir: t.TempDir()},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	reservation, err := store.ReserveRead(t.Context())
	require.NoError(t, err)
	t.Cleanup(reservation.Release)
	txn := reservation.Begin()
	t.Cleanup(func() { _ = txn.Rollback() })

	pool, err := store.GetPool(lcommon.PoolKeyHash{0x01}, true, txn)
	require.NoError(t, err)
	require.Nil(t, pool)
	account, err := store.GetAccountByCredential(0, []byte{0x01}, true, txn)
	require.NoError(t, err)
	require.Nil(t, account)
	utxo, err := store.GetUtxo([]byte{0x01}, 0, txn)
	require.NoError(t, err)
	require.Nil(t, utxo)
}
