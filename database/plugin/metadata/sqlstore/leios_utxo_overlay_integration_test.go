//go:build dingo_db_integration

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

	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestPostgresLeiosRetainedUtxoFallback(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	testLeiosRetainedUtxoFallback(t, newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema))
}

func TestMySQLLeiosRetainedUtxoFallback(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	testLeiosRetainedUtxoFallback(t, newIntegrationSQLStore(t, "mysql", dsn, "mysql", database))
}

func testLeiosRetainedUtxoFallback(t *testing.T, store *Store) {
	t.Helper()
	store.storageMode = types.StorageModeAPI
	seed, transactions := plainOverlayChain(t, 2)
	require.NoError(t, store.SetTransaction(seed, ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()}, 0, nil, true, nil, 0))
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.False(t, enabled)
	count, err := store.PrepareLeiosClosureBatch(acc, txn, transactions)
	require.NoError(t, err)
	require.Equal(t, len(transactions), count)
	for i, tx := range transactions {
		require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, ocommon.Point{Slot: 100, Hash: overlayTxID(0x53, 0)}, uint32(i), nil, true, nil, acc, txn, 0))
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
	spent, err := store.GetUtxo(seed.Hash().Bytes(), 0, nil)
	require.NoError(t, err)
	require.Nil(t, spent)
	history, err := store.GetUtxoIncludingSpent(seed.Hash().Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, history)
	require.Equal(t, transactions[0].Hash().Bytes(), []byte(history.SpentAtTxId))
	live, err := store.GetUtxo(transactions[1].Hash().Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, live)
}
