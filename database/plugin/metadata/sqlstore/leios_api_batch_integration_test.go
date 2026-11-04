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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresLeiosAddressBatch(t *testing.T) {
	t.Parallel()
	dsn, schema := newPostgresIntegrationSchema(t)
	testLeiosAddressBatchBackend(t, newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema))
}

func TestMySQLLeiosAddressBatch(t *testing.T) {
	t.Parallel()
	dsn, name := newMySQLIntegrationDatabase(t)
	testLeiosAddressBatchBackend(t, newIntegrationSQLStore(t, "mysql", dsn, "mysql", name))
}

func testLeiosAddressBatchBackend(t *testing.T, s *Store) {
	t.Helper()
	txn := s.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := s.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	tx, point, _, _ := assetOutputTx(t, 81)
	require.NoError(t, s.SetTransactionLeiosClosureBatched(tx, point, 0, nil, true, nil, acc, txn, 0))
	require.Zero(t, tableCounts(t, s, txn, "utxo_address")["utxo_address"])
	stored, err := s.GetUtxoIncludingSpent(tx.Hash().Bytes(), 0, txn)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Len(t, stored.Assets, 1)
	require.NoError(t, s.SetTransactionLeiosClosureBatched(tx, point, 0, nil, true, nil, acc, txn, 0))
	require.Zero(t, tableCounts(t, s, txn, "utxo_address")["utxo_address"])
	require.NoError(t, s.FlushBatch(acc, txn))
	db, ctx, err := s.dbFromTxn(txn)
	require.NoError(t, err)
	var address string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT address FROM utxo_address").Scan(&address))
	require.Equal(t, tx.Outputs()[0].Address().String(), address)
	require.NoError(t, txn.Commit())
	require.Equal(t, 1, tableCounts(t, s, nil, "utxo_address")["utxo_address"])
}
