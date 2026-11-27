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

package dingo

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func indexerNodeAt(t *testing.T, tip *indexerdb.Tip) (*Node, *indexerdb.Store) {
	t.Helper()
	ledgerDB, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	idx, err := dbtest.NewIndexerDatabase(t, ledgerDB, dbtest.IndexerOptions{})
	require.NoError(t, err)
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)
	if tip != nil {
		require.NoError(t, idx.Transaction(t.Context(), true).Do(
			func(txn *database.Txn) error { return store.SetTip(*tip, txn) },
		))
	}
	return &Node{db: ledgerDB, indexerDB: idx, indexerStore: store}, store
}

// The history expiry pruner the node builds is bounded by the IndexerDB tip,
// so no block an indexed row's payload offset names expires before the writer
// has indexed it.
func TestHistoryExpiryPrunerIsBoundedByTheIndexerTip(t *testing.T) {
	t.Parallel()
	n, _ := indexerNodeAt(t, &indexerdb.Tip{
		Slot: 50, BlockNumber: 5, Hash: []byte("indexer-tip"),
	})
	cfg := n.newHistoryExpiryPrunerConfig()
	require.NotNil(t, cfg.Ceiling, "the pruner is not bounded by the IndexerDB")
	ceiling, ok, err := cfg.Ceiling(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 49, ceiling)
}

// The ledger keeps rolled-back bodies for the block the IndexerDB's rows end
// at, and none while the IndexerDB has indexed nothing.
func TestRolledBackBlockHoldFollowsTheIndexerTip(t *testing.T) {
	t.Parallel()
	n, store := indexerNodeAt(t, nil)
	_, ok, err := rolledBackBlockHold(store)()
	require.NoError(t, err)
	require.False(t, ok, "nothing is indexed yet")

	tip := indexerdb.Tip{Slot: 50, BlockNumber: 5, Hash: []byte("indexer-tip")}
	require.NoError(t, n.indexerDB.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error { return store.SetTip(tip, txn) },
	))
	point, ok, err := rolledBackBlockHold(store)()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 50, point.Slot)
	require.Equal(t, tip.Hash, point.Hash)
}
