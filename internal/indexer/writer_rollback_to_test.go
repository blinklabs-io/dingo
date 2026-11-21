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

package indexer

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func tipOf(b models.Block) indexerdb.Tip {
	return indexerdb.Tip{Slot: b.Slot, BlockNumber: b.Number, Hash: b.Hash}
}

func TestRollbackToRemovesRowsPastTheTarget(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 4)
	require.NoError(t, w.CatchUp(t.Context()))

	require.NoError(t, w.RollbackTo(t.Context(), tipOf(chain.canonical[4])))
	got := rows(t, store)
	require.Len(t, got, 5, "blocks 0-4 stay: %v", got)
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, chain.canonical[4].Hash, tip.Hash)

	// The ledger still has the blocks, so the writer indexes them again: a
	// crash after this rollback and before the ledger truncate is benign.
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain)
}

func TestRollbackToAtOrPastTheTipChangesNothing(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(6, "a")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 10)
	require.NoError(t, w.CatchUp(t.Context()))
	before := rows(t, store)

	require.NoError(t, w.RollbackTo(t.Context(), tipOf(chain.canonical[5])))
	require.NoError(t, w.RollbackTo(t.Context(), indexerdb.Tip{
		Slot: 1000, BlockNumber: 100, Hash: []byte("later"),
	}))
	require.Equal(t, before, rows(t, store))
}

func TestRollbackToHealsATipOffTheCanonicalChain(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 10)
	require.NoError(t, w.CatchUp(t.Context()))

	// Blocks 6-9 are abandoned; the new chain reaches past them. The kept
	// rows must be the canonical prefix, not fork-a blocks 6 and 7.
	chain.extend(6, 6, "b")
	require.NoError(t, w.RollbackTo(t.Context(), tipOf(chain.canonical[7])))
	got := rows(t, store)
	require.Len(t, got, 6, "only blocks 0-5 are common: %v", got)
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, chain.canonical[5].Hash, tip.Hash)
}

// A crash after the ledger truncate and before the IndexerDB rollback leaves
// the IndexerDB tip past the ledger's; the writer's canonical check heals it.
func TestIndexerTipPastATruncatedLedgerHeals(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	chain.forgetAbandoned()
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 10)
	require.NoError(t, w.CatchUp(t.Context()))

	chain.extend(5, 0, "a")
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain)
}
