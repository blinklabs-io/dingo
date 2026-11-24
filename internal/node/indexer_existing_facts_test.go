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

package node

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/stretchr/testify/require"
)

// An IndexerDB whose fact rows were copied in by natural key up to some point,
// as a release that copied them from the ledger wrote them, continues under
// the deriver: the rows it derives for the rest of the chain, on top of the
// copied ones, equal those of an IndexerDB derived from genesis.
func TestExistingIndexerDBContinuesUnderTheDeriver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	consensus := chainStoreTemplate.openEquivalence(t)
	src := indexer.NewDatabaseSource(consensus)
	tip, err := consensus.GetTip(nil)
	require.NoError(t, err)

	// The source of the copied rows: an IndexerDB derived from genesis.
	fullIdx, _ := newEquivalenceIndexer(t, consensus)
	fullWriter, _ := deriveWriter(t, consensus, fullIdx)
	require.NoError(t, fullWriter.CatchUp(ctx))
	fullStore, ok := fullIdx.Metadata().(metadata.FactRowStore)
	require.True(t, ok)

	for _, frac := range [][2]uint64{{1, 3}, {3, 4}} {
		cut := pointAt(t, src, tip.Point.Slot, frac[0], frac[1])
		idx, raw := newEquivalenceIndexer(t, consensus)
		store, bf, d := deriveParts(t, idx)

		// Blocks and their nonces to the cut, then every fact row the
		// deriver wrote dropped: what remains is what a release that copied
		// facts from the ledger left beside the blocks.
		cutSource := &truncatedSource{ChainSource: src, limit: cut}
		require.NoError(t, deriveWriterOver(store, cutSource, bf, bf, d, 700).CatchUp(ctx))
		store2, ok := idx.Metadata().(metadata.FactRowStore)
		require.True(t, ok)
		for _, table := range metadata.FactTables {
			dtxn := idx.MetadataTxn(ctx, true)
			require.NoError(t, store2.DeleteFactRowsAfter(table, 0, dtxn.Metadata()), table.Name)
			require.NoError(t, dtxn.Commit())
		}

		// The fact rows through the cut, copied by natural key.
		for _, table := range metadata.FactTables {
			var cursor int64
			for {
				rtxn := fullIdx.MetadataTxn(ctx, false)
				rows, next, err := fullStore.ReadFactRows(
					ctx, table, 0, cut.Slot, cursor, 1000, rtxn.Metadata(),
				)
				rtxn.Release()
				require.NoError(t, err, table.Name)
				if len(rows.Rows) > 0 {
					wtxn := idx.MetadataTxn(ctx, true)
					require.NoError(t, store2.WriteFactRows(table, rows, wtxn.Metadata()), table.Name)
					require.NoError(t, wtxn.Commit())
				}
				if next == 0 {
					break
				}
				cursor = next
			}
		}
		require.NotEmpty(t, factDump(t, raw)["epoch"], "rows were copied")

		// A deriver built from nothing but the IndexerDB takes over for the
		// rest of the chain, as after a restart.
		store, bf, d = deriveParts(t, idx)
		w := deriveWriterOver(store, src, bf, bf, d, 700)
		require.NoError(t, w.CatchUp(ctx))
		requireGoldenFacts(t, raw)
	}
}
