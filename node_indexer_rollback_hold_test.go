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
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func retainedBodyCount(t *testing.T, db *database.Database) int {
	t.Helper()
	count := 0
	require.NoError(t, db.BlobTxn(false).Do(func(txn *database.Txn) error {
		prefix := []byte(types.RolledBackBlockKeyPrefix)
		it := db.Blob().NewIterator(
			txn.Blob(), types.BlobIteratorOptions{Prefix: prefix},
		)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return it.Err()
	}))
	return count
}

// A rollback below the IndexerDB tip keeps the abandoned bodies, which the
// IndexerDB's rows still name, until the writer has rolled back past them;
// the writer's callback then has the ledger release them. This runs the
// wiring openIndexerWriter installs: the hold on the ledger's database and
// the Moved callback it hands the writer.
func TestNodeKeepsRolledBackBodiesUntilTheIndexerRollsBackPastThem(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	n, points, writers := newSplitLifecycleTestNode(t, 6, nil)
	moved := writers.all()[0].deps.Moved
	require.NotNil(t, moved, "the writer is handed a Moved callback")
	released := func() {
		t.Helper()
		moved()
		testutil.WaitForCondition(t, func() bool {
			return !n.releasingBlocks.Load()
		}, 10*time.Second, "the release to finish")
	}
	setIndexerTip := func(point int) {
		t.Helper()
		block, err := database.BlockByHash(ctx, n.db, points[point].Hash)
		require.NoError(t, err)
		require.NoError(t, n.indexerDB.Transaction(ctx, true).Do(
			func(txn *database.Txn) error {
				return n.indexerStore.SetTip(indexerdb.Tip{
					Slot: block.Slot, BlockNumber: block.Number,
					Hash: block.Hash,
				}, txn)
			},
		))
	}

	// A rollback to block 2 abandons blocks 3 to 5, and the IndexerDB, which
	// has not rolled back yet, ends at block 5.
	for _, point := range points[3:6] {
		block, err := database.BlockByHash(ctx, n.db, point.Hash)
		require.NoError(t, err)
		require.NoError(t, n.db.RetainRolledBackBlock(
			block.Slot, block.Hash, block.Cbor,
		))
	}
	setIndexerTip(5)
	released()
	require.Equal(
		t, 3, retainedBodyCount(t, n.db),
		"bodies the IndexerDB still names were released",
	)

	// The writer rolled back past them to block 2, a chain block.
	setIndexerTip(2)
	released()
	require.Zero(
		t, retainedBodyCount(t, n.db),
		"bodies were kept after the writer rolled back past them",
	)
}
