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
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/indexer"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func txHashesOf(t *testing.T, blocks []gledger.Block) [][][]byte {
	t.Helper()
	out := make([][][]byte, len(blocks))
	for i, blk := range blocks {
		for _, tx := range blk.Transactions() {
			out[i] = append(out[i], tx.Hash().Bytes())
		}
	}
	return out
}

func TestIndexerWriterIndexesAndRollsBackTransactions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newIndexerTestDB(t)
	consensus := consensusOf(t, db)

	chain, err := testfixtures.GenerateConwayChainWithTransactions(4)
	require.NoError(t, err)
	hashes := txHashesOf(t, chain)
	total := 0
	for _, h := range hashes {
		total += len(h)
	}
	require.NotZero(t, total, "fixture must carry transactions")
	for _, b := range chain {
		require.NoError(t, consensus.BlockCreate(models.Block{
			Slot: b.SlotNumber(), Hash: b.Hash().Bytes(),
			PrevHash: b.PrevHash().Bytes(), Number: b.BlockNumber(),
			Cbor: b.Cbor(), Type: uint(b.Type()),
		}, nil))
	}
	last := chain[len(chain)-1]
	require.NoError(t, consensus.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: last.SlotNumber(),
			Hash: last.Hash().Bytes(),
		},
		BlockNumber: last.BlockNumber(),
	}, nil))

	store, err := indexerdb.NewStore(db, db, 10)
	require.NoError(t, err)
	bf := NewBackfill(db, nil, slog.Default())
	bf.SetDelegatorInactivityEnabled(false)
	// Small batches make the second call continue from running state.
	w := indexer.NewWriter(
		store, indexer.NewDatabaseSource(consensus), bf, slog.Default(), 2,
	)
	require.NoError(t, w.CatchUp(ctx))

	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, last.Hash().Bytes(), tip.Hash)
	for i, hs := range hashes {
		for _, h := range hs {
			tx, err := db.GetTransactionByHash(ctx, h, nil)
			require.NoError(t, err)
			require.NotNil(t, tx, "block %d tx missing", i)
		}
	}

	// Roll back to the second block: later transactions go, earlier stay.
	keep := chain[1]
	txn := db.Transaction(ctx, true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return bf.RollbackTo(ctx, txn, ocommon.Point{
			Slot: keep.SlotNumber(), Hash: keep.Hash().Bytes(),
		})
	}))
	for i, hs := range hashes {
		for _, h := range hs {
			tx, err := db.GetTransactionByHash(ctx, h, nil)
			if i <= 1 {
				require.NoError(t, err)
				require.NotNil(t, tx)
			} else {
				require.True(t, err != nil || tx == nil,
					"block %d tx survived rollback", i)
			}
		}
	}
}
