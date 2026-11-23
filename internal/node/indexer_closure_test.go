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
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/leiosclosurefixture"
	dledger "github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newClosureTestDB(t *testing.T) (*database.Database, *sql.DB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		StorageMode: types.StorageModeAPI,
		DataDir:     t.TempDir(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return db, raw
}

// closureTestBackfill builds a writer's row indexer over an IndexerDB seeded
// with the closure's input and a ledger database holding the chain, with the
// epoch boundary, when set, between the announcing and the certifying block.
func closureTestBackfill(
	t *testing.T,
	scenario *leiosclosurefixture.Scenario,
	boundary bool,
) (*Backfill, *database.Database, *sql.DB) {
	t.Helper()
	core, _ := newClosureTestDB(t)
	scenario.StoreChain(t, core)
	idx, raw := newClosureTestDB(t)
	scenario.Seed(t, idx)
	if boundary {
		require.NoError(t, idx.SetEpoch(
			0, 0, nil, nil, nil, nil, dijkstra.EraIdDijkstra, 1000, 1005, nil,
		))
		require.NoError(t, idx.SetEpoch(
			1005,
			1,
			nil,
			nil,
			nil,
			nil,
			dijkstra.EraIdDijkstra,
			1000,
			1005,
			nil,
		))
	}
	bf := NewBackfill(idx, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bf.SetDelegatorInactivityEnabled(false)
	bf.DisableNonceComputation()
	bf.SetClosureSource(dledger.NewDatabaseClosureSource(core))
	return bf, idx, raw
}

// A certifying ranking block carries no transactions of its own; the rows the
// ledger writes for its closure must be the rows the writer writes, whether
// the block is mid-epoch or the first of a new epoch.
func TestIndexBlocksAppliesCertifiedClosureLikeLiveApply(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		boundary bool
	}{
		{"mid_epoch", false},
		{"boundary", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scenario, err := leiosclosurefixture.New()
			require.NoError(t, err)
			bf, idx, raw := closureTestBackfill(t, scenario, tc.boundary)
			blocks := []models.Block{
				leiosclosurefixture.Model(scenario.Announcing),
				leiosclosurefixture.Model(scenario.Certifying),
			}
			require.NoError(t, idx.Transaction(context.Background(), true).Do(
				func(txn *database.Txn) error {
					return bf.IndexBlocks(context.Background(), txn, blocks)
				}))
			require.Equal(
				t,
				leiosclosurefixture.Golden(t, tc.name),
				leiosclosurefixture.Project(t, raw),
			)
			// The offsets point into the endorser-block blob the ledger
			// stored, at the announcing slot and the manifest's hash.
			require.NoError(
				t,
				idx.BlobTxn(false).Do(func(txn *database.Txn) error {
					for _, tx := range scenario.Txs {
						data, err := idx.Blob().
							GetTx(txn.Blob(), tx.Hash().Bytes())
						require.NoError(t, err)
						offset, err := database.DecodeTxOffset(data)
						require.NoError(t, err)
						require.Equal(
							t,
							leiosclosurefixture.AnnouncingSlot,
							offset.BlockSlot,
						)
						require.Equal(
							t,
							scenario.EBHash.Bytes(),
							offset.BlockHash[:],
						)
						require.NotZero(t, offset.ByteLength)
					}
					return nil
				}),
			)
		})
	}
}

// A transaction the closure lists that the IndexerDB already stores is not
// applied again, and one it does not store is.
func TestClosureTransactionsToApplySkipsStoredTransactions(t *testing.T) {
	t.Parallel()
	scenario, err := leiosclosurefixture.New()
	require.NoError(t, err)
	bf, idx, _ := closureTestBackfill(t, scenario, false)
	ctx := context.Background()
	require.NoError(
		t,
		idx.Transaction(ctx, true).Do(func(txn *database.Txn) error {
			keep, err := bf.closureTransactionsToApply(ctx, txn, scenario.Txs)
			require.NoError(t, err)
			require.Equal(t, []int{0, 1}, keep)
			if err := bf.IndexBlocks(ctx, txn, []models.Block{
				leiosclosurefixture.Model(scenario.Announcing),
				leiosclosurefixture.Model(scenario.Certifying),
			}); err != nil {
				return err
			}
			keep, err = bf.closureTransactionsToApply(ctx, txn, scenario.Txs)
			require.NoError(t, err)
			require.Empty(t, keep)
			keep, err = bf.closureTransactionsToApply(
				ctx,
				txn,
				[]lcommon.Transaction{scenario.Txs[1], scenario.Txs[1]},
			)
			require.NoError(t, err)
			require.Empty(t, keep)
			return nil
		}),
	)
}

// A certifying block whose closure cannot be read stops indexing instead of
// leaving the closure's transactions out.
func TestIndexBlocksFailsWhenClosureIsUnavailable(t *testing.T) {
	t.Parallel()
	scenario, err := leiosclosurefixture.New()
	require.NoError(t, err)
	bf, idx, _ := closureTestBackfill(t, scenario, false)
	empty, _ := newClosureTestDB(t)
	require.NoError(
		t,
		empty.BlockCreate(leiosclosurefixture.Model(scenario.Announcing), nil),
	)
	bf.SetClosureSource(dledger.NewDatabaseClosureSource(empty))
	err = idx.Transaction(context.Background(), true).
		Do(func(txn *database.Txn) error {
			return bf.IndexBlocks(context.Background(), txn, []models.Block{
				leiosclosurefixture.Model(scenario.Announcing),
				leiosclosurefixture.Model(scenario.Certifying),
			})
		})
	require.ErrorContains(t, err, "loading certified closure")
}

// Rolling back to the announcing block removes the closure's transactions,
// their outputs and their context rows, and restores the UTxO they spent.
func TestRollbackRemovesCertifiedClosure(t *testing.T) {
	t.Parallel()
	for _, boundary := range []bool{false, true} {
		name := "mid_epoch"
		if boundary {
			name = "boundary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			scenario, err := leiosclosurefixture.New()
			require.NoError(t, err)
			bf, idx, raw := closureTestBackfill(t, scenario, boundary)
			ctx := context.Background()
			require.NoError(t, idx.Transaction(ctx, true).Do(
				func(txn *database.Txn) error {
					return bf.IndexBlocks(ctx, txn, []models.Block{
						leiosclosurefixture.Model(scenario.Announcing),
						leiosclosurefixture.Model(scenario.Certifying),
					})
				}))
			require.NoError(t, idx.Transaction(ctx, true).Do(
				func(txn *database.Txn) error {
					return bf.RollbackTo(ctx, txn, ocommon.NewPoint(
						scenario.Announcing.SlotNumber(),
						scenario.Announcing.Hash().Bytes(),
					))
				}))
			require.Equal(
				t,
				"utxo 7171717171717171717171717171717171717171717171717171717171717171#0"+
					" amount=5000000 added=1 deleted=0 spent_by=\n",
				leiosclosurefixture.Project(t, raw),
			)
		})
	}
}

// An endorser block the ledger keeps as a standalone closure blob is served
// from that blob, so the indexed offsets must use its layout.
func TestIndexBlocksOffsetsFollowStandaloneClosureBacking(t *testing.T) {
	t.Parallel()
	scenario, err := leiosclosurefixture.New()
	require.NoError(t, err)
	bf, idx, _ := closureTestBackfill(t, scenario, false)
	ctx := context.Background()
	core, _ := newClosureTestDB(t)
	scenario.StoreChain(t, core)
	closure, err := dledger.LoadCertifiedClosure(
		ctx, dledger.NewDatabaseClosureSource(core), scenario.Certifying,
	)
	require.NoError(t, err)
	txs, err := closure.Decode()
	require.NoError(t, err)
	bodies, err := closure.BodyCbors()
	require.NoError(t, err)
	standalone, err := dledger.EndorserBlockOffsets(
		txs, bodies, closure.EndorserBlockSlot, closure.EndorserBlockHash,
	)
	require.NoError(t, err)
	listed, err := dledger.EndorserBlockTransactionOffsets(
		txs, bodies, closure.RawTransactions(), closure.EndorserBlockSlot,
		closure.EndorserBlockHash,
	)
	require.NoError(t, err)
	var end uint32
	for _, off := range standalone.TxOffsets {
		end = max(end, off.ByteOffset+off.ByteLength)
	}
	for _, off := range standalone.UtxoOffsets {
		end = max(end, off.ByteOffset+off.ByteLength)
	}
	require.NoError(t, core.PutLeiosClosure(
		closure.EndorserBlockSlot,
		closure.EndorserBlockHash.Bytes(),
		make([]byte, end),
	))
	bf.SetClosureSource(dledger.NewDatabaseClosureSource(core))
	require.NoError(t, idx.Transaction(ctx, true).Do(
		func(txn *database.Txn) error {
			return bf.IndexBlocks(ctx, txn, []models.Block{
				leiosclosurefixture.Model(scenario.Announcing),
				leiosclosurefixture.Model(scenario.Certifying),
			})
		}))
	require.NoError(t, idx.BlobTxn(false).Do(func(txn *database.Txn) error {
		for _, tx := range scenario.Txs {
			var hash [32]byte
			copy(hash[:], tx.Hash().Bytes())
			data, err := idx.Blob().GetTx(txn.Blob(), hash[:])
			require.NoError(t, err)
			offset, err := database.DecodeTxOffset(data)
			require.NoError(t, err)
			require.Equal(t, standalone.TxOffsets[hash].ByteOffset, offset.ByteOffset)
			require.Equal(t, standalone.TxOffsets[hash].ByteLength, offset.ByteLength)
			require.NotEqual(t, listed.TxOffsets[hash].ByteOffset, offset.ByteOffset)
		}
		return nil
	}))
}
