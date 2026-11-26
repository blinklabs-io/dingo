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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestAPINodeServesHistoryFromTheIndexerDB indexes a generated chain's
// transactions into the IndexerDB only, as the writer does, and requires the
// Mesh and UTxO RPC stores the node wires to answer address and transaction
// history from it. The consensus database holds the blocks but no history, so
// a read routed to it finds nothing.
func TestAPINodeServesHistoryFromTheIndexerDB(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	consensus, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	idx, err := dbtest.NewIndexerDatabase(t, consensus, dbtest.IndexerOptions{})
	require.NoError(t, err)

	blocks, err := testfixtures.GenerateConwayChainWithTransactions(5)
	require.NoError(t, err)
	var (
		lastHash []byte
		lastSlot uint64
		txHashes [][]byte
		addrs    []lcommon.Address
	)
	for _, blk := range blocks {
		require.NoError(t, consensus.BlockCreate(models.Block{
			Hash:     blk.Hash().Bytes(),
			PrevHash: blk.PrevHash().Bytes(),
			Cbor:     blk.Cbor(),
			Slot:     blk.SlotNumber(),
			Number:   blk.BlockNumber(),
			Type:     uint(blk.Type()),
		}, nil))
		offsets, err := database.NewBlockIndexer(
			blk.SlotNumber(), blk.Hash().Bytes(),
		).ComputeOffsets(blk.Cbor(), blk)
		require.NoError(t, err)
		point := ocommon.NewPoint(blk.SlotNumber(), blk.Hash().Bytes())
		txHashes, addrs = nil, nil
		for i, tx := range blk.Transactions() {
			require.NoError(t, idx.SetTransaction(
				ctx, tx, point, uint32(i), 0, nil, nil, offsets, nil, 0, //nolint:gosec // a block's transaction count
			))
			txHashes = append(txHashes, tx.Hash().Bytes())
			for _, out := range tx.Outputs() {
				addrs = append(addrs, out.Address())
			}
		}
		lastHash, lastSlot = blk.Hash().Bytes(), blk.SlotNumber()
	}
	// Outputs of the last block are unspent: no later block can consume them.
	require.NotEmpty(t, txHashes, "the last block carries transactions")
	require.NotEmpty(t, addrs)
	store, err := indexerdb.NewStore(idx, idx, 0)
	require.NoError(t, err)
	require.NoError(t, idx.Transaction(ctx, true).Do(
		func(txn *database.Txn) error {
			return store.SetTip(indexerdb.Tip{
				Slot:        lastSlot,
				BlockNumber: blocks[len(blocks)-1].BlockNumber(),
				Hash:        lastHash,
			}, txn)
		},
	))
	n := &Node{db: consensus, indexerDB: idx, indexerStore: store}
	pattern, err := models.ExactUtxoAddressPattern(addrs[0])
	require.NoError(t, err)

	meshDB := n.meshDatabase()
	txs, err := meshDB.GetTransactionsByBlockHash(ctx, lastHash)
	require.NoError(t, err)
	require.Len(t, txs, len(txHashes))
	utxos, err := meshDB.UtxosByAddress(ctx, addrs[:1])
	require.NoError(t, err)
	require.NotEmpty(t, utxos, "Mesh account balance")
	scanned := 0
	_, err = meshDB.ScanUtxosByAddress(
		ctx,
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			Limit:           10,
		},
		100,
		1<<20,
		func(page []models.UtxoWithOrdering) error {
			scanned += len(page)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, len(utxos), scanned, "Mesh account coins")
	balance, err := meshDB.UtxoBalanceByAddressAtSlot(ctx, addrs[0], lastSlot)
	require.NoError(t, err)
	require.EqualValues(t, len(utxos), balance.UtxoCount,
		"Mesh historical account balance")

	queryDB := n.utxorpcQueryDatabase()
	tx, err := queryDB.TransactionByHash(ctx, txHashes[0])
	require.NoError(t, err)
	require.NotNil(t, tx, "UTxO RPC ReadTx")
	page, err := queryDB.UtxosByAddressBoundedPage(
		ctx,
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			Limit:           10,
		},
		100,
		1<<20,
	)
	require.NoError(t, err)
	require.Len(t, page.Utxos, len(utxos), "UTxO RPC SearchUtxos")
	tip, err := queryDB.Tip()
	require.NoError(t, err)
	require.Equal(t, lastSlot, tip.Point.Slot)
}
