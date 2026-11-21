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
	"context"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// supplyIndexer writes script-locked outputs through the metadata primitives
// that maintain the running supply aggregate: block n with n%10 == 0 creates
// an output of n+1 lovelace, and block n with n%20 == 0 spends the output the
// block ten before it created.
type supplyIndexer struct{}

func supplyTxID(block uint64) []byte {
	id := make([]byte, 32)
	binary.BigEndian.PutUint64(id, block)
	return id
}

// supplyAfterBlock is the live script-locked lovelace once block n is
// indexed.
func supplyAfterBlock(n uint64) uint64 {
	var total uint64
	for b := uint64(10); b <= n; b += 10 {
		total += b + 1
		if b%20 == 0 {
			total -= b - 10 + 1
		}
	}
	return total
}

func (supplyIndexer) IndexBlocks(
	_ context.Context, txn *database.Txn, blocks []models.Block,
) error {
	meta := txn.DB().Metadata()
	for _, b := range blocks {
		if b.Number == 0 || b.Number%10 != 0 {
			continue
		}
		if err := meta.CreateUtxo(txn.Metadata(), &models.Utxo{
			TxId:          supplyTxID(b.Number),
			PaymentKey:    make([]byte, 28),
			AddedSlot:     b.Slot,
			Amount:        types.Uint64(b.Number + 1),
			PaymentScript: true,
		}); err != nil {
			return err
		}
		if b.Number%20 != 0 {
			continue
		}
		if err := meta.MarkUtxosDeletedAtSlot(
			txn.Metadata(),
			[]types.UtxoKey{{TxId: supplyTxID(b.Number - 10)}},
			b.Slot,
		); err != nil {
			return err
		}
	}
	return nil
}

func (supplyIndexer) RollbackTo(
	_ context.Context, txn *database.Txn, point ocommon.Point,
) error {
	meta := txn.DB().Metadata()
	if err := meta.DeleteUtxosAfterSlot(point.Slot, txn.Metadata()); err != nil {
		return err
	}
	return meta.SetUtxosNotDeletedAfterSlot(point.Slot, txn.Metadata())
}

// The writer bounds the supply aggregate at the oldest rollback checkpoint it
// keeps, in the transaction of the batch that moved that checkpoint, and a
// rollback to a block above the floor still reads the right total.
func TestWriterTrimsScriptLockedSupplyBehindTheOldestCheckpoint(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	store, err := indexerdb.NewStore(db, db, 10)
	require.NoError(t, err)

	blocks := int(indexerdb.CheckpointRetainBlocks) + 400
	chain := newFakeChain(blocks, "a")
	w := NewWriter(store, chain, supplyIndexer{}, nil, 1000)
	require.NoError(t, w.CatchUp(context.Background()))

	supply := func() uint64 {
		got, err := db.Metadata().GetScriptLockedSupply(nil)
		require.NoError(t, err)
		return got
	}
	last := uint64(blocks - 1)
	require.Equal(t, supplyAfterBlock(last), supply())

	cps, err := store.Checkpoints(nil)
	require.NoError(t, err)
	require.NotEmpty(t, cps)
	floor := cps[len(cps)-1].Slot
	require.Greater(t, floor, uint64(0), "the window must have moved")
	var kept int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM script_locked_supply WHERE slot <= ?", floor,
	).Scan(&kept))
	require.Equal(
		t, 1, kept,
		"only the base row at or below the oldest checkpoint slot remains",
	)

	target := indexerdb.Tip{
		Slot:        chain.canonical[last-100].Slot,
		BlockNumber: last - 100,
		Hash:        chain.canonical[last-100].Hash,
	}
	require.NoError(t, w.RollbackTo(context.Background(), target))
	require.Equal(t, supplyAfterBlock(last-100), supply())

	// A rollback below every kept row, which no writer checkpoint covers,
	// rebuilds the total from the restored outputs.
	deep := indexerdb.Tip{
		Slot:        chain.canonical[101].Slot,
		BlockNumber: 101,
		Hash:        chain.canonical[101].Hash,
	}
	require.NoError(t, w.RollbackTo(context.Background(), deep))
	require.Equal(t, supplyAfterBlock(101), supply())
	require.Greater(t, supply(), uint64(0))
}
