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

package lifecycle_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// praosChain creates n Praos blocks (IDs 1..n, slot id*10) with the tip at
// the last one, so a truncate to one of them reads its nonce.
func praosChain(t *testing.T, n uint64) (*database.Database, []models.Block) {
	t.Helper()
	db := newTestDB(t)
	blocks := make([]models.Block, 0, n)
	for id := uint64(1); id <= n; id++ {
		block := testBlock(id, byte(id))
		block.Type = babbage.BlockTypeBabbage
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	return db, blocks
}

func nonceAt(id uint64, checkpoint bool) badgerutxo.Nonce {
	return badgerutxo.Nonce{
		Point:      blockPoint(id),
		Nonce:      bytes.Repeat([]byte{0xa0 + byte(id)}, 32),
		Checkpoint: checkpoint,
	}
}

// A truncate of the ledger's database takes the target's block nonce, and
// the checkpoint a pruned one is rebuilt from, from the live UTxO set.
func TestTruncateReadsTheTargetNonceFromTheSet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		nonces  []badgerutxo.Nonce
		wantErr string
	}{
		{
			name: "the set holds the target nonce",
			nonces: []badgerutxo.Nonce{
				nonceAt(1, true), nonceAt(2, false), nonceAt(3, false),
				nonceAt(4, false), nonceAt(5, false),
			},
		},
		{
			name: "the target nonce is pruned and a checkpoint survives",
			nonces: []badgerutxo.Nonce{
				nonceAt(1, true), nonceAt(4, false), nonceAt(5, false),
			},
		},
		{
			name:    "no target nonce and no checkpoint",
			nonces:  []badgerutxo.Nonce{nonceAt(4, false), nonceAt(5, false)},
			wantErr: "no earlier checkpoint exists",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, blocks := praosChain(t, 5)
			store := openUtxoSetStore(t, t.TempDir())
			applyBlocks(t, store, 1, 5)
			require.NoError(t, store.LoadNonces(tc.nonces))

			_, err := lifecycle.Truncate(
				context.Background(), db, blocks[2], 0,
				lifecycle.WithTruncateUtxoSet(
					dblifecycle.TruncateUtxoSet{Store: store},
				))

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			tip, err := db.GetTip(nil)
			require.NoError(t, err)
			require.Equal(t, blocks[2].Slot, tip.Point.Slot)
			requireSetAt(t, store, 3)
			rows, err := store.NoncesInSlotRange(0, 1000)
			require.NoError(t, err)
			for _, row := range rows {
				require.LessOrEqual(t, row.Point.Slot, blocks[2].Slot)
			}
		})
	}
}

// noReadsSet is a TruncateUtxoSet that serves none of the rollback's reads.
type noReadsSet struct{}

func (noReadsSet) Tip() (ocommon.Point, bool, error) {
	return ocommon.Point{}, false, nil
}

func (noReadsSet) JournalFloor() (ocommon.Point, bool, error) {
	return ocommon.Point{}, false, nil
}

func (noReadsSet) CanRollbackTo(ocommon.Point) (bool, error) { return true, nil }

func (noReadsSet) RollbackTo(ocommon.Point) error { return nil }

// Without a live UTxO set that answers its reads, a truncate of the ledger's
// database refuses before it removes a block.
func TestTruncateRefusesWithoutASetThatServesItsReads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		options  []lifecycle.TruncateOption
		wantText string
	}{
		{
			name:     "no set opened",
			wantText: "no live UTxO set was opened",
		},
		{
			name: "a set that serves no nonces",
			options: []lifecycle.TruncateOption{
				lifecycle.WithTruncateUtxoSet(noReadsSet{}),
			},
			wantText: "the set does not serve nonces",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, blocks := praosChain(t, 5)

			_, err := lifecycle.Truncate(
				context.Background(), db, blocks[2], 0,
				tc.options...,
			)

			require.ErrorIs(t, err, lifecycle.ErrTruncateNeedsUtxoSet)
			require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
			require.ErrorContains(t, err, tc.wantText)
			for _, b := range blocks {
				_, err := db.BlockByIndex(b.ID, nil)
				require.NoError(
					t, err, "block %d was removed by a refused truncate", b.ID,
				)
			}
		})
	}
}

func pparamUpdateRow(slot, epoch uint64, genesis, cbor byte) badgerutxo.RowPut {
	key := binary.BigEndian.AppendUint64(
		[]byte{badgerutxo.PParamUpdateTag},
		slot,
	)
	key = binary.BigEndian.AppendUint32(key, 0)
	key = append(key, genesis)
	return badgerutxo.RowPut{
		Slot:  slot,
		Key:   key,
		Value: append(binary.BigEndian.AppendUint64(nil, epoch), cbor),
	}
}

// A truncate rolls the set's protocol parameter rows, proposals and markers
// back with it, and the database's reads of them come from the set
// afterwards.
func TestTruncateRollsTheSetsProtocolParametersBack(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	store := openUtxoSetStore(t, t.TempDir())
	require.NoError(t, store.LoadPParams(nil))
	require.NoError(t, store.LoadPParamUpdates(
		func(func(k, v []byte) error) error { return nil },
	))
	require.NoError(t, store.LoadMarkers(nil))
	require.NoError(t, store.LoadNonces([]badgerutxo.Nonce{nonceAt(3, true)}))
	applyBlocks(t, store, 1, 3)
	// Block 4 writes a row, a proposal and the markers real data was
	// confirmed with at epoch 1, which starts after the target block.
	p4 := blockPoint(4)
	_, err := store.ApplyLedgerBatch(&p4, badgerutxo.LedgerBatch{
		Changes: []badgerutxo.Change{{
			Point: p4,
			Added: []badgerutxo.Output{{
				Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{4}, 32), 0),
				Cbor: []byte{0x80, 4},
			}},
		}},
		Epochs: []badgerutxo.EpochRow{
			{Epoch: 0, StartSlot: 0, Value: []byte{0}},
			{Epoch: 1, StartSlot: 35, Value: []byte{1}},
		},
		PParams: []badgerutxo.PParamsRow{
			{Era: 6, Epoch: 0, AddedSlot: 20, Cbor: []byte{1}},
			{Era: 6, Epoch: 1, AddedSlot: 45, Cbor: []byte{2}},
		},
		Rows: []badgerutxo.RowPut{
			pparamUpdateRow(25, 0, 1, 0xa1),
			pparamUpdateRow(45, 1, 2, 0xa2),
		},
		Markers: []badgerutxo.MarkerWrite{
			{Name: badgerutxo.SyntheticV2Marker, Value: "false"},
			{Name: badgerutxo.SyntheticV2ClearedMarker, Value: "1"},
		},
	})
	require.NoError(t, err)
	applyBlocks(t, store, 5, 5)

	_, err = lifecycle.Truncate(
		context.Background(), db, blocks[2], 0,
		lifecycle.WithTruncateUtxoSet(
			dblifecycle.TruncateUtxoSet{Store: store},
		))

	require.NoError(t, err)
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, blocks[2].Slot, tip.Point.Slot)
	requireSetAt(t, store, 3)
	rows, err := store.PParamsForEra(6)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the row added after the target is gone")
	require.Equal(t, uint64(20), rows[0].AddedSlot)
	var proposals int
	require.NoError(t, utxoset.EachPParamUpdate(
		store, func(models.PParamUpdate) error { proposals++; return nil },
	))
	require.Equal(
		t, 1, proposals, "the proposal added after the target is gone",
	)
	for _, name := range []string{
		badgerutxo.SyntheticV2Marker, badgerutxo.SyntheticV2ClearedMarker,
	} {
		_, ok, err := store.Marker(name)
		require.NoError(t, err)
		require.False(
			t, ok, "%s: the target is before the confirming epoch", name,
		)
	}
	listed, err := db.ListPParamsForEra(6, nil)
	require.NoError(t, err)
	require.Len(t, listed, 1)
}

// applyBlockWithTx applies block id with one output, and indexes the
// transaction hash produced in it.
func applyBlockWithTx(
	t *testing.T,
	store *badgerutxo.Store,
	id uint64,
	hash []byte,
) {
	t.Helper()
	p := blockPoint(id)
	_, err := store.ApplyLedgerBatch(&p, badgerutxo.LedgerBatch{
		Changes: []badgerutxo.Change{{
			Point: p,
			Added: []badgerutxo.Output{{
				Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{byte(id)}, 32), 0),
				Cbor: []byte{0x80, byte(id)},
			}},
		}},
		Rows: []badgerutxo.RowPut{{
			Slot:  p.Slot,
			Key:   badgerutxo.TxIndexKey(hash),
			Value: []byte{byte(id)},
		}},
	})
	require.NoError(t, err)
}

// An offline truncate removes the offsets of the transactions the set
// indexed after the target, which the set's rollback would otherwise stop
// naming, and keeps those at or below it.
func TestTruncateRemovesTheTruncatedTransactionOffsets(t *testing.T) {
	t.Parallel()
	db, blocks := praosChain(t, 5)
	store := openUtxoSetStore(t, t.TempDir())
	require.NoError(t, store.LoadNonces([]badgerutxo.Nonce{nonceAt(3, true)}))
	kept := bytes.Repeat([]byte{0xb2}, 32)
	truncated := bytes.Repeat([]byte{0xb4}, 32)
	applyBlocks(t, store, 1, 1)
	applyBlockWithTx(t, store, 2, kept)
	applyBlocks(t, store, 3, 3)
	applyBlockWithTx(t, store, 4, truncated)
	applyBlocks(t, store, 5, 5)
	blob := db.BlobTxn(true)
	for _, hash := range [][]byte{kept, truncated} {
		require.NoError(t, blob.BlobStore().SetTx(blob.Blob(), hash, []byte{1}))
	}
	require.NoError(t, blob.Commit())

	_, err := lifecycle.Truncate(
		context.Background(), db, blocks[2], 0,
		lifecycle.WithTruncateUtxoSet(
			dblifecycle.TruncateUtxoSet{Store: store},
		))

	require.NoError(t, err)
	requireSetAt(t, store, 3)
	read := db.BlobTxn(false)
	defer read.Release()
	_, err = read.BlobStore().GetTx(read.Blob(), truncated)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound,
		"the truncated transaction's offset is left behind")
	_, err = read.BlobStore().GetTx(read.Blob(), kept)
	require.NoError(t, err)
}
