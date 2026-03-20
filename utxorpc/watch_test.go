// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/stretchr/testify/require"
)

func TestWatchTxBuildRollbackMessages_EmitsUndoWhenPointNotFound(t *testing.T) {
	t.Parallel()
	history := []watchTxHistoryEntry{
		{
			point: ocommon.NewPoint(10, []byte{0xaa}),
			appliedTxs: []*watch.AnyChainTx{
				{Chain: &watch.AnyChainTx_Cardano{}},
			},
		},
	}
	out, found := watchTxBuildRollbackMessages(
		&history,
		ocommon.NewPoint(0, nil),
	)
	require.False(t, found)
	require.Len(t, out, 1)
	_, ok := out[0].Action.(*watch.WatchTxResponse_Undo)
	require.True(t, ok, "expected Undo action")
	require.Empty(t, history)
}

func TestWatchTxBuildMessages_IdleOnEmptyBlock(t *testing.T) {
	t.Parallel()
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()
	immBlock, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, immBlock)

	blk, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
	require.NoError(t, err)
	require.Empty(t, blk.Transactions())

	wantHash := append([]byte(nil), blk.Hash().Bytes()...)
	appliedTxs, out, err := watchTxBuildForwardMessages(
		uint(immBlock.Type),
		immBlock.Cbor,
		blk.SlotNumber(),
		blk.BlockNumber(),
		blk.Hash().Bytes(),
		func(ledger.Transaction) bool { return true },
	)
	require.NoError(t, err)
	require.Empty(t, appliedTxs)
	require.Len(t, out, 1)
	idle, ok := out[0].Action.(*watch.WatchTxResponse_Idle)
	require.True(t, ok, "expected Idle action")
	require.Equal(t, blk.SlotNumber(), idle.Idle.GetSlot())
	require.Equal(t, blk.BlockNumber(), idle.Idle.GetHeight())
	require.Equal(t, wantHash, idle.Idle.GetHash())
}

func TestWatchTxBuildMessages_IdleWhenNoPredicateMatch(t *testing.T) {
	t.Parallel()
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()
	// Slot 0 is empty; advance to first block with transactions
	var immBlock *immutable.Block
	for {
		immBlock, err = iter.Next()
		require.NoError(t, err)
		require.NotNil(t, immBlock)
		blk, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		if len(blk.Transactions()) > 0 {
			break
		}
	}
	blk, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
	require.NoError(t, err)
	require.NotEmpty(t, blk.Transactions())

	metaHash := append([]byte(nil), blk.Hash().Bytes()...)
	appliedTxs, out, err := watchTxBuildForwardMessages(
		uint(immBlock.Type),
		immBlock.Cbor,
		blk.SlotNumber(),
		blk.BlockNumber(),
		metaHash,
		func(ledger.Transaction) bool { return false },
	)
	require.NoError(t, err)
	require.Empty(t, appliedTxs)
	require.Len(t, out, 1)
	idle, ok := out[0].Action.(*watch.WatchTxResponse_Idle)
	require.True(t, ok)
	require.Equal(t, blk.SlotNumber(), idle.Idle.GetSlot())
	require.Equal(t, blk.BlockNumber(), idle.Idle.GetHeight())
	require.Equal(t, metaHash, idle.Idle.GetHash())
}

func TestWatchTxBuildMessages_ApplyWhenMatching(t *testing.T) {
	t.Parallel()
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()
	var immBlock *immutable.Block
	for {
		immBlock, err = iter.Next()
		require.NoError(t, err)
		require.NotNil(t, immBlock)
		blk, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		if len(blk.Transactions()) > 0 {
			break
		}
	}
	blk, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
	require.NoError(t, err)

	appliedTxs, out, err := watchTxBuildForwardMessages(
		uint(immBlock.Type),
		immBlock.Cbor,
		blk.SlotNumber(),
		blk.BlockNumber(),
		blk.Hash().Bytes(),
		func(ledger.Transaction) bool { return true },
	)
	require.NoError(t, err)
	require.Len(t, appliedTxs, len(blk.Transactions()))
	require.Len(t, out, len(blk.Transactions()))
	for _, resp := range out {
		_, ok := resp.Action.(*watch.WatchTxResponse_Apply)
		require.True(t, ok, "expected Apply for each transaction")
	}
}

func TestWatchTxBuildRollbackMessages_StopsAtRollbackPoint(t *testing.T) {
	t.Parallel()
	txA := &watch.AnyChainTx{Chain: &watch.AnyChainTx_Cardano{}}
	txB := &watch.AnyChainTx{Chain: &watch.AnyChainTx_Cardano{}}
	history := []watchTxHistoryEntry{
		{
			point:      ocommon.NewPoint(100, []byte{0x01}),
			appliedTxs: []*watch.AnyChainTx{txA},
		},
		{
			point:      ocommon.NewPoint(101, []byte{0x02}),
			appliedTxs: nil,
		},
		{
			point:      ocommon.NewPoint(102, []byte{0x03}),
			appliedTxs: []*watch.AnyChainTx{txB},
		},
	}

	out, found := watchTxBuildRollbackMessages(
		&history,
		ocommon.NewPoint(101, []byte{0x02}),
	)
	require.True(t, found)
	require.Len(t, out, 1)
	undo, ok := out[0].Action.(*watch.WatchTxResponse_Undo)
	require.True(t, ok)
	require.Equal(t, txB, undo.Undo)
	require.Len(t, history, 2)
	require.True(
		t,
		pointsEqual(history[len(history)-1].point, ocommon.NewPoint(101, []byte{0x02})),
	)
}

func TestPointsEqual(t *testing.T) {
	t.Parallel()
	require.True(
		t,
		pointsEqual(
			ocommon.NewPoint(10, []byte{0xaa, 0xbb}),
			ocommon.NewPoint(10, []byte{0xaa, 0xbb}),
		),
	)
	require.False(
		t,
		pointsEqual(
			ocommon.NewPoint(10, []byte{0xaa, 0xbb}),
			ocommon.NewPoint(10, []byte{0xaa, 0xcc}),
		),
	)
	require.False(
		t,
		pointsEqual(
			ocommon.NewPoint(10, []byte{0xaa, 0xbb}),
			ocommon.NewPoint(11, []byte{0xaa, 0xbb}),
		),
	)
}
