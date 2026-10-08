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

package chain_test

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// byronEraHeader reports a real header's identity as a Byron one: Byron
// headers carry no body size, so BlockBodySize is 0 as the real ones return.
type byronEraHeader struct {
	lcommon.BlockHeader
}

func (byronEraHeader) Era() lcommon.Era      { return byron.EraByron }
func (byronEraHeader) BlockBodySize() uint64 { return 0 }

func rangePointOf(b ledger.Block) ocommon.Point {
	return ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes())
}

// wireSizeOfBlock is the number of bytes the server puts on the wire for one
// block: the real MsgBlock encoding of the real block, plus one 8-byte mux
// segment header per 65535-byte segment.
func wireSizeOfBlock(t *testing.T, b ledger.Block) uint64 {
	t.Helper()
	wrapped, err := cbor.Encode([]any{b.Era().Id, cbor.RawMessage(b.Cbor())})
	require.NoError(t, err)
	msg, err := blockfetch.NewMsgBlock(wrapped).MarshalCBOR()
	require.NoError(t, err)
	n := uint64(len(msg))
	segments := (n + muxer.SegmentMaxPayloadLength - 1) /
		muxer.SegmentMaxPayloadLength
	return n + segments*8
}

func queueBlockHeaders(t *testing.T, blocks []ledger.Block) *chain.Chain {
	t.Helper()
	cm, err := chain.NewManager(context.Background(), nil, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	for _, b := range blocks {
		require.NoError(t, c.AddBlockHeader(context.Background(), b.Header()))
	}
	return c
}

func TestQueuedRangeWireBytesMatchesEncodedBlocks(t *testing.T) {
	t.Parallel()

	blocks, err := fixtures.GenerateConwayChainWithTransactions(6)
	require.NoError(t, err)
	c := queueBlockHeaders(t, blocks)

	var want uint64
	for _, b := range blocks[1:5] {
		want += wireSizeOfBlock(t, b)
	}
	got, ok := c.QueuedRangeWireBytes(
		rangePointOf(blocks[1]),
		rangePointOf(blocks[4]),
	)
	require.True(t, ok)
	require.Equal(t, want, got)

	single, ok := c.QueuedRangeWireBytes(
		rangePointOf(blocks[2]),
		rangePointOf(blocks[2]),
	)
	require.True(t, ok)
	require.Equal(t, wireSizeOfBlock(t, blocks[2]), single)
}

func TestQueuedRangeWireBytesNoEstimate(t *testing.T) {
	t.Parallel()

	blocks, err := fixtures.GenerateConwayChainWithTransactions(4)
	require.NoError(t, err)

	t.Run("byron header in range", func(t *testing.T) {
		t.Parallel()
		cm, err := chain.NewManager(context.Background(), nil, nil)
		require.NoError(t, err)
		c := cm.PrimaryChain()
		require.NoError(t, c.AddBlockHeader(
			context.Background(),
			byronEraHeader{BlockHeader: blocks[0].Header()},
		))
		for _, b := range blocks[1:] {
			require.NoError(
				t,
				c.AddBlockHeader(context.Background(), b.Header()),
			)
		}
		_, ok := c.QueuedRangeWireBytes(
			rangePointOf(blocks[0]),
			rangePointOf(blocks[2]),
		)
		require.False(t, ok, "a range containing a Byron header has no estimate")
		_, ok = c.QueuedRangeWireBytes(
			rangePointOf(blocks[1]),
			rangePointOf(blocks[2]),
		)
		require.True(t, ok, "the Shelley-and-later suffix is still estimable")
	})

	t.Run("endpoint not queued", func(t *testing.T) {
		t.Parallel()
		c := queueBlockHeaders(t, blocks[:3])
		_, ok := c.QueuedRangeWireBytes(
			rangePointOf(blocks[1]),
			rangePointOf(blocks[3]),
		)
		require.False(t, ok, "end header missing from the queue")
		_, ok = c.QueuedRangeWireBytes(
			rangePointOf(blocks[3]),
			rangePointOf(blocks[3]),
		)
		require.False(t, ok, "start header missing from the queue")
	})

	t.Run("empty queue", func(t *testing.T) {
		t.Parallel()
		c := queueBlockHeaders(t, nil)
		_, ok := c.QueuedRangeWireBytes(
			rangePointOf(blocks[0]),
			rangePointOf(blocks[1]),
		)
		require.False(t, ok)
	})
}

func TestHeaderRangeAfterBytesCutsByEstimatedBytes(t *testing.T) {
	t.Parallel()

	blocks, err := fixtures.GenerateConwayChainWithTransactions(8)
	require.NoError(t, err)
	c := queueBlockHeaders(t, blocks)
	one := wireSizeOfBlock(t, blocks[0])

	t.Run("budget for three blocks", func(t *testing.T) {
		t.Parallel()
		var budget uint64
		for _, b := range blocks[:3] {
			budget += wireSizeOfBlock(t, b)
		}
		start, end, n := c.HeaderRangeAfterBytes(0, 100, budget)
		require.Equal(t, 3, n)
		require.Equal(t, rangePointOf(blocks[0]).Slot, start.Slot)
		require.Equal(t, rangePointOf(blocks[2]).Slot, end.Slot)
		got, ok := c.QueuedRangeWireBytes(start, end)
		require.True(t, ok)
		require.LessOrEqual(t, got, budget)
	})

	t.Run("skip is honoured", func(t *testing.T) {
		t.Parallel()
		var budget uint64
		for _, b := range blocks[2:4] {
			budget += wireSizeOfBlock(t, b)
		}
		start, end, n := c.HeaderRangeAfterBytes(2, 100, budget)
		require.Equal(t, 2, n)
		require.Equal(t, rangePointOf(blocks[2]).Slot, start.Slot)
		require.Equal(t, rangePointOf(blocks[3]).Slot, end.Slot)
	})

	t.Run("one oversized header still forms a range", func(t *testing.T) {
		t.Parallel()
		_, _, n := c.HeaderRangeAfterBytes(0, 100, one/2)
		require.Equal(t, 1, n)
	})

	t.Run("count still bounds the range", func(t *testing.T) {
		t.Parallel()
		_, _, n := c.HeaderRangeAfterBytes(0, 2, 1<<30)
		require.Equal(t, 2, n)
	})

	t.Run("zero budget is count only", func(t *testing.T) {
		t.Parallel()
		_, _, n := c.HeaderRangeAfterBytes(0, 5, 0)
		require.Equal(t, 5, n)
	})
}
