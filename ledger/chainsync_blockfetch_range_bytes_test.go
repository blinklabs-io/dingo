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

package ledger

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
)

// bodySizedMockHeader is a Shelley-and-later header that reports a body size,
// so a queue of them has a known, large estimated wire size.
type bodySizedMockHeader struct {
	mockHeader
	bodySize uint64
}

func (h bodySizedMockHeader) BlockBodySize() uint64 { return h.bodySize }

func buildBodySizedChain(
	t *testing.T,
	headerCount int,
	bodySize uint64,
) *chain.Chain {
	t.Helper()
	testChain := &chain.Chain{}
	prevHash := lcommon.NewBlake2b256(nil)
	for i := range headerCount {
		hash := lcommon.NewBlake2b256(
			testHashBytes(fmt.Sprintf("range-bytes-hdr-%d", i)),
		)
		require.NoError(t, testChain.AddBlockHeader(bodySizedMockHeader{
			mockHeader: mockHeader{
				hash:        hash,
				prevHash:    prevHash,
				blockNumber: uint64(i + 1),
				slot:        uint64(i + 1),
			},
			bodySize: bodySize,
		}))
		prevHash = hash
	}
	return testChain
}

func TestStartQueuedBlockfetchCutsRangesByEstimatedBytes(t *testing.T) {
	t.Parallel()

	// 1 MiB blocks: a whole 30-header queue is far under the 500-block
	// count cap but 30 MiB in bytes.
	const bodySize = 1 << 20
	testChain := buildBodySizedChain(t, 30, bodySize)
	connId := testChainsyncConnId(6400, 3001)

	var requests []deepCatchupRequest
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				gotConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requests = append(requests, deepCatchupRequest{
					connId: gotConnId,
					start:  start,
					end:    end,
				})
				return uint64(len(requests)), nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.startQueuedBlockfetchLocked(connId, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	require.Len(t, requests, 2)
	active, prefetch := requests[0], requests[1]
	assert.Equal(t, uint64(1), active.start.Slot)
	assert.Equal(
		t,
		uint64(7),
		active.end.Slot,
		"seven 1 MiB blocks fit in BlockfetchMaxRangeBytes; the eighth does not",
	)
	assert.Equal(t, uint64(8), prefetch.start.Slot)
	assert.Equal(t, uint64(14), prefetch.end.Slot)

	got := ls.BlockfetchRangeExpectedBytes(active.start, active.end)
	assert.Greater(t, got, uint64(7*bodySize))
	assert.LessOrEqual(t, got, uint64(BlockfetchMaxRangeBytes))

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

func TestBlockfetchRangeExpectedBytesNoEstimate(t *testing.T) {
	t.Parallel()

	t.Run("nil chain", func(t *testing.T) {
		t.Parallel()
		ls := &LedgerState{}
		assert.Zero(t, ls.BlockfetchRangeExpectedBytes(
			ocommon.Point{Slot: 1, Hash: []byte("a")},
			ocommon.Point{Slot: 2, Hash: []byte("b")},
		))
	})

	t.Run("header without a body size", func(t *testing.T) {
		t.Parallel()
		testChain, _ := buildDeepCatchupChain(t, 3)
		ls := &LedgerState{chain: testChain}
		start, end := testChain.HeaderRange(3)
		assert.Zero(t, ls.BlockfetchRangeExpectedBytes(start, end))
	})

	t.Run("range not in the queue", func(t *testing.T) {
		t.Parallel()
		testChain := buildBodySizedChain(t, 3, 1<<10)
		ls := &LedgerState{chain: testChain}
		start, end := testChain.HeaderRange(3)
		require.NotZero(t, ls.BlockfetchRangeExpectedBytes(start, end))
		end.Hash = []byte("unknown")
		assert.Zero(t, ls.BlockfetchRangeExpectedBytes(start, end))
	})
}
