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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestChainsyncFreshOriginRequiresBlockZero(t *testing.T) {
	t.Parallel()
	ls := newGenesisChainsyncState(t)
	first := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("genesis-first")),
		slot:        3,
		blockNumber: 0,
	}
	second := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("genesis-second")),
		prevHash:    first.hash,
		slot:        6,
		blockNumber: 1,
	}
	_, err := ls.addChainsyncHeader(second, 0)
	var notFit chain.BlockNotFitChainTipError
	require.ErrorAs(
		t,
		err,
		&notFit,
		"network admission must not establish an incomplete chain",
	)
	require.Zero(t, ls.chain.HeaderCount())
	_, err = ls.addChainsyncHeader(first, 0)
	require.NoError(t, err)
	_, err = ls.addChainsyncHeader(second, 0)
	require.NoError(t, err)
	require.Equal(
		t,
		2,
		ls.chain.HeaderCount(),
		"a complete prefix must still be admitted",
	)
	// Direct immutable import deliberately uses the unanchored chain API.
	importDB := newTestDB(t)
	importChain, err := chain.NewManager(context.Background(), importDB, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		importChain.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	require.NoError(
		t,
		importChain.PrimaryChain().
			AddRawBlocks(context.Background(), []chain.RawBlock{{Slot: 100, Hash: testHashBytes("import-anchor"), BlockNumber: 50, Cbor: []byte{0x80}}}),
	)
	require.Equal(t, uint64(50), importChain.PrimaryChain().Tip().BlockNumber)
}

func newGenesisChainsyncState(t *testing.T) *LedgerState {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	return ls
}

func TestChainsyncFreshOriginHandlerRequiresBlockZero(t *testing.T) {
	t.Parallel()
	ls := newGenesisChainsyncState(t)
	ls.metrics.init(prometheus.NewRegistry())
	requests := 0
	ls.config.BlockfetchRequestRangeFunc = func(ouroboros.ConnectionId, ocommon.Point, ocommon.Point) (uint64, error) { requests++; return 0, nil }
	ls.Lock()
	ls.publishSnapshotsLocked()
	ls.Unlock()
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	resyncCh := subscribeChainsyncResync(t, bus)
	admitted := make(chan bool, 3)
	conn := testChainsyncConnId(6601, 3001)
	first := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("handler-first")),
		slot:        3,
		blockNumber: 0,
	}
	second := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("handler-second")),
		prevHash:    first.hash,
		slot:        6,
		blockNumber: 1,
	}
	deliver := func(header mockHeader) {
		generation := ls.loadConsensusSnapshot().generation
		require.NotZero(t, generation)
		require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: conn, HeaderCryptoVerificationGeneration: generation,
			Point: ocommon.NewPoint(
				header.slot,
				header.hash.Bytes(),
			), BlockHeader: header,
			PeerTipAdmission: func(ok bool) { admitted <- ok },
		}))
	}
	deliver(second)
	require.False(
		t,
		testutil.RequireReceive(
			t,
			admitted,
			testutil.AsyncWait,
			"an incomplete network prefix must be rejected",
		),
	)
	require.Zero(
		t,
		ls.chain.HeaderCount(),
		"the real network handler must not queue a nonzero first block",
	)
	require.Empty(t, ls.chain.Tip().Point.Hash)
	require.Zero(t, requests, "an incomplete prefix must not start blockfetch")
	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"the cursor must re-intersect to replay block zero",
	)
	require.Equal(t, conn, resync.ConnectionId)
	require.Equal(t, event.ChainsyncResyncReasonRollbackNotFound, resync.Reason)
	deliver(first)
	require.True(
		t,
		testutil.RequireReceive(
			t,
			admitted,
			testutil.AsyncWait,
			"block zero must be admitted",
		),
	)
	deliver(second)
	require.True(
		t,
		testutil.RequireReceive(
			t,
			admitted,
			testutil.AsyncWait,
			"a complete continuation must be admitted",
		),
	)
	require.Equal(t, 2, ls.chain.HeaderCount())
	require.Equal(t, 1, requests, "the complete prefix must start blockfetch")
	require.Equal(t, second.hash.Bytes(), ls.chain.HeaderTip().Point.Hash)
}
