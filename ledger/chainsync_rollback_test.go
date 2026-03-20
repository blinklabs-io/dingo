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
	"bytes"
	"crypto/sha256"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chainsyncRollbackFixture struct {
	ls            *LedgerState
	connId        ouroboros.ConnectionId
	ancestorTip   ochainsync.Tip
	currentTip    ochainsync.Tip
	ancestorNonce []byte
	forkPoint     ocommon.Point
}

type testSecurityParamLedger struct {
	securityParam int
}

func (m testSecurityParamLedger) SecurityParam() int {
	return m.securityParam
}

func TestHandleEventChainsyncRollbackSynchronizesLedgerTip(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestRollbackChainAndStateRejectedRollbackDoesNotMutateLedgerState(
	t *testing.T,
) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	blocks := []chain.RawBlock{
		{
			Slot:        10,
			Hash:        testHashBytes("rollback-precheck-1"),
			BlockNumber: 1,
			Type:        1,
			Cbor:        []byte{0x80},
		},
		{
			Slot:        20,
			Hash:        testHashBytes("rollback-precheck-2"),
			BlockNumber: 2,
			Type:        1,
			PrevHash:    testHashBytes("rollback-precheck-1"),
			Cbor:        []byte{0x80},
		},
		{
			Slot:        30,
			Hash:        testHashBytes("rollback-precheck-3"),
			BlockNumber: 3,
			Type:        1,
			PrevHash:    testHashBytes("rollback-precheck-2"),
			Cbor:        []byte{0x80},
		},
		{
			Slot:        40,
			Hash:        testHashBytes("rollback-precheck-4"),
			BlockNumber: 4,
			Type:        1,
			PrevHash:    testHashBytes("rollback-precheck-3"),
			Cbor:        []byte{0x80},
		},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(blocks))

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	cm.SetLedger(testSecurityParamLedger{securityParam: 1})

	rollbackPoint := ocommon.NewPoint(blocks[1].Slot, blocks[1].Hash)
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(blocks[3].Slot, blocks[3].Hash),
		BlockNumber: blocks[3].BlockNumber,
	}
	rollbackNonce := []byte("rollback-point-nonce")
	currentNonce := []byte("current-point-nonce")
	require.NoError(
		t,
		db.SetBlockNonce(
			rollbackPoint.Hash,
			rollbackPoint.Slot,
			rollbackNonce,
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			currentNonce,
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = append([]byte(nil), currentNonce...)

	err = ls.rollbackChainAndState(rollbackPoint)
	require.ErrorIs(t, err, chain.ErrRollbackExceedsSecurityParam)

	assert.Equal(t, currentTip, ls.chain.Tip())
	assert.Equal(t, currentTip, ls.currentTip)
	assert.True(t, bytes.Equal(currentNonce, ls.currentTipBlockNonce))

	dbTip, err := db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, currentTip, dbTip)
}

func TestTryResolveForkSynchronizesLedgerTip(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	var requestedConnId ouroboros.ConnectionId
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) error {
		_ = start
		_ = end
		requestedConnId = connId
		return nil
	}
	t.Cleanup(func() {
		fixture.ls.blockfetchRequestRangeCleanup()
	})

	forkHash := testHashBytes("fork-block")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	err := fixture.ls.chain.AddBlockHeader(header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		},
		notFitErr,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())
	assert.Equal(t, fixture.connId, requestedConnId)
	assert.Equal(t, fixture.connId, fixture.ls.activeBlockfetchConnId)
	require.NotNil(t, fixture.ls.chainsyncBlockfetchReadyChan)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestHandleEventChainsyncBlockHeaderMissingAncestorRequestsResync(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	staleHash := testHashBytes("stale-fork-block")
	stalePrevHash := testHashBytes("missing-ancestor")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(staleHash),
		prevHash:    lcommon.NewBlake2b256(stalePrevHash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	fixture.ls.bufferedHeaderEvents = map[ouroboros.ConnectionId][]ChainsyncEvent{
		fixture.connId: {{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		}},
	}

	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
		BlockHeader: header,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
	})
	require.ErrorIs(t, err, errChainsyncResyncRequested)

	select {
	case resync := <-resyncCh:
		assert.Equal(t, fixture.connId, resync.ConnectionId)
		assert.Equal(t, resyncReasonRollbackNotFound, resync.Reason)
	case <-time.After(2 * time.Second):
		t.Fatal("expected chainsync resync event")
	}

	assert.Zero(t, fixture.ls.headerMismatchCount)
	_, ok := fixture.ls.bufferedHeaderEvents[fixture.connId]
	assert.False(t, ok)
}

func TestReplayBufferedHeaderEventsStopsAfterResyncRequest(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) error {
		t.Fatal("unexpected blockfetch request after resync")
		return nil
	}

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	staleHash := testHashBytes("stale-fork-block")
	stalePrevHash := testHashBytes("missing-ancestor")
	validHash := testHashBytes("valid-buffered-header")
	staleHeader := mockHeader{
		hash:        lcommon.NewBlake2b256(staleHash),
		prevHash:    lcommon.NewBlake2b256(stalePrevHash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	validHeader := mockHeader{
		hash:        lcommon.NewBlake2b256(validHash),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 2,
		slot:        fixture.currentTip.Point.Slot + 20,
	}
	fixture.ls.bufferedHeaderEvents = map[ouroboros.ConnectionId][]ChainsyncEvent{
		fixture.connId: {
			{
				ConnectionId: fixture.connId,
				Point: ocommon.NewPoint(
					staleHeader.SlotNumber(),
					staleHeader.Hash().Bytes(),
				),
				BlockHeader: staleHeader,
				Tip: ochainsync.Tip{
					Point: ocommon.NewPoint(
						staleHeader.SlotNumber(),
						staleHeader.Hash().Bytes(),
					),
					BlockNumber: staleHeader.BlockNumber(),
				},
			},
			{
				ConnectionId: fixture.connId,
				Point: ocommon.NewPoint(
					validHeader.SlotNumber(),
					validHeader.Hash().Bytes(),
				),
				BlockHeader: validHeader,
				Tip: ochainsync.Tip{
					Point: ocommon.NewPoint(
						validHeader.SlotNumber(),
						validHeader.Hash().Bytes(),
					),
					BlockNumber: validHeader.BlockNumber(),
				},
			},
		},
	}

	err := fixture.ls.replayBufferedHeaderEvents(fixture.connId)
	require.NoError(t, err)

	select {
	case resync := <-resyncCh:
		assert.Equal(t, fixture.connId, resync.ConnectionId)
		assert.Equal(t, resyncReasonRollbackNotFound, resync.Reason)
	case <-time.After(2 * time.Second):
		t.Fatal("expected chainsync resync event")
	}

	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Equal(t, 0, fixture.ls.chain.HeaderCount())
	assert.Equal(t, ouroboros.ConnectionId{}, fixture.ls.headerPipelineConnId)
	_, ok := fixture.ls.bufferedHeaderEvents[fixture.connId]
	assert.False(t, ok)
}

func TestReplayBufferedHeadersAsyncSkipsActiveBlockfetch(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	requestedCh := make(chan struct{}, 1)
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) error {
		select {
		case requestedCh <- struct{}{}:
		default:
		}
		return nil
	}

	headerHash := testHashBytes("async-buffered-header")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(headerHash),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	fixture.ls.bufferedHeaderEvents = map[ouroboros.ConnectionId][]ChainsyncEvent{
		fixture.connId: {{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		}},
	}

	fixture.ls.chainsyncMutex.Lock()
	fixture.ls.replayBufferedHeadersAsync(fixture.connId)
	fixture.ls.chainsyncBlockfetchMutex.Lock()
	fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	fixture.ls.chainsyncBlockfetchMutex.Unlock()
	fixture.ls.chainsyncMutex.Unlock()

	select {
	case <-requestedCh:
		t.Fatal("unexpected blockfetch request while active batch is present")
	case <-time.After(100 * time.Millisecond):
	}

	fixture.ls.chainsyncMutex.Lock()
	defer fixture.ls.chainsyncMutex.Unlock()
	assert.Equal(t, 0, fixture.ls.chain.HeaderCount())
	assert.Equal(t, ouroboros.ConnectionId{}, fixture.ls.headerPipelineConnId)
	_, ok := fixture.ls.bufferedHeaderEvents[fixture.connId]
	assert.True(t, ok)
}

func TestReconcilePrimaryChainTipWithLedgerTipRollsBackMetadata(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.reconcilePrimaryChainTipWithLedgerTip())

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestProcessChainIteratorRollbackAppliesMatchingRollback(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	err := fixture.ls.processChainIteratorRollback(
		fixture.ancestorTip.Point,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestProcessChainIteratorRollbackSkipsStaleRollback(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	currentNonce := append([]byte(nil), fixture.ls.currentTipBlockNonce...)
	err := fixture.ls.processChainIteratorRollback(
		fixture.ancestorTip.Point,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(currentNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, dbTip)
}

func newChainsyncRollbackFixture(t *testing.T) *chainsyncRollbackFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ancestorHash := testHashBytes("ancestor-block")
	currentHash := testHashBytes("current-block")
	ancestorBlock := chain.RawBlock{
		Slot:        10,
		Hash:        ancestorHash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	currentBlock := chain.RawBlock{
		Slot:        20,
		Hash:        currentHash,
		BlockNumber: 2,
		Type:        1,
		PrevHash:    ancestorHash,
		Cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			ancestorBlock,
			currentBlock,
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	ancestorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ancestorBlock.Slot, ancestorBlock.Hash),
		BlockNumber: ancestorBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	ancestorNonce := []byte("nonce-ancestor")
	currentNonce := []byte("nonce-current")

	require.NoError(
		t,
		db.SetBlockNonce(
			ancestorTip.Point.Hash,
			ancestorTip.Point.Slot,
			ancestorNonce,
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			currentNonce,
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = append([]byte(nil), currentNonce...)
	ls.chainsyncState = SyncingChainsyncState

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}

	return &chainsyncRollbackFixture{
		ls:            ls,
		connId:        connId,
		ancestorTip:   ancestorTip,
		currentTip:    currentTip,
		ancestorNonce: ancestorNonce,
		forkPoint:     ocommon.NewPoint(currentBlock.Slot+10, testHashBytes("fork-point")),
	}
}

func testHashBytes(seed string) []byte {
	sum := sha256.Sum256([]byte(seed))
	return append([]byte(nil), sum[:]...)
}
