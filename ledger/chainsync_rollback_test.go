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
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func TestHandleEventChainsyncRollbackDoesNotSkipDifferentPeerHistory(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	otherConnId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: fixture.ancestorTip.Point.Slot,
				Hash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
			},
			connKey:   connIdKey(otherConnId),
			timestamp: time.Now(),
		},
	}

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
}

func TestHandleEventChainsyncRollbackSkipsSamePeerLoop(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: fixture.ancestorTip.Point.Slot,
				Hash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

func TestTryResolveForkSynchronizesLedgerTip(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

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

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestTryResolveForkQueuesKnownPeerForkSegment(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	forkHash1 := testHashBytes("fork-block-1")
	forkHash2 := testHashBytes("fork-block-2")
	forkHash3 := testHashBytes("fork-block-3")
	header1 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash1),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash2),
		prevHash:    lcommon.NewBlake2b256(forkHash1),
		blockNumber: fixture.currentTip.BlockNumber + 2,
		slot:        fixture.currentTip.Point.Slot + 2,
	}
	header3 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash3),
		prevHash:    lcommon.NewBlake2b256(forkHash2),
		blockNumber: fixture.currentTip.BlockNumber + 3,
		slot:        fixture.currentTip.Point.Slot + 3,
	}

	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(header1.SlotNumber(), forkHash1),
		BlockHeader:  header1,
	})
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(header2.SlotNumber(), forkHash2),
		BlockHeader:  header2,
	})

	err := fixture.ls.chain.AddBlockHeader(header3)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header3.SlotNumber(), forkHash3),
			BlockHeader:  header3,
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(header3.SlotNumber(), forkHash3),
				BlockNumber: header3.BlockNumber(),
			},
		},
		notFitErr,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.Equal(t, 3, fixture.ls.chain.HeaderCount())

	start, end := fixture.ls.chain.HeaderRange(10)
	assert.Equal(t, uint64(header1.SlotNumber()), start.Slot)
	assert.Equal(t, forkHash1, start.Hash)
	assert.Equal(t, uint64(header3.SlotNumber()), end.Slot)
	assert.Equal(t, forkHash3, end.Hash)
}

func TestTryResolveForkUsesObservedPeerHistoryFallback(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	forkHash1 := testHashBytes("observed-fork-block-1")
	forkHash2 := testHashBytes("observed-fork-block-2")
	forkHash3 := testHashBytes("observed-fork-block-3")
	header1 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash1),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash2),
		prevHash:    lcommon.NewBlake2b256(forkHash1),
		blockNumber: fixture.currentTip.BlockNumber + 2,
		slot:        fixture.currentTip.Point.Slot + 2,
	}
	header3 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash3),
		prevHash:    lcommon.NewBlake2b256(forkHash2),
		blockNumber: fixture.currentTip.BlockNumber + 3,
		slot:        fixture.currentTip.Point.Slot + 3,
	}

	observedHeaders := map[string]peerHeaderRecord{
		hex.EncodeToString(forkHash1): {
			event: ChainsyncEvent{
				ConnectionId: fixture.connId,
				Point:        ocommon.NewPoint(header1.SlotNumber(), forkHash1),
				BlockHeader:  header1,
			},
			prevHash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
		},
		hex.EncodeToString(forkHash2): {
			event: ChainsyncEvent{
				ConnectionId: fixture.connId,
				Point:        ocommon.NewPoint(header2.SlotNumber(), forkHash2),
				BlockHeader:  header2,
			},
			prevHash: append([]byte(nil), forkHash1...),
		},
	}
	fixture.ls.config.PeerHeaderLookupFunc = func(
		connId ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		require.Equal(t, fixture.connId, connId)
		record, ok := observedHeaders[hex.EncodeToString(hash)]
		if !ok {
			return ChainsyncEvent{}, nil, false
		}
		return record.event, append([]byte(nil), record.prevHash...), true
	}

	err := fixture.ls.chain.AddBlockHeader(header3)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header3.SlotNumber(), forkHash3),
			BlockHeader:  header3,
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(header3.SlotNumber(), forkHash3),
				BlockNumber: header3.BlockNumber(),
			},
		},
		notFitErr,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.Equal(t, 3, fixture.ls.chain.HeaderCount())

	start, end := fixture.ls.chain.HeaderRange(10)
	assert.Equal(t, uint64(header1.SlotNumber()), start.Slot)
	assert.Equal(t, forkHash1, start.Hash)
	assert.Equal(t, uint64(header3.SlotNumber()), end.Slot)
	assert.Equal(t, forkHash3, end.Hash)
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
	fixture.ls.bufferedHeaderEvents = map[string][]ChainsyncEvent{
		connIdKey(fixture.connId): {{
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
	require.NoError(t, err)

	select {
	case resync := <-resyncCh:
		assert.Equal(t, fixture.connId, resync.ConnectionId)
		assert.Equal(t, resyncReasonRollbackNotFound, resync.Reason)
	case <-time.After(2 * time.Second):
		t.Fatal("expected chainsync resync event")
	}

	assert.Zero(t, fixture.ls.headerMismatchCount)
	_, ok := fixture.ls.bufferedHeaderEvents[connIdKey(fixture.connId)]
	assert.False(t, ok)
}

func TestRollbackPublishesChainsyncResyncAtRollbackPoint(t *testing.T) {
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

	require.NoError(t, fixture.ls.rollback(fixture.ancestorTip.Point))

	select {
	case resync := <-resyncCh:
		assert.Equal(t, "local ledger rollback", resync.Reason)
		assert.Equal(t, fixture.ancestorTip.Point, resync.Point)
		assert.Equal(t, ouroboros.ConnectionId{}, resync.ConnectionId)
	case <-time.After(2 * time.Second):
		t.Fatal("expected chainsync resync event")
	}
}

func TestRecoverAfterLocalRollbackReplaysPeerHeaderHistory(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	require.NoError(
		t,
		fixture.ls.chain.Rollback(fixture.ancestorTip.Point),
	)
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)

	connId := fixture.connId
	requestCount := 0
	fixture.ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &connId
	}
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		requestConnId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) error {
		requestCount++
		assert.True(t, sameConnectionId(connId, requestConnId))
		assert.Equal(t, uint64(11), start.Slot)
		assert.Equal(t, uint64(12), end.Slot)
		return nil
	}

	header1Hash := lcommon.NewBlake2b256(testHashBytes("rollback-replay-1"))
	header2Hash := lcommon.NewBlake2b256(testHashBytes("rollback-replay-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.ancestorTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    header1Hash,
		blockNumber: fixture.ancestorTip.BlockNumber + 2,
		slot:        fixture.ancestorTip.Point.Slot + 2,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header1.slot,
			header1.hash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header2.slot,
				header2.hash.Bytes(),
			),
			BlockNumber: header2.blockNumber,
		},
		BlockHeader: header1,
	})
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header2.slot,
			header2.hash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header2.slot,
				header2.hash.Bytes(),
			),
			BlockNumber: header2.blockNumber,
		},
		BlockHeader: header2,
	})

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	require.True(t, result.Recovered)
	assert.False(t, result.SkipConnectionClose)
	assert.Equal(t, 2, fixture.ls.chain.HeaderCount())
	assert.True(
		t,
		sameConnectionId(fixture.ls.headerPipelineConnId, fixture.connId),
	)
	assert.True(
		t,
		sameConnectionId(
			fixture.ls.selectedBlockfetchConnId,
			fixture.connId,
		),
	)
	assert.True(
		t,
		sameConnectionId(
			fixture.ls.activeBlockfetchConnId,
			fixture.connId,
		),
	)
	assert.Equal(t, 1, requestCount)
	fixture.ls.blockfetchRequestRangeCleanup()
}

func TestRecoverAfterLocalRollbackResetsStateWithoutTrackedClients(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	header := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("rollback-reset-1")),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	require.NoError(t, fixture.ls.chain.AddBlockHeader(header))

	fixture.ls.headerPipelineConnId = fixture.connId
	fixture.ls.selectedBlockfetchConnId = fixture.connId
	fixture.ls.activeBlockfetchConnId = fixture.connId
	fixture.ls.headerMismatchCount = 3
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: fixture.currentTip.Point.Slot,
				Hash: append([]byte(nil), fixture.currentTip.Point.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}
	fixture.ls.bufferedHeaderEvents = map[string][]ChainsyncEvent{
		connIdKey(fixture.connId): {
			{
				ConnectionId: fixture.connId,
				Point: ocommon.NewPoint(
					header.slot,
					header.hash.Bytes(),
				),
				BlockHeader: header,
			},
		},
	}

	result := fixture.ls.RecoverAfterLocalRollback(
		nil,
		fixture.ancestorTip.Point,
	)

	require.False(t, result.Recovered)
	assert.False(t, result.SkipConnectionClose)
	assert.Zero(t, fixture.ls.chain.HeaderCount())
	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Nil(t, fixture.ls.rollbackHistory)
	assert.Nil(t, fixture.ls.bufferedHeaderEvents)
	assert.True(
		t,
		fixture.ls.headerPipelineConnId == (ouroboros.ConnectionId{}),
	)
	assert.True(
		t,
		fixture.ls.selectedBlockfetchConnId == (ouroboros.ConnectionId{}),
	)
	assert.True(
		t,
		fixture.ls.activeBlockfetchConnId == (ouroboros.ConnectionId{}),
	)
}

func TestRecoverAfterLocalRollbackDoesNotUsePreRollbackTipAsStalenessSignal(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	require.False(t, result.Recovered)
	assert.False(t, result.SkipConnectionClose)
	assert.Zero(t, result.PrimaryChainTipSlot)
}

func TestRecoverAfterLocalRollbackReturnsEmptyResultWhenChainNil(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.chain = nil

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	assert.Equal(t, LocalRollbackRecoveryResult{}, result)
}

func TestRecoverAfterLocalRollbackSkipsConnectionCloseWhenPrimaryChainTipPastRollbackPoint(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	queuedHeader := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("rollback-stale-queued")),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	require.NoError(t, fixture.ls.chain.AddBlockHeader(queuedHeader))

	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)
	fixture.ls.headerMismatchCount = 7
	fixture.ls.lastLocalRollbackSeq = 1
	fixture.ls.lastLocalRollbackPoint = ocommon.Point{
		Slot: fixture.ancestorTip.Point.Slot,
		Hash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
	}

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	require.False(t, result.Recovered)
	assert.True(t, result.SkipConnectionClose)
	assert.Equal(
		t,
		fixture.currentTip.Point.Slot,
		result.PrimaryChainTipSlot,
	)
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())
	assert.Equal(t, 7, fixture.ls.headerMismatchCount)
}

func TestHandleEventChainsyncBlockHeaderIgnoresStaleRollForwardBehindTip(
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

	staleHash := testHashBytes("stale-roll-forward")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(staleHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.ancestorTip.Point.Slot + 5,
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
				fixture.currentTip.Point.Slot+10,
				testHashBytes("peer-tip"),
			),
			BlockNumber: fixture.currentTip.BlockNumber + 10,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	select {
	case resync := <-resyncCh:
		t.Fatalf("expected no chainsync resync event, got: %#v", resync)
	case <-time.After(200 * time.Millisecond):
	}
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

func TestProcessChainIteratorRollbackNoopWhenLedgerAlreadyAtPoint(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))

	err := fixture.ls.processChainIteratorRollback(
		fixture.ancestorTip.Point,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
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
	require.ErrorIs(t, err, errRestartLedgerPipeline)

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

func TestLedgerProcessBlocksFromSourceRestartsOnStaleIteratorRollback(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	readChainResultCh := make(chan readChainResult, 1)
	readChainResultCh <- readChainResult{
		rollback:      true,
		rollbackPoint: fixture.ancestorTip.Point,
	}
	close(readChainResultCh)

	err := fixture.ls.ledgerProcessBlocksFromSource(
		context.Background(),
		readChainResultCh,
	)
	require.ErrorIs(t, err, errRestartLedgerPipeline)
}

func newChainsyncRollbackFixture(t *testing.T) *chainsyncRollbackFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

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
