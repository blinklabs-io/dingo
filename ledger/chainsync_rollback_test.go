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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
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

func TestHandleEventChainsyncRollbackRejectsBelowMithrilBoundary(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot
	require.NoError(
		t,
		fixture.ls.db.SetSyncState("mithril_ledger_slot", "20", nil),
	)
	timer := time.NewTimer(time.Hour)
	t.Cleanup(func() { timer.Stop() })
	fixture.ls.activeBlockfetchConnId = fixture.connId
	fixture.ls.selectedBlockfetchConnId = fixture.connId
	fixture.ls.shadowBlockfetchConnId = fixture.connId
	fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	fixture.ls.chainsyncBlockfetchTimeoutTimer = timer
	fixture.ls.pendingBlockfetchEvents = []BlockfetchEvent{
		{Point: fixture.currentTip.Point},
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

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	storedBoundary, err := fixture.ls.db.GetSyncState(
		"mithril_ledger_slot",
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "20", storedBoundary)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected Mithril rollback-boundary resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)
	assert.Equal(t, ouroboros.ConnectionId{}, fixture.ls.activeBlockfetchConnId)
	assert.Equal(t, ouroboros.ConnectionId{}, fixture.ls.selectedBlockfetchConnId)
	assert.Equal(t, ouroboros.ConnectionId{}, fixture.ls.shadowBlockfetchConnId)
	assert.Nil(t, fixture.ls.chainsyncBlockfetchReadyChan)
	assert.Nil(t, fixture.ls.chainsyncBlockfetchTimeoutTimer)
	assert.Empty(t, fixture.ls.pendingBlockfetchEvents)
}

func TestHandleEventChainsyncRollbackPrunesStaleBlockNonces(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	competingHash := testHashBytes("competing-ancestor-slot")
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			competingHash,
			fixture.ancestorTip.Point.Slot,
			[]byte("stale-same-slot"),
			false,
			nil,
		),
	)
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			testHashBytes("future-fork-block"),
			fixture.currentTip.Point.Slot+10,
			[]byte("stale-future"),
			false,
			nil,
		),
	)

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
	)
	require.NoError(t, err)

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+11,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, fixture.ancestorTip.Point.Hash, rows[0].Hash)
	assert.Equal(t, fixture.ancestorTip.Point.Slot, rows[0].Slot)
	assert.Equal(t, fixture.ancestorNonce, rows[0].Nonce)
}

func TestLoadTipPrunesStaleBlockNonces(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	competingHash := testHashBytes("competing-tip-slot")
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			competingHash,
			fixture.currentTip.Point.Slot,
			[]byte("stale-same-slot"),
			false,
			nil,
		),
	)
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			testHashBytes("future-fork-block"),
			fixture.currentTip.Point.Slot+10,
			[]byte("stale-future"),
			false,
			nil,
		),
	)

	require.NoError(t, fixture.ls.loadTip())

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+11,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, fixture.ancestorTip.Point.Hash, rows[0].Hash)
	assert.Equal(t, fixture.currentTip.Point.Hash, rows[1].Hash)
	assert.True(
		t,
		bytes.Equal(
			fixture.ls.currentTipBlockNonce,
			rows[1].Nonce,
		),
	)
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

	// A genuinely un-crossable rollback point: a fork block below our tip
	// that is not present in our chain. The loop detector must break the loop
	// only for points the node cannot apply. A crossable point is instead
	// applied even when it repeats (see
	// TestHandleEventChainsyncRollbackAppliesRepeatedCrossableRollback and
	// TestHandleEventChainsyncRollbackAppliesCrossableRollbackAtLoopThreshold),
	// which is the issue #2790 fix.
	uncrossablePoint := ocommon.Point{
		Slot: fixture.currentTip.Point.Slot - 3,
		Hash: testHashBytes("uncrossable-missing-fork-block"),
	}

	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: uncrossablePoint.Slot,
				Hash: append([]byte(nil), uncrossablePoint.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        uncrossablePoint,
		},
	)
	require.ErrorIs(t, err, ErrRollbackLoopDetected)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

func TestHandleEventChainsyncRollbackExceedsKReconcilesDivergedLedgerTip(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "live-rollback")

	require.NoError(t, fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.Point{},
		},
	))

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, SyncingChainsyncState, fixture.ls.chainsyncState)
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+1,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, fixture.ancestorTip.Point.Hash, rows[0].Hash)
}

func TestTryResolveForkSynchronizesLedgerTip(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	forkHash := testHashBytes("fork-block")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
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
			// The delivered header is the first block after the fork point and
			// is contiguous with the common ancestor. The peer advertises a tip
			// further ahead, so its chain is genuinely longer than ours and the
			// fork is worth resolving.
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber()+10,
					testHashBytes("fork-block-peer-tip-ahead"),
				),
				BlockNumber: header.BlockNumber() + 1,
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

func TestTryResolveForkExceedsKReconcilesDivergedLedgerTip(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "live-fork-resolution")

	localTip := fixture.ls.chain.Tip()
	forkHash := testHashBytes("over-k-fork-resolution-block")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: localTip.BlockNumber + 1,
		slot:        localTip.Point.Slot + 10,
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
	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestTryResolveForkPropagatesAncestorLookupError(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ancestorLookupErr := errors.New("ancestor lookup failed")
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, ancestorLookupErr
	}

	forkHash := testHashBytes("lookup-error-fork-block")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(testHashBytes("lookup-error-ancestor")),
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

	require.False(t, resolved)
	require.ErrorIs(t, err, ancestorLookupErr)
	require.NotErrorIs(t, err, models.ErrBlockNotFound)
}

func TestHandleEventChainsyncBlockHeaderRestoresMismatchCountOnAncestorLookupError(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	ancestorLookupErr := errors.New("ancestor lookup failed")
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, ancestorLookupErr
	}
	fixture.ls.headerMismatchCount = 7

	forkHash := testHashBytes("handler-lookup-error-fork-block")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(testHashBytes("handler-lookup-error-ancestor")),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}

	err := fixture.ls.handleEventChainsyncBlockHeader(
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
	)

	require.ErrorIs(t, err, ancestorLookupErr)
	require.NotErrorIs(t, err, models.ErrBlockNotFound)
	assert.Equal(t, 7, fixture.ls.headerMismatchCount)
}

func TestTryResolveForkDoesNotAdvanceLaggingLedgerTip(t *testing.T) {
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

	aheadHash := testHashBytes("raw-chain-ahead-of-ledger")
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 10,
			Hash:        aheadHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.Equal(
		t,
		fixture.currentTip.Point.Slot+10,
		fixture.ls.chain.Tip().Point.Slot,
	)

	// Simulate the raw primary chain being well ahead of the metadata
	// ledger apply loop during historical catch-up.
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)
	preRollbackSeq := fixture.ls.lastLocalRollbackSeq

	forkHash := testHashBytes("ahead-raw-chain-fork")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 20,
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
			// The delivered header forks off the current block and is contiguous
			// with it. The peer advertises a tip further ahead, so its chain is
			// longer than the local raw chain tip and the fork resolves.
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber()+10,
					testHashBytes("ahead-raw-chain-fork-peer-tip-ahead"),
				),
				BlockNumber: header.BlockNumber() + 1,
			},
		},
		notFitErr,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, preRollbackSeq, fixture.ls.lastLocalRollbackSeq)
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"expected no local rollback resync event",
	)
}

func TestTryResolveForkQueuesKnownPeerForkSegment(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)

	forkHash1 := testHashBytes("fork-block-1")
	forkHash2 := testHashBytes("fork-block-2")
	forkHash3 := testHashBytes("fork-block-3")
	header1 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash1),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash2),
		prevHash:    lcommon.NewBlake2b256(forkHash1),
		blockNumber: fixture.ancestorTip.BlockNumber + 2,
		slot:        fixture.currentTip.Point.Slot + 2,
	}
	header3 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash3),
		prevHash:    lcommon.NewBlake2b256(forkHash2),
		blockNumber: fixture.ancestorTip.BlockNumber + 3,
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
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash2),
		prevHash:    lcommon.NewBlake2b256(forkHash1),
		blockNumber: fixture.ancestorTip.BlockNumber + 2,
		slot:        fixture.currentTip.Point.Slot + 2,
	}
	header3 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash3),
		prevHash:    lcommon.NewBlake2b256(forkHash2),
		blockNumber: fixture.ancestorTip.BlockNumber + 3,
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

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		2*time.Second,
		"expected chainsync resync event",
	)
	assert.Equal(t, fixture.connId, resync.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackNotFound,
		resync.Reason,
	)

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

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		2*time.Second,
		"expected chainsync resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalLedgerRollback,
		resync.Reason,
	)
	assert.Equal(t, fixture.ancestorTip.Point, resync.Point)
	assert.Equal(t, ouroboros.ConnectionId{}, resync.ConnectionId)
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

// Reproduces a chainsync recovery hang seen during multi-pool DevNet
// runs. After a slot battle the chain has already extended past `point`
// with the very block that peer history hands back as the only forkPath
// entry. The recovery loop must skip events whose slot is at or below
// the chain's header tip; otherwise AddBlockHeader rejects the duplicate
// with BlockNotFitChainTipError, clearQueuedHeaders fires, and the
// chainsync session never re-converges with the peer.
func TestRecoverPeerHeaderHistoryFromPointSkipsHeadersAlreadyAtChainTip(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	advancedHash := testHashBytes("recovery-already-at-tip")
	advancedSlot := fixture.currentTip.Point.Slot + 1
	advancedBlockNumber := fixture.currentTip.BlockNumber + 1
	require.NoError(
		t,
		fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
			{
				Slot:        advancedSlot,
				Hash:        advancedHash,
				BlockNumber: advancedBlockNumber,
				Type:        1,
				PrevHash:    fixture.currentTip.Point.Hash,
				Cbor:        []byte{0x80},
			},
		}),
	)
	require.Equal(
		t,
		advancedSlot,
		fixture.ls.chain.HeaderTip().Point.Slot,
		"chain header tip must reflect the post-rollback advance",
	)

	advancedHeader := mockHeader{
		hash:        lcommon.NewBlake2b256(advancedHash),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: advancedBlockNumber,
		slot:        advancedSlot,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			advancedHeader.slot,
			advancedHeader.hash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advancedHeader.slot,
				advancedHeader.hash.Bytes(),
			),
			BlockNumber: advancedHeader.blockNumber,
		},
		BlockHeader: advancedHeader,
	})

	fixture.ls.chainsyncMutex.Lock()
	headerCount, err := fixture.ls.recoverPeerHeaderHistoryFromPointLocked(
		fixture.connId,
		fixture.currentTip.Point,
	)
	fixture.ls.chainsyncMutex.Unlock()

	require.NoError(t, err)
	assert.Zero(t, headerCount)
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

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"expected no chainsync resync event",
	)
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

func TestReconcilePrimaryChainTipWithLedgerTipRollsBackMissingLedgerTipToCommonAncestor(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	forkHash := testHashBytes("startup-primary-chain-fork")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))

	require.NoError(t, fixture.ls.reconcilePrimaryChainTipWithLedgerTip())

	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+1,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, fixture.ancestorTip.Point.Hash, rows[0].Hash)
}

// TestReconcileLivePrimaryChainLedgerDivergenceExportedWrapperRecoversSubKFork
// pins the wire-in the plateau-watchdog path depends on
// (node_chainsync_recycler.go). Sub-K same-slot fork resolutions can
// advance chain.Tip() to the canonical hash while leaving the ledger
// pipeline pinned on the abandoned hash, with no error returned and
// therefore no caller of the existing in-package live-reconciler.
// The exported wrapper is what node-level watchdog code can invoke
// to repair the divergence in place — without it, the plateau path
// can only recycle the upstream peer, which does not unstick a
// locally-pinned ledger.
func TestReconcileLivePrimaryChainLedgerDivergenceExportedWrapperRecoversSubKFork(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	forkHash := testHashBytes("live-sub-k-fork")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.NotEqual(
		t,
		fixture.ls.chain.Tip(),
		fixture.ls.currentTip,
		"test setup must produce a chain/ledger tip divergence",
	)
	require.Equal(
		t,
		fixture.currentTip,
		fixture.ls.currentTip,
		"currentTip must remain at the post-switch abandoned hash",
	)

	reconciled, err := fixture.ls.ReconcileLivePrimaryChainLedgerDivergence(
		"local tip plateau",
		fixture.connId,
	)
	require.NoError(t, err)
	require.True(
		t,
		reconciled,
		"exported reconciler must report success",
	)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
}

// TestReconcilePrimaryChainTipWithLedgerTipCatchesUpWhenAheadBeyondK covers the
// old-Mithril-snapshot shape: the immutable primary chain is far ahead of the
// ledger tip (more than k blocks), but the ledger tip is still a valid ancestor
// on the primary chain. The primary chain must be preserved so ledgerProcessBlocks
// can replay forward and catch up; it must NOT be rewound (which would delete the
// very blocks needed to catch up).
func TestReconcilePrimaryChainTipWithLedgerTipCatchesUpWhenAheadBeyondK(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.currentEra.Id = 1
	fixture.ls.config.CardanoNodeConfig.ShelleyGenesis().SecurityParam = 2
	prevHash := fixture.currentTip.Point.Hash
	blocks := make([]chain.RawBlock, 0, 3)
	for idx, seed := range []string{
		"startup-primary-chain-ahead-1",
		"startup-primary-chain-ahead-2",
		"startup-primary-chain-ahead-3",
	} {
		hash := testHashBytes(seed)
		blockOffset := uint64(idx + 1)
		blocks = append(blocks, chain.RawBlock{
			Slot:        fixture.currentTip.Point.Slot + blockOffset,
			Hash:        hash,
			BlockNumber: fixture.currentTip.BlockNumber + blockOffset,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
	}
	require.NoError(t, fixture.ls.chain.AddRawBlocks(blocks))
	aheadTip := fixture.ls.chain.Tip()
	require.Equal(
		t,
		fixture.currentTip.Point.Slot+3,
		aheadTip.Point.Slot,
	)

	require.NoError(t, fixture.ls.reconcilePrimaryChainTipWithLedgerTip())

	// Ledger tip is unchanged: reconcile does not itself advance the ledger,
	// the forward replay happens later in ledgerProcessBlocks.
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, dbTip)

	// Primary chain is preserved at its original ahead tip, not rewound.
	assert.Equal(t, aheadTip, fixture.ls.chain.Tip())

	// Every block ahead of the ledger tip still exists so it can be replayed.
	for _, block := range blocks {
		_, err := fixture.ls.chain.BlockByPoint(
			ocommon.NewPoint(block.Slot, block.Hash),
			nil,
		)
		assert.NoError(
			t,
			err,
			"ahead block at slot %d should be preserved for catch-up",
			block.Slot,
		)
	}
}

func TestIntersectPointsDoesNotUsePrimaryChainWhenLedgerTipMissing(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	forkHash := testHashBytes("intersect-primary-chain-fork")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))

	points, err := fixture.ls.IntersectPoints(4)
	require.NoError(t, err)

	require.Empty(t, points)
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

func putPrimaryChainOnForkBeyondK(
	t *testing.T,
	fixture *chainsyncRollbackFixture,
	seedPrefix string,
) {
	t.Helper()

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	prevHash := fixture.ancestorTip.Point.Hash
	blocks := make([]chain.RawBlock, 0, 3)
	for idx := range 3 {
		blockOffset := uint64(idx + 1)
		hash := testHashBytes(fmt.Sprintf("%s-fork-%d", seedPrefix, idx))
		blocks = append(blocks, chain.RawBlock{
			Slot:        fixture.currentTip.Point.Slot + blockOffset*5,
			Hash:        hash,
			BlockNumber: fixture.ancestorTip.BlockNumber + blockOffset,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
	}
	require.NoError(t, fixture.ls.chain.AddRawBlocks(blocks))
	require.NotEqual(t, fixture.currentTip, fixture.ls.chain.Tip())
	require.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

func testHashBytes(seed string) []byte {
	sum := sha256.Sum256([]byte(seed))
	return append([]byte(nil), sum[:]...)
}

// A peer whose own reported tip is below the Mithril trust boundary is
// merely behind (still syncing or stuck) — its FindIntersect matched an
// old rung of our intersect ladder, which is not evidence of a competing
// fork. The rollback must still be refused, but the resync reason must
// classify the peer as stale rather than divergent so peer governance
// can back off instead of treating it as hostile.
func TestHandleEventChainsyncRollbackClassifiesStalePeerBelowMithrilBoundary(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot

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

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
			// The peer's own tip sits below our trust boundary.
			Tip: fixture.ancestorTip,
		},
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected stale-peer resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonPeerTipBehindMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)
}

// A peer that claims a tip at or above the Mithril trust boundary yet
// asks us to roll back below it does not carry our certified boundary
// block (always offered as an intersect point), so its chain genuinely
// diverges below the trust anchor and must be rejected as divergent.
func TestHandleEventChainsyncRollbackRejectsDivergentPeerTipAboveMithrilBoundary(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot

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

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
			// The peer claims a tip past our boundary while demanding a
			// rollback below it.
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					fixture.currentTip.Point.Slot+10,
					testHashBytes("divergent-peer-tip"),
				),
				BlockNumber: fixture.currentTip.BlockNumber + 1,
			},
		},
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected divergent-peer resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)
}
