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

package ouroboros

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

func newPendingLeiosServePeer(
	t *testing.T,
) (*Ouroboros, *muxerServerPeer, oleiosfetch.CallbackContext) {
	t.Helper()
	var o *Ouroboros
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{
		ConnClosedOwnerFunc: func(conn *gouroboros.Connection, _ bool, _ error) {
			if o != nil && conn.ChainSync() != nil {
				o.ReleaseLeiosServeWaitersOwner(
					conn.Id(),
					conn.ChainSync().Server,
				)
			}
		},
	})
	o = newOuroboros(
		OuroborosConfig{
			EnableLeios: true,
			ConnManager: cm,
			LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{
				currentSlot: 10,
				slotTime:    time.Now().Add(-time.Minute),
				staleness:   ledger.LeiosAnnouncementFreshOCIN,
			},
		},
	)
	t.Cleanup(func() {
		_ = o.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cm.Stop(ctx)
	})
	peer := newRegisteredBlockfetchServerPeer(
		t,
		o,
		gouroboros.WithLeiosFetchConfig(
			oleiosfetch.NewConfig(o.leiosfetchServerConnOpts()...),
		),
	)
	id := gouroboros.ConnectionId{
		LocalAddr:  peer.peerConn.RemoteAddr(),
		RemoteAddr: peer.peerConn.LocalAddr(),
	}
	conn := cm.GetConnectionById(id)
	require.NotNil(t, conn)
	return o, peer, oleiosfetch.CallbackContext{
		ConnectionId: id,
		Server:       conn.LeiosFetch().Server,
	}
}

func admitPendingManifest(
	t *testing.T,
	o *Ouroboros,
	point ocommon.Point,
	raw []byte,
) *leiosManifestFetchClaim {
	t.Helper()
	o.EnqueueLeiosBlockAnnouncement(
		testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			lcommon.Blake2b256Hash(raw),
			uint64(len(raw)),
		),
	)
	o.leiosAnnouncementsMu.Lock()
	known := o.leiosAnnouncementBindsSlotLocked(point.Hash, point.Slot)
	o.leiosAnnouncementsMu.Unlock()
	require.True(t, known)
	claim := &leiosManifestFetchClaim{done: make(chan struct{})}
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosManifestFetchInProgress.Store(key, claim)
	t.Cleanup(func() { o.finishLeiosManifestFetch(key, claim) })
	return claim
}

func waitPendingLeiosServe(t *testing.T, o *Ouroboros) {
	t.Helper()
	testutil.WaitForCondition(
		t,
		func() bool { return o.leiosFetchServeWaiting.Load() == 1 },
		5*time.Second,
		"real LeiosFetch server to wait for admitted content",
	)
}

func TestLeiosFetchServerWaitsForAdmittedManifest(t *testing.T) {
	t.Parallel()
	o, peer, _ := newPendingLeiosServePeer(t)
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	claim := admitPendingManifest(t, o, point, raw)
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockRequest(point))
	waitPendingLeiosServe(t, o)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(point, raw, nil, leiosStoreAuthoritative),
	)
	o.finishLeiosManifestFetch(leiosBlockKey(point.Slot, point.Hash), claim)
	id, payload := peer.readMessage(t, testutil.AsyncWait)
	require.Equal(t, uint16(oleiosfetch.ProtocolId), id)
	var msg oleiosfetch.MsgBlock
	_, err := cbor.Decode(payload, &msg)
	require.NoError(t, err)
	require.Equal(t, []byte(raw), []byte(msg.BlockRaw))
	// The delayed response must leave the same bearer usable for another request.
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockRequest(point))
	_, payload = peer.readMessage(t, testutil.AsyncWait)
	_, err = cbor.Decode(payload, &msg)
	require.NoError(t, err)
}

func TestLeiosFetchServerWaitsOnlyForRequestedPartialTransactions(
	t *testing.T,
) {
	t.Parallel()
	o, peer, _ := newPendingLeiosServePeer(t)
	tx0, ref0 := testLeiosManifestTx(t, 0)
	_, ref1 := testLeiosManifestTx(t, 1)
	raw, err := cbor.Encode(
		&lcommon.LeiosEndorserBlock{
			TransactionReferences: []lcommon.LeiosTransactionReference{
				ref0,
				ref1,
			},
		},
	)
	require.NoError(t, err)
	point := ocommon.NewPoint(1, lcommon.Blake2b256Hash(raw).Bytes())
	manifest := admitPendingManifest(t, o, point, raw)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(point, raw, nil, leiosStoreAuthoritative),
	)
	o.finishLeiosManifestFetch(leiosBlockKey(point.Slot, point.Hash), manifest)
	pendingCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	o.leiosTxFetchClaimsMu.Lock()
	o.leiosTxFetchClaims = map[string]*leiosTxFetchClaim{
		leiosBlockKey(point.Slot, point.Hash): {
			owner: &leiosTxFetchCandidate{
				ctx:        pendingCtx,
				attemptCtx: pendingCtx,
			},
		},
	}
	o.leiosTxFetchClaimsMu.Unlock()
	peer.send(
		t,
		oleiosfetch.ProtocolId,
		oleiosfetch.NewMsgBlockTxsRequest(point, map[uint16]uint64{0: 1 << 63}),
	)
	waitPendingLeiosServe(t, o)
	o.retainLeiosPartialTxs(
		point.Slot,
		point.Hash,
		[]cbor.RawMessage{tx0, nil},
		nil,
	)
	_, payload := peer.readMessage(t, testutil.AsyncWait)
	var msg oleiosfetch.MsgBlockTxs
	_, err = cbor.Decode(payload, &msg)
	require.NoError(t, err)
	require.Equal(t, []cbor.RawMessage{tx0}, msg.TxsRaw)
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.False(
		t,
		data.completeTxCache(),
		"unrequested transaction must not delay the reply",
	)
}

func TestLeiosFetchServingWaitCancelsOnRealPeerDisconnect(t *testing.T) {
	t.Parallel()
	o, peer, _ := newPendingLeiosServePeer(t)
	o.leiosFetchServeTimeoutOverride = time.Hour
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	admitPendingManifest(t, o, point, raw)
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockRequest(point))
	waitPendingLeiosServe(t, o)
	require.NoError(t, peer.peerConn.Close())
	testutil.WaitForCondition(
		t,
		func() bool { return o.leiosFetchServeWaiting.Load() == 0 },
		5*time.Second,
		"peer disconnect to cancel pending serve without waiting for its hour deadline",
	)
	o.leiosServeWaitersMu.Lock()
	remaining := len(o.leiosServeWaiters)
	o.leiosServeWaitersMu.Unlock()
	require.Zero(t, remaining)
}

func TestLeiosFetchServingRejectsReplacedOwnerAndUnannouncedClaim(
	t *testing.T,
) {
	t.Parallel()
	o, _, ctx := newPendingLeiosServePeer(t)
	o.leiosFetchServeTimeoutOverride = time.Hour
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	claim := admitPendingManifest(t, o, point, raw)
	server := ctx.Server
	ctx.Server = new(oleiosfetch.Server)
	msg, err := o.leiosfetchServerBlockRequest(ctx, point)
	require.Nil(t, msg)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockNotFound)
	require.Zero(t, o.leiosFetchServeWaiting.Load())
	o.leiosAnnouncementsMu.Lock()
	delete(o.leiosAnnouncementSlots, string(point.Hash))
	o.leiosAnnouncementsMu.Unlock()
	require.NotNil(t, claim)
	ctx.Server = server
	msg, err = o.leiosfetchServerBlockRequest(ctx, point)
	require.Nil(t, msg)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockNotFound)
}

func TestLeiosManifestClaimCompletionPreservesNewGeneration(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	key := "occurrence"
	old := &leiosManifestFetchClaim{done: make(chan struct{})}
	next := &leiosManifestFetchClaim{done: make(chan struct{})}
	o.leiosManifestFetchInProgress.Store(key, next)
	o.finishLeiosManifestFetch(key, old)
	o.finishLeiosManifestFetch(key, old)
	got, ok := o.leiosManifestFetchInProgress.Load(key)
	require.True(t, ok)
	require.Same(t, next, got)
	select {
	case <-old.done:
	default:
		t.Fatal("old completion not signalled")
	}
	select {
	case <-next.done:
		t.Fatal("old owner completed replacement claim")
	default:
	}
	o.finishLeiosManifestFetch(key, next)
}

func TestLeiosFetchServingWaitStopsWhenAdmittedFetchFails(t *testing.T) {
	t.Parallel()
	o, peer, ctx := newPendingLeiosServePeer(t)
	o.leiosFetchServeTimeoutOverride = time.Hour
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	claim := admitPendingManifest(t, o, point, raw)
	_, done, closeErr := o.connManager.GetConnectionWithDoneAndError(
		ctx.ConnectionId,
	)
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockRequest(point))
	waitPendingLeiosServe(t, o)
	o.finishLeiosManifestFetch(leiosBlockKey(point.Slot, point.Hash), claim)
	select {
	case <-done:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("failed admitted fetch did not release server")
	}
	require.ErrorIs(t, closeErr(), oleiosfetch.ErrBlockNotFound)
	require.Zero(t, o.leiosFetchServeWaiting.Load())
}

func TestLeiosFetchServingWaitIsBounded(t *testing.T) {
	t.Parallel()
	o, peer, ctx := newPendingLeiosServePeer(t)
	o.leiosFetchServeTimeoutOverride = 20 * time.Millisecond
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	admitPendingManifest(t, o, point, raw)
	_, done, closeErr := o.connManager.GetConnectionWithDoneAndError(
		ctx.ConnectionId,
	)
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockRequest(point))
	select {
	case <-done:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("unfinished fetch exceeded server wait budget")
	}
	require.ErrorIs(t, closeErr(), oleiosfetch.ErrBlockNotFound)
	require.Zero(t, o.leiosFetchServeWaiting.Load())
}

func TestLeiosFetchServingWaiterLimitRejectsWithoutLeakingReservation(
	t *testing.T,
) {
	t.Parallel()
	o, _, ctx := newPendingLeiosServePeer(t)
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	admitPendingManifest(t, o, point, raw)
	o.leiosFetchServeWaiting.Store(leiosFetchServeMaxWaiters)
	msg, err := o.leiosfetchServerBlockRequest(ctx, point)
	require.Nil(t, msg)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockNotFound)
	require.Equal(
		t,
		int32(leiosFetchServeMaxWaiters),
		o.leiosFetchServeWaiting.Load(),
	)
	o.leiosFetchServeWaiting.Store(0)
}

func TestLeiosFetchServingDeadlineSurvivesManifestOwnerHandoffs(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		o, peer, ctx := newPendingLeiosServePeer(t)
		o.leiosFetchServeTimeoutOverride = time.Second
		point, raw := testLeiosEndorserBlockRaw(t, 1)
		claim := admitPendingManifest(t, o, point, raw)
		_, done, _ := o.connManager.GetConnectionWithDoneAndError(
			ctx.ConnectionId,
		)
		peer.send(
			t,
			oleiosfetch.ProtocolId,
			oleiosfetch.NewMsgBlockRequest(point),
		)
		synctest.Wait()
		require.Equal(t, int32(1), o.leiosFetchServeWaiting.Load())
		key := leiosBlockKey(point.Slot, point.Hash)
		for range 3 {
			<-time.After(250 * time.Millisecond)
			next := &leiosManifestFetchClaim{done: make(chan struct{})}
			o.leiosManifestFetchInProgress.Store(key, next)
			o.finishLeiosManifestFetch(key, claim)
			claim = next
			synctest.Wait()
		}
		<-time.After(250 * time.Millisecond)
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("new claim extended the original serving deadline")
		}
		o.finishLeiosManifestFetch(key, claim)
		require.Zero(t, o.leiosFetchServeWaiting.Load())
	})
}

func TestLeiosFetchServingWaitIgnoresOtherConnectionOwnerRelease(t *testing.T) {
	t.Parallel()
	o, peer, ctx := newPendingLeiosServePeer(t)
	o.leiosFetchServeTimeoutOverride = time.Hour
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	admitPendingManifest(t, o, point, raw)
	_, done, _ := o.connManager.GetConnectionWithDoneAndError(ctx.ConnectionId)
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockRequest(point))
	waitPendingLeiosServe(t, o)
	o.ReleaseLeiosServeWaitersOwner(ctx.ConnectionId, new(ochainsync.Server))
	require.Equal(t, int32(1), o.leiosFetchServeWaiting.Load())
	conn := o.connManager.GetConnectionById(ctx.ConnectionId)
	require.NotNil(t, conn)
	o.ReleaseLeiosServeWaitersOwner(ctx.ConnectionId, conn.ChainSync().Server)
	select {
	case <-done:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("actual owner failed to cancel serving wait")
	}
	require.Zero(t, o.leiosFetchServeWaiting.Load())
}

// An owner whose attempt expired still holds the claim until it is handed to
// the next candidate; a waiter must keep waiting through that handoff.
func TestLeiosPendingServeWorkSpansExpiredAttemptHandoff(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { _ = o.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempt, attemptCancel := context.WithCancel(ctx)
	attemptCancel()
	key := leiosBlockKey(7, []byte{1})
	o.leiosTxFetchClaims = map[string]*leiosTxFetchClaim{
		key: {owner: &leiosTxFetchCandidate{ctx: ctx, attemptCtx: attempt}},
	}
	_, txDone, pending := o.pendingLeiosServeWork(key)
	require.True(t, pending)
	require.Nil(t, txDone, "an expired attempt signal would spin the waiter")
}

func TestLeiosFetchServingWaiterLimitAdmitsBelowLimit(t *testing.T) {
	t.Parallel()
	o, _, ctx := newPendingLeiosServePeer(t)
	o.leiosFetchServeTimeoutOverride = 20 * time.Millisecond
	point, raw := testLeiosEndorserBlockRaw(t, 1)
	admitPendingManifest(t, o, point, raw)
	o.leiosFetchServeWaiting.Store(leiosFetchServeMaxWaiters - 1)
	start := time.Now()
	_, err := o.leiosfetchServerBlockRequest(ctx, point)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockNotFound)
	require.GreaterOrEqual(t, time.Since(start), 15*time.Millisecond,
		"a request below the limit must wait instead of being rejected at once")
	require.Equal(
		t,
		int32(leiosFetchServeMaxWaiters-1),
		o.leiosFetchServeWaiting.Load(),
	)
	o.leiosFetchServeWaiting.Store(0)
}
