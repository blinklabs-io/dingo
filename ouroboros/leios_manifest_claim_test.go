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
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// dispatchLeiosFetch composes the production admission and worker helpers for
// tests that need queued work without a notification claim.
func (o *Ouroboros) dispatchLeiosFetch(
	connId gouroboros.ConnectionId,
	fn func(),
) bool {
	guard, admitted := o.reserveLeiosFetch(connId)
	if !admitted {
		return false
	}
	o.dispatchLeiosFetchReserved(guard, fn)
	return true
}

func newHeldManifestOfferPeers(
	t *testing.T,
) (*Ouroboros, *gouroboros.Connection, *gouroboros.Connection) {
	t.Helper()
	first, firstDone := newLeiosFetchConversation(t, leiosFetchHandshake())
	t.Cleanup(func() { _ = first.Close() })
	second, secondDone := newLeiosFetchConversation(t, leiosFetchHandshake())
	t.Cleanup(func() { _ = second.Close() })
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(first, false, "first"))
	require.True(t, cm.AddConnection(second, false, "second"))
	o := newOuroboros(
		OuroborosConfig{
			ConnManager:        cm,
			EnableLeios:        true,
			EnableLeiosTxFetch: true,
		},
	)
	firstGuard := o.leiosFetchGuardFor(first.Id())
	secondGuard := o.leiosFetchGuardFor(second.Id())
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	firstGuard.mu.Lock()
	secondGuard.mu.Lock()
	// Both dispatch queues are held before notification admission. This tests
	// claim-before-dispatch, not how quickly a request reaches the wire.
	// Stop the protocols before unlocking so teardown cannot start a request
	// for which the handshake-only conversations have no response.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := cm.Stop(ctx)
		firstGuard.mu.Unlock()
		secondGuard.mu.Unlock()
		require.NoError(t, err)
		testutil.WaitForCondition(t, func() bool {
			return firstGuard.inflight.Load() == 0 &&
				secondGuard.inflight.Load() == 0
		}, 5*time.Second, "all admitted manifest work must finish during cleanup")
		claims := 0
		o.leiosManifestFetchInProgress.Range(func(_, _ any) bool {
			claims++
			return true
		})
		require.Zero(
			t,
			claims,
			"failed requests must release their manifest claims",
		)
	})
	requireLeiosFetchConversationDone(t, firstDone)
	requireLeiosFetchConversationDone(t, secondDone)
	return o, first, second
}

func TestLeiosManifestOfferClaimsBeforeCrossConnectionDispatch(t *testing.T) {
	t.Parallel()
	o, first, second := newHeldManifestOfferPeers(t)
	firstGuard := o.leiosFetchGuardFor(first.Id())
	secondGuard := o.leiosFetchGuardFor(second.Id())
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	for _, conn := range []struct {
		ctx oleiosnotify.CallbackContext
	}{
		{oleiosnotify.CallbackContext{ConnectionId: first.Id()}},
		{oleiosnotify.CallbackContext{ConnectionId: second.Id()}},
	} {
		require.NoError(t, o.leiosnotifyClientNotification(
			conn.ctx, oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw))),
		))
	}
	// dispatchLeiosFetch increments synchronously, so no scheduler polling is
	// needed: a duplicate admitted fetch is already observable on return.
	require.Equal(
		t,
		int32(1),
		firstGuard.inflight.Load()+secondGuard.inflight.Load(),
		"the same manifest occurrence must admit only one cross-connection fetch",
	)
	otherOccurrence := ocommon.NewPoint(point.Slot+1, point.Hash)
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: second.Id()},
		oleiosnotify.NewMsgBlockOffer(otherOccurrence, uint64(len(raw))),
	))
	require.Equal(t, int32(1), secondGuard.inflight.Load(),
		"a manifest claim must not suppress the same hash at another slot")
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()},
		oleiosnotify.NewMsgBlockTxsOffer(point),
	))
	require.Equal(
		t,
		int32(2),
		firstGuard.inflight.Load(),
		"a pending manifest must not suppress the transaction offer needed to complete it",
	)
}

func TestLeiosManifestOfferReleasesClaimWhenDispatchIsFull(t *testing.T) {
	t.Parallel()
	o, first, second := newHeldManifestOfferPeers(t)
	for range leiosFetchMaxInflightPerConn {
		require.True(t, o.dispatchLeiosFetch(first.Id(), func() {}))
	}
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	offer := oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw)))
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
	))
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: second.Id()}, offer,
	))
	require.Equal(
		t,
		int32(1),
		o.leiosFetchGuardFor(second.Id()).inflight.Load(),
		"dispatch rejection must release the claim so another connection can retry",
	)
}

func TestLeiosManifestOfferDoesNotClaimBeforeAdmission(t *testing.T) {
	t.Parallel()
	o, first, second := newHeldManifestOfferPeers(t)
	for range leiosFetchMaxInflightPerConn {
		require.True(t, o.dispatchLeiosFetch(first.Id(), func() {}))
	}
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	offer := oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw)))
	claimReached := make(chan struct{})
	releaseClaim := make(chan struct{})
	var claimOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseClaim) }) }
	t.Cleanup(release)
	o.leiosFetchClaimPublished = func() {
		claimOnce.Do(func() { close(claimReached) })
		<-releaseClaim
	}
	firstDone := make(chan struct{})
	firstErr := make(chan error, 1)
	go func() {
		defer close(firstDone)
		firstErr <- o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
		)
	}()
	select {
	case <-claimReached:
		// This is the pre-fix ordering: the full peer published the shared
		// claim before dispatch could reject its offer. A healthy peer must
		// still be admitted while that claim is held.
	case <-firstDone:
		// Fixed ordering: the full peer is rejected before it can publish a
		// claim, so its handler returns without reaching the seam.
	case <-time.After(5 * time.Second):
		t.Fatal("manifest offer admission did not complete")
	}
	secondDone := make(chan struct{})
	secondErr := make(chan error, 1)
	go func() {
		defer close(secondDone)
		secondErr <- o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{ConnectionId: second.Id()}, offer,
		)
	}()
	testutil.WaitForCondition(t, func() bool {
		return o.leiosFetchGuardFor(second.Id()).inflight.Load() == 1
	}, 5*time.Second, "healthy peer offer was not admitted")
	require.Equal(
		t,
		int32(1),
		o.leiosFetchGuardFor(second.Id()).inflight.Load(),
		"a full peer's transient claim must not suppress a healthy peer",
	)
	release()
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("full-peer manifest offer did not finish")
	}
	require.NoError(t, <-firstErr)
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("healthy-peer manifest offer did not finish")
	}
	require.NoError(t, <-secondErr)
}

func TestLeiosTxsOfferDoesNotClaimBeforeAdmission(t *testing.T) {
	t.Parallel()
	o, first, second := newHeldManifestOfferPeers(t)
	for range leiosFetchMaxInflightPerConn {
		require.True(t, o.dispatchLeiosFetch(first.Id(), func() {}))
	}
	point, _ := testLeiosEndorserBlockRaw(t, 200)
	offer := oleiosnotify.NewMsgBlockTxsOffer(point)
	claimReached := make(chan struct{})
	releaseClaim := make(chan struct{})
	var claimOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseClaim) }) }
	t.Cleanup(release)
	o.leiosFetchClaimPublished = func() {
		claimOnce.Do(func() { close(claimReached) })
		<-releaseClaim
	}
	firstDone := make(chan struct{})
	firstErr := make(chan error, 1)
	go func() {
		defer close(firstDone)
		firstErr <- o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
		)
	}()
	select {
	case <-claimReached:
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("transaction offer admission did not complete")
	}
	secondDone := make(chan struct{})
	secondErr := make(chan error, 1)
	go func() {
		defer close(secondDone)
		secondErr <- o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{ConnectionId: second.Id()}, offer,
		)
	}()
	testutil.WaitForCondition(t, func() bool {
		return o.leiosFetchGuardFor(second.Id()).inflight.Load() == 1
	}, 5*time.Second, "healthy peer transaction offer was not admitted")
	require.Equal(
		t,
		int32(1),
		o.leiosFetchGuardFor(second.Id()).inflight.Load(),
		"a full peer's transient transaction claim must not suppress a healthy peer",
	)
	release()
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("full-peer transaction offer did not finish")
	}
	require.NoError(t, <-firstErr)
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("healthy-peer transaction offer did not finish")
	}
	require.NoError(t, <-secondErr)
}

func TestLeiosTxsOfferRetriesAfterAdmissionReleases(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x45)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		450,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	bitmap := map[uint16]uint64{0: 1 << 63}
	var requests atomic.Int32
	conn, done := newLeiosFetchConversation(t, append(
		leiosFetchHandshake(),
		ouroboros_mock.ConversationEntryInput{
			ProtocolId: leiosfetch.ProtocolId,
			Message:    leiosfetch.NewMsgBlockTxsRequest(point, bitmap),
			MsgFromCborFunc: func(kind uint, data []byte) (protocol.Message, error) {
				requests.Add(1)
				return leiosfetch.NewMsgFromCbor(kind, data)
			},
		},
		ouroboros_mock.ConversationEntryOutput{
			ProtocolId: leiosfetch.ProtocolId,
			IsResponse: true,
			Messages: []protocol.Message{
				leiosfetch.NewMsgBlockTxs([]cbor.RawMessage{tx}),
			},
		},
	))
	t.Cleanup(func() { _ = conn.Close() })
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	require.True(t, cm.AddConnection(conn, false, "peer"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		ConnManager:        cm,
		EnableLeios:        true,
		EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifestRaw, nil, leiosStoreAuthoritative,
	))

	guard := o.leiosFetchGuardFor(conn.Id())
	guard.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(guard.mu.Unlock) }
	t.Cleanup(unlock)
	for range leiosFetchMaxInflightPerConn {
		require.True(t, o.dispatchLeiosFetch(conn.Id(), func() {}))
	}
	doneChan := make(chan any)
	t.Cleanup(func() { close(doneChan) })
	offer := oleiosnotify.NewMsgBlockTxsOffer(point)
	callback := oleiosnotify.CallbackContext{
		ConnectionId:       conn.Id(),
		ConnectionDoneChan: doneChan,
	}
	require.NoError(t, o.leiosnotifyClientNotification(callback, offer))
	require.NoError(t, o.leiosnotifyClientNotification(callback, offer),
		"duplicate offers must coalesce while admission is full")
	require.Zero(t, requests.Load(), "a full guard must not dispatch early")

	unlock()
	requireLeiosFetchConversationDone(t, done)
	testutil.WaitForCondition(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.completeTxCache() && data.relayTransactionsOffered
	}, 5*time.Second, "deferred transaction offer was not fetched and announced")
	require.Equal(t, int32(1), requests.Load(),
		"coalesced offers must issue exactly one body fetch")
}

func TestLeiosTxsOfferCancellationClearsPendingRetry(t *testing.T) {
	t.Parallel()
	o, first, _ := newHeldManifestOfferPeers(t)
	guard := o.leiosFetchGuardFor(first.Id())
	t.Cleanup(guard.cancelPendingLeiosFetches)
	for range leiosFetchMaxInflightPerConn {
		require.True(t, o.dispatchLeiosFetch(first.Id(), func() {}))
	}
	point, _ := testLeiosEndorserBlockRaw(t, 451)
	doneChan := make(chan any)
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{
			ConnectionId:       first.Id(),
			ConnectionDoneChan: doneChan,
		},
		oleiosnotify.NewMsgBlockTxsOffer(point),
	))
	close(doneChan)
	testutil.WaitForCondition(t, func() bool {
		guard.pendingMu.Lock()
		defer guard.pendingMu.Unlock()
		return len(guard.pending) == 0
	}, 5*time.Second, "connection cancellation must clear pending fetch work")
}

func fillLeiosPendingFetches(
	t *testing.T,
	guard *leiosFetchGuard,
	done <-chan any,
) {
	t.Helper()
	guard.inflight.Store(leiosFetchMaxInflightPerConn)
	for idx := range leiosFetchMaxPendingPerConn {
		require.NoError(t, guard.deferLeiosFetch(
			fmt.Sprintf("%d", idx), done, func() {},
		))
	}
}

func TestLeiosTxsOfferPendingRetryUsesAdmissionOpenedBeforeQueue(t *testing.T) {
	t.Parallel()
	guard := &leiosFetchGuard{}
	guard.inflight.Store(leiosFetchMaxInflightPerConn - 1)
	dispatched := make(chan struct{})
	require.NoError(t, guard.deferLeiosFetch("point", nil, func() {
		close(dispatched)
	}))
	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("offer was stranded after admission opened")
	}
	require.Equal(t, int32(leiosFetchMaxInflightPerConn), guard.inflight.Load())
}

func TestLeiosTxsOfferPendingBoundBlocksUntilSpace(t *testing.T) {
	t.Parallel()
	guard := &leiosFetchGuard{}
	done := make(chan any)
	fillLeiosPendingFetches(t, guard, done)
	result := make(chan error, 1)
	go func() {
		result <- guard.deferLeiosFetch("overflow", done, func() {})
	}()
	testutil.WaitForCondition(t, func() bool {
		guard.pendingMu.Lock()
		defer guard.pendingMu.Unlock()
		return guard.pendingChanged != nil
	}, 5*time.Second, "full pending queue did not apply backpressure")
	select {
	case <-result:
		t.Fatal("full pending queue returned before space opened")
	default:
	}

	(&Ouroboros{}).releaseLeiosFetch(guard)
	require.NoError(t, <-result)
	guard.pendingMu.Lock()
	require.Len(t, guard.pending, leiosFetchMaxPendingPerConn)
	guard.pendingMu.Unlock()
	close(done)
}

func TestLeiosTxsOfferPendingBoundUnblocksOnCancellation(t *testing.T) {
	t.Parallel()
	guard := &leiosFetchGuard{}
	done := make(chan any)
	fillLeiosPendingFetches(t, guard, done)
	result := make(chan error, 1)
	go func() {
		result <- guard.deferLeiosFetch("overflow", done, func() {})
	}()
	testutil.WaitForCondition(t, func() bool {
		guard.pendingMu.Lock()
		defer guard.pendingMu.Unlock()
		return guard.pendingChanged != nil
	}, 5*time.Second, "full pending queue did not apply backpressure")
	close(done)
	require.NoError(t, <-result)
	testutil.WaitForCondition(t, func() bool {
		guard.pendingMu.Lock()
		defer guard.pendingMu.Unlock()
		return len(guard.pending) == 0
	}, 5*time.Second, "connection cancellation did not clear pending work")
}

func TestLeiosTxsOfferHealthyPeerIsNotSuppressedByFailingPeer(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x46)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		460,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	first, firstDone := newLeiosFetchConversation(t, append(
		leiosFetchHandshake(),
		ouroboros_mock.ConversationEntryInput{
			ProtocolId:  leiosfetch.ProtocolId,
			MessageType: leiosfetch.MessageTypeBlockTxsRequest,
		},
		ouroboros_mock.ConversationEntryOutput{
			ProtocolId: leiosfetch.ProtocolId,
			IsResponse: true,
			Messages: []protocol.Message{
				leiosfetch.NewMsgBlockTxs(nil),
			},
		},
	))
	healthy, healthyDone := newLeiosFetchConversation(t, append(
		leiosFetchHandshake(),
		ouroboros_mock.ConversationEntryInput{
			ProtocolId:  leiosfetch.ProtocolId,
			MessageType: leiosfetch.MessageTypeBlockTxsRequest,
		},
		ouroboros_mock.ConversationEntryOutput{
			ProtocolId: leiosfetch.ProtocolId,
			IsResponse: true,
			Messages: []protocol.Message{
				leiosfetch.NewMsgBlockTxs([]cbor.RawMessage{tx}),
			},
		},
	))
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	require.True(t, cm.AddConnection(first, false, "first"))
	require.True(t, cm.AddConnection(healthy, false, "healthy"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		ConnManager:        cm,
		EnableLeios:        true,
		EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifestRaw, nil, leiosStoreAuthoritative,
	))
	firstGuard := o.leiosFetchGuardFor(first.Id())
	healthyGuard := o.leiosFetchGuardFor(healthy.Id())
	firstGuard.mu.Lock()
	healthyGuard.mu.Lock()
	var firstUnlockOnce sync.Once
	var healthyUnlockOnce sync.Once
	unlockFirst := func() { firstUnlockOnce.Do(firstGuard.mu.Unlock) }
	unlockHealthy := func() { healthyUnlockOnce.Do(healthyGuard.mu.Unlock) }
	t.Cleanup(unlockFirst)
	t.Cleanup(unlockHealthy)
	offer := oleiosnotify.NewMsgBlockTxsOffer(point)
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
	))
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: healthy.Id()}, offer,
	))
	require.Equal(t, int32(1), healthyGuard.inflight.Load(),
		"a first peer's claim must not suppress a healthy peer")
	requireLeiosTxFetchClaim(t, o, point, first.Id(), healthy.Id())

	unlockFirst()
	requireLeiosFetchConversationDone(t, firstDone)
	testutil.WaitForCondition(t, func() bool {
		return firstGuard.inflight.Load() == 0
	}, 5*time.Second, "failing peer fetch did not release admission")
	unlockHealthy()
	requireLeiosFetchConversationDone(t, healthyDone)
	testutil.WaitForCondition(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.completeTxCache()
	}, 5*time.Second, "healthy peer did not complete the transaction cache")
}

func requireLeiosTxFetchClaim(
	t *testing.T,
	o *Ouroboros,
	point ocommon.Point,
	owner gouroboros.ConnectionId,
	waiters ...gouroboros.ConnectionId,
) {
	t.Helper()
	o.leiosTxFetchClaimsMu.Lock()
	defer o.leiosTxFetchClaimsMu.Unlock()
	claim := o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)]
	require.NotNil(t, claim)
	require.Equal(t, owner, claim.owner.connId)
	require.Len(t, claim.waiters, len(waiters))
	for idx, waiter := range waiters {
		require.Equal(t, waiter, claim.waiters[idx].connId)
	}
}

type leiosTxOfferTestRequester struct {
	txs            []cbor.RawMessage
	perRound       time.Duration
	immediateFirst bool
	requests       int
	lastNeed       int
}

func (r *leiosTxOfferTestRequester) BlockRequest(
	context.Context,
	ocommon.Point,
) (protocol.Message, error) {
	return nil, fmt.Errorf("unexpected manifest request")
}

func (r *leiosTxOfferTestRequester) BlockTxsRequest(
	ctx context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	if r.perRound > 0 && !(r.immediateFirst && r.requests == 0) {
		timer := time.NewTimer(r.perRound)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	r.requests++
	r.lastNeed = len(requested)
	txs := make([]cbor.RawMessage, 0, len(requested))
	for _, idx := range requested {
		txs = append(txs, r.txs[idx])
	}
	return leiosfetch.NewMsgBlockTxs(txs), nil
}

func testLeiosTxOfferDeadlineFixture(
	t *testing.T,
	slot uint64,
) (*Ouroboros, ocommon.Point, []cbor.RawMessage) {
	t.Helper()
	// One request batch plus a short tail, so a relay that answers the first
	// batch and then stalls leaves a resumable partial cache.
	const txCount = leiosTxFetchWindowsPerRequest*leiosTxFetchWindowSize + 12
	txs := make([]cbor.RawMessage, 0, txCount)
	refs := make([]lcommon.LeiosTransactionReference, 0, txCount)
	for idx := range txCount {
		tx, ref := testLeiosManifestTxIndex(t, 0x50+idx)
		txs = append(txs, tx)
		refs = append(refs, ref)
	}
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: refs,
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		slot,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	o := newOuroboros(OuroborosConfig{
		EnableLeios: true, EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifestRaw, nil, leiosStoreAuthoritative,
	))
	return o, point, txs
}

func TestLeiosTxsOfferDeadlineHandsDribblingOwnerToHealthyWaiter(t *testing.T) {
	t.Parallel()
	o, point, txs := testLeiosTxOfferDeadlineFixture(t, 465)
	o.leiosTxFetchAttemptTimeoutOverride = 100 * time.Millisecond
	owner := &leiosTxOfferTestRequester{
		txs: txs, perRound: time.Second, immediateFirst: true,
	}
	healthy := &leiosTxOfferTestRequester{txs: txs}
	ownerGuard := &leiosFetchGuard{}
	waiterGuard := &leiosFetchGuard{}
	ownerGuard.inflight.Store(1)
	waiterGuard.inflight.Store(1)
	o.dispatchLeiosTxOfferFetch(
		ownerGuard, owner, namedConnId("dribble-owner"),
		"dribble-owner", point, nil,
	)
	o.dispatchLeiosTxOfferFetch(
		waiterGuard, healthy, namedConnId("healthy-waiter"),
		"healthy-waiter", point, nil,
	)
	testutil.WaitForCondition(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.completeTxCache() &&
			ownerGuard.inflight.Load() == 0 && waiterGuard.inflight.Load() == 0
	}, testutil.AsyncWait, "deadline did not hand off to the healthy waiter")
	require.Positive(t, owner.requests)
	require.Less(t, owner.requests, len(txs))
	require.Equal(t, 1, healthy.requests)
	require.Less(t, healthy.lastNeed, len(txs),
		"the waiter must resume the partial cache instead of starting over")
}

func TestLeiosTxsOfferDeadlineWithoutWaiterReleasesClaim(t *testing.T) {
	t.Parallel()
	o, point, txs := testLeiosTxOfferDeadlineFixture(t, 466)
	o.leiosTxFetchAttemptTimeoutOverride = 40 * time.Millisecond
	owner := &leiosTxOfferTestRequester{
		txs: txs, perRound: time.Second, immediateFirst: true,
	}
	guard := &leiosFetchGuard{}
	guard.inflight.Store(1)
	o.dispatchLeiosTxOfferFetch(
		guard, owner, namedConnId("lone-dribble-owner"),
		"lone-dribble-owner", point, nil,
	)
	key := leiosBlockKey(point.Slot, point.Hash)
	testutil.WaitForCondition(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		return guard.inflight.Load() == 0 && o.leiosTxFetchClaims[key] == nil
	}, 5*time.Second, "deadline without a waiter retained the global claim")
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.False(t, data.completeTxCache())
	require.Positive(t, data.partialTxCount(),
		"progress before the deadline must remain cached")
}

func TestLeiosTxsOfferHandsOffAcrossMultipleFailingPeers(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x47)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		461,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	newPeer := func(txs []cbor.RawMessage) (*gouroboros.Connection, <-chan error) {
		return newLeiosFetchConversation(t, append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlockTxs(txs),
				},
			},
		))
	}
	first, firstDone := newPeer(nil)
	second, secondDone := newPeer(nil)
	healthy, healthyDone := newPeer([]cbor.RawMessage{tx})
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	for idx, conn := range []*gouroboros.Connection{first, second, healthy} {
		require.True(t, cm.AddConnection(conn, false, fmt.Sprintf("peer-%d", idx)))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm, EnableLeios: true, EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifestRaw, nil, leiosStoreAuthoritative,
	))
	guards := []*leiosFetchGuard{
		o.leiosFetchGuardFor(first.Id()),
		o.leiosFetchGuardFor(second.Id()),
		o.leiosFetchGuardFor(healthy.Id()),
	}
	for _, guard := range guards {
		guard.mu.Lock()
	}
	var unlock [3]sync.Once
	for idx, guard := range guards {
		t.Cleanup(func() { unlock[idx].Do(guard.mu.Unlock) })
	}
	offer := oleiosnotify.NewMsgBlockTxsOffer(point)
	for _, conn := range []*gouroboros.Connection{first, second, healthy} {
		require.NoError(t, o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{ConnectionId: conn.Id()}, offer,
		))
	}
	requireLeiosTxFetchClaim(
		t, o, point, first.Id(), second.Id(), healthy.Id(),
	)

	unlock[0].Do(guards[0].mu.Unlock)
	requireLeiosFetchConversationDone(t, firstDone)
	testutil.WaitForCondition(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		claim := o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)]
		return claim != nil && claim.owner.connId == second.Id()
	}, 5*time.Second, "first failure did not hand off to the second peer")
	unlock[1].Do(guards[1].mu.Unlock)
	requireLeiosFetchConversationDone(t, secondDone)
	testutil.WaitForCondition(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		claim := o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)]
		return claim != nil && claim.owner.connId == healthy.Id()
	}, 5*time.Second, "second failure did not hand off to the healthy peer")
	unlock[2].Do(guards[2].mu.Unlock)
	requireLeiosFetchConversationDone(t, healthyDone)
	testutil.WaitForCondition(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.completeTxCache()
	}, 5*time.Second, "healthy third peer did not complete the transaction cache")
}

func TestLeiosTxsOfferCancellationBeforeWorkerLaunch(t *testing.T) {
	t.Parallel()
	conn, _ := newLeiosFetchConversation(t, leiosFetchHandshake())
	t.Cleanup(func() { _ = conn.Close() })
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	require.True(t, cm.AddConnection(conn, false, "peer"))
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm, EnableLeios: true, EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	entered := make(chan struct{})
	release := make(chan struct{})
	o.leiosTxFetchBeforeLaunch = func() {
		close(entered)
		<-release
	}
	point := ocommon.NewPoint(462, []byte{0x46, 0x47})
	done := make(chan any)
	result := make(chan error, 1)
	go func() {
		result <- o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{
				ConnectionId: conn.Id(), ConnectionDoneChan: done,
			},
			oleiosnotify.NewMsgBlockTxsOffer(point),
		)
	}()
	<-entered
	close(done)
	close(release)
	require.NoError(t, <-result)
	guard := o.leiosFetchGuardFor(conn.Id())
	testutil.WaitForCondition(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		return guard.inflight.Load() == 0 &&
			o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)] == nil
	}, 5*time.Second, "canceled owner launched or retained its fetch claim")
}

func TestLeiosTxFetchLaunchChecksConnectionBeforeDispatch(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosTxFetchClaims = make(map[string]*leiosTxFetchClaim)
	done := make(chan any)
	guard := &leiosFetchGuard{}
	guard.inflight.Store(1)
	guard.mu.Lock()
	point := ocommon.NewPoint(463, []byte{0x47, 0x48})
	candidate := newLeiosTxFetchCandidate(
		gouroboros.ConnectionId{}, "", guard, nil, point, done,
	)
	candidate.ctx = context.Background()
	candidate.cancel = func() {}
	candidate.attemptCtx = context.Background()
	candidate.attemptDone = func() {}
	close(done)
	defer func() {
		candidate.state.Store(leiosTxFetchCanceled)
		guard.mu.Unlock()
	}()
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosTxFetchClaims[key] = &leiosTxFetchClaim{owner: candidate}

	o.launchLeiosTxFetchOwner(key, candidate)

	require.Zero(t, guard.inflight.Load())
	require.NotContains(t, o.leiosTxFetchClaims, key)
}

func TestLeiosTxsOfferOwnerDisconnectHandsOffToWaiter(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x48)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		464,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	owner, _ := newLeiosFetchConversation(t, leiosFetchHandshake())
	healthy, healthyDone := newLeiosFetchConversation(t, append(
		leiosFetchHandshake(),
		ouroboros_mock.ConversationEntryInput{
			ProtocolId:  leiosfetch.ProtocolId,
			MessageType: leiosfetch.MessageTypeBlockTxsRequest,
		},
		ouroboros_mock.ConversationEntryOutput{
			ProtocolId: leiosfetch.ProtocolId,
			IsResponse: true,
			Messages: []protocol.Message{
				leiosfetch.NewMsgBlockTxs([]cbor.RawMessage{tx}),
			},
		},
	))
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	require.True(t, cm.AddConnection(owner, false, "owner"))
	require.True(t, cm.AddConnection(healthy, false, "healthy"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm, EnableLeios: true, EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifestRaw, nil, leiosStoreAuthoritative,
	))
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	o.leiosTxFetchBeforeLaunch = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	ownerDone := make(chan any)
	result := make(chan error, 1)
	go func() {
		result <- o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{
				ConnectionId: owner.Id(), ConnectionDoneChan: ownerDone,
			},
			oleiosnotify.NewMsgBlockTxsOffer(point),
		)
	}()
	<-entered
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: healthy.Id()},
		oleiosnotify.NewMsgBlockTxsOffer(point),
	))
	requireLeiosTxFetchClaim(t, o, point, owner.Id(), healthy.Id())
	close(ownerDone)
	testutil.WaitForCondition(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		claim := o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)]
		return claim != nil && claim.owner.connId == healthy.Id()
	}, 5*time.Second, "owner disconnect did not promote the healthy waiter")
	close(release)
	require.NoError(t, <-result)
	testutil.WaitForCondition(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.completeTxCache()
	}, 5*time.Second, "owner disconnect did not hand off to the healthy waiter")
	requireLeiosFetchConversationDone(t, healthyDone)
}

func TestLeiosTxsOfferQueuedOwnerDisconnectPromotesWaiterImmediately(t *testing.T) {
	t.Parallel()
	o, point, txs := testLeiosTxOfferDeadlineFixture(t, 467)
	ownerGuard := &leiosFetchGuard{}
	waiterGuard := &leiosFetchGuard{}
	ownerGuard.inflight.Store(1)
	waiterGuard.inflight.Store(1)
	ownerGuard.mu.Lock()
	defer ownerGuard.mu.Unlock()
	waiterGuard.mu.Lock()
	defer waiterGuard.mu.Unlock()
	ownerDone := make(chan any)
	o.dispatchLeiosTxOfferFetch(
		ownerGuard, &leiosTxOfferTestRequester{txs: txs},
		namedConnId("queued-owner"), "queued-owner", point, ownerDone,
	)
	o.dispatchLeiosTxOfferFetch(
		waiterGuard, &leiosTxOfferTestRequester{txs: txs},
		namedConnId("queued-waiter"), "queued-waiter", point, nil,
	)
	close(ownerDone)
	require.Eventually(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		claim := o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)]
		return claim != nil && claim.owner.connKey == "queued-waiter"
	}, 500*time.Millisecond, 10*time.Millisecond,
		"canceled queued owner did not immediately relinquish the claim")
}

func TestLeiosTxsOfferQueuedOwnerDeadlinePromotesWaiter(t *testing.T) {
	t.Parallel()
	o, point, txs := testLeiosTxOfferDeadlineFixture(t, 468)
	o.leiosTxFetchAttemptTimeoutOverride = 40 * time.Millisecond
	ownerGuard := &leiosFetchGuard{}
	waiterGuard := &leiosFetchGuard{}
	ownerGuard.inflight.Store(1)
	waiterGuard.inflight.Store(1)
	ownerGuard.mu.Lock()
	defer ownerGuard.mu.Unlock()
	waiterGuard.mu.Lock()
	defer waiterGuard.mu.Unlock()
	o.dispatchLeiosTxOfferFetch(
		ownerGuard, &leiosTxOfferTestRequester{txs: txs},
		namedConnId("deadline-owner"), "deadline-owner", point, nil,
	)
	o.dispatchLeiosTxOfferFetch(
		waiterGuard, &leiosTxOfferTestRequester{txs: txs},
		namedConnId("deadline-waiter"), "deadline-waiter", point, nil,
	)
	require.Eventually(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		claim := o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)]
		return claim != nil && claim.owner.connKey == "deadline-waiter"
	}, 500*time.Millisecond, 10*time.Millisecond,
		"queued time did not consume the owner's fixed attempt budget")
}

func TestLeiosTxsOfferTransferredOwnerCannotFetchWhenDequeued(t *testing.T) {
	t.Parallel()
	o, point, txs := testLeiosTxOfferDeadlineFixture(t, 469)
	stale := &leiosTxOfferTestRequester{txs: txs}
	healthy := &leiosTxOfferTestRequester{txs: txs}
	ownerGuard := &leiosFetchGuard{}
	waiterGuard := &leiosFetchGuard{}
	ownerGuard.inflight.Store(1)
	waiterGuard.inflight.Store(1)
	ownerGuard.mu.Lock()
	ownerDone := make(chan any)
	o.dispatchLeiosTxOfferFetch(
		ownerGuard, stale, namedConnId("stale-owner"),
		"stale-owner", point, ownerDone,
	)
	o.dispatchLeiosTxOfferFetch(
		waiterGuard, healthy, namedConnId("stale-waiter"),
		"stale-waiter", point, nil,
	)
	close(ownerDone)
	require.Eventually(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.completeTxCache()
	}, 2*time.Second, 10*time.Millisecond)
	ownerGuard.mu.Unlock()
	require.Eventually(t, func() bool {
		return ownerGuard.inflight.Load() == 0
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, stale.requests,
		"a callback dequeued after ownership transfer must not fetch")
	require.Equal(t, 2, healthy.requests, "one request per batch")
}

func TestLeiosTxsOfferOtherPathCompletionRelaysTransactionsOnce(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x61)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(470, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	o := newOuroboros(OuroborosConfig{
		EnableLeios: true, EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("observer", nil, nil)
	guard := &leiosFetchGuard{}
	guard.inflight.Store(1)
	guard.mu.Lock()
	o.dispatchLeiosTxOfferFetch(
		guard, &leiosTxOfferTestRequester{txs: []cbor.RawMessage{tx}},
		namedConnId("alternate-completion"), "alternate-completion", point, nil,
	)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifestRaw, []cbor.RawMessage{tx}, leiosStoreAuthoritative,
	))
	guard.mu.Unlock()
	require.Eventually(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.relayTransactionsOffered && guard.inflight.Load() == 0
	}, 2*time.Second, 10*time.Millisecond)
	o.leiosEBLog.mu.Lock()
	defer o.leiosEBLog.mu.Unlock()
	txOffers := 0
	for _, entry := range o.leiosEBLog.items {
		if entry.entry.txOffer != nil && entry.entry.txOffer.Slot == point.Slot &&
			bytes.Equal(entry.entry.txOffer.Hash, point.Hash) {
			txOffers++
		}
	}
	require.Equal(t, 1, txOffers)
}

func TestLeiosTxsOfferCacheMissIntentSurvivesOwnerCancellation(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x62)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(471, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	conn, _ := newLeiosFetchConversation(t, leiosFetchHandshake())
	t.Cleanup(func() { _ = conn.Close() })
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	require.True(t, cm.AddConnection(conn, false, "cache-miss-owner"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm, EnableLeios: true, EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("observer", nil, nil)
	guard := o.leiosFetchGuardFor(conn.Id())
	guard.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(guard.mu.Unlock) }
	t.Cleanup(unlock)
	ownerDone := make(chan any)
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{
			ConnectionId: conn.Id(), ConnectionDoneChan: ownerDone,
		},
		oleiosnotify.NewMsgBlockTxsOffer(point),
	))
	close(ownerDone)
	require.Eventually(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		return o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)] == nil
	}, 2*time.Second, 10*time.Millisecond,
		"canceled owner must release the exact-point claim")
	_, found := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(t, found, "relay intent alone must not appear as a manifest")
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifestRaw, []cbor.RawMessage{tx}, leiosStoreAuthoritative,
	))
	unlock()
	require.Eventually(t, func() bool {
		data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		return ok && data.relayTransactionsOffered && guard.inflight.Load() == 0
	}, 2*time.Second, 10*time.Millisecond)
	o.leiosEBLog.mu.Lock()
	defer o.leiosEBLog.mu.Unlock()
	txOffers := 0
	for _, entry := range o.leiosEBLog.items {
		if entry.entry.txOffer != nil && entry.entry.txOffer.Slot == point.Slot &&
			bytes.Equal(entry.entry.txOffer.Hash, point.Hash) {
			txOffers++
		}
	}
	require.Equal(t, 1, txOffers,
		"alternate completion must publish the retained transaction offer once")
}

func TestLeiosRelayOfferIntentCacheIsBounded(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	invalid := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size+1))
	o.markLeiosEndorserBlockRelayOffer(invalid)
	require.Nil(t, o.leiosEndorserBlocks[leiosBlockKey(invalid.Slot, invalid.Hash)], "an invalid hash length must not retain relay intent")
	points := make([]ocommon.Point, leiosEndorserBlockCacheMaxEntries+1)
	for idx := range points {
		points[idx] = ocommon.NewPoint(
			uint64(idx+1),
			[]byte(fmt.Sprintf("%032d", idx+1)),
		)
		o.markLeiosEndorserBlockRelayOffer(points[idx])
	}
	o.leiosMu.RLock()
	defer o.leiosMu.RUnlock()
	unique := make(map[*leiosEndorserBlockData]struct{})
	for _, data := range o.leiosEndorserBlocks {
		unique[data] = struct{}{}
	}
	require.LessOrEqual(t, len(unique), leiosEndorserBlockCacheMaxEntries)
	require.Nil(t, o.leiosEndorserBlocks[leiosBlockKey(points[0].Slot, points[0].Hash)], "the oldest distinct relay intent must be evicted at the cache cap")
	latest := o.leiosEndorserBlocks[leiosBlockKey(points[len(points)-1].Slot, points[len(points)-1].Hash)]
	require.NotNil(t, latest)
	require.True(t, latest.relayOfferRequested)
	require.False(t, latest.hasManifest())
}

func TestLeiosRelayOfferIntentCannotEvictManifest(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	manifestPoint, manifestRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(
		manifestPoint,
		manifestRaw,
		nil,
		leiosStoreAuthoritative,
	))

	points := make([]ocommon.Point, leiosEndorserBlockCacheMaxEntries)
	for idx := range points {
		points[idx] = ocommon.NewPoint(
			uint64(idx+2),
			[]byte(fmt.Sprintf("%032d", idx+1)),
		)
		o.markLeiosEndorserBlockRelayOffer(points[idx])
	}

	data, ok := o.lookupLeiosEndorserBlock(
		manifestPoint.Slot,
		manifestPoint.Hash,
	)
	require.True(t, ok, "relay intents must not evict a cached manifest")
	require.True(t, data.hasManifest())
	o.leiosMu.RLock()
	defer o.leiosMu.RUnlock()
	require.Nil(t,
		o.leiosEndorserBlocks[leiosBlockKey(points[0].Slot, points[0].Hash)],
		"the oldest intent must be evicted before a cached manifest",
	)
}

func TestLeiosUnverifiedManifestCannotEvictVerifiedManifest(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	verifiedPoint, verifiedRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(
		verifiedPoint,
		verifiedRaw,
		nil,
		leiosStoreAuthoritative,
	))

	var oldestUnverified ocommon.Point
	for idx := 2; idx < leiosEndorserBlockCacheMaxEntries+2; idx++ {
		point, raw := testLeiosEndorserBlockRaw(t, idx)
		if idx == 2 {
			oldestUnverified = point
		}
		require.NoError(t, o.storeLeiosEndorserBlock(
			point,
			raw,
			nil,
			leiosStorePeerOffered,
		))
	}

	data, ok := o.lookupLeiosEndorserBlock(
		verifiedPoint.Slot,
		verifiedPoint.Hash,
	)
	require.True(t, ok, "unverified manifests must not evict verified content")
	require.True(t, data.slotVerified)
	o.leiosMu.RLock()
	defer o.leiosMu.RUnlock()
	require.Nil(t,
		o.leiosEndorserBlocks[leiosBlockKey(
			oldestUnverified.Slot,
			oldestUnverified.Hash,
		)],
		"the oldest unverified manifest must be evicted before verified content",
	)
}

func TestLeiosRelayOfferIntentMergesWithPersistedCacheReload(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x63)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(472, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosEBLog.registerConn("observer", nil, nil)
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		point.Slot, point.Hash, manifestRaw, []cbor.RawMessage{tx},
	))
	const offerSource = "offer-source"
	o.markLeiosEndorserBlockRelayOfferFrom(point, offerSource)
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.hasManifest())
	require.True(t, data.completeTxCache())
	require.Equal(t, offerSource, data.relayOfferSource)
	data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.relayTransactionsOffered)
	o.leiosEBLog.mu.Lock()
	defer o.leiosEBLog.mu.Unlock()
	txOffers := 0
	for _, entry := range o.leiosEBLog.items {
		if entry.entry.txOffer != nil && entry.entry.txOffer.Slot == point.Slot &&
			bytes.Equal(entry.entry.txOffer.Hash, point.Hash) {
			txOffers++
		}
	}
	require.Equal(t, 1, txOffers,
		"a persisted completion must publish retained transaction intent once")
}

func expireLeiosRelayIntent(
	t *testing.T,
	o *Ouroboros,
	point ocommon.Point,
) {
	t.Helper()
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	current := o.leiosEndorserBlocks[key]
	require.NotNil(t, current)
	require.False(t, current.hasManifest())
	expired := *current
	expired.insertedAt = time.Now().Add(-leiosEndorserBlockCacheTTL - time.Second)
	for _, cacheKey := range current.cacheKeys {
		if o.leiosEndorserBlocks[cacheKey] == current {
			o.leiosEndorserBlocks[cacheKey] = &expired
		}
	}
}

func TestLeiosExpiredRelayIntentIsRemovedOnDatabaseMiss(t *testing.T) {
	t.Parallel()
	o := newTestOuroborosWithLeiosDB(t)
	point := ocommon.NewPoint(
		473,
		lcommon.Blake2b256Hash([]byte("expired-missing-intent")).Bytes(),
	)
	o.markLeiosEndorserBlockRelayOffer(point)
	expireLeiosRelayIntent(t, o, point)
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(t, ok)
	require.Nil(t, data)
	o.leiosMu.RLock()
	defer o.leiosMu.RUnlock()
	require.Nil(t,
		o.leiosEndorserBlocks[leiosBlockKey(point.Slot, point.Hash)],
		"an expired intent must be removed before a database miss returns",
	)
}

func TestLeiosExpiredRelayIntentDoesNotRelayDatabaseReload(t *testing.T) {
	t.Parallel()
	tx, ref := testLeiosManifestTx(t, 0x64)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(474, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosEBLog.registerConn("observer", nil, nil)
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		point.Slot, point.Hash, manifestRaw, []cbor.RawMessage{tx},
	))
	o.markLeiosEndorserBlockRelayOffer(point)
	expireLeiosRelayIntent(t, o, point)
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.hasManifest())
	require.True(t, data.completeTxCache())
	require.False(t, data.relayOfferRequested)
	require.False(t, data.relayTransactionsOffered)
	o.leiosEBLog.mu.Lock()
	defer o.leiosEBLog.mu.Unlock()
	require.Empty(t, o.leiosEBLog.items,
		"an expired intent must not relay reloaded content")
}

func TestLeiosTxsOfferDisconnectedWaiterIsRemoved(t *testing.T) {
	t.Parallel()
	first, _ := newLeiosFetchConversation(t, leiosFetchHandshake())
	second, _ := newLeiosFetchConversation(t, leiosFetchHandshake())
	t.Cleanup(func() { _ = first.Close() })
	t.Cleanup(func() { _ = second.Close() })
	cm := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{})
	require.True(t, cm.AddConnection(first, false, "first"))
	require.True(t, cm.AddConnection(second, false, "second"))
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm, EnableLeios: true, EnableLeiosTxFetch: true,
	})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	o.leiosTxFetchBeforeLaunch = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	point := ocommon.NewPoint(463, []byte{0x47, 0x48})
	firstDone := make(chan any)
	secondDone := make(chan any)
	result := make(chan error, 1)
	go func() {
		result <- o.leiosnotifyClientNotification(
			oleiosnotify.CallbackContext{
				ConnectionId: first.Id(), ConnectionDoneChan: firstDone,
			},
			oleiosnotify.NewMsgBlockTxsOffer(point),
		)
	}()
	<-entered
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{
			ConnectionId: second.Id(), ConnectionDoneChan: secondDone,
		},
		oleiosnotify.NewMsgBlockTxsOffer(point),
	))
	requireLeiosTxFetchClaim(t, o, point, first.Id(), second.Id())
	close(secondDone)
	testutil.WaitForCondition(t, func() bool {
		o.leiosTxFetchClaimsMu.Lock()
		defer o.leiosTxFetchClaimsMu.Unlock()
		claim := o.leiosTxFetchClaims[leiosBlockKey(point.Slot, point.Hash)]
		return claim != nil && len(claim.waiters) == 0 &&
			o.leiosFetchGuardFor(second.Id()).inflight.Load() == 0
	}, 5*time.Second, "disconnected waiter remained eligible for handoff")
	close(firstDone)
	close(release)
	require.NoError(t, <-result)
}

func TestLeiosManifestOfferClaimContentionReleasesAdmission(t *testing.T) {
	t.Parallel()
	o, first, _ := newHeldManifestOfferPeers(t)
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	offer := oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw)))
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosManifestFetchInProgress.Store(key, struct{}{})
	t.Cleanup(func() { o.leiosManifestFetchInProgress.Delete(key) })
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
	))
	require.Zero(t, o.leiosFetchGuardFor(first.Id()).inflight.Load(),
		"losing a shared claim must return its admission slot")
	_, present := o.leiosManifestFetchInProgress.Load(key)
	require.True(t, present, "the other worker's claim must survive")
	for range leiosFetchMaxInflightPerConn {
		require.True(t, o.dispatchLeiosFetch(first.Id(), func() {}))
	}
	require.False(t, o.dispatchLeiosFetch(first.Id(), func() {}))
}

func TestLeiosManifestOfferRechecksCacheAfterDispatch(t *testing.T) {
	t.Parallel()
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	var requests atomic.Int32
	conn, done := newLeiosFetchConversation(t, append(
		leiosFetchHandshake(),
		ouroboros_mock.ConversationEntryInput{
			ProtocolId: leiosfetch.ProtocolId,
			Message:    leiosfetch.NewMsgBlockRequest(point),
			MsgFromCborFunc: func(kind uint, data []byte) (protocol.Message, error) {
				requests.Add(1)
				return leiosfetch.NewMsgFromCbor(kind, data)
			},
		},
		ouroboros_mock.ConversationEntryOutput{
			ProtocolId: leiosfetch.ProtocolId, IsResponse: true,
			Messages: []protocol.Message{leiosfetch.NewMsgBlock(raw)},
		},
	))
	t.Cleanup(func() { _ = conn.Close() })
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	require.True(t, cm.AddConnection(conn, false, "peer"))
	o := newOuroboros(OuroborosConfig{ConnManager: cm, EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	guard := o.leiosFetchGuardFor(conn.Id())
	guard.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(guard.mu.Unlock) }
	t.Cleanup(unlock)
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw))),
	))
	require.Equal(t, int32(1), guard.inflight.Load())
	// Model another fetch completing after admission but before this worker
	// acquires its guard. The entry-point cache check has already missed.
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(point, raw, nil, leiosStorePeerOffered),
	)
	unlock()
	testutil.WaitForCondition(t, func() bool {
		return guard.inflight.Load() == 0
	}, 5*time.Second, "queued manifest worker must finish")
	require.Zero(
		t,
		requests.Load(),
		"a cache-filled queued offer must not fetch the manifest again",
	)
	_, claimed := o.leiosManifestFetchInProgress.Load(
		leiosBlockKey(point.Slot, point.Hash),
	)
	require.False(t, claimed, "cache-hit worker must release its claim")
	// Positive control: the still-live mock must observe an actual request.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := conn.LeiosFetch().Client.BlockRequest(ctx, point)
	require.NoError(t, err)
	requireLeiosFetchConversationDone(t, done)
	require.Equal(t, int32(1), requests.Load())
}

func TestLeiosManifestOfferReleasesClaimAfterSuccessfulFetch(t *testing.T) {
	t.Parallel()
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	conn, done := newLeiosFetchConversation(t, append(
		leiosFetchHandshake(),
		ouroboros_mock.ConversationEntryInput{
			ProtocolId: leiosfetch.ProtocolId, MessageType: leiosfetch.MessageTypeBlockRequest,
		},
		ouroboros_mock.ConversationEntryOutput{
			ProtocolId: leiosfetch.ProtocolId, IsResponse: true,
			Messages: []protocol.Message{leiosfetch.NewMsgBlock(raw)},
		},
	))
	t.Cleanup(func() { _ = conn.Close() })
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	require.True(t, cm.AddConnection(conn, false, "peer"))
	o := newOuroboros(OuroborosConfig{ConnManager: cm, EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw))),
	))
	requireLeiosFetchConversationDone(t, done)
	guard := o.leiosFetchGuardFor(conn.Id())
	testutil.WaitForCondition(t, func() bool {
		return guard.inflight.Load() == 0
	}, 5*time.Second, "successful manifest worker must finish")
	cached, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(
		t,
		ok,
		"successful manifest fetch must populate the occurrence cache",
	)
	require.Equal(t, []byte(raw), cached.blockRaw)
	_, claimed := o.leiosManifestFetchInProgress.Load(
		leiosBlockKey(point.Slot, point.Hash),
	)
	require.False(
		t,
		claimed,
		"successful manifest fetch must release its claim",
	)
}
