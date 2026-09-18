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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gouroboros "github.com/blinklabs-io/gouroboros"
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

func TestLeiosOfferClaimContentionReleasesAdmission(t *testing.T) {
	t.Parallel()
	for _, transactions := range []bool{false, true} {
		name := "manifest"
		if transactions {
			name = "transactions"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o, first, _ := newHeldManifestOfferPeers(t)
			point, raw := testLeiosEndorserBlockRaw(t, 200)
			var offer protocol.Message = oleiosnotify.NewMsgBlockOffer(
				point, uint64(len(raw)),
			)
			claims := &o.leiosManifestFetchInProgress
			if transactions {
				offer = oleiosnotify.NewMsgBlockTxsOffer(point)
				claims = &o.leiosFetchInProgress
			}
			key := leiosBlockKey(point.Slot, point.Hash)
			claims.Store(key, struct{}{})
			t.Cleanup(func() { claims.Delete(key) })
			require.NoError(t, o.leiosnotifyClientNotification(
				oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
			))
			require.Zero(t, o.leiosFetchGuardFor(first.Id()).inflight.Load(),
				"losing a shared claim must return its admission slot")
			_, present := claims.Load(key)
			require.True(t, present, "the other worker's claim must survive")
			for range leiosFetchMaxInflightPerConn {
				require.True(t, o.dispatchLeiosFetch(first.Id(), func() {}))
			}
			require.False(t, o.dispatchLeiosFetch(first.Id(), func() {}))
		})
	}
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
