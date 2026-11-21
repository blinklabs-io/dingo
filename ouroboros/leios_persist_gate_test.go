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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/leiosdb"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

// failLeiosStoreWrites makes every write to db's Leios store fail until the
// returned flag is cleared.
func failLeiosStoreWrites(
	t *testing.T,
	db *database.Database,
) *atomic.Bool {
	t.Helper()
	var fail atomic.Bool
	fail.Store(true)
	hookLeiosStore(t, db, &leiosStoreHooks{
		putVolatile: func(leiosdb.Ref, leiosdb.Content) error {
			if fail.Load() {
				return errors.New("store unavailable")
			}
			return nil
		},
	})
	return &fail
}

// cachedNotDurableLeiosBlock returns an Ouroboros whose cache holds a complete,
// slot-verified endorser block whose write failed.
func cachedNotDurableLeiosBlock(
	t *testing.T,
	idx int,
) (*Ouroboros, *atomic.Bool, ocommon.Point, []byte, *leiosEndorserBlockData) {
	t.Helper()
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosAnnouncementLedger = &fakeLeiosAnnouncementLedger{
		slotTime: time.Now(),
	}
	fail := failLeiosStoreWrites(t, o.leiosDatabase())
	point, raw, data := leiosGateTestBlock(t, idx, 1)
	require.Error(t, o.storeLeiosEndorserBlock(
		point, raw, data.txsRaw, leiosStoreAuthoritative,
	))
	cached, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, cached.completeTxCache())
	require.True(t, cached.slotVerified)
	require.False(t, o.leiosEndorserBlockDurable(cached))
	return o, fail, point, raw, cached
}

// leiosGateTestBlock builds an endorser block with txCount real transactions,
// which the cache accepts for a complete entry.
func leiosGateTestBlock(
	t *testing.T,
	slot int,
	txCount int,
) (ocommon.Point, []byte, *leiosEndorserBlockData) {
	t.Helper()
	txs := make([]cbor.RawMessage, txCount)
	for i := range txCount {
		txs[i] = testDijkstraTx(t, byte(i))
	}
	manifest := haskellEncodeLeiosEb(txs)
	point := ocommon.NewPoint(
		uint64(slot), //nolint:gosec // test slot
		lcommon.Blake2b256Hash(manifest).Bytes(),
	)
	return point, manifest, &leiosEndorserBlockData{
		point: point, blockRaw: manifest, txsRaw: txs, txCount: txCount,
	}
}

func swapInLeiosDatabase(t *testing.T, o *Ouroboros) *database.Database {
	t.Helper()
	withDB := newTestOuroborosWithLeiosDB(t)
	o.ledgerState = withDB.ledgerState
	return o.leiosDatabase()
}

// A fresh offer of a block cached whole whose write failed must dispatch the
// retry rather than return early; a durable one must not dispatch anything.
func TestLeiosTxsOfferRetriesFailedWrite(t *testing.T) {
	t.Parallel()
	o, first, second := newHeldManifestOfferPeers(t)
	guard := o.leiosFetchGuardFor(first.Id())
	db := swapInLeiosDatabase(t, o)
	fail := failLeiosStoreWrites(t, db)
	point, raw, data := leiosGateTestBlock(t, 410, 1)
	announceLeiosElectionForTest(t, o, point, uint64(len(raw)))
	require.Error(t, o.storeLeiosEndorserBlock(
		point, raw, data.txsRaw, leiosStoreAuthoritative,
	))
	o.leiosPeerAnnouncedForTest(first.Id(), nil, point)
	o.leiosPeerAnnouncedForTest(second.Id(), nil, point)
	offer := oleiosnotify.NewMsgBlockTxsOffer(point)

	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
	))
	require.Equal(
		t, int32(1), guard.inflight.Load(),
		"an offer of a block whose write failed must retry the write",
	)

	// Durable now: the same offer is skipped.
	fail.Store(false)
	cached, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.NoError(t, o.ensureLeiosEndorserBlockDurable(cached))
	secondGuard := o.leiosFetchGuardFor(second.Id())
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: second.Id()}, offer,
	))
	require.Zero(
		t, secondGuard.inflight.Load(),
		"a durable block must not dispatch work on every offer",
	)
}

// The manifest offer's already-cached shortcut has the same duty.
func TestLeiosManifestOfferRetriesFailedWrite(t *testing.T) {
	t.Parallel()
	o, first, _ := newHeldManifestOfferPeers(t)
	guard := o.leiosFetchGuardFor(first.Id())
	db := swapInLeiosDatabase(t, o)
	failLeiosStoreWrites(t, db)
	point, raw, _ := leiosGateTestBlock(t, 411, 1)
	announceLeiosElectionForTest(t, o, point, uint64(len(raw)))
	require.Error(t, o.storeLeiosEndorserBlock(
		point, raw, nil, leiosStoreAuthoritative,
	))
	cachedData, cached := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, cached)
	require.True(t, o.leiosEndorserBlockNeedsWrite(cachedData))
	o.leiosPeerAnnouncedForTest(first.Id(), nil, point)
	require.Zero(t, guard.inflight.Load())

	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()},
		oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw))),
	))
	require.Equal(
		t, int32(1), guard.inflight.Load(),
		"a manifest offer of a block whose write failed must retry the write",
	)
}

// A block whose write failed is not offered downstream by a later peer offer,
// and is released by the write that finally succeeds.
func TestLeiosRelayOfferWaitsForDurableContent(t *testing.T) {
	t.Parallel()
	o, fail, point, _, cached := cachedNotDurableLeiosBlock(t, 412)
	announceLeiosElectionForTest(t, o, point, uint64(len(cached.blockRaw)))
	// A connection reads from the log's tail at its first request, so it
	// registers before anything is appended.
	registered, _ := o.leiosEBLog.next("relay")
	require.Nil(t, registered)

	o.markLeiosEndorserBlockRelayOfferFrom(point, "peer")
	offer, _ := o.leiosEBLog.next("relay")
	require.Nil(t, offer, "content that is not durable must not be offered")

	fail.Store(false)
	require.NoError(t, o.ensureLeiosEndorserBlockDurable(cached))
	offer, _ = o.leiosEBLog.next("relay")
	require.NotNil(t, offer, "the successful write must release the offer")
}

// A header relayed for a block whose write failed stays parked: a released
// header prompts the receiver to request the closure.
func TestLeiosRelayAnnouncementWaitsForDurableContent(t *testing.T) {
	t.Parallel()
	o, fail, point, raw, cached := cachedNotDurableLeiosBlock(t, 413)
	header := testLeiosAnnouncingHeader(t, point, uint64(len(raw)))
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	require.True(t, ok)
	registered, _ := o.leiosEBLog.next("relay")
	require.Nil(t, registered)
	require.NoError(t, o.recordLeiosAnnouncement(
		header.Cbor(), header.Cbor(), ebHash, ebSize, header, "peer", true,
	))
	released, _ := o.leiosEBLog.next("relay")
	require.Nil(t, released, "a header for non-durable content must not relay")

	fail.Store(false)
	o.markLeiosEndorserBlockRelayOfferFrom(point, "peer")
	require.NoError(t, o.ensureLeiosEndorserBlockDurable(cached))
	released, _ = o.leiosEBLog.next("relay")
	require.NotNil(t, released, "the successful write must release the header")
}

// With no database nothing can be durable, so nothing waits for it.
func TestLeiosEndorserBlockNeedsWriteIgnoresMissingDatabase(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	_, _, data := leiosGateTestBlock(t, 414, 1)
	data.slotVerified = true
	require.False(t, o.leiosEndorserBlockNeedsWrite(data))
	require.True(t, o.leiosRelayContentReady(data))
}

// The ledger's by-point request must not be told a block is available when its
// write failed, and a durable block is served without a peer.
func TestFetchEndorserBlockByPointRetriesFailedWrite(t *testing.T) {
	t.Parallel()
	o, fail, point, _, _ := cachedNotDurableLeiosBlock(t, 415)

	require.Error(
		t,
		o.FetchEndorserBlockByPoint(context.Background(), point.Slot, point.Hash),
		"a cached block whose write failed is not available",
	)
	fail.Store(false)
	require.NoError(t, o.FetchEndorserBlockByPoint(
		context.Background(), point.Slot, point.Hash,
	))
	_, err := o.leiosDatabase().GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(t, err)
	_, err = o.leiosDatabase().GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.NoError(t, o.FetchEndorserBlockByPoint(
		context.Background(), point.Slot, point.Hash,
	), "a durable block needs no peer")
}

func TestClassifyLeiosFetchFailureLocalPersistence(t *testing.T) {
	t.Parallel()
	for _, err := range []error{
		fmt.Errorf("store txs: %w", errLeiosPersistWrite),
		fmt.Errorf("persist cached endorser block: %w", errLeiosPersistStopped),
	} {
		require.Equal(t, leiosFetchFailureLocal, classifyLeiosFetchFailure(err))
	}
	require.Equal(
		t,
		leiosFetchFailureTransient,
		classifyLeiosFetchFailure(errors.New("manifest fetch: timeout")),
	)
}

// A failed LeiosDB write is this node's fault, not the peer's.
func TestFetchEndorserBlockOnConnLocalWriteFailureKeepsPeerHealthy(
	t *testing.T,
) {
	t.Parallel()
	o, _, point, _, _ := cachedNotDurableLeiosBlock(t, 416)
	connId := namedConnId("healthy")

	err := o.fetchEndorserBlockOnConn(
		context.Background(),
		connId,
		nil,
		point,
		time.Now().Add(leiosBackfillPerAttemptTimeout),
	)
	require.ErrorIs(t, err, errLeiosPersistWrite)
	g := o.leiosFetchGuardFor(connId)
	require.Zero(t, g.consecutiveFailures.Load())
	require.False(t, g.inCooldown(time.Now()))
}

// Another peer would write to the same failing store, so the failure ends the
// by-point request instead of failing over.
func TestFetchEndorserBlockByPointDoesNotFailOverOnLocalWriteFailure(
	t *testing.T,
) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "one", "two")
	state := chainsync.NewState(nil, nil)
	state.SetClientConnId(peers[0].Id())
	o := newOuroboros(OuroborosConfig{ConnManager: cm, ChainsyncState: state})
	point := ocommon.NewPoint(200, []byte{1, 2})
	attempts := 0
	err := o.fetchEndorserBlockWithCandidates(
		context.Background(),
		point.Slot,
		point.Hash,
		[]ouroboros.ConnectionId{peers[0].Id(), peers[1].Id()},
		cm.GetConnectionById,
		func(
			context.Context,
			ouroboros.ConnectionId,
			*leiosfetch.Client,
			ocommon.Point,
			time.Time,
		) error {
			attempts++
			return fmt.Errorf("store txs: %w", errLeiosPersistWrite)
		},
	)
	require.ErrorIs(t, err, errLeiosPersistWrite)
	require.Equal(t, 1, attempts, "a local failure must not try another peer")
}
