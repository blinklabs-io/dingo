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
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	databaseTypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

func putLeiosRecords(
	t *testing.T,
	o *Ouroboros,
	records ...leiosRetentionRecord,
) {
	t.Helper()
	for _, r := range records {
		require.NoError(t, o.leiosDatabase().SetLeiosEB(
			r.slot, r.hash, []byte("manifest"), []cbor.RawMessage{},
		))
	}
}

// Certifications are pinned before collection, so a certified endorser block
// leaves the volatile partition by promotion and survives, while an unpinned
// one behind the immutable point is collected.
func TestLeiosPersistenceGCPinsBeforeCollecting(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 100)
	certified := leiosRetentionRecord{50, leiosRetentionHash(1)}
	stale := leiosRetentionRecord{60, leiosRetentionHash(2)}
	atTip := leiosRetentionRecord{100, leiosRetentionHash(3)}
	putLeiosRecords(t, o, certified, stale, atTip)
	store, err := o.leiosDatabase().LeiosDB()
	require.NoError(t, err)
	var passes atomic.Int32
	o.leiosPinAdvance = func(context.Context) error {
		passes.Add(1)
		_, err := store.Pin(
			leiosdb.Ref{Slot: certified.slot, Hash: certified.hash},
			leiosdb.Ref{Slot: 51, Hash: leiosRetentionHash(9)},
		)
		if err != nil {
			return err
		}
		return store.AdvancePinnedThrough(
			leiosdb.Ref{Slot: 100, Hash: leiosRetentionHash(8)},
		)
	}

	o.runLeiosPersistenceGC(context.Background(), nil)

	require.EqualValues(t, 1, passes.Load())
	requireLeiosEBPersisted(t, o, certified, true)
	requireLeiosEBPersisted(t, o, stale, false)
	requireLeiosEBPersisted(t, o, atTip, true)
}

// A pin walk that has not reached the immutable point holds the collection
// bound back to its own watermark: a certification not yet pinned must not be
// collected.
func TestLeiosPersistenceGCStopsAtPinWatermark(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 100)
	below := leiosRetentionRecord{50, leiosRetentionHash(1)}
	atWatermark := leiosRetentionRecord{60, leiosRetentionHash(2)}
	above := leiosRetentionRecord{70, leiosRetentionHash(3)}
	putLeiosRecords(t, o, below, atWatermark, above)
	store, err := o.leiosDatabase().LeiosDB()
	require.NoError(t, err)
	o.leiosPinAdvance = func(context.Context) error {
		return store.AdvancePinnedThrough(
			leiosdb.Ref{Slot: 60, Hash: leiosRetentionHash(8)},
		)
	}

	o.runLeiosPersistenceGC(context.Background(), nil)

	requireLeiosEBPersisted(t, o, below, false)
	requireLeiosEBPersisted(t, o, atWatermark, true)
	requireLeiosEBPersisted(t, o, above, true)
}

// A pin rollback demotes a certified endorser block and lowers the store's
// collection watermark. A later pass must collect against the lowered bounds,
// not the frontier an earlier pass published, or it deletes the demoted record.
func TestLeiosPersistenceGCAfterPinRollbackKeepsDemotedRecord(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 100)
	certified := leiosRetentionRecord{50, leiosRetentionHash(1)}
	putLeiosRecords(t, o, certified)
	store, err := o.leiosDatabase().LeiosDB()
	require.NoError(t, err)
	o.leiosPinAdvance = func(context.Context) error {
		_, err := store.Pin(
			leiosdb.Ref{Slot: certified.slot, Hash: certified.hash},
			leiosdb.Ref{Slot: 51, Hash: leiosRetentionHash(9)},
		)
		if err != nil {
			return err
		}
		return store.AdvancePinnedThrough(
			leiosdb.Ref{Slot: 100, Hash: leiosRetentionHash(8)},
		)
	}
	o.runLeiosPersistenceGC(context.Background(), nil)
	requireLeiosEBPersisted(t, o, certified, true)

	demoted, err := store.RollbackPins(
		leiosdb.Ref{Slot: 40, Hash: leiosRetentionHash(7)},
	)
	require.NoError(t, err)
	require.Equal(t, 1, demoted)
	withImmutableSlot(o, 30)
	o.leiosPinAdvance = func(context.Context) error { return nil }

	o.runLeiosPersistenceGC(context.Background(), nil)

	requireLeiosEBPersisted(t, o, certified, true)
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		certified.slot, certified.hash, []byte("manifest"), []cbor.RawMessage{},
	))
}

// With a pin walk wired, the serving bound is the pin-limited immutable slot
// raised to the collection watermark, and the watermark alone bounds it before
// the walk has pinned anything.
func TestLeiosServingBoundRaisesToCollectionWatermark(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.leiosPinAdvance = func(context.Context) error { return nil }
	store, err := o.leiosDatabase().LeiosDB()
	require.NoError(t, err)

	_, err = store.GarbageCollect(context.Background(), 60)
	require.NoError(t, err)
	bound, known := o.leiosServingBound(o.leiosDatabase())
	require.True(t, known)
	require.EqualValues(t, 60, bound)

	withImmutableSlot(o, 100)
	require.NoError(t, store.AdvancePinnedThrough(
		leiosdb.Ref{Slot: 40, Hash: leiosRetentionHash(8)},
	))
	bound, known = o.leiosServingBound(o.leiosDatabase())
	require.True(t, known)
	require.EqualValues(t, 60, bound)
}

func TestLeiosPersistenceGCCollectsNothingWhenPinWalkFails(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 100)
	old := leiosRetentionRecord{50, leiosRetentionHash(1)}
	putLeiosRecords(t, o, old)
	o.leiosPinAdvance = func(context.Context) error {
		return errors.New("chain read failed")
	}

	o.runLeiosPersistenceGC(context.Background(), nil)

	requireLeiosEBPersisted(t, o, old, true)
}

// Collection follows the primary chain: a chain event wakes the worker, which
// then collects what the immutable point has newly passed. No timer drives it.
func TestLeiosPersistenceGCRunsOnChainEvent(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	var (
		immutable atomic.Uint64
		lookups   atomic.Int32
	)
	immutable.Store(10)
	o.leiosImmutableSlot = func(context.Context) (uint64, bool, error) {
		lookups.Add(1)
		return immutable.Load(), true, nil
	}
	old := leiosRetentionRecord{50, leiosRetentionHash(1)}
	putLeiosRecords(t, o, old)

	o.startLeiosPersistenceGC(0, false)
	// The worker's first pass resolves the immutable point once, at 10,
	// before the point moves.
	testutil.WaitForCondition(t, func() bool {
		return lookups.Load() >= 1
	}, 5*time.Second, "collection worker never made its first pass")

	immutable.Store(100)
	o.signalLeiosGC(event.Event{})
	testutil.WaitForCondition(t, func() bool {
		_, err := o.leiosDatabase().GetLeiosEBManifest(old.hash, old.slot)
		return errors.Is(err, databaseTypes.ErrBlobKeyNotFound)
	}, 5*time.Second, "chain event did not trigger collection")
}

// An endorser block behind the persisted collection watermark is refused for
// serving even where the live immutable point has not moved past it, unless a
// certifying immutable ranking block pinned it.
func TestLeiosFetchServerRefusesBehindCollectionWatermark(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 10)
	tx, ref := testLeiosManifestTx(t, 1)
	raw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	})
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(raw).Bytes()
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		50, hash, raw, []cbor.RawMessage{tx},
	))
	store, err := o.leiosDatabase().LeiosDB()
	require.NoError(t, err)
	_, err = store.GarbageCollect(context.Background(), 40)
	require.NoError(t, err)
	point := ocommon.NewPoint(50, hash)

	_, err = o.leiosfetchServerBlockRequest(
		oleiosfetch.CallbackContext{},
		point,
	)
	require.NoError(t, err, "slot 50 is above the watermark")

	_, err = store.GarbageCollect(context.Background(), 60)
	require.NoError(t, err)
	require.False(t, o.leiosServingRefused(ocommon.NewPoint(70, hash)))
	require.True(
		t,
		o.leiosServingRefused(point),
		"behind the watermark and unpinned",
	)

	pinLeiosEB(t, o.leiosDatabase(), 50, hash)
	require.False(t, o.leiosServingRefused(point), "a pinned block is served")
}

// An unsolicited offer behind the immutable tip, or behind the collection
// watermark, is dropped; the boundary slot and later slots are not.
func TestLeiosOfferBehindImmutableTipIsRefused(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.leiosAnnouncementLedger = nil
	withImmutableSlot(o, 100)
	offer := func(slot uint64) bool {
		return o.leiosOfferedPointPlausible(
			ocommon.NewPoint(
				slot,
				leiosRetentionHash(1),
			),
			"peer-"+time.Now().String(),
		)
	}
	require.False(t, offer(99))
	require.True(t, offer(100))
	require.True(t, offer(150))

	store, err := o.leiosDatabase().LeiosDB()
	require.NoError(t, err)
	_, err = store.GarbageCollect(context.Background(), 200)
	require.NoError(t, err)
	require.False(t, offer(150), "behind the collection watermark")
	require.True(t, offer(200))
}
