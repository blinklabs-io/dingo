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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/leiosdb"
	databaseTypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// leiosPersistTestEntry builds one endorser block plus a complete transaction
// set of txCount bodies of roughly txBytes each.
//
// Each transaction body is a real CBOR byte string, not an arbitrary buffer:
// the write path re-encodes txsRaw through cbor.Encode, which fails on
// malformed members and would take the manifest write down with it.
func leiosPersistTestEntry(
	t *testing.T,
	idx int,
	txCount int,
	txBytes int,
) (ocommon.Point, cbor.RawMessage, *leiosEndorserBlockData) {
	t.Helper()
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, idx, txCount)
	txsRaw := make([]cbor.RawMessage, 0, txCount)
	for i := range txCount {
		body := make([]byte, txBytes)
		body[0] = byte(idx)
		if txBytes > 1 {
			body[1] = byte(i)
		}
		encoded, err := cbor.Encode(body)
		require.NoError(t, err)
		txsRaw = append(txsRaw, cbor.RawMessage(encoded))
	}
	data := &leiosEndorserBlockData{
		point:    point,
		blockRaw: blockRaw,
		txsRaw:   txsRaw,
		txCount:  txCount,
	}
	return point, blockRaw, data
}

// leiosStoreHooks wraps a Leios store so a test can slow or fail the
// newest-slot lookup the collection worker makes, or the writes the
// persistence path makes.
type leiosStoreHooks struct {
	leiosdb.Store
	maxSlot     func() (uint64, bool, error)
	putVolatile func(leiosdb.Ref, leiosdb.Content) error
}

func (s *leiosStoreHooks) PutVolatile(
	eb leiosdb.Ref,
	content leiosdb.Content,
) error {
	if s.putVolatile != nil {
		if err := s.putVolatile(eb, content); err != nil {
			return err
		}
	}
	return s.Store.PutVolatile(eb, content)
}

// hookLeiosStore installs hooks over db's Leios store for the rest of the
// test and returns the store underneath them.
func hookLeiosStore(
	t *testing.T,
	db *database.Database,
	hooks *leiosStoreHooks,
) leiosdb.Store {
	t.Helper()
	base, err := db.LeiosDB()
	require.NoError(t, err)
	hooks.Store = base
	db.SetLeiosDB(hooks)
	t.Cleanup(func() { db.SetLeiosDB(base) })
	return base
}

// blockLeiosStoreWrites makes every write to db's Leios store wait until
// release is called. entered receives once per write that reached the store.
func blockLeiosStoreWrites(
	t *testing.T,
	db *database.Database,
) (entered <-chan struct{}, release func()) {
	t.Helper()
	enteredCh := make(chan struct{}, 16)
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	hookLeiosStore(t, db, &leiosStoreHooks{
		putVolatile: func(leiosdb.Ref, leiosdb.Content) error {
			enteredCh <- struct{}{}
			<-gate
			return nil
		},
	})
	return enteredCh, release
}

// leiosFetchPeer serves one endorser block's manifest and transactions the way
// the reference node encodes them.
type leiosFetchPeer struct {
	*haskellBlockTxsPeer
	manifestRaw []byte
}

func (p *leiosFetchPeer) BlockRequest(
	context.Context,
	ocommon.Point,
) (protocol.Message, error) {
	return &leiosfetch.MsgBlock{BlockRaw: p.manifestRaw}, nil
}

// newLeiosFetchTestPeer returns an Ouroboros backed by a real LeiosDB, a peer
// serving a txCount-transaction endorser block at slot, and that block's
// point, with its election already observed.
func newLeiosFetchTestPeer(
	t *testing.T,
	slot uint64,
	txCount int,
) (*Ouroboros, *leiosFetchPeer, ocommon.Point) {
	t.Helper()
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosAnnouncementLedger = &fakeLeiosAnnouncementLedger{
		slotTime: time.Now(),
	}
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	txs := make([]cbor.RawMessage, txCount)
	for i := range txCount {
		txs[i] = testDijkstraTx(t, byte(i))
	}
	manifestRaw := haskellEncodeLeiosEb(txs)
	point := ocommon.NewPoint(
		slot,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	announceLeiosElectionForTest(t, o, point, uint64(len(manifestRaw)))
	return o, &leiosFetchPeer{
		haskellBlockTxsPeer: &haskellBlockTxsPeer{txs: txs},
		manifestRaw:         manifestRaw,
	}, point
}

// A fetch that reports success has written the endorser block to the LeiosDB:
// nothing that relies on that success may find the record missing.
func TestFetchLeiosTxOfferPersistsBeforeReportingSuccess(t *testing.T) {
	t.Parallel()
	o, peer, point := newLeiosFetchTestPeer(t, 700, 3)

	require.True(t, o.fetchLeiosTxOffer(
		context.Background(), peer, "peer", point,
	))

	db := o.leiosDatabase()
	manifest, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(
		t,
		err,
		"fetch reported success before the manifest was durable",
	)
	require.Equal(t, peer.manifestRaw, manifest)
	txs, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(
		t,
		err,
		"fetch reported success before the transactions were durable",
	)
	require.Equal(t, peer.txs, txs)
}

// A failed write fails the fetch and keeps the block out of the relay. The
// transactions stay cached, so the next offer has nothing to fetch; it must
// still retry the write rather than report the block as held.
func TestFetchLeiosTxOfferFailsOnPersistErrorAndRetryWrites(t *testing.T) {
	t.Parallel()
	o, peer, point := newLeiosFetchTestPeer(t, 701, 3)
	db := o.leiosDatabase()
	var failTxWrites atomic.Bool
	failTxWrites.Store(true)
	var txWrites atomic.Int32
	hookLeiosStore(t, db, &leiosStoreHooks{
		putVolatile: func(_ leiosdb.Ref, content leiosdb.Content) error {
			if len(content.Txs) == 0 {
				return nil
			}
			txWrites.Add(1)
			if failTxWrites.Load() {
				return errors.New("store unavailable")
			}
			return nil
		},
	})

	require.False(t, o.fetchLeiosTxOffer(
		context.Background(), peer, "peer", point,
	), "a fetch whose write failed must not report success")
	_, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
	relayOffer, _ := o.leiosEBLog.next("relay")
	require.Nil(
		t,
		relayOffer,
		"a block that is not durable must not be offered",
	)

	failTxWrites.Store(false)
	requests := peer.requests
	require.True(t, o.fetchLeiosTxOffer(
		context.Background(), peer, "peer", point,
	))
	require.Equal(t, requests, peer.requests,
		"the retry must reuse the cached transactions")
	require.Equal(t, int32(2), txWrites.Load(), "the retry must write again")
	txs, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, peer.txs, txs)
}

// Every connection re-offers a block it has; once its content is durable those
// offers must not each rewrite it.
func TestFetchLeiosTxOfferDoesNotRewriteDurableBlock(t *testing.T) {
	t.Parallel()
	o, peer, point := newLeiosFetchTestPeer(t, 702, 2)
	var writes atomic.Int32
	hookLeiosStore(t, o.leiosDatabase(), &leiosStoreHooks{
		putVolatile: func(leiosdb.Ref, leiosdb.Content) error {
			writes.Add(1)
			return nil
		},
	})

	require.True(t, o.fetchLeiosTxOffer(
		context.Background(), peer, "peer", point,
	))
	written := writes.Load()
	require.NotZero(t, written)
	for range 3 {
		require.True(t, o.fetchLeiosTxOffer(
			context.Background(), peer, "other", point,
		))
		require.NoError(t, o.storeLeiosEndorserBlock(
			point, peer.manifestRaw, nil, leiosStorePeerOffered,
		))
	}
	require.Equal(t, written, writes.Load())
}

func TestLeiosPersistManifestThenCompleteWritesTransactions(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)
	txsRaw := []cbor.RawMessage{
		testDijkstraTx(t, 0),
		testDijkstraTx(t, 1),
	}

	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	require.NotNil(t, db)

	// First the manifest only (no txs yet), as the backfiller's manifest fetch
	// does; then the complete block once its txs are fetched.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStoreAuthoritative,
	))
	manifest, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), manifest)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStoreAuthoritative,
	))
	gotTxs, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, txsRaw, gotTxs)
}

// The bounded wait in Close covers a write the store never finishes, and Close
// says so instead of reporting a clean shutdown.
// Not t.Parallel: swaps the package-level leiosPersistShutdownDrainTimeout.
func TestCloseReportsUnconfirmedLeiosPersistWrite(t *testing.T) {
	origTimeout := leiosPersistShutdownDrainTimeout
	leiosPersistShutdownDrainTimeout = 20 * time.Millisecond
	t.Cleanup(func() { leiosPersistShutdownDrainTimeout = origTimeout })

	o := newTestOuroborosWithLeiosDB(t)
	entered, release := blockLeiosStoreWrites(t, o.leiosDatabase())
	point, raw, data := leiosPersistTestEntry(t, 50, 1, 4)
	written := make(chan error, 1)
	go func() {
		_, err := o.persistLeiosEndorserBlock(point, raw, data)
		written <- err
	}()
	testutil.RequireReceive(t, entered, 5*time.Second, "write did not start")

	require.ErrorIs(t, o.Close(), ErrLeiosPersistDrainUnconfirmed)
	release()
	require.NoError(t, testutil.RequireReceive(
		t, written, 5*time.Second, "write did not finish",
	))
	require.NoError(t, o.Close())
}

func TestClosePreventsLeiosPersistence(t *testing.T) {
	t.Parallel()

	t.Run("close before first write", func(t *testing.T) {
		o := newTestOuroborosWithLeiosDB(t)
		o.config.LeiosPersistenceRetentionSlots = 100
		require.NoError(t, o.Close())

		point, blockRaw, data := leiosPersistTestEntry(t, 0, 1, 4)
		_, err := o.persistLeiosEndorserBlock(point, blockRaw, data)
		require.ErrorIs(t, err, errLeiosPersistStopped)

		require.False(t, o.leiosPersistGCStarted.Load())
		_, err = o.leiosDatabase().GetLeiosEBManifest(point.Hash, point.Slot)
		require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
	})

	t.Run("write in flight before close", func(t *testing.T) {
		o := newTestOuroborosWithLeiosDB(t)
		db := o.leiosDatabase()
		entered, release := blockLeiosStoreWrites(t, db)
		point, blockRaw, data := leiosPersistTestEntry(t, 1, 1, 4)
		written := make(chan error, 1)
		go func() {
			_, err := o.persistLeiosEndorserBlock(point, blockRaw, data)
			written <- err
		}()
		testutil.RequireReceive(
			t,
			entered,
			5*time.Second,
			"write did not start",
		)

		closeDone := make(chan error, 1)
		go func() { closeDone <- o.Close() }()
		testutil.RequireNoReceive(
			t, closeDone, 50*time.Millisecond,
			"Close returned while a write was in flight",
		)
		release()
		require.NoError(t, testutil.RequireReceive(
			t, closeDone, 5*time.Second, "Close did not finish",
		))
		require.NoError(t, testutil.RequireReceive(
			t, written, 5*time.Second, "write did not finish",
		))
		got, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
		require.NoError(t, err)
		require.Equal(t, []byte(blockRaw), got)
	})
}

// Each wait receives the full shutdown budget independently.
func TestLeiosPersistenceWorkersEachGetFullDrainBudget(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	ok := o.beginLeiosPersistWrite()
	require.True(t, ok)
	defer o.endLeiosPersistWrite()
	o.leiosPersistGCMu.Lock()
	o.leiosPersistGCStop = make(chan struct{})
	o.leiosPersistGCDone = make(chan struct{})
	o.leiosPersistGCMu.Unlock()
	o.leiosPersistGCStarted.Store(true)

	const budget = 400 * time.Millisecond
	waits := make([]time.Duration, 0, 2)
	wait := func(_ <-chan struct{}, timeout time.Duration) bool {
		waits = append(waits, timeout)
		return true
	}

	require.True(t, o.stopLeiosPersistenceWorkersWithWait(budget, wait))
	require.Equal(t, []time.Duration{budget, budget}, waits)
}

func TestLeiosPersistPauseWaitsForInFlightWrite(t *testing.T) {
	t.Parallel()
	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	entered, release := blockLeiosStoreWrites(t, db)
	point, raw, data := leiosPersistTestEntry(t, 71, 4, 512)
	written := make(chan error, 1)
	go func() {
		_, err := o.persistLeiosEndorserBlock(point, raw, data)
		written <- err
	}()
	testutil.RequireReceive(t, entered, 5*time.Second, "write did not start")

	paused := make(chan error, 1)
	go func() { paused <- o.PauseLeiosPersistWriterForLiveLifecycleOp() }()
	testutil.RequireNoReceive(
		t, paused, 50*time.Millisecond,
		"pause returned while a write was in flight",
	)
	release()
	require.NoError(t, testutil.RequireReceive(
		t, written, 5*time.Second, "write did not finish",
	))
	require.NoError(t, testutil.RequireReceive(
		t, paused, 5*time.Second, "pause did not finish",
	))
	manifest, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(raw), manifest)

	nextPoint, nextRaw, nextData := leiosPersistTestEntry(t, 72, 4, 512)
	_, err = o.persistLeiosEndorserBlock(nextPoint, nextRaw, nextData)
	require.ErrorIs(t, err, errLeiosPersistStopped)
}

// A live Restore/Truncate pauses persistence and then discards this
// Ouroboros for a replacement, so the pause must refuse writes for good: a
// fetch worker still running here must not reach the store being closed, even
// after the pause returned and LedgerState was reassigned.
func TestLeiosPersistPauseForLiveLifecycleOpRefusesWritesForGood(
	t *testing.T,
) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	oldDB := o.leiosDatabase()
	require.NotNil(t, oldDB)

	point1, blockRaw1 := testLeiosEndorserBlockRawWithRefs(t, 20, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point1, blockRaw1, nil, leiosStoreAuthoritative,
	))
	cached, ok := o.lookupLeiosEndorserBlock(point1.Slot, point1.Hash)
	require.True(t, ok)
	require.True(t, o.leiosEndorserBlockDurable(cached))

	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())

	replacement := newTestOuroborosWithLeiosDB(t)
	newDB := replacement.leiosDatabase()
	require.NotNil(t, newDB)
	o.ledgerState = replacement.ledgerState

	point2, blockRaw2 := testLeiosEndorserBlockRawWithRefs(t, 21, 1)
	require.ErrorIs(t, o.storeLeiosEndorserBlock(
		point2, blockRaw2, nil, leiosStoreAuthoritative,
	), errLeiosPersistStopped)
	_, err := newDB.GetLeiosEBManifest(point2.Hash, point2.Slot)
	require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
	_, err = oldDB.GetLeiosEBManifest(point2.Hash, point2.Slot)
	require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
}

// A pause that cannot confirm an in-flight write must leave persistence
// stopped: the caller escalates to a supervised restart, and a write accepted
// meanwhile could land in a store that is about to be closed or replaced.
// Not t.Parallel: swaps the package-level leiosPersistShutdownDrainTimeout.
func TestLeiosPersistPauseForLiveLifecycleOpFailsClosedOnUnconfirmedDrain(
	t *testing.T,
) {
	origTimeout := leiosPersistShutdownDrainTimeout
	leiosPersistShutdownDrainTimeout = 20 * time.Millisecond
	t.Cleanup(func() { leiosPersistShutdownDrainTimeout = origTimeout })

	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	entered, release := blockLeiosStoreWrites(t, db)
	point, raw, data := leiosPersistTestEntry(t, 30, 1, 4)
	written := make(chan error, 1)
	go func() {
		_, err := o.persistLeiosEndorserBlock(point, raw, data)
		written <- err
	}()
	testutil.RequireReceive(t, entered, 5*time.Second, "write did not start")

	require.ErrorIs(
		t,
		o.PauseLeiosPersistWriterForLiveLifecycleOp(),
		ErrLeiosPersistDrainUnconfirmed,
	)
	next, nextRaw, nextData := leiosPersistTestEntry(t, 31, 1, 4)
	_, err := o.persistLeiosEndorserBlock(next, nextRaw, nextData)
	require.ErrorIs(t, err, errLeiosPersistStopped)

	release()
	require.NoError(t, testutil.RequireReceive(
		t, written, 5*time.Second, "write did not finish",
	))
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())
	_, err = o.persistLeiosEndorserBlock(next, nextRaw, nextData)
	require.ErrorIs(t, err, errLeiosPersistStopped)
}

func TestLeiosPersistPauseAfterConcurrentWrites(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	const writers = 4
	const perWriter = 25

	type entry struct {
		point ocommon.Point
		raw   cbor.RawMessage
	}
	entries := make([][]entry, writers)
	for w := range writers {
		for i := range perWriter {
			point, raw := testLeiosEndorserBlockRawWithRefs(
				t, 100+w*perWriter+i, 1,
			)
			entries[w] = append(entries[w], entry{point, raw})
		}
	}

	errs := make(chan error, writers*perWriter)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for _, en := range entries[w] {
				_, err := o.persistLeiosEndorserBlock(en.point, en.raw, nil)
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())
	require.Nil(t, o.leiosPersistWritesIdle())
	db := o.leiosDatabase()
	for _, writerEntries := range entries {
		for _, en := range writerEntries {
			_, err := db.GetLeiosEBManifest(en.point.Hash, en.point.Slot)
			require.NoError(t, err)
		}
	}
}

func TestLeiosPersistenceWriterRunsDuringSlowGCScan(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	db := o.leiosDatabase()
	require.NotNil(t, db)

	oldPoint, oldRaw := testLeiosEndorserBlockRawWithRefs(t, 30, 1)
	require.NoError(t, db.SetLeiosEB(oldPoint.Slot, oldPoint.Hash, oldRaw, nil))

	releaseScan := make(chan struct{})
	var releaseScanOnce sync.Once
	release := func() { releaseScanOnce.Do(func() { close(releaseScan) }) }
	started := make(chan struct{})
	var startedOnce sync.Once
	var base leiosdb.Store
	base = hookLeiosStore(t, db, &leiosStoreHooks{
		maxSlot: func() (uint64, bool, error) {
			startedOnce.Do(func() { close(started) })
			<-releaseScan
			return base.MaxSlot()
		},
	})
	t.Cleanup(func() {
		release()
		require.NoError(t, o.Close())
	})

	o.startLeiosPersistenceGC(0, false)
	testutil.RequireReceive(
		t, started, time.Second, "GC did not begin its max-slot scan",
	)

	point, blockRaw, data := leiosPersistTestEntry(t, 40, 1, 4)
	written := make(chan error, 1)
	go func() {
		_, err := o.persistLeiosEndorserBlock(point, blockRaw, data)
		written <- err
	}()
	require.NoError(t, testutil.RequireReceive(
		t, written, time.Second,
		"persistence write stalled behind the GC's slow scan",
	))
	_, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(t, err)
}

func TestLeiosPersistenceWriterDoesNotRestorePrunedSlots(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	db := o.leiosDatabase()
	require.NotNil(t, db)

	newerPoint, newerRaw := testLeiosEndorserBlockRawWithRefs(t, 30, 1)
	require.NoError(t, db.SetLeiosEB(
		newerPoint.Slot,
		newerPoint.Hash,
		newerRaw,
		nil,
	))
	o.leiosPersistGCOnce.Do(func() {})
	o.runLeiosPersistenceGC(context.Background(), nil)

	point, blockRaw, data := leiosPersistTestEntry(t, 10, 1, 4)
	_, err := o.persistLeiosEndorserBlock(point, blockRaw, data)
	require.NoError(t, err, "a pruned slot is skipped, not failed")
	require.NoError(t, o.Close())

	_, err = db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
}

// With no immutable point the collection bound follows the newest persisted
// endorser block, so a persisted block must wake the collector itself.
func TestLeiosPersistSignalsCollectionAfterPersist(t *testing.T) {
	t.Parallel()
	o := newTestOuroborosWithLeiosDB(t)
	point, raw, data := leiosPersistTestEntry(t, 300, 4, 512)

	_, err := o.persistLeiosEndorserBlock(point, raw, data)
	require.NoError(t, err)

	testutil.RequireReceive(
		t, o.leiosGCSignal, 2*time.Second,
		"collection must be signalled after an endorser block persists",
	)
}

func TestLeiosVerifiedEbSlotRestoresFromPersistedManifest(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 42, 1)
	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{testDijkstraTx(t, 0)},
		leiosStoreAuthoritative,
	))

	// Simulate a restart: the persisted manifest remains, while the process
	// watermark and in-memory cache are rebuilt from zero.
	o.leiosMaxVerifiedEbSlot.Store(0)
	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()
	o.restoreLeiosVerifiedEbSlot()

	require.Equal(t, point.Slot, o.MaxVerifiedEndorserBlockSlot())
}

// TestLeiosVerifiedEbSlotRestoredByNewOuroboros pins the call site rather
// than the helper. The test above calls restoreLeiosVerifiedEbSlot directly,
// so deleting the o.restoreLeiosVerifiedEbSlot() line from newOuroboros
// leaves ./ouroboros/ fully green and the restore ships inert -- the same
// class TestBuildDingoConfigWiresForgeTolerances was added for on the config
// knobs. Constructing a second Ouroboros over the same
// database and reading the exported watermark is what closes it.
func TestLeiosVerifiedEbSlotRestoredByNewOuroboros(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 4242, 1)
	first := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, first.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{testDijkstraTx(t, 0)},
		leiosStoreAuthoritative,
	))

	// A restart: a brand-new Ouroboros over the same database, with nothing
	// carried over in process and nothing called on it but the constructor.
	second := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LedgerState:             first.ledgerState,
		LeiosAnnouncementLedger: first.ledgerState,
	})
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	require.Equal(t, point.Slot, second.MaxVerifiedEndorserBlockSlot())
}

func TestLeiosPersistenceGCRetainsConfiguredSlotWindow(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	db := o.leiosDatabase()
	require.NotNil(t, db)
	txs := []cbor.RawMessage{mustCbor(t, "tx")}
	oldHash := make([]byte, 32)
	boundaryHash := make([]byte, 32)
	newHash := make([]byte, 32)
	oldHash[0], boundaryHash[0], newHash[0] = 1, 2, 3
	for _, record := range []struct {
		slot uint64
		hash []byte
	}{
		{slot: 10, hash: oldHash},
		{slot: 20, hash: boundaryHash},
		{slot: 30, hash: newHash},
	} {
		require.NoError(t, db.SetLeiosEB(
			record.slot,
			record.hash,
			[]byte("manifest"),
			txs,
		))
	}

	o.runLeiosPersistenceGC(context.Background(), nil)
	_, err := db.GetLeiosEBManifest(oldHash, 10)
	require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
	for _, record := range []struct {
		slot uint64
		hash []byte
	}{
		{slot: 20, hash: boundaryHash},
		{slot: 30, hash: newHash},
	} {
		_, err := db.GetLeiosEBManifest(record.hash, record.slot)
		require.NoError(t, err)
	}
}

func (s *leiosStoreHooks) MaxSlot() (uint64, bool, error) {
	if s.maxSlot != nil {
		return s.maxSlot()
	}
	return s.Store.MaxSlot()
}

func TestLeiosPersistenceGCRetriesAfterStartupWatermarkScanFailure(
	t *testing.T,
) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	t.Cleanup(func() { require.NoError(t, o.Close()) })

	db := o.leiosDatabase()
	require.NotNil(t, db)
	base, err := db.LeiosDB()
	require.NoError(t, err)
	db.SetLeiosDB(&leiosStoreHooks{
		Store: base,
		maxSlot: func() (uint64, bool, error) {
			return 0, false, errors.New("scan failed")
		},
	})
	_, err = db.MaxLeiosEBSlot()
	require.Error(t, err)

	o.restoreLeiosVerifiedEbSlot()
	require.True(
		t,
		o.leiosPersistGCStarted.Load(),
		"GC must retry its watermark scan after startup restoration fails",
	)
}

func TestWaitForLeiosPersistWorkerPrefersCompletedWorkerAtExpiredDeadline(
	t *testing.T,
) {
	t.Parallel()

	done := make(chan struct{})
	close(done)
	for range 100 {
		require.True(t, waitForLeiosPersistWorker(done, 0))
		require.True(t, waitForLeiosPersistWorker(done, -time.Second))
	}
}

func TestLeiosPersistenceGCPausesBeforeLiveDatabaseReplacement(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	o.startLeiosPersistenceGC(0, false)
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())
	require.False(t, o.leiosPersistGCStarted.Load())
}

// TestLeiosPersistTwoOccurrencesOfSameHashPersistIndependently verifies that
// durable blob records distinguish occurrences by hash and slot, so
// when two live occurrences of the same content-addressed hash existed at
// different slots, the second persist silently overwrote the first --
// making it permanently unavailable for historical re-serving once its
// in-memory entry expired, even though both remained legitimately cached in
// memory. The blob store (SetLeiosEB/GetLeiosEBManifest/GetLeiosEBTxs) is now
// keyed by (slot, hash), so both occurrences persist and reload
// independently.
func TestLeiosPersistTwoOccurrencesOfSameHashPersistIndependently(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 30, 1)
	second := ocommon.Point{Slot: point.Slot + 1, Hash: point.Hash}
	txs1 := []cbor.RawMessage{testDijkstraTx(t, 0)}
	txs2 := []cbor.RawMessage{testDijkstraTx(t, 1)}

	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			txs1,
			leiosStoreAuthoritative,
		),
	)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			second,
			blockRaw,
			txs2,
			leiosStoreAuthoritative,
		),
	)

	db := o.leiosDatabase()
	require.NotNil(t, db)

	manifest1, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), manifest1)
	gotTxs1, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, txs1, gotTxs1)

	manifest2, err := db.GetLeiosEBManifest(second.Hash, second.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), manifest2)
	gotTxs2, err := db.GetLeiosEBTxs(second.Hash, second.Slot)
	require.NoError(t, err)
	require.Equal(t, txs2, gotTxs2)
}

// Not t.Parallel: swaps the package-global leiosPersistShutdownDrainTimeout.
func TestCloseReportsUnconfirmedLeiosPersistenceGCDrain(t *testing.T) {
	o := newTestOuroborosWithLeiosDB(t)

	originalTimeout := leiosPersistShutdownDrainTimeout
	leiosPersistShutdownDrainTimeout = 10 * time.Millisecond
	t.Cleanup(func() { leiosPersistShutdownDrainTimeout = originalTimeout })

	o.leiosPersistGCMu.Lock()
	o.leiosPersistGCStop = make(chan struct{})
	o.leiosPersistGCDone = make(chan struct{})
	o.leiosPersistGCMu.Unlock()
	o.leiosPersistGCStarted.Store(true)

	require.ErrorIs(t, o.Close(), ErrLeiosPersistDrainUnconfirmed)
	close(o.leiosPersistGCDone)
	require.NoError(t, o.Close())
}

func TestLeiosPersistenceLifecycleOperationsSerialize(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.leiosPersistLifecycleMu.Lock()
	locked := true
	defer func() {
		if locked {
			o.leiosPersistLifecycleMu.Unlock()
		}
	}()

	start := make(chan struct{})
	closeReady := make(chan struct{})
	pauseReady := make(chan struct{})
	closeDone := make(chan error, 1)
	pauseDone := make(chan error, 1)
	go func() {
		close(closeReady)
		<-start
		closeDone <- o.Close()
	}()
	go func() {
		close(pauseReady)
		<-start
		pauseDone <- o.PauseLeiosPersistWriterForLiveLifecycleOp()
	}()
	<-closeReady
	<-pauseReady
	close(start)
	runtime.Gosched()

	select {
	case err := <-closeDone:
		t.Fatalf("Close bypassed the persistence lifecycle lock: %v", err)
	case err := <-pauseDone:
		t.Fatalf("Pause bypassed the persistence lifecycle lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	o.leiosPersistLifecycleMu.Unlock()
	locked = false
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Close did not finish after the persistence lifecycle lock was released",
		)
	}
	select {
	case err := <-pauseDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Pause did not finish after the persistence lifecycle lock was released",
		)
	}
}

func TestLeiosPersistenceConstructorStartsGC(t *testing.T) {
	t.Parallel()
	first := newTestOuroborosWithLeiosDB(t)
	db := first.leiosDatabase()
	old, oldRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 1)
	latest, latestRaw := testLeiosEndorserBlockRawWithRefs(t, 30, 1)
	require.NoError(t, db.SetLeiosEB(old.Slot, old.Hash, oldRaw, nil))
	require.NoError(t, db.SetLeiosEB(latest.Slot, latest.Hash, latestRaw, nil))
	second := newOuroboros(
		OuroborosConfig{
			EnableLeios:                    true,
			LedgerState:                    first.ledgerState,
			LeiosAnnouncementLedger:        first.ledgerState,
			LeiosPersistenceRetentionSlots: 10,
		},
	)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.True(t, second.leiosPersistGCStarted.Load())
	require.Eventually(t, func() bool {
		_, err := db.GetLeiosEBManifest(old.Hash, old.Slot)
		return errors.Is(err, databaseTypes.ErrBlobKeyNotFound)
	}, time.Second, time.Millisecond)
	_, err := db.GetLeiosEBManifest(latest.Hash, latest.Slot)
	require.NoError(t, err)
}

const leiosPersistTestEventType event.EventType = "test.leios.persist"

// A handler that registers a write after Close has begun must be refused, not
// block on the lifecycle lock Close would otherwise hold while waiting for it.
func TestCloseDoesNotDeadlockOnHandlerRegisteringWrite(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	bus := newWiringTestEventBus(t)
	o.eventBus = bus
	point, raw, data := leiosPersistTestEntry(t, 60, 1, 4)
	entered := make(chan struct{})
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	writeErr := make(chan error, 1)
	o.subscribeTracked(leiosPersistTestEventType, func(event.Event) {
		close(entered)
		<-gate
		_, err := o.persistLeiosEndorserBlock(point, raw, data)
		writeErr <- err
	})
	bus.Publish(
		leiosPersistTestEventType,
		event.NewEvent(leiosPersistTestEventType, nil),
	)
	testutil.RequireReceive(t, entered, 5*time.Second, "handler did not start")

	closeDone := make(chan error, 1)
	go func() { closeDone <- o.Close() }()
	// Close refuses writes before it waits for handlers, so once the flag is
	// set it is waiting on the handler.
	testutil.WaitForCondition(t, func() bool {
		o.leiosPersistMu.Lock()
		defer o.leiosPersistMu.Unlock()
		return o.leiosPersistClosed
	}, 5*time.Second, "Close did not start")
	release()

	require.NoError(t, testutil.RequireReceive(
		t, closeDone, 5*time.Second, "Close deadlocked on an in-flight handler",
	))
	require.ErrorIs(t, testutil.RequireReceive(
		t, writeErr, 5*time.Second, "handler did not finish",
	), errLeiosPersistStopped)
}

// A handler stuck in a store write must not hang Close past the drain bound.
// Not t.Parallel: swaps the package-level leiosPersistShutdownDrainTimeout.
func TestCloseBoundsHandlerStuckInPersistWrite(t *testing.T) {
	origTimeout := leiosPersistShutdownDrainTimeout
	leiosPersistShutdownDrainTimeout = 50 * time.Millisecond
	t.Cleanup(func() { leiosPersistShutdownDrainTimeout = origTimeout })

	o := newTestOuroborosWithLeiosDB(t)
	bus := newWiringTestEventBus(t)
	o.eventBus = bus
	entered, release := blockLeiosStoreWrites(t, o.leiosDatabase())
	point, raw, data := leiosPersistTestEntry(t, 61, 1, 4)
	handlerDone := make(chan struct{})
	o.subscribeTracked(leiosPersistTestEventType, func(event.Event) {
		defer close(handlerDone)
		_, _ = o.persistLeiosEndorserBlock(point, raw, data)
	})
	bus.Publish(
		leiosPersistTestEventType,
		event.NewEvent(leiosPersistTestEventType, nil),
	)
	testutil.RequireReceive(t, entered, 5*time.Second, "write did not start")

	closeDone := make(chan error, 1)
	go func() { closeDone <- o.Close() }()
	require.ErrorIs(t, testutil.RequireReceive(
		t, closeDone, 5*time.Second, "Close hung on a stuck handler",
	), ErrLeiosPersistDrainUnconfirmed)
	release()
	testutil.RequireReceive(
		t, handlerDone, 5*time.Second, "handler did not finish",
	)
}
