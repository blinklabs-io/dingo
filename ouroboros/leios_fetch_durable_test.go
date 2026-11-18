// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// oneTxLeiosOfferRequester serves a one-transaction endorser block.
type oneTxLeiosOfferRequester struct {
	blockRaw []byte
	tx       cbor.RawMessage
}

func (r *oneTxLeiosOfferRequester) BlockRequest(
	context.Context,
	ocommon.Point,
) (protocol.Message, error) {
	return &leiosfetch.MsgBlock{BlockRaw: r.blockRaw}, nil
}

func (r *oneTxLeiosOfferRequester) BlockTxsRequest(
	context.Context,
	ocommon.Point,
	map[uint16]uint64,
) (protocol.Message, error) {
	return leiosfetch.NewMsgBlockTxs([]cbor.RawMessage{r.tx}), nil
}

// A completed peer fetch must have its manifest and transactions in the
// Leios store when it reports success, even when the asynchronous persistence
// queue cannot accept the write.
func TestFetchLeiosTxOfferReturnsOnlyAfterDurableWrite(t *testing.T) {
	t.Parallel()

	for name, disable := range map[string]func(*Ouroboros){
		"queue full": func(o *Ouroboros) {
			o.leiosPersistMu.Lock()
			defer o.leiosPersistMu.Unlock()
			for i := range leiosPersistMaxPending {
				o.leiosPersistPending[fmt.Sprintf("filler-%d", i)] =
					&leiosPersistJob{}
			}
		},
		"writer stopped": func(o *Ouroboros) {
			close(o.leiosPersistStop)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := newTestOuroborosWithPausedLeiosPersistWriter(t)
			disable(o)
			// Without a ledger the announcement binding never ages out, so the
			// low test slot stays a live election.
			o.leiosAnnouncementLedger = nil

			tx, ref := testLeiosManifestTx(t, 7)
			manifestRaw, err := lcommon.LeiosEndorserBlock{
				TransactionReferences: []lcommon.LeiosTransactionReference{
					ref,
				},
			}.MarshalCBOR()
			require.NoError(t, err)
			point := ocommon.NewPoint(
				300,
				lcommon.Blake2b256Hash(manifestRaw).Bytes(),
			)
			announceLeiosElectionForTest(
				t, o, point, uint64(len(manifestRaw)),
			)
			client := &oneTxLeiosOfferRequester{
				blockRaw: manifestRaw,
				tx:       tx,
			}

			fetched := o.fetchLeiosTxOffer(
				context.Background(), client, "peer", point,
			)

			db := o.leiosDatabase()
			require.NotNil(t, db)
			manifest, _ := db.GetLeiosEBManifest(point.Hash, point.Slot)
			txs, _ := db.GetLeiosEBTxs(point.Hash, point.Slot)
			durable := string(manifest) == string(manifestRaw) &&
				len(txs) == 1
			require.Equal(
				t,
				durable,
				fetched,
				"fetch success must coincide with the endorser block being in the Leios store",
			)
			require.True(t, fetched, "the fetch itself must succeed")
		})
	}
}

// failingPutLeiosStore fails every volatile write that carries transactions
// while failing is set, so a manifest write succeeds and the write completing
// the endorser block fails.
type failingPutLeiosStore struct {
	leiosdb.Store
	failing atomic.Bool
}

func (s *failingPutLeiosStore) PutVolatile(
	eb leiosdb.Ref,
	content leiosdb.Content,
) error {
	if s.failing.Load() && content.Txs != nil {
		return errors.New("injected leios store write failure")
	}
	return s.Store.PutVolatile(eb, content)
}

// A fetch whose store write fails must report failure and leave no complete
// cache entry behind, so the next offer fetches and writes the block again
// instead of finding it cached and reporting success without a durable copy.
func TestFetchLeiosTxOfferFailsWhenDurableWriteFails(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	o.leiosAnnouncementLedger = nil
	db := o.leiosDatabase()
	require.NotNil(t, db)
	base, err := db.LeiosDB()
	require.NoError(t, err)
	store := &failingPutLeiosStore{Store: base}
	store.failing.Store(true)
	db.SetLeiosDB(store)
	t.Cleanup(func() { db.SetLeiosDB(base) })

	tx, ref := testLeiosManifestTx(t, 9)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		301,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	announceLeiosElectionForTest(t, o, point, uint64(len(manifestRaw)))
	client := &oneTxLeiosOfferRequester{blockRaw: manifestRaw, tx: tx}

	require.False(
		t,
		o.fetchLeiosTxOffer(context.Background(), client, "peer", point),
		"a fetch whose store write failed must not report success",
	)
	cached, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(
		t,
		ok && cached.completeTxCache(),
		"a failed write must not leave a complete cache entry",
	)

	store.failing.Store(false)
	require.True(
		t,
		o.fetchLeiosTxOffer(context.Background(), client, "peer", point),
		"the next offer must fetch again once the store accepts writes",
	)
	txs, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Len(t, txs, 1)
}

// blockingPutLeiosStore holds every volatile write that carries transactions
// until release is closed, reporting through entered that one has started.
type blockingPutLeiosStore struct {
	leiosdb.Store
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
}

func (s *blockingPutLeiosStore) PutVolatile(
	eb leiosdb.Ref,
	content leiosdb.Content,
) error {
	if content.Txs != nil {
		s.enteredOnce.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.Store.PutVolatile(eb, content)
}

// A fetched endorser block must not be visible as complete in the cache while
// its durable write is still in progress: a concurrent reader would otherwise
// report the block fetched before the write had a chance to fail.
func TestStoreLeiosEndorserBlockHidesEntryUntilDurable(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	db := o.leiosDatabase()
	require.NotNil(t, db)
	base, err := db.LeiosDB()
	require.NoError(t, err)
	store := &blockingPutLeiosStore{
		Store:   base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	db.SetLeiosDB(store)
	t.Cleanup(func() { db.SetLeiosDB(base) })

	tx, ref := testLeiosManifestTx(t, 11)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		302,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)

	stored := make(chan error, 1)
	go func() {
		stored <- o.storeLeiosEndorserBlock(
			point,
			manifestRaw,
			[]cbor.RawMessage{tx},
			leiosStoreBackfill,
		)
	}()
	<-store.entered
	cached, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	visible := ok && cached.completeTxCache()
	close(store.release)
	require.NoError(t, <-stored)
	require.False(
		t,
		visible,
		"the entry must not be complete in the cache before its write returns",
	)
	cached, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok && cached.completeTxCache())
}

// A synchronous fetch write after a live lifecycle pause must restart the
// retention GC the pause stopped, as an asynchronous enqueue does; otherwise
// records written by fetches are never collected.
func TestStoreLeiosEndorserBlockRestartsGCAfterPause(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.startLeiosPersistenceGC(0, false)
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())
	require.False(t, o.leiosPersistGCStarted.Load())

	tx, ref := testLeiosManifestTx(t, 12)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		303,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		manifestRaw,
		[]cbor.RawMessage{tx},
		leiosStoreBackfill,
	))
	require.True(
		t,
		o.leiosPersistGCStarted.Load(),
		"a synchronous fetch write must restart the paused retention GC",
	)
}

// After close or a writer stop, a synchronous fetch write is refused and the
// fetch fails rather than reporting a block it did not persist.
func TestStoreLeiosEndorserBlockFailsAfterClose(t *testing.T) {
	t.Parallel()

	for name, stop := range map[string]func(*Ouroboros){
		"close": func(o *Ouroboros) { require.NoError(t, o.Close()) },
		"stop writer": func(o *Ouroboros) {
			o.StopLeiosPersistWriter()
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := newTestOuroborosWithLeiosDB(t)
			stop(o)

			tx, ref := testLeiosManifestTx(t, 13)
			manifestRaw, err := lcommon.LeiosEndorserBlock{
				TransactionReferences: []lcommon.LeiosTransactionReference{
					ref,
				},
			}.MarshalCBOR()
			require.NoError(t, err)
			point := ocommon.NewPoint(
				304,
				lcommon.Blake2b256Hash(manifestRaw).Bytes(),
			)
			err = o.storeLeiosEndorserBlock(
				point,
				manifestRaw,
				[]cbor.RawMessage{tx},
				leiosStoreBackfill,
			)
			require.ErrorIs(t, err, errLeiosPersistClosed)
			_, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
			require.False(
				t,
				ok,
				"a refused write must not leave a cache entry",
			)
		})
	}
}

// Not t.Parallel: swaps the package-global leiosPersistShutdownDrainTimeout.
//
// A synchronous fetch write stuck in a blob store must not hang a writer stop,
// a live lifecycle pause, or close: each waits for it within the shutdown
// drain bound, and the pause and close report the write unconfirmed so
// storage is not replaced or closed under it.
func TestLeiosPersistStopIsBoundedByStuckSyncWrite(t *testing.T) {
	originalTimeout := leiosPersistShutdownDrainTimeout
	leiosPersistShutdownDrainTimeout = 20 * time.Millisecond
	t.Cleanup(func() { leiosPersistShutdownDrainTimeout = originalTimeout })

	for name, stop := range map[string]func(*Ouroboros) error{
		"stop writer": func(o *Ouroboros) error {
			o.StopLeiosPersistWriter()
			return nil
		},
		"pause": func(o *Ouroboros) error {
			return o.PauseLeiosPersistWriterForLiveLifecycleOp()
		},
		"close": func(o *Ouroboros) error {
			return o.Close()
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := newTestOuroborosWithLeiosDB(t)
			db := o.leiosDatabase()
			require.NotNil(t, db)
			base, err := db.LeiosDB()
			require.NoError(t, err)
			store := &blockingPutLeiosStore{
				Store:   base,
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}
			db.SetLeiosDB(store)

			tx, ref := testLeiosManifestTx(t, 14)
			manifestRaw, err := lcommon.LeiosEndorserBlock{
				TransactionReferences: []lcommon.LeiosTransactionReference{
					ref,
				},
			}.MarshalCBOR()
			require.NoError(t, err)
			point := ocommon.NewPoint(
				305,
				lcommon.Blake2b256Hash(manifestRaw).Bytes(),
			)
			stored := make(chan error, 1)
			go func() {
				stored <- o.storeLeiosEndorserBlock(
					point,
					manifestRaw,
					[]cbor.RawMessage{tx},
					leiosStoreBackfill,
				)
			}()
			testutil.RequireReceive(
				t, store.entered, 2*time.Second, "sync write started",
			)

			stopped := make(chan error, 1)
			go func() { stopped <- stop(o) }()
			stopErr, ok := receiveWithin(stopped, 2*time.Second)
			// Unblock before asserting so every goroutine exits either way.
			close(store.release)
			require.NoError(t, <-stored)
			if !ok {
				stopErr = <-stopped
			}
			db.SetLeiosDB(base)
			require.True(
				t,
				ok,
				"a stuck synchronous write must not hang the stop",
			)
			if name != "stop writer" {
				require.ErrorIs(t, stopErr, ErrLeiosPersistDrainUnconfirmed)
			}
		})
	}
}

// receiveWithin reports the value received from ch within timeout, and false
// when nothing arrived.
func receiveWithin[T any](ch <-chan T, timeout time.Duration) (T, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v, true
	case <-timer.C:
		var zero T
		return zero, false
	}
}
