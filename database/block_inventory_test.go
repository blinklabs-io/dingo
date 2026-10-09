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

package database

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

type noInventoryIteratorBlobStore struct {
	blob.BlobStore
}

type noWriteConflictBlobStore struct {
	blob.BlobStore
}

type observedTransactionBlobStore struct {
	blob.BlobStore
	opened chan struct{}
}

type doneObservedContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

type txnConstructionResult struct {
	txn *Txn
	err error
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func (s observedTransactionBlobStore) NewTransaction(
	readWrite bool,
) types.Txn {
	s.opened <- struct{}{}
	return s.BlobStore.NewTransaction(readWrite)
}

type inventoryIteratorCounts struct {
	items atomic.Uint64
	nexts atomic.Uint64
}

type countingInventoryBlobStore struct {
	blob.BlobStore
	counts *inventoryIteratorCounts
}

func (s countingInventoryBlobStore) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	return &countingInventoryIterator{
		BlobIterator: s.BlobStore.NewIterator(txn, opts),
		counts:       s.counts,
	}
}

type countingInventoryIterator struct {
	types.BlobIterator
	counts *inventoryIteratorCounts
}

func (it *countingInventoryIterator) Item() types.BlobItem {
	it.counts.items.Add(1)
	return it.BlobIterator.Item()
}

func (it *countingInventoryIterator) Next() {
	it.counts.nexts.Add(1)
	it.BlobIterator.Next()
}

func (noInventoryIteratorBlobStore) NewIterator(
	types.Txn,
	types.BlobIteratorOptions,
) types.BlobIterator {
	panic("block inventory lookup must not create an iterator")
}

func TestBlockInventoryTracksCommittedMutations(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	first := models.Block{
		ID: 1, Slot: 100, Hash: randomHash(t), Cbor: []byte{0x80},
	}
	second := models.Block{
		ID: 2, Slot: 200, Hash: randomHash(t), Cbor: []byte{0x80},
	}
	require.NoError(t, db.BlockCreate(first, nil))
	require.NoError(t, db.BlockCreate(second, nil))

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	require.Equal(t, uint64(100), oldest)

	txn := db.BlobTxn(true)
	require.NoError(t, BlockDeleteTxn(txn, first))
	require.NoError(t, txn.Commit())
	count, oldest, err = db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.Equal(t, uint64(200), oldest)

	txn = db.BlobTxn(true)
	require.NoError(t, db.tombstoneBlockTxn(txn, second.Slot, second.Hash))
	require.NoError(t, txn.Commit())
	count, oldest, err = db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, oldest)
}

func TestBlockInventoryTracksSyntheticBlockContent(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	real := models.Block{
		ID: 1, Slot: 100, Hash: randomHash(t), Cbor: []byte{0x80},
	}
	require.NoError(t, db.BlockCreate(real, nil))
	require.NoError(t, db.SetGenesisCbor(
		50, randomHash(t), []byte{0x80}, nil,
	))

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	require.Equal(t, uint64(50), oldest)
}

func TestBlockInventoryRollsBackWithBlockWrite(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	require.NoError(t, db.BlockCreate(models.Block{
		ID: 1, Slot: 100, Hash: randomHash(t), Cbor: []byte{0x80},
	}, txn))
	require.NoError(t, txn.Rollback())

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, oldest)
}

func TestBlockInventoryInitializesLegacyStoreBeforeQueries(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	firstHash := randomHash(t)
	secondHash := randomHash(t)
	insertTestBlock(t, db, 100, firstHash, []byte{0x80})
	insertTestBlock(t, db, 200, secondHash, []byte{0x80})

	txn := db.BlobTxn(true)
	require.NoError(t, txn.BlobStore().Delete(txn.Blob(), blockInventoryKey))
	require.NoError(t, txn.Commit())
	require.NoError(t, db.initBlockInventory())

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	require.Equal(t, uint64(100), oldest)

	txn = db.BlockBlobTxn()
	require.NoError(t, BlockDeleteTxn(txn, models.Block{
		ID: 1, Slot: 100, Hash: firstHash,
	}))
	require.NoError(t, txn.Commit())
	count, oldest, err = db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.Equal(t, uint64(200), oldest)
}

func TestBlockInventoryLookupDoesNotScanBlocks(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	insertTestBlock(t, db, 100, randomHash(t), []byte{0x80})
	db.SetBlobStore(noInventoryIteratorBlobStore{BlobStore: db.Blob()})

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.Equal(t, uint64(100), oldest)
}

func TestBlockInventoryRejectsCorruption(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	require.NoError(t, txn.BlobStore().Set(
		txn.Blob(), blockInventoryKey, []byte("corrupt"),
	))
	require.NoError(t, txn.Commit())

	_, _, err := db.CountBlocksAndOldestSlot(nil)
	require.ErrorContains(t, err, "invalid block inventory length")
}

func TestBlockInventorySerializesConcurrentUpdates(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	const blocks = 32
	var wg sync.WaitGroup
	errs := make(chan error, blocks)
	for i := range blocks {
		wg.Go(func() {
			hash := make([]byte, 32)
			hash[0] = byte(i + 1)
			errs <- db.BlockCreate(models.Block{
				ID:   uint64(i + 1),
				Slot: uint64(i + 1),
				Hash: hash,
				Cbor: []byte{0x80},
			}, nil)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(blocks), count)
	require.Equal(t, uint64(1), oldest)
}

func TestBlockInventoryCallerTransactionDoesNotWaitBehindMutation(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	db.SetBlobStore(noWriteConflictBlobStore{BlobStore: db.Blob()})
	holder := db.BlockBlobTxn()
	defer holder.Rollback() //nolint:errcheck

	callerTxn := db.BlobTxn(true)
	defer callerTxn.Rollback() //nolint:errcheck
	err := db.BlockCreate(models.Block{
		ID: 1, Slot: 1, Hash: randomHash(t), Cbor: []byte{0x80},
	}, callerTxn)
	require.ErrorContains(t, err, "block inventory mutation is busy")
}

func TestBlockInventoryCallerDoesNotBypassGateOnBadger(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	holder := db.BlockBlobTxn()
	defer holder.Rollback() //nolint:errcheck

	callerTxn := db.BlobTxn(true)
	defer callerTxn.Rollback() //nolint:errcheck
	err := db.BlockCreate(models.Block{
		ID: 1, Slot: 1, Hash: randomHash(t), Cbor: []byte{0x80},
	}, callerTxn)
	require.ErrorContains(t, err, "block inventory mutation is busy")
}

func TestBlockBatchTxnSerializesInventoryOnBadger(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	first := db.BlockBatchTxn()
	defer first.Rollback() //nolint:errcheck
	require.NoError(t, db.BlockCreate(models.Block{
		ID: 1, Slot: 1, Hash: randomHash(t), Cbor: []byte{0x80},
	}, first))

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		second := db.BlockBatchTxn()
		defer second.Rollback() //nolint:errcheck
		if err := db.BlockCreate(models.Block{
			ID: 2, Slot: 2, Hash: randomHash(t), Cbor: []byte{0x80},
		}, second); err != nil {
			secondDone <- err
			return
		}
		secondDone <- second.Commit()
	}()
	<-secondStarted
	select {
	case err := <-secondDone:
		t.Fatalf("second batch completed before first: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, first.Commit())
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second Badger inventory batch did not complete")
	}

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	require.Equal(t, uint64(1), oldest)
}

func TestBlockBatchTransactionSerializesInventoryOnBadger(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	first := db.BlockBatchTransaction(t.Context())
	defer first.Rollback() //nolint:errcheck
	require.NoError(t, db.BlockCreate(models.Block{
		ID: 1, Slot: 1, Hash: randomHash(t), Cbor: []byte{0x80},
	}, first))

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		second := db.BlockBatchTransaction(t.Context())
		defer second.Rollback() //nolint:errcheck
		if err := db.BlockCreate(models.Block{
			ID: 2, Slot: 2, Hash: randomHash(t), Cbor: []byte{0x80},
		}, second); err != nil {
			secondDone <- err
			return
		}
		secondDone <- second.Commit()
	}()
	<-secondStarted
	select {
	case err := <-secondDone:
		t.Fatalf("second coordinated batch completed before first: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, first.Commit())
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second coordinated Badger batch did not complete")
	}

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	require.Equal(t, uint64(1), oldest)
}

func TestBlockBatchTxnSerializesStoreWithoutConflictDetection(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	opened := make(chan struct{}, 2)
	db.SetBlobStore(observedTransactionBlobStore{
		BlobStore: db.Blob(),
		opened:    opened,
	})
	first := db.BlockBatchTxn()
	defer first.Rollback() //nolint:errcheck
	<-opened

	secondDone := make(chan *Txn, 1)
	go func() {
		secondDone <- db.BlockBatchTxn()
	}()
	select {
	case <-opened:
		t.Fatal("fallback store opened an overlapping block transaction")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, first.Rollback())
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("serialized block transaction did not open after release")
	}
	select {
	case second := <-secondDone:
		require.NoError(t, second.Rollback())
	case <-time.After(5 * time.Second):
		t.Fatal("serialized block transaction construction did not return")
	}
}

func TestBlockBatchTransactionSerializesStoreWithoutConflictDetection(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	opened := make(chan struct{}, 2)
	db.SetBlobStore(observedTransactionBlobStore{
		BlobStore: db.Blob(),
		opened:    opened,
	})
	first := db.BlockBatchTransaction(t.Context())
	defer first.Rollback() //nolint:errcheck
	<-opened

	secondDone := make(chan *Txn, 1)
	go func() {
		secondDone <- db.BlockBatchTransaction(t.Context())
	}()
	select {
	case <-opened:
		t.Fatal("fallback store opened an overlapping coordinated transaction")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, first.Rollback())
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("serialized coordinated transaction did not open after release")
	}
	select {
	case second := <-secondDone:
		require.NoError(t, second.Rollback())
	case <-time.After(5 * time.Second):
		t.Fatal("serialized coordinated transaction construction did not return")
	}
}

func TestBlockBatchTransactionContextCancelsInventoryAdmission(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	opened := make(chan struct{}, 2)
	db.SetBlobStore(observedTransactionBlobStore{
		BlobStore: db.Blob(),
		opened:    opened,
	})
	first, err := db.BlockBatchTransactionContext(t.Context())
	require.NoError(t, err)
	defer first.Rollback() //nolint:errcheck
	testutil.RequireReceive(
		t,
		opened,
		time.Second,
		"first transaction must open its blob handle",
	)

	baseCtx, cancel := context.WithCancel(t.Context())
	ctx := &doneObservedContext{
		Context: baseCtx,
		entered: make(chan struct{}),
	}
	resultCh := make(chan txnConstructionResult, 1)
	go func() {
		txn, err := db.BlockBatchTransactionContext(ctx)
		resultCh <- txnConstructionResult{txn: txn, err: err}
	}()
	testutil.RequireReceive(
		t,
		ctx.entered,
		time.Second,
		"transaction construction must enter the inventory gate wait",
	)
	cancel()
	got := testutil.RequireReceive(
		t,
		resultCh,
		time.Second,
		"cancelled transaction construction must return",
	)
	require.Nil(t, got.txn)
	require.ErrorIs(t, got.err, context.Canceled)
	require.ErrorContains(t, got.err, "acquire block inventory mutation gate")
	select {
	case <-opened:
		t.Fatal("cancelled construction opened a backend transaction")
	default:
	}
	require.NoError(t, first.Rollback())

	pauseCtx, pauseCancel := context.WithTimeout(t.Context(), time.Second)
	defer pauseCancel()
	resume, err := db.PauseCommitsContext(pauseCtx)
	require.NoError(t, err, "cancelled waiter must release the commit barrier")
	resume()

	third, err := db.BlockBatchTxnContext(t.Context())
	require.NoError(t, err, "cancelled waiter must not retain the inventory gate")
	t.Cleanup(third.Release)
	require.NoError(t, third.Rollback())
}

func TestBlockBatchTxnContextCancelsInventoryAdmission(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	first, err := db.BlockBatchTxnContext(t.Context())
	require.NoError(t, err)
	t.Cleanup(first.Release)

	baseCtx, cancel := context.WithCancel(t.Context())
	ctx := &doneObservedContext{
		Context: baseCtx,
		entered: make(chan struct{}),
	}
	resultCh := make(chan txnConstructionResult, 1)
	go func() {
		txn, err := db.BlockBatchTxnContext(ctx)
		resultCh <- txnConstructionResult{txn: txn, err: err}
	}()
	testutil.RequireReceive(
		t,
		ctx.entered,
		time.Second,
		"blob transaction construction must enter the inventory gate wait",
	)
	cancel()
	got := testutil.RequireReceive(
		t,
		resultCh,
		time.Second,
		"cancelled blob transaction construction must return",
	)
	require.Nil(t, got.txn)
	require.ErrorIs(t, got.err, context.Canceled)
	require.ErrorContains(t, got.err, "acquire block inventory mutation gate")
	require.NoError(t, first.Rollback())

	next, err := db.BlockBatchTxnContext(t.Context())
	require.NoError(t, err, "cancelled waiter must not retain the inventory gate")
	t.Cleanup(next.Release)
	require.NoError(t, next.Rollback())
}

func TestBlockBatchTransactionIncludesSyntheticBlockInventory(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	outer := db.BlockBatchTransaction(t.Context())
	defer outer.Rollback() //nolint:errcheck
	hash := randomHash(t)

	require.NoError(t, db.SetGenesisCbor(1, hash, []byte{0x80}, outer))
	require.NoError(t, outer.Commit())

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.Equal(t, uint64(1), oldest)
}

func TestBlockBatchTransactionComposesWithGenericWriterAndPause(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	holder := db.BlockBlobTxn()
	defer holder.Rollback() //nolint:errcheck

	ctx, cancel := context.WithCancel(t.Context())
	generic := db.TransactionContext(ctx, true)
	defer generic.Rollback() //nolint:errcheck

	batchReady := make(chan *Txn, 1)
	go func() { batchReady <- db.BlockBatchTransaction(t.Context()) }()
	require.Eventually(t, func() bool {
		db.commitBarrier.mu.Lock()
		defer db.commitBarrier.mu.Unlock()
		return db.commitBarrier.readers == 2
	}, 5*time.Second, time.Millisecond)

	paused := make(chan func(), 1)
	go func() { paused <- db.PauseCommits() }()
	require.Eventually(t, func() bool {
		db.commitBarrier.mu.Lock()
		defer db.commitBarrier.mu.Unlock()
		return db.commitBarrier.writerWaiting
	}, 5*time.Second, time.Millisecond)

	cancel()
	err := generic.Do(func(txn *Txn) error {
		return db.BlockCreate(models.Block{
			ID: 1, Slot: 1, Hash: randomHash(t), Cbor: []byte{0x80},
		}, txn)
	})
	require.ErrorContains(t, err, "block inventory mutation is busy")

	require.NoError(t, holder.Rollback())
	var batch *Txn
	select {
	case batch = <-batchReady:
	case <-time.After(5 * time.Second):
		t.Fatal("block batch remained blocked after inventory release")
	}
	require.NoError(t, db.BlockCreate(models.Block{
		ID: 2, Slot: 2, Hash: randomHash(t), Cbor: []byte{0x80},
	}, batch))
	require.NoError(t, batch.Commit())

	select {
	case resume := <-paused:
		resume()
	case <-time.After(5 * time.Second):
		t.Fatal("PauseCommits remained blocked after both writers finished")
	}
	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.Equal(t, uint64(2), oldest)
}

func TestBlockInventoryBoundsSequentialOldestRemovalWork(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	const blockCount = 256
	blocks := make([]models.Block, 0, blockCount)
	for i := range blockCount {
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash, uint64(i+1))
		block := models.Block{
			ID:   uint64(i + 1),
			Slot: uint64(i/2 + 1),
			Hash: hash,
			Cbor: []byte{0x80},
		}
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	counts := &inventoryIteratorCounts{}
	db.SetBlobStore(countingInventoryBlobStore{
		BlobStore: db.Blob(),
		counts:    counts,
	})
	for i, block := range blocks {
		txn := db.BlockBlobTxn()
		if i%2 == 0 {
			require.NoError(t, db.tombstoneBlockTxn(txn, block.Slot, block.Hash))
		} else {
			require.NoError(t, BlockDeleteTxn(txn, block))
		}
		require.NoError(t, txn.Commit())

		count, oldest, err := db.CountBlocksAndOldestSlot(nil)
		require.NoError(t, err)
		require.Equal(t, uint64(blockCount-i-1), count)
		if i+1 == blockCount {
			require.Zero(t, oldest)
		} else {
			require.Equal(t, blocks[i+1].Slot, oldest)
		}
	}

	// Each non-final removal reads one successor key. The work does not grow
	// with the number of older tombstones or remaining retained blocks.
	require.Equal(t, uint64(blockCount-1), counts.items.Load())
	require.Zero(t, counts.nexts.Load())
}
