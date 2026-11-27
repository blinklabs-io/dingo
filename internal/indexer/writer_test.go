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

package indexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// fakeChain is an in-memory canonical chain plus abandoned blocks.
type fakeChain struct {
	mu        sync.Mutex
	canonical []models.Block
	stored    map[string]models.Block
	// staleOnce, when set, is returned by the next BlocksAfter call instead
	// of the canonical blocks, as when the chain switches between the
	// writer reading its tip and reading blocks.
	staleOnce []models.Block
	forget    bool
}

func mkBlock(number uint64, prev []byte, tag string) models.Block {
	h := []byte(fmt.Sprintf("%s-%04d", tag, number))
	return models.Block{
		Number: number, Slot: number * 10, Hash: h, PrevHash: prev,
	}
}

func newFakeChain(n int, tag string) *fakeChain {
	c := &fakeChain{stored: map[string]models.Block{}}
	c.extend(0, n, tag)
	return c
}

// forgetAbandoned makes later extend calls drop the blocks they replace from
// storage, as a ledger rollback deletes the bodies of the blocks it abandons.
func (c *fakeChain) forgetAbandoned() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forget = true
}

// extend replaces the canonical chain above number-1 with n new blocks.
func (c *fakeChain) extend(from, n int, tag string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.forget {
		for _, b := range c.canonical[from:] {
			delete(c.stored, string(b.Hash))
		}
	}
	c.canonical = c.canonical[:from]
	var prev []byte
	if from > 0 {
		prev = c.canonical[from-1].Hash
	}
	for i := from; i < from+n; i++ {
		b := mkBlock(uint64(i), prev, tag)
		c.canonical = append(c.canonical, b)
		c.stored[string(b.Hash)] = b
		prev = b.Hash
	}
}

func (c *fakeChain) LedgerTip(context.Context) (ochainsync.Tip, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.canonical) == 0 {
		return ochainsync.Tip{}, nil
	}
	b := c.canonical[len(c.canonical)-1]
	return ochainsync.Tip{
		Point: ocommon.Point{Slot: b.Slot, Hash: b.Hash}, BlockNumber: b.Number,
	}, nil
}

func (c *fakeChain) BlocksAfter(
	_ context.Context, after indexerdb.Tip, through uint64, limit int,
) ([]models.Block, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []models.Block
	started := after.IsZero()
	for _, b := range c.canonical {
		if !started {
			started = bytes.Equal(b.Hash, after.Hash)
			continue
		}
		if b.Slot <= through && len(out) < limit {
			out = append(out, b)
		}
	}
	if c.staleOnce != nil {
		out, c.staleOnce = c.staleOnce, nil
	}
	return out, nil
}

func (c *fakeChain) IsCanonical(_ context.Context, hash []byte) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.canonical {
		if bytes.Equal(b.Hash, hash) {
			return true, nil
		}
	}
	return false, nil
}

func (c *fakeChain) BlockByHash(
	_ context.Context,
	hash []byte,
) (models.Block, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.stored[string(hash)]
	if !ok {
		return models.Block{}, models.ErrBlockNotFound
	}
	return b, nil
}

// syncRowIndexer writes one real sync_state row per block, so batch and tip
// atomicity is observed in the SQL store.
type syncRowIndexer struct {
	mu           sync.Mutex
	failOnNumber map[uint64]bool
	indexed      []uint64
}

const rowPrefix = "test_idx_"

func (s *syncRowIndexer) IndexBlocks(
	_ context.Context, txn *database.Txn, blocks []models.Block,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db := txn.DB()
	for _, b := range blocks {
		if err := db.SetSyncState(
			rowPrefix+strconv.FormatUint(b.Number, 10),
			hex.EncodeToString(b.Hash), txn,
		); err != nil {
			return err
		}
		s.indexed = append(s.indexed, b.Number)
		if s.failOnNumber[b.Number] {
			return errors.New("injected failure after writing rows")
		}
	}
	return nil
}

func (s *syncRowIndexer) RollbackTo(
	_ context.Context, txn *database.Txn, point ocommon.Point,
) error {
	db := txn.DB()
	keys, err := db.ListSyncStateKeysByPrefix(rowPrefix, txn)
	if err != nil {
		return err
	}
	for _, k := range keys {
		n, err := strconv.ParseUint(k[len(rowPrefix):], 10, 64)
		if err != nil {
			return err
		}
		if n*10 > point.Slot || (point.Slot == 0 && len(point.Hash) == 0) {
			if err := db.DeleteSyncState(k, txn); err != nil {
				return err
			}
		}
	}
	return nil
}

func newTestStore(t *testing.T) *indexerdb.Store {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{Role: database.RoleIndexer})
	require.NoError(t, err)
	store, err := indexerdb.NewStore(db, db, 10)
	require.NoError(t, err)
	return store
}

func rows(t *testing.T, store *indexerdb.Store) map[uint64]string {
	t.Helper()
	db := store.Database()
	keys, err := db.ListSyncStateKeysByPrefix(rowPrefix, nil)
	require.NoError(t, err)
	out := map[uint64]string{}
	for _, k := range keys {
		n, err := strconv.ParseUint(k[len(rowPrefix):], 10, 64)
		require.NoError(t, err)
		v, err := db.GetSyncState(k, nil)
		require.NoError(t, err)
		out[n] = v
	}
	return out
}

func requireMatchesChain(t *testing.T, store *indexerdb.Store, c *fakeChain) {
	t.Helper()
	got := rows(t, store)
	require.Len(t, got, len(c.canonical))
	for _, b := range c.canonical {
		require.Equal(
			t,
			hex.EncodeToString(b.Hash),
			got[b.Number],
			"block %d",
			b.Number,
		)
	}
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	last := c.canonical[len(c.canonical)-1]
	require.Equal(t, last.Number, tip.BlockNumber)
	require.Equal(t, last.Hash, tip.Hash)
}

func TestCatchUpIndexesCanonicalChainInBatches(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	idx := &syncRowIndexer{}
	w := NewWriter(store, chain, idx, nil, 3)
	batches := 0
	w.afterBatch = func(indexerdb.Tip) { batches++ }
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain)
	require.Equal(t, 4, batches)
}

func TestFailedBatchLeavesNeitherRowsNorTip(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	idx := &syncRowIndexer{failOnNumber: map[uint64]bool{5: true}}
	w := NewWriter(store, chain, idx, nil, 3)
	require.Error(t, w.CatchUp(t.Context()))

	// Batch 1 (blocks 0-2) committed. Batch 2 wrote rows for 3-5 and then
	// failed: none of its rows or its tip may be visible.
	got := rows(t, store)
	require.Len(t, got, 3)
	require.NotContains(t, got, uint64(3))
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), tip.BlockNumber)

	// A new writer resumes at the tip without re-reading committed blocks.
	idx2 := &syncRowIndexer{}
	w2 := NewWriter(store, chain, idx2, nil, 3)
	require.NoError(t, w2.CatchUp(t.Context()))
	require.Equal(t, []uint64{3, 4, 5, 6, 7, 8, 9}, idx2.indexed)
	requireMatchesChain(t, store, chain)
}

func TestRollbackToIntersectionAndReplayFork(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	idx := &syncRowIndexer{}
	w := NewWriter(store, chain, idx, nil, 4)
	require.NoError(t, w.CatchUp(t.Context()))

	// The chain switches at block 6: blocks 6-9 are abandoned and a
	// longer fork replaces them.
	chain.extend(6, 6, "b")
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain)
}

func TestRollbackDeeperThanOneBatch(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 10)
	require.NoError(t, w.CatchUp(t.Context()))

	// Fork point is below the last 3 blocks; the replacement chain is
	// shorter than what was indexed.
	chain.extend(5, 2, "b")
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain)
}

func TestIndexerTipAtLedgerTipIsNoOp(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(5, "a")
	idx := &syncRowIndexer{}
	w := NewWriter(store, chain, idx, nil, 10)
	require.NoError(t, w.CatchUp(t.Context()))
	idx.indexed = nil
	require.NoError(t, w.CatchUp(t.Context()))
	require.Empty(t, idx.indexed)
}

func TestMidBatchForkIndexesNothingAcrossBrokenLink(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(6, "a")
	idx := &syncRowIndexer{}
	w := NewWriter(store, chain, idx, nil, 3)
	require.NoError(t, w.CatchUp(t.Context()))
	chain.extend(6, 4, "a")
	stale := mkBlock(6, []byte("abandoned-parent"), "z")
	chain.staleOnce = []models.Block{stale, mkBlock(7, stale.Hash, "z")}
	idx.indexed = nil
	require.NoError(t, w.CatchUp(t.Context()))
	// The stale read was discarded before any row was written: each canonical
	// block is indexed exactly once.
	require.Equal(t, []uint64{6, 7, 8, 9}, idx.indexed)
	requireMatchesChain(t, store, chain)
}

func TestLegacyBackfillCheckpointSeedsTip(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	cp := chain.canonical[4]
	require.NoError(t, store.Database().Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase: "metadata", LastSlot: cp.Slot, LastBlockHash: cp.Hash,
		}, nil,
	))
	idx := &syncRowIndexer{}
	w := NewWriter(store, chain, idx, nil, 100)
	require.NoError(t, w.CatchUp(t.Context()))
	require.Equal(t, []uint64{5, 6, 7, 8, 9}, idx.indexed)
}

func TestRunFollowsWake(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(3, "a")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 10)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	indexedTo := func(n uint64) func() bool {
		return func() bool {
			tip, err := store.Tip(nil)
			return err == nil && !tip.IsZero() && tip.BlockNumber == n
		}
	}
	testutil.WaitForCondition(
		t,
		indexedTo(2),
		30*time.Second,
		"initial catch-up",
	)
	chain.extend(3, 2, "a")
	w.Wake()
	testutil.WaitForCondition(t, indexedTo(4), 30*time.Second, "wake catch-up")
}

func TestWriterRollsBackThroughCheckpointsWhenAbandonedBlocksAreGone(
	t *testing.T,
) {
	t.Parallel()
	chain := newFakeChain(6, "a")
	store := newTestStore(t)
	idx := &syncRowIndexer{}
	// Batches of two put checkpoints at blocks 1, 3 and 5.
	w := NewWriter(store, chain, idx, nil, 2)
	require.NoError(t, w.CatchUp(context.Background()))

	chain.forgetAbandoned()
	chain.extend(3, 3, "b") // blocks 3..5 replaced and their bodies dropped
	require.NoError(t, w.CatchUp(context.Background()))

	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, "b-0005", string(tip.Hash))
	// The rollback went to checkpoint block 1, the newest still on the
	// chain, so blocks 2..5 were indexed again and 0..1 were not.
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 2, 3, 4, 5}, idx.indexed)
	got := rows(t, store)
	require.Len(t, got, 6)
	for n := uint64(0); n <= 2; n++ {
		require.Equal(
			t,
			hex.EncodeToString(fmt.Appendf(nil, "a-%04d", n)),
			got[n],
		)
	}
	for n := uint64(3); n <= 5; n++ {
		require.Equal(
			t,
			hex.EncodeToString(fmt.Appendf(nil, "b-%04d", n)),
			got[n],
			"block %d must come from the new chain",
			n,
		)
	}
	// The checkpoint on the abandoned fork went with the rollback.
	cps, err := store.Checkpoints(nil)
	require.NoError(t, err)
	for _, cp := range cps {
		require.NotEqual(t, "a-0003", string(cp.Hash))
		require.NotEqual(t, "a-0005", string(cp.Hash))
	}
}

func TestWriterRollsBackToNothingWhenNoCheckpointIsOnTheChain(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(4, "a")
	store := newTestStore(t)
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 2)
	require.NoError(t, w.CatchUp(context.Background()))

	chain.forgetAbandoned()
	chain.extend(0, 4, "b") // the whole chain replaced
	require.NoError(t, w.CatchUp(context.Background()))
	got := rows(t, store)
	require.Len(t, got, 4)
	for n, h := range got {
		require.Equal(t, hex.EncodeToString(fmt.Appendf(nil, "b-%04d", n)), h)
	}
}

// notFromGenesis is a chain that starts at an imported state rather than at
// genesis.
type notFromGenesis struct {
	*fakeChain
}

func (notFromGenesis) FromGenesis(context.Context) (bool, error) {
	return false, nil
}

func TestWriterStopsWhenNoRollbackAncestorExistsBelowAnImportedState(
	t *testing.T,
) {
	t.Parallel()
	chain := newFakeChain(4, "a")
	store := newTestStore(t)
	var lost atomic.Int32
	w := NewWriter(store, notFromGenesis{chain}, &syncRowIndexer{}, nil, 2).
		WithRollbackAncestorLost(func() { lost.Add(1) })
	require.NoError(t, w.CatchUp(t.Context()))

	chain.forgetAbandoned()
	chain.extend(0, 4, "b")
	err := w.CatchUp(t.Context())
	require.ErrorIs(t, err, ErrNoRollbackAncestor)
	require.EqualValues(t, 1, lost.Load(), "the lost ancestor is reported")
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.EqualValues(t, 3, tip.BlockNumber, "the IndexerDB is not wiped")
	require.Len(t, rows(t, store), 4)
}

func TestRunReportsRepeatedIdenticalFailures(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(3, "a")
	idx := &syncRowIndexer{failOnNumber: map[uint64]bool{1: true}}
	counts := make(chan int, 16)
	w := NewWriter(store, chain, idx, nil, 10).
		WithFailureReport(func(n int) { counts <- n })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	for want := 1; want <= 3; want++ {
		select {
		case got := <-counts:
			require.Equal(t, want, got)
		case <-time.After(30 * time.Second):
			t.Fatalf("no failure report %d", want)
		}
		w.Wake()
	}

	idx.mu.Lock()
	idx.failOnNumber = nil
	idx.mu.Unlock()
	w.Wake()
	for {
		select {
		case got := <-counts:
			if got == 0 {
				return
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the count never returned to zero after a success")
		}
		w.Wake()
	}
}

func TestRunRetriesFailureWithoutAnotherWake(t *testing.T) {
	t.Parallel()
	for _, recoverPass := range []bool{true, false} {
		t.Run(fmt.Sprintf("recover=%t", recoverPass), func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t)
			chain := newFakeChain(3, "retry")
			idx := &syncRowIndexer{failOnNumber: map[uint64]bool{1: true}}
			counts := make(chan int, 8)
			w := NewWriter(store, chain, idx, nil, 10).
				WithFailureReport(func(n int) {
					if recoverPass && n == 1 {
						idx.mu.Lock()
						idx.failOnNumber = nil
						idx.mu.Unlock()
					}
					counts <- n
				})
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			t.Cleanup(cancel)
			require.Equal(t, 1, testutil.RequireReceive(
				t, counts, 5*time.Second, "failed pass",
			))
			if recoverPass {
				require.Zero(t, testutil.RequireReceive(
					t, counts, 5*time.Second, "automatic retry without a wake",
				))
				require.Len(t, rows(t, store), 3)
			}
			cancel()
			require.ErrorIs(t, testutil.RequireReceive(
				t, done, 5*time.Second, "writer cancellation",
			), context.Canceled)
		})
	}
}

// sizeLimitedIndexer refuses a call for more than limit blocks the way the
// blob store refuses a transaction past its size, formatting the cause with
// %v as the storage layers do.
type sizeLimitedIndexer struct {
	syncRowIndexer
	limit int
	calls atomic.Int64
}

func (s *sizeLimitedIndexer) IndexBlocks(
	ctx context.Context, txn *database.Txn, blocks []models.Block,
) error {
	s.calls.Add(1)
	if len(blocks) > s.limit {
		return fmt.Errorf("set utxo offset: %v", badger.ErrTxnTooBig)
	}
	return s.syncRowIndexer.IndexBlocks(ctx, txn, blocks)
}

func TestCatchUpShrinksBatchWhenBlobWritesAreTooBig(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(40, "a")
	idx := &sizeLimitedIndexer{limit: 5}
	w := NewWriter(store, chain, idx, nil, 32)
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain)
}

func TestCatchUpFailsWhenOneBlockIsTooBig(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(4, "a")
	idx := &sizeLimitedIndexer{limit: 0}
	w := NewWriter(store, chain, idx, nil, 4)
	err := w.CatchUp(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, badger.ErrTxnTooBig.Error())
	// 4 blocks, 2, 1: then the single block cannot shrink further.
	require.Equal(t, int64(3), idx.calls.Load())
}

// limitRecordingChain records the block limit of every BlocksAfter call.
type limitRecordingChain struct {
	*fakeChain
	limits []int
}

func (c *limitRecordingChain) BlocksAfter(
	ctx context.Context, after indexerdb.Tip, through uint64, limit int,
) ([]models.Block, error) {
	c.limits = append(c.limits, limit)
	return c.fakeChain.BlocksAfter(ctx, after, through, limit)
}

// denseBelowIndexer refuses a call for more than limit blocks when it starts
// below block dense, as a dense region refuses an oversized transaction.
type denseBelowIndexer struct {
	syncRowIndexer
	limit int
	dense uint64
}

func (d *denseBelowIndexer) IndexBlocks(
	ctx context.Context, txn *database.Txn, blocks []models.Block,
) error {
	if blocks[0].Number < d.dense && len(blocks) > d.limit {
		return fmt.Errorf("set utxo offset: %w", badger.ErrTxnTooBig)
	}
	return d.syncRowIndexer.IndexBlocks(ctx, txn, blocks)
}

func TestCatchUpGrowsBatchBackAfterTheDenseRegion(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := &limitRecordingChain{fakeChain: newFakeChain(40, "a")}
	idx := &denseBelowIndexer{limit: 4, dense: 8}
	w := NewWriter(store, chain, idx, nil, 8)
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain.fakeChain)
	// Blocks 0..7 fail at 8 and commit at 4; the limit climbs back to 8 after
	// each commit, so the batches past the dense region are full again.
	require.Equal(t, []int{8, 4, 8, 4, 8, 8, 8, 8}, chain.limits)
}

// withContent gives every block of the chain a CBOR body of size bytes.
func (c *fakeChain) withContent(size int) *fakeChain {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.canonical {
		c.canonical[i].Cbor = make([]byte, size)
		c.stored[string(c.canonical[i].Hash)] = c.canonical[i]
	}
	return c
}

// catchUpBatches indexes the chain and returns the committed batch count.
func catchUpBatches(
	t *testing.T, chain *fakeChain, idx BlockIndexer, batch int, sparse bool,
) (int, *indexerdb.Store) {
	t.Helper()
	store := newTestStore(t)
	w := NewWriter(store, chain, idx, nil, batch)
	if sparse {
		w.WithSparseBatches()
		w.sparseBytes = 1000
	}
	batches := 0
	w.afterBatch = func(indexerdb.Tip) { batches++ }
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain)
	return batches, store
}

func TestSparseBatchesCommitFewerTimesForSmallBlocks(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(64, "a").withContent(10)
	plain, plainStore := catchUpBatches(t, chain, &syncRowIndexer{}, 4, false)
	sparse, sparseStore := catchUpBatches(t, chain, &syncRowIndexer{}, 4, true)
	require.Equal(t, 16, plain)
	// 4 blocks of 10 bytes stay under the budget, so each batch reads up to
	// SparseBatchFactor*4 = 32 blocks.
	require.Equal(t, 2, sparse)
	require.Equal(t, rows(t, plainStore), rows(t, sparseStore))
}

func TestSparseBatchesKeepBlockLimitForDenseBlocks(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(64, "a").withContent(1000)
	plain, _ := catchUpBatches(t, chain, &syncRowIndexer{}, 4, false)
	sparse, _ := catchUpBatches(t, chain, &syncRowIndexer{}, 4, true)
	require.Equal(t, plain, sparse)
}

func TestSparseBatchesStillHalveOnOversizedTransaction(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(200, "a").withContent(10)
	idx := &sizeLimitedIndexer{limit: 5}
	_, store := catchUpBatches(t, chain, idx, 8, true)
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(199), tip.BlockNumber)
}

// vanishingChain fails its second BlocksAfter call, as when the block a
// sparse batch continues from is rolled back between reads.
type vanishingChain struct {
	*fakeChain
	calls atomic.Int32
}

func (c *vanishingChain) BlocksAfter(
	ctx context.Context, after indexerdb.Tip, through uint64, limit int,
) ([]models.Block, error) {
	if c.calls.Add(1) == 2 {
		return nil, errors.New("locate block to continue after: not found")
	}
	return c.fakeChain.BlocksAfter(ctx, after, through, limit)
}

func TestSparseBatchesKeepReadPrefixWhenContinuationFails(t *testing.T) {
	t.Parallel()
	chain := &vanishingChain{fakeChain: newFakeChain(64, "a").withContent(10)}
	store := newTestStore(t)
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 4).WithSparseBatches()
	w.sparseBytes = 1000
	require.NoError(t, w.CatchUp(t.Context()))
	requireMatchesChain(t, store, chain.fakeChain)
}

// An empty IndexerDB over a block history that starts after genesis, as a
// Mithril restore that kept only recent blocks leaves, cannot be derived: the
// writer refuses it, and Run stops and reports it rather than retrying.
func TestWriterRefusesABlockHistoryThatDoesNotStartAtGenesis(t *testing.T) {
	t.Parallel()
	chain := newFakeChain(8, "a")
	chain.mu.Lock()
	chain.canonical = chain.canonical[3:]
	chain.mu.Unlock()
	store := newTestStore(t)
	fatal := make(chan error, 1)
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 2).
		WithFatal(func(err error) { fatal <- err })

	require.ErrorIs(t, w.CatchUp(t.Context()), ErrChainNotFromGenesis)
	require.Empty(t, rows(t, store), "nothing is indexed past the gap")

	done := make(chan error, 1)
	go func() { done <- w.Run(t.Context()) }()
	require.ErrorIs(t,
		testutil.RequireReceive(t, done, 5*time.Second, "Run must stop"),
		ErrChainNotFromGenesis)
	require.ErrorIs(t,
		testutil.RequireReceive(t, fatal, time.Second, "the stop is reported"),
		ErrChainNotFromGenesis)
}
