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

package lifecycle_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// cancelAfterNErrChecks is a context.Context whose Err() returns nil for
// the first n calls, then context.Canceled for every call after that —
// used to simulate a cancellation landing partway through a single batch,
// deterministically, without racing a real timer against real deletes.
type cancelAfterNErrChecks struct {
	context.Context
	n     int64
	count atomic.Int64
}

func (c *cancelAfterNErrChecks) Err() error {
	if c.count.Add(1) > c.n {
		return context.Canceled
	}
	return nil
}

func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return db
}

func testBlock(id uint64, hashByte byte) models.Block {
	return models.Block{
		ID:     id,
		Slot:   id * 10,
		Hash:   bytes.Repeat([]byte{hashByte}, 32),
		Cbor:   []byte{0x80},
		Number: id,
		Type:   1,
	}
}

// TestDeleteBlocksAfterRemovesOnlyBlocksAboveThreshold verifies that
// blocks at or below afterID survive and every block above it is deleted.
func TestDeleteBlocksAfterRemovesOnlyBlocksAboveThreshold(t *testing.T) {
	db := newTestDB(t)

	for id := uint64(1); id <= 5; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 2, 5, 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), blocksDeleted)

	for id := uint64(1); id <= 2; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(t, err, "block %d should survive truncation", id)
	}
	for id := uint64(3); id <= 5; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIsf(
			t, err, models.ErrBlockNotFound,
			"block %d should have been deleted", id,
		)
	}
}

// TestDeleteBlocksAfterNoopWhenTipAtOrBelowThreshold verifies that a
// threshold at or above the current tip deletes nothing.
func TestDeleteBlocksAfterNoopWhenTipAtOrBelowThreshold(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 5, 1, 0,
	)
	require.NoError(t, err)
	require.Zero(t, blocksDeleted)

	_, err = db.BlockByIndex(1, nil)
	require.NoError(t, err)
}

// TestDeleteBlocksAfterRespectsSmallBatchSize verifies that a batch size
// forcing multiple transactions still deletes exactly the same blocks.
func TestDeleteBlocksAfterRespectsSmallBatchSize(t *testing.T) {
	db := newTestDB(t)
	for id := uint64(1); id <= 10; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	// batchSize=1 forces multiple transactions; the end result must be the
	// same as a single large batch.
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 3, 10, 1,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(7), blocksDeleted)

	for id := uint64(1); id <= 3; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoError(t, err)
	}
	for id := uint64(4); id <= 10; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
}

// TestDeleteBlocksAfterNoticesCancellationMidBatch guards against
// a real bug: ctx was only checked once per batch (before
// entering that batch's transaction), so with the default 10,000-block
// batch size, a cancellation landing partway through a single large batch
// used to sit unnoticed until the entire batch finished deleting —
// potentially a long delay for a disaster-recovery truncate an operator
// just asked to cancel. This uses a single batch (batchSize larger than
// the whole block range) and a context that reports "not yet cancelled"
// for exactly the one check made before the batch starts, then
// "cancelled" for every check after that: with only a once-per-batch
// check, that single pre-batch check passes and the whole batch (all
// blocks) completes with no error; with a per-block check, the very
// first block inside the batch observes the cancellation, so the whole
// batch's transaction rolls back and no blocks are deleted at all.
func TestDeleteBlocksAfterNoticesCancellationMidBatch(t *testing.T) {
	db := newTestDB(t)
	const numBlocks = 20
	for id := uint64(1); id <= numBlocks; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	// batchSize=0 defaults to DefaultBlockDeleteBatchSize (10,000), well
	// above numBlocks, so DeleteBlocksAfter processes every block in a
	// single batch/transaction.
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 1}
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(ctx, db, 0, numBlocks, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(
		t, blocksDeleted,
		"a rolled-back batch must not be counted as deleted",
	)

	for id := uint64(1); id <= numBlocks; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(
			t, err,
			"block %d must survive: the whole batch's transaction must "+
				"roll back once cancellation is noticed mid-batch", id,
		)
	}
}

// TestDeleteBlocksAfterCanceledContext verifies that a pre-cancelled
// context is caught before any batch runs, returning context.Canceled.
func TestDeleteBlocksAfterCanceledContext(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, db.BlockCreate(testBlock(2, 0x02), nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(ctx, db, 0, 2, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, blocksDeleted)
}

// countingBlobStore wraps a real blob.BlobStore and counts calls to the
// operations DeleteBlocksAfter's range scan can drive per numeric ID vs.
// per batch, so a test can assert directly that cost scales with blocks
// actually stored rather than with how wide an ID range is (see
// TestDeleteBlocksAfterSkipsSparseGapWithoutPerIDLookups).
type countingBlobStore struct {
	blob.BlobStore
	getCalls      atomic.Int64
	iteratorCalls atomic.Int64
	deleteCalls   atomic.Int64
}

func (c *countingBlobStore) Get(txn types.Txn, key []byte) ([]byte, error) {
	c.getCalls.Add(1)
	return c.BlobStore.Get(txn, key)
}

func (c *countingBlobStore) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	c.iteratorCalls.Add(1)
	return c.BlobStore.NewIterator(txn, opts)
}

func (c *countingBlobStore) DeleteBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	id uint64,
) error {
	c.deleteCalls.Add(1)
	return c.BlobStore.DeleteBlock(txn, slot, hash, id)
}

func (c *countingBlobStore) reset() {
	c.getCalls.Store(0)
	c.iteratorCalls.Store(0)
	c.deleteCalls.Store(0)
}

// newCountingTestDB composes a real badger+sqlite test database like
// newTestDB, but wraps the blob store in countingBlobStore so a test can
// measure how many times DeleteBlocksAfter calls into the blob store.
func newCountingTestDB(t *testing.T) (*database.Database, *countingBlobStore) {
	t.Helper()
	config := &database.Config{DataDir: t.TempDir()}
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))

	realBlob, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)
	counting := &countingBlobStore{BlobStore: realBlob}

	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)

	db, err := database.New(
		config,
		database.Stores{Blob: counting, Metadata: metadataStore},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = host.Stop(context.Background())
	})
	return db, counting
}

// TestDeleteBlocksAfterSkipsSparseGapWithoutPerIDLookups guards against
// cubic-dev-ai finding #2: the original loop probed every numeric ID in
// (afterID, tipID] one at a time (via db.BlockByIndex, one blob Get per
// ID), so a Mithril-bootstrap-style sparse gap of unimported IDs turned a
// small deletion into one remote lookup per absent ID -- for a
// cloud-backed store that is a catastrophic number of remote calls for a
// deep truncate spanning a large gap. The fix seeks the ordered "bi" index
// once per batch and walks forward only over entries that actually exist,
// so cost must track the handful of blocks really stored, not the width
// of the gap between them.
func TestDeleteBlocksAfterSkipsSparseGapWithoutPerIDLookups(t *testing.T) {
	db, counting := newCountingTestDB(t)

	// A huge never-imported gap between id 3 and id 100_000, mirroring the
	// sparse chains buildSparseTestChain builds in truncate_test.go for the
	// same reason (a Mithril bootstrap/drain import leaving unimported ID
	// ranges).
	const gapEnd = 100_000
	ids := []uint64{1, 2, 3, gapEnd}
	for _, id := range ids {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}
	counting.reset()

	// batchSize bigger than the whole range: a single batch/transaction
	// covers everything, so any per-batch (rather than per-ID) cost shows
	// up as a small constant number of iterator/Get calls.
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 0, gapEnd, gapEnd*2,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(len(ids)), blocksDeleted)

	// Snapshot the counters driven by DeleteBlocksAfter itself before the
	// verification loop below issues its own Get calls (via BlockByIndex)
	// against the same counting store.
	getCallsDuringDelete := counting.getCalls.Load()
	iteratorCallsDuringDelete := counting.iteratorCalls.Load()
	deleteCallsDuringDelete := counting.deleteCalls.Load()

	for _, id := range ids {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIsf(t, err, models.ErrBlockNotFound,
			"block %d should have been deleted", id)
	}

	// One iterator seek covers the whole batch...
	require.LessOrEqual(t, iteratorCallsDuringDelete, int64(1))
	// ...and the range scan itself never calls Get at all: the ordered
	// "bi" index walk resolves each present entry straight from the
	// iterator, so work is proportional to the four blocks actually
	// stored, not to the 100,000-wide numeric gap between them. (Contrast
	// the old per-ID-probe implementation, which issued one Get per ID
	// across the whole gap -- about 100,000 calls here.)
	require.Zero(t, getCallsDuringDelete)
	require.Equal(t, int64(len(ids)), deleteCallsDuringDelete)
}

// erroringIterator mimics how the real gcs/s3 iterators behave when their
// eager listing call fails (see gcsIterator/s3Iterator): Valid/ValidForPrefix
// report false immediately -- identical to "prefix is genuinely empty" --
// and the stored error is only observable through Err().
type erroringIterator struct {
	err error
}

func (i *erroringIterator) Rewind()                           {}
func (i *erroringIterator) Seek(prefix []byte)                {}
func (i *erroringIterator) Valid() bool                       { return false }
func (i *erroringIterator) ValidForPrefix(prefix []byte) bool { return false }
func (i *erroringIterator) Next()                             {}
func (i *erroringIterator) Item() types.BlobItem              { return nil }
func (i *erroringIterator) Close()                            {}
func (i *erroringIterator) Err() error                        { return i.err }

// erroringIteratorBlobStore wraps a real blob.BlobStore and replaces every
// iterator it hands out with erroringIterator, simulating a cloud listing
// call that failed before yielding a single key.
type erroringIteratorBlobStore struct {
	blob.BlobStore
	err error
}

func (e *erroringIteratorBlobStore) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	return &erroringIterator{err: e.err}
}

func newErroringIteratorTestDB(
	t *testing.T,
	iterErr error,
) *database.Database {
	t.Helper()
	config := &database.Config{DataDir: t.TempDir()}
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))

	realBlob, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)

	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)

	db, err := database.New(
		config,
		database.Stores{
			Blob:     &erroringIteratorBlobStore{BlobStore: realBlob, err: iterErr},
			Metadata: metadataStore,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = host.Stop(context.Background())
	})
	return db
}

// TestDeleteBlocksAfterSurfacesIteratorErrorInsteadOfTreatingItAsEmptyRange
// guards against a real gap: the real gcs/s3 iterators page their full key
// listing eagerly inside NewIterator, and a failed list call is recorded on
// the iterator itself, surfacing only through Err() -- ValidForPrefix
// reports false immediately, indistinguishable from "this prefix has no
// keys". Without checking it.Err(), a failed listing would make
// DeleteBlocksAfter conclude the batch's range is simply empty, commit
// (deleting nothing), and report success -- silently letting a subsequent
// metadata truncation advance past blob blocks that were never actually
// examined, let alone deleted.
func TestDeleteBlocksAfterSurfacesIteratorErrorInsteadOfTreatingItAsEmptyRange(
	t *testing.T,
) {
	sentinel := errors.New("simulated cloud list failure")
	db := newErroringIteratorTestDB(t, sentinel)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, db.BlockCreate(testBlock(2, 0x02), nil))

	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 0, 2, 0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinel)
	require.Zero(t, blocksDeleted)

	for id := uint64(1); id <= 2; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(
			t, err,
			"block %d must survive: a failed listing must not be reported "+
				"as a successfully completed, empty batch", id,
		)
	}
}

// midWalkErrorIterator wraps a real BlobIterator and induces a listing
// failure after n successful ValidForPrefix checks, mirroring a cloud
// paginator that fails partway through a multi-page listing: it visited the
// first n keys through the real iterator, but the next ValidForPrefix call
// reports false -- indistinguishable from "no more keys" -- while Err()
// surfaces the injected error.
type midWalkErrorIterator struct {
	types.BlobIterator
	remaining int
	err       error
}

func (i *midWalkErrorIterator) ValidForPrefix(prefix []byte) bool {
	if i.remaining <= 0 {
		return false
	}
	i.remaining--
	return i.BlobIterator.ValidForPrefix(prefix)
}

func (i *midWalkErrorIterator) Err() error {
	if i.remaining <= 0 {
		return i.err
	}
	return i.BlobIterator.Err()
}

// midWalkErrorBlobStore wraps a real blob.BlobStore and replaces every
// iterator it hands out with midWalkErrorIterator.
type midWalkErrorBlobStore struct {
	blob.BlobStore
	n   int
	err error
}

func (m *midWalkErrorBlobStore) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	return &midWalkErrorIterator{
		BlobIterator: m.BlobStore.NewIterator(txn, opts),
		remaining:    m.n,
		err:          m.err,
	}
}

func newMidWalkErrorTestDB(
	t *testing.T,
	n int,
	iterErr error,
) *database.Database {
	t.Helper()
	config := &database.Config{DataDir: t.TempDir()}
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))

	realBlob, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)

	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)

	db, err := database.New(
		config,
		database.Stores{
			Blob:     &midWalkErrorBlobStore{BlobStore: realBlob, n: n, err: iterErr},
			Metadata: metadataStore,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = host.Stop(context.Background())
	})
	return db
}

// TestDeleteBlocksAfterSurfacesIteratorErrorPartwayThroughWalk guards the
// other half of the same gap: a listing failure that happens after some
// keys were already visited, not before any. Without checking it.Err()
// after the walk, the loop exiting because ValidForPrefix went false would
// look identical to "reached the end of this batch's range", the batch
// would commit whatever partial deletes it made, and DeleteBlocksAfter
// would report success -- even though blocks later in the range were never
// examined at all (not "absent", simply never looked at) because the
// listing itself broke. This must instead fail the whole batch so nothing
// commits, exactly as if the transaction's own db.Blob*() calls had failed.
func TestDeleteBlocksAfterSurfacesIteratorErrorPartwayThroughWalk(
	t *testing.T,
) {
	sentinel := errors.New("simulated mid-listing cloud failure")
	// n=2: the iterator behaves normally for the first two entries (blocks
	// 1 and 2), then reports "no more keys" while Err() reveals the
	// injected failure -- before ever reaching block 3.
	db := newMidWalkErrorTestDB(t, 2, sentinel)
	for id := uint64(1); id <= 3; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 0, 3, 0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinel)
	require.Zero(t, blocksDeleted)

	// A real (non-cloud) blob store's transaction rolls back on error, so
	// nothing committed: all three blocks must survive even though the
	// first two were visited and issued for deletion before the failure
	// was detected.
	for id := uint64(1); id <= 3; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(
			t, err,
			"block %d must survive: a partial listing failure must roll "+
				"back the whole batch, not commit a silent partial delete",
			id,
		)
	}
}

// cloudLikeTxn wraps a real blob.BlobStore transaction so it behaves like
// the real cloud plugins' gcsTxn/s3Txn (see database/plugin/blob/gcs and
// .../aws): every Set/Delete call already took effect against the
// underlying store the instant it was made, and Commit/Rollback are both
// effectively no-ops with respect to undoing that -- there is no staged,
// rollback-able state to discard. Badger's real transaction buffers writes
// until Commit, so to reproduce that irreversibility using badger as the
// backing engine, Rollback on a read-write transaction commits the
// underlying real transaction instead of discarding it: whatever mutations
// already happened before the rollback stay, exactly like an interrupted
// cloud batch. Read-only transactions behave normally (there is nothing to
// lose either way).
type cloudLikeTxn struct {
	real      types.Txn
	readWrite bool
	finished  bool
}

func (t *cloudLikeTxn) Commit() error {
	if t.finished {
		return nil
	}
	t.finished = true
	return t.real.Commit()
}

func (t *cloudLikeTxn) Rollback() error {
	if t.finished {
		return nil
	}
	t.finished = true
	if !t.readWrite {
		return t.real.Rollback()
	}
	return t.real.Commit()
}

func (t *cloudLikeTxn) RollbackIsNoop() bool {
	return true
}

// cloudLikeBlobStore wraps a real blob.BlobStore and makes its
// transactions irreversible the way the real cloud plugins are (see
// cloudLikeTxn), so a test can exercise DeleteBlocksAfter's cloud-specific
// partial-batch-failure behavior without needing real GCS/S3 credentials.
type cloudLikeBlobStore struct {
	blob.BlobStore
}

func (c *cloudLikeBlobStore) NewTransaction(readWrite bool) types.Txn {
	return &cloudLikeTxn{
		real:      c.BlobStore.NewTransaction(readWrite),
		readWrite: readWrite,
	}
}

func unwrapCloudLikeTxn(txn types.Txn) types.Txn {
	if c, ok := txn.(*cloudLikeTxn); ok {
		return c.real
	}
	return txn
}

func (c *cloudLikeBlobStore) Get(txn types.Txn, key []byte) ([]byte, error) {
	return c.BlobStore.Get(unwrapCloudLikeTxn(txn), key)
}

func (c *cloudLikeBlobStore) Set(txn types.Txn, key, val []byte) error {
	return c.BlobStore.Set(unwrapCloudLikeTxn(txn), key, val)
}

func (c *cloudLikeBlobStore) Delete(txn types.Txn, key []byte) error {
	return c.BlobStore.Delete(unwrapCloudLikeTxn(txn), key)
}

func (c *cloudLikeBlobStore) NewIterator(
	txn types.Txn,
	opts types.BlobIteratorOptions,
) types.BlobIterator {
	return c.BlobStore.NewIterator(unwrapCloudLikeTxn(txn), opts)
}

func (c *cloudLikeBlobStore) SetCommitTimestamp(
	ts int64,
	txn types.Txn,
) error {
	return c.BlobStore.SetCommitTimestamp(ts, unwrapCloudLikeTxn(txn))
}

func (c *cloudLikeBlobStore) SetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	cborData []byte,
	id uint64,
	blockType uint,
	height uint64,
	prevHash []byte,
) error {
	return c.BlobStore.SetBlock(
		unwrapCloudLikeTxn(txn),
		slot,
		hash,
		cborData,
		id,
		blockType,
		height,
		prevHash,
	)
}

func (c *cloudLikeBlobStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	return c.BlobStore.GetBlock(unwrapCloudLikeTxn(txn), slot, hash)
}

func (c *cloudLikeBlobStore) DeleteBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
	id uint64,
) error {
	return c.BlobStore.DeleteBlock(unwrapCloudLikeTxn(txn), slot, hash, id)
}

func (c *cloudLikeBlobStore) TombstoneBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) error {
	return c.BlobStore.TombstoneBlock(unwrapCloudLikeTxn(txn), slot, hash)
}

func (c *cloudLikeBlobStore) GetBlockURL(
	ctx context.Context,
	txn types.Txn,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	return c.BlobStore.GetBlockURL(ctx, unwrapCloudLikeTxn(txn), point)
}

func (c *cloudLikeBlobStore) SetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
	cborData []byte,
) error {
	return c.BlobStore.SetUtxo(
		unwrapCloudLikeTxn(txn), txId, outputIdx, cborData,
	)
}

func (c *cloudLikeBlobStore) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	return c.BlobStore.GetUtxo(unwrapCloudLikeTxn(txn), txId, outputIdx)
}

func (c *cloudLikeBlobStore) DeleteUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	return c.BlobStore.DeleteUtxo(unwrapCloudLikeTxn(txn), txId, outputIdx)
}

func (c *cloudLikeBlobStore) SetTx(
	txn types.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	return c.BlobStore.SetTx(unwrapCloudLikeTxn(txn), txHash, offsetData)
}

func (c *cloudLikeBlobStore) GetTx(
	txn types.Txn,
	txHash []byte,
) ([]byte, error) {
	return c.BlobStore.GetTx(unwrapCloudLikeTxn(txn), txHash)
}

func (c *cloudLikeBlobStore) DeleteTx(txn types.Txn, txHash []byte) error {
	return c.BlobStore.DeleteTx(unwrapCloudLikeTxn(txn), txHash)
}

// newCloudLikeTestDB composes a real badger+sqlite test database like
// newTestDB, but wraps the blob store in cloudLikeBlobStore so deletes
// issued during a batch cannot be undone by a later Rollback, matching how
// the real GCS/S3 plugins behave.
func newCloudLikeTestDB(t *testing.T) *database.Database {
	t.Helper()
	config := &database.Config{DataDir: t.TempDir()}
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))

	realBlob, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)

	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{DataDir: config.DataDir},
	)
	require.NoError(t, err)

	db, err := database.New(
		config,
		database.Stores{
			Blob:     &cloudLikeBlobStore{BlobStore: realBlob},
			Metadata: metadataStore,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = host.Stop(context.Background())
	})
	return db
}

// TestDeleteBlocksAfterHonestlyReportsProgressOnCloudLikeMidBatchFailureAndResumeIsSafe
// guards against cubic-dev-ai finding #1: for a cloud-backed blob store
// (GCS/S3), individual Delete calls are not rollback-able -- once issued,
// they are permanent regardless of what happens to the rest of the batch
// afterward (see gcsTxn/s3Txn's Commit/Rollback, both no-ops). The
// original code assumed txn.Do's rollback always undid a failed batch and
// reported 0 blocks removed on any batch error, which silently hid real
// data loss for a cloud-backed store when a batch failed or was cancelled
// partway through. This uses cloudLikeBlobStore to reproduce that
// irreversibility deterministically (without needing real GCS/S3
// credentials) and checks two things: the returned count on a mid-batch
// failure reflects the blocks that were actually, permanently deleted (not
// zero), and resuming with the same range afterward is safe -- it deletes
// exactly the remaining blocks, with no error and no double-delete.
func TestDeleteBlocksAfterHonestlyReportsProgressOnCloudLikeMidBatchFailureAndResumeIsSafe(
	t *testing.T,
) {
	db := newCloudLikeTestDB(t)
	const numBlocks = 5
	for id := uint64(1); id <= numBlocks; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	// batchSize larger than numBlocks: everything is one batch/transaction.
	// The context reports "not cancelled" for the first 3 checks (the
	// pre-batch check, then blocks 1 and 2), then "cancelled" from the 4th
	// check onward (before block 3 is processed) -- so exactly 2 blocks
	// are actually deleted before the batch's transaction function returns
	// the error.
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(ctx, db, 0, numBlocks, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(
		t, uint64(2), blocksDeleted,
		"a cloud-like store cannot roll back deletes it already issued; "+
			"the returned count must reflect that, not report zero",
	)

	// The 2 blocks actually deleted before cancellation are permanently
	// gone; the rest survive.
	for id := uint64(1); id <= 2; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIsf(t, err, models.ErrBlockNotFound,
			"block %d was already deleted before cancellation and cannot "+
				"come back", id)
	}
	for id := uint64(3); id <= numBlocks; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(t, err, "block %d must still be present", id)
	}

	// Resuming with the identical range must be safe: it should delete
	// exactly the remaining 3 blocks, without erroring or double-deleting
	// the 2 already gone.
	blocksDeleted, err = lifecycle.DeleteBlocksAfter(
		context.Background(), db, 0, numBlocks, 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), blocksDeleted)
	for id := uint64(1); id <= numBlocks; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIsf(t, err, models.ErrBlockNotFound,
			"block %d should have been deleted", id)
	}
}
