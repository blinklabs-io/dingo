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
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isolateBlockByHashMetrics runs exact-total assertions in their own process.
// Any parallel database test can increment these process-wide metrics, even
// without reading or resetting them. A mutex around the metric tests alone
// therefore cannot isolate them. Each child runs just the selected test using
// the same test binary, including its race instrumentation when enabled.
func isolateBlockByHashMetrics(t *testing.T) bool {
	t.Helper()
	const marker = "DINGO_BLOCK_BY_HASH_METRIC_TEST"
	if os.Getenv(marker) == t.Name() {
		return false
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		os.Args[0],
		"-test.run=^"+t.Name()+"$",
		"-test.v",
	)
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated metric test failed: %v\n%s", err, output)
	}
	return true
}

// resetBlockByHashStats zeros the hit/miss counters between tests.
func resetBlockByHashStats() {
	blockByHashIndexHits.Store(0)
	blockByHashIndexMisses.Store(0)
}

// TestBlockByHashTxn_UnknownHashRecordsMissAndNotFound verifies that an
// unknown hash increments the miss counter (so operators can track the
// index miss rate from ) and returns ErrBlockNotFound directly on
// the index miss, without any fallback scan.
func TestBlockByHashTxn_UnknownHashRecordsMissAndNotFound(t *testing.T) {
	t.Parallel()
	if isolateBlockByHashMetrics(t) {
		return
	}

	db := newTestDB(t)
	resetBlockByHashStats()

	const seeded = 16
	for i := range seeded {
		insertTestBlock(t, db, uint64(i+1), randomHash(t), []byte("cbor"))
	}

	unknown := randomHash(t)
	_, err := BlockByHash(db, unknown)
	require.ErrorIs(
		t,
		err,
		models.ErrBlockNotFound,
		"unknown hash must surface as ErrBlockNotFound so fork-resolution can rotate peers",
	)

	hits, misses := BlockByHashStats()
	assert.Equal(
		t,
		uint64(0),
		hits,
		"no hash-index hit expected for unknown hash",
	)
	assert.Equal(
		t,
		uint64(1),
		misses,
		"miss counter must record the false-fallback so operators can track the back-fill rate (#2105)",
	)
}

// TestBlockByHashTxn_KnownHashStillResolves guards the fast path: every
// block written via BlockCreate gets a hash-index entry, and a
// lookup must hit it in O(1) and return the block.
func TestBlockByHashTxn_KnownHashStillResolves(t *testing.T) {
	t.Parallel()
	if isolateBlockByHashMetrics(t) {
		return
	}

	db := newTestDB(t)
	resetBlockByHashStats()

	hash := randomHash(t)
	insertTestBlock(t, db, 42, hash, []byte("payload"))

	got, err := BlockByHash(db, hash)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got.Slot)
	assert.Equal(t, hash, got.Hash)

	hits, misses := BlockByHashStats()
	assert.Equal(t, uint64(1), hits, "indexed lookup must take the fast path")
	assert.Equal(t, uint64(0), misses)
}

// TestBlockByHashTxn_EmptyIndexEntryIsCorruption asserts that a hash-
// index entry whose value is an empty byte slice surfaces a descriptive
// non-ErrBlockNotFound error rather than a soft miss. An empty value
// means the index was written but the pointer is invalid: a local DB
// problem the operator needs to see, not a fork-resolution miss.
func TestBlockByHashTxn_EmptyIndexEntryIsCorruption(t *testing.T) {
	t.Parallel()
	if isolateBlockByHashMetrics(t) {
		return
	}

	db := newTestDB(t)
	resetBlockByHashStats()

	hash := randomHash(t)
	hashIndexKey := types.BlockHashIndexKey(hash)
	txn := db.BlobTxn(true)
	require.NoError(t, db.Blob().Set(txn.Blob(), hashIndexKey, []byte{}))
	require.NoError(t, txn.Commit())

	_, err := BlockByHash(db, hash)
	require.Error(t, err)
	assert.NotErrorIs(t, err, models.ErrBlockNotFound,
		"empty index entry must not be reported as a soft miss")
	assert.True(t,
		strings.Contains(err.Error(), "empty block hash index entry"),
		"error should identify corruption: got %v", err)

	hits, misses := BlockByHashStats()
	assert.Equal(t, uint64(0), hits)
	assert.Equal(t, uint64(0), misses,
		"corruption must not be folded into the miss counter")
}

// TestRegisterBlockByHashMetrics_PerRegistry verifies that every registry
// passed to RegisterBlockByHashMetrics exposes the hash-index counters, not
// just the first one in the process, and that reusing a registry is a no-op.
func TestRegisterBlockByHashMetrics_PerRegistry(t *testing.T) {
	t.Parallel()
	if isolateBlockByHashMetrics(t) {
		return
	}

	resetBlockByHashStats()
	blockByHashIndexMisses.Add(3)

	for i := range 2 {
		reg := prometheus.NewRegistry()
		require.NoError(t, RegisterBlockByHashMetrics(reg))
		// reuse must not error or duplicate
		require.NoError(t, RegisterBlockByHashMetrics(reg))

		families, err := reg.Gather()
		require.NoError(t, err)
		found := map[string]float64{}
		for _, mf := range families {
			for _, m := range mf.GetMetric() {
				found[mf.GetName()] = m.GetCounter().GetValue()
			}
		}
		assert.Contains(t, found,
			"dingo_database_block_hash_index_hits_total",
			"registry %d must expose the hit counter", i)
		assert.Equal(t, float64(3),
			found["dingo_database_block_hash_index_misses_total"],
			"registry %d must read the shared miss total", i)
	}
	resetBlockByHashStats()
}

// BenchmarkBlockByHashTxn_UnknownHash measures the cost of the fork-
// resolution miss path on a small DB.
//
// Run with: go test -bench=BenchmarkBlockByHashTxn -benchmem ./database/
func BenchmarkBlockByHashTxn_UnknownHash(b *testing.B) {
	db := newBenchDB(b)
	const seeded = 1024
	for i := range seeded {
		hash := make([]byte, 32)
		hash[0] = byte(i)
		hash[1] = byte(i >> 8)
		block := models.Block{
			Slot: uint64(i + 1),
			Hash: hash,
			Cbor: []byte("cbor"),
			Type: 1,
		}
		require.NoError(b, db.BlockCreate(block, nil))
	}

	unknown := make([]byte, 32)
	for i := range unknown {
		unknown[i] = 0xFF
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = BlockByHash(db, unknown)
	}
}

func newBenchDB(b *testing.B) *Database {
	b.Helper()
	cfg := &Config{DataDir: ""}
	db, err := newTestDatabase(b, cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// TestCountBlocksAndOldestSlot_EmptyDatabase verifies that a database
// with no blocks reports a zero count and a zero oldest slot.
func TestCountBlocksAndOldestSlot_EmptyDatabase(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	count, oldestSlot, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, oldestSlot)
}

// TestCountBlocksAndOldestSlot_CountsAndFindsOldest verifies that blocks
// inserted out of slot order still report the correct count and oldest slot.
func TestCountBlocksAndOldestSlot_CountsAndFindsOldest(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	// Inserted out of slot order on purpose: the oldest slot must be
	// found by content, not by insertion order.
	insertTestBlock(t, db, 300, randomHash(t), []byte{0x80})
	insertTestBlock(t, db, 100, randomHash(t), []byte{0x80})
	insertTestBlock(t, db, 200, randomHash(t), []byte{0x80})

	count, oldestSlot, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), count)
	require.Equal(t, uint64(100), oldestSlot)
}

// TestCountBlocksAndOldestSlot_ExcludesTombstonedBlocks verifies that a
// history-expiry-pruned block (its bp key kept alive with a tombstone
// marker so bi/bh lookups still resolve — see TombstoneBlock) is excluded
// from both the count and the oldest-slot search: its content isn't
// actually retained, so counting it would overstate how much history is
// available and understate how far back retained history actually goes.
func TestCountBlocksAndOldestSlot_ExcludesTombstonedBlocks(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	oldestHash := randomHash(t)
	insertTestBlock(t, db, 100, oldestHash, []byte{0x80})
	insertTestBlock(t, db, 200, randomHash(t), []byte{0x80})

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().TombstoneBlock(txn.Blob(), 100, oldestHash)
	}))

	count, oldestSlot, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(1),
		count,
		"the tombstoned block must not be counted",
	)
	require.Equal(
		t, uint64(200), oldestSlot,
		"the tombstoned block's slot must not be reported as the oldest",
	)
}

func TestExtractTransactionOffsets(t *testing.T) {
	t.Parallel()

	// Load blocks from immutable test data
	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err, "failed to open immutable database")

	// Get an iterator starting from the beginning
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err, "failed to create block iterator")
	defer iter.Close()

	// Test offset extraction on multiple blocks
	blocksWithTx := 0
	maxBlocksToTest := 500

	for i := range maxBlocksToTest {
		immBlock, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected error reading block %d: %s", i, err)
		}
		if immBlock == nil {
			break // End of chain
		}

		// Parse the block
		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		if err != nil {
			// Skip blocks that can't be parsed (e.g., Byron EBB)
			continue
		}

		// Skip blocks without transactions (e.g., Byron EBB or empty blocks)
		txs := block.Transactions()
		if len(txs) == 0 {
			continue
		}

		blocksWithTx++

		// Extract offsets
		offsets, err := common.ExtractTransactionOffsets(immBlock.Cbor)
		require.NoError(
			t,
			err,
			"failed to extract offsets from block %d (slot %d)",
			i,
			immBlock.Slot,
		)

		// Verify we got offsets for all transactions
		assert.Equal(
			t,
			len(txs),
			len(offsets.Transactions),
			"transaction count mismatch for block %d (slot %d)",
			i,
			immBlock.Slot,
		)

		// Verify each offset points to valid data
		for txIdx, tx := range txs {
			if txIdx >= len(offsets.Transactions) {
				continue
			}

			loc := offsets.Transactions[txIdx]

			// Verify body offset is within block bounds
			bodyEnd := uint64(loc.Body.Offset) + uint64(loc.Body.Length)
			assert.LessOrEqual(t, bodyEnd, uint64(len(immBlock.Cbor)),
				"body offset out of bounds for tx %d in block %d", txIdx, i)

			// Extract body CBOR and verify it matches transaction body
			if loc.Body.Offset > 0 && loc.Body.Length > 0 &&
				bodyEnd <= uint64(len(immBlock.Cbor)) {
				extractedBody := immBlock.Cbor[loc.Body.Offset : loc.Body.Offset+loc.Body.Length]

				// The extracted data should be valid CBOR (basic sanity check)
				assert.Greater(t, len(extractedBody), 0,
					"extracted body is empty for tx %d in block %d", txIdx, i)

				// Compare with transaction's stored CBOR if available
				txCbor := tx.Cbor()
				if len(txCbor) > 0 {
					// Transaction CBOR includes both body and witnesses,
					// so extracted body should be a prefix or we need to
					// compare differently based on era
					assert.LessOrEqual(
						t,
						len(extractedBody),
						len(txCbor)+1000,
						"extracted body unexpectedly larger than tx cbor for tx %d in block %d",
						txIdx,
						i,
					)
				}
			}

			// Verify witness offset is within block bounds
			witnessEnd := uint64(
				loc.Witness.Offset,
			) + uint64(
				loc.Witness.Length,
			)
			assert.LessOrEqual(t, witnessEnd, uint64(len(immBlock.Cbor)),
				"witness offset out of bounds for tx %d in block %d", txIdx, i)

			// Verify datum, redeemer, and script offsets if present
			for hash, datumLoc := range loc.Datums {
				datumEnd := uint64(datumLoc.Offset) + uint64(datumLoc.Length)
				assert.LessOrEqual(t, datumEnd, uint64(len(immBlock.Cbor)),
					"datum offset out of bounds for hash %x in tx %d block %d",
					hash[:8], txIdx, i)
			}

			for key, redeemerLoc := range loc.Redeemers {
				redeemerEnd := uint64(
					redeemerLoc.Offset,
				) + uint64(
					redeemerLoc.Length,
				)
				assert.LessOrEqual(
					t,
					redeemerEnd,
					uint64(len(immBlock.Cbor)),
					"redeemer offset out of bounds for key (%d,%d) in tx %d block %d",
					key.Tag,
					key.Index,
					txIdx,
					i,
				)
			}

			for hash, scriptLoc := range loc.Scripts {
				scriptEnd := uint64(scriptLoc.Offset) + uint64(scriptLoc.Length)
				assert.LessOrEqual(t, scriptEnd, uint64(len(immBlock.Cbor)),
					"script offset out of bounds for hash %x in tx %d block %d",
					hash[:8], txIdx, i)
			}
		}

		// Stop after testing some blocks with transactions
		if blocksWithTx >= 20 {
			break
		}
	}

	assert.Greater(
		t,
		blocksWithTx,
		0,
		"no blocks with transactions were tested",
	)
	t.Logf("Successfully tested %d blocks with transactions", blocksWithTx)
}

func TestExtractTransactionOffsetsEmptyBlock(t *testing.T) {
	t.Parallel()

	// Test that the function handles blocks with empty/minimal structure
	// Byron EBB blocks have a different structure

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err, "failed to open immutable database")

	// Get first block (typically Byron genesis or EBB)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err, "failed to create block iterator")
	defer iter.Close()

	immBlock, err := iter.Next()
	require.NoError(t, err, "failed to get first block")
	require.NotNil(t, immBlock, "expected at least one block")

	// Should not panic on Byron blocks
	offsets, err := common.ExtractTransactionOffsets(immBlock.Cbor)
	// Error is acceptable for unsupported block formats
	if err == nil {
		assert.NotNil(t, offsets, "offsets should not be nil when no error")
	}
}

type countingBlockReadStore struct {
	blob.BlobStore
	getBlockCalls int
}

func (s *countingBlockReadStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	s.getBlockCalls++
	return s.BlobStore.GetBlock(txn, slot, hash)
}

type localBlockReadStore struct {
	blob.BlobStore
	archiveFallbackCalls int
}

func (s *localBlockReadStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	s.archiveFallbackCalls++
	return s.BlobStore.GetBlock(txn, slot, hash)
}

func (s *localBlockReadStore) GetBlockLocal(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	return s.BlobStore.GetBlock(txn, slot, hash)
}

func testIndexedBlock(slot, id uint64, hashByte byte) models.Block {
	return models.Block{
		ID:     id,
		Slot:   slot,
		Hash:   bytes.Repeat([]byte{hashByte}, 32),
		Cbor:   []byte{0x80},
		Number: id,
		Type:   1,
	}
}

func TestBlockBySlotReturnsHighestIndexedBlockForSlot(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	const slot = uint64(42)

	lowerIDBlock := testIndexedBlock(slot, 10, 0x10)
	higherIDBlock := testIndexedBlock(slot, 11, 0x11)
	require.NoError(t, db.BlockCreate(lowerIDBlock, nil))
	require.NoError(t, db.BlockCreate(higherIDBlock, nil))

	block, err := BlockBySlot(db, slot)
	require.NoError(t, err)
	require.Equal(t, higherIDBlock.ID, block.ID)
	require.Equal(t, higherIDBlock.Hash, block.Hash)
}

func TestBlockBySlotSkipsStaleSameSlotIndex(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	const slot = uint64(42)

	lowerIDBlock := testIndexedBlock(slot, 10, 0x10)
	higherIDBlock := testIndexedBlock(slot, 11, 0x11)
	require.NoError(t, db.BlockCreate(lowerIDBlock, nil))
	require.NoError(t, db.BlockCreate(higherIDBlock, nil))

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().Set(
			txn.Blob(),
			types.BlockBlobIndexKey(higherIDBlock.ID),
			types.BlockBlobKey(lowerIDBlock.Slot, lowerIDBlock.Hash),
		)
	}))

	block, err := BlockBySlot(db, slot)
	require.NoError(t, err)
	require.Equal(t, lowerIDBlock.ID, block.ID)
	require.Equal(t, lowerIDBlock.Hash, block.Hash)
}

func TestBlockPointByIndexDoesNotReadBlockContent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	block := testIndexedBlock(42, 7, 0x42)
	require.NoError(t, db.BlockCreate(block, nil))

	store := &countingBlockReadStore{BlobStore: db.Blob()}
	db.SetBlobStore(store)

	point, err := db.BlockPointByIndex(block.ID, nil)
	require.NoError(t, err)
	require.Equal(t, block.Slot, point.Slot)
	require.Equal(t, block.Hash, point.Hash)
	require.Zero(
		t,
		store.getBlockCalls,
		"point-only lookup must not load block CBOR or trigger archive fallback",
	)
}

func TestBlockIDByPointLocalBypassesArchiveFallback(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	block := testIndexedBlock(42, 7, 0x42)
	require.NoError(t, db.BlockCreate(block, nil))

	store := &localBlockReadStore{BlobStore: db.Blob()}
	db.SetBlobStore(store)

	blockID, err := BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, block.Hash),
	)
	require.NoError(t, err)
	require.Equal(t, block.ID, blockID)
	require.Zero(t, store.archiveFallbackCalls)

	_, err = BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, bytes.Repeat([]byte{0xff}, 32)),
	)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.Zero(t, store.archiveFallbackCalls)

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().TombstoneBlock(
			txn.Blob(), block.Slot, block.Hash,
		)
	}))
	blockID, err = BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, block.Hash),
	)
	require.NoError(t, err)
	require.Equal(t, block.ID, blockID)
	require.Zero(t, store.archiveFallbackCalls)

	// Older cloud tombstones did not retain metadata. They cannot recover a
	// local ID and must remain a non-match instead of aliasing block ID zero.
	txn = db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().Delete(
			txn.Blob(),
			types.BlockBlobMetadataKey(
				types.BlockBlobKey(block.Slot, block.Hash),
			),
		)
	}))
	_, err = BlockIDByPointLocal(
		db,
		ocommon.NewPoint(block.Slot, block.Hash),
	)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.Zero(t, store.archiveFallbackCalls)
}

func TestBlockAtOrAfterIndexSkipsSparseIndexes(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	blocks := []models.Block{
		testIndexedBlock(10, 1, 0x01),
		testIndexedBlock(20, 1_000_000, 0x02),
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}

	block, err := db.BlockAtOrAfterIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, blocks[1].ID, block.ID)
	require.Equal(t, blocks[1].Hash, block.Hash)

	_, err = db.BlockAtOrAfterIndex(blocks[1].ID+1, nil)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

func TestBlockAtOrAfterIndexSkipsInvalidIndexMappings(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	olderBlock := testIndexedBlock(10, 1, 0x01)
	nextBlock := testIndexedBlock(30, 300, 0x03)
	require.NoError(t, db.BlockCreate(olderBlock, nil))
	require.NoError(t, db.BlockCreate(nextBlock, nil))

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		// This index resolves, but its target belongs to an older index.
		if err := db.Blob().Set(
			txn.Blob(),
			types.BlockBlobIndexKey(100),
			types.BlockBlobKey(olderBlock.Slot, olderBlock.Hash),
		); err != nil {
			return err
		}
		// This index points at a block that is no longer present.
		return db.Blob().Set(
			txn.Blob(),
			types.BlockBlobIndexKey(200),
			types.BlockBlobKey(20, bytes.Repeat([]byte{0x02}, 32)),
		)
	}))

	block, err := db.BlockAtOrAfterIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, nextBlock.ID, block.ID)
	require.Equal(t, nextBlock.Hash, block.Hash)
}

// TestBlockBeforeSlotSkipsSyntheticBlobs verifies BlockBeforeSlot returns the
// highest real ranking block before a slot and skips synthetic blobs. Genesis
// CBOR and Leios endorser blocks are persisted at block-blob keys via
// SetGenesisCbor with ID=0 and no chain index; returning one to the
// epoch-nonce lab computation saves a non-chain hash. Older PrevHash-based lab
// lookup also saved an empty lastEpochBlockNonce here, collapsing the new
// epoch's nonce to the NeutralNonce identity and failing leader-VRF checks.
func TestBlockBeforeSlotSkipsSyntheticBlobs(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	realBlock := models.Block{
		ID:       7,
		Slot:     100,
		Hash:     bytes.Repeat([]byte{0xaa}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   7,
		Type:     6,
	}
	require.NoError(t, db.BlockCreate(realBlock, nil))

	// Synthetic endorser-block blob at a HIGHER slot than the real block but
	// still before the query slot. SetGenesisCbor stores it with ID=0 and a
	// nil PrevHash.
	ebHash := bytes.Repeat([]byte{0xcc}, 32)
	require.NoError(t, db.SetGenesisCbor(110, ebHash, []byte{0x80}, nil))

	got, err := BlockBeforeSlot(db, 120)
	require.NoError(t, err)
	require.Equal(
		t,
		realBlock.Slot,
		got.Slot,
		"BlockBeforeSlot must skip the synthetic blob at slot 110 and return "+
			"the real ranking block at slot 100",
	)
	require.Equal(t, realBlock.Hash, got.Hash)
	require.Equal(
		t,
		realBlock.PrevHash,
		got.PrevHash,
		"the real block's PrevHash must survive for chain continuity",
	)
}

// TestBlockBeforeSlotSyntheticOnlyNotFound verifies that when only synthetic
// blobs precede the slot (no real ranking block), BlockBeforeSlot reports
// ErrBlockNotFound rather than returning a synthetic blob.
func TestBlockBeforeSlotSyntheticOnlyNotFound(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	ebHash := bytes.Repeat([]byte{0xcc}, 32)
	require.NoError(t, db.SetGenesisCbor(110, ebHash, []byte{0x80}, nil))

	_, err := BlockBeforeSlot(db, 120)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestBlockByNumberResolvesEveryIndexedBlock pins the height-identifier
// lookup the bark archive service needs: block numbers are not indexed in
// the blob store, so the resolution is a binary search over the block-ID
// space and every number in the chain must come back as its own block.
func TestBlockByNumberResolvesEveryIndexedBlock(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	blocks := make([]models.Block, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	for _, want := range blocks {
		got, err := BlockByNumber(db, want.Number)
		require.NoError(t, err)
		require.Equal(t, want.ID, got.ID)
		require.Equal(t, want.Slot, got.Slot)
		require.True(t, bytes.Equal(want.Hash, got.Hash))
	}
}

// TestBlockByNumberSkipsSparseIndexGap proves the search tolerates gaps in
// the block-ID space, which a Mithril bootstrap or drain import leaves
// behind, rather than treating a missing probe as the end of the range. A
// target above the gap is the discriminating case: a fallback that merely
// shrinks the upper bound on a missing probe converges into the low range
// and never finds it.
func TestBlockByNumberSkipsSparseIndexGap(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ids := []uint64{1, 2, 3, 1000, 1001, 1002}
	blocks := make([]models.Block, 0, len(ids))
	for i, id := range ids {
		// #nosec G115 -- fixed small test fixture values.
		block := testIndexedBlock(id*10, id, byte(i+1))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	below, err := BlockByNumber(db, blocks[1].Number)
	require.NoError(t, err)
	require.Equal(t, blocks[1].ID, below.ID)

	above, err := BlockByNumber(db, blocks[4].Number)
	require.NoError(t, err)
	require.Equal(t, blocks[4].ID, above.ID)
}

// TestResolveBlockNumberBoundIsSeparableFromTheSearch pins the split that
// keeps a batch of block-number lookups from re-reading the chain tip once
// per number. Resolving the bound is a reverse iteration over the block
// index, which the s3 and gcs plugins answer by listing every block-index
// object in the bucket, so the bound is resolved by the caller and carried
// into each search.
func TestResolveBlockNumberBoundIsSeparableFromTheSearch(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	empty, err := ResolveBlockNumberBound(db)
	require.NoError(t, err)
	require.False(t, empty.Resolved, "an empty chain bounds nothing")

	blocks := make([]models.Block, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	bound, err := ResolveBlockNumberBound(db)
	require.NoError(t, err)
	require.True(t, bound.Resolved)
	require.Equal(t, blocks[4].ID, bound.HighestID)
	require.Equal(t, blocks[4].Number, bound.HighestNumber)

	// One bound answers every number in the chain.
	for _, want := range blocks {
		got, err := BlockByNumberBounded(db, want.Number, bound)
		require.NoError(t, err)
		require.Equal(t, want.ID, got.ID)
		require.Equal(t, want.Slot, got.Slot)
		require.True(t, bytes.Equal(want.Hash, got.Hash))
	}

	_, err = BlockByNumberBounded(db, blocks[4].Number+1, bound)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestBlockByNumberResolvesCompactBlockMetadata pins the height lookup
// against the other block-metadata encoding. The badger plugin writes a
// compact binary value instead of CBOR for run mode "serve" or "leios"
// with storage mode "core", and the search reads that value directly
// rather than through GetBlock, so decoding it as CBOR would fail every
// height lookup and every bound resolution on exactly the configurations
// a production node runs.
func TestBlockByNumberResolvesCompactBlockMetadata(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabaseWithRunMode(
		t,
		&Config{DataDir: "", StorageMode: types.StorageModeCore},
		"serve",
	)
	require.NoError(t, err)

	blocks := make([]models.Block, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}

	// The stored value really is the compact encoding, not CBOR -- without
	// this the test would still pass if the plugin silently fell back.
	txn := db.BlobTxn(false)
	t.Cleanup(func() { _ = txn.Rollback() })
	raw, err := db.Blob().Get(
		txn.Blob(),
		types.BlockBlobMetadataKey(
			types.BlockBlobKey(blocks[0].Slot, blocks[0].Hash),
		),
	)
	require.NoError(t, err)
	require.True(
		t,
		bytes.HasPrefix(raw, types.BlockMetadataBinaryMagic[:]),
		"expected compact block metadata for run mode serve and storage mode core",
	)

	bound, err := ResolveBlockNumberBound(db)
	require.NoError(t, err)
	require.True(t, bound.Resolved)
	require.Equal(t, blocks[4].ID, bound.HighestID)
	require.Equal(t, blocks[4].Number, bound.HighestNumber)

	for _, want := range blocks {
		got, err := BlockByNumber(db, want.Number)
		require.NoError(t, err)
		require.Equal(t, want.ID, got.ID)
		require.Equal(t, want.Slot, got.Slot)
		require.True(t, bytes.Equal(want.Hash, got.Hash))
	}
}

// TestBlockByNumberBoundedRejectsUnresolvedBound pins the fail-closed zero
// value: a caller that never resolved a bound must get ErrBlockNotFound
// rather than a search over an ID space the bound says is empty, which
// would report the same thing for the wrong reason.
func TestBlockByNumberBoundedRejectsUnresolvedBound(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	for i := uint64(1); i <= 3; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
	}

	_, err := BlockByNumberBounded(db, 1, BlockNumberBound{})
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestBlockByNumberReportsMissingNumbersAsNotFound proves an absent height
// is reported as models.ErrBlockNotFound rather than a generic error, which
// is what lets the bark archive service classify it as a not_found
// reference instead of failing the whole batch.
func TestBlockByNumberReportsMissingNumbersAsNotFound(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	_, err := BlockByNumber(db, 1)
	require.ErrorIs(t, err, models.ErrBlockNotFound)

	for i := uint64(1); i <= 3; i++ {
		block := testIndexedBlock(i*10, i, byte(i))
		require.NoError(t, db.BlockCreate(block, nil))
	}

	_, err = BlockByNumber(db, 99)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}
