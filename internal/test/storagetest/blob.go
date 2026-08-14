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

// Package storagetest is a shared conformance suite for blob.BlobStore and
// metadata.MetadataStore implementations. Each storage plugin gets a thin
// in-package test file that constructs its own store and hands it to
// RunBlobStoreConformance or RunMetadataStoreConformance, so every backend is
// checked against the same behavioral contract instead of each plugin
// inventing its own CRUD test shape. See
// database/plugin/PLUGIN_DEVELOPMENT.md for how to wire a new plugin's
// conformance test.
package storagetest

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// RunBlobStoreConformance exercises the backend-neutral contract documented
// on blob.BlobStore against newStore(). newStore is called once; the
// returned store is reused (sequentially, except where a subtest documents
// concurrent use) across every subtest so the suite works unmodified against
// backends where construction is expensive (a real S3/GCS bucket).
//
// The suite only asserts behavior the interface doc comments and
// DATABASE.md's Cross-Store Durability Contract commit every implementation
// to. It deliberately does not assert visibility ordering between
// concurrent, uncommitted transactions: badger's snapshot isolation, SQL
// locking, and the cloud plugins' staged-commit model disagree on that, and
// none of blob.BlobStore's doc comments promise a specific answer.
func RunBlobStoreConformance(
	t *testing.T,
	newStore func(t *testing.T) blob.BlobStore,
) {
	t.Helper()
	store := newStore(t)

	t.Run("KVRoundTrip", func(t *testing.T) {
		key := conformanceKey(t, "kv")
		val := []byte("conformance-value")

		writeTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(writeTxn, key, val))
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		got, err := store.Get(readTxn, key)
		require.NoError(t, err)
		require.Equal(t, val, got)
	})

	t.Run("SetOverwritesExistingKey", func(t *testing.T) {
		key := conformanceKey(t, "update")

		firstTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(firstTxn, key, []byte("first-value")))
		require.NoError(t, firstTxn.Commit())

		secondTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(secondTxn, key, []byte("second-value")))
		require.NoError(t, secondTxn.Commit())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		got, err := store.Get(readTxn, key)
		require.NoError(t, err)
		require.Equal(t, []byte("second-value"), got)
	})

	t.Run("SetEmptyValueIsNotADeletion", func(t *testing.T) {
		// DATABASE.md's Cross-Store Durability Contract: "A staged
		// zero-length write is a value, not a deletion: Set with an empty
		// slice reads back as an empty blob inside the transaction and the
		// key is still listed by iterators." A staged-write implementation
		// that used a nil/empty value as its own "this key is deleted"
		// sentinel would violate this by conflating the two.
		//
		// The key already exists (committed with a non-empty value) before
		// being staged to empty: DATABASE.md separately documents that a
		// cloud plugin's iterator lists directly from the bucket and does
		// not merge in keys staged for writing that the bucket does not
		// have yet, so a key with no committed existence at all is never
		// the right fixture for "still listed" -- it would fail for a
		// reason unrelated to the empty-vs-deleted distinction this subtest
		// targets.
		key := conformanceKey(t, "empty-value")
		seedTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(seedTxn, key, []byte("not-empty-yet")))
		require.NoError(t, seedTxn.Commit())

		writeTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(writeTxn, key, []byte{}))
		// Same-transaction visibility, before commit.
		gotStaged, err := store.Get(writeTxn, key)
		require.NoError(t, err)
		require.Empty(t, gotStaged)
		iter := store.NewIterator(
			writeTxn,
			types.BlobIteratorOptions{Prefix: key},
		)
		iter.Rewind()
		require.True(t, iter.Valid(), "empty value must still be listed")
		iter.Close()
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		got, err := store.Get(readTxn, key)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("GetMissingKeyReturnsErrBlobKeyNotFound", func(t *testing.T) {
		key := conformanceKey(t, "missing")
		txn := store.NewTransaction(false)
		defer func() { require.NoError(t, txn.Rollback()) }()
		_, err := store.Get(txn, key)
		require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	})

	t.Run("DeleteRemovesKey", func(t *testing.T) {
		key := conformanceKey(t, "delete")
		writeTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(writeTxn, key, []byte("x")))
		require.NoError(t, writeTxn.Commit())

		deleteTxn := store.NewTransaction(true)
		require.NoError(t, store.Delete(deleteTxn, key))
		require.NoError(t, deleteTxn.Commit())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		_, err := store.Get(readTxn, key)
		require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	})

	t.Run("NilTransactionRejected", func(t *testing.T) {
		key := conformanceKey(t, "nil-txn")
		_, err := store.Get(nil, key)
		require.ErrorIs(t, err, types.ErrNilTxn)
		require.ErrorIs(t, store.Set(nil, key, []byte("x")), types.ErrNilTxn)
		require.ErrorIs(t, store.Delete(nil, key), types.ErrNilTxn)
	})

	t.Run("ReadYourOwnWriteWithinTransaction", func(t *testing.T) {
		key := conformanceKey(t, "read-your-write")
		val := []byte("staged-or-uncommitted")
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, key, val))
		got, err := store.Get(txn, key)
		require.NoError(t, err)
		require.Equal(t, val, got)
		require.NoError(t, txn.Rollback())
	})

	t.Run("RollbackDiscardsUncommittedWrites", func(t *testing.T) {
		key := conformanceKey(t, "rollback")
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, key, []byte("never-committed")))
		require.NoError(t, txn.Rollback())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		_, err := store.Get(readTxn, key)
		require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	})

	t.Run("CommitTimestampRoundTrip", func(t *testing.T) {
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetCommitTimestamp(424242, txn))
		require.NoError(t, txn.Commit())

		got, err := store.GetCommitTimestamp()
		require.NoError(t, err)
		require.Equal(t, int64(424242), got)
	})

	t.Run("BlockRoundTrip", func(t *testing.T) {
		slot := uint64(100)
		hash := conformanceKey(t, "block-hash")
		cbor := []byte{0x82, 0x01, 0x02}

		writeTxn := store.NewTransaction(true)
		require.NoError(
			t,
			store.SetBlock(writeTxn, slot, hash, cbor, 1, 0, 7, nil),
		)
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		gotCbor, gotMeta, err := store.GetBlock(readTxn, slot, hash)
		require.NoError(t, err)
		require.Equal(t, cbor, gotCbor)
		require.Equal(t, uint64(1), gotMeta.ID)
		require.Equal(t, uint64(7), gotMeta.Height)
		require.NoError(t, readTxn.Rollback())

		deleteTxn := store.NewTransaction(true)
		require.NoError(t, store.DeleteBlock(deleteTxn, slot, hash, 1))
		require.NoError(t, deleteTxn.Commit())

		verifyTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, verifyTxn.Rollback()) }()
		_, _, err = store.GetBlock(verifyTxn, slot, hash)
		require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	})

	t.Run("GetBlockURLOnMissingBlockFailsCleanly", func(t *testing.T) {
		// GetBlockURL is only meaningfully implemented by the cloud plugins
		// (badger unconditionally errors "not supported"), so this only
		// asserts what every implementation actually shares: called on a
		// block that was never written, it returns an error rather than
		// panicking or hanging, and does so within the timeout bound. The
		// happy path (a committed block signs successfully) and the
		// staged-but-uncommitted contract are backend-specific -- see
		// TestBlobStoreGetBlockURLSignsCommittedBlock and
		// TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock in the aws
		// and gcs packages.
		hash := conformanceKey(t, "block-url-missing")
		start := time.Now()
		txn := store.NewTransaction(false)
		defer func() { require.NoError(t, txn.Rollback()) }()
		_, _, err := store.GetBlockURL(
			t.Context(),
			txn,
			ocommon.Point{Slot: 999_000, Hash: hash},
		)
		require.Error(t, err)
		require.Less(t, time.Since(start), 10*time.Second)
	})

	t.Run("TombstoneBlockReturnsErrHistoryExpired", func(t *testing.T) {
		slot := uint64(200)
		hash := conformanceKey(t, "tombstone-hash")
		cbor := []byte{0x82, 0x03, 0x04}

		writeTxn := store.NewTransaction(true)
		require.NoError(
			t,
			store.SetBlock(writeTxn, slot, hash, cbor, 2, 0, 8, nil),
		)
		require.NoError(t, writeTxn.Commit())

		tombstoneTxn := store.NewTransaction(true)
		require.NoError(t, store.TombstoneBlock(tombstoneTxn, slot, hash))
		require.NoError(t, tombstoneTxn.Commit())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		_, _, err := store.GetBlock(readTxn, slot, hash)
		require.ErrorIs(t, err, types.ErrHistoryExpired)
		var expired *types.HistoryExpiredError
		require.ErrorAs(t, err, &expired)
		require.Equal(t, slot, expired.Slot)
		require.Equal(t, hash, expired.Hash)
	})

	t.Run("UtxoRoundTrip", func(t *testing.T) {
		txID := conformanceKey(t, "utxo-tx")
		outputIdx := uint32(3)
		cbor := []byte{0x81, 0x05}

		writeTxn := store.NewTransaction(true)
		require.NoError(t, store.SetUtxo(writeTxn, txID, outputIdx, cbor))
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		got, err := store.GetUtxo(readTxn, txID, outputIdx)
		require.NoError(t, err)
		require.Equal(t, cbor, got)
		require.NoError(t, readTxn.Rollback())

		deleteTxn := store.NewTransaction(true)
		require.NoError(t, store.DeleteUtxo(deleteTxn, txID, outputIdx))
		require.NoError(t, deleteTxn.Commit())

		verifyTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, verifyTxn.Rollback()) }()
		_, err = store.GetUtxo(verifyTxn, txID, outputIdx)
		require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	})

	t.Run("TxRoundTrip", func(t *testing.T) {
		txHash := conformanceKey(t, "tx-hash")
		offsetData := []byte{0x01, 0x02, 0x03, 0x04}

		writeTxn := store.NewTransaction(true)
		require.NoError(t, store.SetTx(writeTxn, txHash, offsetData))
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		got, err := store.GetTx(readTxn, txHash)
		require.NoError(t, err)
		require.Equal(t, offsetData, got)
		require.NoError(t, readTxn.Rollback())

		deleteTxn := store.NewTransaction(true)
		require.NoError(t, store.DeleteTx(deleteTxn, txHash))
		require.NoError(t, deleteTxn.Commit())

		verifyTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, verifyTxn.Rollback()) }()
		_, err = store.GetTx(verifyTxn, txHash)
		require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	})

	t.Run("IteratorEnumeratesWrittenKeys", func(t *testing.T) {
		prefix := conformanceKey(t, "iter")
		want := map[string][]byte{}
		writeTxn := store.NewTransaction(true)
		for i := range 5 {
			key := fmt.Appendf(nil, "%s/%02d", prefix, i)
			val := fmt.Appendf(nil, "value-%02d", i)
			require.NoError(t, store.Set(writeTxn, key, val))
			want[string(key)] = val
		}
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		iter := store.NewIterator(
			readTxn,
			types.BlobIteratorOptions{Prefix: prefix},
		)
		defer iter.Close()
		got := map[string][]byte{}
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			got[string(item.Key())] = value
		}
		require.NoError(t, iter.Err())
		require.Equal(t, want, got)
	})

	t.Run("ReverseIteratorEnumeratesKeysInDescendingOrder", func(t *testing.T) {
		// The cloud plugins take an entirely different code path for
		// Reverse than for forward iteration (a spooled, sorted key file
		// built up front, vs. streaming pages lazily from the bucket as
		// IteratorEnumeratesWrittenKeys above does) -- exercise it
		// explicitly rather than assuming symmetry with the forward case.
		prefix := conformanceKey(t, "reverse-iter")
		wantOrder := make([]string, 0, 5)
		writeTxn := store.NewTransaction(true)
		for i := range 5 {
			key := fmt.Appendf(nil, "%s/%02d", prefix, i)
			require.NoError(t, store.Set(writeTxn, key, []byte("v")))
			wantOrder = append(wantOrder, string(key))
		}
		require.NoError(t, writeTxn.Commit())
		slices.Reverse(wantOrder)

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		iter := store.NewIterator(
			readTxn,
			types.BlobIteratorOptions{Prefix: prefix, Reverse: true},
		)
		defer iter.Close()
		// Reverse iteration must Seek to a key past the prefix's range
		// (conventionally prefix+0xff, as database/block.go's own reverse
		// scans do) rather than Rewind: Rewind on a reverse iterator seeks
		// to the last key in the whole keyspace, not the last key with this
		// prefix, and the store here is shared across every other subtest
		// in this suite so that keyspace holds far more than this prefix's
		// own keys.
		seekKey := append(slices.Clone(prefix), 0xff)
		var gotOrder []string
		for iter.Seek(seekKey); iter.ValidForPrefix(prefix); iter.Next() {
			gotOrder = append(gotOrder, string(iter.Item().Key()))
		}
		require.NoError(t, iter.Err())
		require.Equal(t, wantOrder, gotOrder)
	})

	t.Run("ConcurrentWritesToDistinctKeysAllSucceed", func(t *testing.T) {
		const writers = 8
		prefix := conformanceKey(t, "concurrent")
		var wg sync.WaitGroup
		errs := make([]error, writers)
		for i := range writers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Appendf(nil, "%s/%02d", prefix, i)
				txn := store.NewTransaction(true)
				if err := store.Set(txn, key, []byte("ok")); err != nil {
					errs[i] = err
					return
				}
				errs[i] = txn.Commit()
			}(i)
		}
		wg.Wait()
		for i, err := range errs {
			require.NoErrorf(t, err, "writer %d", i)
		}

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		for i := range writers {
			key := fmt.Appendf(nil, "%s/%02d", prefix, i)
			got, err := store.Get(readTxn, key)
			require.NoError(t, err)
			require.Equal(t, []byte("ok"), got)
		}
	})

	t.Run("DiskSizeAndSyncSucceed", func(t *testing.T) {
		size, err := store.DiskSize()
		require.NoError(t, err)
		require.GreaterOrEqual(t, size, int64(0))
		require.NoError(t, store.Sync())
	})

	t.Run("LargePayloadRoundTrip", func(t *testing.T) {
		// 1MiB is small enough to keep the suite fast across every backend
		// (including real S3/GCS network round trips) while still large
		// enough to exercise a backend's buffering/spooling path instead of
		// the single-TCP-segment case every other subtest covers. A
		// repeating, non-uniform pattern (rather than all-zero bytes) still
		// catches a truncation or off-by-one bug that a compressible
		// all-zero payload could hide.
		const payloadSize = 1 << 20
		payload := make([]byte, payloadSize)
		for i := range payload {
			payload[i] = byte(i % 251)
		}
		key := conformanceKey(t, "large-kv")

		writeTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(writeTxn, key, payload))
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readTxn.Rollback()) }()
		got, err := store.Get(readTxn, key)
		require.NoError(t, err)
		require.Equal(t, payload, got)

		// Blocks are the largest values a real node ever stores through this
		// interface, so exercise the same payload through the block path
		// specifically rather than only the raw KV path above.
		blockHash := conformanceKey(t, "large-block-hash")
		writeBlockTxn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			writeBlockTxn,
			99_000,
			blockHash,
			payload,
			1,
			0,
			1,
			nil,
		))
		require.NoError(t, writeBlockTxn.Commit())

		readBlockTxn := store.NewTransaction(false)
		defer func() { require.NoError(t, readBlockTxn.Rollback()) }()
		gotBlock, _, err := store.GetBlock(readBlockTxn, 99_000, blockHash)
		require.NoError(t, err)
		require.Equal(t, payload, gotBlock)
	})

	t.Run("OperationsCompleteWithinTimeout", func(t *testing.T) {
		// Not a benchmark: a generous bound that only catches a genuine
		// hang (a leaked lock, an unbounded retry loop, a network call with
		// no deadline) rather than measuring throughput.
		const bound = 10 * time.Second
		key := conformanceKey(t, "timeout-bound")
		start := time.Now()

		writeTxn := store.NewTransaction(true)
		require.NoError(t, store.Set(writeTxn, key, []byte("bounded")))
		require.NoError(t, writeTxn.Commit())

		readTxn := store.NewTransaction(false)
		_, err := store.Get(readTxn, key)
		require.NoError(t, err)
		require.NoError(t, readTxn.Rollback())

		require.Less(
			t,
			time.Since(start),
			bound,
			"a commit+read pair took longer than %s; likely a hang rather "+
				"than a slow backend",
			bound,
		)
	})
}

// conformanceKey derives a key unique to t's subtest name and label so
// concurrently running conformance suites (e.g. one per backend, or a
// migration test composing two) never collide on the same logical key.
func conformanceKey(t *testing.T, label string) []byte {
	t.Helper()
	return []byte("storagetest:" + t.Name() + ":" + label)
}
