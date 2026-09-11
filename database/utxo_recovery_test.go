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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestResolveUtxoCborWithRecoveryReconstructsMissingBlob is a regression
// test for a bot-review finding on blinklabs-io/dingo#4082's worker-pool
// fix (PR #4084): ledger.queryShelleyUtxoWhole switched from
// IterateLiveUtxos' inline loadCbor (which recovers a missing blob from the
// producing block via recoverUtxoCbor) to a bare CborCache().ResolveUtxoCbor
// call that silently dropped the row on ErrBlobKeyNotFound instead. This
// proves the replacement, ResolveUtxoCborWithRecovery, actually performs the
// same recovery loadCbor did rather than only wrapping the bare resolve.
func TestResolveUtxoCborWithRecoveryReconstructsMissingBlob(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	// Write the block, blob offsets, and metadata rows directly -- bypassing
	// Database.SetTransaction/SetGapBlockTransaction -- the same bypass
	// TestSetTransactionRecoveryPopulatesProducerFK uses, so the produced
	// UTxO is live with an offset reference before its blob entry is
	// deleted below.
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(),
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	// Simulate a blob gone missing for an otherwise still-live UTxO (the
	// scenario recoverUtxoCbor exists for): delete just this one output's
	// blob entry while its metadata row (written by seedLiveProducerForWarmTest
	// via SetGapBlockTransaction) stays live.
	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	require.NoError(t, writeTxn.Commit())

	// Confirm the scenario actually exercises the fallback: the bare
	// tiered-cache resolve must miss first.
	_, err = db.CborCache().ResolveUtxoCbor(txId, outputIdx, nil)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound,
		"test setup must reproduce a genuinely missing blob")

	recovered, err := db.ResolveUtxoCborWithRecovery(txId, outputIdx, nil)
	require.NoError(t, err, "a missing-but-reconstructable blob must recover")
	require.NotEmpty(t, recovered)

	wantCbor := utxo.Output.Cbor()
	require.NotEmpty(t, wantCbor, "fixture output must carry its own CBOR")
	require.Equal(t, []byte(wantCbor), recovered)
}

// TestResolveUtxoCborWithRecoveryUpgradesBlobOnlyTxnForRecovery covers a
// human-review finding on PR #4084: a caller resolving many refs
// concurrently (queryShelleyUtxoWhole's worker pool) passes a blob-only
// *Txn (BlobTxn, Metadata() == nil) so the resolve hot path never holds a
// metadata connection from the shared read pool. This proves recovery's
// metadata-based fallback (utxoRecoveryBlockForTx, once the blob-based tx
// lookup misses) still succeeds correctly end-to-end against that
// blob-only txn, rather than failing or panicking for lack of a metadata
// handle -- ResolveUtxoCborWithRecovery's own explicit on-demand upgrade
// makes this guarantee independent of the metadata store's
// GetTransactionByHash also separately tolerating a nil types.Txn
// (confirmed: this test still passes with that explicit upgrade removed,
// since GetTransactionByHash falls back to its own ad-hoc pooled
// connection either way -- see ResolveUtxoCborWithRecovery's doc comment
// for why the explicit upgrade is kept anyway).
func TestResolveUtxoCborWithRecoveryUpgradesBlobOnlyTxnForRecovery(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(),
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	// Also delete the tx-offset blob entry storeBlockOffsetsOnly wrote:
	// utxoRecoveryBlockForTx tries the blob-based tx lookup
	// (fetchTxBlobSlotAndHash) first, and it would otherwise satisfy
	// recovery without ever reaching the metadata-based fallback this test
	// means to exercise.
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	blobOnlyTxn := db.BlobTxn(false)
	defer blobOnlyTxn.Release()
	require.Nil(
		t, blobOnlyTxn.Metadata(),
		"test setup must reproduce a genuinely blob-only txn",
	)

	recovered, err := db.ResolveUtxoCborWithRecovery(
		txId, outputIdx, blobOnlyTxn,
	)
	require.NoError(
		t, err,
		"recovery must succeed even when the caller's txn has no "+
			"metadata handle",
	)
	wantCbor := utxo.Output.Cbor()
	require.NotEmpty(t, wantCbor, "fixture output must carry its own CBOR")
	require.Equal(t, []byte(wantCbor), recovered)
}

// TestResolveUtxoCborWithRecoveryPropagatesUnrecoverable proves a UTxO whose
// producing block cannot be located at all (recovery itself fails) surfaces
// ErrUtxoCborUnavailable rather than being silently treated as resolved --
// this is the sentinel ledger.queryShelleyUtxoWhole's worker loop checks to
// decide whether to still omit a row after actually trying recovery, versus
// the pre-fix behavior of omitting on the bare ErrBlobKeyNotFound alone.
func TestResolveUtxoCborWithRecoveryPropagatesUnrecoverable(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	txId := make([]byte, 32)
	txId[0] = 0xEE
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(itxn *Txn) error {
		return db.CreateUtxo(itxn, &models.Utxo{
			TxId:      txId,
			OutputIdx: 0,
			AddedSlot: 1,
		})
	}))
	txn.Release()

	_, err = db.ResolveUtxoCborWithRecovery(txId, 0, nil)
	require.True(t, errors.Is(err, ErrUtxoCborUnavailable),
		"a UTxO with no indexed producer block must report unavailable, not succeed silently")
}

// TestRepairUtxoBlobWritesThroughCallersPinnedStore is a regression test for
// a cubic finding on PR #4084: ResolveUtxoCborWithRecovery's block lookup was
// fixed to read through the caller's already-pinned blob store rather than
// whatever store is *currently* installed (see Txn.withMetadataForRecovery),
// but repairUtxoBlob's write-back still opened a fresh NewBlobOnlyTxn, which
// re-pins the current store -- splitting one logical recovery into a read
// against one store and a repair write into a different one whenever a
// SetBlobStore swap lands in between.
//
// Proves the fix end-to-end: install a second, empty store between the read
// and the repair, and confirm the repaired offset lands in the store the
// caller was actually pinned to (the one the block lookup used, and the only
// one that has the producer block data at all), not the newly-installed one.
func TestRepairUtxoBlobWritesThroughCallersPinnedStore(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	originalStore := db.Blob()
	require.NotNil(t, originalStore)

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	// Written against the original store, before any swap below.
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(),
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	// Delete just this output's blob entry so the resolve misses and
	// recovery kicks in, same setup as the sibling recovery tests.
	writeTxn := db.Transaction(true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		originalStore.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	require.NoError(t, writeTxn.Commit())

	// Open a read-only Txn against the original store *before* the swap --
	// this is what pins ResolveUtxoCborWithRecovery to the original store
	// for the whole call, the same as a real caller already holding a txn
	// opened earlier than a concurrent SetBlobStore.
	callerTxn := db.BlobTxn(false)
	require.True(
		t, originalStore == callerTxn.BlobStore(),
		"test setup must pin the caller's txn to the original store",
	)

	// Install a second, empty store -- simulating a blob-store rotation
	// (e.g. bark) happening concurrently with this in-flight recovery.
	newStore, err := badger.New(badger.WithDataDir(t.TempDir()))
	require.NoError(t, err)
	prev, drain := db.SetBlobStore(newStore)
	require.True(t, originalStore == prev)

	recovered, err := db.ResolveUtxoCborWithRecovery(
		txId, outputIdx, callerTxn,
	)
	require.NoError(
		t, err,
		"recovery must succeed by reading the caller's pinned (original) "+
			"store, even though a different store is now installed",
	)
	require.NotEmpty(t, recovered)

	// Release the caller's pin before draining: drain waits for every pin
	// on the retired (original) store to clear, and callerTxn is the one
	// still holding it.
	callerTxn.Release()
	drain()
	// db.Close() does not close installed blob stores (it only stops the
	// metrics goroutine), and originalStore is no longer the database's
	// installed store once SetBlobStore swapped it out -- so it is this
	// test's own responsibility to close it, the same as newStore below.
	// Declared before the read-only check transactions' own defers so it
	// runs after them (LIFO): those still need originalStore open.
	defer func() {
		require.NoError(t, originalStore.Close())
	}()
	defer func() {
		require.NoError(t, newStore.Close())
	}()

	// The repair write-back must have landed in the *original* store...
	checkTxn := originalStore.NewTransaction(false)
	defer checkTxn.Rollback() //nolint:errcheck
	repaired, err := originalStore.GetUtxo(checkTxn, txId, outputIdx)
	require.NoError(
		t, err,
		"repair must have written the offset back into the original store",
	)
	require.NotEmpty(t, repaired)

	// ...and must not have landed in the newly-installed store, which would
	// only happen if repairUtxoBlob re-pinned the current store instead of
	// reusing the caller's.
	newCheckTxn := newStore.NewTransaction(false)
	defer newCheckTxn.Rollback() //nolint:errcheck
	_, err = newStore.GetUtxo(newCheckTxn, txId, outputIdx)
	require.ErrorIs(
		t, err, types.ErrBlobKeyNotFound,
		"repair must not write into the newly-installed store",
	)
}
