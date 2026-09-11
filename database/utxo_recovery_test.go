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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
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

// TestResolveUtxoCborWithRecoveryUpgradesMetadataOnlyTxnForRecovery is the
// regression test for a cubic review finding on PR #4084: the mirror image
// of the blob-only case above. A metadata-only Txn (Blob() == nil) hitting
// a missing blob was passed straight into recoverUtxoCbor with no blob
// handle at all, so utxoRecoveryBlockForTx's block lookup
// (BlockByPointTxn) returned ErrNilTxn instead of reconstructing the CBOR
// -- recovery failed even though the metadata needed to locate the
// producing block was right there.
func TestResolveUtxoCborWithRecoveryUpgradesMetadataOnlyTxnForRecovery(
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
	// Also delete the tx-offset blob entry so the blob-based lookup misses
	// and utxoRecoveryBlockForTx falls to the metadata-based path -- the
	// one this test means to exercise. See the sibling blob-only test's
	// identical comment.
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	metadataOnlyTxn := db.MetadataTxn(false)
	defer metadataOnlyTxn.Release()
	require.Nil(
		t, metadataOnlyTxn.Blob(),
		"test setup must reproduce a genuinely metadata-only txn",
	)

	recovered, err := db.ResolveUtxoCborWithRecovery(
		txId, outputIdx, metadataOnlyTxn,
	)
	require.NoError(
		t, err,
		"recovery must succeed even when the caller's txn has no blob "+
			"handle",
	)
	wantCbor := utxo.Output.Cbor()
	require.NotEmpty(t, wantCbor, "fixture output must carry its own CBOR")
	require.Equal(t, []byte(wantCbor), recovered)
}

// TestResolveUtxoCborWithRecoveryMetadataOnlyWriteCapableCallerPersistsRepair
// is the regression test for a cubic review finding on PR #4084:
// withBlobForRecovery copied t.readWrite into aug.readWrite, so a
// write-capable metadata-only caller made aug's freshly-opened blobTxn
// write-capable too. repairUtxoBlob then took its "use the caller's own
// blob txn" branch (txn.IsReadWrite() true) and wrote the recovered offset
// into aug.blobTxn expecting the caller to eventually commit it -- but
// aug.blobTxn is never the caller's own handle and is always torn down by
// ResolveUtxoCborWithRecovery's deferred cleanup (aug.Release, which only
// ever rolls back), discarding the repair every time regardless of
// whether the caller's own txn ever committed.
//
// Proves the fix: even with a write-capable metadata-only caller, the
// repaired offset is durably persisted in the blob store once
// ResolveUtxoCborWithRecovery returns -- because withBlobForRecovery now
// forces aug.readWrite false, routing the write through repairUtxoBlob's
// independent-writer branch, which commits on its own.
func TestResolveUtxoCborWithRecoveryMetadataOnlyWriteCapableCallerPersistsRepair(
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
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	// Write-capable, unlike the sibling upgrade test above -- this is the
	// caller shape the finding is about.
	metadataOnlyTxn := db.MetadataTxn(true)
	defer metadataOnlyTxn.Release()
	require.Nil(
		t, metadataOnlyTxn.Blob(),
		"test setup must reproduce a genuinely metadata-only txn",
	)
	require.True(
		t, metadataOnlyTxn.IsReadWrite(),
		"test setup must reproduce a genuinely write-capable caller",
	)

	_, err = db.ResolveUtxoCborWithRecovery(txId, outputIdx, metadataOnlyTxn)
	require.NoError(t, err, "UTxO must recover successfully")

	checkTxn := blob.NewTransaction(false)
	defer checkTxn.Rollback() //nolint:errcheck
	repaired, err := blob.GetUtxo(checkTxn, txId, outputIdx)
	require.NoError(
		t, err,
		"repair must be durably committed to the blob store, not "+
			"discarded by aug's deferred rollback",
	)
	require.NotEmpty(t, repaired)
}

// TestResolveUtxoCborWithRecoverySharedBlobRollbackDoesNotFinishCallersTxn
// is the regression test for a chrisguiney review finding on PR #4084: the
// !t.sharedBlob guard added to Txn.rollback() (see withMetadataForRecovery)
// was load-bearing but untested -- every existing recovery test resolves
// only one row per caller txn, so removing the guard still left
// go test ./database/... ./ledger/... green.
//
// queryShelleyUtxoWhole's worker pool reuses one BlobTxn(false) across every
// job it handles. Without the guard, recovering the first row's missing
// blob calls withMetadataForRecovery, which shares the caller's blobTxn;
// releasing that augmented Txn afterward would then roll back -- and so
// finish -- the underlying provider transaction the caller's own txn still
// points at, failing every resolve attempted through it afterward.
//
// Reproduces that shape directly: two independent UTxOs resolved through
// the same blob-only Txn, the first needing recovery and the second not.
func TestResolveUtxoCborWithRecoverySharedBlobRollbackDoesNotFinishCallersTxn(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	// UTxO A: recoverable -- blob and tx-offset entries deleted below, so
	// resolving it requires reconstructing from the producing block.
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

	producedA := producer.tx.Produced()
	require.NotEmpty(t, producedA)
	utxoA := producedA[0]
	txIdA := utxoA.Id.Id().Bytes()
	outputIdxA := utxoA.Id.Index()

	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txIdA, outputIdxA),
	)
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txIdA),
	)

	// UTxO B: independent and blob-intact -- resolves via the bare
	// tiered-cache path, no recovery involved. Seeded through the same
	// write txn as A's deletions above, committed together below.
	txIdB := bytes.Repeat([]byte{0xB2}, 32)
	const outputIdxB = uint32(0)
	require.NoError(t, db.CreateUtxo(writeTxn, &models.Utxo{
		TxId:      txIdB,
		OutputIdx: outputIdxB,
		AddedSlot: 100,
	}))
	wantCborB := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	require.NoError(
		t,
		blob.SetUtxo(writeTxn.Blob(), txIdB, outputIdxB, wantCborB),
	)
	require.NoError(t, writeTxn.Commit())

	callerTxn := db.BlobTxn(false)
	defer callerTxn.Release()
	require.Nil(
		t, callerTxn.Metadata(),
		"test setup must reproduce a genuinely blob-only txn",
	)

	_, err = db.ResolveUtxoCborWithRecovery(txIdA, outputIdxA, callerTxn)
	require.NoError(t, err, "UTxO A must recover successfully")

	recoveredB, err := db.ResolveUtxoCborWithRecovery(
		txIdB, outputIdxB, callerTxn,
	)
	require.NoError(
		t, err,
		"resolving B through the same caller txn afterward must still "+
			"succeed -- recovering A must not have finished the shared "+
			"underlying blob transaction",
	)
	require.Equal(t, wantCborB, recoveredB)
}

// TestResolveUtxoCborWithRecoverySharedMetadataRollbackDoesNotFinishCallersTxn
// is the regression test for a cubic review finding on PR #4084:
// withBlobForRecovery's aug borrows the caller's metadataTxn (the mirror
// image of withMetadataForRecovery's borrowed blobTxn) but an earlier
// version left sharedMetadata unset. Releasing aug after recovery then
// rolled back -- and so finished -- the caller's own metadata transaction,
// discarding a write-capable caller's uncommitted metadata as a side
// effect of a call that only meant to add blob access for one recovery.
//
// Proves the caller's own metadata handle is still usable after recovery
// completes: a plain read through metadataOnlyTxn.Metadata() must not fail
// with "transaction already finished".
func TestResolveUtxoCborWithRecoverySharedMetadataRollbackDoesNotFinishCallersTxn(
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
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	metadataOnlyTxn := db.MetadataTxn(false)
	defer metadataOnlyTxn.Release()
	require.Nil(
		t, metadataOnlyTxn.Blob(),
		"test setup must reproduce a genuinely metadata-only txn",
	)

	_, err = db.ResolveUtxoCborWithRecovery(txId, outputIdx, metadataOnlyTxn)
	require.NoError(t, err, "UTxO must recover successfully")

	_, err = db.Metadata().GetTransactionByHash(
		txId, metadataOnlyTxn.Metadata(),
	)
	require.NoError(
		t, err,
		"using the caller's own metadata handle after recovery must "+
			"still succeed -- recovering must not have finished the "+
			"shared underlying metadata transaction",
	)
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

	// Registered now, before newStore/drain exist below, via forward
	// references closed over by the cleanup funcs -- so a require failure
	// anywhere below (e.g. the pinned-store fix regressing) still releases
	// callerTxn's pin and drains/closes both stores instead of leaking
	// them past FailNow, which would otherwise leave callerTxn's pin held
	// and mask the real failure behind a confusing hang or close error.
	// t.Cleanup runs strictly after this function's own defers (checkTxn/
	// newCheckTxn's Rollback below), and in the reverse of registration
	// order, so registering newStore/originalStore's Close before drain's
	// call before callerTxn's Release here gives the needed teardown
	// order: release the pin, then drain, then close each store.
	var (
		newStore blob.BlobStore
		drain    func()
	)
	t.Cleanup(func() {
		if newStore != nil {
			require.NoError(t, newStore.Close())
		}
	})
	t.Cleanup(func() {
		require.NoError(t, originalStore.Close())
	})
	t.Cleanup(func() {
		if drain != nil {
			drain()
		}
	})
	t.Cleanup(callerTxn.Release)

	// Confirm the scenario actually exercises recovery (and so the repair
	// write-back this test asserts on): the bare tiered-cache resolve must
	// miss first, same premise check as the sibling recovery tests. Without
	// this, a tiered-cache entry surviving from setup would let
	// ResolveUtxoCborWithRecovery return early on the cache hit, and this
	// test would instead fail confusingly at the originalStore.GetUtxo
	// check below rather than testing the pinned-store repair path.
	_, err = db.CborCache().ResolveUtxoCbor(txId, outputIdx, callerTxn)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound,
		"test setup must reproduce a genuinely missing blob")

	// Install a second, empty store -- simulating a blob-store rotation
	// (e.g. bark) happening concurrently with this in-flight recovery.
	// Sized down from badger's defaults (see TestBadgerValueLogFileSize's
	// doc comment): the default reserves 2GiB per store on Windows, which
	// a CI runner opening several of these concurrently can exhaust.
	newStore, err = badger.New(
		badger.WithDataDir(t.TempDir()),
		badger.WithValueLogFileSize(testutil.TestBadgerValueLogFileSize),
		badger.WithMemTableSize(testutil.TestBadgerMemTableSize),
	)
	require.NoError(t, err)
	var prev blob.BlobStore
	prev, drain = db.SetBlobStore(newStore)
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
