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
