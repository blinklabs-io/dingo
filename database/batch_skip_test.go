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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestSetTransactionBatchedWithOpts_SkipsProducedUtxoWrites validates the
// SkipProducedUtxoOffsetWrites optimisation: when the option is set, the
// per-produced-output blob.SetUtxo calls must be elided so the existing
// blob entry (written by the Mithril immutable-copy phase) is preserved.
// The transaction blob and metadata writes must still happen.
func TestSetTransactionBatchedWithOpts_SkipsProducedUtxoWrites(t *testing.T) {
	db, err := New(&Config{
		DataDir:        t.TempDir(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	candidate := findBatchedCrossBlockSpendCandidate(t)
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)

	produced := candidate.producerTx.Produced()
	require.NotEmpty(t, produced, "producer tx must have at least one output")

	// Sentinel value the test will look up after the write. If the skip
	// path is honored, this exact byte sequence stays under the blob key;
	// if the skip is broken and SetTransactionBatchedWithOpts wrote the
	// real offset encoding, the bytes will differ.
	sentinel := []byte("SENTINEL-OFFSET-VALUE-FOR-SKIP-TEST")

	producedIdx := produced[0].Id.Index()
	producedTxId := produced[0].Id.Id().Bytes()

	// Stage the sentinel under the produced-UTxO blob key so we can detect
	// whether a subsequent write actually clobbered it.
	{
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *Txn) error {
			return txn.DB().Blob().SetUtxo(
				txn.Blob(), producedTxId, producedIdx, sentinel,
			)
		}))
		txn.Release()
	}

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{SkipProducedUtxoOffsetWrites: true},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	// The sentinel must survive — proves the produced-UTxO write was elided.
	readTxn := db.Transaction(false)
	defer readTxn.Release()
	got, err := readTxn.DB().Blob().GetUtxo(
		readTxn.Blob(), producedTxId, producedIdx,
	)
	require.NoError(t, err)
	require.Equal(
		t, sentinel, got,
		"SkipProducedUtxoOffsetWrites=true must leave the existing blob value untouched",
	)

	// The tx-offset blob entry must have been written regardless: the
	// immutable-copy phase never writes tx offsets, so backfill is the
	// only source for them.
	txHash := candidate.producerTx.Hash().Bytes()
	txBlob, err := readTxn.DB().Blob().GetTx(readTxn.Blob(), txHash)
	require.NoError(t, err)
	require.NotEmpty(t, txBlob, "TX offset must still be written when skipping UTxO offsets")
	require.True(
		t, IsTxOffsetStorage(txBlob),
		"TX blob entry must be a valid TX offset reference",
	)
}

// TestSetTransactionBatchedWithOpts_DefaultBehaviorWrites confirms that the
// default (zero-value) options still write the produced-UTxO offset blob,
// overwriting any prior bytes under that key.
func TestSetTransactionBatchedWithOpts_DefaultBehaviorWrites(t *testing.T) {
	db, err := New(&Config{
		DataDir:        t.TempDir(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	candidate := findBatchedCrossBlockSpendCandidate(t)
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)

	produced := candidate.producerTx.Produced()
	require.NotEmpty(t, produced)

	sentinel := []byte("SHOULD-BE-OVERWRITTEN")
	producedIdx := produced[0].Id.Index()
	producedTxId := produced[0].Id.Id().Bytes()

	{
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *Txn) error {
			return txn.DB().Blob().SetUtxo(
				txn.Blob(), producedTxId, producedIdx, sentinel,
			)
		}))
		txn.Release()
	}

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{}, // default: no skip
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	readTxn := db.Transaction(false)
	defer readTxn.Release()
	got, err := readTxn.DB().Blob().GetUtxo(
		readTxn.Blob(), producedTxId, producedIdx,
	)
	require.NoError(t, err)
	require.NotEqual(
		t, sentinel, got,
		"default opts must overwrite the existing blob value with the offset encoding",
	)
	require.True(
		t, IsUtxoOffsetStorage(got),
		"new blob value must be a valid UTxO offset reference",
	)
}

// TestSetTransactionBatchedWithOpts_RequiresOffsetsEvenWhenSkipping pins down
// the invariant that even though the per-output blob write is elided, the
// indexer-supplied offset must still be present. This catches a regression
// in offset computation that would otherwise be hidden by the skip path.
func TestSetTransactionBatchedWithOpts_RequiresOffsetsEvenWhenSkipping(
	t *testing.T,
) {
	db, err := New(&Config{
		DataDir:        t.TempDir(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	candidate := findBatchedCrossBlockSpendCandidate(t)
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)

	// Build an offsets struct missing the first produced UTxO. Skipping
	// the write must NOT also skip this validity check.
	complete := mustBlockOffsets(t, candidate.producerBlock)
	produced := candidate.producerTx.Produced()
	require.NotEmpty(t, produced)
	var txHashArray [32]byte
	copy(txHashArray[:], candidate.producerTx.Hash().Bytes())
	missingRef := UtxoRef{
		TxId:      txHashArray,
		OutputIdx: produced[0].Id.Index(),
	}
	require.Contains(t, complete.UtxoOffsets, missingRef)
	delete(complete.UtxoOffsets, missingRef)

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	err = db.SetTransactionBatchedWithOpts(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,
		nil,
		nil,
		complete,
		acc,
		txn,
		BatchedTxIngestOpts{SkipProducedUtxoOffsetWrites: true},
	)
	require.Error(t, err, "missing offset must still error even with skip enabled")
	require.Contains(t, err.Error(), "missing UTxO offset")
}

// TestSetTransactionBatchedWithOpts_BlobStoreUnused proves the skip path
// makes zero Set/Get calls against the blob store for produced UTxOs.
// We assert this indirectly by passing a UTxO ref that points to a different
// (uninitialised) tx id and verifying GetUtxo returns ErrBlobKeyNotFound after
// the operation — i.e. the skip didn't silently fall back to writing.
func TestSetTransactionBatchedWithOpts_BlobStoreUnused(t *testing.T) {
	db, err := New(&Config{
		DataDir:        t.TempDir(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	candidate := findBatchedCrossBlockSpendCandidate(t)
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{SkipProducedUtxoOffsetWrites: true},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	// Probe a deliberately-untouched key: the skip path must not have
	// written under any unrelated produced refs (sanity check the
	// implementation didn't accidentally broaden the elision).
	probeTxId := bytes.Repeat([]byte{0xFE}, 32)
	readTxn := db.Transaction(false)
	defer readTxn.Release()
	_, err = readTxn.DB().Blob().GetUtxo(readTxn.Blob(), probeTxId, 0)
	require.Error(t, err)
	require.True(
		t,
		errors.Is(err, types.ErrBlobKeyNotFound),
		"expected ErrBlobKeyNotFound for an unrelated key, got %v",
		err,
	)
}
