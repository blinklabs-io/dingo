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
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// producedSentinel returns a deterministic per-output sentinel that
// (importantly) is NOT a valid CborOffset encoding, so a successful
// blob.SetUtxo call from the production code path will overwrite it
// with bytes that pass IsUtxoOffsetStorage.
func producedSentinel(idx uint32) []byte {
	return []byte(fmt.Sprintf("SENTINEL-PRODUCED-UTXO-%d", idx))
}

// txSentinel returns a non-offset byte sequence used to seed a TX blob
// key so we can detect whether SetTransactionBatchedWithOpts overwrote
// it with a real EncodeTxOffset result.
func txSentinel() []byte {
	return []byte("SENTINEL-TX-NOT-AN-OFFSET-ENCODING")
}

// stagedProducer stores the candidate producer block, then replaces every
// produced-UTxO blob entry with a sentinel. The returned map gives the
// caller a sentinel-per-ref lookup for post-condition assertions.
func stagedProducer(
	t *testing.T,
	db *Database,
	candidate batchedCrossBlockSpendCandidate,
) map[UtxoRef][]byte {
	t.Helper()
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)

	var txHashArray [32]byte
	copy(txHashArray[:], candidate.producerTx.Hash().Bytes())

	sentinels := make(map[UtxoRef][]byte)
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		blob := txn.DB().Blob()
		for _, utxo := range candidate.producerTx.Produced() {
			ref := UtxoRef{
				TxId:      txHashArray,
				OutputIdx: utxo.Id.Index(),
			}
			s := producedSentinel(ref.OutputIdx)
			sentinels[ref] = s
			if err := blob.SetUtxo(
				txn.Blob(),
				ref.TxId[:],
				ref.OutputIdx,
				s,
			); err != nil {
				return err
			}
		}
		return nil
	}))
	txn.Release()
	return sentinels
}

// stageTxSentinel overwrites the tx-blob entry for the producer tx with
// a non-offset sentinel so the test can prove SetTransactionBatchedWithOpts
// actually called blob.SetTx (rather than relying on the seed left behind
// by storeBlockOffsetsOnly).
func stageTxSentinel(t *testing.T, db *Database, txHash []byte) []byte {
	t.Helper()
	s := txSentinel()
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return txn.DB().Blob().SetTx(txn.Blob(), txHash, s)
	}))
	txn.Release()
	return s
}

func openTestDB(t *testing.T) *Database {
	t.Helper()
	db, err := New(&Config{
		DataDir:        t.TempDir(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close() //nolint:errcheck
	})
	return db
}

// TestSetTransactionBatchedWithOpts_SkipsAllProducedUtxoWrites verifies the
// SkipProducedUtxoOffsetWrites optimisation elides the blob.SetUtxo call for
// EVERY produced output of the transaction. The test stages a unique sentinel
// under every produced-UTxO key; if the skip path is honored, all sentinels
// survive. If any one was overwritten, the implementation would have to be
// silently writing some refs while elising others.
//
// Addresses reviewer feedback that the prior version probed only one or an
// unrelated key, which could not detect a partial regression.
func TestSetTransactionBatchedWithOpts_SkipsAllProducedUtxoWrites(t *testing.T) {
	db := openTestDB(t)

	candidate := findBatchedCrossBlockSpendCandidate(t)
	sentinels := stagedProducer(t, db, candidate)
	require.NotEmpty(t, sentinels, "producer tx must have at least one output")

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

	// Every produced ref must still hold its sentinel. This is the
	// direct proof that no blob.SetUtxo call landed on any produced key.
	readTxn := db.Transaction(false)
	defer readTxn.Release()
	for ref, want := range sentinels {
		got, err := readTxn.DB().Blob().GetUtxo(
			readTxn.Blob(), ref.TxId[:], ref.OutputIdx,
		)
		require.NoError(t, err, "GetUtxo %x#%d", ref.TxId[:8], ref.OutputIdx)
		require.Equalf(
			t, want, got,
			"produced UTxO %x#%d was overwritten — skip did not elide its write",
			ref.TxId[:8], ref.OutputIdx,
		)
	}
}

// TestSetTransactionBatchedWithOpts_TxOffsetStillWritten proves that even
// when produced-UTxO writes are elided, the TX offset write still happens.
// The seed left by storeBlockOffsetsOnly is replaced with a non-offset
// sentinel so the post-condition is meaningful: the tx blob must be a valid
// offset reference after the call (which it is not before).
//
// Addresses reviewer feedback that the prior assertion was tautological
// because storeBlockOffsetsOnly had already seeded the tx key.
func TestSetTransactionBatchedWithOpts_TxOffsetStillWritten(t *testing.T) {
	db := openTestDB(t)

	candidate := findBatchedCrossBlockSpendCandidate(t)
	stagedProducer(t, db, candidate)

	txHash := candidate.producerTx.Hash().Bytes()
	sentinel := stageTxSentinel(t, db, txHash)

	// Sanity-check the seed: before the call, the tx-blob entry must NOT
	// look like a valid offset. Without this, a no-op implementation
	// would still pass the post-condition.
	{
		readTxn := db.Transaction(false)
		got, err := readTxn.DB().Blob().GetTx(readTxn.Blob(), txHash)
		readTxn.Release()
		require.NoError(t, err)
		require.Equal(t, sentinel, got)
		require.False(
			t, IsTxOffsetStorage(got),
			"sentinel must not be confusable with a valid offset encoding",
		)
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

	readTxn := db.Transaction(false)
	defer readTxn.Release()
	got, err := readTxn.DB().Blob().GetTx(readTxn.Blob(), txHash)
	require.NoError(t, err)
	require.NotEqual(
		t, sentinel, got,
		"TX sentinel must be overwritten — TX offset writes are not part of the skip",
	)
	require.True(
		t, IsTxOffsetStorage(got),
		"TX blob entry must be a valid TX offset reference after the call",
	)
}

// TestSetTransactionBatchedWithOpts_DefaultBehaviorOverwrites confirms the
// default (zero-value) options still write produced-UTxO offset blobs.
func TestSetTransactionBatchedWithOpts_DefaultBehaviorOverwrites(t *testing.T) {
	db := openTestDB(t)

	candidate := findBatchedCrossBlockSpendCandidate(t)
	sentinels := stagedProducer(t, db, candidate)
	require.NotEmpty(t, sentinels)

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
	for ref, sentinel := range sentinels {
		got, err := readTxn.DB().Blob().GetUtxo(
			readTxn.Blob(), ref.TxId[:], ref.OutputIdx,
		)
		require.NoError(t, err)
		require.NotEqualf(
			t, sentinel, got,
			"default opts must overwrite produced UTxO %x#%d",
			ref.TxId[:8], ref.OutputIdx,
		)
		require.True(
			t, IsUtxoOffsetStorage(got),
			"overwritten value must be a valid UTxO offset reference",
		)
	}
}

// TestSetTransactionBatchedWithOpts_RequiresOffsetsEvenWhenSkipping pins the
// invariant that even though the per-output blob write is elided, the
// indexer-supplied offset must still be present. This catches a regression
// in offset computation that would otherwise be hidden by the skip path.
func TestSetTransactionBatchedWithOpts_RequiresOffsetsEvenWhenSkipping(
	t *testing.T,
) {
	db := openTestDB(t)

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

	err := db.SetTransactionBatchedWithOpts(
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

// TestSetTransactionBatchedWithOpts_SkipConsumedInputRecovery verifies that
// enabling SkipConsumedInputRecovery elides the per-input GetUtxoIncludingSpent
// checks. During Mithril backfill, immutable blocks are replayed in slot order
// against a metadata store being populated from the same history, so consumed
// inputs are guaranteed to already exist from earlier producer transactions.
// This test ensures the skip path counts inputs that would have been checked.
func TestSetTransactionBatchedWithOpts_SkipConsumedInputRecovery(t *testing.T) {
	db := openTestDB(t)

	candidate := findBatchedCrossBlockSpendCandidate(t)

	// Store producer block first so consumed inputs exist in metadata
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
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
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
	txn.Release()

	// Now ingest the consumer with skip enabled
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
	var stats types.BackfillHotPathStats
	acc2 := db.NewBatchAccumulator()
	txn2 := db.Transaction(true)
	defer txn2.Release()
	defer txn2.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc2,
		txn2,
		BatchedTxIngestOpts{
			SkipConsumedInputRecovery: true,
			Stats:                     &stats,
		},
	))
	require.NoError(t, db.FlushBatch(acc2, txn2))
	require.NoError(t, txn2.Commit())

	// Verify the counter shows skipped inputs
	consumed := candidate.consumerTx.Consumed()
	require.Greater(t, len(consumed), 0, "consumer tx must have at least one input")
	require.Equal(
		t, uint64(len(consumed)), stats.SkippedInputRecovery,
		"SkippedInputRecovery must count all consumed inputs when skip is enabled",
	)
}

// TestSetTransactionBatchedWithOpts_DefaultDoesNotSkipInputRecovery confirms
// that when SkipConsumedInputRecovery is false (default), the recovery path
// is still executed and the counter remains zero.
func TestSetTransactionBatchedWithOpts_DefaultDoesNotSkipInputRecovery(
	t *testing.T,
) {
	db := openTestDB(t)

	candidate := findBatchedCrossBlockSpendCandidate(t)

	// Store producer block first
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
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
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
	txn.Release()

	// Ingest consumer with default options (skip disabled)
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
	var stats types.BackfillHotPathStats
	acc2 := db.NewBatchAccumulator()
	txn2 := db.Transaction(true)
	defer txn2.Release()
	defer txn2.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc2,
		txn2,
		BatchedTxIngestOpts{
			SkipConsumedInputRecovery: false, // explicit default
			Stats:                     &stats,
		},
	))
	require.NoError(t, db.FlushBatch(acc2, txn2))
	require.NoError(t, txn2.Commit())

	// Verify the skip counter is zero when recovery is not skipped
	require.Equal(
		t, uint64(0), stats.SkippedInputRecovery,
		"SkippedInputRecovery must be zero when skip is disabled",
	)
}
