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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/stretchr/testify/require"
)

// TestSetTransactionBatched_SameBatchProducerSpentViaInFlight ingests a
// producer and the transaction that spends its output into the SAME batch
// accumulator before any flush. When the consumer is processed the output it
// spends exists only in the in-flight accumulator — it has never been written
// to the metadata store. Correct provenance (the produced row ends up present
// and marked spent) proves the in-flight lookup carries same-batch provenance
// without depending on blob/metadata recovery.
func TestSetTransactionBatched_SameBatchProducerSpentViaInFlight(t *testing.T) {
	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

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
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	utxo, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(
		t,
		utxo,
		"same-batch produced output must be created at flush",
	)
	require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
	require.Equal(t, candidate.consumerTx.Hash().Bytes(), []byte(utxo.SpentAtTxId))
}

// TestSetTransactionBatched_CrossBatchProducerResolvesFromDB flushes the
// producer in one batch, then ingests the consumer in a second batch whose
// accumulator does not contain the producer. The spend must resolve through
// the metadata-store fallthrough, exactly as before this optimisation.
func TestSetTransactionBatched_CrossBatchProducerResolvesFromDB(t *testing.T) {
	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

	// Batch 1: ingest and flush the producer so its output is committed.
	acc1 := db.NewBatchAccumulator()
	txn1 := db.Transaction(true)
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc1,
		txn1,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc1, txn1))
	require.NoError(t, txn1.Commit())
	txn1.Release()

	// The producer output is now a committed, live row — not in-flight.
	pre, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, pre)
	require.Zero(
		t,
		pre.DeletedSlot,
		"producer output must be live before the consumer batch",
	)

	// Batch 2: a fresh accumulator (empty in-flight index) ingests the
	// consumer; the spend must resolve via the metadata store.
	acc2 := db.NewBatchAccumulator()
	txn2 := db.Transaction(true)
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
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc2, txn2))
	require.NoError(t, txn2.Commit())
	txn2.Release()

	post, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, post)
	require.Equal(t, candidate.consumerPoint.Slot, post.DeletedSlot)
	require.Equal(t, candidate.consumerTx.Hash().Bytes(), []byte(post.SpentAtTxId))
}

// TestSetTransactionBatched_MissingProducerNotFabricated ingests only the
// consumer, with no producer either in-flight or committed. A genuinely
// missing historical producer must not be hidden or fabricated: the in-flight
// optimisation only short-circuits real same-batch producers.
func TestSetTransactionBatched_MissingProducerNotFabricated(t *testing.T) {
	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc,
		txn,
		BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	utxo, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.Nil(
		t,
		utxo,
		"genuinely missing producer must not be hidden or fabricated",
	)
}

// ingestSameBatchProducerConsumer ingests the producer and consumer of the
// candidate through the batched path into one accumulator and flushes.
func ingestSameBatchProducerConsumer(
	t *testing.T,
	db *Database,
	candidate batchedCrossBlockSpendCandidate,
) {
	t.Helper()
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0, nil, nil,
		mustBlockOffsets(t, candidate.producerBlock),
		acc, txn, BatchedTxIngestOpts{},
	))
	require.NoError(t, db.SetTransactionBatchedWithOpts(
		candidate.consumerTx,
		candidate.consumerPoint,
		candidate.consumerIdx,
		0, nil, nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		acc, txn, BatchedTxIngestOpts{},
	))
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
}

// TestSetTransactionBatched_InFlightDoesNotSkipExistingRowRepair guards the
// resumed-backfill case: when the consumed output already exists in metadata
// as a partially-written spent row (DeletedSlot == consumer slot, SpentAtTxId
// == nil from a prior partial run) and the producer is re-ingested into the
// same batch (so it is also "in-flight"), the consumer must still backfill the
// spender link. The in-flight short-circuit must run after the existing-row
// repair: the flush's batchSpendUtxos only updates rows where deleted_slot = 0
// and could not fix this row later.
//
// Two passes make the simulation faithful: the first pass ingests normally
// (creating the consumer transaction row that spent_at_tx_id references), then
// spent_at_tx_id is cleared to mimic the partial-write state, and the second
// pass must repair it.
func TestSetTransactionBatched_InFlightDoesNotSkipExistingRowRepair(
	t *testing.T,
) {
	db := openTestDB(t)
	candidate := findBatchedCrossBlockSpendCandidate(t)

	// Pass 1: ingest normally so the output is spent and the consumer tx row
	// exists.
	ingestSameBatchProducerConsumer(t, db, candidate)

	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok)

	// Mimic a partial prior run: the row stays deleted at the consumer slot
	// but loses its spender hash.
	require.NoError(t, store.DB().
		Model(&models.Utxo{}).
		Where("tx_id = ? AND output_idx = ?",
			candidate.input.Id().Bytes(), candidate.input.Index()).
		Update("spent_at_tx_id", nil).Error)

	pre, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(), candidate.input.Index(), nil,
	)
	require.NoError(t, err)
	require.NotNil(t, pre)
	require.Equal(t, candidate.consumerPoint.Slot, pre.DeletedSlot)
	require.Nil(t, pre.SpentAtTxId, "precondition: spender hash cleared")

	// Pass 2: re-ingest. The producer is in-flight again, but because the row
	// already exists the consumer must repair the spender link rather than
	// short-circuit on the in-flight lookup.
	ingestSameBatchProducerConsumer(t, db, candidate)

	post, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(), candidate.input.Index(), nil,
	)
	require.NoError(t, err)
	require.NotNil(t, post)
	require.Equal(t, candidate.consumerPoint.Slot, post.DeletedSlot)
	require.Equal(
		t,
		candidate.consumerTx.Hash().Bytes(),
		[]byte(post.SpentAtTxId),
		"spender link must be backfilled for a pre-existing same-slot row",
	)
}
