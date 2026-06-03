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
	require.Equal(t, candidate.consumerTx.Hash().Bytes(), utxo.SpentAtTxId)
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
	require.Equal(t, candidate.consumerTx.Hash().Bytes(), post.SpentAtTxId)
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
