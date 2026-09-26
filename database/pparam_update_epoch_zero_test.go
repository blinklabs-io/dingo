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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package database

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// A proposal for epoch 0, made during epoch 0 before its slot of no return, is
// a current proposal the reference adopts at the boundary into epoch 1. Both
// transaction ingestion paths must store it.
func TestSetTransactionStoresEpochZeroProposals(t *testing.T) {
	t.Parallel()
	updateCbor, err := cbor.Encode(map[uint64]any{0: 200})
	require.NoError(t, err)
	var update shelley.ShelleyProtocolParameterUpdate
	_, err = cbor.Decode(updateCbor, &update)
	require.NoError(t, err)
	genesis := lcommon.Blake2b224{0x42}
	updates := map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate{
		genesis: update,
	}
	requireStored := func(t *testing.T, db *Database) {
		t.Helper()
		rows, err := db.Metadata().GetPParamUpdates(0, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, genesis.Bytes(), rows[0].GenesisHash)
		require.Equal(t, uint64(0), rows[0].Epoch)
		require.Equal(t, update.Cbor(), rows[0].Cbor)
	}

	t.Run("block", func(t *testing.T) {
		t.Parallel()
		db := openTestDB(t)
		candidate := findGapRollbackCandidateWithoutCertificates(t)
		for _, block := range candidate.producerBlocks {
			storeBlockOffsetsOnly(t, db, block)
		}
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
		require.True(t, candidate.consumerTx.IsValid())
		require.NoError(t, db.SetTransaction(
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			0,
			updates,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		))
		requireStored(t, db)
	})

	t.Run("batch", func(t *testing.T) {
		t.Parallel()
		db := openTestDB(t)
		candidate := findBatchedCrossBlockSpendCandidate(t)
		stagedProducer(t, db, candidate)
		require.True(t, candidate.producerTx.IsValid())
		acc := db.NewBatchAccumulator()
		txn := db.Transaction(true)
		defer txn.Release()
		defer txn.Rollback() //nolint:errcheck
		require.NoError(t, db.SetTransactionBatchedWithOpts(
			candidate.producerTx,
			candidate.producerPoint,
			candidate.producerIdx,
			0,
			updates,
			nil,
			mustBlockOffsets(t, candidate.producerBlock),
			acc,
			txn,
			BatchedTxIngestOpts{},
		))
		require.NoError(t, db.FlushBatch(acc, txn))
		require.NoError(t, txn.Commit())
		requireStored(t, db)
	})
}
