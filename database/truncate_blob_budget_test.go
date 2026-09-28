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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newOnDiskTestDB builds a test database whose blob store is on disk. The
// badger plugin only applies MemTableSize to an on-disk store (an in-memory
// one takes badger's 64 MiB default), and the memtable is what sizes the
// per-transaction entry budget these tests are about.
func newOnDiskTestDB(t *testing.T) *Database {
	t.Helper()
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err, "failed to create test database")
	return db
}

// A badger transaction accepts a bounded number of staged entries:
// maxBatchSize is 15% of the memtable and maxBatchCount is that divided by
// skl.MaxNodeSize (96 bytes), and checkSize rejects the entry that would
// reach either. The test blob store asks for testutil.TestBadgerMemTableSize,
// so a transaction here holds roughly 13,000 entries -- cheap enough to
// exceed in a unit test, where production's 128 MiB memtable would need
// ~209,715.
//
// Deletes are counted by the same budget, so a rollback that stages more
// blob deletes than this exhausts the transaction. Everything staged after
// that point fails, including the 8-byte commit timestamp Txn.Commit writes
// into the same transaction -- which turns a tolerated partial blob cleanup
// into a rollback that cannot be committed at all
// (blinklabs-io/dingo#4657).
const (
	testBadgerMaxBatchSize  = 15 * testutil.TestBadgerMemTableSize / 100
	testBadgerMaxBatchCount = testBadgerMaxBatchSize / 96
	// overBudgetBlobDeletes comfortably exceeds that budget without making
	// the fixture slow to seed.
	overBudgetBlobDeletes = testBadgerMaxBatchCount + 4_000
)

// seedRollbackUtxos writes overBudgetBlobDeletes UTxOs above rollbackSlot,
// both the metadata rows TruncateAfterSlot collects and the blob objects it
// deletes. The blobs go in through their own bounded transactions: a single
// transaction could not hold them either.
func seedRollbackUtxos(
	t *testing.T,
	db *Database,
	rollbackSlot uint64,
) []models.Utxo {
	t.Helper()

	utxos := make([]models.Utxo, 0, overBudgetBlobDeletes)
	for i := range overBudgetBlobDeletes {
		txId := make([]byte, 32)
		//nolint:gosec // loop counter, always in range
		binary.BigEndian.PutUint32(txId[:4], uint32(i))
		utxos = append(utxos, models.Utxo{
			TxId:      txId,
			OutputIdx: 0,
			AddedSlot: rollbackSlot + 1,
			Amount:    1_000_000,
		})
	}

	seedTxn := db.MetadataTxn(true)
	require.NoError(t, seedTxn.Do(func(txn *Txn) error {
		return db.Metadata().ImportUtxos(utxos, txn.Metadata())
	}))
	seedTxn.Release()

	const blobBatch = 2_000
	for start := 0; start < len(utxos); start += blobBatch {
		end := min(start+blobBatch, len(utxos))
		batch := utxos[start:end]
		blobTxn := NewBlobOnlyTxn(db, true)
		store := blobTxn.BlobStore()
		require.NotNil(t, store)
		for _, utxo := range batch {
			require.NoError(
				t,
				store.SetUtxo(
					blobTxn.Blob(),
					utxo.TxId,
					utxo.OutputIdx,
					[]byte("utxo-cbor"),
				),
			)
		}
		require.NoError(t, blobTxn.Commit())
	}
	return utxos
}

// countUtxoBlobs reports how many of the given UTxOs still have blob data.
func countUtxoBlobs(t *testing.T, db *Database, utxos []models.Utxo) int {
	t.Helper()
	txn := db.Transaction(false)
	defer txn.Release()
	store := txn.BlobStore()
	require.NotNil(t, store)
	var found int
	for _, utxo := range utxos {
		if _, err := store.GetUtxo(
			txn.Blob(),
			utxo.TxId,
			utxo.OutputIdx,
		); err == nil {
			found++
		}
	}
	return found
}

// TestTruncateAfterSlotCommitsWithOverBudgetBlobDeletes is the startup
// rollback of blinklabs-io/dingo#4657: TruncateAfterSlot runs inside a
// combined transaction the caller commits, and it stages every rolled-back
// UTxO's blob delete into that one transaction. Past badger's per-
// transaction budget every further staged write is rejected, and the last of
// them is the commit timestamp Txn.Commit writes -- so the whole rollback
// commit fails, nothing is durable, and the next start recomputes the same
// delete set and fails identically.
//
// A rollback that cannot finish its blob cleanup must degrade to orphaned
// blobs, which callers already tolerate (ErrBlobDeleteIncomplete /
// recordBlobOrphansOnCommit), never to a transaction that cannot commit.
func TestTruncateAfterSlotCommitsWithOverBudgetBlobDeletes(t *testing.T) {
	t.Parallel()

	db := newOnDiskTestDB(t)

	const rollbackSlot = uint64(1_500)
	targetBlock := testIndexedBlock(rollbackSlot, 1, 0x15)
	require.NoError(t, db.BlockCreate(targetBlock, nil))

	utxos := seedRollbackUtxos(t, db, rollbackSlot)
	require.Equal(
		t,
		len(utxos),
		countUtxoBlobs(t, db, utxos),
		"fixture must start with every UTxO blob present",
	)

	point := ocommon.Point{Slot: rollbackSlot, Hash: targetBlock.Hash}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		_, _, err := db.TruncateAfterSlot(point, 0, txn)
		return err
	}), "rollback commit must survive a blob-delete set larger than the "+
		"blob store's per-transaction budget")

	// The rollback still means what it meant: the metadata that names the
	// rolled-back UTxOs is gone, whether or not their blobs could be.
	readTxn := db.Transaction(false)
	defer readTxn.Release()
	remaining, err := db.Metadata().GetUtxosAddedAfterSlot(
		rollbackSlot,
		readTxn.Metadata(),
	)
	require.NoError(t, err)
	require.Empty(t, remaining, "rolled-back UTxO metadata must be gone")

	// Whatever did not fit is orphaned rather than lost work: most of the
	// set is deleted, and only the tail beyond the budget survives.
	orphans := countUtxoBlobs(t, db, utxos)
	require.Positive(
		t,
		orphans,
		"the over-budget tail should be left as orphans",
	)
	require.Less(
		t,
		orphans,
		len(utxos)/2,
		"the bulk of the delete set must still have been staged",
	)
}

// TestDeleteTxBlobsLeavesRoomForCommit is the transaction-blob half of the
// same defect: deleteTxBlobs stages the caller's whole hash set when the
// caller supplies a blob handle, so the batching bound it declares is dead
// on exactly the path a rollback takes. The staged set must stop short of
// the budget and report the remainder as ErrBlobDeleteIncomplete, leaving
// the enclosing transaction able to commit.
func TestDeleteTxBlobsLeavesRoomForCommit(t *testing.T) {
	t.Parallel()

	db := newOnDiskTestDB(t)

	txHashes := make([][]byte, 0, overBudgetBlobDeletes)
	for i := range overBudgetBlobDeletes {
		hash := bytes.Repeat([]byte{0x00}, 32)
		//nolint:gosec // loop counter, always in range
		binary.BigEndian.PutUint32(hash[:4], uint32(i))
		txHashes = append(txHashes, hash)
	}

	txn := db.Transaction(true)
	err := deleteTxBlobs(db, txHashes, txn)
	require.Error(t, err, "the over-budget tail must be reported")
	require.ErrorIs(t, err, ErrBlobDeleteIncomplete)
	require.NoError(
		t,
		txn.Commit(),
		"the commit timestamp write must still fit after a bounded "+
			"staged delete set",
	)
}
