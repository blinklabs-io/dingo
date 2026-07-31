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
	"github.com/blinklabs-io/dingo/database/recovery"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockHashForIndex builds a distinct 32-byte hash per block index, so chains
// longer than 256 blocks do not repeat a hash.
func blockHashForIndex(i int) []byte {
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash, uint64(i))
	return hash
}

// linkedChain builds a run of blocks whose prev-hash links form a chain.
func linkedChain(t *testing.T, db *Database, count int) []models.Block {
	t.Helper()
	blocks := make([]models.Block, 0, count)
	var prevHash []byte
	for i := 1; i <= count; i++ {
		block := models.Block{
			ID:       uint64(i),
			Slot:     uint64(i) * 10,
			Hash:     blockHashForIndex(i),
			PrevHash: prevHash,
			Cbor:     []byte{0x80},
			Number:   uint64(i),
			Type:     1,
		}
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
		prevHash = block.Hash
	}
	return blocks
}

func setTipTo(t *testing.T, db *Database, block models.Block) {
	t.Helper()
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.NewPoint(block.Slot, block.Hash),
		BlockNumber: block.Number,
	}, nil))
}

func TestRecoveryStateSourceReportsTips(t *testing.T) {
	db := newTestDB(t)
	blocks := linkedChain(t, db, 3)
	setTipTo(t, db, blocks[2])

	source := db.RecoveryStateSource()
	metaTip, blockNumber, err := source.MetadataTip()
	require.NoError(t, err)
	assert.Equal(t, blocks[2].Slot, metaTip.Slot)
	assert.Equal(t, blocks[2].Number, blockNumber)

	blobTip, err := source.BlobTip()
	require.NoError(t, err)
	assert.Equal(t, blocks[2].Slot, blobTip.Slot)
	assert.Equal(t, blocks[2].Hash, blobTip.Hash)
}

func TestRecoveryStateSourceBlobTipOnEmptyStore(t *testing.T) {
	db := newTestDB(t)
	blobTip, err := db.RecoveryStateSource().BlobTip()
	require.NoError(t, err)
	assert.True(t, blobTip.IsZero())
}

func TestRecoveryStateSourceRecentBlocksAreNewestFirst(t *testing.T) {
	db := newTestDB(t)
	blocks := linkedChain(t, db, 5)

	refs, err := db.RecoveryStateSource().RecentBlocks(3)
	require.NoError(t, err)
	require.Len(t, refs, 3)
	assert.Equal(t, blocks[4].Hash, refs[0].Hash)
	assert.Equal(t, blocks[3].Hash, refs[1].Hash)
	assert.Equal(t, blocks[2].Hash, refs[2].Hash)
	// The linkage the continuity check relies on has to survive the round
	// trip through the blob store.
	assert.Equal(t, refs[1].Hash, refs[0].PrevHash)
}

func TestRecoveryStateSourceRecentBlocksRejectsNonPositiveLimit(t *testing.T) {
	db := newTestDB(t)
	linkedChain(t, db, 2)
	refs, err := db.RecoveryStateSource().RecentBlocks(0)
	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestRecoveryStateSourceFindsOrphansAboveSlot(t *testing.T) {
	db := newTestDB(t)
	blocks := linkedChain(t, db, 5)

	orphans, err := db.RecoveryStateSource().OrphanBlobs(blocks[2].Slot, 10)
	require.NoError(t, err)
	require.Len(t, orphans, 2)
	assert.Equal(t, blocks[3].Slot, orphans[0].Slot)
	assert.Equal(t, blocks[4].Slot, orphans[1].Slot)
	assert.Equal(t, blocks[3].Hash, orphans[0].Hash)
}

func TestRecoveryStateSourceOrphanLimitIsHonoured(t *testing.T) {
	db := newTestDB(t)
	linkedChain(t, db, 5)
	orphans, err := db.RecoveryStateSource().OrphanBlobs(0, 2)
	require.NoError(t, err)
	assert.Len(t, orphans, 2)
}

func TestRecoveryStateSourceCheckUtxosOnEmptyStore(t *testing.T) {
	db := newTestDB(t)
	result, err := db.RecoveryStateSource().CheckUtxos(10)
	require.NoError(t, err)
	assert.Zero(t, result.Checked)
	assert.Empty(t, result.Unresolvable)
}

func TestTrimBlobAboveRemovesOnlyBlocksAboveTheBoundary(t *testing.T) {
	db := newTestDB(t)
	blocks := linkedChain(t, db, 5)

	removed, err := db.TrimBlobAbove(blocks[2].Slot)
	require.NoError(t, err)
	assert.Equal(t, 2, removed)

	refs, err := db.RecoveryStateSource().RecentBlocks(10)
	require.NoError(t, err)
	require.Len(t, refs, 3)
	assert.Equal(t, blocks[2].Hash, refs[0].Hash)

	orphans, err := db.RecoveryStateSource().OrphanBlobs(blocks[2].Slot, 10)
	require.NoError(t, err)
	assert.Empty(t, orphans)
}

func TestTrimBlobAboveSpansMultipleBatches(t *testing.T) {
	db := newTestDB(t)
	// More blocks than one removal pass handles, so the batching loop has to
	// rescan and keep going rather than stopping after the first batch.
	count := trimBatchSize + 25
	linkedChain(t, db, count)

	removed, err := db.TrimBlobAbove(0)
	require.NoError(t, err)
	assert.Equal(t, count, removed)

	orphans, err := db.RecoveryStateSource().OrphanBlobs(0, count)
	require.NoError(t, err)
	assert.Empty(t, orphans)
}

func TestTrimBlobAboveIsANoOpWithNothingAbove(t *testing.T) {
	db := newTestDB(t)
	blocks := linkedChain(t, db, 3)
	removed, err := db.TrimBlobAbove(blocks[2].Slot)
	require.NoError(t, err)
	assert.Zero(t, removed)
}

func TestResetCommitFenceMakesStoresAgree(t *testing.T) {
	db := newTestDB(t)
	// Put the stores out of step the way an interrupted commit would, with
	// the blob store carrying the newer fence.
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(txn *Txn) error {
		return db.Blob().SetCommitTimestamp(999, txn.Blob())
	}))
	_, staleBlobTS, staleErr := db.RecoveryStateSource().CommitTimestamps()
	require.NoError(t, staleErr)
	require.Equal(t, int64(999), staleBlobTS)

	require.NoError(t, db.ResetCommitFence())

	metadataTS, blobTS, err := db.RecoveryStateSource().CommitTimestamps()
	require.NoError(t, err)
	assert.Equal(t, blobTS, metadataTS)
	assert.Positive(t, metadataTS)
}

func TestCombinedCommitRecordsAndResolvesJournalIntent(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:  dir,
		Recovery: &recovery.Config{Dir: dir, SyncJournal: true},
	})
	require.NoError(t, err)
	require.NotNil(t, db.Recovery())

	block := models.Block{
		ID:     1,
		Slot:   10,
		Hash:   bytes.Repeat([]byte{0x01}, 32),
		Cbor:   []byte{0x80},
		Number: 1,
		Type:   1,
	}
	require.NoError(t, db.BlockCreate(block, nil))
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.SetTip(ochainsync.Tip{
			Point:       ocommon.NewPoint(block.Slot, block.Hash),
			BlockNumber: block.Number,
		}, txn)
	}))

	var begins, commits int
	var intent recovery.Intent
	require.NoError(
		t,
		db.Recovery().Replay(func(r recovery.Record) error {
			switch r.Type {
			case recovery.RecordTypeBegin:
				begins++
				intent = r.Intent
			case recovery.RecordTypeCommit:
				commits++
			}
			return nil
		}),
	)
	assert.Positive(t, begins)
	assert.Equal(t, begins, commits, "every intent should be resolved")
	// The tip write describes the transaction, so recovery can name the
	// point a crash would have interrupted.
	assert.Equal(t, recovery.IntentBlockAdd, intent.Kind)
	assert.Equal(t, block.Slot, intent.Slot)
	assert.Equal(t, block.Hash, intent.Hash)
}

func TestExplicitRecoveryIntentWinsOverTheAutomaticOne(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:  dir,
		Recovery: &recovery.Config{Dir: dir, SyncJournal: true},
	})
	require.NoError(t, err)

	txn := db.Transaction(true)
	txn.SetRecoveryIntent(recovery.Intent{
		Kind: recovery.IntentRollback,
		Slot: 99,
	})
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.SetTip(ochainsync.Tip{
			Point: ocommon.NewPoint(500, bytes.Repeat([]byte{0x02}, 32)),
		}, txn)
	}))

	var last recovery.Intent
	require.NoError(
		t,
		db.Recovery().Replay(func(r recovery.Record) error {
			if r.Type == recovery.RecordTypeBegin {
				last = r.Intent
			}
			return nil
		}),
	)
	assert.Equal(t, recovery.IntentRollback, last.Kind)
	assert.Equal(t, uint64(99), last.Slot)
}

func TestDatabaseWithoutRecoveryHasNoManager(t *testing.T) {
	db := newTestDB(t)
	assert.Nil(t, db.Recovery())
	// The commit path must stay happy with a nil manager, since that is the
	// configuration every existing deployment starts from.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.SetTip(ochainsync.Tip{
			Point: ocommon.NewPoint(1, bytes.Repeat([]byte{0x03}, 32)),
		}, txn)
	}))
}

func TestUtxoValidationModeFallsBackToStrictFlag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		mode   types.UtxoValidationMode
		want   types.UtxoValidationMode
		strict bool
	}{
		{want: types.UtxoValidationIgnore},
		{strict: true, want: types.UtxoValidationFail},
		{
			mode:   types.UtxoValidationWarn,
			strict: true,
			want:   types.UtxoValidationWarn,
		},
		{mode: types.UtxoValidationIgnore, want: types.UtxoValidationIgnore},
	}
	for _, tc := range cases {
		cfg := &Config{
			StrictUtxoValidation: tc.strict,
			UtxoValidationMode:   tc.mode,
		}
		assert.Equal(t, tc.want, cfg.utxoValidationMode())
	}
}
