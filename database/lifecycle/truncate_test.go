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

package lifecycle_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// seedCip163Certificate records a certificate through the public database
// ingestion boundary, mirroring ledger's own account_expiry_rollback_test.go
// seedRollbackCertificate helper (a different package, so not directly
// reusable) -- this keeps the test independent of a concrete metadata
// plugin and its SQL schema.
func seedCip163Certificate(
	t *testing.T,
	db *database.Database,
	slot uint64,
	cert lcommon.Certificate,
) {
	t.Helper()
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[len(txID)-8:], slot)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithCertificates(cert)
	require.NoError(t, db.SetTransactionMetadataOnly(
		context.Background(),
		tx,
		ocommon.NewPoint(slot, txID),
		0,
		map[int]uint64{0: 0},
		nil, 0,
	))
}

func TestTruncateMidnightJournalsRemovesStateAfterTarget(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 3)
	idx, err := dbtest.NewIndexerDatabase(t, f.db, dbtest.IndexerOptions{})
	require.NoError(t, err)
	store := idx.Metadata()
	archive := testutil.NewPayloadArchive()
	store.(types.PayloadResolverSetter).SetPayloadResolver(archive)
	txn := store.Transaction(t.Context())
	defer txn.Rollback() //nolint:errcheck
	txn.(types.PayloadLocatorSetter).SetPayloadLocator(archive)
	for _, block := range f.blocks[1:] {
		require.NoError(t, store.CreateMidnightAriadneRollback(txn,
			&models.MidnightAriadneRollback{
				BlockNumber:    block.Number,
				Epoch:          block.Number,
				PreviousExists: true,
				PreviousDatum:  []byte{byte(block.Number)},
			},
		))
		require.NoError(t, store.CreateMidnightCandidateRemoval(txn,
			&models.MidnightCandidateRemoval{
				BlockNumber: block.Number,
				TxHash:      []byte{byte(block.Number)},
				OutputIndex: 0,
				Datum:       []byte{byte(block.Number)},
			},
		))
		require.NoError(t, store.UpsertMidnightEpochTransition(txn,
			&models.MidnightEpochTransition{
				BlockNumber:    block.Number,
				PreviousEpoch:  block.Number - 1,
				PreviousExists: true,
			},
		))
	}
	require.NoError(t, txn.Commit())
	require.NoError(t, store.SetBackfillCheckpoint(&models.BackfillCheckpoint{
		Phase:     "midnight",
		LastSlot:  f.blocks[2].Slot,
		Completed: true,
	}, nil))

	require.NoError(t, lifecycle.TruncateMidnightJournals(
		t.Context(), idx,
		ocommon.NewPoint(f.blocks[1].Slot, f.blocks[1].Hash),
		f.blocks[1].Number,
	))

	rollbacksAtTarget, err := store.FindMidnightAriadneRollbacksByBlock(
		nil, f.blocks[1].Number,
	)
	require.NoError(t, err)
	require.Len(t, rollbacksAtTarget, 1)
	rollbacksAfterTarget, err := store.FindMidnightAriadneRollbacksByBlock(
		nil, f.blocks[2].Number,
	)
	require.NoError(t, err)
	require.Empty(t, rollbacksAfterTarget)

	candidateRemovalsAtTarget, err := store.FindMidnightCandidateRemovalsByBlock(
		nil, f.blocks[1].Number,
	)
	require.NoError(t, err)
	require.Len(t, candidateRemovalsAtTarget, 1)
	candidateRemovalsAfterTarget, err := store.FindMidnightCandidateRemovalsByBlock(
		nil, f.blocks[2].Number,
	)
	require.NoError(t, err)
	require.Empty(t, candidateRemovalsAfterTarget)

	epochTransitionAtTarget, err := store.GetMidnightEpochTransitionByBlock(
		nil, f.blocks[1].Number,
	)
	require.NoError(t, err)
	require.NotNil(t, epochTransitionAtTarget)
	epochTransitionAfterTarget, err := store.GetMidnightEpochTransitionByBlock(
		nil, f.blocks[2].Number,
	)
	require.NoError(t, err)
	require.Nil(t, epochTransitionAfterTarget)

	checkpoint, err := store.GetBackfillCheckpoint("midnight", nil)
	require.NoError(t, err)
	require.NotNil(t, checkpoint)
	require.Equal(t, f.blocks[1].Slot, checkpoint.LastSlot)
	require.False(t, checkpoint.Completed)
}

// buildTestChain creates n blocks (IDs and Numbers 1..n) and sets the tip
// to the last one.
func buildTestChain(t *testing.T, n uint64) *chainFixture {
	t.Helper()
	db := newTestDB(t)
	blocks := make([]models.Block, 0, n)
	for id := uint64(1); id <= n; id++ {
		block := testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	store, set := utxoSetFor(t, blocks)
	return &chainFixture{db: db, blocks: blocks, store: store, set: set}
}

type chainFixture struct {
	db     *database.Database
	blocks []models.Block
	// store is the live UTxO set, holding one output per block, and set is
	// the option that has Truncate roll it back with db.
	store *badgerutxo.Store
	set   lifecycle.TruncateOption
}

// utxoSetFor opens a live UTxO set that applied blocks, one output each.
func utxoSetFor(
	t *testing.T,
	blocks []models.Block,
) (*badgerutxo.Store, lifecycle.TruncateOption) {
	t.Helper()
	store := openUtxoSetStore(t, t.TempDir())
	for _, b := range blocks {
		_, err := store.Apply(
			ocommon.NewPoint(b.Slot, b.Hash),
			[]badgerutxo.Output{{
				Ref:  badgerutxo.NewRef(b.Hash, 0),
				Cbor: []byte{0x80, byte(b.ID)},
			}},
			nil,
		)
		require.NoError(t, err)
	}
	return store, lifecycle.WithTruncateUtxoSet(
		dblifecycle.TruncateUtxoSet{Store: store},
	)
}

// buildSparseTestChain creates blocks at the given, deliberately
// non-contiguous internal IDs (Slot/Number scaled from each ID the same
// way testBlock does) and sets the tip to the last one -- simulating the
// real gap Mithril bootstrap/drain import can legitimately leave in the
// block-ID space (see database.BlockAtOrAfterIndex's doc comment and its
// own TestBlockAtOrAfterIndexSkipsSparseIndexes).
func buildSparseTestChain(t *testing.T, ids []uint64) *chainFixture {
	t.Helper()
	db := newTestDB(t)
	blocks := make([]models.Block, 0, len(ids))
	for _, id := range ids {
		block := testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	store, set := utxoSetFor(t, blocks)
	return &chainFixture{db: db, blocks: blocks, store: store, set: set}
}

// TestTruncateReportsActualDeletedCountForSparseIndex verifies that
// blocksRemoved counts blocks actually deleted, not tipBlock.ID -
// target.ID: that subtraction is only a valid count when every ID in the
// range is a real block, but for a chain with Mithril bootstrap/drain gaps
// that range is merely an upper bound. With a gap of ~997 missing IDs
// between target (ID 3) and tip (ID 1002), an ID-subtraction count would
// report 999 blocks removed even though only 3 (IDs 1000-1002) actually
// exist and get deleted.
func TestTruncateReportsActualDeletedCountForSparseIndex(t *testing.T) {
	t.Parallel()

	f := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})
	target := f.blocks[2] // ID 3

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0, f.set)

	require.NoError(t, err)
	require.Equal(
		t, uint64(3), blocksRemoved,
		"must count the 3 blocks (IDs 1000-1002) actually deleted, not "+
			"tipBlock.ID-target.ID (999)",
	)

	for _, b := range f.blocks[:3] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
	for _, b := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
}

// TestTruncateRemovesBlocksAndIsIdempotentAtTip verifies that Truncate
// removes everything past the target and is a zero-block no-op if repeated.
func TestTruncateRemovesBlocksAndIsIdempotentAtTip(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	target := f.blocks[2] // truncate to block 3, removing 4 and 5

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(), f.db, target, 0, f.set)

	require.NoError(t, err)
	require.Equal(t, uint64(2), blocksRemoved)

	for _, b := range f.blocks[:3] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
	for _, b := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}

	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Slot, tip.Point.Slot)

	// Truncating again to the same (now current) target is a no-op, not
	// an error, and reports zero blocks removed.
	blocksRemoved, err = lifecycle.Truncate(
		context.Background(), f.db, target, 0, f.set)

	require.NoError(t, err)
	require.Zero(t, blocksRemoved)
}

// A truncate removes blocks above the target, so a history-expiry cursor past
// the target is lowered to it.
func TestTruncateLowersHistoryExpiryCursor(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	require.NoError(t, f.db.SetChainState(
		database.HistoryExpiryCursorKey,
		strconv.FormatUint(f.blocks[4].Slot, 10),
		nil,
	))

	_, err := lifecycle.Truncate(
		context.Background(), f.db, f.blocks[2], 0, f.set)

	require.NoError(t, err)

	got, err := f.db.GetChainState(database.HistoryExpiryCursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(f.blocks[2].Slot, 10), got)
}

// TestTruncateRemovesBlobTailAheadOfMetadataTip reproduces the live truncate
// state where blockfetch has persisted blocks beyond the last block applied to
// ledger metadata. The operator target can equal the metadata tip while the
// indexed blob chain still has a speculative tail; that tail must be removed
// rather than treating the truncate as a no-op.
func TestTruncateRemovesBlobTailAheadOfMetadataTip(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	target := f.blocks[2]
	require.NoError(t, f.db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: target.Slot,
			Hash: target.Hash,
		},
		BlockNumber: target.Number,
	}, nil))

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0, f.set)

	require.NoError(t, err)
	require.Equal(t, uint64(2), blocksRemoved)

	for _, block := range f.blocks[:3] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.NoError(t, err)
	}
	for _, block := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Slot, tip.Point.Slot)
	require.Equal(t, target.Hash, tip.Point.Hash)
}

func TestTruncateRejectsTargetInUnappliedBlobTail(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	metadataTarget := f.blocks[2]
	require.NoError(t, f.db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: metadataTarget.Slot,
			Hash: metadataTarget.Hash,
		},
		BlockNumber: metadataTarget.Number,
	}, nil))

	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		f.blocks[3],
		0, f.set)

	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	for _, block := range f.blocks {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsTargetImmediatelyFollowedBySameSlotBlock guards
// against a real blob/metadata divergence bug: if the block
// immediately after target shares its exact slot (the Byron epoch-
// boundary-block pattern), DeleteBlocksAfter would remove that later
// block from the blob store by ID, while database.TruncateAfterSlot's
// slot-based cutoff would keep its metadata rows, since they share
// target's own slot -- leaving transactions/certificates/UTxOs and other
// metadata for a block no longer present in the blob store. Truncate must
// refuse such a target outright, before deleting anything, rather than
// let that divergence happen.
func TestTruncateRejectsTargetImmediatelyFollowedBySameSlotBlock(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	block1 := testBlock(1, 0x01)
	require.NoError(t, db.BlockCreate(block1, nil))

	// block2 shares block1's slot -- the Byron pattern where an epoch
	// boundary block and the epoch's first regular block occupy the same
	// slot -- and immediately follows it in the ID space.
	block2 := testBlock(2, 0x02)
	block2.Slot = block1.Slot
	require.NoError(t, db.BlockCreate(block2, nil))

	block3 := testBlock(3, 0x03)
	require.NoError(t, db.BlockCreate(block3, nil))

	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block3.Slot, Hash: block3.Hash},
		BlockNumber: block3.Number,
	}, nil))

	_, set := utxoSetFor(t, []models.Block{block1, block2, block3})
	_, err := lifecycle.Truncate(context.Background(), db, block1, 0, set)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "same slot")

	// Nothing must have been touched: block2 and block3 must still exist.
	_, err = database.BlockByHash(context.Background(), db, block2.Hash)
	require.NoError(t, err, "block2 must survive a rejected truncate")
	_, err = database.BlockByHash(context.Background(), db, block3.Hash)
	require.NoError(t, err, "block3 must survive a rejected truncate")

	// A target with no same-slot successor (block2 itself: block3's slot
	// differs) must still be allowed -- the rejection is specific to the
	// same-slot case, not a blanket block on this chain.
	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		db,
		block2,
		0,
		set)

	require.NoError(t, err)
	require.Equal(t, uint64(1), blocksRemoved)
}

// TestTruncateRejectsTargetBeforeMithrilBoundary verifies that a target
// before the recorded Mithril floor is refused, but exactly at it is allowed.
func TestTruncateRejectsTargetBeforeMithrilBoundary(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	boundarySlot := f.blocks[3].Slot
	require.NoError(t, f.db.SetChainState(
		"mithril_ledger_slot", strconv.FormatUint(boundarySlot, 10), nil,
	))

	// blocks[2] is before the boundary; truncating there must be refused.
	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		f.blocks[2],
		0, f.set)

	require.Error(t, err)

	// blocks[3] is exactly at the boundary and must be allowed.
	blocksRemoved, err := lifecycle.Truncate(
		context.Background(), f.db, f.blocks[3], 0, f.set)

	require.NoError(t, err)
	require.Equal(t, uint64(1), blocksRemoved)
}

// A Mithril sync records its trust boundary only when it completes, so a
// database it left in progress holds imported outputs above any target and
// no boundary to refuse it. Truncating there would delete outputs a resumed
// import never rewrites.
func TestTruncateRejectsIncompleteMithrilSync(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	require.NoError(t, f.db.SetChainState("sync_status", "in_progress", nil))

	_, err := lifecycle.Truncate(
		context.Background(), f.db, f.blocks[2], 0, f.set)

	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	// The set holds the outputs every block above the target added.
	requireSetAt(t, f.store, 5)
	_, err = database.BlockByHash(
		context.Background(), f.db, f.blocks[4].Hash,
	)
	require.NoError(t, err, "a refused truncate must keep blocks")
}

// A Mithril import that completes after a truncate was interrupted raises the
// trust boundary and imports outputs at that slot. The marker's older floor
// must not authorize deleting them.
func TestTruncateRefusesResumeAfterMithrilBoundaryRises(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, f.set)
	require.ErrorIs(t, err, context.Canceled)

	// An import above the interrupted truncate's target writes its outputs.
	imported := f.blocks[4]
	_, err = f.store.Apply(
		ocommon.NewPoint(imported.Slot, imported.Hash),
		[]badgerutxo.Output{{
			Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{0x62}, 32), 0),
			Cbor: []byte{0x80, 0x62},
		}},
		nil,
	)
	require.NoError(t, err)
	setTip, _, err := f.store.Tip()
	require.NoError(t, err)
	setCount, err := f.store.Count()
	require.NoError(t, err)
	require.NoError(t, f.db.SetChainState(
		"mithril_ledger_slot",
		strconv.FormatUint(f.blocks[4].Slot, 10),
		nil,
	))

	_, err = lifecycle.Truncate(
		context.Background(), f.db, models.Block{}, 1, f.set)

	require.ErrorContains(t, err, "above the pending truncate")
	gotTip, _, err := f.store.Tip()
	require.NoError(t, err)
	require.Equal(t, setTip, gotTip, "a refused resume must keep the set")
	gotCount, err := f.store.Count()
	require.NoError(t, err)
	require.Equal(t, setCount, gotCount, "a refused resume must keep outputs")
	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending, "a refused resume must keep the marker")
}

// A Mithril sync that starts after an interrupted truncate has recorded no
// boundary yet, so the resume is refused while it is in progress.
func TestTruncateRefusesResumeDuringMithrilSync(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, f.set)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, f.db.SetChainState("sync_status", "in_progress", nil))

	_, err = lifecycle.Truncate(
		context.Background(), f.db, models.Block{}, 1, f.set)

	require.ErrorContains(t, err, "in progress")
	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending)
}

// TestTruncateDetectsAndResumesInterruptedBatchedDelete verifies that a
// cancellation after one blob-delete batch cannot leave an undetectable
// database with metadata pointing at missing blocks. The durable marker must
// survive the failure, and a later call must finish the original operation
// without needing to resolve the now-missing old tip first.
func TestTruncateDetectsAndResumesInterruptedBatchedDelete(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	// n=3: Truncate's own pre-mutation ctx.Err() check (verifying a
	// pre-cancelled caller never reaches setPendingTruncate) consumes the
	// first free check, then DeleteBlocksAfter's outer-loop check and the
	// first batch's one in-range item each consume one more -- three free
	// checks land the cancellation right after the first block's delete
	// commits, exactly like the two-check budget did before that
	// pre-mutation check existed.
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}

	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, f.set)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, f.blocks[1].ID, pending.TargetID)
	require.Equal(t, f.blocks[4].ID, pending.TipID)

	// The first batch committed, while metadata still reports the old tip.
	_, err = f.db.BlockByIndex(f.blocks[2].ID, nil)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].Slot, tip.Point.Slot)

	// A retry resumes from the marker. The target argument is deliberately
	// empty because the durable marker is now the authoritative operation.
	_, err = lifecycle.Truncate(
		context.Background(),
		f.db,
		models.Block{},
		1, f.set)

	require.NoError(t, err)

	pending, err = lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(t, pending)
	tip, err = f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].Slot, tip.Point.Slot)
	for _, block := range f.blocks[2:] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
}

// TestGetPendingTruncateRejectsMarkerWithCorruptedTipID verifies that changing
// the deletion upper bound in an otherwise valid durable marker is detected.
func TestGetPendingTruncateRejectsMarkerWithCorruptedTipID(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, f.set)
	require.ErrorIs(t, err, context.Canceled)

	marker, err := f.db.GetChainState(database.PendingTruncateKey, nil)
	require.NoError(t, err)
	var pending lifecycle.PendingTruncate
	require.NoError(t, json.Unmarshal([]byte(marker), &pending))
	pending.TipID--
	corruptedMarker, err := json.Marshal(pending)
	require.NoError(t, err)
	require.NoError(t, f.db.SetChainState(
		database.PendingTruncateKey,
		string(corruptedMarker),
		nil,
	))

	got, err := lifecycle.GetPendingTruncate(f.db)
	require.Error(t, err)
	require.ErrorContains(t, err, "checksum mismatch")
	require.Nil(t, got)

	// Reading the corrupted marker must not continue the interrupted delete.
	for _, b := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsPendingMarkerAfterBlobTipAdvance verifies that a valid
// marker cannot resume against blobs appended after its authenticated tip.
func TestTruncateRejectsPendingMarkerAfterBlobTipAdvance(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, f.set)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, f.blocks[4].ID, pending.TipID)

	advancedTip := testBlock(6, 0x06)
	require.NoError(t, f.db.BlockCreate(advancedTip, nil))

	_, err = lifecycle.Truncate(
		context.Background(),
		f.db,
		models.Block{},
		1, f.set)

	require.Error(t, err)
	require.ErrorContains(t, err, "does not match newer current blob tip")

	// Recovery must fail before deleting any additional blobs or truncating
	// metadata. The already-committed first batch remains the only mutation.
	for _, block := range []models.Block{
		f.blocks[3],
		f.blocks[4],
		advancedTip,
	} {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.NoError(t, err)
	}
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].Slot, tip.Point.Slot)
}

// TestTruncateResumesWithStaleHighestIndexAfterPartialCloudDelete models a
// cloud DeleteBlock failure after the block object is removed but before its
// "bi" index is removed. Recovery must authenticate the remaining index
// directly and retry cleanup instead of requiring the missing block object.
func TestTruncateResumesWithStaleHighestIndexAfterPartialCloudDelete(
	t *testing.T,
) {
	t.Parallel()

	f := buildTestChain(t, 5)
	target := f.blocks[1]
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, target, 1, f.set)
	require.ErrorIs(t, err, context.Canceled)

	tip := f.blocks[4]
	txn := f.db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return f.db.Blob().Delete(
			txn.Blob(),
			types.BlockBlobKey(tip.Slot, tip.Hash),
		)
	}))

	// The highest "bi" entry still exists, but resolving it through the
	// deleted block object reproduces the state that previously blocked
	// GetPendingTruncate before cleanup could retry.
	_, err = f.db.BlockByIndex(tip.ID, nil)
	require.Error(t, err)

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		models.Block{},
		1, f.set)

	require.NoError(t, err)
	require.Equal(t, uint64(2), blocksRemoved)

	for _, block := range f.blocks[2:] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.Error(t, err)
	}
	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(t, pending)
}

// TestTruncateResumesWhenBlobTipWasAheadOfMetadataTip verifies that marker
// validation does not confuse the latest downloaded blob with the applied
// metadata tip. BlockFetch is allowed to persist such a speculative tail.
func TestTruncateResumesWhenBlobTipWasAheadOfMetadataTip(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	appliedTip := f.blocks[3]
	require.NoError(t, f.db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: appliedTip.Slot,
			Hash: appliedTip.Hash,
		},
		BlockNumber: appliedTip.Number,
	}, nil))

	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, f.set)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, f.blocks[4].ID, pending.TipID)

	_, err = lifecycle.Truncate(
		context.Background(), f.db, models.Block{}, 1, f.set)

	require.NoError(t, err)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].Slot, tip.Point.Slot)
}

// TestTruncateRejectsPreCancelledContextWithoutRecordingMarker verifies
// that a context already cancelled before Truncate ever attempts a
// mutation is reported the same way every other pre-mutation validation
// failure is -- ErrTruncateNotStarted-wrapped -- and, critically, does
// not leave a durable pending marker behind: without this, a caller would
// be forced to "resume" an operation that in fact never touched the
// database at all.
func TestTruncateRejectsPreCancelledContextWithoutRecordingMarker(
	t *testing.T,
) {
	t.Parallel()

	f := buildTestChain(t, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 0, f.set)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(
		t,
		pending,
		"a pre-cancelled Truncate must not record a pending marker",
	)
}

// The resolvers only read, so every case shares one chain: a dense one for the
// plain lookups and the nearest-ancestor rule, and a sparse one whose ID gap
// would make a per-ID probe skip or misreport a block.
func TestResolveTarget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := buildTestChain(t, 5) // blocks[i].Slot == (i+1)*10
	t.Run("by hash", func(t *testing.T) {
		target, err := lifecycle.ResolveTargetByHash(ctx, f.db, f.blocks[2].Hash)
		require.NoError(t, err)
		require.Equal(t, f.blocks[2].ID, target.ID)
	})
	t.Run("by slot resolves to the nearest ancestor", func(t *testing.T) {
		target, err := lifecycle.ResolveTargetBySlot(ctx, f.db, f.blocks[2].Slot)
		require.NoError(t, err)
		require.Equal(t, f.blocks[2].ID, target.ID)

		target, err = lifecycle.ResolveTargetBySlot(ctx, f.db, f.blocks[2].Slot+5)
		require.NoError(t, err)
		require.Equal(t, f.blocks[2].ID, target.ID)

		tipTarget, err := lifecycle.ResolveTargetBySlot(ctx, f.db, f.blocks[4].Slot+100)
		require.NoError(t, err)
		require.Equal(t, f.blocks[4].ID, tipTarget.ID)
	})
	t.Run("by number", func(t *testing.T) {
		for _, b := range f.blocks {
			target, err := lifecycle.ResolveTargetByNumber(ctx, f.db, b.Number)
			require.NoError(t, err)
			require.Equal(t, b.ID, target.ID)
		}
		_, err := lifecycle.ResolveTargetByNumber(ctx, f.db, 100)
		require.Error(t, err, "a number ahead of the tip must not resolve")
	})

	sparse := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})
	t.Run("sparse index gap", func(t *testing.T) {
		for _, b := range []models.Block{sparse.blocks[1], sparse.blocks[4]} {
			bySlot, err := lifecycle.ResolveTargetBySlot(ctx, sparse.db, b.Slot)
			require.NoError(t, err)
			require.Equal(t, b.ID, bySlot.ID)
			byNumber, err := lifecycle.ResolveTargetByNumber(ctx, sparse.db, b.Number)
			require.NoError(t, err)
			require.Equal(t, b.ID, byNumber.ID)
		}
	})
}

// Every rejected target leaves the chain untouched, so the cases share one
// chain and re-check it after each.
func TestTruncateRejectsInvalidTargets(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	tip := f.blocks[len(f.blocks)-1]
	requireChainIntact := func(t *testing.T) {
		t.Helper()
		for _, b := range f.blocks {
			_, err := f.db.BlockByIndex(b.ID, nil)
			require.NoError(t, err)
		}
	}
	for _, tc := range []struct {
		name   string
		target models.Block
		// aheadOfTip targets fail with a different error than a mismatch.
		aheadOfTip bool
	}{
		{name: "ahead of tip", target: testBlock(99, 0x63), aheadOfTip: true},
		{name: "mismatched hash", target: withHash(f.blocks[2], bytes.Repeat([]byte{0xFF}, 32))},
		{name: "mismatched slot", target: withSlot(f.blocks[2], f.blocks[2].Slot+1)},
		{name: "tip ID with mismatched hash", target: withHash(tip, bytes.Repeat([]byte{0xFF}, 32))},
		{name: "tip ID with mismatched slot", target: withSlot(tip, tip.Slot+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := lifecycle.Truncate(
				context.Background(), f.db, tc.target, 0, f.set)

			require.Error(t, err)
			if !tc.aheadOfTip {
				require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
			}
			requireChainIntact(t)
		})
	}
}

func withHash(b models.Block, hash []byte) models.Block {
	b.Hash = hash
	return b
}

func withSlot(b models.Block, slot uint64) models.Block {
	b.Slot = slot
	return b
}

// The marker is cleared after the rewind commits, so a stop between the two
// leaves it over a truncate that already finished. Resuming it must complete
// without changing the truncated chain.
func TestTruncateResumesAMarkerLeftAfterTheRewindCommitted(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, f.set)
	require.ErrorIs(t, err, context.Canceled)
	marker, err := f.db.GetChainState(database.PendingTruncateKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, marker)

	_, err = lifecycle.Truncate(
		context.Background(), f.db, models.Block{}, 1, f.set)

	require.NoError(t, err)
	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(t, pending, "a finished truncate cleared its marker")

	require.NoError(t, f.db.SetChainState(
		database.PendingTruncateKey, marker, nil,
	))
	_, err = lifecycle.Truncate(
		context.Background(), f.db, models.Block{}, 1, f.set)

	require.NoError(t, err)
	pending, err = lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(t, pending)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].Slot, tip.Point.Slot)
	_, err = f.db.BlockByIndex(f.blocks[1].ID, nil)
	require.NoError(t, err)
}

// A pending-truncate marker is only trusted when it names a real block, so
// both malformed shapes are rejected against one chain.
func TestGetPendingTruncateRejectsInvalidMarker(t *testing.T) {
	t.Parallel()

	f := buildTestChain(t, 5)
	target := f.blocks[1]
	forgedHash := bytes.Repeat([]byte{0xFF}, 32)
	for name, marker := range map[string]string{
		"zero target ID": `{"tipId":5}`,
		"hash not matching the stored block": fmt.Sprintf(
			`{"targetId":%d,"targetSlot":%d,"targetHash":%q,"tipId":%d}`,
			target.ID, target.Slot,
			base64.StdEncoding.EncodeToString(forgedHash), f.blocks[4].ID,
		),
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, f.db.SetChainState(
				database.PendingTruncateKey, marker, nil,
			))
			pending, err := lifecycle.GetPendingTruncate(f.db)
			require.Error(t, err)
			require.Nil(t, pending)
		})
	}
}

// A truncate removes every block above the target, so a rollback high-water
// mark above the target index is lowered to it. A node restarted on the
// truncated chain would otherwise measure rollbacks from a block it no longer
// holds and refuse any rollback within K of the new tip.
func TestTruncateLowersRollbackHighWater(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		seed string
		want string
	}{
		{name: "above target is lowered", seed: "5", want: "3"},
		{name: "at target is kept", seed: "3", want: "3"},
		{name: "below target is kept", seed: "2", want: "2"},
		{name: "absent is kept", seed: "", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := buildTestChain(t, 5)
			if tc.seed != "" {
				require.NoError(t, f.db.SetChainState(
					database.RollbackHighWaterKey, tc.seed, nil,
				))
			}

			_, err := lifecycle.Truncate(
				context.Background(), f.db, f.blocks[2], 0, f.set)

			require.NoError(t, err)

			got, err := f.db.GetChainState(
				database.RollbackHighWaterKey, nil,
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
