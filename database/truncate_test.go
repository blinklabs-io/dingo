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
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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
// During rollback, TruncateAfterSlot runs inside a
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

// TestTruncateAfterSlotObservesDuration verifies the rollback-truncation sweep
// is measured. Without this the only symptom of a multi-second truncation is
// the ledger going silent, which is what made a ~90s chain freeze invisible.
func TestTruncateAfterSlotObservesDuration(t *testing.T) {
	db := newTestDB(t)

	reg := prometheus.NewRegistry()
	require.NoError(t, RegisterTruncateMetrics(reg))
	// Registering a second registry must not panic or error.
	require.NoError(t, RegisterTruncateMetrics(prometheus.NewRegistry()))

	before := collectHistogramCount(t, reg, truncateResultSuccess)

	targetBlock := testIndexedBlock(1500, 1, 0x15)
	require.NoError(t, db.BlockCreate(targetBlock, nil))
	point := ocommon.Point{Slot: 1500, Hash: targetBlock.Hash}
	_, _, err := db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err)

	require.Equal(
		t,
		before+1,
		collectHistogramCount(t, reg, truncateResultSuccess),
		"TruncateAfterSlot must record its duration",
	)
}

// TestTruncateAfterSlotRecordsFailureSeparately verifies a failed sweep is not
// reported as a completed truncation. The duration is still observed -- a sweep
// that failed held the ledger write lock for that long -- but under the failure
// label, and the log line says failed rather than complete.
func TestTruncateAfterSlotRecordsFailureSeparately(t *testing.T) {
	db := newTestDB(t)

	reg := prometheus.NewRegistry()
	require.NoError(t, RegisterTruncateMetrics(reg))
	beforeOK := collectHistogramCount(t, reg, truncateResultSuccess)
	beforeFail := collectHistogramCount(t, reg, truncateResultFailure)

	// A rollback point above slot 0 whose block row does not exist makes
	// TruncateAfterSlot fail when it looks the block up for the new tip.
	point := ocommon.Point{
		Slot: 4242,
		Hash: bytes.Repeat([]byte{0x99}, 32),
	}
	_, _, err := db.TruncateAfterSlot(point, 0, nil)
	require.Error(t, err)

	assert.Equal(
		t,
		beforeOK,
		collectHistogramCount(t, reg, truncateResultSuccess),
		"a failed sweep must not be counted as a success",
	)
	assert.Equal(
		t,
		beforeFail+1,
		collectHistogramCount(t, reg, truncateResultFailure),
		"a failed sweep must still record its duration under the failure label",
	)
}

// TestRegisterTruncateMetricsNilRegistry verifies a nil registry is tolerated,
// matching the other database metric registrations.
func TestRegisterTruncateMetricsNilRegistry(t *testing.T) {
	require.NoError(t, RegisterTruncateMetrics(nil))
}

func collectHistogramCount(
	t *testing.T,
	reg *prometheus.Registry,
	result string,
) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() !=
			"dingo_database_truncate_after_slot_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "result" &&
					label.GetValue() == result {
					return metric.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	// A labelled child that has never been observed is absent from the
	// gathered output, which is a legitimate count of zero.
	return 0
}

// TestTruncateAfterSlotAllowsTargetWithPrunedNonceWhenCheckpointSurvives
// reproduces a live-incident finding: dingo prunes non-checkpoint
// block_nonce rows older than 3 epochs behind the current tip
// (ledger/state.go's cleanupBlockNoncesBefore), keeping only each epoch's
// single checkpoint row. A disaster-recovery truncate
// (database/lifecycle/truncate.go) can roll the tip back to an arbitrary
// historical point that is *older* than that 3-epoch retention window --
// exactly the case that already ran against a live database and produced a
// wrong epoch nonce for the epoch immediately following the truncate point
// (every VRF verification in that epoch then failed against real, canonical
// chain headers on 4 independent nodes).
//
// TruncateAfterSlot fetches the new tip's evolving nonce with a single
// exact-point lookup (GetBlockNonce(point)). GetBlockNonce returns (nil,
// nil) -- no error -- when no row matches the point, so when the target
// block's own block_nonce row has already been pruned (its epoch's
// checkpoint is some other, earlier block), TruncateAfterSlot used to
// silently return an empty nonce instead of failing loudly. A node restart
// after such a truncate then seeded the resumed evolving-nonce fold
// (LedgerState.loadTip -> ledgerProcessBlocks' runningNonce) from empty
// bytes, corrupting every block nonce computed for the remainder of the
// epoch and, through it, the following epoch's nonce.
//
// TruncateAfterSlot now allows this truncate to proceed with a nil nonce
// instead: as long as a checkpoint row survives at or before the target's
// slot, LedgerState's startup heal (healTruncateGapBlockNonces) can fold the
// evolving nonce forward from it through the still-present block CBOR
// between checkpoint and target -- this package has no ledger/era knowledge
// to do that fold itself (see AGENTS.md's database/ledger boundary), so it
// only verifies a checkpoint exists here and defers the reconstruction.
// TestHealTruncateGapBlockNonces_ReconstructsFromCheckpoint (ledger package)
// proves that reconstruction produces the correct nonce.
//
// This test does not fold real VRF outputs (it does not need real block
// content to demonstrate the defect): it shows that a truncate target whose
// own block_nonce row was pruned succeeds with a nil nonce -- not a silently
// wrong one -- when an earlier, non-pruned checkpoint row exists for the
// same epoch.
func TestTruncateAfterSlotAllowsTargetWithPrunedNonceWhenCheckpointSurvives(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	// Epoch checkpoint block: the first block persisted in its epoch,
	// whose block_nonce row survives the 3-epoch retention pruning
	// (ledger/state.go's cleanupBlockNoncesBefore keeps checkpoints
	// indefinitely).
	checkpointBlock := testIndexedBlock(1000, 1, 0x10)
	require.NoError(t, db.BlockCreate(checkpointBlock, nil))
	checkpointNonce := bytes.Repeat([]byte{0xc1}, 32)
	require.NoError(t, db.SetBlockNonce(
		checkpointBlock.Hash,
		checkpointBlock.Slot,
		checkpointNonce,
		true, // isCheckpoint
		nil,
	))

	// Truncate target: a later, ordinary (non-checkpoint) block in the
	// same epoch. In production this is the disaster-recovery truncate's
	// target -- often many epochs behind the pre-truncate tip. Given a
	// Praos (post-Byron) block type explicitly: testIndexedBlock defaults
	// to Type 1 (byron.BlockTypeByronMain), which the fix under test
	// deliberately exempts from the empty-nonce check since Byron blocks
	// have no Praos nonce at all. babbage.BlockTypeBabbage matches the
	// live incident, which occurred entirely within the Babbage era.
	targetBlock := testIndexedBlock(1500, 2, 0x15)
	targetBlock.Type = babbage.BlockTypeBabbage
	require.NoError(t, db.BlockCreate(targetBlock, nil))
	targetNonce := bytes.Repeat([]byte{0xc2}, 32)
	require.NoError(t, db.SetBlockNonce(
		targetBlock.Hash,
		targetBlock.Slot,
		targetNonce,
		false, // isCheckpoint
		nil,
	))

	// Sanity check: before pruning, TruncateAfterSlot correctly returns
	// the target block's own nonce.
	point := ocommon.Point{Slot: targetBlock.Slot, Hash: targetBlock.Hash}
	_, nonceBeforePruning, err := db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err)
	require.Equal(t, targetNonce, nonceBeforePruning,
		"sanity check: truncate must return the target's own nonce "+
			"while its row still exists")

	// Simulate the routine 3-epoch retention pruning that runs during
	// normal operation: it removes the target's own (non-checkpoint) row
	// while preserving the earlier checkpoint. In production this can
	// happen long before any truncate is requested, simply because the
	// live chain kept advancing for 3+ epochs after slot 1500.
	require.NoError(t, db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		targetBlock.Slot+1,
		nil,
	))
	prunedNonce, err := db.GetBlockNonce(point, nil)
	require.NoError(t, err)
	require.Empty(t, prunedNonce,
		"sanity check: the target's own block_nonce row must actually "+
			"be gone after retention pruning")
	survivingCheckpoint, err := db.GetBlockNonce(
		ocommon.Point{Slot: checkpointBlock.Slot, Hash: checkpointBlock.Hash},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, checkpointNonce, survivingCheckpoint,
		"sanity check: the epoch's checkpoint row must survive retention pruning")

	// Re-run the exact same truncate against the now-pruned target. Before
	// fix, this silently returned (Tip, nil-nonce, nil-error) --
	// the exact live-incident mechanism: a deep 'dingo database truncate'
	// landing on a pruned slot silently corrupted the resumed nonce chain,
	// causing every VRF verification in the following epoch to fail against
	// real, canonical chain headers on 4 independent Preview-testnet nodes.
	// It must still return a nil nonce here (there is nothing else to
	// return -- the target's own row is gone), but must NOT error now that
	// a checkpoint survives to reconstruct from: LedgerState's startup heal
	// is responsible for actually computing and persisting the correct
	// nonce before anything folds forward from it.
	_, nonceAfterPruning, err := db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err,
		"TruncateAfterSlot must allow a truncate whose target's block_nonce "+
			"row was pruned when an earlier checkpoint survives to "+
			"reconstruct from, deferring the fold to LedgerState's startup "+
			"heal instead of rejecting a genuinely recoverable truncate")
	require.Nil(t, nonceAfterPruning)
}

// TestTruncateAfterSlotRejectsTargetWithPrunedNonceAndNoCheckpoint verifies
// the fallback this package retains for the case reconstruction genuinely
// cannot handle: when no checkpoint row survives at or before the truncate
// target at all (e.g. block_nonce history predating checkpoint retention, or
// outright corruption), there is nothing for LedgerState's startup heal to
// fold forward from, so TruncateAfterSlot must still fail loudly instead of
// letting the tip end up with an unreconstructable nil nonce.
func TestTruncateAfterSlotRejectsTargetWithPrunedNonceAndNoCheckpoint(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	// Truncate target with its own (non-checkpoint) nonce row, but no
	// checkpoint row anywhere before it.
	targetBlock := testIndexedBlock(1500, 1, 0x15)
	targetBlock.Type = babbage.BlockTypeBabbage
	require.NoError(t, db.BlockCreate(targetBlock, nil))
	targetNonce := bytes.Repeat([]byte{0xc2}, 32)
	require.NoError(t, db.SetBlockNonce(
		targetBlock.Hash,
		targetBlock.Slot,
		targetNonce,
		false, // isCheckpoint
		nil,
	))

	point := ocommon.Point{Slot: targetBlock.Slot, Hash: targetBlock.Hash}

	// Simulate the target's own row being pruned (e.g. by routine 3-epoch
	// retention) with no checkpoint anywhere to fall back to.
	require.NoError(t, db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		targetBlock.Slot+1,
		nil,
	))
	prunedNonce, err := db.GetBlockNonce(point, nil)
	require.NoError(t, err)
	require.Empty(t, prunedNonce,
		"sanity check: the target's own block_nonce row must actually "+
			"be gone")

	_, nonceAfterPruning, err := db.TruncateAfterSlot(point, 0, nil)
	require.Error(t, err,
		"TruncateAfterSlot must reject a target whose block_nonce row was "+
			"pruned when no checkpoint survives to reconstruct from -- "+
			"there is nothing for the startup heal to fold forward from, "+
			"so silently continuing would corrupt every subsequent epoch's "+
			"VRF-verification nonce")
	require.Contains(t, err.Error(), "no earlier checkpoint exists")
	require.Nil(t, nonceAfterPruning)
}

// TestTruncateAfterSlotRestoresPoolDenormalizedFields verifies that
// truncating past a pool re-registration reverts the pool's denormalized
// pledge/cost/VRF/reward-account fields to the surviving prior
// registration's values, rather than leaving the discarded
// registration's values in place. RestorePoolStateAtSlot detects which
// pools need reverting by querying PoolRegistration rows with
// added_slot > the target slot -- exactly the rows DeleteCertificatesAfterSlot
// removes, so this only passes if pool state is restored before
// certificates are deleted, not after.
func TestTruncateAfterSlotRestoresPoolDenormalizedFields(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	targetBlock := testIndexedBlock(1500, 1, 0x15)
	require.NoError(t, db.BlockCreate(targetBlock, nil))

	poolKeyHash := bytes.Repeat([]byte{0x99}, 28)
	vrfBefore := bytes.Repeat([]byte{0xa1}, 32)
	rewardBefore := bytes.Repeat([]byte{0xb1}, 28)
	vrfAfter := bytes.Repeat([]byte{0xa2}, 32)
	rewardAfter := bytes.Repeat([]byte{0xb2}, 28)

	// Initial registration, before the truncate target.
	require.NoError(t, db.ImportPool(nil,
		&models.Pool{
			PoolKeyHash:   poolKeyHash,
			Pledge:        100,
			Cost:          200,
			VrfKeyHash:    vrfBefore,
			RewardAccount: rewardBefore,
		},
		&models.PoolRegistration{
			PoolKeyHash:   poolKeyHash,
			AddedSlot:     1000,
			Pledge:        100,
			Cost:          200,
			VrfKeyHash:    vrfBefore,
			RewardAccount: rewardBefore,
		},
	))

	// Re-registration with different terms, after the truncate target.
	// This is the one truncate must discard, restoring the pool to its
	// pre-re-registration state.
	require.NoError(t, db.ImportPool(nil,
		&models.Pool{
			PoolKeyHash:   poolKeyHash,
			Pledge:        999,
			Cost:          888,
			VrfKeyHash:    vrfAfter,
			RewardAccount: rewardAfter,
		},
		&models.PoolRegistration{
			PoolKeyHash:   poolKeyHash,
			AddedSlot:     2000,
			Pledge:        999,
			Cost:          888,
			VrfKeyHash:    vrfAfter,
			RewardAccount: rewardAfter,
		},
	))

	// Sanity check: the pool currently reflects the later registration.
	poolBefore, err := db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(999), uint64(poolBefore.Pledge))

	point := ocommon.Point{Slot: 1500, Hash: targetBlock.Hash}
	_, _, err = db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err)

	poolAfter, err := db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), uint64(poolAfter.Pledge),
		"pledge must revert to the surviving pre-truncate registration")
	require.Equal(t, uint64(200), uint64(poolAfter.Cost),
		"cost must revert to the surviving pre-truncate registration")
	require.Equal(t, vrfBefore, poolAfter.VrfKeyHash,
		"vrf_key_hash must revert to the surviving pre-truncate registration")
	require.Equal(t, rewardBefore, poolAfter.RewardAccount,
		"reward_account must revert to the surviving pre-truncate registration")
}

// TestRollbackAfterSlotLowersHistoryExpiryCursor verifies a slot rollback
// lowers the expiry cursor to the rollback point and leaves a cursor at or
// below it alone. Replay stores blocks only above the rollback point, so a
// cursor there still covers every replayed block, and an untouched lower
// cursor keeps routine rollbacks from restarting the scan at slot 0.
func TestRollbackAfterSlotLowersHistoryExpiryCursor(t *testing.T) {
	rollbacks := []struct {
		name     string
		rollback func(*Database, ocommon.Point) error
	}{
		{
			name: "TruncateAfterSlot",
			rollback: func(db *Database, point ocommon.Point) error {
				_, _, err := db.TruncateAfterSlot(point, 0, nil)
				return err
			},
		},
		{
			name: "RollbackMetadataAfterSlot",
			rollback: func(db *Database, point ocommon.Point) error {
				return db.RollbackMetadataAfterSlot(point, 0, nil)
			},
		},
	}
	cursors := []struct {
		name   string
		cursor string
		want   string
	}{
		{name: "below point", cursor: "1400", want: "1400"},
		{name: "at point", cursor: "1500", want: "1500"},
		{name: "above point", cursor: "1600", want: "1500"},
		{name: "absent", cursor: "", want: ""},
	}
	for _, rb := range rollbacks {
		for _, tc := range cursors {
			t.Run(rb.name+"/"+tc.name, func(t *testing.T) {
				db := newTestDB(t)
				targetBlock := testIndexedBlock(1500, 1, 0x15)
				require.NoError(t, db.BlockCreate(targetBlock, nil))
				if tc.cursor != "" {
					require.NoError(
						t,
						db.SetSyncState(HistoryExpiryCursorSyncKey, tc.cursor, nil),
					)
				}
				point := ocommon.Point{Slot: 1500, Hash: targetBlock.Hash}
				require.NoError(t, rb.rollback(db, point))
				got, err := db.GetSyncState(HistoryExpiryCursorSyncKey, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	}
}

// TestTruncateAfterSlotLoadsNonceForSlotZeroBlock covers a network whose
// first block is a real (non-Byron) block at slot 0. A rollback to that block
// is not a rollback to origin: the point carries the block's hash, so its
// height and nonce must load. Returning a nil nonce made the next applied
// block seed the evolving nonce from the genesis hash instead.
func TestTruncateAfterSlotLoadsNonceForSlotZeroBlock(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	block0 := testIndexedBlock(0, 1, 0x20)
	block0.Number = 0
	block0.Type = babbage.BlockTypeBabbage
	require.NoError(t, db.BlockCreate(block0, nil))
	nonce0 := bytes.Repeat([]byte{0xd0}, 32)
	require.NoError(t, db.SetBlockNonce(
		block0.Hash, block0.Slot, nonce0, true, nil,
	))
	block1 := testIndexedBlock(20, 2, 0x21)
	block1.Type = babbage.BlockTypeBabbage
	require.NoError(t, db.BlockCreate(block1, nil))
	require.NoError(t, db.SetBlockNonce(
		block1.Hash, block1.Slot, bytes.Repeat([]byte{0xd1}, 32), false, nil,
	))

	tip, nonce, err := db.TruncateAfterSlot(
		ocommon.Point{Slot: 0, Hash: block0.Hash}, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, nonce0, nonce,
		"rollback to a slot-0 block must return that block's nonce")
	require.Equal(t, block0.Hash, tip.Point.Hash)
	require.Equal(t, block0.Number, tip.BlockNumber)
}

// TestTruncateAfterSlotByronSlotZeroBlockHasNoNonce keeps the Byron-start
// behavior: a slot-0 Byron block carries no Praos nonce and is exempt from
// the empty-nonce check.
func TestTruncateAfterSlotByronSlotZeroBlockHasNoNonce(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	block0 := testIndexedBlock(0, 1, 0x30)
	block0.Number = 0
	block0.Type = byron.BlockTypeByronEbb
	require.NoError(t, db.BlockCreate(block0, nil))

	tip, nonce, err := db.TruncateAfterSlot(
		ocommon.Point{Slot: 0, Hash: block0.Hash}, 0, nil,
	)
	require.NoError(t, err)
	require.Empty(t, nonce)
	require.Equal(t, block0.Hash, tip.Point.Hash)
}

// TestTruncateAfterSlotOriginClearsNonce verifies a rollback to true origin
// (slot 0, empty hash) still returns no nonce and no block height.
func TestTruncateAfterSlotOriginClearsNonce(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	block0 := testIndexedBlock(0, 1, 0x40)
	block0.Number = 0
	block0.Type = babbage.BlockTypeBabbage
	require.NoError(t, db.BlockCreate(block0, nil))
	require.NoError(t, db.SetBlockNonce(
		block0.Hash, block0.Slot, bytes.Repeat([]byte{0xe0}, 32), true, nil,
	))

	tip, nonce, err := db.TruncateAfterSlot(ocommon.NewPointOrigin(), 0, nil)
	require.NoError(t, err)
	require.Empty(t, nonce)
	require.Zero(t, tip.BlockNumber)
	require.Empty(t, tip.Point.Hash)
}
