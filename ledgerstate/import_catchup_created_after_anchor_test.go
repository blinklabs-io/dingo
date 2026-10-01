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

package ledgerstate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestImportLedgerStateCatchUpRestoresPostAnchorCreatedAndSpentUtxo covers
// dingo#4770's uncovered majority class: a UTxO both *created* and spent
// after the snapshot's anchor. Unlike TestImportLedgerStateCatchUpRestores-
// PostAnchorSpentUtxo's output (live at the anchor, spent afterward), this
// output never appears in the snapshot's live set at all -- the anchor
// predates its creation -- so it never reaches hydrateImportedUtxo's ON
// CONFLICT clear. It is created and spent purely by local block replay, so a
// catch-up/reward-repair re-import of the same anchor leaves it exactly as
// it found it unless the import path itself restores it.
func TestImportLedgerStateCatchUpRestoresPostAnchorCreatedAndSpentUtxo(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x55}, 28),
		bytes.Repeat([]byte{0x66}, 28),
	)
	// inlineUTxOMap keys its single entry's tx hash as 0x40 repeated 32
	// times (see inlineUTxOMap in import_deferred_reward_live_stake_test.go).
	xTxID := bytes.Repeat([]byte{0x40}, 32)

	newImportConfig := func(tipSlot uint64) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Original bootstrap import: X is live at anchor slot 1000. Nothing
	// else exists yet -- the anchor predates the output under test.
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// 2. A real post-anchor block spends X and creates a brand-new output O,
	// through the ordinary block-apply path -- exactly what a node synced
	// past its bootstrap anchor does. O did not exist at the anchor, so no
	// snapshot could ever have declared it live.
	const txSeedA = 0x71
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	oTxID := bytes.Repeat([]byte{txSeedA}, 32)
	liveO, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveO, "precondition: O live after being created")

	// 3. A further real post-anchor block spends O.
	const txSeedB = 0x72
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, oTxID, 0, 1_500,
	))
	spentO, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentO, "precondition: O spent for real after creation")

	// 4. Catch-up / reward-repair re-import of the same anchor. The
	// snapshot's live UTxO set only knows about slot-1000 state (X); it says
	// nothing about O at all, since O was created thereafter. This is the
	// dingo#4770 gap hydrateImportedUtxo's ON CONFLICT clear cannot reach.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// This is the discriminating assertion. A real replay of the block that
	// spent O would reuse tx B's own hash, and setTransactionWithAccumulator
	// treats a consumed input already spent *by that same hash* as an
	// idempotent no-op (bytes.Equal(spentBy, hash) => continue) regardless
	// of whether the row was ever actually restored -- so a later replay
	// succeeding proves nothing on its own. The state has to be checked
	// here, immediately after the re-import.
	liveOAfterCatchUp, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, liveOAfterCatchUp,
		"O was created and spent entirely after the anchor; the re-import "+
			"must still restore it",
	)

	// 5. Real replay of the block that created O. insertUtxoModelChecked's
	// ON CONFLICT (tx_id, output_idx) DO NOTHING insert leaves an existing
	// row exactly as it finds it, so this must not disturb the just-restored
	// output.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	liveOAfterReplayA, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, liveOAfterReplayA,
		"O must still be live after replaying the block that created it",
	)

	// 6. Real replay of the block that spent O, reusing the same
	// transaction hash a real chain replay would. This must apply cleanly:
	// a live-stake underflow, or a spend rejected against a row incorrectly
	// left dead, would error here.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, oTxID, 0, 1_500,
	), "replaying the real block that spent O must apply after the repair")
	spentOAfterReplayB, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentOAfterReplayB, "O must be spent again")
}

// TestImportLedgerStateReconcileCatchUpRestoresPostAnchorCreatedAndSpentUtxo
// is TestImportLedgerStateCatchUpRestoresPostAnchorCreatedAndSpentUtxo's
// second import run with Reconcile: true. O is already spent (not live) by
// the time the reconcile scan runs, so reconcile's own tombstoning pass
// (which only visits currently-live rows) does not additionally touch it;
// this proves the fix holds under the literal catch-up flag too, alongside
// the legacy reward-repair shape (Reconcile: false) the first test covers.
func TestImportLedgerStateReconcileCatchUpRestoresPostAnchorCreatedAndSpentUtxo(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x57}, 28),
		bytes.Repeat([]byte{0x68}, 28),
	)
	xTxID := bytes.Repeat([]byte{0x40}, 32)
	govStateTxHash := bytes.Repeat([]byte{0x93}, 32)

	newImportConfig := func(tipSlot uint64, reconcile bool) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database:  db,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			Reconcile: reconcile,
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				CertStateData:       minimalCertStateData(t),
				GovStateData:        testGovStateData(t, govStateTxHash, 100),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Bootstrap (Reconcile: false).
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, false),
	))

	// 2. A real post-anchor block spends X and creates O.
	const txSeedA = 0x73
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	oTxID := bytes.Repeat([]byte{txSeedA}, 32)

	// 3. A further real post-anchor block spends O.
	const txSeedB = 0x74
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, oTxID, 0, 1_500,
	))
	spentO, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentO, "precondition: O spent for real after creation")

	// 4. A literal catch-up import: Reconcile: true, same anchor.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, true),
	))

	// Discriminating assertion -- see the non-reconcile test's comment for
	// why a later replay succeeding is not sufficient on its own.
	liveOAfterCatchUp, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, liveOAfterCatchUp,
		"O was created and spent entirely after the anchor; the reconcile "+
			"catch-up must still restore it",
	)

	// 5. Real replay of the block that created O (no-op ON CONFLICT insert).
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	liveOAfterReplayA, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveOAfterReplayA, "O must still be live")

	// 6. Real replay of the block that spent O, same transaction hash.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, oTxID, 0, 1_500,
	), "replaying the real block that spent O must apply after the "+
		"reconcile catch-up")
	spentOAfterReplayB, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentOAfterReplayB, "O must be spent again")
}

// TestImportLedgerStateReconcileRestoresPostAnchorLiveUtxoTombstonedByReconcile
// covers the interaction between reconcile's own stale-row tombstoning and
// the dingo#4770 fix: an output created after the anchor and still live
// locally (never spent) is, like any row absent from the snapshot's live
// key set, marked inactive by reconcileStaleLedgerState -- reconcile has no
// way to know the row postdates the anchor rather than having been spent
// before it. RestorePostAnchorCreatedUtxos runs after reconcile specifically
// so it can also repair this: reconcile tombstones the row at exactly the
// anchor slot (not after it), so the fix's deleted_slot <> 0 predicate,
// rather than deleted_slot > anchorSlot, is what catches it.
func TestImportLedgerStateReconcileRestoresPostAnchorLiveUtxoTombstonedByReconcile(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x59}, 28),
		bytes.Repeat([]byte{0x6a}, 28),
	)
	xTxID := bytes.Repeat([]byte{0x40}, 32)
	govStateTxHash := bytes.Repeat([]byte{0x94}, 32)

	newImportConfig := func(tipSlot uint64, reconcile bool) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database:  db,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			Reconcile: reconcile,
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{5_000_000},
				),
				CertStateData:       minimalCertStateData(t),
				GovStateData:        testGovStateData(t, govStateTxHash, 100),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Bootstrap (Reconcile: false).
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, false),
	))

	// 2. A real post-anchor block spends X and creates P. Unlike O in the
	// sibling tests, P is never spent locally -- it stays live, exactly like
	// a real unspent change output would.
	const txSeedA = 0x75
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	pTxID := bytes.Repeat([]byte{txSeedA}, 32)
	liveP, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveP, "precondition: P live after being created")

	// 3. A literal catch-up import at the same anchor, Reconcile: true. The
	// snapshot's live set only ever declares X; P is absent from it, exactly
	// like a genuinely-spent row would be, so reconcileStaleLedgerState
	// tombstones P at anchorSlot as part of its ordinary "absent from the
	// snapshot" sweep.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, true),
	))

	// Discriminating assertion: P must survive the reconcile catch-up even
	// though reconcile's own pass had no way to distinguish "created after
	// the anchor" from "spent before it".
	liveOAfterCatchUp, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, liveOAfterCatchUp,
		"P was created after the anchor and never spent; the reconcile "+
			"catch-up must not leave it tombstoned",
	)

	// 4. Real replay of the block that created P (no-op ON CONFLICT insert).
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	liveAfterReplayA, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveAfterReplayA, "P must still be live")

	// 5. A genuine real spend of P must still apply cleanly (a live-stake
	// underflow against the credential P's address carries would error
	// here).
	const txSeedC = 0x76
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedC, pTxID, 0, 2_000,
	), "spending the restored output must apply after the reconcile catch-up")
	spentAfterRealSpend, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentAfterRealSpend, "P must be spent")
}

// TestImportLedgerStateCatchUpDoesNotResurrectPreAnchorSnapshotDeadUtxo
// guards RestorePostAnchorCreatedUtxos' scoping: it must restore only a
// UTxO created after the anchor, never a UTxO the snapshot could have
// judged. Y is live at the (only) bootstrap anchor, so its added_slot is at
// or before that anchor; it is then spent for real, and re-imported with a
// snapshot that (synthetically, for this test only -- a real snapshot at
// the same anchor would still include Y) omits it. A predicate keyed on
// deleted_slot alone (deleted_slot > anchorSlot) would incorrectly revive Y;
// only added_slot > anchorSlot correctly leaves it spent.
func TestImportLedgerStateCatchUpDoesNotResurrectPreAnchorSnapshotDeadUtxo(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x5b}, 28),
		bytes.Repeat([]byte{0x6c}, 28),
	)
	// inlineUTxOMap keys entry i's tx hash as (0x40+i) repeated 32 times.
	xTxID := bytes.Repeat([]byte{0x40}, 32)
	yTxID := bytes.Repeat([]byte{0x41}, 32)

	newImportConfig := func(tipSlot uint64, amounts []uint64) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData:            inlineUTxOMap(t, addr, amounts),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Bootstrap: both X and Y live at anchor slot 1000.
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(),
		newImportConfig(anchorSlot, []uint64{5_000_000, 3_000_000}),
	))
	liveY, err := db.Metadata().GetUtxo(yTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveY, "precondition: Y live after bootstrap")

	// 2. A real post-anchor block spends Y. Y's added_slot is anchorSlot
	// (from the bootstrap import), strictly at-or-before the anchor.
	require.NoError(t, applySpendingTransaction(
		t, db, 0x81, yTxID, 0, 1_500,
	))
	spentY, err := db.Metadata().GetUtxo(yTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentY, "precondition: Y spent for real")

	// 3. Re-import the same anchor, this time with a snapshot that only
	// carries X. This is a synthetic input solely to isolate the
	// added_slot boundary in the fix's predicate.
	require.NoError(t, ImportLedgerState(
		context.Background(),
		newImportConfig(anchorSlot, []uint64{5_000_000}),
	))

	stillSpentY, err := db.Metadata().GetUtxo(yTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t, stillSpentY,
		"Y's added_slot does not postdate the anchor, so the re-import "+
			"must not resurrect it despite deleted_slot > anchorSlot",
	)
	liveXAfterCatchUp, err := db.Metadata().GetUtxo(xTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveXAfterCatchUp, "X remains live after the re-import")
}
