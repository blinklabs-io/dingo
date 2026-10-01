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

// TestImportLedgerStateCatchUpRollsBackPostAnchorCreatedAndSpentUtxo covers
// an output created and spent after the anchor. It is absent from the
// snapshot, so the import's conflict path never sees it. The discriminating
// assertion is that the row is absent after re-import: it must be deleted for
// replay to re-create, not revived in place (see the UtxosDeleteRolledback
// call in ImportLedgerState).
func TestImportLedgerStateCatchUpRollsBackPostAnchorCreatedAndSpentUtxo(
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
	// times (see inlineUTxOMap in import_test.go).
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

	// 1. Bootstrap: X is live at anchor slot 1000.
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// 2. A post-anchor block spends X and creates O.
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

	// 4. Re-import the same anchor; the snapshot does not contain O.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	oAfterCatchUp, err := db.Metadata().GetUtxoIncludingSpent(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t, oAfterCatchUp,
		"O was created and spent entirely after the anchor; the re-import "+
			"must roll it back so replay re-creates it fresh",
	)

	// 5. Replaying the creating block re-creates O.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	liveOAfterReplayA, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, liveOAfterReplayA,
		"O must be live again after replaying the block that created it",
	)

	// 6. Replaying the spending block applies against the re-created row.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, oTxID, 0, 1_500,
	), "replaying the real block that spent O must apply after the repair")
	spentOAfterReplayB, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentOAfterReplayB, "O must be spent again")
}

// TestImportLedgerStateReconcileCatchUpRollsBackPostAnchorCreatedAndSpentUtxo
// is the same case with the second import run as Reconcile: true.
func TestImportLedgerStateReconcileCatchUpRollsBackPostAnchorCreatedAndSpentUtxo(
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

	// Discriminating assertion -- see the non-reconcile test's comment.
	oAfterCatchUp, err := db.Metadata().GetUtxoIncludingSpent(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t, oAfterCatchUp,
		"O was created and spent entirely after the anchor; the reconcile "+
			"catch-up must roll it back",
	)

	// 5. Real replay of the block that created O.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	liveOAfterReplayA, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveOAfterReplayA, "O must be live again")

	// 6. Real replay of the block that spent O, same transaction hash.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, oTxID, 0, 1_500,
	), "replaying the real block that spent O must apply after the "+
		"reconcile catch-up")
	spentOAfterReplayB, err := db.Metadata().GetUtxo(oTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentOAfterReplayB, "O must be spent again")
}

// TestImportLedgerStateCatchUpRollsBackPostAnchorLiveUnspentUtxo covers an
// output created after the anchor and never spent. It is rolled back like a
// spent one: left live, it would count toward mark snapshots taken before
// replay reaches its creation slot.
func TestImportLedgerStateCatchUpRollsBackPostAnchorLiveUnspentUtxo(
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

	// 1. Bootstrap: X live at anchor slot 1000.
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// 2. A real post-anchor block spends X and creates P. Unlike O above, P
	// is never spent locally -- it stays live, exactly like a real unspent
	// change output would.
	const txSeedA = 0x75
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	pTxID := bytes.Repeat([]byte{txSeedA}, 32)
	liveP, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveP, "precondition: P live after being created")

	// 3. A reward-repair re-import at the same anchor, Reconcile: false. The
	// snapshot's live set only ever declares X; P is absent from it, having
	// been created after the anchor.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// Discriminating assertion: P's row must be gone entirely immediately
	// after re-import -- the deliberate widening this test documents.
	pAfterCatchUp, err := db.Metadata().GetUtxoIncludingSpent(pTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t, pAfterCatchUp,
		"P was created after the anchor and never spent; the re-import "+
			"must still roll it back for replay to re-create",
	)

	// 4. Real replay of the block that created P re-creates it fresh.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	liveAfterReplayA, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveAfterReplayA, "P must be live again")

	// 5. A genuine real spend of P must still apply cleanly (a live-stake
	// underflow against the credential P's address carries would error
	// here).
	const txSeedC = 0x76
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedC, pTxID, 0, 2_000,
	), "spending the restored output must apply after the repair")
	spentAfterRealSpend, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentAfterRealSpend, "P must be spent")
}

// TestImportLedgerStateReconcileCatchUpRollsBackPostAnchorLiveUnspentUtxo is
// the same case under Reconcile: true. The roll-back runs before reconcile,
// which would otherwise tombstone the row at the anchor slot because it is
// absent from the snapshot.
func TestImportLedgerStateReconcileCatchUpRollsBackPostAnchorLiveUnspentUtxo(
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
	xTxID := bytes.Repeat([]byte{0x40}, 32)
	govStateTxHash := bytes.Repeat([]byte{0x95}, 32)

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

	// 2. A real post-anchor block spends X and creates P, never spent.
	const txSeedA = 0x77
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	pTxID := bytes.Repeat([]byte{txSeedA}, 32)

	// 3. A literal catch-up import at the same anchor, Reconcile: true.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, true),
	))

	pAfterCatchUp, err := db.Metadata().GetUtxoIncludingSpent(pTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t, pAfterCatchUp,
		"P was created after the anchor and never spent; the reconcile "+
			"catch-up must roll it back, not tombstone it",
	)

	// 4. Real replay of the block that created P.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	liveAfterReplayA, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveAfterReplayA, "P must be live again")

	// 5. A genuine real spend of P.
	const txSeedC = 0x78
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedC, pTxID, 0, 2_000,
	), "spending the restored output must apply after the reconcile catch-up")
	spentAfterRealSpend, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentAfterRealSpend, "P must be spent")
}

// TestImportLedgerStateCatchUpExcludesPostAnchorUtxosFromRewardLiveStake
// asserts the property the roll-back exists to protect: reward_live_stake,
// which ComputeEpochBoundarySnapshot reads through GetLiveStakeInputsForPools
// with no slot argument, must hold only stake that was live at the anchor
// once the re-import finishes. P (created after the anchor, spent) and Q
// (created after the anchor, still live) both pay the same credential K.
// Clearing P's spend in place instead of deleting it would leave both P and
// Q counted against K at the anchor; leaving Q alone would count Q. Only the
// roll-back leaves K at zero until replay re-creates Q at its real slot.
func TestImportLedgerStateCatchUpExcludesPostAnchorUtxosFromRewardLiveStake(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x5f}, 28),
		bytes.Repeat([]byte{0x70}, 28),
	)
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

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	utxoStake := func(tag uint8, key []byte) string {
		t.Helper()
		var stake string
		err := raw.QueryRow(
			"SELECT COALESCE(SUM(CAST(utxo_stake AS INTEGER)), 0) "+
				"FROM reward_live_stake "+
				"WHERE credential_tag = ? AND staking_key = ?",
			tag, key,
		).Scan(&stake)
		require.NoError(t, err)
		return stake
	}

	// 1. Bootstrap: X live at anchor slot 1000.
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	// 2. Post-anchor blocks: A spends X and creates P; B spends P and
	// creates Q, which stays live. Both outputs pay the same credential.
	const txSeedA, txSeedB = 0x79, 0x7a
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	pTxID := bytes.Repeat([]byte{txSeedA}, 32)
	p, err := db.Metadata().GetUtxo(pTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, p, "precondition: P live after being created")
	require.NotEmpty(t, p.StakingKey, "precondition: P carries a credential")
	tag, key := p.CredentialTag, p.StakingKey
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, pTxID, 0, 1_300,
	))
	qTxID := bytes.Repeat([]byte{txSeedB}, 32)
	require.Equal(
		t, "1000000", utxoStake(tag, key),
		"precondition: block apply counts live Q against the credential",
	)

	// 3. Re-import the same anchor.
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))

	require.Equal(
		t, "0", utxoStake(tag, key),
		"no output paying this credential existed at the anchor; the "+
			"aggregate an epoch-boundary snapshot reads must not count P or Q "+
			"before replay re-creates them",
	)

	// 4. Replay A and B: Q is re-created at its real slot and counted then.
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedA, xTxID, 0, 1_200,
	))
	require.NoError(t, applySpendingTransaction(
		t, db, txSeedB, pTxID, 0, 1_300,
	))
	q, err := db.Metadata().GetUtxo(qTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, q, "Q must be live after replay")
	require.Equal(
		t, "1000000", utxoStake(tag, key),
		"after replay only Q is live for this credential",
	)
}

// TestImportLedgerStateCatchUpDoesNotRollBackPreAnchorSnapshotDeadUtxo
// guards the added_slot scope: Y was created at the anchor and spent after
// it, and a synthetic re-import omits it. A predicate keyed on deleted_slot
// would delete or revive Y; added_slot > anchor leaves it present and spent.
func TestImportLedgerStateCatchUpDoesNotRollBackPreAnchorSnapshotDeadUtxo(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x5d}, 28),
		bytes.Repeat([]byte{0x6e}, 28),
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
	// added_slot boundary in UtxosDeleteRolledback's predicate.
	require.NoError(t, ImportLedgerState(
		context.Background(),
		newImportConfig(anchorSlot, []uint64{5_000_000}),
	))

	yAfterCatchUp, err := db.Metadata().GetUtxoIncludingSpent(yTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, yAfterCatchUp,
		"Y's added_slot does not postdate the anchor, so the re-import "+
			"must not roll it back even though the snapshot omits it",
	)
	require.NotZero(
		t, yAfterCatchUp.DeletedSlot,
		"Y must remain spent, not resurrected",
	)
	stillSpentY, err := db.Metadata().GetUtxo(yTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, stillSpentY, "Y must remain spent via the live-only getter")

	liveXAfterCatchUp, err := db.Metadata().GetUtxo(xTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveXAfterCatchUp, "X remains live after the re-import")
}
