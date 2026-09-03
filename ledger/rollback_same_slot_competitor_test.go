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

package ledger

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	sameSlotAncestorSlot  = 10
	sameSlotContestedSlot = 20
)

// sameSlotCompetitorFixture holds a ledger whose applied tip is a block at
// sameSlotContestedSlot, with one UTxO produced at sameSlotAncestorSlot and
// consumed by that applied block.
type sameSlotCompetitorFixture struct {
	ls            *LedgerState
	db            *database.Database
	appliedTip    ochainsync.Tip
	ancestorPoint ocommon.Point
	survivingHash []byte
	spentTxId     []byte
}

func newSameSlotCompetitorFixture(
	t *testing.T,
) *sameSlotCompetitorFixture {
	t.Helper()
	return newSameSlotCompetitorFixtureOpts(t, true)
}

// newSameSlotCompetitorFixtureOpts builds the fixture, optionally omitting the
// ancestor's recorded nonce so that no applied ancestor exists below the
// contested slot.
func newSameSlotCompetitorFixtureOpts(
	t *testing.T,
	seedAncestorNonce bool,
) *sameSlotCompetitorFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	ancestorHash := testHashBytes("3678-ancestor")
	survivingHash := testHashBytes("3678-surviving")
	competitorHash := testHashBytes("3678-competitor")

	// The primary chain holds the ancestor and the block at the contested slot
	// that survives chain selection. The ledger's applied tip is a *different*
	// block at that same slot -- an abandoned same-slot competitor whose effects
	// were applied to the UTxO set before chain selection moved off it. This is
	// the shape enforceDurableTipFloor repairs: it hands rollback the durable
	// applied floor while currentTip names the same-slot competitor.
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			{
				Slot:        sameSlotAncestorSlot,
				Hash:        ancestorHash,
				BlockNumber: 1,
				Type:        1,
				Cbor:        []byte{0x80},
			},
			{
				Slot:        sameSlotContestedSlot,
				Hash:        survivingHash,
				BlockNumber: 2,
				Type:        1,
				PrevHash:    ancestorHash,
				Cbor:        []byte{0x80},
			},
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	// Block nonces record which blocks were applied.
	// latestLedgerPrimaryChainAncestor reads them to find the newest applied
	// ancestor below the contested slot.
	if seedAncestorNonce {
		require.NoError(
			t,
			db.SetBlockNonce(
				ancestorHash,
				sameSlotAncestorSlot,
				[]byte("nonce-3678-ancestor"),
				true,
				nil,
			),
		)
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			survivingHash,
			sameSlotContestedSlot,
			[]byte("nonce-3678-surviving"),
			false,
			nil,
		),
	)

	// The competitor was applied before chain selection moved off it, so its
	// nonce is recorded. That recorded nonce is what distinguishes an applied
	// same-slot competitor, whose effects are in the UTxO set, from a merely
	// in-memory tip that was never applied.
	require.NoError(
		t,
		db.SetBlockNonce(
			competitorHash,
			sameSlotContestedSlot,
			[]byte("nonce-3678-competitor"),
			false,
			nil,
		),
	)

	appliedTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(sameSlotContestedSlot, competitorHash),
		BlockNumber: 2,
	}
	require.NoError(t, db.SetTip(appliedTip, nil))
	ls.currentTip = appliedTip
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	// One UTxO produced at the ancestor slot and consumed by the applied
	// block at the contested slot, exactly as a normal block application
	// leaves it: the row survives, soft-deleted with deleted_slot set to the
	// consuming block's slot.
	spentTxId := testHashBytes("3678-utxo-producer")
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		return db.CreateUtxo(txn, &models.Utxo{
			TxId:        spentTxId,
			OutputIdx:   0,
			AddedSlot:   sameSlotAncestorSlot,
			DeletedSlot: sameSlotContestedSlot,
			Amount:      types.Uint64(1_000_000),
		})
	}))

	return &sameSlotCompetitorFixture{
		ls:            ls,
		db:            db,
		appliedTip:    appliedTip,
		ancestorPoint: ocommon.NewPoint(sameSlotAncestorSlot, ancestorHash),
		survivingHash: survivingHash,
		spentTxId:     spentTxId,
	}
}

// inputInLiveSet reports whether the consumed UTxO is present in the live UTxO
// set, using the same database.UtxoByRef lookup that LedgerView.UtxoById
// delegates to. That lookup applies the deleted_slot filter, so it is the
// predicate that decides Conway bad-inputs and, through it, the consumed term
// of value conservation.
//
// Presence is judged on ErrUtxoNotFound rather than on a nil error: a row
// seeded directly into metadata carries no blob CBOR (models.Utxo.Cbor is not
// persisted by CreateUtxo), so the decode step of UtxoById cannot succeed for a
// synthetic UTxO. ErrUtxoCborUnavailable and a decode failure both mean the row
// IS in the live set, which is the question this test asks.
func (f *sameSlotCompetitorFixture) inputInLiveSet(t *testing.T) bool {
	t.Helper()

	var live bool
	txn := f.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, err := f.db.UtxoByRef(f.spentTxId, 0, txn)
		live = !errors.Is(err, database.ErrUtxoNotFound)
		return nil
	}))
	return live
}

// TestRollbackSameSlotCompetitorRestoresConsumedUtxo covers issue #3678.
//
// A rollback target that shares the applied tip's slot but carries a different
// hash used to fall through to database.TruncateAfterSlot's slot-only UTxO
// predicates (added_slot > slot, deleted_slot > slot). Nothing at the contested
// slot matched, so the UTxOs the abandoned block consumed stayed soft-deleted
// with no row left to restore them, while the tip was reported as repaired.
//
// The next block that legitimately spends such an input cannot resolve it,
// which Conway reports as bad inputs (rule 29) and, because value conservation
// sums consumed over only the inputs that did resolve, as value not conserved
// (rule 32) in the same pass -- both from the one divergence.
//
// This drives LedgerState.rollback, the entry point every recovery path uses,
// and asserts live-set membership through the database.UtxoByRef lookup that
// LedgerView.UtxoById delegates to, rather than querying deleted_slot directly.
func TestRollbackSameSlotCompetitorRestoresConsumedUtxo(t *testing.T) {
	fixture := newSameSlotCompetitorFixture(t)

	// While the applied block at the contested slot stands, its consumed
	// input is correctly unresolvable.
	require.False(
		t,
		fixture.inputInLiveSet(t),
		"consumed input should not be in the live set before the rollback",
	)

	require.NoError(
		t,
		fixture.ls.rollback(
			ocommon.NewPoint(
				sameSlotContestedSlot,
				fixture.survivingHash,
			),
		),
	)

	// The contested slot must be truncated whole, so the input the abandoned
	// block consumed is live again and resolvable at the validated point.
	require.True(
		t,
		fixture.inputInLiveSet(t),
		"consumed input must be restored to the live set after rolling back past the contested slot",
	)

	// The ledger must sit at an applied point, not at the competitor it was
	// handed, so the block at the contested slot can be re-applied.
	require.Equal(
		t,
		fixture.ancestorPoint,
		fixture.ls.currentTip.Point,
		"tip should be redirected to the applied ancestor below the contested slot",
	)
}

// TestRollbackSameSlotCompetitorWithoutAncestorFailsLoudly covers the other
// half of issue #3678's acceptance criteria: when the contested slot cannot be
// truncated because no applied ancestor below it can be found, the rollback
// must fail with a persistent diagnostic instead of reporting a repair that
// left the UTxO set diverged.
func TestRollbackSameSlotCompetitorWithoutAncestorFailsLoudly(t *testing.T) {
	// No ancestor nonce, so no applied block exists below the contested slot.
	fixture := newSameSlotCompetitorFixtureOpts(t, false)

	err := fixture.ls.rollback(
		ocommon.NewPoint(sameSlotContestedSlot, fixture.survivingHash),
	)
	require.ErrorIs(t, err, ErrNoAppliedAncestorBelowContestedSlot)

	// The tip must not move, so the failure stays visible to the recovery
	// caller instead of being reported as a completed repair.
	require.Equal(
		t,
		fixture.appliedTip.Point,
		fixture.ls.currentTip.Point,
		"tip must not move when the contested slot cannot be truncated",
	)
}
