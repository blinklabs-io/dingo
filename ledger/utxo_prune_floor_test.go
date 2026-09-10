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
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Slot grid for the prune-floor fixture. The Shelley genesis used here
// (k=432, f=0.05) gives a stability window W of 3k/f = 25920 slots, so with a
// tip at pruneFixtureTipSlot the consumed-UTxO sweep prunes everything with
// deleted_slot <= pruneFixtureFloorSlot (140000-W).
//
// The at-tip rewind schedule then steps the ledger tip 140000 -> 114080 ->
// 88160. Attempt 2 asks for W/2 below the tip (127040) and findRewindPoint
// resolves that to the nearest committed block, 114080; attempt 3 asks for a
// full W below the *new* tip, 88160. That recomputation from the lowered tip
// is what carries the descent past the floor -- the per-attempt cap is a
// stability window, but the cumulative descent is not.
const (
	pruneFixtureStabilityWindow = 25_920
	pruneFixtureRootSlot        = 10_000
	pruneFixtureProducerSlot    = 50_000
	pruneFixtureDeepRewindSlot  = 88_160
	pruneFixtureConsumerSlot    = 110_000
	pruneFixtureFloorSlot       = 114_080
	pruneFixtureRetainedSlot    = 120_000
	pruneFixtureTipSlot         = 140_000
)

type prunedUtxoFixture struct {
	ls *LedgerState
	db *database.Database
	// prunedTxId is consumed at pruneFixtureConsumerSlot, at or below the
	// prune floor, so its row is hard-deleted by the consumed-UTxO sweep.
	prunedTxId []byte
	// retainedTxId is consumed above the prune floor, so its row survives the
	// sweep and rollback can still restore it. It is the control that keeps a
	// failure of the pruned probe from being read as a dead fixture.
	retainedTxId []byte
}

func newPrunedUtxoFixture(t *testing.T, mithrilLedgerSlot uint64) *prunedUtxoFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 1000}),
	)

	type fixtureBlock struct {
		slot   uint64
		number uint64
		hash   []byte
	}
	blocks := []fixtureBlock{
		{pruneFixtureRootSlot, 1, testHashBytes("3766-root")},
		{pruneFixtureProducerSlot, 2, testHashBytes("3766-producer")},
		{pruneFixtureDeepRewindSlot, 3, testHashBytes("3766-deep")},
		{pruneFixtureConsumerSlot, 4, testHashBytes("3766-consumer")},
		{pruneFixtureFloorSlot, 5, testHashBytes("3766-floor")},
		{pruneFixtureTipSlot, 6, testHashBytes("3766-tip")},
	}
	rawBlocks := make([]chain.RawBlock, 0, len(blocks))
	for i, b := range blocks {
		var prevHash []byte
		if i > 0 {
			prevHash = blocks[i-1].hash
		}
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        b.slot,
			Hash:        b.hash,
			BlockNumber: b.number,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(rawBlocks))

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

	// Every fixture block was applied, so each carries a recorded nonce.
	for _, b := range blocks {
		require.NoError(
			t,
			db.SetBlockNonce(b.hash, b.slot, []byte("nonce-3766"), false, nil),
		)
	}

	// One epoch covering the whole fixture grid, so the era reload that
	// follows every rollback keeps the ledger in Conway and the stability
	// window at 3k/f rather than falling back to the Byron default.
	require.NoError(t, db.SetEpoch(
		0,
		1,
		[]byte("nonce-3766-epoch"),
		[]byte("evolving-3766"),
		[]byte("candidate-3766"),
		[]byte("last-3766"),
		eras.ConwayEraDesc.Id,
		1,
		1_000_000,
		nil,
	))

	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(pruneFixtureTipSlot, blocks[len(blocks)-1].hash),
		BlockNumber: blocks[len(blocks)-1].number,
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip
	ls.currentEra = eras.ConwayEraDesc
	ls.mithrilLedgerSlot = mithrilLedgerSlot
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()
	ls.syncUpstreamTipSlot.Store(pruneFixtureTipSlot)

	f := &prunedUtxoFixture{
		ls:           ls,
		db:           db,
		prunedTxId:   testHashBytes("3766-utxo-pruned"),
		retainedTxId: testHashBytes("3766-utxo-retained"),
	}
	seed := func(txId []byte, addedSlot, deletedSlot uint64) {
		mdTxn := db.MetadataTxn(true)
		require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
			return db.CreateUtxo(txn, &models.Utxo{
				TxId:        txId,
				OutputIdx:   0,
				AddedSlot:   addedSlot,
				DeletedSlot: deletedSlot,
				Amount:      types.Uint64(1_000_000),
			})
		}))
	}
	seed(f.prunedTxId, pruneFixtureProducerSlot, pruneFixtureConsumerSlot)
	seed(f.retainedTxId, pruneFixtureProducerSlot, pruneFixtureRetainedSlot)
	return f
}

// inLiveSet mirrors the probe used by the issue #3678 rollback tests: it asks
// the database.UtxoByRef lookup that LedgerView.UtxoById delegates to, so it
// exercises the deleted_slot filter that decides Conway bad-inputs and, through
// it, the consumed term of value conservation. A row seeded straight into
// metadata carries no blob CBOR, so ErrUtxoCborUnavailable counts as present;
// any error other than ErrUtxoNotFound is a lookup failure and fails the test.
func (f *prunedUtxoFixture) inLiveSet(t *testing.T, txId []byte) bool {
	t.Helper()
	var live bool
	txn := f.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, err := f.db.UtxoByRef(txId, 0, txn)
		switch {
		case err == nil, errors.Is(err, database.ErrUtxoCborUnavailable):
			live = true
			return nil
		case errors.Is(err, database.ErrUtxoNotFound):
			live = false
			return nil
		default:
			return err
		}
	}))
	return live
}

// driveAtTipRecovery runs the production at-tip recovery entry point the
// requested number of times against one persistent failure identity, which is
// what escalates the rewind schedule.
func (f *prunedUtxoFixture) driveAtTipRecovery(t *testing.T, rounds int) {
	t.Helper()
	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(
			pruneFixtureTipSlot+1,
			testHashBytes("3766-failing"),
		),
		TxHash: testHashBytes("3766-failing-tx"),
		Cause:  errors.New("bad input(s)"),
	}
	for i := range rounds {
		handled, err := f.ls.recoverAtTipFromTxValidationError(validationErr)
		require.NoError(t, err, "recovery round %d", i+1)
		require.True(t, handled, "recovery round %d", i+1)
	}
}

// assertLiveSetConsistentAtTip checks the invariant a rollback must preserve:
// at the ledger tip recovery settled on, every seeded output produced at or
// below that tip and consumed above it is resolvable, and every output already
// consumed at or below it is not. The second half is what keeps the fix from
// being "make lookups more permissive": refusing the rewind must not resurrect
// a genuinely spent output.
func (f *prunedUtxoFixture) assertLiveSetConsistentAtTip(t *testing.T) {
	t.Helper()
	tipSlot := f.ls.currentTip.Point.Slot
	for _, probe := range []struct {
		name        string
		txId        []byte
		addedSlot   uint64
		deletedSlot uint64
	}{
		{"pruned", f.prunedTxId, pruneFixtureProducerSlot, pruneFixtureConsumerSlot},
		{"retained", f.retainedTxId, pruneFixtureProducerSlot, pruneFixtureRetainedSlot},
	} {
		live := f.inLiveSet(t, probe.txId)
		switch {
		case probe.deletedSlot <= tipSlot:
			require.False(
				t,
				live,
				"%s output was consumed at slot %d, at or below ledger tip %d, and must not be in the live set",
				probe.name,
				probe.deletedSlot,
				tipSlot,
			)
		case probe.addedSlot <= tipSlot:
			require.True(
				t,
				live,
				"%s output produced at slot %d and consumed at slot %d must be in the live set at ledger tip %d",
				probe.name,
				probe.addedSlot,
				probe.deletedSlot,
				tipSlot,
			)
		}
	}
}

// TestAtTipRecoveryRewindBelowConsumedUtxoPruneFloor covers issue #3766.
//
// cleanupConsumedUtxos hard-deletes consumed UTxO rows whose deleted_slot is at
// or below tip-stabilityWindow. database.TruncateAfterSlot restores consumed
// UTxOs with an UPDATE (deleted_slot > slot), so a rollback below that prune
// floor cannot restore anything the sweep already removed -- and used to report
// the ledger repaired anyway. The at-tip recovery rewind schedule reaches such
// a target because each escalating attempt rewinds a further stability window
// below the *current* tip while the prune floor stays fixed at the highest tip
// the node reached.
//
// Blocks the node applied cleanly then become unapplyable: their inputs resolve
// to nothing, which Conway reports as bad inputs and, because value
// conservation sums consumed over only the inputs that resolve, as value not
// conserved with consumed 0 in the same pass.
//
// The recovery schedule here walks 140000 -> 114080 -> 88160, and 88160 is
// below the 114080 sweep floor.
func TestAtTipRecoveryRewindBelowConsumedUtxoPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)

	require.Equal(
		t,
		uint64(pruneFixtureStabilityWindow),
		f.ls.calculateStabilityWindow(),
		"fixture slot grid assumes a 3k/f stability window",
	)

	// Production consumed-UTxO sweep at the highest tip the node reached.
	f.ls.cleanupConsumedUtxos()
	require.False(
		t,
		f.inLiveSet(t, f.prunedTxId),
		"output consumed at or below the prune floor must be hard-deleted by the sweep",
	)
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(pruneFixtureFloorSlot),
		floor,
		"the sweep must record how deep it removed spent rows",
	)

	f.driveAtTipRecovery(t, 3)

	// The live UTxO set must agree with the point recovery settled on.
	f.assertLiveSetConsistentAtTip(t)
	// ...which it can only do if the tip was never rewound below the point
	// from which the consumed-UTxO sweep can still restore state.
	require.GreaterOrEqual(
		t,
		f.ls.currentTip.Point.Slot,
		uint64(pruneFixtureFloorSlot),
		"recovery rewound the ledger below the consumed UTxO prune floor",
	)
	require.Positive(
		t,
		promtestutil.ToFloat64(f.ls.metrics.atTipRecoveryPruneFloorClamped),
		"the refused rewind must be visible to an operator",
	)
}

// TestAtTipRecoveryPruneFloorBindsAboveMithrilAnchor covers the Mithril-
// bootstrapped shape reported in issue #3766. The Mithril anchor sits far below
// the consumed-UTxO prune floor, so the existing trust-boundary check admits
// every target the rewind schedule produces while the sweep has already made
// them unrestorable. The prune floor is the binding constraint, and the node
// halts at the anchor only after the descent has destroyed the UTxO set on the
// way down.
func TestAtTipRecoveryPruneFloorBindsAboveMithrilAnchor(t *testing.T) {
	const mithrilAnchorSlot = pruneFixtureRootSlot
	f := newPrunedUtxoFixture(t, mithrilAnchorSlot)

	f.ls.cleanupConsumedUtxos()
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Greater(
		t,
		floor,
		uint64(mithrilAnchorSlot),
		"the fixture must place the prune floor above the Mithril anchor",
	)

	f.driveAtTipRecovery(t, 3)

	// The Mithril check alone would have allowed the descent: every target
	// the schedule produced is above the anchor.
	require.False(
		t,
		f.ls.recoveryRollbackExceedsMithrilBoundary(
			ocommon.NewPoint(pruneFixtureDeepRewindSlot, nil),
		),
		"the descent target is above the Mithril anchor, so only the prune floor can refuse it",
	)
	f.assertLiveSetConsistentAtTip(t)
	require.GreaterOrEqual(
		t,
		f.ls.currentTip.Point.Slot,
		uint64(pruneFixtureFloorSlot),
	)
}

// TestRollbackBelowConsumedUtxoPruneFloorIsRefused pins the backstop every
// rewind path funnels through. Callers other than at-tip recovery -- a peer
// rollback, the durable-tip-floor repair, replay recovery -- reach
// LedgerState.rollback directly, and it must refuse before mutating anything
// rather than move the tip and report a repair it cannot perform.
func TestRollbackBelowConsumedUtxoPruneFloorIsRefused(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()
	tipBefore := f.ls.currentTip

	err := f.ls.rollback(
		ocommon.NewPoint(
			pruneFixtureDeepRewindSlot,
			testHashBytes("3766-deep"),
		),
	)
	require.ErrorIs(t, err, ErrRollbackBelowUtxoPruneFloor)
	require.Equal(
		t,
		tipBefore.Point,
		f.ls.currentTip.Point,
		"a refused rollback must leave the ledger tip where it was",
	)

	// A target at or above the floor is still allowed: the floor refuses the
	// rewinds it cannot restore, not every rewind.
	require.NoError(
		t,
		f.ls.rollback(
			ocommon.NewPoint(
				pruneFixtureFloorSlot,
				testHashBytes("3766-floor"),
			),
		),
	)
	require.Equal(
		t,
		uint64(pruneFixtureFloorSlot),
		f.ls.currentTip.Point.Slot,
	)
	require.True(
		t,
		f.inLiveSet(t, f.retainedTxId),
		"an output consumed above the prune floor must still be restored by an allowed rollback",
	)
}

// TestRollbackIsAppliableRejectsBelowConsumedUtxoPruneFloor keeps the loop
// detector's crossability predicate in step with the rollback it predicts.
// Reporting a target below the prune floor as crossable would make the detector
// insist on applying a rollback rollbackChainAndStateDeferred refuses.
func TestRollbackIsAppliableRejectsBelowConsumedUtxoPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()

	require.True(
		t,
		f.ls.rollbackIsAppliable(
			ocommon.NewPoint(
				pruneFixtureFloorSlot,
				testHashBytes("3766-floor"),
			),
		),
		"a target at the prune floor is still crossable",
	)
	require.False(
		t,
		f.ls.rollbackIsAppliable(
			ocommon.NewPoint(
				pruneFixtureDeepRewindSlot,
				testHashBytes("3766-deep"),
			),
		),
		"a target below the prune floor cannot be crossed",
	)
}

// TestConsumedUtxoPruneFloorIsReadFromTheDatabase pins where the floor comes
// from. It is deliberately not mirrored in memory: a mirror is only refreshed
// after the sweep's transaction commits, so between commit and refresh it
// reports a lower floor than the database holds, and a rollback admitted on
// that stale value is exactly the divergence the floor exists to refuse.
func TestConsumedUtxoPruneFloorIsReadFromTheDatabase(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)

	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Zero(t, floor, "nothing has been swept yet")

	f.ls.cleanupConsumedUtxos()

	floor, err = f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(pruneFixtureFloorSlot), floor)

	// A floor written by another writer -- a prior run, or the sweep's own
	// transaction before any in-process cache could observe it -- is honored
	// immediately, with no reload step.
	require.NoError(t, f.db.SetSyncState(
		"consumed_utxo_prune_slot",
		strconv.FormatUint(pruneFixtureRetainedSlot, 10),
		nil,
	))
	below, seen, err := f.ls.rollbackBelowConsumedUtxoPruneFloor(
		ocommon.NewPoint(pruneFixtureFloorSlot, nil),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(pruneFixtureRetainedSlot), seen)
	require.True(
		t,
		below,
		"the check must read the persisted floor, not a cached copy",
	)

	// A malformed value fails closed rather than reading as "nothing swept".
	require.NoError(t, f.db.SetSyncState(
		"consumed_utxo_prune_slot", "not-a-slot", nil,
	))
	_, _, err = f.ls.rollbackBelowConsumedUtxoPruneFloor(
		ocommon.NewPoint(pruneFixtureFloorSlot, nil),
	)
	require.Error(t, err)
	require.ErrorIs(
		t,
		f.ls.rollback(
			ocommon.NewPoint(
				pruneFixtureDeepRewindSlot,
				testHashBytes("3766-deep"),
			),
		),
		strconv.ErrSyntax,
		"an unreadable floor must refuse the rollback",
	)
}

// TestRollbackChainAndStateRefusesRedirectBelowPruneFloor covers the ordering
// hazard between the same-slot competitor redirect (issue #3678) and the prune
// floor. rollbackChainAndStateDeferred truncates the primary chain and only then
// synchronizes the ledger. A target sitting exactly on the floor whose hash
// differs from the applied tip resolves to an applied ancestor strictly below
// the floor, so checking the unresolved point would admit it here and refuse it
// only after chain.Rollback had already run, splitting the chain from the
// ledger.
func TestRollbackChainAndStateRefusesRedirectBelowPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()

	// Put the applied ledger tip on a same-slot competitor at the floor, with
	// a recorded nonce so the redirect treats it as genuinely applied.
	competitorHash := testHashBytes("3766-floor-competitor")
	require.NoError(t, f.db.SetBlockNonce(
		competitorHash,
		pruneFixtureFloorSlot,
		[]byte("nonce-3766-competitor"),
		false,
		nil,
	))
	competitorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(pruneFixtureFloorSlot, competitorHash),
		BlockNumber: 5,
	}
	require.NoError(t, f.db.SetTip(competitorTip, nil))
	f.ls.currentTip = competitorTip
	f.ls.publishSnapshotsLocked()

	chainTipBefore := f.ls.chain.Tip().Point

	// The target's slot equals the floor, so an unresolved check passes; the
	// redirect resolves it below the floor.
	target := ocommon.NewPoint(
		pruneFixtureFloorSlot,
		testHashBytes("3766-floor"),
	)
	resolved, err := f.ls.resolveRollbackTarget(target, competitorTip)
	require.NoError(t, err)
	require.Less(
		t,
		resolved.Slot,
		uint64(pruneFixtureFloorSlot),
		"fixture must produce a redirect below the floor",
	)

	require.ErrorIs(
		t,
		f.ls.rollbackChainAndStateDeferred(target, nil),
		ErrRollbackBelowUtxoPruneFloor,
	)
	require.Equal(
		t,
		chainTipBefore,
		f.ls.chain.Tip().Point,
		"the primary chain must not be truncated for a refused rollback",
	)
	require.Equal(
		t,
		competitorTip.Point,
		f.ls.currentTip.Point,
		"the ledger tip must not move for a refused rollback",
	)
}

// TestHandleEventChainsyncRollbackRejectsBelowPruneFloor pins that a peer
// rollback the prune floor refuses is handled as peer divergence, not as a
// local fault. handleEventChainsync routes any error returned by the rollback
// handler to FatalErrorFunc, so returning one here would let a peer's choice of
// rollback point terminate the node. The handler must instead reject the peer
// chain and ask for a fresh intersection, exactly as the Mithril boundary does.
func TestHandleEventChainsyncRollbackRejectsBelowPruneFloor(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	// A floor above the rollback target, as a sweep at a higher tip would
	// have left behind.
	require.NoError(t, fixture.ls.db.SetSyncState(
		"consumed_utxo_prune_slot",
		strconv.FormatUint(fixture.currentTip.Point.Slot, 10),
		nil,
	))

	fatalCalls := 0
	fixture.ls.config.FatalErrorFunc = func(error) { fatalCalls++ }

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)
	require.NoError(
		t,
		err,
		"a refused rollback must not surface as an error the fatal path acts on",
	)
	require.Zero(t, fatalCalls)

	// Neither side moved: the chain was not truncated and the ledger tip
	// stands.
	require.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	require.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected prune-floor rollback resync event",
	)
	require.Equal(
		t,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
		e.Reason,
	)
	require.Equal(t, fixture.connId, e.ConnectionId)
}
