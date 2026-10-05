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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// A deferred header-validation failure has to be distinguishable from every
// other pipeline error, because it is the one class that is *deterministic*:
// the block is already in the chain store, so restarting the pipeline reads
// the identical block and fails identically, forever. Transaction validation
// failures already carry a type that routes them into recovery; header
// validation carried a bare fmt.Errorf, which is why it looped instead.
//
// Rejecting the block is correct and stays correct -- the point of the type
// is to reject the *chain* rather than to spin on it.
func TestHeaderValidationErrorIsIdentifiable(t *testing.T) {
	t.Parallel()

	point := ocommon.Point{Slot: 119799023, Hash: []byte{0xab, 0xcd}}
	cause := errors.New("VRF leader value exceeds stake-derived threshold")
	err := &headerValidationError{BlockPoint: point, Cause: cause}

	var target *headerValidationError
	require.True(t, errors.As(err, &target),
		"the pipeline must be able to recognise this error class")
	require.Equal(t, point.Slot, target.BlockPoint.Slot)

	require.ErrorIs(t, err, cause,
		"the underlying validation error must stay inspectable")
	require.Contains(t, err.Error(), "119799023")
	require.Contains(t, err.Error(), "exceeds stake-derived threshold")

	// Still identifiable once the pipeline has wrapped it.
	wrapped := fmt.Errorf("process block batch: %w", err)
	require.True(t, errors.As(wrapped, &target))
	require.ErrorIs(t, wrapped, cause)
}

// Recovery must not fire for unrelated failures, or an ordinary transient
// error would start rewinding the chain.
func TestHeaderValidationRecoveryIgnoresOtherErrors(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	for _, err := range []error{
		errors.New("some unrelated failure"),
		errStaleChainIterator,
		&txValidationError{},
	} {
		recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(err)
		require.NoError(t, recoverErr)
		require.False(t, recovered,
			"only a header-validation failure may trigger this recovery")
	}
}

// Without a chain manager there is nothing to rewind, so recovery declines
// rather than reporting a rewind it did not perform -- a false "recovered"
// would send the pipeline straight back into the same block.
func TestHeaderValidationRecoveryDeclinesWithoutChainManager(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	err := &headerValidationError{
		BlockPoint: ocommon.Point{Slot: 42},
		Cause:      errors.New("rejected"),
	}
	recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(err)
	require.NoError(t, recoverErr)
	require.False(t, recovered)
}

// The decline branches above are the cheap half. This covers what Part 2 is
// actually built on: a rejected block sitting above the ledger tip is dropped
// from the primary chain, the ledger is rolled back with it, a resync is
// published, and the pipeline is told it may restart.
func TestHeaderValidationRecoveryRewindsPastRejectedBlock(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	// Required because the underlying chain rollback refuses to run without
	// the chain manager's K. Note this is not the same K the windowing loop
	// in rollbackPrimaryChainInSecurityParamWindows uses — that reads
	// ls.SecurityParam(), which falls back to a large default with no
	// CardanoNodeConfig — so this two-block rewind takes the single-step
	// path. The multi-window branch is not exercised here; this test covers
	// tryRecoverFromHeaderValidationError's own behaviour, not the shared
	// rollback helper's windowing.
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	// Ledger applied through slot 3; slots 4 and 5 are on the chain but not
	// yet applied. The deferred check rejects slot 4.
	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip
	ls.metrics.init(prometheus.NewRegistry())
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(context.Background()))
	require.Equal(t, blocks[4].Slot, cm.PrimaryChain().Tip().Point.Slot,
		"the rejected block and its successor should start on the chain")

	validationErr := &headerValidationError{
		BlockPoint: makeTestPoint(blocks[3]),
		Cause:      errors.New("VRF leader value exceeds threshold"),
	}
	// A declined attempt has not repaired anything and must not consume the
	// one same-tip metadata repair available to the first completed recovery.
	ls.mithrilLedgerSlot = ledgerTip.Point.Slot + 1000
	recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(validationErr)
	require.NoError(t, recoverErr)
	require.False(t, recovered)
	require.Equal(t, blocks[4].Slot, cm.PrimaryChain().Tip().Point.Slot)

	ls.mithrilLedgerSlot = 0
	generationBefore := ls.rewardInputGeneration.Load()
	recovered, recoverErr = ls.tryRecoverFromHeaderValidationError(validationErr)
	require.NoError(t, recoverErr)
	require.True(t, recovered,
		"a rejected block above the ledger tip must be recoverable")
	require.Greater(t, ls.rewardInputGeneration.Load(), generationBefore,
		"the first completed rewind must repair same-tip metadata")

	require.Equal(t, ledgerTipBlock.Slot, cm.PrimaryChain().Tip().Point.Slot,
		"the primary chain must be rewound past the rejected block, or the "+
			"pipeline re-reads it")

	select {
	case evt := <-resyncEvents:
		data, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		require.Equal(
			t,
			event.ChainsyncResyncReasonHeaderValidationRecovery,
			data.Reason,
		)
		require.Equal(t, ledgerTipBlock.Slot, data.Point.Slot)
	default:
		t.Fatal("recovery must publish a resync so chainsync re-delivers")
	}
}

func TestLedgerProcessBlocksRecoversReadChainValidationFailure(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := make([]models.Block, 0, 4)
	for slot := uint64(1); slot <= 4; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	ledgerTipBlock := blocks[1]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)

	ls := &LedgerState{
		db:                db,
		chain:             cm.PrimaryChain(),
		currentTip:        ledgerTip,
		validationEnabled: true,
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(context.Background()))

	done := make(chan struct{})
	results := make(chan readChainResult, 1)
	results <- readChainResult{
		err: &headerValidationError{
			BlockPoint: makeTestPoint(blocks[2]),
			Cause:      errors.New("invalid operational certificate"),
		},
		done: done,
	}
	close(results)

	err = ls.ledgerProcessBlocksFromSource(t.Context(), results)
	require.ErrorIs(t, err, errRestartLedgerPipeline)
	require.Equal(t, ledgerTipBlock.Slot, cm.PrimaryChain().Tip().Point.Slot)
	select {
	case <-done:
	default:
		t.Fatal("reader result was not released after validation recovery")
	}
	// Prove the rewind was performed by tryRecoverFromHeaderValidationError
	// specifically, not by some other path that happens to leave the same
	// tip: only that recovery publishes this resync reason.
	select {
	case evt := <-resyncEvents:
		data, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		require.Equal(
			t,
			event.ChainsyncResyncReasonHeaderValidationRecovery,
			data.Reason,
		)
		require.Equal(t, ledgerTipBlock.Slot, data.Point.Slot)
	default:
		t.Fatal(
			"header-validation recovery must publish a resync so chainsync " +
				"re-delivers",
		)
	}
}

// A rejected block at or behind the ledger tip has no rewind target that
// precedes it, so both the chain rewind and the ledger rollback would be
// no-ops against their own position and the block would survive. Reporting
// "recovered" there is worse than declining: the pipeline goes straight back
// into the same block, and because every restart looks like a successful
// recovery the stuck-pipeline signal never fires either.
func TestHeaderValidationRecoveryDeclinesAtOrBehindLedgerTip(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := make([]models.Block, 0, 3)
	for slot := uint64(1); slot <= 3; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(context.Background()))
	chainTipBefore := cm.PrimaryChain().Tip().Point.Slot

	for _, name := range []string{"at the tip", "behind the tip"} {
		failing := ledgerTipBlock
		if name == "behind the tip" {
			failing = blocks[1]
		}
		t.Run(name, func(t *testing.T) {
			recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(
				&headerValidationError{
					BlockPoint: makeTestPoint(failing),
					Cause:      errors.New("rejected"),
				},
			)
			require.NoError(t, recoverErr)
			require.False(t, recovered,
				"declining lets the failure surface; a false recovery hides it")
			require.Equal(t, chainTipBefore,
				cm.PrimaryChain().Tip().Point.Slot,
				"declining must not disturb the chain")
			select {
			case <-resyncEvents:
				t.Fatal("a declined recovery must not publish a resync")
			default:
			}
		})
	}
}

// Chain selection runs concurrently with the ledger pipeline, and it can move
// the primary chain off the ledger tip between the moment this recovery reads
// that tip and the moment it tries to rewind to it. The chain then refuses the
// rollback with ErrRollbackPointNotOnChain rather than splicing across forks.
//
// Reporting that as a failure is the wrong answer twice over. The rejected
// block is already gone from the primary chain -- chain selection removed it,
// which is the outcome this recovery exists to produce -- so there is nothing
// left to drop; and returning an error sends the deterministic header failure
// back through the pipeline as if it were unhandled. Treat it as handled and
// let the pipeline restart against the chain selection actually chose.
//
// Nothing is rewound and no resync is published, because both would act on a
// point the chain no longer holds. The move that abandoned it published its
// own rollback already, and pointing chainsync back at a dead branch would
// undo that.
func TestHeaderValidationRecoveryYieldsWhenChainSelectionMovedOn(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	// K=3 so the setup rollback below (tip slot 5 back to slot 2) is
	// allowed; the recovery under test never reaches a depth check.
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 3}))

	// Ledger applied through slot 3; the deferred check rejects slot 4.
	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(context.Background()))

	// Chain selection abandons the ledger tip: the primary chain drops back
	// past slot 3, so slot 3's block index now sits ahead of the chain tip
	// and the chain no longer holds it.
	require.NoError(t, cm.PrimaryChain().Rollback(context.Background(), makeTestPoint(blocks[1])))
	require.ErrorIs(t,
		cm.PrimaryChain().ValidateRollback(context.Background(), makeTestPoint(ledgerTipBlock)),
		chain.ErrRollbackPointNotOnChain,
		"the setup must leave the ledger tip off the primary chain, or this "+
			"test is not exercising the race it exists for")
	tipBeforeRecovery := cm.PrimaryChain().Tip().Point.Slot

	recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(blocks[3]),
			Cause:      errors.New("VRF leader value exceeds threshold"),
		},
	)
	require.NoError(t, recoverErr,
		"a chain that moved on is not a recovery failure; returning an "+
			"error here retries the deterministic header failure instead")
	require.True(t, recovered,
		"the rejected block is already off the primary chain, so the "+
			"pipeline should restart rather than report the failure")

	require.Equal(t, tipBeforeRecovery, cm.PrimaryChain().Tip().Point.Slot,
		"recovery must not rewind a chain it does not hold the point on")

	select {
	case evt := <-resyncEvents:
		t.Fatalf(
			"no resync may be published for a point the chain no longer "+
				"holds; got one for slot %d",
			evt.Data.(event.ChainsyncResyncEvent).Point.Slot,
		)
	default:
	}
}

// The rewind can raise the same not-on-chain sentinel the pre-check does,
// when chain selection moves in the gap between them. Both call sites decide
// through this one classifier so they cannot diverge -- if only the pre-check
// treated the condition as benign, the race would still be able to send the
// rejected header back round the pipeline as an unhandled error.
//
// Asserted on the classifier rather than end-to-end because that gap cannot
// be opened deterministically from a test: it needs chain selection to move
// between two adjacent calls. What is testable is that the classification is
// the same wherever the error comes from, and that it stays narrow.
func TestYieldedToChainSelectionClassifiesOnlyNotOnChain(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	validationErr := &headerValidationError{
		BlockPoint: ocommon.Point{Slot: 42},
		Cause:      errors.New("VRF leader value exceeds threshold"),
	}
	rewindPoint := ocommon.Point{Slot: 41}

	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"no error is not a yield", nil, false},
		{
			"the sentinel itself",
			chain.ErrRollbackPointNotOnChain,
			true,
		},
		{
			// The rewind wraps it twice over before the caller sees it, so
			// matching has to be on the sentinel and not on the message.
			"the sentinel wrapped by the rewind path",
			fmt.Errorf(
				"lookup rollback point: %w",
				fmt.Errorf("%w: slot 41", chain.ErrRollbackPointNotOnChain),
			),
			true,
		},
		{
			// Rolling back further than K is a real refusal to act on, not a
			// chain that moved: yielding would report a rewind that never
			// happened as a successful recovery.
			"a different chain refusal",
			chain.ErrRollbackExceedsSecurityParam,
			false,
		},
		{"an unrelated failure", errors.New("disk full"), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, ls.yieldedToChainSelection(
				c.err, validationErr, rewindPoint, "rewind",
			))
		})
	}
}

// The same-tip repair is bounded to the first completed recovery.
// tryRecoverFromHeaderValidationError passes repairSameTip only while
// lastHeaderValidationFailure does not already record this failure at this
// tip, so a redelivery of the identical rejected header reuses the state the
// first repair produced instead of paying a full metadata sweep on every
// retry of a header the network keeps offering.
func TestHeaderValidationRecoveryRepairsSameTipOnlyOnce(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip
	ls.metrics.init(prometheus.NewRegistry())
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(context.Background()))

	validationErr := &headerValidationError{
		BlockPoint: makeTestPoint(blocks[3]),
		Cause:      errors.New("VRF leader value exceeds threshold"),
	}

	generationBeforeFirst := ls.rewardInputGeneration.Load()
	recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(
		validationErr,
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered)
	require.Greater(t, ls.rewardInputGeneration.Load(), generationBeforeFirst,
		"the first completed rewind must repair same-tip metadata")
	require.Equal(t, ledgerTipBlock.Slot, ls.currentTip.Point.Slot)

	generationBeforeRepeat := ls.rewardInputGeneration.Load()
	recovered, recoverErr = ls.tryRecoverFromHeaderValidationError(
		validationErr,
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered)
	require.Equal(t, generationBeforeRepeat, ls.rewardInputGeneration.Load(),
		"a redelivered identical header at the same tip must reuse the "+
			"completed repair")
}

// Byron epoch-boundary blocks take the first slot of their epoch, and the
// epoch's first regular block takes the same slot whenever one is minted
// there. Mainnet does it at every Byron boundary -- the genesis EBB
// 89d9b5a5 at slot 0 is the direct parent of f0f7892b, also at slot 0 -- so a
// chain holding two distinct blocks at one slot is ordinary history rather
// than a fork.
func sameSlotBoundaryBlocks(t testing.TB) []models.Block {
	t.Helper()
	specs := []struct {
		slot uint64
		seed string
	}{
		{slot: 1, seed: "ancestor-1"},
		{slot: 2, seed: "ancestor-2"},
		{slot: 3, seed: "epoch-boundary"},
		{slot: 3, seed: "first-block-of-epoch"},
		{slot: 4, seed: "successor"},
	}
	blocks := make([]models.Block, 0, len(specs))
	for i, spec := range specs {
		hash := sha256.Sum256([]byte(spec.seed))
		block := models.Block{
			ID:     uint64(i + 1), //nolint:gosec
			Slot:   spec.slot,
			Hash:   hash[:],
			Number: uint64(i + 1), //nolint:gosec
			Type:   1,
			Cbor:   []byte{0x80},
		}
		if i > 0 {
			block.PrevHash = append([]byte(nil), blocks[i-1].Hash...)
		}
		blocks = append(blocks, block)
	}
	return blocks
}

type sameSlotRecoveryFixture struct {
	ls           *LedgerState
	cm           *chain.ChainManager
	blocks       []models.Block
	resyncEvents <-chan event.Event
}

func newSameSlotRecoveryFixture(t *testing.T) *sameSlotRecoveryFixture {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := sameSlotBoundaryBlocks(t)
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	// The ledger has applied the epoch-boundary block. The block that failed
	// validation is its direct successor, which shares its slot.
	boundary := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(boundary),
		BlockNumber: boundary.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = ledgerTip
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(context.Background()))
	require.Equal(t, blocks[4].Slot, cm.PrimaryChain().Tip().Point.Slot,
		"the rejected block and its successor start on the chain")

	return &sameSlotRecoveryFixture{
		ls:           ls,
		cm:           cm,
		blocks:       blocks,
		resyncEvents: resyncEvents,
	}
}

func TestHeaderValidationRecoveryDeclinesPastFailureAtSameSlot(
	t *testing.T,
) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	laterBlock := f.blocks[3]
	laterTip := ochainsync.Tip{
		Point:       makeTestPoint(laterBlock),
		BlockNumber: laterBlock.Number,
	}
	require.NoError(t, f.ls.db.SetTip(laterTip, nil))
	f.ls.currentTip = laterTip
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(boundary),
			Cause:      errors.New("failing EBB precedes the applied block"),
		},
	)

	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"a later same-slot tip must not be treated as preceding the failed EBB")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point)
	select {
	case <-f.resyncEvents:
		t.Fatal("declined recovery must not publish a resync")
	default:
	}
}

// The ledger tip is a valid rewind target whenever it is a different block
// from the one that failed and the chain orders it first. At a Byron epoch
// boundary that pair shares a slot, so a slot-only precedence test reports
// "no rewind target precedes it" for a target that does, declines a recovery
// that would have dropped the rejected block, and leaves the pipeline reading
// the same persisted block until the stuck detector fires.
func TestHeaderValidationRecoveryRewindsToSameSlotPredecessor(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	failing := f.blocks[3]
	require.Equal(t, boundary.Slot, failing.Slot)
	require.NotEqual(t, boundary.Hash, failing.Hash)

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(failing),
			Cause:      errors.New("VRF leader value exceeds threshold"),
		},
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered,
		"the applied epoch-boundary block precedes the rejected block and "+
			"is a rewind target")
	require.Equal(t, makeTestPoint(boundary),
		f.cm.PrimaryChain().Tip().Point,
		"the chain must be rewound onto the boundary block, not left "+
			"holding the rejected block at the same slot")

	select {
	case evt := <-f.resyncEvents:
		data, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		require.Equal(
			t,
			event.ChainsyncResyncReasonHeaderValidationRecovery,
			data.Reason,
		)
		require.Equal(t, makeTestPoint(boundary), data.Point)
	default:
		t.Fatal("recovery must publish a resync so chainsync re-delivers")
	}
}

// The same slot with the same hash is the ledger tip itself, which is what
// the guard exists to refuse: nothing would be dropped, so reporting a
// recovery would hide the failure from the stuck detector.
func TestHeaderValidationRecoveryDeclinesAtSameSlotSameHash(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(f.blocks[2]),
			Cause:      errors.New("rejected"),
		},
	)
	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"the ledger tip cannot be a rewind target for itself")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point,
		"declining must not disturb the chain")
	select {
	case <-f.resyncEvents:
		t.Fatal("a declined recovery must not publish a resync")
	default:
	}
}

// TestHeaderValidationRecoveryPenalizesOnlyTheResponsiblePeer pins that a
// deterministic deferred-validation failure targets the connection that
// supplied the block, and nothing else: a failure with no recorded source, or
// one caused by local state rather than the block, penalizes no peer.
func TestHeaderValidationRecoveryPenalizesOnlyTheResponsiblePeer(t *testing.T) {
	t.Parallel()

	source := testConnectionId(6101, 3001)
	for _, tc := range []struct {
		name      string
		source    ouroboros.ConnectionId
		cause     error
		wantBlame bool
	}{
		{
			"invalid block from a known peer",
			source,
			errors.New("VRF leader value exceeds threshold"),
			true,
		},
		{
			"unknown source",
			ouroboros.ConnectionId{},
			errors.New("VRF leader value exceeds threshold"),
			false,
		},
		{
			"local snapshot gap",
			source,
			fmt.Errorf("gap: %w", errLeaderStakeSnapshotUnavailable),
			false,
		},
		{
			"still deferred",
			source,
			fmt.Errorf("wait: %w", errHeaderVerificationDeferred),
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
			require.NoError(t, err)
			t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

			blocks := make([]models.Block, 0, 5)
			for slot := uint64(1); slot <= 5; slot++ {
				block := makeTestBlock(slot, slot)
				if len(blocks) > 0 {
					block.PrevHash = append(
						[]byte(nil), blocks[len(blocks)-1].Hash...,
					)
				}
				blocks = append(blocks, block)
				require.NoError(t, db.BlockCreate(block, nil))
			}
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			require.NoError(
				t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
			)
			ledgerTip := ochainsync.Tip{
				Point:       makeTestPoint(blocks[2]),
				BlockNumber: blocks[2].Number,
			}
			require.NoError(t, db.SetTip(ledgerTip, nil))

			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Close)
			_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)
			ls := &LedgerState{
				db:    db,
				chain: cm.PrimaryChain(),
				config: LedgerStateConfig{
					ChainManager: cm,
					EventBus:     bus,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}
			ls.currentTip = ledgerTip
			ls.metrics.init(prometheus.NewRegistry())
			require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip())

			recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(
				&headerValidationError{
					BlockPoint: makeTestPoint(blocks[3]),
					Cause:      tc.cause,
					Source:     tc.source,
				},
			)
			require.NoError(t, recoverErr)
			require.True(t, recovered)

			first := testutil.RequireReceive(
				t, resyncEvents, 2*time.Second, "general resync",
			)
			general, ok := first.Data.(event.ChainsyncResyncEvent)
			require.True(t, ok)
			require.Equal(
				t,
				event.ChainsyncResyncReasonHeaderValidationRecovery,
				general.Reason,
			)
			require.Equal(t, ouroboros.ConnectionId{}, general.ConnectionId)

			if !tc.wantBlame {
				require.Never(
					t,
					func() bool { return len(resyncEvents) > 0 },
					100*time.Millisecond,
					10*time.Millisecond,
					"no peer may be penalized",
				)
				return
			}
			second := testutil.RequireReceive(
				t, resyncEvents, 2*time.Second, "peer penalty",
			)
			penalty, ok := second.Data.(event.ChainsyncResyncEvent)
			require.True(t, ok)
			require.Equal(
				t,
				event.ChainsyncResyncReasonDeferredHeaderValidationFailure,
				penalty.Reason,
			)
			require.Equal(t, tc.source, penalty.ConnectionId)
			require.Equal(t, blocks[2].Slot, penalty.Point.Slot)
		})
	}
}
