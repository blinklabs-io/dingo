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
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

	cm, err := chain.NewManager(db, nil)
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
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip())
	require.Equal(t, blocks[4].Slot, cm.PrimaryChain().Tip().Point.Slot,
		"the rejected block and its successor should start on the chain")

	recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(blocks[3]),
			Cause:      errors.New("VRF leader value exceeds threshold"),
		},
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered,
		"a rejected block above the ledger tip must be recoverable")

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

// A rejected block at or behind the ledger tip has no rewind target that
// precedes it, so both the chain rewind and the ledger rollback would be
// no-ops against their own position and the block would survive. Reporting
// "recovered" there is worse than declining: the pipeline goes straight back
// into the same block, and because every restart looks like a successful
// recovery the stuck-pipeline signal never fires either.
func TestHeaderValidationRecoveryDeclinesAtOrBehindLedgerTip(t *testing.T) {
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

	cm, err := chain.NewManager(db, nil)
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
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip())
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
