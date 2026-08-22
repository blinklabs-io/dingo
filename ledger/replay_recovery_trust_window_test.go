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
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trustWindowLedger is a LedgerState sitting at tip with a Mithril trust
// anchor, plus the resync events its recovery publishes.
type trustWindowLedger struct {
	ls        *LedgerState
	ledgerTip ochainsync.Tip
	failing   *txValidationError
	resyncs   chan event.ChainsyncResyncEvent
}

// newTrustWindowLedger builds the shape of issue #3261: the applied ledger tip
// sits exactly on the Mithril trust anchor and the failing block is a short
// distance past it, so the only rewind target at or above the anchor is the tip
// itself. Every deeper target the at-tip recovery schedule produces lands on
// the one earlier block, far below the anchor, and is refused by the trust
// boundary guard.
func newTrustWindowLedger(
	t *testing.T,
	anchorSlot uint64,
) *trustWindowLedger {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	parentBlock := testRawBlock("trust-window-parent", 100, 1, nil)
	ledgerTipBlock := testRawBlock(
		"trust-window-ledger-tip",
		200000,
		2,
		parentBlock.Hash,
	)
	failingBlock := testRawBlock(
		"trust-window-failing",
		200020,
		3,
		ledgerTipBlock.Hash,
	)
	require.NoError(t, cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
		parentBlock,
		ledgerTipBlock,
		failingBlock,
	}))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	resyncs := make(chan event.ChainsyncResyncEvent, 64)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			resync, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncs <- resync:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})
	activeConnId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 32610},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 61), Port: 32611},
	}

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		EventBus:          bus,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			return &activeConnId
		},
	})
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(ls))
	ls.metrics.init(prometheus.NewRegistry())

	ledgerTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			ledgerTipBlock.Slot,
			ledgerTipBlock.Hash,
		),
		BlockNumber: ledgerTipBlock.BlockNumber,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))
	ls.currentTip = ledgerTip
	ls.currentTipBlockNonce = []byte("nonce-trust-window-tip")
	ls.mithrilLedgerSlot = anchorSlot
	ls.validationEnabled = true
	ls.reachedTip.Store(true)
	ls.publishSnapshotsLocked()
	producerTxHash := testHashBytes("trust-window-producer-tx")
	seedReplayRecoveryTransaction(
		t,
		db,
		producerTxHash,
		parentBlock.Hash,
		parentBlock.Slot,
	)

	return &trustWindowLedger{
		ls:        ls,
		ledgerTip: ledgerTip,
		resyncs:   resyncs,
		failing: &txValidationError{
			BlockPoint: ocommon.NewPoint(
				failingBlock.Slot,
				failingBlock.Hash,
			),
			TxHash: testHashBytes("trust-window-failing-tx"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  producerTxHash,
					index: 0,
				},
			},
			// Conway rule 45. Deterministic in the same sense as a
			// duplicate input, but state-dependent, so no fixed error
			// list classifies it.
			Cause: errors.New(
				"conway certificate rule 45: delegate to an unregistered stake credential",
			),
		},
	}
}

func (f *trustWindowLedger) drainResyncs() int {
	count := 0
	for {
		select {
		case <-f.resyncs:
			count++
		default:
			return count
		}
	}
}

// TestAtTipRecoveryInsideMithrilTrustWindowReachesTerminalState covers issue
// #3261: when every rewind target the at-tip recovery schedule produces lies
// inside the Mithril protected window, the trust boundary guard refuses all of
// them. Each refusal rewinds to the applied tip and asks ChainSync for a fresh
// intersection, which cannot help for a canonical block -- every peer serves
// the same one. Recovery must therefore stop and surface a terminal condition
// instead of rejecting peers forever.
func TestAtTipRecoveryInsideMithrilTrustWindowReachesTerminalState(
	t *testing.T,
) {
	// Anchor exactly at the applied tip: rewinding to the tip is legal,
	// everything deeper is not.
	fixture := newTrustWindowLedger(t, 200000)

	const maxAttempts = 32
	halted := false
	recoveries := 0
	for range maxAttempts {
		recovered, err := fixture.ls.tryRecoverFromTxValidationError(
			fixture.failing,
		)
		if err != nil {
			require.ErrorIs(
				t,
				err,
				errHaltLedgerPipeline,
				"the only terminal error recovery may raise is a pipeline halt",
			)
			assert.False(
				t,
				recovered,
				"a halted recovery must not report itself recovered",
			)
			halted = true
			break
		}
		require.True(t, recovered)
		recoveries++
	}
	require.True(
		t,
		halted,
		"recovery inside the Mithril trust window must reach a terminal state, not retry forever",
	)
	assert.Less(
		t,
		recoveries,
		maxAttempts,
		"the terminal state must be reached in a bounded number of attempts",
	)
	assert.Positive(
		t,
		recoveries,
		"the existing recovery path must be tried before declaring a halt",
	)

	// The terminal state is sticky and quiet: further deliveries of the same
	// block keep halting and must not ask for yet another fresh intersection.
	fixture.drainResyncs()
	for range 3 {
		recovered, err := fixture.ls.tryRecoverFromTxValidationError(
			fixture.failing,
		)
		assert.False(t, recovered)
		require.ErrorIs(t, err, errHaltLedgerPipeline)
	}
	assert.Zero(
		t,
		fixture.drainResyncs(),
		"a halted recovery must not keep rotating peers",
	)
}

// TestAtTipRecoveryAboveMithrilTrustWindowKeepsRecovering is the negative case.
// A failure whose rewind targets stay at or above the trust anchor is still
// repairable by replaying a different local history, so it must keep taking the
// existing recovery path however often it repeats. Short-circuiting it into a
// halt would turn an ordinary fork-selection problem into an outage.
func TestAtTipRecoveryAboveMithrilTrustWindowKeepsRecovering(t *testing.T) {
	// Anchor far below the earliest block, so no rewind target the schedule
	// produces is ever refused by the trust boundary guard.
	fixture := newTrustWindowLedger(t, 50)

	for i := range 32 {
		recovered, err := fixture.ls.tryRecoverFromTxValidationError(
			fixture.failing,
		)
		require.NoError(
			t,
			err,
			"attempt %d: a repairable failure must not halt the pipeline",
			i+1,
		)
		require.True(t, recovered, "attempt %d must still recover", i+1)
	}
}

// TestReplayRecoveryBelowMithrilTrustBoundaryReachesTerminalState covers the
// post-bootstrap replay path from issue #3301. The producer is canonical but
// below the imported anchor, so its parent can never be a legal local rewind
// target. Changing failing block/transaction identities must not rearm the
// budget while the applied tip remains fixed; replay failures can creep
// forward in exactly that shape.
func TestReplayRecoveryBelowMithrilTrustBoundaryReachesTerminalState(
	t *testing.T,
) {
	fixture := newTrustWindowLedger(t, 200000)
	fixture.ls.reachedTip.Store(false)

	var halted bool
	for attempt := range maxMithrilBoundaryRecoveryRejections + 2 {
		failure := *fixture.failing
		failure.BlockPoint = ocommon.NewPoint(
			fixture.failing.BlockPoint.Slot+uint64(attempt),
			testHashBytes(fmt.Sprintf("replay-failing-block-%d", attempt)),
		)
		failure.TxHash = testHashBytes(
			fmt.Sprintf("replay-failing-tx-%d", attempt),
		)

		recovered, err := fixture.ls.tryRecoverFromTxValidationError(&failure)
		if errors.Is(err, errHaltLedgerPipeline) {
			assert.False(t, recovered)
			halted = true
			break
		}
		require.NoError(t, err)
		require.True(t, recovered)
	}
	require.True(
		t,
		halted,
		"replay recovery must halt even when failure identities vary at a fixed applied tip",
	)
	assert.Equal(t, fixture.ledgerTip, fixture.ls.currentTip)
	assert.Equal(
		t,
		1.0,
		promtestutil.ToFloat64(
			fixture.ls.metrics.mithrilTrustWindowUnrepairable,
		),
		"the terminal replay state must be operator-visible",
	)
}

// TestMithrilBoundaryRollbackFailureDoesNotConsumeRecoveryBudget covers the
// chain-selection race called out in review: a local rollback failure is not a
// successful replay attempt and must not count toward declaring validation
// unrepairable.
func TestMithrilBoundaryRollbackFailureDoesNotConsumeRecoveryBudget(
	t *testing.T,
) {
	fixture := newTrustWindowLedger(t, 200000)

	// Establish one successful boundary recovery, which removes the failing
	// block from the primary chain while keeping its point available to this
	// synthetic validation error.
	require.NoError(
		t,
		fixture.ls.rejectRecoveryAtMithrilBoundary(
			"test boundary recovery",
			fixture.failing,
			func(uint64, ocommon.Point) {},
		),
	)
	require.Equal(
		t,
		1,
		fixture.ls.mithrilBoundaryRecovery.rejections,
	)

	// Model chain selection moving off the captured ledger tip before the
	// local rollback begins. The point no longer exists on the primary chain,
	// so every attempt fails in rollback mechanics rather than completing a
	// validation-recovery replay.
	fixture.ls.currentTip = ochainsync.Tip{Point: fixture.failing.BlockPoint}
	for range maxMithrilBoundaryRecoveryRejections + 2 {
		err := fixture.ls.rejectRecoveryAtMithrilBoundary(
			"test boundary recovery",
			fixture.failing,
			func(uint64, ocommon.Point) {},
		)
		require.Error(t, err)
		require.NotErrorIs(
			t,
			err,
			errHaltLedgerPipeline,
			"rollback mechanics failures must not exhaust validation recovery",
		)
	}
	assert.Equal(
		t,
		1,
		fixture.ls.mithrilBoundaryRecovery.rejections,
	)
	assert.Zero(
		t,
		promtestutil.ToFloat64(
			fixture.ls.metrics.mithrilTrustWindowUnrepairable,
		),
	)
}
