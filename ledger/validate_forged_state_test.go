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
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

const forgedStateTestTimeout = 10 * time.Second

// forgedValidationWaits signals every time forged-block validation logs that
// it is waiting for the ledger state to settle, so a test can act at exactly
// that point instead of guessing when the validator reached it.
type forgedValidationWaits struct {
	waits chan struct{}
}

func (h *forgedValidationWaits) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *forgedValidationWaits) Handle(_ context.Context, r slog.Record) error {
	if r.Message == forgedValidationWaitMsg {
		h.waits <- struct{}{}
	}
	return nil
}

func (h *forgedValidationWaits) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *forgedValidationWaits) WithGroup(string) slog.Handler { return h }

// stubForgedTxBlock is a one-transaction block naming parent as its
// predecessor. Its Babbage era keeps the Conway-only reference-script budget
// out of the way of the transaction session under test.
type stubForgedTxBlock struct {
	stubValidateBlock
	parent lcommon.Blake2b256
	txs    []lcommon.Transaction
}

func (b *stubForgedTxBlock) Header() lcommon.BlockHeader { return b }

func (b *stubForgedTxBlock) PrevHash() lcommon.Blake2b256 { return b.parent }

func (b *stubForgedTxBlock) BlockNumber() uint64 { return 2 }

func (b *stubForgedTxBlock) Transactions() []lcommon.Transaction { return b.txs }

func forgedStateTestHash(b byte) lcommon.Blake2b256 {
	var h lcommon.Blake2b256
	h[0] = b
	return h
}

// newForgedStateTestLedger returns a ledger at tip whose transaction rule
// counts its calls and returns verdict.
func newForgedStateTestLedger(
	t *testing.T,
	tip lcommon.Blake2b256,
	verdict error,
) (*LedgerState, *atomic.Int32, chan struct{}) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	calls := &atomic.Int32{}
	ls := newFakeEraLedgerState(db, func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error {
		calls.Add(1)
		return verdict
	}, nil)
	waits := &forgedValidationWaits{waits: make(chan struct{}, 64)}
	ls.config.Logger = slog.New(waits)
	// The forged slot outlasts every test; the tests about the slot bound
	// move its end.
	setForgedStateTestSlotEnd(ls, time.Now().Add(time.Hour))
	setForgedStateTestTip(ls, tip)
	return ls, calls, waits.waits
}

// setForgedStateTestSlotEnd places the end of the stub block's slot 10 at
// end, on a chain of one-second slots.
func setForgedStateTestSlotEnd(ls *LedgerState, end time.Time) {
	systemStart := end.Add(-11 * time.Second)
	ls.timeConverter = NewSlotTimeConverter(SlotTimeConverterDeps{
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
			return nil, errors.New("no era summary in this fixture")
		},
		ShelleyGenesis: func() *shelley.ShelleyGenesis {
			return &shelley.ShelleyGenesis{SystemStart: systemStart}
		},
		EpochCache: func() []models.Epoch {
			return []models.Epoch{
				{StartSlot: 0, LengthInSlots: 100_000, SlotLength: 1000},
			}
		},
	})
}

func setForgedStateTestTip(ls *LedgerState, tip lcommon.Blake2b256) {
	ls.Lock()
	defer ls.Unlock()
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(9, tip.Bytes()),
		BlockNumber: 1,
	}
	ls.publishSnapshotsLocked()
}

func newForgedStateTestBlock(parent lcommon.Blake2b256) *stubForgedTxBlock {
	return &stubForgedTxBlock{
		stubValidateBlock: stubValidateBlock{slot: 10},
		parent:            parent,
		txs: []lcommon.Transaction{
			&conway.ConwayTransaction{TxIsValid: true},
		},
	}
}

// validateForgedTxsAsync runs validation in the background and waits until it
// reports waiting for the ledger to settle. Returning first is the failure
// this exists to catch, so it fails the test with whatever came back.
func validateForgedTxsAsync(
	ctx context.Context,
	t *testing.T,
	ls *LedgerState,
	block *stubForgedTxBlock,
	waits <-chan struct{},
) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- ls.validateForgedTxs(ctx, block) }()
	select {
	case <-waits:
	case err := <-result:
		t.Fatalf(
			"validation returned instead of waiting for the ledger to settle: %v",
			err,
		)
	case <-time.After(forgedStateTestTimeout):
		t.Fatal("validation neither waited nor returned")
	}
	return result
}

// A block apply in flight when a forged block is self-validated makes the
// session non-current before its first transaction. The block is not
// invalid, so validation waits for the transition to publish and validates
// against the settled state instead of dropping the block.
func TestValidateForgedBlockWaitsOutAnInFlightTransition(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	ls, calls, waits := newForgedStateTestLedger(t, parent, nil)
	block := newForgedStateTestBlock(parent)

	finish := ls.beginTxValidationTransition()
	t.Cleanup(finish)
	result := validateForgedTxsAsync(t.Context(), t, ls, block, waits)
	finish()

	require.NoError(
		t,
		testutil.RequireReceive(
			t,
			result,
			forgedStateTestTimeout,
			"forged validation result",
		),
	)
	require.Equal(
		t,
		int32(1),
		calls.Load(),
		"transactions are validated once, against the settled state",
	)
}

// A ledger that was still applying the forged block's parent when validation
// started settles on that parent: the state the block extends, so it is
// validated there.
func TestValidateForgedBlockValidatesOnceTheLedgerReachesTheParent(
	t *testing.T,
) {
	t.Parallel()
	grandparent := forgedStateTestHash(0x01)
	parent := forgedStateTestHash(0x02)
	ls, calls, waits := newForgedStateTestLedger(t, grandparent, nil)
	block := newForgedStateTestBlock(parent)

	finish := ls.beginTxValidationTransition()
	t.Cleanup(finish)
	result := validateForgedTxsAsync(t.Context(), t, ls, block, waits)
	setForgedStateTestTip(ls, parent)
	finish()

	require.NoError(
		t,
		testutil.RequireReceive(
			t,
			result,
			forgedStateTestTimeout,
			"forged validation result",
		),
	)
	require.Equal(t, int32(1), calls.Load())
}

// A ledger that settles on any other tip -- a sibling of the parent, or a
// block past it -- has left the chain the forged block extends. Validating
// there would judge the block against state it does not build on, so
// validation stops without a verdict.
func TestValidateForgedBlockStopsWhenTheTipLeavesTheParent(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	ls, calls, waits := newForgedStateTestLedger(t, parent, nil)
	block := newForgedStateTestBlock(parent)

	finish := ls.beginTxValidationTransition()
	t.Cleanup(finish)
	result := validateForgedTxsAsync(t.Context(), t, ls, block, waits)
	setForgedStateTestTip(ls, forgedStateTestHash(0x03))
	finish()

	err := testutil.RequireReceive(
		t,
		result,
		forgedStateTestTimeout,
		"forged validation result",
	)
	require.ErrorIs(t, err, forging.ErrForgedBlockStateChanged)
	require.Zero(t, calls.Load(), "no verdict is taken against a foreign tip")
}

// Waiting never outlives the forged slot: once it has ended there is nothing
// left to save, so validation stops at once rather than waiting at all.
func TestValidateForgedBlockDoesNotWaitPastTheForgedSlot(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	ls, calls, waits := newForgedStateTestLedger(t, parent, nil)
	setForgedStateTestSlotEnd(ls, time.Now().Add(-time.Hour))
	block := newForgedStateTestBlock(parent)
	finish := ls.beginTxValidationTransition()
	t.Cleanup(finish)

	err := ls.validateForgedTxs(t.Context(), block)

	require.ErrorIs(t, err, forging.ErrForgedBlockStateChanged)
	require.Zero(t, calls.Load())
	require.Empty(t, waits, "an ended slot is not waited for")
}

// A slot whose end cannot be placed gives no bound to wait within, so
// validation does not wait at all: a transition that never finished would
// otherwise hold the forger indefinitely.
func TestValidateForgedBlockDoesNotWaitWithoutASlotEnd(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	ls, calls, waits := newForgedStateTestLedger(t, parent, nil)
	ls.timeConverter = NewSlotTimeConverter(SlotTimeConverterDeps{
		HardForkSummary: func(uint64) (*hardfork.Summary, error) {
			return nil, errors.New("no era summary in this fixture")
		},
		ShelleyGenesis: func() *shelley.ShelleyGenesis { return nil },
		EpochCache:     func() []models.Epoch { return nil },
	})
	block := newForgedStateTestBlock(parent)
	finish := ls.beginTxValidationTransition()
	t.Cleanup(finish)

	err := ls.validateForgedTxs(t.Context(), block)

	require.ErrorIs(t, err, forging.ErrForgedBlockStateChanged)
	require.Zero(t, calls.Load())
	require.Empty(t, waits)
}

// Inside the slot, waiting is still bounded: each attempt needs a fresh
// publication or finished transition to start, and attempts are capped, so
// a ledger that never settles cannot spin the forger.
func TestValidateForgedBlockCapsAttemptsWhenTheStateNeverSettles(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	ls, calls, waits := newForgedStateTestLedger(t, parent, nil)
	block := newForgedStateTestBlock(parent)

	held := ls.beginTxValidationTransition()
	t.Cleanup(held)
	result := validateForgedTxsAsync(t.Context(), t, ls, block, waits)
	waited := 1
	for {
		// Another writer finishes, which wakes the validator, while the
		// held transition keeps every attempt non-current.
		ls.beginTxValidationTransition()()
		select {
		case <-waits:
			waited++
			continue
		case err := <-result:
			require.ErrorIs(t, err, forging.ErrForgedBlockStateChanged)
		case <-time.After(forgedStateTestTimeout):
			t.Fatal("validation neither waited again nor returned")
		}
		break
	}
	require.Equal(t, forgedValidationMaxAttempts-1, waited)
	require.Zero(t, calls.Load())
}

// Cancellation while waiting returns the context's error, as cancellation
// during validation always has.
func TestValidateForgedBlockReturnsCancellationWhileWaiting(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	ls, _, waits := newForgedStateTestLedger(t, parent, nil)
	block := newForgedStateTestBlock(parent)

	finish := ls.beginTxValidationTransition()
	t.Cleanup(finish)
	ctx, cancel := context.WithCancel(t.Context())
	result := validateForgedTxsAsync(ctx, t, ls, block, waits)
	cancel()

	err := testutil.RequireReceive(
		t,
		result,
		forgedStateTestTimeout,
		"forged validation result",
	)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, forging.ErrForgedBlockStateChanged)
}

// A transaction that breaks a ledger rule against a current state is a
// verdict. It is returned as the failure it is, never as a changed state,
// and is not retried.
func TestValidateForgedBlockReturnsARuleFailureWithoutRetrying(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	rule := errors.New("BadInputsUTxO")
	ls, calls, waits := newForgedStateTestLedger(t, parent, rule)
	block := newForgedStateTestBlock(parent)

	err := ls.validateForgedTxs(t.Context(), block)

	require.ErrorIs(t, err, rule)
	require.NotErrorIs(t, err, forging.ErrForgedBlockStateChanged)
	require.Equal(t, int32(1), calls.Load())
	require.Empty(t, waits)
}

// newForgedStateTestChain wires a primary chain of three generated blocks
// (A, B, P) into ls and returns them. The forger's tip gates admit a slot
// while the ledger trails the primary chain by up to two blocks, so a block
// can be forged on P while the ledger has applied only A.
func newForgedStateTestChain(
	t *testing.T,
	ls *LedgerState,
) []lcommon.Block {
	t.Helper()
	cm, err := chain.NewManager(t.Context(), ls.db, nil)
	require.NoError(t, err)
	primary := cm.PrimaryChain()
	blocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		1,
		1,
		3,
	)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, primary.AddBlock(t.Context(), block, nil))
	}
	ls.Lock()
	ls.chain = primary
	ls.Unlock()
	return blocks
}

func setForgedStateTestTipBlock(ls *LedgerState, block lcommon.Block) {
	ls.Lock()
	defer ls.Unlock()
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			block.SlotNumber(),
			block.Hash().Bytes(),
		),
		BlockNumber: block.BlockNumber(),
	}
	ls.publishSnapshotsLocked()
}

// The ledger trails the primary chain: applied tip A, unapplied B and P, and
// a block forged on P. Applying B moves the ledger to neither the tip
// validation started from nor the parent, but B is on the chain the block
// extends, so validation keeps waiting and judges the block once P is
// applied.
func TestValidateForgedBlockFollowsTheLedgerAlongTheParentChain(
	t *testing.T,
) {
	t.Parallel()
	ls, calls, waits := newForgedStateTestLedger(
		t,
		forgedStateTestHash(0x01),
		nil,
	)
	chainBlocks := newForgedStateTestChain(t, ls)
	a, b, p := chainBlocks[0], chainBlocks[1], chainBlocks[2]
	setForgedStateTestTipBlock(ls, a)
	block := newForgedStateTestBlock(p.Hash())

	applyB := ls.beginTxValidationTransition()
	t.Cleanup(applyB)
	result := validateForgedTxsAsync(t.Context(), t, ls, block, waits)
	// B publishes while P's apply has already begun.
	applyP := ls.beginTxValidationTransition()
	t.Cleanup(applyP)
	setForgedStateTestTipBlock(ls, b)
	applyB()
	select {
	case <-waits:
	case err := <-result:
		t.Fatalf("validation stopped on the parent's own chain: %v", err)
	case <-time.After(forgedStateTestTimeout):
		t.Fatal("validation neither waited nor returned")
	}
	setForgedStateTestTipBlock(ls, p)
	applyP()

	require.NoError(
		t,
		testutil.RequireReceive(
			t,
			result,
			forgedStateTestTimeout,
			"forged validation result",
		),
	)
	require.Equal(t, int32(1), calls.Load())
}

// With the primary chain known, a tip off the segment ending at the parent
// is still a chain the block does not extend.
func TestValidateForgedBlockStopsOnATipOffTheParentChain(t *testing.T) {
	t.Parallel()
	ls, calls, waits := newForgedStateTestLedger(
		t,
		forgedStateTestHash(0x01),
		nil,
	)
	chainBlocks := newForgedStateTestChain(t, ls)
	a, p := chainBlocks[0], chainBlocks[2]
	setForgedStateTestTipBlock(ls, a)
	block := newForgedStateTestBlock(p.Hash())

	finish := ls.beginTxValidationTransition()
	t.Cleanup(finish)
	result := validateForgedTxsAsync(t.Context(), t, ls, block, waits)
	setForgedStateTestTip(ls, forgedStateTestHash(0x03))
	finish()

	err := testutil.RequireReceive(
		t,
		result,
		forgedStateTestTimeout,
		"forged validation result",
	)
	require.ErrorIs(t, err, forging.ErrForgedBlockStateChanged)
	require.Zero(t, calls.Load())
}

// A verdict reached on a session that stopped being current is not a verdict
// on the block: the rule failure may come from the state that moved. It is
// reported as a changed state so the block is retried or skipped, never
// dropped as invalid.
func TestValidateForgedBlockDoesNotTrustARuleFailureFromAStaleSession(
	t *testing.T,
) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	moved := forgedStateTestHash(0x02)
	rule := errors.New("BadInputsUTxO")
	ls, _, _ := newForgedStateTestLedger(t, parent, nil)
	// The state moves while the transaction is being judged, then the rule
	// fails against what the session saw.
	ls.Lock()
	ls.currentEra.ValidateTxFunc = func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error {
		setForgedStateTestTip(ls, moved)
		return rule
	}
	ls.publishSnapshotsLocked()
	ls.Unlock()
	block := newForgedStateTestBlock(parent)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	err := ls.validateForgedTxs(ctx, block)

	require.NotErrorIs(t, err, rule)
}

// The forger admits a parent that is still unapplied, so the ledger can
// trail the block's parent with nothing in flight. Judging the block there
// would use state without the unapplied ancestors' spent inputs and
// certificates, so validation waits for the ledger to reach the parent and
// takes its verdict only then.
func TestValidateForgedBlockWaitsForUnappliedAncestors(t *testing.T) {
	t.Parallel()
	rule := errors.New("unknown voter")
	ls, calls, waits := newForgedStateTestLedger(
		t,
		forgedStateTestHash(0x01),
		rule,
	)
	chainBlocks := newForgedStateTestChain(t, ls)
	a, b, p := chainBlocks[0], chainBlocks[1], chainBlocks[2]
	setForgedStateTestTipBlock(ls, a)
	block := newForgedStateTestBlock(p.Hash())

	result := validateForgedTxsAsync(t.Context(), t, ls, block, waits)
	require.Zero(t, calls.Load(), "no verdict before the parent is applied")

	setForgedStateTestTipBlock(ls, b)
	select {
	case <-waits:
	case err := <-result:
		t.Fatalf("validation returned with the parent still unapplied: %v", err)
	case <-time.After(forgedStateTestTimeout):
		t.Fatal("validation neither waited nor returned")
	}
	require.Zero(t, calls.Load(), "no verdict before the parent is applied")

	setForgedStateTestTipBlock(ls, p)
	err := testutil.RequireReceive(
		t,
		result,
		forgedStateTestTimeout,
		"forged validation result",
	)
	require.ErrorIs(t, err, rule, "the verdict is taken at the parent")
	require.Equal(t, int32(1), calls.Load())
}

// A slot that ends while an ancestor is still unapplied drops the block
// unjudged instead of validating it against the applied state.
func TestValidateForgedBlockDropsUnappliedAncestorsAtSlotEnd(t *testing.T) {
	t.Parallel()
	ls, calls, waits := newForgedStateTestLedger(
		t,
		forgedStateTestHash(0x01),
		nil,
	)
	setForgedStateTestSlotEnd(ls, time.Now().Add(-time.Hour))
	chainBlocks := newForgedStateTestChain(t, ls)
	setForgedStateTestTipBlock(ls, chainBlocks[0])
	block := newForgedStateTestBlock(chainBlocks[2].Hash())

	err := ls.validateForgedTxs(t.Context(), block)

	require.ErrorIs(t, err, forging.ErrForgedBlockStateChanged)
	require.Zero(t, calls.Load())
	require.Empty(t, waits)
}

// When the parent is the applied tip nothing is unapplied: validation
// judges the block in its first session without waiting.
func TestValidateForgedBlockDoesNotWaitWhenParentIsApplied(t *testing.T) {
	t.Parallel()
	parent := forgedStateTestHash(0x01)
	ls, calls, waits := newForgedStateTestLedger(t, parent, nil)

	require.NoError(
		t,
		ls.validateForgedTxs(t.Context(), newForgedStateTestBlock(parent)),
	)
	require.Equal(t, int32(1), calls.Load())
	require.Empty(t, waits)
}

// A verdict is taken only at the block's parent. A ledger tip that trails a
// parent the primary chain has already passed, a tip past the parent (the
// rival an alternative block competes with, once applied), and a tip off
// the primary chain are each a state the block does not extend, so the
// block is dropped unjudged at once instead of being judged there.
func TestValidateForgedBlockTakesNoVerdictOffTheParent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		tip    func(blocks []lcommon.Block) (lcommon.Block, bool)
		parent func(blocks []lcommon.Block) lcommon.Blake2b256
	}{
		{
			name: "trailing a parent the primary chain has passed",
			tip: func(blocks []lcommon.Block) (lcommon.Block, bool) {
				return blocks[0], true
			},
			parent: func(blocks []lcommon.Block) lcommon.Blake2b256 {
				return blocks[1].Hash()
			},
		},
		{
			name: "past the parent",
			tip: func(blocks []lcommon.Block) (lcommon.Block, bool) {
				return blocks[2], true
			},
			parent: func(blocks []lcommon.Block) lcommon.Blake2b256 {
				return blocks[1].Hash()
			},
		},
		{
			name: "off the primary chain",
			tip: func([]lcommon.Block) (lcommon.Block, bool) {
				return nil, false
			},
			parent: func(blocks []lcommon.Block) lcommon.Blake2b256 {
				return blocks[2].Hash()
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, calls, waits := newForgedStateTestLedger(
				t,
				forgedStateTestHash(0x09),
				errors.New("unknown voter"),
			)
			chainBlocks := newForgedStateTestChain(t, ls)
			if tip, ok := tc.tip(chainBlocks); ok {
				setForgedStateTestTipBlock(ls, tip)
			}
			block := newForgedStateTestBlock(tc.parent(chainBlocks))

			err := ls.validateForgedTxs(t.Context(), block)

			require.ErrorIs(t, err, forging.ErrForgedBlockStateChanged)
			require.Zero(t, calls.Load(), "no verdict away from the parent")
			require.Empty(t, waits)
		})
	}
}
