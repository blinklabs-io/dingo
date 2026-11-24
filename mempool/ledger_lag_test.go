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

package mempool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// laggingLedgerValidator models a ledger that applies blocks after the chain
// store has announced them. It holds a UTxO set and an applied tip, and
// validates a transaction's consumed inputs the way LedgerView.UtxoById
// resolves them: pending spends first, then pending outputs, then the ledger.
type laggingLedgerValidator struct {
	mu    sync.Mutex
	utxos map[utxoref.Key]struct{}
	tip   uint64
	// onValidate, when set, runs once before the next validation.
	onValidate func()
}

func newLaggingLedgerValidator(tipSlot uint64) *laggingLedgerValidator {
	return &laggingLedgerValidator{
		utxos: make(map[utxoref.Key]struct{}),
		tip:   tipSlot,
	}
}

func (v *laggingLedgerValidator) fund(in indexTxInput) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.utxos[utxoref.Key{
		TxId:  lcommon.NewBlake2b256(in.hash[:]),
		Index: in.index,
	}] = struct{}{}
}

// applyBlock applies txs as the ledger would and moves the tip to slot.
func (v *laggingLedgerValidator) applyBlock(
	slot uint64,
	txs ...gledger.Transaction,
) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, tx := range txs {
		for _, in := range tx.Consumed() {
			delete(v.utxos, utxoref.ForInput(in))
		}
		for _, utxo := range tx.Produced() {
			v.utxos[utxoref.ForUtxo(utxo)] = struct{}{}
		}
	}
	v.tip = slot
}

func (v *laggingLedgerValidator) Tip() ochainsync.Tip {
	v.mu.Lock()
	defer v.mu.Unlock()
	return ochainsync.Tip{Point: ocommon.Point{Slot: v.tip}}
}

func (v *laggingLedgerValidator) ValidateTx(tx gledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil, nil)
}

func (v *laggingLedgerValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	consumed map[utxoref.Key]struct{},
	created map[utxoref.Key]lcommon.Utxo,
	_ *utxoref.StateOverlay,
) error {
	v.mu.Lock()
	hook := v.onValidate
	v.onValidate = nil
	v.mu.Unlock()
	if hook != nil {
		hook()
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, in := range tx.Consumed() {
		key := utxoref.ForInput(in)
		if _, ok := consumed[key]; ok {
			return fmt.Errorf("utxo %s: already consumed", key.String())
		}
		if _, ok := created[key]; ok {
			continue
		}
		if _, ok := v.utxos[key]; !ok {
			return fmt.Errorf("utxo %s: not found", key.String())
		}
	}
	for _, in := range tx.ReferenceInputs() {
		key := utxoref.ForInput(in)
		if _, ok := consumed[key]; ok {
			return fmt.Errorf("utxo %s: already consumed", key.String())
		}
		if _, ok := created[key]; ok {
			continue
		}
		if _, ok := v.utxos[key]; !ok {
			return fmt.Errorf("utxo %s: not found", key.String())
		}
	}
	return nil
}

// sessionLedgerValidator opens validation sessions on a
// laggingLedgerValidator. A session sees the ledger as it is when the session
// opens, and the ledger does not move while it is open.
type sessionLedgerValidator struct {
	*laggingLedgerValidator
	// onOpen, when set, runs once as the next session opens: blocks it
	// applies are part of that session's ledger.
	onOpen func()
	// failOpens is how many of the next sessions fail to open.
	failOpens int
}

var errTestSessionUnavailable = errors.New("test: session unavailable")

func (v *sessionLedgerValidator) WithTxValidationSession(
	_ context.Context,
	fn func(
		validate func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]lcommon.Utxo,
			*utxoref.StateOverlay,
		) error,
		stillCurrent func() bool,
		commitIfCurrent func(func() error) (bool, error),
	) error,
) error {
	v.mu.Lock()
	hook := v.onOpen
	v.onOpen = nil
	fail := v.failOpens > 0
	if fail {
		v.failOpens--
	}
	v.mu.Unlock()
	if fail {
		return errTestSessionUnavailable
	}
	if hook != nil {
		hook()
	}
	return fn(
		v.ValidateTxWithOverlay,
		func() bool { return true },
		func(commit func() error) (bool, error) { return true, commit() },
	)
}

type ledgerLagOptions struct {
	// sessions makes the validator open validation sessions.
	sessions        bool
	cleanupInterval time.Duration
}

type ledgerLagFixture struct {
	m *Mempool
	v *laggingLedgerValidator
	// s is the session validator when the fixture opens sessions.
	s      *sessionLedgerValidator
	parent gledger.Transaction
	// parentRaw, childRaw and siblingRaw are the CBOR of the parent and of
	// two transactions spending its first and second outputs.
	parentRaw  []byte
	childRaw   []byte
	siblingRaw []byte
	child      gledger.Transaction
	sibling    gledger.Transaction
	funding    indexTxInput
}

func (f *ledgerLagFixture) settledCount() int {
	f.m.RLock()
	defer f.m.RUnlock()
	return len(f.m.overlay.settled)
}

func (f *ledgerLagFixture) has(hash string) bool {
	_, ok := f.m.GetTransaction(hash)
	return ok
}

// newLedgerLagFixture pools a parent and a child spending its first output.
// The ledger tip is at slot 99 and holds only the parent's funding input.
func newLedgerLagFixture(
	t *testing.T,
	useDAG bool,
	currentSlot func() uint64,
) *ledgerLagFixture {
	t.Helper()
	return newLedgerLagFixtureWith(t, useDAG, currentSlot, ledgerLagOptions{})
}

func newLedgerLagFixtureWith(
	t *testing.T,
	useDAG bool,
	currentSlot func() uint64,
	opts ledgerLagOptions,
) *ledgerLagFixture {
	t.Helper()
	v := newLaggingLedgerValidator(99)
	var (
		validator TxValidator = v
		s         *sessionLedgerValidator
	)
	if opts.sessions {
		s = &sessionLedgerValidator{laggingLedgerValidator: v}
		validator = s
	}
	cfg := MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validator,
		MempoolCapacity: 1024 * 1024,
		CurrentSlotFunc: currentSlot,
		CleanupInterval: opts.cleanupInterval,
	}
	var (
		m   *Mempool
		err error
	)
	if useDAG {
		var pool *DAG
		pool, err = NewDAG(cfg)
		if pool != nil {
			m = pool.Mempool
		}
	} else {
		m, err = NewMempool(cfg)
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.Stop(context.Background()))
		m.eventBus.Close()
	})

	f := &ledgerLagFixture{
		m:       m,
		v:       v,
		s:       s,
		funding: indexTxInput{hash: [32]byte{0xf1}},
	}
	v.fund(f.funding)
	f.parentRaw, f.parent = buildIndexTx(t, indexTxSpec{
		inputs: []indexTxInput{f.funding},
		outputs: []lcommon.Address{
			indexTestAddress(t, 1), indexTestAddress(t, 2),
		},
	})
	var parentId [32]byte
	copy(parentId[:], f.parent.Hash().Bytes())
	f.childRaw, f.child = buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{{hash: parentId, index: 0}},
		outputs: []lcommon.Address{indexTestAddress(t, 3)},
	})
	f.siblingRaw, f.sibling = buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{{hash: parentId, index: 1}},
		outputs: []lcommon.Address{indexTestAddress(t, 4)},
	})
	ctx := context.Background()
	era := uint(conway.EraIdConway)
	require.NoError(t, m.AddTransaction(ctx, era, f.parentRaw))
	require.NoError(t, m.AddTransaction(ctx, era, f.childRaw))
	require.True(t, f.has(f.parent.Hash().String()))
	require.True(t, f.has(f.child.Hash().String()))
	return f
}

// The chain store announces a block before the ledger applies it. Pool
// maintenance in that gap must not judge the descendants of the transactions
// the block confirmed against a ledger that does not yet hold their inputs.
func TestChainUpdateKeepsDescendantsUntilLedgerAppliesConfirmingBlock(
	t *testing.T,
) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixture(t, useDAG, nil)
		ctx := context.Background()
		era := uint(conway.EraIdConway)
		parentHash := f.parent.Hash().String()
		childHash := f.child.Hash().String()
		block := blockCarryingTxs(t, 100, f.parentRaw)

		// Block 100 confirms the parent; the ledger is still at slot 99.
		f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.False(t, f.has(parentHash), "confirmed parent is evicted")
		require.True(
			t, f.has(childHash),
			"child of a confirmed parent survives revalidation while the "+
				"ledger has not applied the confirming block",
		)

		// Admission in the same gap: a new child of the confirmed parent is
		// valid on the chain the block extends.
		require.NoError(
			t, f.m.AddTransaction(ctx, era, f.siblingRaw),
			"child of a confirmed parent is admitted before the ledger "+
				"applies the confirming block",
		)

		// The ledger applies the block. Both children stay valid.
		f.v.applyBlock(100, f.parent)
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.True(t, f.has(childHash))
		require.True(t, f.has(f.sibling.Hash().String()))
		require.Len(t, f.m.Transactions(), 2)
		require.Zero(t, f.settledCount(), "applied block releases settled")
	})
}

// A confirmed transaction offered again before the ledger has applied its
// block spends inputs the block already spent, so it is refused rather than
// pooled a second time.
func TestAdmissionRefusesConfirmedTransactionBeforeLedgerApplies(
	t *testing.T,
) {
	t.Parallel()
	f := newLedgerLagFixture(t, false, nil)
	ctx := context.Background()
	f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTxs(t, 100, f.parentRaw),
	}))
	require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
	require.Error(t, f.m.AddTransaction(
		ctx, uint(conway.EraIdConway), f.parentRaw,
	))
	require.False(t, f.has(f.parent.Hash().String()))
}

// A pooled parent whose input a block spent in another transaction is
// invalid, and so is its child, once the ledger applies that block.
func TestChainUpdateDropsDescendantOfDoubleSpentParent(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixture(t, useDAG, nil)
		ctx := context.Background()
		_, rival := buildIndexTx(t, indexTxSpec{
			inputs:  []indexTxInput{f.funding},
			outputs: []lcommon.Address{indexTestAddress(t, 9)},
		})
		rivalRaw := rival.Cbor()
		block := blockCarryingTxs(t, 100, rivalRaw)

		f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		f.v.applyBlock(100, rival)
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))

		require.False(t, f.has(f.parent.Hash().String()))
		require.False(t, f.has(f.child.Hash().String()))
		require.Empty(t, f.m.Transactions())
	})
}

// A confirming block rolled back before the ledger applies it confirms
// nothing: its transaction returns to the pool.
func TestRolledBackConfirmationRestoresParentBeforeLedgerApplies(
	t *testing.T,
) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixture(t, useDAG, nil)
		ctx := context.Background()
		block := blockCarryingTxs(t, 100, f.parentRaw)
		block.Hash = []byte{0xb1}

		f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{Block: block}))
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.False(t, f.has(f.parent.Hash().String()))

		f.m.noteChainUpdate(chainUpdate(chain.ChainRollbackEvent{
			RolledBackBlocks: []models.Block{block},
		}))
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.True(
			t, f.has(f.parent.Hash().String()),
			"a transaction whose confirming block rolled back is pooled again",
		)
	})
}

// A locally forged block is removed from the pool through RemoveTxsByHash
// right after the chain store adopts it, also before the ledger applies it.
func TestForgedBlockRemovalKeepsDescendantsUntilLedgerApplies(t *testing.T) {
	t.Parallel()
	f := newLedgerLagFixture(t, false, func() uint64 { return 100 })
	ctx := context.Background()
	f.m.RemoveTxsByHash([]string{f.parent.Hash().String()})
	require.NoError(t, f.m.rebuildOverlay(ctx))
	require.True(t, f.has(f.child.Hash().String()))
	f.v.applyBlock(100, f.parent)
	require.NoError(t, f.m.rebuildOverlay(ctx))
	require.True(t, f.has(f.child.Hash().String()))
}

// A ledger that never applies the confirming block, as after a rollback the
// mempool missed, does not keep the confirmed outputs resolvable for longer
// than settledRetention.
func TestSettledTransactionsReleasedWhenLedgerNeverApplies(t *testing.T) {
	t.Parallel()
	f := newLedgerLagFixture(t, false, nil)
	ctx := context.Background()
	f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTxs(t, 100, f.parentRaw),
	}))
	require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
	require.True(t, f.has(f.child.Hash().String()))
	require.Equal(t, 1, f.settledCount())

	f.m.Lock()
	f.m.overlay.settled[0].since = time.Now().Add(-settledRetention - time.Second)
	f.m.Unlock()
	f.m.releaseSettled()
	require.Zero(t, f.settledCount())
	require.NoError(t, f.m.rebuildOverlay(ctx))
	require.False(
		t, f.has(f.child.Hash().String()),
		"a child of a parent the ledger never applied is dropped once the "+
			"parent is released",
	)
}

// A block the ledger has already applied when the mempool hears of it
// settles nothing.
func TestConfirmationAfterLedgerApplySettlesNothing(t *testing.T) {
	t.Parallel()
	f := newLedgerLagFixture(t, false, nil)
	f.v.applyBlock(100, f.parent)
	f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTxs(t, 100, f.parentRaw),
	}))
	require.NoError(t, f.m.rebuildAfterChainUpdate(context.Background()))
	require.Zero(t, f.settledCount())
	require.True(t, f.has(f.child.Hash().String()))
}

// Settled transactions are released once the ledger's applied tip reaches
// their block or they outlive settledRetention, and are bounded by a byte
// budget, oldest first.
func TestSettledChangeReleasesAppliedExpiredAndOverBudget(t *testing.T) {
	t.Parallel()
	now := time.Now()
	entry := func(hash string, slot uint64, since time.Time) settledTx {
		return settledTx{
			at:    appliedTx{hash: hash},
			slot:  slot,
			since: since,
			bytes: 10,
		}
	}
	settledHashes := func(o *utxoOverlay) []string {
		hashes := make([]string, 0, len(o.settled))
		for _, st := range o.settled {
			hashes = append(hashes, st.at.hash)
		}
		return hashes
	}
	o := newUtxoOverlay()
	o.updateSettled(nil, []settledTx{
		entry("expired", 300, now.Add(-settledRetention-time.Second)),
		entry("oldest", 300, now.Add(-2*time.Second)),
		entry("newest", 300, now.Add(-time.Second)),
		entry("applied", 200, now),
	}, nil)
	o.applied = []appliedTx{{hash: "fresh"}, {hash: "behind"}}

	// Within budget: only the applied and the expired entries go, and only
	// the pool transaction whose block is ahead of the tip settles.
	settled, unsettled := o.settledChange(
		map[string]uint64{"fresh": 301, "behind": 150},
		map[string]int64{"fresh": 10, "behind": 10},
		nil, 200, now, 30,
	)
	require.Len(t, settled, 1)
	require.Equal(t, "fresh", settled[0].at.hash)
	require.Equal(t, map[string]struct{}{
		"applied": {}, "expired": {},
	}, unsettled)
	o.updateSettled(
		map[string]struct{}{"fresh": {}, "behind": {}}, settled, unsettled,
	)
	require.Equal(t, []string{"oldest", "newest", "fresh"}, settledHashes(o))
	require.Equal(t, int64(30), o.settledBytes)

	// Over budget: the oldest goes first.
	settled, unsettled = o.settledChange(nil, nil, nil, 200, now, 20)
	require.Empty(t, settled)
	require.Equal(t, map[string]struct{}{"oldest": {}}, unsettled)
	o.updateSettled(nil, nil, unsettled)
	require.Equal(t, []string{"newest", "fresh"}, settledHashes(o))
	require.Equal(t, int64(20), o.settledBytes)
}

// A block confirming a pool transaction while a revalidation is in flight is
// replayed onto the candidate pool, settled outputs included, so the
// published pool keeps the confirmed parent's outputs for its child.
func TestConfirmationDuringRevalidationKeepsSettledOutputs(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixture(t, useDAG, func() uint64 { return 100 })
		ctx := context.Background()
		f.v.mu.Lock()
		f.v.onValidate = func() {
			f.m.RemoveTxsByHash([]string{f.parent.Hash().String()})
		}
		f.v.mu.Unlock()
		require.NoError(t, f.m.rebuildOverlay(ctx))
		require.False(t, f.has(f.parent.Hash().String()))
		require.True(t, f.has(f.child.Hash().String()))
		require.Equal(t, 1, f.settledCount())

		// The next revalidation, ledger still behind, keeps the child.
		require.NoError(t, f.m.rebuildOverlay(ctx))
		require.True(t, f.has(f.child.Hash().String()))
	})
}

// spendParentOutput returns a transaction outside the pool that spends the
// parent's output at index.
func (f *ledgerLagFixture) spendParentOutput(
	t *testing.T,
	index uint32,
) gledger.Transaction {
	t.Helper()
	var parentId [32]byte
	copy(parentId[:], f.parent.Hash().Bytes())
	_, tx := buildIndexTx(t, indexTxSpec{
		inputs:  []indexTxInput{{hash: parentId, index: index}},
		outputs: []lcommon.Address{indexTestAddress(t, 9)},
	})
	return tx
}

// ageSettled backdates every settled transaction past settledRetention.
func (f *ledgerLagFixture) ageSettled() {
	f.m.Lock()
	defer f.m.Unlock()
	for i := range f.m.overlay.settled {
		f.m.overlay.settled[i].since = time.Now().Add(
			-settledRetention - time.Second,
		)
	}
}

// The ledger can apply the confirming block, and a later block spending the
// confirmed parent's output outside the pool, after a revalidation has read
// the applied tip but before its validation session opens. The parent's
// settled outputs must not shadow that spend in the session.
func TestRevalidationDropsChildOfSettledOutputSpentBeforeSessionOpens(
	t *testing.T,
) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixtureWith(
			t, useDAG, nil, ledgerLagOptions{sessions: true},
		)
		ctx := context.Background()
		f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
			Block: blockCarryingTxs(t, 100, f.parentRaw),
		}))
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.True(t, f.has(f.child.Hash().String()))
		require.Equal(t, 1, f.settledCount())

		rival := f.spendParentOutput(t, 0)
		f.s.mu.Lock()
		f.s.onOpen = func() {
			f.v.applyBlock(100, f.parent)
			f.v.applyBlock(101, rival)
		}
		f.s.mu.Unlock()
		require.NoError(t, f.m.rebuildOverlay(ctx))
		require.False(
			t, f.has(f.child.Hash().String()),
			"a child spending an output the session's ledger has spent is "+
				"dropped",
		)
		require.Zero(t, f.settledCount())
	})
}

// Admission after the ledger has applied the confirming block, and a later
// block spending the confirmed parent's output outside the pool, judges the
// output by the ledger, not by the parent's settled outputs.
func TestAdmissionRefusesSettledOutputTheLedgerSpent(t *testing.T) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixture(t, useDAG, nil)
		ctx := context.Background()
		f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
			Block: blockCarryingTxs(t, 100, f.parentRaw),
		}))
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.Equal(t, 1, f.settledCount())

		f.v.applyBlock(100, f.parent)
		f.v.applyBlock(101, f.spendParentOutput(t, 1))
		require.Error(t, f.m.AddTransaction(
			ctx, uint(conway.EraIdConway), f.siblingRaw,
		))
		require.False(t, f.has(f.sibling.Hash().String()))
		require.Zero(t, f.settledCount())
	})
}

// A settled parent released while a revalidation is in flight, here past
// settledRetention with the ledger never applying its block, takes its
// outputs out of the candidate pool too. The child validated against them
// is judged again rather than published.
func TestSettledReleaseDuringRevalidationRevalidatesDescendants(
	t *testing.T,
) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixture(t, useDAG, nil)
		ctx := context.Background()
		f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
			Block: blockCarryingTxs(t, 100, f.parentRaw),
		}))
		require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
		require.True(t, f.has(f.child.Hash().String()))
		require.Equal(t, 1, f.settledCount())

		f.v.mu.Lock()
		f.v.onValidate = func() {
			f.ageSettled()
			f.m.releaseSettled()
		}
		f.v.mu.Unlock()
		require.NoError(t, f.m.rebuildOverlay(ctx))
		require.Zero(t, f.settledCount())
		require.False(
			t, f.has(f.child.Hash().String()),
			"a child of a parent released during revalidation is judged "+
				"without the parent's outputs",
		)
	})
}

// The expiry ticker releasing a settled parent whose block the ledger never
// applied revalidates the pool, so the parent's child does not wait for the
// next chain update.
func TestExpiredSettledReleaseRevalidatesPool(t *testing.T) {
	t.Parallel()
	f := newLedgerLagFixtureWith(
		t, false, nil,
		ledgerLagOptions{cleanupInterval: 10 * time.Millisecond},
	)
	ctx := context.Background()
	f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTxs(t, 100, f.parentRaw),
	}))
	require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
	require.True(t, f.has(f.child.Hash().String()))
	f.ageSettled()

	require.NoError(t, f.m.Start(ctx))
	require.Eventually(t, func() bool {
		return !f.has(f.child.Hash().String())
	}, 5*time.Second, 10*time.Millisecond,
		"the child of a released parent the ledger never applied is dropped",
	)
	require.Zero(t, f.settledCount())
}

// pendingReferencer pools a transaction that spends a funded input of its own
// and takes the parent's second output as a reference input, then confirms
// the parent and the child in block 100 with the ledger behind. The
// referencing transaction is the only pool transaction left, and it depends
// on the settled parent only through its reference input.
func (f *ledgerLagFixture) pendingReferencer(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	funding := indexTxInput{hash: [32]byte{0xf2}}
	f.v.fund(funding)
	var parentId [32]byte
	copy(parentId[:], f.parent.Hash().Bytes())
	raw, tx := buildIndexTx(t, indexTxSpec{
		inputs:          []indexTxInput{funding},
		referenceInputs: []indexTxInput{{hash: parentId, index: 1}},
		outputs:         []lcommon.Address{indexTestAddress(t, 5)},
	})
	require.NoError(t, f.m.AddTransaction(ctx, uint(conway.EraIdConway), raw))
	f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTxs(t, 100, f.parentRaw, f.childRaw),
	}))
	require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
	hash := tx.Hash().String()
	require.True(t, f.has(hash), "referencer survives the ledger lag")
	require.Len(t, f.m.Transactions(), 1)
	require.Equal(t, 2, f.settledCount())
	return hash
}

// A transaction that only references a settled output is judged again when
// a revalidation in flight replays the release of that output.
func TestSettledReleaseDuringRevalidationRevalidatesReferencer(
	t *testing.T,
) {
	t.Parallel()
	forBothPools(t, func(t *testing.T, useDAG bool) {
		f := newLedgerLagFixture(t, useDAG, nil)
		hash := f.pendingReferencer(t)
		f.v.mu.Lock()
		f.v.onValidate = func() {
			f.ageSettled()
			f.m.releaseSettled()
		}
		f.v.mu.Unlock()
		require.NoError(t, f.m.rebuildOverlay(context.Background()))
		require.Zero(t, f.settledCount())
		require.False(
			t, f.has(hash),
			"a transaction referencing a released output is judged without it",
		)
	})
}

// The expiry ticker revalidates the pool when it releases an output that a
// pool transaction only references.
func TestExpiredSettledReleaseRevalidatesReferencer(t *testing.T) {
	t.Parallel()
	f := newLedgerLagFixtureWith(
		t, false, nil,
		ledgerLagOptions{cleanupInterval: 10 * time.Millisecond},
	)
	hash := f.pendingReferencer(t)
	f.ageSettled()
	require.NoError(t, f.m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return !f.has(hash)
	}, 5*time.Second, 10*time.Millisecond,
		"a transaction referencing a released output is dropped",
	)
}

// A rebuild the expiry ticker starts after a settled release, and that
// fails, is retried on a later tick: the release itself is not repeated, so
// it cannot be what triggers the retry.
func TestExpiredSettledReleaseRetriesFailedRebuild(t *testing.T) {
	t.Parallel()
	f := newLedgerLagFixtureWith(
		t, false, nil,
		ledgerLagOptions{
			sessions:        true,
			cleanupInterval: 10 * time.Millisecond,
		},
	)
	ctx := context.Background()
	f.m.noteChainUpdate(chainUpdate(chain.ChainBlockEvent{
		Block: blockCarryingTxs(t, 100, f.parentRaw),
	}))
	require.NoError(t, f.m.rebuildAfterChainUpdate(ctx))
	require.True(t, f.has(f.child.Hash().String()))
	f.ageSettled()
	f.s.mu.Lock()
	f.s.failOpens = 1
	f.s.mu.Unlock()

	require.NoError(t, f.m.Start(ctx))
	require.Eventually(t, func() bool {
		return !f.has(f.child.Hash().String())
	}, 5*time.Second, 10*time.Millisecond,
		"the rebuild that failed after the release is retried",
	)
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	require.Zero(t, f.s.failOpens, "the first rebuild did fail")
}
