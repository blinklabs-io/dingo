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
	return nil
}

type ledgerLagFixture struct {
	m      *Mempool
	v      *laggingLedgerValidator
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
	v := newLaggingLedgerValidator(99)
	cfg := MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       v,
		MempoolCapacity: 1024 * 1024,
		CurrentSlotFunc: currentSlot,
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
