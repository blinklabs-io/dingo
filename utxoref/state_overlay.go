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

package utxoref

import (
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// StateOverlay holds, in order, the transactions that are pending or already
// selected for a block and that change ledger state beyond the UTxO set:
// reward withdrawals, certificates, direct deposits and governance
// proposals. View folds them into a lcommon.BlockLedgerState, so a later
// transaction is validated against the state its predecessors leave.
//
// The fold is incremental: View applies only the transactions recorded since
// the previous View, so a pool of k transactions costs k applications in
// total rather than k per validation. The folded state caches what it read
// from the base, so it is keyed to the base's generation, and a View at a
// different generation discards it and folds again from the first transaction.
//
// A refold keeps the recorded transactions and so cannot tell whether the new
// base already contains some of them. Moved reports that this happened, and a
// holder whose transactions can be confirmed must then reconcile its
// transactions against the new base rather than trust the View. An overlay
// that holds only transactions yet to be applied to any base, such as the one
// a block builder fills, can ignore it.
// A transaction recorded with ApplyEncoded is held only as the caller's CBOR
// slice and is decoded when folded, so the overlay retains no decoded
// transaction and adds no bytes the caller does not already count.
//
// The folded state does not model UTxOs: the mempool's consumed and created
// maps already cover every pending transaction, and a second copy that
// answered UtxoById would let an output a stored transaction created resolve
// after an unstored one spent it. A transaction that changes only UTxOs is
// not recorded.
//
// An overlay cannot drop a transaction; the mempool builds a new overlay from
// the survivors instead. View must not run concurrently with another View on
// the same overlay, and the returned state is valid only until the next View.
//
// A nil overlay holds no transactions.
type StateOverlay struct {
	mu      sync.Mutex
	entries []stateEntry
	// folded counts the entries applied to state.
	folded int
	state  *lcommon.BlockLedgerState
	base   *swappableState
	// gen is the base generation the entries were recorded under, once a View
	// has observed one.
	gen      uint64
	genKnown bool
	// moved is set when a View found the base generation different from gen.
	moved bool
}

type stateEntry struct {
	txType uint
	cbor   []byte
	// tx is set only for entries recorded decoded, which are short-lived
	// block-building overlays.
	tx lcommon.Transaction
}

// NewStateOverlay returns an empty overlay.
func NewStateOverlay() *StateOverlay {
	return &StateOverlay{}
}

// ChangesState reports whether tx changes ledger state that the UTxO
// overlay does not carry. A phase-2-invalid transaction does not: only its
// collateral changes, and that is a UTxO effect.
func ChangesState(tx lcommon.Transaction) bool {
	if tx == nil || !tx.IsValid() {
		return false
	}
	bodies := []lcommon.TransactionBody{tx}
	if leveled, ok := tx.(lcommon.LeveledTransaction); ok {
		levels, err := leveled.LedgerEffectLevels()
		if err != nil {
			// Unreadable levels cannot be proven inert.
			return true
		}
		bodies = bodies[:0]
		for _, level := range levels {
			if len(level.DirectDeposits) > 0 {
				return true
			}
			bodies = append(bodies, level.Body)
		}
	}
	for _, body := range bodies {
		if len(body.Withdrawals()) > 0 ||
			len(body.Certificates()) > 0 ||
			len(body.ProposalProcedures()) > 0 {
			return true
		}
	}
	return false
}

// Apply records tx after it is accepted. A transaction that changes no
// state beyond the UTxO set is ignored. The overlay keeps the decoded
// transaction, so use ApplyEncoded for anything long-lived.
func (o *StateOverlay) Apply(tx lcommon.Transaction) {
	if !ChangesState(tx) {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.entries = append(o.entries, stateEntry{tx: tx})
}

// ApplyEncoded records a transaction for which ChangesState is true, given
// as the CBOR the caller already retains. The overlay keeps the slice itself,
// not a copy, so the caller must not modify it and must not count it twice.
func (o *StateOverlay) ApplyEncoded(txType uint, cbor []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.entries = append(o.entries, stateEntry{txType: txType, cbor: cbor})
}

// Len returns the number of recorded transactions.
func (o *StateOverlay) Len() int {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries)
}

// Empty returns an overlay with no transactions that carries this overlay's
// base generation and Moved state, so a pool rebuilt from survivors does not
// lose track of the base its survivors were validated against.
func (o *StateOverlay) Empty() *StateOverlay {
	if o == nil {
		return NewStateOverlay()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return &StateOverlay{gen: o.gen, genKnown: o.genKnown, moved: o.moved}
}

// Moved reports whether a View ran at a base generation other than the one the
// recorded transactions were validated under. The flag does not clear.
func (o *StateOverlay) Moved() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.moved
}

// View returns base with the recorded transactions applied. With nothing
// recorded it returns base unchanged. pp supplies the key deposit that a
// pre-Conway stake registration certificate records. generation identifies the
// published ledger state base reads, and must differ from every earlier
// generation of the same ledger, because the folded state caches reads from
// base and is refolded whenever generation changes.
//
// A failure to decode or apply a recorded transaction fails the View, and
// the next View refolds from the first transaction.
func (o *StateOverlay) View(
	base lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
	generation uint64,
) (lcommon.LedgerState, error) {
	if o == nil {
		return base, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.entries) == 0 {
		o.gen, o.genKnown = generation, true
		return base, nil
	}
	if !o.genKnown {
		o.gen, o.genKnown = generation, true
	} else if o.gen != generation {
		o.gen = generation
		o.moved = true
		o.state = nil
	}
	if o.state == nil {
		o.base = &swappableState{}
		o.state = lcommon.NewBlockLedgerState(o.base)
		o.folded = 0
	}
	o.base.LedgerState = base
	for o.folded < len(o.entries) {
		entry := o.entries[o.folded]
		tx := entry.tx
		if tx == nil {
			var err error
			tx, err = safedecode.Transaction(entry.txType, entry.cbor)
			if err != nil {
				o.state = nil
				return nil, fmt.Errorf("decode pending transaction: %w", err)
			}
		}
		if err := o.state.ApplyTransaction(effectsOnly(tx), pp); err != nil {
			// A failed application can leave the transaction half applied.
			o.state = nil
			return nil, fmt.Errorf(
				"apply pending transaction %s: %w",
				tx.Hash(),
				err,
			)
		}
		o.folded++
	}
	return o.state, nil
}

// swappableState is the base of the folded state. The overlay outlives the
// LedgerState it validates against, so each View points it at the current
// one. UnwrapLedgerState lets capability lookups reach that state.
type swappableState struct {
	lcommon.LedgerState
}

func (s *swappableState) UnwrapLedgerState() lcommon.LedgerState {
	return s.LedgerState
}

// effectsOnlyTx hides a transaction's UTxO effects from BlockLedgerState, so
// that UtxoById resolves only through the base and the caller's own maps.
type effectsOnlyTx struct {
	lcommon.Transaction
}

func (effectsOnlyTx) Consumed() []lcommon.TransactionInput { return nil }

func (effectsOnlyTx) Produced() []lcommon.Utxo { return nil }

// leveledEffectsOnlyTx keeps the staged effects of a LeveledTransaction.
type leveledEffectsOnlyTx struct {
	effectsOnlyTx
	leveled lcommon.LeveledTransaction
}

func (t leveledEffectsOnlyTx) LedgerEffectLevels() (
	[]lcommon.LedgerEffectLevel,
	error,
) {
	return t.leveled.LedgerEffectLevels()
}

func effectsOnly(tx lcommon.Transaction) lcommon.Transaction {
	stripped := effectsOnlyTx{Transaction: tx}
	if leveled, ok := tx.(lcommon.LeveledTransaction); ok {
		return leveledEffectsOnlyTx{effectsOnlyTx: stripped, leveled: leveled}
	}
	return stripped
}
