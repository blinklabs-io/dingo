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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// StateOverlay holds, in order, the transactions that are pending or already
// selected for a block and that change ledger state beyond the UTxO set:
// reward withdrawals, certificates, direct deposits and governance
// proposals. View folds them into a lcommon.BlockLedgerState, so a later
// transaction is validated against the state its predecessors leave.
//
// It stores the transactions rather than a folded state because the mempool
// removes and evicts arbitrary members and rebuilds from the survivors, and a
// BlockLedgerState cannot undo a transaction. A transaction that changes only
// UTxOs is not stored; the consumed and created maps carry it.
//
// A nil overlay holds no transactions.
type StateOverlay struct {
	txs []lcommon.Transaction
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
// state beyond the UTxO set is ignored.
func (o *StateOverlay) Apply(tx lcommon.Transaction) {
	if ChangesState(tx) {
		o.txs = append(o.txs, tx)
	}
}

// Len returns the number of recorded transactions.
func (o *StateOverlay) Len() int {
	if o == nil {
		return 0
	}
	return len(o.txs)
}

// View returns base with the recorded transactions applied. With nothing
// recorded it returns base unchanged. pp supplies the key deposit that a
// pre-Conway stake registration certificate records.
func (o *StateOverlay) View(
	base lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (lcommon.LedgerState, error) {
	if o == nil || len(o.txs) == 0 {
		return base, nil
	}
	view := lcommon.NewBlockLedgerState(base)
	for _, tx := range o.txs {
		if err := view.ApplyTransaction(tx, pp); err != nil {
			return nil, fmt.Errorf(
				"apply pending transaction %s: %w",
				tx.Hash(),
				err,
			)
		}
	}
	return view, nil
}
