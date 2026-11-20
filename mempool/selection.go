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
	"container/heap"
	"math/big"

	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// SelectionPolicy is the order in which block production is offered pending
// transactions. It does not change admission, eviction, relay, or the order
// Transactions reports.
type SelectionPolicy string

const (
	// SelectionAdmission offers transactions in admission order. It is the
	// default, and the empty policy means the same.
	SelectionAdmission SelectionPolicy = "admission"
	// SelectionFeeDensity offers transactions paying the most fee per
	// serialized byte first, never ahead of a pending parent.
	SelectionFeeDensity SelectionPolicy = "feeDensity"
)

// Valid reports whether the policy is part of the stable config surface.
func (p SelectionPolicy) Valid() bool {
	switch p {
	case "", SelectionAdmission, SelectionFeeDensity:
		return true
	default:
		return false
	}
}

// BlockCandidates returns a snapshot of the pending transactions in the order
// the configured selection policy offers them to block production.
func (m *Mempool) BlockCandidates() []MempoolTransaction {
	txs := m.Transactions()
	if m.config.Selection != SelectionFeeDensity {
		return txs
	}
	return OrderByFeeDensity(txs)
}

// OrderByFeeDensity reorders txs, given in admission order, so that among the
// transactions whose pending parents are already placed, the one paying the
// most fee per CBOR byte comes next. Equal densities keep admission order.
//
// A transaction that spends, references, or pledges as collateral an output of
// another transaction in txs is never placed before it. The block builder
// validates each transaction against the outputs of those selected before it,
// so a child ahead of its parent would be dropped from the block. The
// consequence is that a high-fee child waits for a low-fee parent; parents are
// not promoted for the fees of their children.
//
// A transaction that cannot be decoded ranks as paying no fee and depending on
// nothing. The block builder skips it either way.
func OrderByFeeDensity(txs []MempoolTransaction) []MempoolTransaction {
	if len(txs) < 2 {
		return txs
	}
	candidates := make([]feeDensityCandidate, len(txs))
	producer := make(map[utxoref.Key]int)
	decoded := make([]gledger.Transaction, len(txs))
	for i, tx := range txs {
		candidates[i] = feeDensityCandidate{
			index: i,
			fee:   new(big.Int),
			size:  big.NewInt(int64(max(len(tx.Cbor), 1))),
		}
		ledgerTx, err := gledger.NewTransactionFromCbor(tx.Type, tx.Cbor)
		if err != nil {
			continue
		}
		decoded[i] = ledgerTx
		if fee := ledgerTx.Fee(); fee != nil && fee.Sign() > 0 {
			candidates[i].fee.Set(fee)
		}
		for _, produced := range ledgerTx.Produced() {
			producer[utxoref.ForInput(produced.Id)] = i
		}
	}

	children := make([][]int, len(txs))
	pendingParents := make([]int, len(txs))
	for i, ledgerTx := range decoded {
		if ledgerTx == nil {
			continue
		}
		parents := make(map[int]struct{})
		for _, inputs := range [][]lcommon.TransactionInput{
			ledgerTx.Inputs(),
			ledgerTx.ReferenceInputs(),
			ledgerTx.Collateral(),
		} {
			for _, input := range inputs {
				parent, ok := producer[utxoref.ForInput(input)]
				if !ok || parent == i {
					continue
				}
				if _, seen := parents[parent]; seen {
					continue
				}
				parents[parent] = struct{}{}
				children[parent] = append(children[parent], i)
				pendingParents[i]++
			}
		}
	}

	ready := &feeDensityHeap{candidates: candidates}
	for i := range txs {
		if pendingParents[i] == 0 {
			heap.Push(ready, i)
		}
	}
	ordered := make([]MempoolTransaction, 0, len(txs))
	placed := make([]bool, len(txs))
	for ready.Len() > 0 {
		i := heap.Pop(ready).(int)
		ordered = append(ordered, txs[i])
		placed[i] = true
		for _, child := range children[i] {
			pendingParents[child]--
			if pendingParents[child] == 0 {
				heap.Push(ready, child)
			}
		}
	}
	// Transaction hashes make a dependency cycle impossible, but a
	// transaction must never be lost from the snapshot if one appeared.
	for i, tx := range txs {
		if !placed[i] {
			ordered = append(ordered, tx)
		}
	}
	return ordered
}

type feeDensityCandidate struct {
	fee   *big.Int
	size  *big.Int
	index int
}

// feeDensityHeap is a max-heap of candidate indexes by fee per byte. It
// compares fee_a*size_b with fee_b*size_a, so no precision is lost to
// division.
type feeDensityHeap struct {
	candidates []feeDensityCandidate
	items      []int
}

func (h *feeDensityHeap) Len() int { return len(h.items) }

func (h *feeDensityHeap) Less(i, j int) bool {
	a := h.candidates[h.items[i]]
	b := h.candidates[h.items[j]]
	left := new(big.Int).Mul(a.fee, b.size)
	right := new(big.Int).Mul(b.fee, a.size)
	if cmp := left.Cmp(right); cmp != 0 {
		return cmp > 0
	}
	return a.index < b.index
}

func (h *feeDensityHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *feeDensityHeap) Push(x any) {
	h.items = append(h.items, x.(int))
}

func (h *feeDensityHeap) Pop() any {
	last := len(h.items) - 1
	item := h.items[last]
	h.items = h.items[:last]
	return item
}
