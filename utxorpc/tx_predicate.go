// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	submit "github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
)

// txPredicateNode is the Cardano evaluation shape for utxorpc TxPredicate
// (submit and watch protos share the same fields). Conversion from generated
// *TxPredicate types happens once at the API boundary; recursion uses only
// this struct.
type txPredicateNode struct {
	match *cardano.TxPattern
	not   []*txPredicateNode
	allOf []*txPredicateNode
	anyOf []*txPredicateNode
}

// txPredicateFromSubmit maps submit.TxPredicate into txPredicateNode.
func txPredicateFromSubmit(p *submit.TxPredicate) *txPredicateNode {
	if p == nil {
		return nil
	}
	n := &txPredicateNode{}
	if m := p.GetMatch(); m != nil {
		n.match = m.GetCardano()
	}
	for _, c := range p.GetNot() {
		if c != nil {
			n.not = append(n.not, txPredicateFromSubmit(c))
		}
	}
	for _, c := range p.GetAllOf() {
		if c != nil {
			n.allOf = append(n.allOf, txPredicateFromSubmit(c))
		}
	}
	for _, c := range p.GetAnyOf() {
		if c != nil {
			n.anyOf = append(n.anyOf, txPredicateFromSubmit(c))
		}
	}
	return n
}

// txPredicateFromWatch maps watch.TxPredicate into txPredicateNode.
func txPredicateFromWatch(p *watch.TxPredicate) *txPredicateNode {
	if p == nil {
		return nil
	}
	n := &txPredicateNode{}
	if m := p.GetMatch(); m != nil {
		n.match = m.GetCardano()
	}
	for _, c := range p.GetNot() {
		if c != nil {
			n.not = append(n.not, txPredicateFromWatch(c))
		}
	}
	for _, c := range p.GetAllOf() {
		if c != nil {
			n.allOf = append(n.allOf, txPredicateFromWatch(c))
		}
	}
	for _, c := range p.GetAnyOf() {
		if c != nil {
			n.anyOf = append(n.anyOf, txPredicateFromWatch(c))
		}
	}
	return n
}

// txPatternLeaf matches a transaction against a Cardano TxPattern (outputs,
// inputs via ledger state, mint/move asset filters).
type txPatternLeaf func(tx gledger.Transaction, pat *cardano.TxPattern) bool

// matchesSubmitTxPredicate evaluates a submit.TxPredicate for WatchMempool.
// A nil predicate matches all transactions.
func (u *Utxorpc) matchesSubmitTxPredicate(
	tx gledger.Transaction,
	p *submit.TxPredicate,
) bool {
	if p == nil {
		return true
	}
	return evalTxPredicate(tx, txPredicateFromSubmit(p), u.matchesTxPattern)
}

// matchesWatchTxPredicate evaluates a watch.TxPredicate for WatchTx.
// A nil predicate matches all transactions.
func (u *Utxorpc) matchesWatchTxPredicate(
	tx gledger.Transaction,
	p *watch.TxPredicate,
) bool {
	if p == nil {
		return true
	}
	return evalTxPredicate(tx, txPredicateFromWatch(p), u.matchesTxPattern)
}

// evalTxPredicate recursively applies match, not, all_of, and any_of per
// utxorpc TxPredicate:
//   - match: leaf match on Cardano TxPattern
//   - not: for each child c, require ¬eval(c); multiple entries are ANDed
//   - all_of: every child must match; empty all_of adds no constraint
//   - any_of: at least one child must match; empty any_of cannot be satisfied
//
// If several of these are set on the same node, results are AND-combined.
// A node with no constraints (no match, no not children, no non-empty
// all_of/any_of) does not match.
func evalTxPredicate(
	tx gledger.Transaction,
	p *txPredicateNode,
	leaf txPatternLeaf,
) bool {
	if p == nil {
		return false
	}
	var active bool
	ok := true
	if p.match != nil {
		active = true
		ok = leaf(tx, p.match)
	}
	for _, c := range p.not {
		active = true
		ok = ok && !evalTxPredicate(tx, c, leaf)
	}
	if len(p.allOf) > 0 {
		active = true
		allOk := true
		for _, c := range p.allOf {
			if !evalTxPredicate(tx, c, leaf) {
				allOk = false
				break
			}
		}
		ok = ok && allOk
	}
	if len(p.anyOf) > 0 {
		active = true
		anyOk := false
		for _, c := range p.anyOf {
			if evalTxPredicate(tx, c, leaf) {
				anyOk = true
				break
			}
		}
		ok = ok && anyOk
	}
	if !active {
		return false
	}
	return ok
}
