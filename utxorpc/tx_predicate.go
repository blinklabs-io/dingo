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

// predOutcome is the result of evaluating a TxPredicate subtree against a tx.
// predUnevaluable means the implementation cannot decide (unsupported chain,
// malformed filter, etc.); it must not be negated as if it were predNoMatch.
type predOutcome int

const (
	predUnevaluable predOutcome = iota
	predNoMatch
	predMatch
)

// txPredicateNode is the Cardano evaluation shape for utxorpc TxPredicate
// (submit and watch protos share the same fields). Conversion from generated
// *TxPredicate types happens once at the API boundary; recursion uses only
// this struct.
type txPredicateNode struct {
	match *cardano.TxPattern
	// matchNonCardano is true when the protobuf had a Match field but no
	// Cardano TxPattern (unsupported chain). The subtree is unevaluable.
	matchNonCardano bool
	not             []*txPredicateNode
	allOf           []*txPredicateNode
	anyOf           []*txPredicateNode
}

type txPatternProto interface {
	GetCardano() *cardano.TxPattern
}

type txPredicateProto[T any, M txPatternProto] interface {
	GetMatch() M
	GetNot() []T
	GetAllOf() []T
	GetAnyOf() []T
}

func buildTxPredicateNodeFromProto[T txPredicateProto[T, M], M txPatternProto](
	p T,
	isNilPredicate func(T) bool,
	isNilMatch func(M) bool,
) *txPredicateNode {
	if isNilPredicate(p) {
		return nil
	}
	n := &txPredicateNode{}
	if m := p.GetMatch(); !isNilMatch(m) {
		if c := m.GetCardano(); c != nil {
			n.match = c
		} else {
			n.matchNonCardano = true
		}
	}
	for _, c := range p.GetNot() {
		if !isNilPredicate(c) {
			n.not = append(
				n.not,
				buildTxPredicateNodeFromProto(c, isNilPredicate, isNilMatch),
			)
		}
	}
	for _, c := range p.GetAllOf() {
		if !isNilPredicate(c) {
			n.allOf = append(
				n.allOf,
				buildTxPredicateNodeFromProto(c, isNilPredicate, isNilMatch),
			)
		}
	}
	for _, c := range p.GetAnyOf() {
		if !isNilPredicate(c) {
			n.anyOf = append(
				n.anyOf,
				buildTxPredicateNodeFromProto(c, isNilPredicate, isNilMatch),
			)
		}
	}
	return n
}

func isNilPtr[T any](p *T) bool {
	return p == nil
}

// txPredicateFromSubmit maps submit.TxPredicate into txPredicateNode.
func txPredicateFromSubmit(p *submit.TxPredicate) *txPredicateNode {
	return buildTxPredicateNodeFromProto(
		p,
		isNilPtr[submit.TxPredicate],
		isNilPtr[submit.AnyChainTxPattern],
	)
}

// txPredicateFromWatch maps watch.TxPredicate into txPredicateNode.
func txPredicateFromWatch(p *watch.TxPredicate) *txPredicateNode {
	return buildTxPredicateNodeFromProto(
		p,
		isNilPtr[watch.TxPredicate],
		isNilPtr[watch.AnyChainTxPattern],
	)
}

// txPatternLeaf matches a transaction against a Cardano TxPattern (outputs,
// inputs via ledger state, mint/move asset filters).
type txPatternLeaf func(tx gledger.Transaction, pat *cardano.TxPattern) predOutcome

// matchTxPredicateNode evaluates a prebuilt txPredicateNode (from
// txPredicateFromSubmit or txPredicateFromWatch). Callers that omit a
// protobuf predicate must not invoke this with a nil node; nil node
// yields predNoMatch. Unevaluable predicates do not match (no stream).
func (u *Utxorpc) matchTxPredicateNode(
	tx gledger.Transaction,
	node *txPredicateNode,
) bool {
	return evalTxPredicateOutcome(tx, node, u.matchesTxPattern) == predMatch
}

// andOutcome combines two conjuncts. A definite non-match dominates
// unevaluable: false ∧ unknown is false (so all_of can fail even when one
// branch cannot be evaluated).
func andOutcome(a, b predOutcome) predOutcome {
	if a == predNoMatch || b == predNoMatch {
		return predNoMatch
	}
	if a == predUnevaluable || b == predUnevaluable {
		return predUnevaluable
	}
	return predMatch
}

func orOutcome(a, b predOutcome) predOutcome {
	if a == predMatch || b == predMatch {
		return predMatch
	}
	if a == predUnevaluable || b == predUnevaluable {
		return predUnevaluable
	}
	return predNoMatch
}

func notOutcome(sub predOutcome) predOutcome {
	switch sub {
	case predMatch:
		return predNoMatch
	case predNoMatch:
		return predMatch
	case predUnevaluable:
		return predUnevaluable
	default:
		return predUnevaluable
	}
}

func combineNodeParts(parts []predOutcome) predOutcome {
	if len(parts) == 0 {
		return predNoMatch
	}
	out := predMatch
	for _, p := range parts {
		out = andOutcome(out, p)
	}
	return out
}

func combineANDBranches(parts []predOutcome) predOutcome {
	if len(parts) == 0 {
		return predMatch
	}
	out := predMatch
	for _, p := range parts {
		out = andOutcome(out, p)
	}
	return out
}

func combineORBranches(parts []predOutcome) predOutcome {
	if len(parts) == 0 {
		return predNoMatch
	}
	out := predNoMatch
	for _, p := range parts {
		out = orOutcome(out, p)
	}
	return out
}

// evalTxPredicateOutcome recursively applies match, not, all_of, and any_of.
// predUnevaluable propagates through composites; not() only negates definite
// match vs non-match.
func evalTxPredicateOutcome(
	tx gledger.Transaction,
	p *txPredicateNode,
	leaf txPatternLeaf,
) predOutcome {
	if p == nil {
		return predNoMatch
	}
	parts := make([]predOutcome, 0, 4)
	if p.matchNonCardano {
		parts = append(parts, predUnevaluable)
	} else if p.match != nil {
		parts = append(parts, leaf(tx, p.match))
	}
	for _, c := range p.not {
		parts = append(parts, notOutcome(evalTxPredicateOutcome(tx, c, leaf)))
	}
	if len(p.allOf) > 0 {
		sub := make([]predOutcome, 0, len(p.allOf))
		for _, c := range p.allOf {
			sub = append(sub, evalTxPredicateOutcome(tx, c, leaf))
		}
		parts = append(parts, combineANDBranches(sub))
	}
	if len(p.anyOf) > 0 {
		sub := make([]predOutcome, 0, len(p.anyOf))
		for _, c := range p.anyOf {
			sub = append(sub, evalTxPredicateOutcome(tx, c, leaf))
		}
		parts = append(parts, combineORBranches(sub))
	}
	return combineNodeParts(parts)
}
