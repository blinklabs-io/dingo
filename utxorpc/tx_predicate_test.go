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
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	submit "github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
)

// stubLeaf returns true only when the pattern pointer equals pMatch.
func stubLeaf(pMatch *cardano.TxPattern) txPatternLeaf {
	return func(_ gledger.Transaction, p *cardano.TxPattern) bool {
		return p == pMatch
	}
}

func matchSubmit(p *cardano.TxPattern) *submit.TxPredicate {
	return &submit.TxPredicate{
		Match: &submit.AnyChainTxPattern{
			Chain: &submit.AnyChainTxPattern_Cardano{Cardano: p},
		},
	}
}

func matchWatch(p *cardano.TxPattern) *watch.TxPredicate {
	return &watch.TxPredicate{
		Match: &watch.AnyChainTxPattern{
			Chain: &watch.AnyChainTxPattern_Cardano{Cardano: p},
		},
	}
}

func evalSubmit(
	tx gledger.Transaction,
	p *submit.TxPredicate,
	leaf txPatternLeaf,
) bool {
	return evalTxPredicate(tx, txPredicateFromSubmit(p), leaf)
}

func TestEvalTxPredicate_Match(t *testing.T) {
	pA := &cardano.TxPattern{}
	pB := &cardano.TxPattern{}
	leaf := stubLeaf(pA)

	require.True(t, evalSubmit(nil, matchSubmit(pA), leaf))
	require.False(t, evalSubmit(nil, matchSubmit(pB), leaf))
}

func TestEvalTxPredicate_Not(t *testing.T) {
	pA := &cardano.TxPattern{}
	pB := &cardano.TxPattern{}
	leaf := stubLeaf(pA)

	t.Run("single_inverts", func(t *testing.T) {
		pred := &submit.TxPredicate{Not: []*submit.TxPredicate{matchSubmit(pA)}}
		require.False(t, evalSubmit(nil, pred, leaf))

		pred2 := &submit.TxPredicate{Not: []*submit.TxPredicate{matchSubmit(pB)}}
		require.True(t, evalSubmit(nil, pred2, leaf))
	})

	t.Run("multiple_not_and_combined", func(t *testing.T) {
		// Inner matches pA: both ¬match(pA) and ¬match(pB) must hold.
		pred := &submit.TxPredicate{
			Not: []*submit.TxPredicate{
				matchSubmit(pA),
				matchSubmit(pB),
			},
		}
		require.False(t, evalSubmit(nil, pred, leaf))

		// Leaf never matches: each ¬inner is true, so the full not is true.
		leafNever := func(_ gledger.Transaction, _ *cardano.TxPattern) bool {
			return false
		}
		require.True(t, evalSubmit(nil, pred, leafNever))
	})
}

func TestEvalTxPredicate_AllOf(t *testing.T) {
	pA := &cardano.TxPattern{}
	pB := &cardano.TxPattern{}
	leaf := func(_ gledger.Transaction, p *cardano.TxPattern) bool {
		return p == pA || p == pB
	}

	t.Run("all_must_hold", func(t *testing.T) {
		pred := &submit.TxPredicate{
			AllOf: []*submit.TxPredicate{
				matchSubmit(pA),
				matchSubmit(pB),
			},
		}
		require.True(t, evalSubmit(nil, pred, leaf))
	})

	t.Run("one_fails", func(t *testing.T) {
		pC := &cardano.TxPattern{}
		pred := &submit.TxPredicate{
			AllOf: []*submit.TxPredicate{
				matchSubmit(pA),
				matchSubmit(pC),
			},
		}
		require.False(t, evalSubmit(nil, pred, leaf))
	})
}

func TestEvalTxPredicate_AnyOf(t *testing.T) {
	pA := &cardano.TxPattern{}
	pB := &cardano.TxPattern{}
	pC := &cardano.TxPattern{}
	leaf := stubLeaf(pA)

	pred := &submit.TxPredicate{
		AnyOf: []*submit.TxPredicate{
			matchSubmit(pB),
			matchSubmit(pA),
			matchSubmit(pC),
		},
	}
	require.True(t, evalSubmit(nil, pred, leaf))

	predNone := &submit.TxPredicate{
		AnyOf: []*submit.TxPredicate{
			matchSubmit(pB),
			matchSubmit(pC),
		},
	}
	require.False(t, evalSubmit(nil, predNone, leaf))
}

func TestEvalTxPredicate_Nested(t *testing.T) {
	pA := &cardano.TxPattern{}
	pB := &cardano.TxPattern{}
	leaf := stubLeaf(pA)

	// all_of( any_of(match pB, match pA), not(match pA) )
	// any_of -> true (pA); not(match pA) -> false -> whole false
	pred := &submit.TxPredicate{
		AllOf: []*submit.TxPredicate{
			{
				AnyOf: []*submit.TxPredicate{
					matchSubmit(pB),
					matchSubmit(pA),
				},
			},
			{Not: []*submit.TxPredicate{matchSubmit(pA)}},
		},
	}
	require.False(t, evalSubmit(nil, pred, leaf))

	// all_of( not(match pB), any_of(match pB, match pA) )
	// not(pB) true, any_of true -> true
	pred2 := &submit.TxPredicate{
		AllOf: []*submit.TxPredicate{
			{Not: []*submit.TxPredicate{matchSubmit(pB)}},
			{
				AnyOf: []*submit.TxPredicate{
					matchSubmit(pB),
					matchSubmit(pA),
				},
			},
		},
	}
	require.True(t, evalSubmit(nil, pred2, leaf))
}

func TestEvalTxPredicate_WatchParity(t *testing.T) {
	pA := &cardano.TxPattern{}
	pB := &cardano.TxPattern{}
	leaf := stubLeaf(pA)

	s := &submit.TxPredicate{
		AnyOf: []*submit.TxPredicate{
			matchSubmit(pB),
			matchSubmit(pA),
		},
	}
	w := &watch.TxPredicate{
		AnyOf: []*watch.TxPredicate{
			matchWatch(pB),
			matchWatch(pA),
		},
	}
	require.Equal(
		t,
		evalTxPredicate(nil, txPredicateFromSubmit(s), leaf),
		evalTxPredicate(nil, txPredicateFromWatch(w), leaf),
	)
}

func TestEvalTxPredicate_EmptyNode(t *testing.T) {
	pred := &submit.TxPredicate{}
	require.False(
		t,
		evalSubmit(nil, pred, stubLeaf(&cardano.TxPattern{})),
	)
}

func TestEvalTxPredicate_NilTree(t *testing.T) {
	require.False(
		t,
		evalTxPredicate(nil, nil, stubLeaf(&cardano.TxPattern{})),
	)
}

func TestEvalTxPredicate_FromSubmitNil(t *testing.T) {
	require.Nil(t, txPredicateFromSubmit(nil))
}

func TestMatchesSubmitTxPredicate_NilPredicate(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
	})
	require.True(t, u.matchesSubmitTxPredicate(nil, nil))
}

func TestMatchesWatchTxPredicate_NilPredicate(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
	})
	require.True(t, u.matchesWatchTxPredicate(nil, nil))
}
