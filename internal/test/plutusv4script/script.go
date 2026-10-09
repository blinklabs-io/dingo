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

// Package plutusv4script builds PlutusV4 scripts that inspect the
// ScriptContext, shared by the era validation tests and the ledger tests that
// drive the same scripts through block application, replay and the mempool.
package plutusv4script

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/builtin"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// Term is a script term over de Bruijn indices.
type Term = syn.Term[syn.DeBruijn]

// Apply applies a builtin to its arguments, forcing it as often as its
// polymorphism requires.
func Apply(fn builtin.DefaultFunction, args ...Term) Term {
	var term Term = &syn.Builtin{DefaultFunction: fn}
	forces := 0
	//nolint:exhaustive // Every other builtin used here takes no force.
	switch fn {
	case builtin.SndPair, builtin.FstPair:
		forces = 2
	case builtin.HeadList, builtin.TailList, builtin.IfThenElse:
		forces = 1
	}
	for range forces {
		term = &syn.Force[syn.DeBruijn]{Term: term}
	}
	for _, arg := range args {
		term = &syn.Apply[syn.DeBruijn]{Function: term, Argument: arg}
	}
	return term
}

// Field selects the index-th element of a list.
func Field(list Term, index int) Term {
	for range index {
		list = Apply(builtin.TailList, list)
	}
	return Apply(builtin.HeadList, list)
}

// ConstrFields returns the field list of a Constr.
func ConstrFields(term Term) Term {
	return Apply(builtin.SndPair, Apply(builtin.UnConstrData, term))
}

// BoolConstant is a boolean literal.
func BoolConstant(b bool) Term {
	return &syn.Constant{Con: &syn.Bool{Inner: b}}
}

// IntConstant is an integer literal.
func IntConstant(n int64) Term {
	return &syn.Constant{Con: &syn.Integer{Inner: big.NewInt(n)}}
}

// ContextScript builds a PlutusV4 script that succeeds only when cond,
// evaluated against the ScriptContext, is true.
func ContextScript(
	t testing.TB,
	cond func(ctx Term) Term,
) lcommon.PlutusV4Script {
	t.Helper()
	ctx := Term(&syn.Var[syn.DeBruijn]{Name: 1})
	body := &syn.Force[syn.DeBruijn]{Term: Apply(
		builtin.IfThenElse,
		cond(ctx),
		&syn.Delay[syn.DeBruijn]{Term: &syn.Constant{Con: &syn.Unit{}}},
		&syn.Delay[syn.DeBruijn]{Term: &syn.Error{}},
	)}
	flat, err := syn.Encode(&syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: body},
	})
	require.NoError(t, err)
	wrapper, err := cbor.Encode(flat)
	require.NoError(t, err)
	return lcommon.PlutusV4Script(wrapper)
}

// MaybeScript succeeds when the Maybe selected by sel has the given
// constructor tag: 0 is Just, 1 is Nothing. It compares the tag rather than
// the whole value, because Just carries a field and so never equals a bare
// Constr 0.
func MaybeScript(
	t testing.TB,
	sel func(ctx Term) Term,
	tag int64,
) lcommon.PlutusV4Script {
	t.Helper()
	return ContextScript(t, func(ctx Term) Term {
		return Apply(
			builtin.EqualsInteger,
			Apply(builtin.FstPair, Apply(builtin.UnConstrData, sel(ctx))),
			IntConstant(tag),
		)
	})
}

// TxInfoSubTxIx selects txInfoSubTxIx from the ScriptContext.
func TxInfoSubTxIx(ctx Term) Term {
	txInfo := Field(ConstrFields(ctx), 0)
	return Field(ConstrFields(txInfo), 1)
}

// GuardingTopTxInfo selects the TopTxInfo of a GuardingScript purpose.
func GuardingTopTxInfo(ctx Term) Term {
	scriptInfo := Field(ConstrFields(ctx), 2)
	return Field(ConstrFields(scriptInfo), 1)
}

// TxInfo field positions of the Dijkstra account maps.
const (
	DirectDepositsField   = 8
	BalanceIntervalsField = 9
	RequiredGuardsField   = 12
)

const (
	keyCredentialTag    = 0
	scriptCredentialTag = 1
)

// MapOrderScript succeeds only when the TxInfo map at fieldIndex lists a
// script credential (Constr 1) first and a key credential (Constr 0) second.
// The reference orders script entries before key entries, which is the
// reverse of the credential type numbers.
func MapOrderScript(
	t testing.TB,
	fieldIndex int,
) lcommon.PlutusV4Script {
	t.Helper()
	return ContextScript(t, func(ctx Term) Term {
		txInfo := Field(ConstrFields(ctx), 0)
		entries := Apply(
			builtin.UnMapData,
			Field(ConstrFields(txInfo), fieldIndex),
		)
		tagIs := func(entry Term, tag int64) Term {
			credential := Apply(builtin.FstPair, entry)
			return Apply(
				builtin.EqualsInteger,
				Apply(
					builtin.FstPair,
					Apply(builtin.UnConstrData, credential),
				),
				IntConstant(tag),
			)
		}
		return &syn.Force[syn.DeBruijn]{Term: Apply(
			builtin.IfThenElse,
			tagIs(Apply(builtin.HeadList, entries), scriptCredentialTag),
			&syn.Delay[syn.DeBruijn]{
				Term: tagIs(Field(entries, 1), keyCredentialTag),
			},
			&syn.Delay[syn.DeBruijn]{Term: BoolConstant(false)},
		)}
	})
}

// ScriptCredential is the credential naming the script.
func ScriptCredential(s lcommon.Script) lcommon.Credential {
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: lcommon.Blake2b224(s.Hash()),
	}
}
