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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// utxoValidationRuleName reports the runtime function name of a validation
// rule. It exists for assertions and diagnostics only: production resolution
// keys on the upstream rule Id, because common.ComposeUtxoValidationRules
// replaces phase-2-gated rules with anonymous wrappers and upstream moves
// shared rules between era packages. Never use it as a lookup key.
func utxoValidationRuleName(fn lcommon.UtxoValidationRuleFunc) string {
	if fn == nil {
		return ""
	}
	pc := reflect.ValueOf(fn).Pointer()
	if runtimeFn := runtime.FuncForPC(pc); runtimeFn != nil {
		return runtimeFn.Name()
	}
	return fmt.Sprintf("%x", pc)
}

// TestResolveUtxoValidationSkipIndexResolvesPhase2WrappedRule is the
// regression guard for blinklabs-io/dingo#3821: it fails if
// resolveUtxoValidationSkipIndex ever goes back to matching upstream rules by
// validation function identity or runtime name.
//
// common.ComposeUtxoValidationRules replaces every phase-2-gated entry with an
// anonymous wrapper closure, so the original function is unreachable from the
// composed list. A function-keyed resolver panics; an Id-keyed one does not.
func TestResolveUtxoValidationSkipIndexResolvesPhase2WrappedRule(t *testing.T) {
	descriptors := []lcommon.UtxoValidationRuleDescriptor{
		{
			Id:        lcommon.UtxoValidationRuleMetadata,
			Validator: conway.UtxoValidateMetadata,
		},
		{
			Id:        lcommon.UtxoValidationRuleCommitteeCertificates,
			Validator: conway.UtxoValidateCommitteeCertificates,
		},
		{
			Id:        lcommon.UtxoValidationRuleUnknownVoters,
			Validator: conway.UtxoValidateUnknownVoters,
		},
	}
	rules := lcommon.ComposeUtxoValidationRules(
		lcommon.AlwaysUtxoValidationRules(descriptors[0].Validator),
		lcommon.Phase2ValidUtxoValidationRules(
			descriptors[1].Validator,
			descriptors[2].Validator,
		),
	)
	require.Len(t, rules, len(descriptors))

	// Premise of the guard: the gated entries really are wrapped, so neither
	// function identity nor function name can find them. Without this the
	// assertions below could pass vacuously against a future upstream that
	// stops wrapping.
	require.Equal(
		t,
		utxoValidationRuleName(descriptors[0].Validator),
		utxoValidationRuleName(rules[0]),
		"an always-run rule must keep its function identity",
	)
	for _, index := range []int{1, 2} {
		require.NotEqual(
			t,
			utxoValidationRuleName(descriptors[index].Validator),
			utxoValidationRuleName(rules[index]),
			"a phase-2-gated rule must be wrapped by upstream compose",
		)
	}

	require.Equal(t, 0, resolveUtxoValidationSkipIndex(
		descriptors, rules, lcommon.UtxoValidationRuleMetadata,
	))
	require.Equal(t, 1, resolveUtxoValidationSkipIndex(
		descriptors, rules, lcommon.UtxoValidationRuleCommitteeCertificates,
	))
	require.Equal(t, 2, resolveUtxoValidationSkipIndex(
		descriptors, rules, lcommon.UtxoValidationRuleUnknownVoters,
	))
}

// TestConwayUpstreamGatedRulesAreWrapped proves the wrapping the guard above
// simulates is what the pinned gouroboros release actually does, so the
// package-init panic #3821 reported cannot silently stop being reachable.
func TestConwayUpstreamGatedRulesAreWrapped(t *testing.T) {
	descriptors := conway.UtxoValidationRuleDescriptors()
	require.Len(t, conway.UtxoValidationRules, len(descriptors))
	for _, id := range []lcommon.UtxoValidationRuleId{
		lcommon.UtxoValidationRuleCommitteeCertificates,
		lcommon.UtxoValidationRuleUnknownVoters,
	} {
		index := slices.IndexFunc(
			descriptors,
			func(d lcommon.UtxoValidationRuleDescriptor) bool {
				return d.Id == id
			},
		)
		require.GreaterOrEqual(t, index, 0, string(id))
		require.NotEqual(
			t,
			utxoValidationRuleName(descriptors[index].Validator),
			utxoValidationRuleName(conway.UtxoValidationRules[index]),
			"upstream %s is no longer wrapped; the #3821 guard needs review",
			id,
		)
	}
}

func TestResolveUtxoValidationSkipIndexPanics(t *testing.T) {
	descriptors := []lcommon.UtxoValidationRuleDescriptor{
		{
			Id:        lcommon.UtxoValidationRuleMetadata,
			Validator: conway.UtxoValidateMetadata,
		},
	}
	rules := []lcommon.UtxoValidationRuleFunc{conway.UtxoValidateMetadata}

	t.Run("empty id", func(t *testing.T) {
		require.PanicsWithValue(
			t,
			"UTxO validation skip rule Id is empty",
			func() {
				resolveUtxoValidationSkipIndex(descriptors, rules, "")
			},
		)
	})
	t.Run("absent id", func(t *testing.T) {
		require.Panics(t, func() {
			resolveUtxoValidationSkipIndex(
				descriptors, rules, lcommon.UtxoValidationRuleUnknownVoters,
			)
		})
	})
	t.Run("duplicate id", func(t *testing.T) {
		dup := append(
			slices.Clone(descriptors),
			descriptors[0],
		)
		require.Panics(t, func() {
			resolveUtxoValidationSkipIndex(
				dup,
				append(slices.Clone(rules), rules[0]),
				lcommon.UtxoValidationRuleMetadata,
			)
		})
	})
	t.Run("descriptor and rule count diverge", func(t *testing.T) {
		require.Panics(t, func() {
			resolveUtxoValidationSkipIndex(
				descriptors,
				append(slices.Clone(rules), conway.UtxoValidateMetadata),
				lcommon.UtxoValidationRuleMetadata,
			)
		})
	})
	t.Run("nil upstream rule", func(t *testing.T) {
		require.Panics(t, func() {
			resolveUtxoValidationSkipIndex(
				descriptors,
				[]lcommon.UtxoValidationRuleFunc{nil},
				lcommon.UtxoValidationRuleMetadata,
			)
		})
	})
}

// utxoValidationRuleReplacement is the Dingo rule installed in place of an
// upstream rule, together with the upstream function name expected at that
// rule's Id. The function name is an assertion, not a lookup key: it catches
// an upstream Id being reattached to a different validator.
type utxoValidationRuleReplacement struct {
	upstreamFuncName string
	dingoFunc        lcommon.UtxoValidationRuleFunc
}

type eraUtxoValidationRuleComposition struct {
	era         string
	descriptors []lcommon.UtxoValidationRuleDescriptor
	upstream    []lcommon.UtxoValidationRuleFunc
	built       []indexedUtxoValidationRule
	// dropped maps each removed rule Id to the upstream function name expected
	// at that Id.
	dropped map[lcommon.UtxoValidationRuleId]string
	// replaced maps each rule Id whose upstream rule Dingo swaps out.
	replaced map[lcommon.UtxoValidationRuleId]utxoValidationRuleReplacement
	// retained lists rule Ids that must still run, with the upstream function
	// name expected at each. This is the negative case: resolving by Id must
	// not remove a rule Dingo does not intend to remove.
	retained map[lcommon.UtxoValidationRuleId]string
}

func eraUtxoValidationRuleCompositions() []eraUtxoValidationRuleComposition {
	return []eraUtxoValidationRuleComposition{
		{
			era:         "shelley",
			descriptors: shelley.UtxoValidationRuleDescriptors(),
			upstream:    shelley.UtxoValidationRules,
			built:       shelleyUtxoValidationRules,
			dropped: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleFeeTooSmall: "shelley.UtxoValidateFeeTooSmallUtxo",
				lcommon.UtxoValidationRuleMaxTxSize:   "shelley.UtxoValidateMaxTxSizeUtxo",
			},
			retained: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleValueNotConserved: "shelley.UtxoValidateValueNotConservedUtxo",
				lcommon.UtxoValidationRuleSignatures:        "shelley.UtxoValidateSignatures",
			},
		},
		{
			era:         "allegra",
			descriptors: allegra.UtxoValidationRuleDescriptors(),
			upstream:    allegra.UtxoValidationRules,
			built:       allegraUtxoValidationRules,
			dropped: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleFeeTooSmall: "allegra.UtxoValidateFeeTooSmallUtxo",
				lcommon.UtxoValidationRuleMaxTxSize:   "allegra.UtxoValidateMaxTxSizeUtxo",
			},
			retained: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleValueNotConserved:       "allegra.UtxoValidateValueNotConservedUtxo",
				lcommon.UtxoValidationRuleOutsideValidityInterval: "allegra.UtxoValidateOutsideValidityIntervalUtxo",
			},
		},
		{
			era:         "alonzo",
			descriptors: alonzo.UtxoValidationRuleDescriptors(),
			upstream:    alonzo.UtxoValidationRules,
			built:       alonzoUtxoValidationRules,
			dropped: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRulePlutusScripts: "alonzo.UtxoValidatePlutusScripts",
			},
			retained: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleExtraneousRedeemers: "alonzo.UtxoValidateExtraneousRedeemers",
				lcommon.UtxoValidationRuleNativeScripts:       "alonzo.UtxoValidateNativeScripts",
				lcommon.UtxoValidationRuleDelegation:          "alonzo.UtxoValidateDelegation",
			},
		},
		{
			era:         "babbage",
			descriptors: babbage.UtxoValidationRuleDescriptors(),
			upstream:    babbage.UtxoValidationRules,
			built:       babbageUtxoValidationRules,
			dropped: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRulePlutusScripts: "babbage.UtxoValidatePlutusScripts",
			},
			retained: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleMalformedReferenceScripts: "babbage.UtxoValidateMalformedReferenceScripts",
				lcommon.UtxoValidationRuleWithdrawals:               "babbage.UtxoValidateWithdrawals",
			},
		},
		{
			era:         "conway",
			descriptors: conway.UtxoValidationRuleDescriptors(),
			upstream:    conway.UtxoValidationRules,
			built:       conwayUtxoValidationRules,
			dropped: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleFeeTooSmall:   "conway.UtxoValidateFeeTooSmallUtxo",
				lcommon.UtxoValidationRulePlutusScripts: "conway.UtxoValidatePlutusScripts",
			},
			replaced: map[lcommon.UtxoValidationRuleId]utxoValidationRuleReplacement{
				lcommon.UtxoValidationRuleConwayFeaturesWithPlutusV1V2: {
					upstreamFuncName: "conway.UtxoValidateConwayFeaturesWithPlutusV1V2",
					dingoFunc:        validateConwayFeaturesWithNeededPlutusV1V2,
				},
				lcommon.UtxoValidationRuleCommitteeCertificates: {
					upstreamFuncName: "conway.UtxoValidateCommitteeCertificates",
					dingoFunc:        validateCommitteeCertificates,
				},
				lcommon.UtxoValidationRuleUnknownVoters: {
					upstreamFuncName: "conway.UtxoValidateUnknownVoters",
					dingoFunc:        validateUnknownVoters,
				},
			},
			retained: map[lcommon.UtxoValidationRuleId]string{
				// Added upstream in gouroboros v0.202.6 and intentionally not
				// skipped: Conway validation now enforces a declared
				// currentTreasuryValue (transaction body key 21).
				lcommon.UtxoValidationRuleCurrentTreasuryValue: "common.UtxoValidateCurrentTreasuryValue",
				lcommon.UtxoValidationRuleExUnitsTooBig:        "conway.UtxoValidateExUnitsTooBigUtxo",
				lcommon.UtxoValidationRuleNativeScripts:        "conway.UtxoValidateNativeScripts",
				lcommon.UtxoValidationRuleCertificateDeposits:  "conway.UtxoValidateCertificateDeposits",
			},
		},
		{
			era:         "dijkstra",
			descriptors: gdijkstra.UtxoValidationRuleDescriptors(),
			upstream:    gdijkstra.UtxoValidationRules,
			built:       dijkstraPhase1UtxoValidationRules,
			dropped: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRulePlutusScripts: "dijkstra.UtxoValidatePlutusScripts",
			},
			replaced: map[lcommon.UtxoValidationRuleId]utxoValidationRuleReplacement{
				lcommon.UtxoValidationRuleCommitteeCertificates: {
					upstreamFuncName: "conway.UtxoValidateCommitteeCertificates",
					dingoFunc:        validateCommitteeCertificates,
				},
				lcommon.UtxoValidationRuleUnknownVoters: {
					upstreamFuncName: "conway.UtxoValidateUnknownVoters",
					dingoFunc:        validateUnknownVoters,
				},
			},
			retained: map[lcommon.UtxoValidationRuleId]string{
				lcommon.UtxoValidationRuleCurrentTreasuryValue: "common.UtxoValidateCurrentTreasuryValue",
				lcommon.UtxoValidationRuleExUnitsTooBig:        "dijkstra.UtxoValidateExUnitsTooBigUtxo",
				lcommon.UtxoValidationRuleCertificateDeposits:  "conway.UtxoValidateCertificateDeposits",
			},
		},
	}
}

// TestEraUtxoValidationRuleCompositions pins, for every era that removes or
// replaces an upstream rule, which upstream rule Ids Dingo drops, which it
// swaps for a local implementation, and that nothing else is disturbed.
//
// Resolving by Id is only correct if the Ids select the same rules the previous
// pin's function names did, so each expected Id is cross-checked against the
// upstream function name at that Id. An upstream reorder, rename, or Id
// reassignment fails here instead of silently changing what Dingo validates.
func TestEraUtxoValidationRuleCompositions(t *testing.T) {
	for _, era := range eraUtxoValidationRuleCompositions() {
		t.Run(era.era, func(t *testing.T) {
			require.Len(
				t,
				era.upstream,
				len(era.descriptors),
				"upstream descriptor and rule lists must stay aligned",
			)

			resolveExpected := func(
				id lcommon.UtxoValidationRuleId,
				wantFuncName string,
			) int {
				index := resolveUtxoValidationSkipIndex(
					era.descriptors,
					era.upstream,
					id,
				)
				require.Equal(t, id, era.descriptors[index].Id)
				require.Equal(
					t,
					wantFuncName,
					shortUtxoValidationRuleName(era.descriptors[index].Validator),
					"upstream rule %s is no longer implemented by %s",
					id,
					wantFuncName,
				)
				return index
			}

			droppedIndexes := map[int]lcommon.UtxoValidationRuleId{}
			for id, wantFuncName := range era.dropped {
				droppedIndexes[resolveExpected(id, wantFuncName)] = id
			}
			replacedIndexes := map[int]lcommon.UtxoValidationRuleId{}
			for id, replacement := range era.replaced {
				index := resolveExpected(id, replacement.upstreamFuncName)
				replacedIndexes[index] = id
				droppedIndexes[index] = id
			}

			// The built list must be exactly the upstream list minus the
			// dropped indexes, with the replaced indexes reinstated, each once,
			// in ascending upstream order.
			wantIndexes := make([]int, 0, len(era.upstream))
			for index := range era.upstream {
				if _, dropped := droppedIndexes[index]; dropped {
					if _, replaced := replacedIndexes[index]; !replaced {
						continue
					}
				}
				wantIndexes = append(wantIndexes, index)
			}
			gotIndexes := make([]int, 0, len(era.built))
			for _, rule := range era.built {
				gotIndexes = append(gotIndexes, rule.index)
			}
			require.Equal(
				t,
				wantIndexes,
				gotIndexes,
				"built rule positions must match the upstream list minus the dropped rules",
			)

			byIndex := map[int]lcommon.UtxoValidationRuleFunc{}
			for _, rule := range era.built {
				byIndex[rule.index] = rule.validationFunc
			}

			for index, id := range replacedIndexes {
				replacement := era.replaced[id]
				require.Equal(
					t,
					utxoValidationRuleName(replacement.dingoFunc),
					utxoValidationRuleName(byIndex[index]),
					"%s must run Dingo's replacement rule", id,
				)
			}
			for index, id := range droppedIndexes {
				if _, replaced := replacedIndexes[index]; replaced {
					continue
				}
				require.NotContains(
					t,
					byIndex,
					index,
					"upstream rule %s must be removed", id,
				)
			}
			for id, wantFuncName := range era.retained {
				index := resolveExpected(id, wantFuncName)
				require.Contains(
					t,
					byIndex,
					index,
					"upstream rule %s must still run", id,
				)
				require.Equal(
					t,
					utxoValidationRuleName(era.upstream[index]),
					utxoValidationRuleName(byIndex[index]),
					"upstream rule %s must run the upstream implementation", id,
				)
			}
		})
	}
}

// shortUtxoValidationRuleName trims the module path from a validation rule's
// runtime function name, leaving "<package>.<Func>".
func shortUtxoValidationRuleName(fn lcommon.UtxoValidationRuleFunc) string {
	name := utxoValidationRuleName(fn)
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return name[i+1:]
		}
	}
	return name
}

// treasuryUnavailableLedgerState mirrors *ledger.LedgerView while
// blinklabs-io/dingo#3687 is open: TreasuryValue is a mandatory
// common.LedgerState method that Dingo does not implement yet.
type treasuryUnavailableLedgerState struct {
	*mockLedgerState
	calls int
}

func (s *treasuryUnavailableLedgerState) TreasuryValue() (uint64, error) {
	s.calls++
	return 0, errors.New("not implemented")
}

// TestCurrentTreasuryValueRuleGuardsOnDeclaredValue pins the guard the
// gouroboros v0.202.6 bump depends on.
//
// common.UtxoValidateCurrentTreasuryValue is new in v0.202.6 and now runs for
// every Conway and Dijkstra transaction, but it reads
// LedgerState.TreasuryValue only for a transaction that declares
// currentTreasuryValue (transaction body key 21). Dingo's provider still
// returns an error, so this rule must stay unreachable for ordinary traffic
// until #3687 lands. If upstream drops the guard, this test fails instead of
// the node rejecting every transaction.
func TestCurrentTreasuryValueRuleGuardsOnDeclaredValue(t *testing.T) {
	pp := &conway.ConwayProtocolParameters{}
	for _, tc := range []struct {
		name       string
		declared   *big.Int
		wantCalls  int
		wantErrors bool
	}{
		{name: "no declared treasury value", declared: nil},
		{
			name:       "declared treasury value",
			declared:   big.NewInt(1),
			wantCalls:  1,
			wantErrors: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := newConwayFeaturesTestTx(newTestInput(0x91, 0))
			tx.currentTreasuryValue = tc.declared
			ls := &treasuryUnavailableLedgerState{
				mockLedgerState: newMockLedgerState(),
			}
			err := lcommon.UtxoValidateCurrentTreasuryValue(tx, 0, ls, pp)
			if tc.wantErrors {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(
				t,
				tc.wantCalls,
				ls.calls,
				"TreasuryValue must be consulted only for a declared value",
			)
		})
	}
}
