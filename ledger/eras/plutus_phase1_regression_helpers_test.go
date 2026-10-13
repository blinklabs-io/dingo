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

package eras

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// retainProductionRules narrows a production-built rule list to the rules
// carrying the given upstream ids. The rules keep the wiring dingo builds
// (buildBabbageValidationRules, buildConwayValidationRules), so a rule that the
// build drops is absent here too, while unrelated rules that need a fully
// formed transaction stay out of the way.
func retainProductionRules(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	descriptors []lcommon.UtxoValidationRuleDescriptor,
	ids ...lcommon.UtxoValidationRuleId,
) []indexedUtxoValidationRule {
	t.Helper()
	keep := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		found := false
		for index, descriptor := range descriptors {
			if descriptor.Id == id {
				keep[index] = struct{}{}
				found = true
			}
		}
		require.True(t, found, "no upstream rule with id %v", id)
	}
	ret := make([]indexedUtxoValidationRule, 0, len(ids))
	for _, rule := range rules {
		if _, ok := keep[rule.index]; ok {
			ret = append(ret, rule)
		}
	}
	return ret
}

// keepBabbageProductionRules must not be combined with t.Parallel: it swaps
// the package-level Babbage rule list.
func keepBabbageProductionRules(
	t *testing.T,
	ids ...lcommon.UtxoValidationRuleId,
) {
	t.Helper()
	original := babbageUtxoValidationRules
	babbageUtxoValidationRules = retainProductionRules(
		t, original, babbage.UtxoValidationRuleDescriptors(), ids...,
	)
	t.Cleanup(func() { babbageUtxoValidationRules = original })
}

// keepConwayProductionRules must not be combined with t.Parallel: it swaps
// the package-level Conway rule lists.
func keepConwayProductionRules(
	t *testing.T,
	ids ...lcommon.UtxoValidationRuleId,
) {
	t.Helper()
	originalRules := conwayUtxoValidationRules
	originalPhase1Rules := conwayPhase1UtxoValidationRules
	conwayUtxoValidationRules = retainProductionRules(
		t, originalRules, conway.UtxoValidationRuleDescriptors(), ids...,
	)
	conwayPhase1UtxoValidationRules = retainProductionRules(
		t, originalPhase1Rules, conway.UtxoValidationRuleDescriptors(), ids...,
	)
	t.Cleanup(func() {
		conwayUtxoValidationRules = originalRules
		conwayPhase1UtxoValidationRules = originalPhase1Rules
	})
}

// testDatumHashOutput is an output at addr locked with a datum hash.
type testDatumHashOutput struct {
	testOutput
	addr      lcommon.Address
	datumHash lcommon.Blake2b256
}

func (o testDatumHashOutput) Address() lcommon.Address { return o.addr }

func (o testDatumHashOutput) DatumHash() *lcommon.Blake2b256 {
	return &o.datumHash
}

// CollateralReturn reports no collateral return; without it the embedded nil
// lcommon.Transaction would panic in rules that consult it.
func (m *mockProducedValidityTx) CollateralReturn() lcommon.TransactionOutput {
	return nil
}
