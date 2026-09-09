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
	"fmt"
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// conwayDivergencePparams are the minimum protocol parameters needed to reach
// the bad-input and value-conservation rules without tripping an earlier rule.
func conwayDivergencePparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: conway.MinProtocolVersionConway,
		},
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

// newConwayDivergenceTx builds a real ConwayTransaction from CBOR so that
// validation runs against the production decoder rather than a hand-rolled
// mock. Passing outputAmount == 0 omits the output list entirely.
func newConwayDivergenceTx(
	t *testing.T,
	inputHashByte byte,
	fee uint64,
	outputAmount uint64,
) *conway.ConwayTransaction {
	t.Helper()

	inputHash := make([]byte, 32)
	inputHash[0] = inputHashByte
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number: 258,
			Content: []any{
				[]any{inputHash, uint64(0)},
			},
		},
		2: fee,
	}
	if outputAmount > 0 {
		// Shelley-era output form: [address, coin]. A 29-byte payment-key
		// address (header byte + 28-byte key hash) is the smallest form the
		// address decoder accepts.
		addr := make([]byte, 29)
		addr[0] = 0x60
		bodyMap[1] = []any{
			[]any{addr, outputAmount},
		}
	}
	txCbor, err := cbor.Encode([]any{bodyMap, map[uint]any{}, true, nil})
	require.NoError(t, err)

	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return tx
}

// conwayUtxoValidationRuleIndex resolves the position of the upstream Conway
// rule carrying the stable semantic identifier id, exactly as production
// resolution does in resolveUtxoValidationSkipIndex (ledger/eras/validation.go):
// by descriptor Id, never by a pinned position.
//
// Position is not a stable property. gouroboros composes
// conway.UtxoValidationRules from the ordered descriptor list, so any upstream
// insertion renumbers every rule after it; the Id does not move. Tests that
// pinned literal positions broke on the v0.202.5 and v0.202.9 bumps
// (issues #3764, #3976, #3983), while production, which keys on the Id, did
// not.
func conwayUtxoValidationRuleIndex(
	t *testing.T,
	id lcommon.UtxoValidationRuleId,
) int {
	t.Helper()
	descriptors := conway.UtxoValidationRuleDescriptors()
	require.Len(
		t,
		conway.UtxoValidationRules,
		len(descriptors),
		"upstream descriptor list and composed rule list must agree in length",
	)
	index := slices.IndexFunc(
		descriptors,
		func(d lcommon.UtxoValidationRuleDescriptor) bool {
			return d.Id == id
		},
	)
	require.GreaterOrEqual(
		t,
		index,
		0,
		"upstream Conway must declare validation rule %q",
		id,
	)
	require.NotNil(
		t,
		conway.UtxoValidationRules[index],
		"upstream Conway rule %q must compose to a callable rule",
		id,
	)
	return index
}

// TestConwayUtxoValidationRulesRemainInProductionRuleSet fails if a rule the
// tests below depend on silently disappears from the slice Dingo actually
// runs. buildConwayValidationRules drops upstream rules by Id (the skip list in
// ledger/eras/conway.go), so a rule added to that list, or removed upstream,
// would stop firing while the transactions that should be rejected quietly
// start validating.
//
// It asserts presence and reachability, not position: the index each rule
// occupies is read from the upstream descriptors at run time, so an upstream
// insertion renumbers the expectation along with the rule.
func TestConwayUtxoValidationRulesRemainInProductionRuleSet(t *testing.T) {
	for _, id := range []lcommon.UtxoValidationRuleId{
		lcommon.UtxoValidationRuleBadInputs,
		lcommon.UtxoValidationRuleValueNotConserved,
	} {
		index := conwayUtxoValidationRuleIndex(t, id)
		assert.True(
			t,
			slices.ContainsFunc(
				conwayUtxoValidationRules,
				func(rule indexedUtxoValidationRule) bool {
					return rule.index == index &&
						rule.validationFunc != nil
				},
			),
			"conway rule %q (upstream index %d) must remain in the composed production rule set",
			id,
			index,
		)
	}
}

// TestValidateTxConwayGenuinelyMissingInputStillRejected is the negative case
// for the rollback restore fix in LedgerState.rollback: an input that is
// genuinely absent from the ledger must still be rejected as a bad input, and
// must still be reported under the bad-inputs rule. A restore fix that made
// input resolution more permissive would turn this into a consensus hazard.
func TestValidateTxConwayGenuinelyMissingInputStillRejected(t *testing.T) {
	badInputsIndex := conwayUtxoValidationRuleIndex(
		t,
		lcommon.UtxoValidationRuleBadInputs,
	)
	tx := newConwayDivergenceTx(t, 0xaa, 200_000, 0)

	err := ValidateTxConway(
		tx,
		0,
		newMockLedgerState(),
		conwayDivergencePparams(),
	)
	require.Error(t, err)

	var badInputs shelley.BadInputsUtxoError
	require.ErrorAs(t, err, &badInputs)
	require.Len(t, badInputs.Inputs, 1)
	assert.Equal(t, tx.Inputs()[0].String(), badInputs.Inputs[0].String())
	assert.Contains(
		t,
		err.Error(),
		fmt.Sprintf("conway utxo validation rule %d:", badInputsIndex),
	)
}

// TestValidateTxConwayGenuinelyUnbalancedStillRejected is the negative case for
// the value-conservation half of issue #3678: a transaction whose inputs all
// resolve but whose consumed and produced values genuinely differ must still be
// rejected, and must still be reported under the value-not-conserved rule.
func TestValidateTxConwayGenuinelyUnbalancedStillRejected(t *testing.T) {
	notConservedIndex := conwayUtxoValidationRuleIndex(
		t,
		lcommon.UtxoValidationRuleValueNotConserved,
	)
	tx := newConwayDivergenceTx(t, 0xbb, 200_000, 200_000)

	ls := newMockLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(5_000_000))

	err := ValidateTxConway(
		tx,
		0,
		ls,
		conwayDivergencePparams(),
	)
	require.Error(t, err)

	var notConserved shelley.ValueNotConservedUtxoError
	require.ErrorAs(t, err, &notConserved)
	require.NotNil(t, notConserved.Consumed)
	require.NotNil(t, notConserved.Produced)
	assert.Equal(
		t,
		big.NewInt(5_000_000),
		notConserved.Consumed,
		"consumed should be the resolved input value",
	)
	assert.Equal(t, big.NewInt(400_000), notConserved.Produced)
	assert.Contains(
		t,
		err.Error(),
		fmt.Sprintf("conway utxo validation rule %d:", notConservedIndex),
	)

	// The input resolved, so bad-inputs must NOT also fire. This is what
	// separates a genuinely unbalanced transaction from the single-cause
	// pairing in issue #3678, where one unresolvable input produces both.
	var badInputs shelley.BadInputsUtxoError
	assert.NotErrorAs(t, err, &badInputs)
}
