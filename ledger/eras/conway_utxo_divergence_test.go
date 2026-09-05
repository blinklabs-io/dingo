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
	"math/big"
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

// TestConwayUtxoValidationRuleIndexesArePinned pins the upstream rule indexes
// that dingo reports in "conway utxo validation rule %d" (see
// ValidateTxConway). The indexes are positions in the gouroboros
// conway.UtxoValidationRules slice, so an upstream insertion silently
// renumbers every operator-visible diagnostic and every issue report written
// against the old numbering. UtxoValidateRequiredRedeemers was inserted at 31
// in gouroboros v0.202.5, which moved value-not-conserved from 31 to 32.
func TestConwayUtxoValidationRuleIndexesArePinned(t *testing.T) {
	rules := conway.UtxoValidationRules
	for idx, want := range map[int]string{
		29: "UtxoValidateBadInputsUtxo",
		32: "UtxoValidateValueNotConservedUtxo",
	} {
		require.Greater(t, len(rules), idx)
		name := utxoValidationRuleName(rules[idx])
		assert.Contains(
			t,
			name,
			want,
			"conway rule index %d should resolve to %s, got %s",
			idx,
			want,
			name,
		)
	}
}

// TestValidateTxConwayGenuinelyMissingInputStillRejected is the negative case
// for the rollback restore fix in LedgerState.rollback: an input that is
// genuinely absent from the ledger must still be rejected as a bad input, and
// must still be reported under rule index 29. A restore fix that made input
// resolution more permissive would turn this into a consensus hazard.
func TestValidateTxConwayGenuinelyMissingInputStillRejected(t *testing.T) {
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
	assert.Contains(t, err.Error(), "conway utxo validation rule 29:")
}

// TestValidateTxConwayGenuinelyUnbalancedStillRejected is the negative case for
// the value-conservation half of issue #3678: a transaction whose inputs all
// resolve but whose consumed and produced values genuinely differ must still be
// rejected, and must still be reported under rule index 32.
func TestValidateTxConwayGenuinelyUnbalancedStillRejected(t *testing.T) {
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
	assert.Contains(t, err.Error(), "conway utxo validation rule 32:")

	// The input resolved, so bad-inputs must NOT also fire. This is what
	// separates a genuinely unbalanced transaction from the single-cause
	// pairing in issue #3678, where one unresolvable input produces both.
	var badInputs shelley.BadInputsUtxoError
	assert.NotErrorAs(t, err, &badInputs)
}
