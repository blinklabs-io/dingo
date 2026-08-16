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

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch is the Conway
// counterpart to TestPlutusBudgetComparisonIncludesFinalSlippageBatch.
//
// evaluateConwayPlutusScript is where this change did the most: the flag
// selecting restrictive mode was renamed, and the suppression of the CEK
// machine's trailing slippage flush was dropped from each of the V1, V2 and
// V3 language branches. The Alonzo and Babbage cases cannot cover it, and
// the immutable test corpus that measured this change contains no Conway
// Plutus evaluations at all, so without this the path that changed most had
// no guard.
//
// The declared budget is zero on purpose. Restrictive validation runs the
// script against the enormous budget and compares afterwards, so the whole
// measured cost shows up in the overage — including the trailing batch that
// Haskell flushes on a successful return. Suppressing that flush again
// lowers the reported figures and fails this test.
func TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch(t *testing.T) {
	// Conway supplies the datum for a spending script, so the program takes
	// three arguments: datum, redeemer, script context.
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Lambda[syn.DeBruijn]{
					Body: &syn.Constant{Con: &syn.Unit{}},
				},
			},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)

	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	// Clear the phase-1 rule set so the phase-2 budget comparison is what
	// fails, rather than a fee or UTxO check on the mock transaction.
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	plutusScript := lcommon.PlutusV1Script(scriptBytes)
	scriptHash := plutusScript.Hash()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)

	spendInput := newTestInput(0x01, 0)
	witnesses := &mockWitnessSet{
		plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
		redeemers: &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagSpend,
						Index: 0,
					},
					val: lcommon.RedeemerValue{
						ExUnits: lcommon.ExUnits{},
					},
				},
			},
		},
	}
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType:    txTypeAlonzo,
			witnesses: witnesses,
		},
		inputs: []lcommon.TransactionInput{spendInput},
	}

	// Conway rejects a spending script whose UTxO carries neither a datum
	// nor a datum hash before phase 2 runs, so the output needs one.
	datumCbor, err := cbor.Encode(uint64(42))
	require.NoError(t, err)
	spent, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(addr.String()).
		WithLovelace(1_000_000).
		WithDatum(datumCbor).
		Build()
	require.NoError(t, err)

	ls := newMockLedgerState()
	ls.addUtxo(spendInput, spent)

	err = ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
		},
	)
	require.Error(t, err)

	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	assert.Equal(t, lcommon.RedeemerTagSpend, plutusErr.Tag)
	assert.Equal(t, uint32(0), plutusErr.Index)
	// Pinning the exact figures is the point: the flush is what the change
	// restored, and any drift in what restrictive mode reports for this
	// program shows up here rather than as a silently different budget.
	assert.Contains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget: used (160100 cpu, 1100 mem)",
	)
}
