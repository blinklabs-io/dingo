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
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConwayPhase2RejectsGenuineScriptFailureNotAsBudgetOverage covers the
// discriminator between the two ways phase-2 can reject a Conway transaction.
//
// TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch pins the overage
// arm: a script that succeeds but costs more than its redeemer declares is
// rejected with "script exceeded declared budget". This is the other arm. A
// script that evaluates to error is also rejected, but for a different reason,
// and the two must not be conflated.
//
// The distinction is load-bearing because restrictive evaluation raises the
// machine limit to MaxTxExUnits and compares the consumed amount against the
// declared budget only *after* the script returns. A genuine evaluation error
// returns before that comparison, so it must surface as the Plutus failure it
// is. Reporting it as a budget overage would misattribute a script bug to the
// fee the submitter declared, and would make an over-generous declared budget
// look like the cause of a failure it cannot affect.
//
// The script is a V1 minting script applied to two arguments, redeemer and
// script context, matching the shape the overage test uses so both arms
// exercise the same production path: ValidateTxConway ->
// validateTxPlutusConwayWithContext -> evaluateConwayPlutusScript.
func TestConwayPhase2RejectsGenuineScriptFailureNotAsBudgetOverage(
	t *testing.T,
) {
	// (redeemer, ctx) -> Error. The program is well formed and fully applied,
	// so it reaches the CEK machine and fails there rather than failing to
	// decode.
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Error{},
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
	// Clear the phase-1 rule set so the phase-2 outcome is what fails, rather
	// than a fee or UTxO check on the mock transaction.
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	plutusScript := lcommon.PlutusV1Script(scriptBytes)
	scriptHash := plutusScript.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)

	// The declared budget is deliberately far above the script's cost, so a
	// budget overage cannot be the reason for rejection.
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
				redeemers: &mockRedeemers{
					entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagMint,
								Index: 0,
							},
							val: lcommon.RedeemerValue{
								ExUnits: lcommon.ExUnits{
									Steps:  1_000_000,
									Memory: 1_000_000,
								},
							},
						},
					},
				},
			},
		},
		assetMint: &assetMint,
	}

	err = ValidateTxConway(
		tx,
		0,
		newMockLedgerState(),
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  1_000_000,
				Memory: 1_000_000,
			},
		},
	)
	require.Error(t, err, "a genuine Plutus failure must be rejected")

	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	assert.Equal(t, lcommon.RedeemerTagMint, plutusErr.Tag)
	assert.Equal(t, uint32(0), plutusErr.Index)
	assert.NotContains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget",
		"a genuine script failure must not be reported as a budget overage",
	)
}
