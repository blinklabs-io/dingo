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

// TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch is the Conway
// counterpart to TestPlutusBudgetComparisonIncludesFinalSlippageBatch.
//
// evaluateConwayPlutusScript is where this change did the most: the flag
// selecting restrictive mode was renamed, and the suppression of the CEK
// machine's trailing slippage flush was dropped from each of the V1, V2 and
// V3 language branches. The Alonzo and Babbage cases cannot reach it, and
// the immutable corpus that measured this change contains no Conway Plutus
// evaluations at all, so the path that changed most had no guard.
//
// The purpose is minting rather than spending on purpose. A V1 minting
// script is applied to two arguments, redeemer and script context, which is
// the same shape the Alonzo and Babbage cases evaluate. That keeps the
// expected cost equal to the Haskell-derived 112100 CPU / 800 memory those
// cases assert, instead of a third figure whose only provenance is this
// implementation's own output. Spending would require a datum under Conway
// and so a three-argument program, whose cost nothing external pins.
//
// The declared budget is zero on purpose. Restrictive validation runs the
// script against the protocol transaction budget and compares afterwards, so
// the entire measured cost appears in the overage — including the trailing
// batch the Haskell machine flushes on a successful return. Under-reporting
// that batch lowers these figures and fails this test.
func TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch(t *testing.T) {
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Constant{Con: &syn.Unit{}},
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
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)

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
								ExUnits: lcommon.ExUnits{},
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
	require.Error(t, err)

	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	assert.Equal(t, lcommon.RedeemerTagMint, plutusErr.Tag)
	assert.Equal(t, uint32(0), plutusErr.Index)
	assert.Contains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget: used (112100 cpu, 800 mem)",
	)

	t.Run("restrictive evaluation is capped by protocol transaction budget", func(t *testing.T) {
		err := ValidateTxConway(
			tx,
			0,
			newMockLedgerState(),
			&conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 9,
				},
				MaxTxExUnits: lcommon.ExUnits{
					Steps:  1_000,
					Memory: 100,
				},
			},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of budget")
	})
}
