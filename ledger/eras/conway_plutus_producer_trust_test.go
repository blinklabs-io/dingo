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

// Regression coverage for the Conway phase-2 declared-budget check
// (blinklabs-io/dingo#3627, #3735).
//
// A follower briefly wedged when its locally metered ex-units for a
// producer-valid transaction came out marginally above the redeemer's declared
// budget, so restrictive phase-2 re-rejected a block the honest network had
// accepted. The fix is NOT to trust an over-budget block: cardano-ledger uses
// the declared per-redeemer ExUnits as the CEK budget on the apply path, so a
// genuinely over-budget block is an EvaluationError -> ValidationTagMismatch
// and MUST be rejected. Accepting it would make dingo follow a chain the
// reference node rejects and under-charge the min fee.
//
// The real defect was upstream in gouroboros: the Conway ScriptContext encoded
// a *closed* validity-interval upper bound for a TTL-only transaction where
// Conway uses strictUpperBound (gouroboros#2171), so the script datum -- and
// therefore the metered cost -- differed from the network's. gouroboros#2170
// era-gates that closure. With the corrected ScriptContext the locally metered
// cost matches the producer's, the declared budget is no longer exceeded, and
// strict phase-2 accepts the block with no producer-trust path.
//
// These tests assert the strict invariant that holds on every path (apply,
// mempool, forging, replay): a script whose true cost exceeds its declared
// budget is rejected, and a script whose declared budget covers its
// (correctly metered) cost is accepted -- both through the REAL production
// phase-2 path (ValidateTxConway -> validateTxPlutusConwayWithContext ->
// evaluateConwayPlutusScript) with a REAL Plutus V1 script.

// encodeV1Script flat+cbor encodes a V1 program the way a script witness holds
// it.
func encodeV1Script(t *testing.T, body syn.Term[syn.DeBruijn]) lcommon.PlutusV1Script {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		// Two-argument minting script: (redeemer, scriptContext) -> body.
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{Body: body},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	return lcommon.PlutusV1Script(scriptBytes)
}

// buildMintTx wires a V1 minting script into a mock Conway transaction whose
// redeemer declares the given ex-unit budget.
func buildMintTx(
	plutusScript lcommon.PlutusV1Script,
	declared lcommon.ExUnits,
) (*mockConwayFeeTx, lcommon.ScriptHash) {
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
							val: lcommon.RedeemerValue{ExUnits: declared},
						},
					},
				},
			},
		},
		assetMint: &assetMint,
	}
	return tx, scriptHash
}

// clearPhase1Rules disables the phase-1 rule set so the phase-2 budget behavior
// is what the assertions observe, not a fee/UTxO check on the mock tx.
func clearPhase1Rules(t *testing.T) {
	t.Helper()
	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil
}

func pparamsWithMax(max lcommon.ExUnits) *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 9},
		MaxTxExUnits:    max,
	}
}

// The V1 minting script "(redeemer, ctx) -> Unit" costs a Haskell-pinned
// 112100 cpu / 800 mem (see TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch).
var (
	successBody = syn.Term[syn.DeBruijn](&syn.Constant{Con: &syn.Unit{}})
	usedCpu     = int64(112100)
	usedMem     = int64(800)
)

// A transaction whose script actually costs more than its redeemer declares is
// REJECTED with the real production error on every path -- apply included.
// cardano-ledger meters the same overage as an EvaluationError and rejects the
// block (ValidationTagMismatch), so dingo must too; accepting it would follow a
// chain the reference node rejects. (This is the case #3735 originally proposed
// to accept; it must fail instead.)
func TestConwayPhase2RejectsBudgetOverage(t *testing.T) {
	clearPhase1Rules(t)
	script := encodeV1Script(t, successBody)
	tx, scriptHash := buildMintTx(script, lcommon.ExUnits{}) // declared 0/0
	ls := newMockLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	}))
	require.Error(t, err, "an over-declared-budget script must be rejected")
	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	// This is the exact production error string #3627 is about.
	assert.Contains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget: used (112100 cpu, 800 mem)",
	)
}

// When the redeemer's declared budget covers the script's correctly metered
// cost -- as it does on canonical blocks once the ScriptContext is computed
// correctly (gouroboros#2170 era-gates the TTL-only validity-interval upper
// bound) -- strict phase-2 ACCEPTS the transaction with no producer-trust path.
// This is the honest post-fix shape of the block that formerly wedged the
// follower: the metered cost equals what the producer declared.
func TestConwayPhase2AcceptsDeclaredBudgetCoveringMeteredCost(t *testing.T) {
	clearPhase1Rules(t)
	script := encodeV1Script(t, successBody)
	// Declare exactly the pinned metered cost: used == declared, so the strict
	// used>declared post-check does not fire.
	tx, _ := buildMintTx(script, lcommon.ExUnits{
		Steps:  usedCpu,
		Memory: usedMem,
	})
	ls := newMockLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	}))
	require.NoError(
		t,
		err,
		"a script whose declared budget covers its metered cost must be accepted strictly, with no trust path",
	)
}

// A GENUINE script failure (the script evaluates to error/False) is rejected
// even when its declared budget is generous, and it is reported as a plutus
// script failure -- never confused with a budget overage.
func TestConwayPhase2RejectsGenuineScriptFailure(t *testing.T) {
	clearPhase1Rules(t)
	// (redeemer, ctx) -> Error: fully evaluates then fails, like a script that
	// returns False / calls error.
	script := encodeV1Script(t, &syn.Error{})
	tx, scriptHash := buildMintTx(script, lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	})
	ls := newMockLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	}))
	require.Error(t, err, "a genuine Plutus failure must be rejected")
	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	assert.NotContains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget",
		"genuine failure must not be misclassified as a budget overage",
	)
}
