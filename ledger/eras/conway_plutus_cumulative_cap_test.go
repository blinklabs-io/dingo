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
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests are the multi-redeemer regression coverage for the cumulative
// cap gap flagged on blinklabs-io/dingo#3627 (CodeRabbit HIGH / cubic conf 9).
// The producer-budget trust drops ONLY the per-redeemer *declared*-budget
// compare; the transaction-wide protocol MaxTxExUnits ceiling must NEVER be
// relaxed. Otherwise two scripts that each individually meter under
// MaxTxExUnits could together exceed it and still be accepted under trust.

// unitApplyBody builds "[ (lam _ Unit) (con integer n) ]": it fully evaluates
// to Unit regardless of n, but different n produce different flat bytes and
// therefore a different script hash. Two such scripts have identical execution
// cost, so a transaction carrying both meters exactly 2x a single script.
func unitApplyBody(n int64) syn.Term[syn.DeBruijn] {
	return &syn.Apply[syn.DeBruijn]{
		Function: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Constant{Con: &syn.Unit{}},
		},
		Argument: &syn.Constant{Con: &syn.Integer{Inner: big.NewInt(n)}},
	}
}

// buildTwoMintTx wires two distinct V1 minting scripts into one mock Conway
// transaction, each with its own mint policy and redeemer (declared budgets
// declaredA/declaredB).
func buildTwoMintTx(
	scriptA, scriptB lcommon.PlutusV1Script,
	declaredA, declaredB lcommon.ExUnits,
) *mockConwayFeeTx {
	hashA := scriptA.Hash()
	hashB := scriptB.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(hashA): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
			lcommon.Blake2b224(hashB): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	// Mint redeemer indexes address policies in hash-sorted order, so pair the
	// declared budget with the policy that will occupy each index.
	declaredLow, declaredHigh := declaredA, declaredB
	if string(hashB.Bytes()) < string(hashA.Bytes()) {
		declaredLow, declaredHigh = declaredB, declaredA
	}
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{scriptA, scriptB},
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
							val: lcommon.RedeemerValue{ExUnits: declaredLow},
						},
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagMint,
								Index: 1,
							},
							val: lcommon.RedeemerValue{ExUnits: declaredHigh},
						},
					},
				},
			},
		},
		assetMint: &assetMint,
	}
	return tx
}

// measureSingleScriptUsed runs one distinct minting script over the real
// phase-2 apply path (declared budget 0, so the overage is reported) and
// returns the ex-units the local meter charged. It is used to size the
// per-transaction maximum for the cumulative-cap cases without hard-coding an
// evaluator-pinned constant for the unitApply script shape.
func measureSingleScriptUsed(t *testing.T) lcommon.ExUnits {
	t.Helper()
	script := encodeV1Script(t, unitApplyBody(1))
	tx, _ := buildMintTx(script, lcommon.ExUnits{}) // declared 0/0
	ls := newApplyPathLedgerState()
	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(lcommon.ExUnits{
		Steps:  1_000_000_000,
		Memory: 1_000_000_000,
	}))
	require.NoError(t, err)
	require.Len(t, ls.reported, 1)
	return ls.reported[0].used
}

// Two scripts each individually <= MaxTxExUnits but together > MaxTxExUnits are
// REJECTED even under producer trust: the transaction-wide cap is never
// relaxed.
func TestProducerTrust_CumulativeCapRejectsTwoScriptsOverMax(t *testing.T) {
	clearPhase1Rules(t)
	perScript := measureSingleScriptUsed(t)
	require.Positive(t, perScript.Steps)
	require.Positive(t, perScript.Memory)

	// Max sits between one and two scripts on BOTH dimensions: each script is
	// individually in bounds, the pair is not.
	maxExUnits := lcommon.ExUnits{
		Steps:  perScript.Steps + perScript.Steps/2,
		Memory: perScript.Memory + perScript.Memory/2,
	}

	scriptA := encodeV1Script(t, unitApplyBody(1))
	scriptB := encodeV1Script(t, unitApplyBody(2))
	require.NotEqual(t, scriptA.Hash(), scriptB.Hash())

	tx := buildTwoMintTx(
		scriptA,
		scriptB,
		lcommon.ExUnits{},
		lcommon.ExUnits{},
	)
	ls := newApplyPathLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(maxExUnits))
	require.Error(
		t,
		err,
		"cumulative used ex-units over MaxTxExUnits must be rejected even under trust",
	)
	var tooBig alonzo.ExUnitsTooBigUtxoError
	require.ErrorAs(
		t,
		err,
		&tooBig,
		"rejection must be the transaction-wide ExUnitsTooBig cap, not a per-script failure",
	)
	assert.Equal(t, maxExUnits, tooBig.MaxTxExUnits)
	assert.Greater(t, tooBig.TotalExUnits.Steps, maxExUnits.Steps)
}

// Control: the same two scripts under a maximum that DOES accommodate their sum
// are ACCEPTED under trust, proving the cap rejection above is the sum crossing
// MaxTxExUnits and not merely the presence of two scripts.
func TestProducerTrust_CumulativeCapAcceptsTwoScriptsUnderMax(t *testing.T) {
	clearPhase1Rules(t)
	perScript := measureSingleScriptUsed(t)

	// Room for both scripts plus margin.
	maxExUnits := lcommon.ExUnits{
		Steps:  perScript.Steps*2 + perScript.Steps,
		Memory: perScript.Memory*2 + perScript.Memory,
	}

	scriptA := encodeV1Script(t, unitApplyBody(1))
	scriptB := encodeV1Script(t, unitApplyBody(2))
	require.NotEqual(t, scriptA.Hash(), scriptB.Hash())

	tx := buildTwoMintTx(
		scriptA,
		scriptB,
		lcommon.ExUnits{},
		lcommon.ExUnits{},
	)
	ls := newApplyPathLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(maxExUnits))
	require.NoError(
		t,
		err,
		"two over-declared scripts whose sum stays under MaxTxExUnits are accepted under trust",
	)
	// Both per-redeemer overages are reported (declared 0 < used).
	require.Len(t, ls.reported, 2)
}

// Sanity: a single script over MaxTxExUnits still fails (the machine limit is
// MaxTxExUnits even on the trust path), so trust never lets one script exceed
// the per-transaction ceiling on its own.
func TestProducerTrust_SingleScriptOverMaxStillFails(t *testing.T) {
	clearPhase1Rules(t)
	perScript := measureSingleScriptUsed(t)

	// Max below a single script's real cost.
	maxExUnits := lcommon.ExUnits{
		Steps:  perScript.Steps / 2,
		Memory: perScript.Memory * 4,
	}
	script := encodeV1Script(t, unitApplyBody(1))
	tx, _ := buildMintTx(script, lcommon.ExUnits{})
	ls := newApplyPathLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(maxExUnits))
	require.Error(
		t,
		err,
		"a single script exceeding MaxTxExUnits must fail even under trust",
	)
}
