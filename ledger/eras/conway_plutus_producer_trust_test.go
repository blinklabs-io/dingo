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

// These tests are the follower-safety regression coverage for
// blinklabs-io/dingo#3627. They drive the REAL production phase-2 path
// (ValidateTxConway -> validateTxPlutusConwayWithContext ->
// evaluateConwayPlutusScript) with a REAL Plutus V1 script, so the error under
// test is the genuine
//
//	fmt.Errorf("script exceeded declared budget: used (...), declared (...)")
//
// that conway.go produces — not a synthetic error object. A block on the chain
// we follow can carry a transaction whose script our local cost meter charges
// marginally over its declared ex-units even though the producer/Haskell node
// accepted it; rejecting it deterministically re-rejects the block forever and
// wedges the follower off canonical. The fix makes ONLY the followed-chain
// apply path defer to the producer (exact/non-restrictive evaluation); mempool
// admission and forging stay strict.

// overageEvent records a ReportProducerPlutusBudgetOverage callback.
type overageEvent struct {
	scriptHash lcommon.ScriptHash
	tag        lcommon.RedeemerTag
	index      uint32
	used       lcommon.ExUnits
	declared   lcommon.ExUnits
}

// applyPathLedgerState models the followed-chain block-apply LedgerView: it
// implements the same TrustProducerPlutusBudget()/ReportProducerPlutusBudgetOverage()
// capabilities *ledger.LedgerView exposes there, on top of the shared test
// double. newMockLedgerState() (no trust) models the strict mempool/forging
// paths.
type applyPathLedgerState struct {
	*mockLedgerState
	reported []overageEvent
}

func newApplyPathLedgerState() *applyPathLedgerState {
	return &applyPathLedgerState{mockLedgerState: newMockLedgerState()}
}

func (s *applyPathLedgerState) TrustProducerPlutusBudget() bool { return true }

func (s *applyPathLedgerState) ReportProducerPlutusBudgetOverage(
	scriptHash lcommon.ScriptHash,
	tag lcommon.RedeemerTag,
	index uint32,
	used lcommon.ExUnits,
	declared lcommon.ExUnits,
) {
	s.reported = append(s.reported, overageEvent{
		scriptHash: scriptHash,
		tag:        tag,
		index:      index,
		used:       used,
		declared:   declared,
	})
}

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
// With a declared budget of zero it is a genuine over-declared-budget case.
var (
	successBody = syn.Term[syn.DeBruijn](&syn.Constant{Con: &syn.Unit{}})
	usedCpu     = int64(112100)
	usedMem     = int64(800)
)

// (a) Followed-chain apply path TRUSTS a budget overage: the block is accepted
//
//	and the overage is reported (observable "trusting producer" log).
func TestProducerTrust_ApplyPathAcceptsBudgetOverage(t *testing.T) {
	clearPhase1Rules(t)
	script := encodeV1Script(t, successBody)
	tx, scriptHash := buildMintTx(script, lcommon.ExUnits{}) // declared 0/0
	ls := newApplyPathLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	}))
	require.NoError(
		t,
		err,
		"apply path must ACCEPT a producer-valid over-declared-budget script (#3627)",
	)
	require.Len(t, ls.reported, 1, "the tolerated overage must be reported")
	ev := ls.reported[0]
	assert.Equal(t, scriptHash, ev.scriptHash)
	assert.Equal(t, lcommon.RedeemerTagMint, ev.tag)
	assert.Equal(t, usedCpu, ev.used.Steps)
	assert.Equal(t, usedMem, ev.used.Memory)
	assert.Equal(t, int64(0), ev.declared.Steps)
	assert.Equal(t, int64(0), ev.declared.Memory)
}

// (b) The strict paths (mempool admission / forging), modeled by a ledger state
//
//	WITHOUT the trust capability, still REJECT the same overage with the real
//	production error. We must never produce or relay-admit an over-budget tx.
func TestProducerTrust_StrictPathRejectsBudgetOverage(t *testing.T) {
	clearPhase1Rules(t)
	script := encodeV1Script(t, successBody)
	tx, scriptHash := buildMintTx(script, lcommon.ExUnits{}) // declared 0/0
	ls := newMockLedgerState()                               // no trust capability

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	}))
	require.Error(t, err, "strict path must REJECT an over-budget script")
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

// (c) Even on the trusting apply path, a GENUINE script failure (the script
//
//	evaluates to error/False) still REJECTS and is NOT reported as an
//	overage. Exact mode drops only the used>declared post-check, never the
//	script's own evaluation error.
func TestProducerTrust_ApplyPathRejectsGenuineFailure(t *testing.T) {
	clearPhase1Rules(t)
	// (redeemer, ctx) -> Error: fully evaluates then fails, like a script that
	// returns False / calls error.
	script := encodeV1Script(t, &syn.Error{})
	tx, scriptHash := buildMintTx(script, lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	})
	ls := newApplyPathLedgerState()

	err := ValidateTxConway(tx, 0, ls, pparamsWithMax(lcommon.ExUnits{
		Steps:  1_000_000,
		Memory: 1_000_000,
	}))
	require.Error(
		t,
		err,
		"a genuine Plutus failure must still be rejected even when the producer is trusted",
	)
	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	assert.NotContains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget",
		"genuine failure must not be misclassified as a budget overage",
	)
	assert.Empty(
		t,
		ls.reported,
		"a genuine failure must never be reported as a tolerated overage",
	)
}
