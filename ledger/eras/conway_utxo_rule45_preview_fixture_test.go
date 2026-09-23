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
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

// Preview parameters for epoch 672 (protocol version 9), from the chain's own
// epoch parameters for that epoch.
const (
	previewConwayTxFile     = "preview-conway-tx-58083610.cbor"
	previewConwayCostModels = "preview-costmodels-pv9-epoch672.json"

	previewConwayTxId       = "f5a0e06f3147c499c324041d16f510db07dd875aa0a80b7b0b2f2e990f9d7e45"
	previewConwaySlot       = 58_083_610
	previewConwayProtoMajor = 9

	// The malformed reference script's hash, matching dingo#4393's log text
	// ("malformed reference scripts: [e985ee15...]") exactly.
	previewConwayMalformedScriptHash = "e985ee15101d2cef31eab9bd0e6ea55423b26aabf78171b87ed440af"
)

func previewConwayProtocolParams(
	t *testing.T,
) *conway.ConwayProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV1 []int64 `json:"PlutusV1"`
		PlutusV2 []int64 `json:"PlutusV2"`
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(t, json.Unmarshal(
		readErasFixture(t, previewConwayCostModels),
		&costModels,
	))
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: previewConwayProtoMajor,
			Minor: 0,
		},
		CostModels: map[uint][]int64{
			0: costModels.PlutusV1,
			1: costModels.PlutusV2,
			2: costModels.PlutusV3,
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 14_000_000,
			Steps:  10_000_000_000,
		},
	}
}

// TestConwayUtxoRule45AcceptsPreviewMalformedVersionReferenceScript is the
// regression test for blinklabs-io/dingo#4393: a genesis Preview sync
// deterministically and permanently halted at block 2,423,581 / slot
// 58083610 because conway UTXO validation rule 45
// (conway.UtxoValidateMalformedReferenceScripts, wired unmodified into
// ValidateTxConway's rule table) rejected a real, canonical, producer-accepted
// transaction. The rejected transaction carries a PlutusV2 reference script
// (hash e985ee15..., 123 bytes) whose flat header declares UPLC program
// version 89.49.145 -- not a real compiled script, but nothing in upstream
// plutus-ledger-api's deserialiseScript inspects the program version for a
// script that is merely present as a reference script output and never
// executed. The UPLC-version gate belongs only at execution time
// (mkTermToEvaluate), which plutigo already applies correctly via
// syn.ValidateTermVersionForExecution.
//
// plutigo v0.7.1's validateProgramVersion applied that whitelist at decode
// time instead, in the same code path used for well-formedness checks of
// unexecuted reference scripts, so dingo rejected every peer's identical copy
// of this canonical block and could not resync Preview from genesis on any
// build. plutigo v0.7.2 (blinklabs-io/plutigo#415) moves the gate to
// execution time; this test fails before that bump and passes after it.
func TestConwayUtxoRule45AcceptsPreviewMalformedVersionReferenceScript(
	t *testing.T,
) {
	t.Parallel()
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, previewConwayTxFile),
	)
	require.NoError(t, err)
	require.Equal(t, previewConwayTxId, tx.Hash().String())
	require.True(t, tx.IsValid())

	require.Len(t, tx.Outputs(), 2)
	refScript := tx.Outputs()[0].ScriptRef()
	require.NotNil(
		t,
		refScript,
		"fixture must carry output 0's reference script",
	)
	require.Equal(
		t,
		previewConwayMalformedScriptHash,
		refScript.Hash().String(),
	)

	pp := previewConwayProtocolParams(t)

	// This is the exact, unwrapped validation function
	// buildConwayValidationRules wires into ValidateTxConway's rule table for
	// upstream rule Id UtxoValidationRuleMalformedReferenceScripts --
	// dingo#4393's "conway utxo validation rule 45" -- not an extracted
	// helper the production entry point bypasses.
	err = conway.UtxoValidateMalformedReferenceScripts(
		tx,
		previewConwaySlot,
		nil,
		pp,
	)
	require.NoError(
		t,
		err,
		"a real, producer-accepted Preview reference script must not be "+
			"rejected for its unexecuted UPLC program version",
	)

	var malformed lcommon.MalformedReferenceScriptsError
	require.False(
		t,
		errors.As(err, &malformed),
		"must not classify the reference script as malformed",
	)
}

// TestConwayPlutusV2ScriptStillRejectsMalformedVersionAtExecution is the
// negative case dingo#4393's root-cause analysis requires alongside the fix:
// the same 123-byte script that rule 45 must now accept as an unexecuted
// reference script must still be rejected if anything ever tries to execute
// it, so the fix does not also remove the execution-time gate
// (syn.ValidateTermVersionForExecution) that a script actually invoked as a
// witness/redeemer target still needs. Script.Evaluate decodes and validates
// the program before applying any arguments, so this observes the gate
// directly without needing a spending context for this particular script.
func TestConwayPlutusV2ScriptStillRejectsMalformedVersionAtExecution(
	t *testing.T,
) {
	t.Parallel()
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, previewConwayTxFile),
	)
	require.NoError(t, err)

	refScript := tx.Outputs()[0].ScriptRef()
	require.NotNil(t, refScript)
	v2Script, ok := refScript.(lcommon.PlutusV2Script)
	require.True(
		t,
		ok,
		"fixture script must decode as PlutusV2Script, got %T",
		refScript,
	)

	evalContext := cek.NewDefaultEvalContext(
		cek.LanguageVersionV2,
		cek.ProtoVersion{Major: previewConwayProtoMajor},
	)
	zero := data.NewInteger(big.NewInt(0))
	_, evalErr := v2Script.Evaluate(
		zero,
		zero,
		zero,
		lcommon.ExUnits{Memory: 14_000_000, Steps: 10_000_000_000},
		evalContext,
	)
	require.Error(
		t,
		evalErr,
		"a script with an unsupported UPLC program version must still be "+
			"rejected when actually executed",
	)
}
