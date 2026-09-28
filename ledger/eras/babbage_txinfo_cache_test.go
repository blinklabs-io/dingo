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
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	babbageCacheTxFee        = 200_000
	babbageCacheInputValue   = 2_000_000
	babbageCacheOutputValue  = 1_000_000
	babbageCacheValidityFrom = 41_098_000
	babbageCacheValidityTtl  = 41_099_000
	babbageCacheRedeemerMem  = 1_000_000
	babbageCacheRedeemerCpu  = 1_000_000_000
)

// alwaysSucceedsScriptBytes is the CBOR-wrapped flat encoding of
// `(program 1.0.0 (lam _ (lam _ (lam _ (con unit ())))))`: a validator that
// accepts three arguments -- datum, redeemer, and script context, the shape a
// spending script is applied to -- and returns unit. PlutusV1 and PlutusV2 both
// run UPLC 1.0.0, so the same bytes serve either language and the two differ
// only in the language prefix byte the script hash is taken over, which is what
// puts them at two distinct addresses.
func alwaysSucceedsScriptBytes(t *testing.T) []byte {
	t.Helper()
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
	return scriptBytes
}

// scriptAddressBytes is the enterprise address paying to scriptHash, which is
// what makes a UTxO at it spendable only by running that script and so gives
// its spend redeemer that script hash.
func scriptAddressBytes(t *testing.T, scriptHash lcommon.ScriptHash) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return addrBytes
}

// newBabbageMultiRedeemerTx builds a Babbage transaction that spends one
// script-locked UTxO per entry in scripts, in the order given, and returns it
// with a ledger state holding those UTxOs.
//
// Each spent output carries the same datum by hash and each input gets its own
// spend redeemer, so a transaction built from two PlutusV1 scripts produces two
// PlutusV1 redeemers sharing one transaction -- the shape that distinguishes a
// per-transaction TxInfo build from a per-redeemer one. Both validity bounds
// are set, so each TxInfo build costs wantSlotToTimeCallsPerTxInfoBuild
// SlotToTime calls.
func newBabbageMultiRedeemerTx(
	t *testing.T,
	scripts []lcommon.Script,
) (lcommon.Transaction, *mockLedgerState) {
	t.Helper()
	require.NotEmpty(t, scripts)

	// A plain integer datum. The hash is taken over the same bytes that go
	// into the witness set, so the spent outputs' datum hashes resolve
	// against it and each spend purpose carries a real datum.
	datumCbor, err := cbor.Encode(uint64(42))
	require.NoError(t, err)
	datumHash := lcommon.Blake2b256Hash(datumCbor)

	var inputs []any
	var v1Scripts []any
	var v2Scripts []any
	var redeemers []any
	outputCbors := make([][]byte, 0, len(scripts))
	for idx, tmpScript := range scripts {
		inputHash := make([]byte, 32)
		// #nosec G115 -- idx is bounded by the caller's script count
		inputHash[0] = byte(0xa0 + idx)
		inputs = append(inputs, []any{inputHash, uint64(0)})
		switch s := tmpScript.(type) {
		case lcommon.PlutusV1Script:
			v1Scripts = append(v1Scripts, []byte(s))
		case lcommon.PlutusV2Script:
			v2Scripts = append(v2Scripts, []byte(s))
		default:
			t.Fatalf("unsupported script type %T", tmpScript)
		}
		redeemers = append(redeemers, []any{
			uint64(lcommon.RedeemerTagSpend),
			uint64(idx),
			uint64(42),
			[]any{
				uint64(babbageCacheRedeemerMem),
				uint64(babbageCacheRedeemerCpu),
			},
		})
		outputCbor, err := cbor.Encode(map[uint]any{
			0: scriptAddressBytes(t, tmpScript.Hash()),
			1: uint64(babbageCacheInputValue),
			2: []any{uint64(0), datumHash.Bytes()},
		})
		require.NoError(t, err)
		outputCbors = append(outputCbors, outputCbor)
	}

	witnessSet := map[uint]any{
		4: []any{uint64(42)},
		5: redeemers,
	}
	if len(v1Scripts) > 0 {
		witnessSet[3] = v1Scripts
	}
	if len(v2Scripts) > 0 {
		witnessSet[6] = v2Scripts
	}
	bodyMap := map[uint]any{
		0: inputs,
		1: []any{
			map[uint]any{
				0: scriptAddressBytes(t, scripts[0].Hash()),
				1: uint64(babbageCacheOutputValue),
			},
		},
		2: uint64(babbageCacheTxFee),
		3: uint64(babbageCacheValidityTtl),
		8: uint64(babbageCacheValidityFrom),
	}
	txCbor, err := cbor.Encode([]any{bodyMap, witnessSet, true, nil})
	require.NoError(t, err)
	tx, err := babbage.NewBabbageTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.Len(t, tx.Inputs(), len(scripts))
	require.Len(t, tx.Witnesses().PlutusData(), 1)
	require.Equal(
		t,
		datumHash,
		tx.Witnesses().PlutusData()[0].Hash(),
		"witness datum must hash to the datum hash on the spent outputs",
	)

	ls := newMockLedgerState()
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	for idx, outputCbor := range outputCbors {
		var output babbage.BabbageTransactionOutput
		_, err := cbor.Decode(outputCbor, &output)
		require.NoError(t, err)
		require.NotNil(t, output.DatumHash())
		ls.addUtxo(tx.Inputs()[idx], &output)
	}
	return tx, ls
}

func babbagePlutusV1Script(t *testing.T) lcommon.PlutusV1Script {
	t.Helper()
	return lcommon.PlutusV1Script(alwaysSucceedsScriptBytes(t))
}

func babbagePlutusV2Script(t *testing.T) lcommon.PlutusV2Script {
	t.Helper()
	return lcommon.PlutusV2Script(alwaysSucceedsScriptBytes(t))
}

// Babbage's redeemer loop ranges over the PlutusV2 TxInfo's Redeemers, so a
// transaction carrying any redeemer at all builds the V2 TxInfo once before the
// loop. A PlutusV1 redeemer then needs the V1 TxInfo, whose script context
// differs (V1 has no reference inputs or inline datums) and so cannot be
// substituted. Two builds per transaction is therefore the contract these tests
// pin, and the number that must not grow with the redeemer count.
const wantBabbageTxInfoBuilds = 2

// The Babbage TxInfo cache's PlutusV1 half has no coverage from the
// preview-fixture tests: that fixture carries a single PlutusV2 redeemer, so
// TestEvaluateTxBabbagePreviewBuildsTxInfoOnce and its Validate counterpart
// never call txInfos.v1(). A regression that rebuilt the TxInfo per PlutusV1
// redeemer -- the exact shape the cache removed -- would leave them green.
//
// Each case varies only the number and language of the redeemers, and every one
// asserts the same total. That is the property under test: the SlotToTime count
// is a function of the transaction, not of its redeemers. Rebuilding per
// redeemer would make the three cases read 6, 8 and 10 calls respectively.
func babbageTxInfoCacheCases(t *testing.T) []struct {
	name    string
	scripts []lcommon.Script
} {
	t.Helper()
	v1 := babbagePlutusV1Script(t)
	v2 := babbagePlutusV2Script(t)
	return []struct {
		name    string
		scripts []lcommon.Script
	}{
		{name: "two PlutusV1 redeemers", scripts: []lcommon.Script{v1, v1}},
		{
			name:    "three PlutusV1 redeemers",
			scripts: []lcommon.Script{v1, v1, v1},
		},
		{
			name:    "mixed PlutusV1 and PlutusV2 redeemers",
			scripts: []lcommon.Script{v1, v1, v2, v2},
		},
	}
}

func TestEvaluateTxBabbageBuildsTxInfoOncePerLanguage(t *testing.T) {
	for _, testCase := range babbageTxInfoCacheCases(t) {
		t.Run(testCase.name, func(t *testing.T) {
			tx, ls := newBabbageMultiRedeemerTx(t, testCase.scripts)

			_, _, redeemerExUnits, err := EvaluateTxBabbage(
				tx,
				ls,
				previewBabbageProtocolParams(t),
			)
			require.NoError(t, err)
			require.Len(
				t,
				redeemerExUnits,
				len(testCase.scripts),
				"every redeemer must be evaluated",
			)
			assert.Equal(
				t,
				wantBabbageTxInfoBuilds*wantSlotToTimeCallsPerTxInfoBuild,
				ls.slotToTimeCalls,
				"TxInfo must be built once per language per transaction, not once per redeemer",
			)
		})
	}
}

func TestValidateTxBabbageBuildsTxInfoOncePerLanguage(t *testing.T) {
	for _, testCase := range babbageTxInfoCacheCases(t) {
		t.Run(testCase.name, func(t *testing.T) {
			withoutBabbageUtxoValidationRules(t)
			tx, ls := newBabbageMultiRedeemerTx(t, testCase.scripts)

			require.NoError(t, ValidateTxBabbage(
				tx,
				babbageCacheValidityFrom,
				ls,
				previewBabbageProtocolParams(t),
			))
			assert.Equal(
				t,
				wantBabbageTxInfoBuilds*wantSlotToTimeCallsPerTxInfoBuild,
				ls.slotToTimeCalls,
				"TxInfo must be built once per language per transaction, not once per redeemer",
			)
		})
	}
}
