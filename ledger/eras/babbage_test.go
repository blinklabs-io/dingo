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
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Preview protocol parameters at slot 41098839 (epoch 475), from the chain's
// own epoch parameters for that epoch.
const (
	previewBabbageTxFile     = "preview-babbage-tx-41098839.cbor"
	previewBabbageInputsFile = "preview-babbage-inputs-41098839.cbor"
	previewBabbageCostModels = "preview-costmodels-pv8.json"

	previewBabbageTxId = "eab27325c569121613728db87a4d8333ce74cc857bbe4991875ceab8787d0213"

	previewBabbagePlutusV1Params = 166
	previewBabbagePlutusV2Params = 175
	previewBabbageProtoMajor     = 8
	previewBabbageProtoMinor     = 0
	previewBabbageMaxTxExMem     = 14_000_000
	previewBabbageMaxTxExSteps   = 10_000_000_000

	// Preview has a one-second slot from genesis, so POSIX time is the slot
	// plus the system start. The script context carries the transaction's
	// validity range as POSIX milliseconds, so a placeholder conversion would
	// not reproduce the bytes the producer's evaluator saw.
	previewSystemStart = 1_666_656_000
)

// previewBabbageFundingTxIds are the transactions that funded the two spent
// inputs and the two reference inputs, in fixture order. The last one carries
// the 6193-byte PlutusV2 script 87b8d92f92af4c5482452d4625e88b86a0b1289c02e6f23e63c1a2f7
// as a reference script.
var previewBabbageFundingTxIds = []string{
	"b397db253225de00a016c6562361398cb4e702305b35f7699a79f155c41a7214",
	"20f9a5a89ed5da223f992427733c6fbe6e44cf4a35f48ead39b1e8366cd92d94",
	"a4ac5522165d75cc19f11ae3b0a07e1f1adff11227373e54feca0cd50a972645",
	"5bfd1a40780d575afc715480d8b35e45f90598cf52ce9f6eef319a797f28a350",
}

func readPreviewBabbageFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return raw
}

func previewBabbageProtocolParams(t *testing.T) *babbage.BabbageProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV1 []int64 `json:"PlutusV1"`
		PlutusV2 []int64 `json:"PlutusV2"`
	}
	require.NoError(t, json.Unmarshal(
		readPreviewBabbageFixture(t, previewBabbageCostModels),
		&costModels,
	))
	require.Len(t, costModels.PlutusV1, previewBabbagePlutusV1Params)
	require.Len(t, costModels.PlutusV2, previewBabbagePlutusV2Params)
	return &babbage.BabbageProtocolParameters{
		ProtocolMajor: previewBabbageProtoMajor,
		ProtocolMinor: previewBabbageProtoMinor,
		CostModels: map[uint][]int64{
			0: costModels.PlutusV1,
			1: costModels.PlutusV2,
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: previewBabbageMaxTxExMem,
			Steps:  previewBabbageMaxTxExSteps,
		},
	}
}

// TestEvaluateTxBabbagePreviewDuplicateRequiredSigner pins the execution units
// of preview transaction eab27325... (slot 41098839, epoch 475, protocol
// version 8) against the budget its producer declared. The transaction body
// lists the same required signer hash twice. cardano-ledger holds
// reqSignerHashes as a Set and renders txInfoSignatories with Set.toList, so
// the reference evaluator sees one signatory; rendering both makes the spending
// validator take 621 extra CEK steps and 16 extra builtin calls, for 16359467
// CPU and 62240 memory over the declared budget, which rejects a block the
// network accepted and wedges a preview replay. See.
//
// The declared budget is an external oracle: cardano-node computed it with the
// reference evaluator. Equality in both directions catches an overcharge, which
// rejects a canonical block, and an undercharge, which accepts a transaction
// the network rejects.
func TestEvaluateTxBabbagePreviewDuplicateRequiredSigner(t *testing.T) {
	tx, err := babbage.NewBabbageTransactionFromCbor(
		readPreviewBabbageFixture(t, previewBabbageTxFile),
	)
	require.NoError(t, err)
	require.Equal(t, previewBabbageTxId, tx.Hash().String())
	require.True(t, tx.IsValid())
	require.Len(t, tx.RequiredSigners(), 2)
	require.Equal(
		t,
		tx.RequiredSigners()[0].String(),
		tx.RequiredSigners()[1].String(),
		"fixture must keep the duplicated required signer",
	)

	var inputTxBytes [][]byte
	_, err = cbor.Decode(
		readPreviewBabbageFixture(t, previewBabbageInputsFile),
		&inputTxBytes,
	)
	require.NoError(t, err)
	require.Len(t, inputTxBytes, len(previewBabbageFundingTxIds))

	ls := newMockLedgerState()
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	ls.slotToTime = func(slot uint64) (time.Time, error) {
		return time.Unix(int64(slot)+previewSystemStart, 0).UTC(), nil
	}
	for idx, raw := range inputTxBytes {
		inputTx, err := babbage.NewBabbageTransactionFromCbor(raw)
		require.NoError(t, err)
		require.Equal(
			t,
			previewBabbageFundingTxIds[idx],
			inputTx.Hash().String(),
		)
		for outputIdx, output := range inputTx.Outputs() {
			input := shelley.NewShelleyTransactionInput(
				inputTx.Hash().String(),
				outputIdx,
			)
			ls.addUtxo(&input, output)
		}
	}

	_, _, redeemerExUnits, err := EvaluateTxBabbage(
		tx,
		ls,
		previewBabbageProtocolParams(t),
	)
	require.NoError(t, err)

	declared := map[lcommon.RedeemerKey]lcommon.ExUnits{}
	for key, value := range tx.Witnesses().Redeemers().Iter() {
		declared[key] = value.ExUnits
	}
	require.Equal(
		t,
		map[lcommon.RedeemerKey]lcommon.ExUnits{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
				Memory: 3_389_879,
				Steps:  908_604_883,
			},
		},
		declared,
		"fixture must carry the producer-declared budget",
	)
	require.Equal(
		t,
		declared,
		redeemerExUnits,
		"evaluated execution units must equal the "+
			"producer-declared budget exactly",
	)
}

// loadPreviewBabbageFixture decodes the shared preview-babbage-tx-41098839.cbor
// fixture and its funding transactions into a ready-to-use mock ledger state,
// factored out of TestEvaluateTxBabbagePreviewDuplicateRequiredSigner so the
// TxInfo-cache tests below can reuse the same real transaction without
// re-deriving its inputs.
func loadPreviewBabbageFixture(
	t *testing.T,
) (lcommon.Transaction, *mockLedgerState) {
	t.Helper()
	tx, err := babbage.NewBabbageTransactionFromCbor(
		readPreviewBabbageFixture(t, previewBabbageTxFile),
	)
	require.NoError(t, err)

	var inputTxBytes [][]byte
	_, err = cbor.Decode(
		readPreviewBabbageFixture(t, previewBabbageInputsFile),
		&inputTxBytes,
	)
	require.NoError(t, err)
	require.Len(t, inputTxBytes, len(previewBabbageFundingTxIds))

	ls := newMockLedgerState()
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	ls.slotToTime = func(slot uint64) (time.Time, error) {
		return time.Unix(int64(slot)+previewSystemStart, 0).UTC(), nil
	}
	for idx, raw := range inputTxBytes {
		inputTx, err := babbage.NewBabbageTransactionFromCbor(raw)
		require.NoError(t, err)
		require.Equal(
			t,
			previewBabbageFundingTxIds[idx],
			inputTx.Hash().String(),
		)
		for outputIdx, output := range inputTx.Outputs() {
			input := shelley.NewShelleyTransactionInput(
				inputTx.Hash().String(),
				outputIdx,
			)
			ls.addUtxo(&input, output)
		}
	}
	return tx, ls
}

// wantSlotToTimeCallsPerTxInfoBuild is the fixture's own validity interval
// shape: both a lower and an upper bound are present, so validityRangeInfo
// (called once per TxInfo build) calls SlotToTime twice.
const wantSlotToTimeCallsPerTxInfoBuild = 2

// TestEvaluateTxBabbagePreviewBuildsTxInfoOnce and
// TestValidateTxBabbagePreviewBuildsTxInfoOnce pin the fix for the babbage
// redeemer loop rebuilding its Plutus TxInfo (and re-translating the
// transaction's validity interval through SlotToTime) once per redeemer
// instead of once per transaction -- the same class of redundant,
// per-invocation rebuild that cachedEvalContext already fixed for the cost
// model side of script evaluation. This fixture carries exactly one redeemer
// (see TestEvaluateTxBabbagePreviewDuplicateRequiredSigner's declared-budget
// assertion), so even here -- the best case for the old code -- TxInfo was
// still built twice: once outside the loop merely to read .Redeemers, and
// again inside the loop's PlutusV2 case, for 2x wantSlotToTimeCallsPerTxInfoBuild
// SlotToTime calls. A transaction with more redeemers of the same language
// version would have paid for one extra rebuild per redeemer instead of a
// single fixed extra.
func TestEvaluateTxBabbagePreviewBuildsTxInfoOnce(t *testing.T) {
	tx, ls := loadPreviewBabbageFixture(t)

	_, _, _, err := EvaluateTxBabbage(
		tx,
		ls,
		previewBabbageProtocolParams(t),
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		wantSlotToTimeCallsPerTxInfoBuild,
		ls.slotToTimeCalls,
		"TxInfo must be built exactly once per transaction, not once per redeemer",
	)
}

func TestValidateTxBabbagePreviewBuildsTxInfoOnce(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)
	tx, ls := loadPreviewBabbageFixture(t)

	err := ValidateTxBabbage(
		tx,
		41_098_839,
		ls,
		previewBabbageProtocolParams(t),
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		wantSlotToTimeCallsPerTxInfoBuild,
		ls.slotToTimeCalls,
		"TxInfo must be built exactly once per transaction, not once per redeemer",
	)
}

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

// A redeemerless transaction whose TTL is inside the horizon behaves the same
// either way, so the gate cannot be hiding a translation that used to succeed.
func TestValidateTxBabbageWithoutRedeemersInsideHorizon(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot+100, map[uint]any{}),
	)
	require.NoError(t, err)

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	require.NoError(t, ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	))
}

func TestEvaluateTxBabbageSkipsScriptContextWithoutRedeemers(t *testing.T) {
	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, map[uint]any{}),
	)
	require.NoError(t, err)

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	_, exUnits, redeemerExUnits, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{}, exUnits)
	assert.Empty(t, redeemerExUnits)
	assert.Zero(t, ls.slotToTimeCalls)
}

type declaredValidityConwayTx struct {
	*mockConwayFeeTx
	valid bool
}

type validityOutcomeRedeemers struct {
	*mockRedeemers
}

func newConwayValidityOutcomeTx(
	t *testing.T,
	valid bool,
	version lang.LanguageVersion,
	scriptFails bool,
	exUnits lcommon.ExUnits,
) *declaredValidityConwayTx {
	t.Helper()
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
	if scriptFails {
		// This malformed Flat payload reaches the evaluator rather than the
		// budget check, exercising the execution-error outcome path.
		scriptBytes = []byte{0x41, 0x00}
	}

	var script lcommon.Script
	witnesses := &mockWitnessSet{redeemers: &validityOutcomeRedeemers{
		mockRedeemers: &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagMint,
						Index: 0,
					},
					val: lcommon.RedeemerValue{ExUnits: exUnits},
				},
			},
		},
	}}
	switch version {
	case lang.LanguageVersionV1:
		plutusScript := lcommon.PlutusV1Script(scriptBytes)
		script = plutusScript
		witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{plutusScript}
	case lang.LanguageVersionV2:
		plutusScript := lcommon.PlutusV2Script(scriptBytes)
		script = plutusScript
		witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{plutusScript}
	default:
		t.Fatalf("unsupported Plutus version %v", version)
	}
	scriptHash := script.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	return &declaredValidityConwayTx{
		valid: valid,
		mockConwayFeeTx: &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType:    txTypeAlonzo,
				witnesses: witnesses,
			},
			assetMint: &assetMint,
		},
	}
}

// TestValidateTxBabbageRejectsPlutusV2WhenSynthetic covers:
// real cardano-ledger rejects a transaction using a PlutusV2 script outright,
// at the UTXOW level before any script evaluation runs, whenever PlutusV2 has
// no real cost model configured yet (NoCostModel, the formal rule "languages
// txw ⊆ dom(costmdls pp)"). Dingo's HardForkBabbage instead fabricates a
// value specifically so internal validation always has one, which -- absent
// this check -- would let Dingo accept the same transaction a real network
// rejects.
func TestValidateTxBabbageRejectsPlutusV2WhenSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic covers the common case
// once real data exists (or on a database predating this tracking, where the
// bootstrap fallback resolves to false for a real, non-default value): the
// same PlutusV2 transaction must validate exactly as it did before this
// check existed.
func TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	// syntheticV2CostModel left at its zero value (false).
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			// A real (non-synthetic) PlutusV2 cost model, same as production
			// carries once an on-chain update lands. requiredCostModel
			// fails closed on a missing entry, so this must be
			// populated for the "not synthetic" case to actually reach
			// evaluation instead of being rejected before it ever does.
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// TestValidateTxBabbageAllowsPlutusV2WhenLedgerStateReportsNothing covers the
// fail-safe default: an lcommon.LedgerState implementation that does not
// implement syntheticV2CostModelReporter at all (any caller other than
// ledger.LedgerView's own *ledger.LedgerView) must not be treated as
// synthetic -- this check is additive and must never fire for a caller that
// simply doesn't carry the signal.
func TestValidateTxBabbageAllowsPlutusV2WhenLedgerStateReportsNothing(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		plainMockLedgerState{newMockLedgerState()},
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// plainMockLedgerState embeds the lcommon.LedgerState INTERFACE (not the
// concrete *mockLedgerState type), so method promotion exposes exactly that
// interface's method set and nothing more -- in particular, not
// SyntheticV2CostModelInEffect, even though the concrete value stored in it
// (a *mockLedgerState) happens to have that extra method. This stands in for
// any lcommon.LedgerState implementation other than *ledger.LedgerView,
// which is the only production type this check's type assertion expects to
// see.
type plainMockLedgerState struct {
	lcommon.LedgerState
}

// TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic covers the fee/ex-units
// estimation counterpart: a transaction ValidateTxBabbage would reject must
// not be quoted a fee estimate implying it's valid.
func TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic mirrors
// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic for EvaluateTxBabbage.
func TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}
