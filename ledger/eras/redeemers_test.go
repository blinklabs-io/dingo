// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturedRedeemer struct {
	key   lcommon.RedeemerKey
	value lcommon.RedeemerValue
}

func TestBabbageDuplicateRedeemersUseLastValue(t *testing.T) {
	tx := newDuplicateBabbageRedeemerTx(t)

	normalizedTx, ok := normalizeDuplicateRedeemers(tx).(*babbage.BabbageTransaction)
	require.True(t, ok)
	assert.Equal(t, tx.Cbor(), normalizedTx.Cbor())
	assert.Equal(
		t,
		tx.WitnessSet.WsRedeemers.Cbor(),
		normalizedTx.WitnessSet.WsRedeemers.Cbor(),
	)
	normalizedRedeemers := normalizedTx.WitnessSet.WsRedeemers
	entries := collectRedeemers(normalizedRedeemers)
	require.Len(t, entries, 1)
	assert.Equal(t, lcommon.RedeemerTagSpend, entries[0].key.Tag)
	assert.Equal(t, uint32(0), entries[0].key.Index)
	assert.Equal(t, []byte{0x03}, entries[0].value.Data.Cbor())
	assert.Equal(
		t,
		lcommon.ExUnits{Memory: 60, Steps: 600},
		entries[0].value.ExUnits,
	)
	assert.Equal(
		t,
		[]uint{0},
		normalizedRedeemers.Indexes(lcommon.RedeemerTagSpend),
	)
	assert.Equal(
		t,
		entries[0].value,
		normalizedRedeemers.Value(0, lcommon.RedeemerTagSpend),
	)

	declared, err := DeclaredExUnits(tx)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{Memory: 60, Steps: 600}, declared)
	pp := &babbage.BabbageProtocolParameters{
		MaxTxExUnits: lcommon.ExUnits{Memory: 60, Steps: 600},
	}
	require.Error(t, babbage.UtxoValidateExUnitsTooBigUtxo(tx, 0, nil, pp))
	require.NoError(
		t,
		babbage.UtxoValidateExUnitsTooBigUtxo(normalizedTx, 0, nil, pp),
	)

	ls := newMockLedgerState()
	resolvedInput := lcommon.Utxo{
		Id:     tx.Inputs()[0],
		Output: newTestOutput(1_000_000),
	}
	txInfo, err := script.NewTxInfoV2FromTransaction(
		ls,
		normalizedTx,
		[]lcommon.Utxo{resolvedInput},
		false,
	)
	require.NoError(t, err)
	require.Len(t, txInfo.Redeemers, 1)
	txInfoRedeemerData, err := data.Encode(txInfo.Redeemers[0].Value.Data)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x03}, txInfoRedeemerData)
}

func TestValidateTxBabbageDuplicateRedeemersUseLastBudget(t *testing.T) {
	originalRules := babbageUtxoValidationRules
	t.Cleanup(func() {
		babbageUtxoValidationRules = originalRules
	})
	babbageUtxoValidationRules = []indexedUtxoValidationRule{
		{
			index:          0,
			validationFunc: babbage.UtxoValidateExUnitsTooBigUtxo,
		},
	}

	ls := newMockLedgerState()
	ls.skipPhase2Validation = true
	require.NoError(
		t,
		ValidateTxBabbage(
			newDuplicateBabbageRedeemerTx(t),
			0,
			ls,
			&babbage.BabbageProtocolParameters{
				MaxTxExUnits: lcommon.ExUnits{Memory: 60, Steps: 600},
			},
		),
	)
}

func TestEvaluateTxBabbageDuplicateRedeemersRunsOnce(t *testing.T) {
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
	plutusScript := lcommon.PlutusV2Script(scriptBytes)

	key := lcommon.RedeemerKey{
		Tag:   lcommon.RedeemerTagSpend,
		Index: 0,
	}
	value := lcommon.RedeemerValue{
		ExUnits: lcommon.ExUnits{Memory: 1_000_000, Steps: 1_000_000},
	}
	witnesses := &mockWitnessSet{
		plutusV2Scripts: []lcommon.PlutusV2Script{plutusScript},
		redeemers: &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{key: key, val: value},
				{key: key, val: value},
				{key: key, val: value},
				{key: key, val: value},
				{key: key, val: value},
				{key: key, val: value},
			},
		},
	}
	spendInput := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType:    txTypeAlonzo,
			witnesses: witnesses,
		},
		inputs: []lcommon.TransactionInput{spendInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(
		spendInput,
		testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       newTestScriptAddress(t, plutusScript),
		},
	)

	_, totalExUnits, perRedeemer, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			CostModels: map[uint][]int64{
				1: DefaultPlutusV2CostModel,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Memory: 1_000_000,
				Steps:  1_000_000,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{Memory: 800, Steps: 161_100}, totalExUnits)
	require.Len(t, perRedeemer, 1)
	assert.Equal(
		t,
		lcommon.ExUnits{Memory: 800, Steps: 161_100},
		perRedeemer[key],
	)
}

func TestNormalizeDuplicateRedeemersPreservesDistinctPointers(t *testing.T) {
	witnessSet := map[uint]any{
		5: []any{
			testRedeemer(0, 1, 2, 20, 200),
			testRedeemer(0, 0, 1, 10, 100),
		},
	}
	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot, witnessSet),
	)
	require.NoError(t, err)

	normalizedTx, ok := normalizeDuplicateRedeemers(tx).(*babbage.BabbageTransaction)
	require.True(t, ok)
	assert.Same(t, tx, normalizedTx)
	entries := collectRedeemers(normalizedTx.WitnessSet.WsRedeemers)
	require.Len(t, entries, 2)
	assert.Equal(t, uint32(0), entries[0].key.Index)
	assert.Equal(t, []byte{0x01}, entries[0].value.Data.Cbor())
	assert.Equal(t, uint32(1), entries[1].key.Index)
	assert.Equal(t, []byte{0x02}, entries[1].value.Data.Cbor())

	declared, err := DeclaredExUnits(tx)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{Memory: 30, Steps: 300}, declared)
}

func collectRedeemers(
	redeemers lcommon.TransactionWitnessRedeemers,
) []capturedRedeemer {
	if redeemers == nil {
		return nil
	}
	ret := make([]capturedRedeemer, 0)
	for key, value := range redeemers.Iter() {
		ret = append(ret, capturedRedeemer{key: key, value: value})
	}
	return ret
}

func newDuplicateBabbageRedeemerTx(t *testing.T) *babbage.BabbageTransaction {
	t.Helper()
	witnessSet := map[uint]any{
		5: []any{
			testRedeemer(0, 0, 1, 10, 100),
			testRedeemer(0, 0, 1, 20, 200),
			testRedeemer(0, 0, 2, 30, 300),
			testRedeemer(0, 0, 2, 40, 400),
			testRedeemer(0, 0, 3, 50, 500),
			testRedeemer(0, 0, 3, 60, 600),
		},
	}
	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot, witnessSet),
	)
	require.NoError(t, err)
	return tx
}

func testRedeemer(
	tag uint64,
	index uint64,
	redeemerData uint64,
	memory uint64,
	steps uint64,
) []any {
	return []any{tag, index, redeemerData, []any{memory, steps}}
}
