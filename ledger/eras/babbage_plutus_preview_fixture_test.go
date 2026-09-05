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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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
// network accepted and wedges a preview replay. See blinklabs-io/dingo#3935.
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
