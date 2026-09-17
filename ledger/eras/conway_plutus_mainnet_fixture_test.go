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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

// Mainnet parameters for the fixture's epoch. The transaction is in epoch 653;
// epochs 652 and 653 carry an identical protocol version and cost model, so one
// parameter file serves the block and its inputs.
const (
	mainnetFixtureTxFile    = "mainnet-conway-tx-2d295dbf.cbor"
	mainnetFixtureInputFile = "mainnet-conway-inputs-2d295dbf.cbor"
	mainnetFixtureCostFile  = "mainnet-costmodels-pv11-epoch653.json"
	mainnetFixtureTxId      = "2d295dbffc2898b14ccc9b359a342305006fdcf6ccd7907b444419520a98f351"
	mainnetFixtureSlot      = 196_783_015
	mainnetProtoMajor       = 11
	mainnetProtoMinor       = 0
	mainnetMaxTxExMem       = 16_500_000
	mainnetMaxTxExSteps     = 10_000_000_000

	// Mainnet Shelley genesis: systemStart 1506203091, a Byron prefix of
	// 4492800 20-second slots, 1-second slots afterwards.
	mainnetSystemStart   = 1_506_203_091
	mainnetByronSlots    = 4_492_800
	mainnetByronSlotSecs = 20
)

// mainnetFixtureLedgerState gives the mock ledger state mainnet's real
// slot/time conversion, so the script context carries the same validity
// range in POSIX milliseconds that the block producer's evaluator saw.
type mainnetFixtureLedgerState struct {
	*mockLedgerState
}

func (mainnetFixtureLedgerState) SlotToTime(slot uint64) (time.Time, error) {
	if slot < mainnetByronSlots {
		return time.Unix(
			mainnetSystemStart+int64(slot)*mainnetByronSlotSecs,
			0,
		).UTC(), nil
	}
	byronEnd := int64(mainnetSystemStart) +
		int64(mainnetByronSlots)*mainnetByronSlotSecs
	return time.Unix(byronEnd+int64(slot-mainnetByronSlots), 0).UTC(), nil
}

func (mainnetFixtureLedgerState) TimeToSlot(t time.Time) (uint64, error) {
	byronEnd := int64(mainnetSystemStart) +
		int64(mainnetByronSlots)*mainnetByronSlotSecs
	if t.Unix() < byronEnd {
		return uint64(
			(t.Unix() - mainnetSystemStart) / mainnetByronSlotSecs,
		), nil
	}
	return mainnetByronSlots + uint64(t.Unix()-byronEnd), nil
}

func mainnetFixtureProtocolParams(t *testing.T) *conway.ConwayProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV1 []int64 `json:"PlutusV1"`
		PlutusV2 []int64 `json:"PlutusV2"`
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(t, json.Unmarshal(
		readErasFixture(t, mainnetFixtureCostFile),
		&costModels,
	))
	require.Len(t, costModels.PlutusV3, 350)
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: mainnetProtoMajor,
			Minor: mainnetProtoMinor,
		},
		CostModels: map[uint][]int64{
			0: costModels.PlutusV1,
			1: costModels.PlutusV2,
			2: costModels.PlutusV3,
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: mainnetMaxTxExMem,
			Steps:  mainnetMaxTxExSteps,
		},
	}
}

// mainnetFixtureInputTxs decodes the transactions that funded the fixture
// transaction's inputs and reference inputs.
func mainnetFixtureInputTxs(t *testing.T) []*conway.ConwayTransaction {
	t.Helper()
	var inputTxBytes [][]byte
	_, err := cbor.Decode(
		readErasFixture(t, mainnetFixtureInputFile),
		&inputTxBytes,
	)
	require.NoError(t, err)
	ret := make([]*conway.ConwayTransaction, 0, len(inputTxBytes))
	for _, raw := range inputTxBytes {
		inputTx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(t, err)
		ret = append(ret, inputTx)
	}
	return ret
}

// TestValidateTxPlutusConwayMainnetStorageDecodedUtxos replays a canonical
// mainnet transaction through phase-2 validation with its inputs resolved the
// way the node resolves them: from stored output CBOR re-decoded by
// ledger.NewTransactionOutputFromCbor, which is what database/models.Utxo.Decode
// calls. Resolving inputs from block-decoded outputs instead bypasses that
// decode and cannot observe a rendering that depends on the concrete output
// type it selects.
//
// One of the fixture's inputs uses the legacy array output encoding carrying a
// datum hash, which that decoder resolves to *alonzo.AlonzoTransactionOutput.
// A rendering of that type that drops the datum hash puts NoOutputDatum in the
// PlutusV3 script context, and the transaction's withdrawal validator calls
// Plutus `error` on it, rejecting a block the network accepted. See
// blinklabs-io/dingo#3860 and blinklabs-io/gouroboros#2213.
func TestValidateTxPlutusConwayMainnetStorageDecodedUtxos(t *testing.T) {
	pp := mainnetFixtureProtocolParams(t)
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, mainnetFixtureTxFile),
	)
	require.NoError(t, err)
	require.Equal(t, mainnetFixtureTxId, tx.Hash().String())
	require.True(t, tx.IsValid())

	ls := mainnetFixtureLedgerState{mockLedgerState: newMockLedgerState()}
	ls.networkId = uint(lcommon.AddressNetworkMainnet)
	for _, inputTx := range mainnetFixtureInputTxs(t) {
		for idx, output := range inputTx.Outputs() {
			stored, err := gledger.NewTransactionOutputFromCbor(output.Cbor())
			require.NoError(
				t,
				err,
				"decode stored output %s#%d",
				inputTx.Hash().String(),
				idx,
			)
			ls.addUtxo(
				shelley.NewShelleyTransactionInput(
					inputTx.Hash().String(),
					idx,
				),
				stored,
			)
		}
	}

	// The network accepted this transaction, so every script in it must
	// succeed within its declared execution budget.
	require.NoError(
		t,
		ValidateTxPlutusConway(tx, mainnetFixtureSlot, ls, pp),
	)

	// cardano-node computed the declared budgets with the reference
	// evaluator, so equality in both directions catches an overcharge, which
	// rejects a block the network accepted, and an undercharge, which accepts
	// a transaction the network rejects.
	_, _, redeemerExUnits, err := EvaluateTxConway(tx, ls, pp)
	require.NoError(t, err)
	declared := map[lcommon.RedeemerKey]lcommon.ExUnits{}
	for key, value := range tx.Witnesses().Redeemers().Iter() {
		declared[key] = value.ExUnits
	}
	require.NotEmpty(t, declared)
	require.Equal(
		t,
		declared,
		redeemerExUnits,
		"evaluated execution units must equal the "+
			"producer-declared budget exactly",
	)
}

// TestMainnetFixtureStorageDecodePreservesScriptContextRendering pins the
// invariant the phase-2 replay depends on for every output in the fixture, not
// only the one input whose validator noticed: re-decoding a stored output must
// render the same script-context PlutusData as the block-decoded output it was
// stored from. The concrete type the decoder selects may differ from the
// block's era type; the rendering may not.
func TestMainnetFixtureStorageDecodePreservesScriptContextRendering(t *testing.T) {
	for _, inputTx := range mainnetFixtureInputTxs(t) {
		for idx, output := range inputTx.Outputs() {
			ref := fmt.Sprintf("%s#%d", inputTx.Hash().String(), idx)
			stored, err := gledger.NewTransactionOutputFromCbor(output.Cbor())
			require.NoError(t, err, "decode stored output %s", ref)
			want, err := data.Encode(output.ToPlutusData())
			require.NoError(t, err, "encode block-decoded output %s", ref)
			got, err := data.Encode(stored.ToPlutusData())
			require.NoError(t, err, "encode stored output %s", ref)
			require.Equal(
				t,
				hex.EncodeToString(want),
				hex.EncodeToString(got),
				"stored output %s re-decoded as %T must render the same "+
					"script-context PlutusData as the block-decoded %T",
				ref,
				stored,
				output,
			)
		}
	}
}
