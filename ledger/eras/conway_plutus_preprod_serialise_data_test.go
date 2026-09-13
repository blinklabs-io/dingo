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
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

const (
	preprodSerialiseDataTxFile     = "preprod-conway-tx-133016611.cbor"
	preprodSerialiseDataInputsFile = "preprod-conway-inputs-133016611.cbor"

	preprodSerialiseDataTxId = "2c528f4e28e2b8fd47539fe51eca8b7dafced3d1b82f48edf4a57e075b525ff5"

	// The policy this transaction mints under. It names its asset
	// blake2b_256(serialiseData(the seed TxOutRef carried in the redeemer)).
	preprodSerialiseDataPolicy = "22f3deb8008f5843205e0bd52f912bd3ee546238c95cfeae94bf7edd"

	// The asset name the chain actually minted: the hash of the
	// INDEFINITE-length encoding of that TxOutRef, which is what the Plutus
	// reference encoder writes. The redeemer is definite-encoded on the wire,
	// so rendering the wire encoding into the script context makes the policy
	// compute 5e13e57434d51b2bf8a3693608a308936b206485ca9bc405dbee44f6af21668b
	// instead, and call error.
	preprodSerialiseDataAssetName = "c714816533babf58a158870eaa49db187e1e1f83f67579070d2025974129a858"
)

// preprodSerialiseDataFundingTxIds funded the three spent inputs and the two
// reference inputs, in fixture order. One reference input carries the PlutusV3
// minting policy as a reference script, the other the config datum it reads.
var preprodSerialiseDataFundingTxIds = []string{
	"3fd10d2c06901f5440c23f1dd757c8ed61679a064e5975cbf2e2b571fd3da0d5",
	"edb451ce575ceaa1190189b91afe67d197460f1dc4f0c67f2fc32ee208cd810d",
	"eca0c28d10de6e36c7c671fd6260713f845d41c5430befefa529f03902658304",
	"100b74c93a22172ed2fa7ad8a4a4b6d72b2538f14a19414f8684c941cea75b6f",
}

// TestEvaluateTxConwayPreprodSerialiseData replays preprod transaction
// 2c528f4e... (slot 133016611, epoch 311, protocol version 11), whose minting
// policy names its asset after blake2b_256(serialiseData(seed TxOutRef)).
//
// cardano-ledger rebuilds every script-visible value rather than carrying the
// transaction CBOR into the script context, so a script always observes the
// indefinite-length field list the Plutus encoder writes. Passing the
// definite-length wire encoding through instead changes what serialiseData
// returns, so the policy computed a different asset name than the network
// minted, called error, and wedged a preprod replay at this block. See
// blinklabs-io/dingo#3860.
func TestEvaluateTxConwayPreprodSerialiseData(t *testing.T) {
	t.Parallel()
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, preprodSerialiseDataTxFile),
	)
	require.NoError(t, err)
	require.Equal(t, preprodSerialiseDataTxId, tx.Hash().String())
	require.True(t, tx.IsValid())

	mint := tx.AssetMint()
	require.NotNil(t, mint)
	policies := mint.Policies()
	require.Len(t, policies, 1)
	require.Equal(t, preprodSerialiseDataPolicy, policies[0].String())
	assetNames := mint.Assets(policies[0])
	require.Len(t, assetNames, 1)
	require.Equal(
		t,
		preprodSerialiseDataAssetName,
		hex.EncodeToString(assetNames[0]),
		"fixture must carry the asset name the network minted",
	)

	var inputTxBytes [][]byte
	_, err = cbor.Decode(
		readErasFixture(t, preprodSerialiseDataInputsFile),
		&inputTxBytes,
	)
	require.NoError(t, err)
	require.Len(t, inputTxBytes, len(preprodSerialiseDataFundingTxIds))

	ls := preprodLedgerState{mockLedgerState: newMockLedgerState()}
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	for idx, raw := range inputTxBytes {
		inputTx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(t, err)
		require.Equal(
			t,
			preprodSerialiseDataFundingTxIds[idx],
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

	_, _, redeemerExUnits, err := EvaluateTxConway(
		tx,
		ls,
		preprodFixtureProtocolParams(t),
	)
	require.NoError(
		t,
		err,
		"the network accepted this transaction, so its minting policy must succeed",
	)

	declared := map[lcommon.RedeemerKey]lcommon.ExUnits{}
	for key, value := range tx.Witnesses().Redeemers().Iter() {
		declared[key] = value.ExUnits
	}
	require.Len(t, declared, 1)
	require.Len(t, redeemerExUnits, 1)
	for key, used := range redeemerExUnits {
		budget, ok := declared[key]
		require.True(
			t,
			ok,
			"evaluated a redeemer the transaction does not declare",
		)
		require.LessOrEqual(t, used.Steps, budget.Steps)
		require.LessOrEqual(t, used.Memory, budget.Memory)
	}

	// Block validation constructs its own redeemer rather than using the
	// evaluator's TxInfo. Exercise that entry point with the same wire data.
	require.NoError(t, ValidateTxPlutusConway(
		tx,
		133016611,
		ls,
		preprodFixtureProtocolParams(t),
	))
}
