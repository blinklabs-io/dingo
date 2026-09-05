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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// previewOracleBlockContext is the off-chain state a canonical preview block
// needs before its transaction can be re-validated: the resolved inputs and
// reference inputs, the era's cost models and execution unit ceiling, and the
// network's slot-to-time mapping.
type previewOracleBlockContext struct {
	SystemStartUnix int64  `json:"systemStartUnix"`
	ProtocolVersion []uint `json:"protocolVersion"`
	MaxTxExUnits    struct {
		Memory int64 `json:"memory"`
		Steps  int64 `json:"steps"`
	} `json:"maxTxExUnits"`
	CostModels    map[string][]int64 `json:"costModels"`
	ResolvedUtxos map[string]string  `json:"resolvedUtxos"`
}

// TestValidateTxPlutusConwayPreviewWithdrawalOracle re-validates preview block
// 4625519 (slot 121707875), whose single transaction
// e8691d7c4003815928bc9de9017a3388d7fc0a168383c08d14633732a7c0bb33 carries a
// Plutus V3 withdrawal oracle validator that verifies an Ed25519 signature over
// serialiseData of a payload containing integers with 101-byte magnitudes. A
// serialiseData encoding that does not chunk a bignum magnitude makes that
// validator return "error explicitly called", which rejects a canonical block
// and freezes the tip. See blinklabs-io/dingo#3780.
func TestValidateTxPlutusConwayPreviewWithdrawalOracle(t *testing.T) {
	raw, err := os.ReadFile(
		filepath.Join("testdata", "preview-block-121707875.cbor"),
	)
	require.NoError(t, err)
	blk, err := gledger.NewBlockFromCbor(conway.BlockTypeConway, raw)
	require.NoError(t, err)
	txs := blk.Transactions()
	require.Len(t, txs, 1)
	tx := txs[0]
	require.Equal(
		t,
		"e8691d7c4003815928bc9de9017a3388d7fc0a168383c08d14633732a7c0bb33",
		tx.Hash().String(),
	)
	require.True(t, tx.IsValid())

	ctxRaw, err := os.ReadFile(
		filepath.Join("testdata", "preview-block-121707875-context.json"),
	)
	require.NoError(t, err)
	var blockCtx previewOracleBlockContext
	require.NoError(t, json.Unmarshal(ctxRaw, &blockCtx))
	require.Len(t, blockCtx.ProtocolVersion, 2)

	ls := newMockLedgerState()
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	systemStart := blockCtx.SystemStartUnix
	ls.slotToTime = func(slot uint64) (time.Time, error) {
		return time.Unix(int64(slot)+systemStart, 0).UTC(), nil
	}
	for ref, outputHex := range blockCtx.ResolvedUtxos {
		txId, idx, found := strings.Cut(ref, "#")
		require.True(t, found, "malformed utxo reference %q", ref)
		outputIdx, err := strconv.Atoi(idx)
		require.NoError(t, err)
		outputCbor, err := hex.DecodeString(outputHex)
		require.NoError(t, err)
		var output babbage.BabbageTransactionOutput
		_, err = cbor.Decode(outputCbor, &output)
		require.NoError(t, err, "decode output %s", ref)
		input := shelley.NewShelleyTransactionInput(txId, outputIdx)
		ls.addUtxo(&input, &output)
	}
	require.Len(t, ls.utxos, len(tx.Inputs())+len(tx.ReferenceInputs()))

	costModels := make(map[uint][]int64, len(blockCtx.CostModels))
	for language, model := range blockCtx.CostModels {
		languageId, err := strconv.ParseUint(language, 10, 8)
		require.NoError(t, err)
		costModels[uint(languageId)] = model
	}
	pp := &conway.ConwayProtocolParameters{
		CostModels: costModels,
		MaxTxExUnits: lcommon.ExUnits{
			Memory: blockCtx.MaxTxExUnits.Memory,
			Steps:  blockCtx.MaxTxExUnits.Steps,
		},
	}
	pp.ProtocolVersion.Major = blockCtx.ProtocolVersion[0]
	pp.ProtocolVersion.Minor = blockCtx.ProtocolVersion[1]

	// The network accepted this block, so every script in it must succeed
	// within its declared execution budget.
	require.NoError(t, ValidateTxPlutusConway(tx, blk.SlotNumber(), ls, pp))

	// A correct serialiseData encoding must still reject a payload the oracle
	// key did not sign, so flipping one bit of the redeemer's signature has to
	// fail the same validator.
	tamperedTxCbor := make([]byte, len(tx.Cbor()))
	copy(tamperedTxCbor, tx.Cbor())
	sigOffset := bytes.Index(tamperedTxCbor, oracleSignature)
	require.NotEqual(t, -1, sigOffset, "oracle signature not found in tx")
	tamperedTxCbor[sigOffset] ^= 0x01
	tamperedTx, err := conway.NewConwayTransactionFromCbor(tamperedTxCbor)
	require.NoError(t, err)
	err = ValidateTxPlutusConway(tamperedTx, blk.SlotNumber(), ls, pp)
	require.ErrorContains(
		t,
		err,
		"plutus script failed (hash=473da51f9b910d257655e18d57ee6454ee345cb8d674d120ffd8f9c1, tag=3, index=0): execute script: error explicitly called",
	)
}

// oracleSignature is the Ed25519 signature carried by the reward redeemer at
// index 0 of the fixture block's transaction.
var oracleSignature = mustDecodeHex(
	"cfe0ef14f622ba7da09f40f815e61649437e2f1aa3d03567fde5a7c897adb693" +
		"87aa55dab9d23b7b42a2c8ee63f1bf997da7672166582865d12dc44515733d07",
)

func mustDecodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
