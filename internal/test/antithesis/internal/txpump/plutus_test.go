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

package txpump

import (
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sampleScriptHash = make([]byte, 28)

// sampleCostModel stands in for the protocol PlutusV3 cost model; the script
// data hash only needs to commit to whatever values are supplied.
var sampleCostModel = make([]int64, 251)

func init() {
	for i := range sampleScriptHash {
		sampleScriptHash[i] = byte(i + 0x40)
	}
	for i := range sampleCostModel {
		sampleCostModel[i] = int64(i * 7)
	}
}

func samplePlutusInputs() []UTxO {
	return []UTxO{
		{TxHash: sampleHash, Index: 0, Amount: 10_000_000},
	}
}

// TestPumpTakeLockedPlutusUTxOSkipsQuarantinedOutput verifies that an available
// script output can be selected while an unconfirmed script output remains
// quarantined for a later round.
func TestPumpTakeLockedPlutusUTxOSkipsQuarantinedOutput(t *testing.T) {
	pump := &Pump{}
	pump.addLockedPlutusUTxO(UTxO{
		TxHash:      "pending",
		availableAt: time.Now().Add(time.Hour),
	})
	pump.addLockedPlutusUTxO(UTxO{TxHash: "available"})

	utxo, ok := pump.takeLockedPlutusUTxO()
	require.True(t, ok)
	require.Equal(t, "available", utxo.TxHash)
	_, ok = pump.takeLockedPlutusUTxO()
	require.False(t, ok)
}

func TestAlwaysSucceedsScriptAcceptsAnyContextWithinBudget(t *testing.T) {
	script := common.PlutusV3Script(alwaysSucceedsScript())
	evalContext := cek.NewDefaultEvalContext(
		cek.LanguageVersionV3, cek.ProtoVersion{Major: 10},
	)
	for _, ctx := range []data.PlutusData{
		data.NewInteger(big.NewInt(0)),
		data.NewConstr(0, data.NewByteString([]byte{0x01})),
	} {
		used, err := script.Evaluate(ctx, plutusUnlockExUnits, evalContext)
		require.NoError(t, err)
		require.LessOrEqual(t, used.Memory, plutusUnlockExUnits.Memory)
		require.LessOrEqual(t, used.Steps, plutusUnlockExUnits.Steps)
	}
}

func TestAlwaysSucceedsScriptHashIsLedgerHash(t *testing.T) {
	want := common.Blake2b224Hash(append([]byte{common.ScriptRefTypePlutusV3}, alwaysSucceedsScript()...))
	require.Equal(t, want.Bytes(), alwaysSucceedsScriptHash())
	addr := scriptAddressFromHash(alwaysSucceedsScriptHash())
	require.Equal(t, byte(0x70), addr[0])
	require.Equal(t, alwaysSucceedsScriptHash(), addr[1:])
}

// ---- PlutusLock tests ----

func TestBuildPlutusLockTx_LocksWithInlineDatumAndSigns(t *testing.T) {
	key := testSigningKey(0x88)
	inputs := []UTxO{{TxHash: sampleHash, Amount: 10_000_000, SigningKey: key}}
	txBytes, err := BuildPlutusLockTx(
		inputs, alwaysSucceedsScriptHash(), plutusLockAmount, MinFee, sampleAddr,
	)
	require.NoError(t, err)
	tx := requireSignedBy(t, txBytes, key)

	outputs := tx.Outputs()
	require.Len(t, outputs, 2)
	addr, err := outputs[0].Address().Bytes()
	require.NoError(t, err)
	require.Equal(t, scriptAddressFromHash(alwaysSucceedsScriptHash()), addr)
	require.Equal(t, plutusLockAmount, outputs[0].Amount().Uint64())
	require.NotNil(t, outputs[0].Datum(), "script output must carry an inline datum")
	require.Equal(t, uint64(10_000_000)-plutusLockAmount-MinFee, outputs[1].Amount().Uint64())
}

func TestBuildPlutusLockTx_NoInputs(t *testing.T) {
	_, err := BuildPlutusLockTx(nil, sampleScriptHash, minSendAmount, MinFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusLockTx_EmptyScriptHash(t *testing.T) {
	_, err := BuildPlutusLockTx(samplePlutusInputs(), []byte{}, minSendAmount, MinFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusLockTx_AmountBelowMinimum(t *testing.T) {
	_, err := BuildPlutusLockTx(samplePlutusInputs(), sampleScriptHash, minSendAmount-1, MinFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusLockTx_InvalidTxHash(t *testing.T) {
	_, err := BuildPlutusLockTx(
		[]UTxO{{TxHash: "not-hex", Amount: 10_000_000}},
		sampleScriptHash, minSendAmount, MinFee, sampleAddr,
	)
	require.Error(t, err)
	_, err = BuildPlutusLockTx(
		[]UTxO{{TxHash: "abcd", Amount: 10_000_000}},
		sampleScriptHash, minSendAmount, MinFee, sampleAddr,
	)
	require.Error(t, err)
}

func TestBuildPlutusLockTx_InsufficientInputs(t *testing.T) {
	_, err := BuildPlutusLockTx(
		[]UTxO{{TxHash: sampleHash, Amount: minSendAmount}},
		sampleScriptHash, minSendAmount, MinFee, sampleAddr,
	)
	require.Error(t, err)
}

func TestBuildPlutusLockTx_RejectsDustChange(t *testing.T) {
	_, err := BuildPlutusLockTx(
		[]UTxO{{TxHash: sampleHash, Amount: plutusLockAmount + MinFee + 1}},
		sampleScriptHash, plutusLockAmount, MinFee, sampleAddr,
	)
	require.ErrorContains(t, err, "below the minimum output")
}

func TestBuildPlutusLockTx_MissingChangeAddr(t *testing.T) {
	_, err := BuildPlutusLockTx(samplePlutusInputs(), sampleScriptHash, minSendAmount, MinFee, nil)
	require.Error(t, err)
}

func TestBuildPlutusLockTx_IsDeterministic(t *testing.T) {
	a, err := BuildPlutusLockTx(samplePlutusInputs(), sampleScriptHash, minSendAmount, MinFee, sampleAddr)
	require.NoError(t, err)
	b, err := BuildPlutusLockTx(samplePlutusInputs(), sampleScriptHash, minSendAmount, MinFee, sampleAddr)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildPlutusLockTx must be deterministic")
}

// ---- PlutusUnlock tests ----

func sampleUnlockInputs() (UTxO, UTxO) {
	locked := UTxO{TxHash: sampleHash, Index: 0, Amount: plutusLockAmount}
	collateral := UTxO{
		TxHash:     "1111111111111111111111111111111111111111111111111111111111111111",
		Index:      1,
		Amount:     5_000_000,
		SigningKey: testSigningKey(0x99),
	}
	return locked, collateral
}

func TestBuildPlutusUnlockTx_SpendsScriptWithCommittedWitnesses(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	txBytes, err := BuildPlutusUnlockTx(
		locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr,
	)
	require.NoError(t, err)
	tx := requireSignedBy(t, txBytes, collateral.SigningKey)

	require.Len(t, tx.Inputs(), 1)
	require.Equal(t, uint32(0), tx.Inputs()[0].Index())
	require.Len(t, tx.Collateral(), 1)
	require.Equal(t, uint32(1), tx.Collateral()[0].Index())

	scripts := tx.WitnessSet.PlutusV3Scripts()
	require.Len(t, scripts, 1)
	require.Equal(t, alwaysSucceedsScriptHash(), scripts[0].Hash().Bytes())

	require.Equal(t, []uint{0}, tx.WitnessSet.Redeemers().Indexes(common.RedeemerTagSpend))
	require.Equal(t, plutusUnlockExUnits,
		tx.WitnessSet.Redeemers().Value(0, common.RedeemerTagSpend).ExUnits)

	// The ledger hashes the transaction's own redeemer bytes followed by the
	// PlutusV3 language view.
	langViews, err := common.EncodeLangViews(
		map[uint]struct{}{plutusV3: {}},
		map[uint][]int64{plutusV3: sampleCostModel},
	)
	require.NoError(t, err)
	redeemerCbor := tx.WitnessSet.WsRedeemers.Cbor()
	require.NotEmpty(t, redeemerCbor)
	want := common.Blake2b256Hash(append(append([]byte{}, redeemerCbor...), langViews...))
	require.NotNil(t, tx.ScriptDataHash())
	require.Equal(t, want, *tx.ScriptDataHash())

	outputs := tx.Outputs()
	require.Len(t, outputs, 1)
	require.Equal(t, plutusLockAmount-plutusUnlockFee, outputs[0].Amount().Uint64())
}

func TestBuildPlutusUnlockTx_RequiresCostModel(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	_, err := BuildPlutusUnlockTx(locked, collateral, nil, plutusUnlockFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusUnlockTx_RequiresKeyLockedCollateral(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	collateral.SigningKey = nil
	_, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusUnlockTx_RequiresCollateralCoverage(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	collateral.Amount = plutusUnlockFee
	_, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusUnlockTx_InvalidTxHash(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	locked.TxHash = "not-hex"
	_, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusUnlockTx_InsufficientLockedAmount(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	locked.Amount = plutusUnlockFee
	_, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr)
	require.Error(t, err)
}

func TestBuildPlutusUnlockTx_MissingChangeAddr(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	_, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, nil)
	require.Error(t, err)
}

func TestBuildPlutusUnlockTx_IsDeterministic(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	a, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr)
	require.NoError(t, err)
	b, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildPlutusUnlockTx must be deterministic")
}

// TestBuildPlutusUnlockTx_DecodesAsConway guards the top-level encoding.
func TestBuildPlutusUnlockTx_DecodesAsConway(t *testing.T) {
	locked, collateral := sampleUnlockInputs()
	txBytes, err := BuildPlutusUnlockTx(locked, collateral, sampleCostModel, plutusUnlockFee, sampleAddr)
	require.NoError(t, err)
	var tx conway.ConwayTransaction
	_, err = cbor.Decode(txBytes, &tx)
	require.NoError(t, err)
	require.True(t, tx.IsValid())
}
