// Copyright 2025 Blink Labs Software
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
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// testInput implements lcommon.TransactionInput for testing.
type testInput struct {
	txId  lcommon.Blake2b256
	index uint32
}

func (i testInput) Id() lcommon.Blake2b256 { return i.txId }
func (i testInput) Index() uint32          { return i.index }

func (i testInput) String() string { return fmt.Sprintf("%s#%d", i.txId, i.index) }

func (i testInput) MarshalJSON() ([]byte, error) { return []byte(`"` + i.String() + `"`), nil }

func (i testInput) Utxorpc() (*utxorpc.TxInput, error) { return &utxorpc.TxInput{}, nil }

func (i testInput) ToPlutusData() data.PlutusData { return data.NewConstr(0) }

// testOutput implements lcommon.TransactionOutput for testing.
type testOutput struct {
	amount *big.Int
}

func (o testOutput) Address() lcommon.Address { return lcommon.Address{} }

func (o testOutput) Amount() *big.Int { return o.amount }

func (o testOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] { return nil }

func (o testOutput) Datum() *lcommon.Datum { return nil }

func (o testOutput) DatumHash() *lcommon.Blake2b256 { return nil }

func (o testOutput) Cbor() []byte { return nil }

func (o testOutput) Utxorpc() (*utxorpc.TxOutput, error) { return &utxorpc.TxOutput{}, nil }

func (o testOutput) ScriptRef() lcommon.Script { return nil }

func (o testOutput) ToPlutusData() data.PlutusData { return data.NewConstr(0) }

func (o testOutput) String() string { return "testOutput" }

func newTestInput(hashByte byte, index uint32) testInput {
	var hash lcommon.Blake2b256
	hash[0] = hashByte
	return testInput{txId: hash, index: index}
}

func newTestOutput(amount uint64) testOutput {
	return testOutput{amount: new(big.Int).SetUint64(amount)}
}

// testByronTx wraps byron.ByronTransaction to override
// Inputs() and Outputs() for testing.
type testByronTx struct {
	byron.ByronTransaction
	inputs  []lcommon.TransactionInput
	outputs []lcommon.TransactionOutput
}

func (t *testByronTx) Inputs() []lcommon.TransactionInput {
	return t.inputs
}

func (t *testByronTx) Outputs() []lcommon.TransactionOutput {
	return t.outputs
}

func TestValidateTxByron_ValidTransaction(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	assert.NoError(t, err)
}

func TestValidateTxByron_MainnetRedeemWitness(t *testing.T) {
	// This is the transaction that failed at Mainnet slot 3313. Its witness
	// is a constructor-2 redeem witness with the [vkey, signature] payload
	// wrapped in CBOR tag 24.
	txCbor, err := hex.DecodeString(
		"82839f8200d8185824825820a12a839c25a01fa5d118167db5acdbd9e38172ae8f00e5ac0a4997ef792a200700ff9f8282d818584283581c6c9982e7f2b6dcc5eaa880e8014568913c8868d9f0f86eb687b2633ca101581e581c010d876783fb2b4d0d17c86df29af8d35356ed3d1827bf4744f06700001a8dc672c11a000f4240ffa0818202d81858658258208c0bdedfbbab26a1308300512ffb1b220f068ee13f7612afb076c22de3fb764158406cc41635a9794234966629ccfa2a5b089a20ae392f0e92154ff97eda30ff7a082a65fc4b362c24cf58c27f30103b1f1345e15479cf4b80cd4134c0f9dca83109",
	)
	redeemTx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)
	// v0.193.3 does not expose the tag-24-wrapped constructor-2 witness
	// through TransactionWitnessSet; the ledger must retain and validate it
	// from the raw Byron witness values.
	assert.Empty(t, redeemTx.Witnesses().Vkey())

	producerOutputCbor, err := hex.DecodeString(
		"82582b82d818582183581c4041adf6b03851a9c85db3f028995504fb4ba48b50703ab1b9841350a0021ad658e71f1a000f4240",
	)
	require.NoError(t, err)
	producerOutput, err := byron.NewByronTransactionOutputFromCbor(
		producerOutputCbor,
	)
	require.NoError(t, err)

	ls := newMockLedgerState()
	ls.networkId = lcommon.AddressNetworkMainnet
	ls.protocolMagic = byron.MainnetProtocolMagic
	ls.addUtxo(redeemTx.Inputs()[0], producerOutput)
	assert.NoError(t, ValidateTxByron(redeemTx, 3313, ls, nil))
}

func TestValidateTxByron_MainnetBootstrapWitness(t *testing.T) {
	// This is the transaction immediately after the redeem-witness regression
	// above. Its constructor-0 witness is a tag-24-wrapped [extended public
	// key, signature] pair, which older gouroboros releases do not expose.
	txCbor, err := hex.DecodeString(
		"82839f8200d81858248258206497b33b10fa2619c6efbd9f874ecd1c91badb10bf70850732aab45b90524d9e00ff9f8282d818584283581c37f1f51e41efe8713f9755e78bb61af0bb822af6fb31788dba18e27ba101581e581c010d876783fb2b59f088db6d41359ae0a3868a0e411b4dde5713f870001a570841701a000b20128282d818584283581c5d4704fc22524e98ea5b9580ab2a29396b8ad2a92764d08ce23ea1e5a101581e581cd2c9d85d9e2ce454557363216e45b9f015e9b5c2617f0294ac5bc2d0001ae8c1444d1a000186a0ffa0818200d818588582584042a2100a4bce0f08ed211f980d7a848915fd48953be80b4b4fb3a9bbf8aea206cc8a84c83896f3d716fe0fc6ae8d5ae5554109c1fff5b6ca6c53cc74741dcad25840c26a80389d8bee813ed786d4cf395bbc304f43bef1b75eb5f989e915451cbe5610f8bf7dc843392070e4a470ebb7614da37f78c8a879da8eb0fc2f7f8ffd0107",
	)
	bootstrapTx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)

	inputAddress, err := lcommon.NewAddress(
		"DdzFFzCqrhsszHTvbjTmYje5hehGbadkT6WgWbaqCy5XNxNttsPNF13eAjjBHYT7JaLJz2XVxiucam1EvwBRPSTiCrT4TNCBas4hfzic",
	)
	require.NoError(t, err)
	input := byron.ByronTransactionOutput{
		OutputAddress: inputAddress,
		OutputAmount:  3_000_000_000,
	}

	ls := newMockLedgerState()
	ls.networkId = lcommon.AddressNetworkMainnet
	ls.protocolMagic = byron.MainnetProtocolMagic
	ls.addUtxo(bootstrapTx.Inputs()[0], input)
	assert.NoError(t, ValidateTxByron(bootstrapTx, 3336, ls, nil))
}

func TestValidateTxByron_ValidMultipleInputsOutputs(
	t *testing.T,
) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
			newTestInput(0x02, 0),
			newTestInput(0x03, 1),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(500_000),
			newTestOutput(300_000),
			newTestOutput(200_000),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	assert.NoError(t, err)
}

func TestValidateTxByron_EmptyInputs(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &InputSetEmptyByronError{})
	assert.Contains(t, err.Error(), "no inputs")
}

func TestValidateTxByron_NilInputs(t *testing.T) {
	tx := &testByronTx{
		inputs: nil,
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &InputSetEmptyByronError{})
}

func TestValidateTxByron_EmptyOutputs(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &OutputSetEmptyByronError{})
	assert.Contains(t, err.Error(), "no outputs")
}

func TestValidateTxByron_NilOutputs(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: nil,
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &OutputSetEmptyByronError{})
}

func TestValidateTxByron_ZeroValueOutput(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(0),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &OutputNotPositiveByronError{})
	assert.Contains(t, err.Error(), "non-positive value")
}

func TestValidateTxByron_NegativeValueOutput(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			testOutput{amount: big.NewInt(-100)},
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &OutputNotPositiveByronError{})
}

func TestValidateTxByron_NilAmountOutput(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			testOutput{amount: nil},
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &OutputNotPositiveByronError{})
}

func TestValidateTxByron_DuplicateInputs(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
			newTestInput(0x01, 0), // duplicate
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &DuplicateInputByronError{})
	assert.Contains(t, err.Error(), "duplicate input")
}

func TestValidateTxByron_SameTxDifferentIndex(t *testing.T) {
	// Same transaction hash but different output indices
	// should be valid
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
			newTestInput(0x01, 1),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	assert.NoError(t, err)
}

func TestValidateTxByron_MultipleErrors(t *testing.T) {
	// Empty inputs AND empty outputs should both be reported
	tx := &testByronTx{
		inputs:  []lcommon.TransactionInput{},
		outputs: []lcommon.TransactionOutput{},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &InputSetEmptyByronError{})
	assert.ErrorAs(t, err, &OutputSetEmptyByronError{})
}

func TestValidateTxByron_SecondOutputZeroValue(t *testing.T) {
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
			newTestOutput(0), // second output is zero
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &OutputNotPositiveByronError{})
}

func TestByronEraDesc_HasValidateTxFunc(t *testing.T) {
	assert.NotNil(
		t,
		ByronEraDesc.ValidateTxFunc,
		"ByronEraDesc should have ValidateTxFunc set",
	)
}

// --- Mock LedgerState for UTxO-aware tests ---

// errUtxoNotFound is returned when a UTxO is not found in the
// mock ledger state.
var errUtxoNotFound = errors.New("UTxO not found")

// mockLedgerState implements lcommon.LedgerState for testing
// UTxO-aware Byron validation rules.
type mockLedgerState struct {
	utxos                map[string]lcommon.Utxo
	networkId            uint
	protocolMagic        uint32
	skipPhase2Validation bool
	utxoLookups          int
}

func newMockLedgerState() *mockLedgerState {
	return &mockLedgerState{
		utxos: make(map[string]lcommon.Utxo),
	}
}

func (m *mockLedgerState) addUtxo(
	input lcommon.TransactionInput,
	output lcommon.TransactionOutput,
) {
	key := fmt.Sprintf("%s#%d", input.Id(), input.Index())
	m.utxos[key] = lcommon.Utxo{
		Id:     input,
		Output: output,
	}
}

func (m *mockLedgerState) UtxoById(
	input lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	m.utxoLookups++
	key := fmt.Sprintf("%s#%d", input.Id(), input.Index())
	utxo, ok := m.utxos[key]
	if !ok {
		return lcommon.Utxo{}, errUtxoNotFound
	}
	return utxo, nil
}

func (m *mockLedgerState) NetworkId() uint { return m.networkId }

func (m *mockLedgerState) ByronProtocolMagic() (uint32, error) {
	return m.protocolMagic, nil
}

func (m *mockLedgerState) SkipPhase2Validation() bool {
	return m.skipPhase2Validation
}

// Stub implementations for the remaining LedgerState
// interface methods. These are unused by Byron validation.

func (m *mockLedgerState) StakeRegistration(
	_ []byte,
) ([]lcommon.StakeRegistrationCertificate, error) {
	return nil, nil
}

func (m *mockLedgerState) IsStakeCredentialRegistered(
	_ lcommon.Credential,
) bool {
	return false
}

func (m *mockLedgerState) SlotToTime(
	_ uint64,
) (time.Time, error) {
	return time.Time{}, nil
}

func (m *mockLedgerState) TimeToSlot(
	_ time.Time,
) (uint64, error) {
	return 0, nil
}

func (m *mockLedgerState) PoolCurrentState(
	_ lcommon.PoolKeyHash,
) (*lcommon.PoolRegistrationCertificate, *uint64, error) {
	return nil, nil, nil
}

func (m *mockLedgerState) IsPoolRegistered(
	_ lcommon.PoolKeyHash,
) bool {
	return false
}

func (m *mockLedgerState) IsVrfKeyInUse(
	_ lcommon.Blake2b256,
) (bool, lcommon.PoolKeyHash, error) {
	return false, lcommon.PoolKeyHash{}, nil
}

func (m *mockLedgerState) CalculateRewards(
	_ lcommon.AdaPots,
	_ lcommon.RewardSnapshot,
	_ lcommon.RewardParameters,
) (*lcommon.RewardCalculationResult, error) {
	return nil, nil
}

func (m *mockLedgerState) GetAdaPots() lcommon.AdaPots {
	return lcommon.AdaPots{}
}

func (m *mockLedgerState) UpdateAdaPots(
	_ lcommon.AdaPots,
) error {
	return nil
}

func (m *mockLedgerState) GetRewardSnapshot(
	_ uint64,
) (lcommon.RewardSnapshot, error) {
	return lcommon.RewardSnapshot{}, nil
}

func (m *mockLedgerState) IsRewardAccountRegistered(
	_ lcommon.Credential,
) bool {
	return false
}

func (m *mockLedgerState) RewardAccountBalance(
	_ lcommon.Credential,
) (*uint64, error) {
	return nil, nil
}

func (m *mockLedgerState) CommitteeMember(
	_ lcommon.Blake2b224,
) (*lcommon.CommitteeMember, error) {
	return nil, nil
}

func (m *mockLedgerState) CommitteeMembers() (
	[]lcommon.CommitteeMember,
	error,
) {
	return nil, nil
}

func (m *mockLedgerState) DRepRegistration(
	_ lcommon.Blake2b224,
) (*lcommon.DRepRegistration, error) {
	return nil, nil
}

func (m *mockLedgerState) DRepRegistrations() (
	[]lcommon.DRepRegistration,
	error,
) {
	return nil, nil
}

func (m *mockLedgerState) Constitution() (
	*lcommon.Constitution,
	error,
) {
	return nil, nil
}

func (m *mockLedgerState) TreasuryValue() (uint64, error) {
	return 0, nil
}

func (m *mockLedgerState) GovActionById(
	_ lcommon.GovActionId,
) (*lcommon.GovActionState, error) {
	return nil, nil
}

func (m *mockLedgerState) GovActionExists(
	_ lcommon.GovActionId,
) bool {
	return false
}

func (m *mockLedgerState) CostModels() map[lcommon.PlutusLanguage]lcommon.CostModel {
	return nil
}

// --- Tests for UTxO-aware Byron validation rules ---

func TestByronValidateBadInputs_AllInputsExist(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	input2 := newTestInput(0x02, 0)
	ls.addUtxo(input1, newTestOutput(1_000_000))
	ls.addUtxo(input2, newTestOutput(2_000_000))

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{input1, input2},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(2_500_000),
		},
	}
	err := byronValidateBadInputs(tx, 0, ls, nil)
	assert.NoError(t, err)
}

func TestByronValidateBadInputs_MissingInput(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	ls.addUtxo(input1, newTestOutput(1_000_000))

	missingInput := newTestInput(0xFF, 0)
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			input1,
			missingInput,
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(500_000),
		},
	}
	err := byronValidateBadInputs(tx, 0, ls, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &BadInputsByronError{})
	assert.Contains(t, err.Error(), "bad input")
}

func TestByronValidateBadInputs_AllMissing(t *testing.T) {
	ls := newMockLedgerState()
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
			newTestInput(0x02, 0),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(500_000),
		},
	}
	err := byronValidateBadInputs(tx, 0, ls, nil)
	require.Error(t, err)
	var badErr BadInputsByronError
	require.ErrorAs(t, err, &badErr)
	assert.Len(t, badErr.Inputs, 2)
}

func TestByronValidateValueConserved_Valid(t *testing.T) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	ls.addUtxo(input1, newTestOutput(3_000_000))

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{input1},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(2_800_000), // 200k implicit fee
		},
	}
	err := byronValidateValueConserved(tx, 0, ls, nil)
	assert.NoError(t, err)
}

func TestByronValidateValueConserved_ExactMatch(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	ls.addUtxo(input1, newTestOutput(1_000_000))

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{input1},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000), // zero fee is valid
		},
	}
	err := byronValidateValueConserved(tx, 0, ls, nil)
	assert.NoError(t, err)
}

func TestByronValidateValueConserved_OutputExceedsInput(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	ls.addUtxo(input1, newTestOutput(1_000_000))

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{input1},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(2_000_000), // outputs > inputs
		},
	}
	err := byronValidateValueConserved(tx, 0, ls, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &ValueNotConservedByronError{})
	assert.Contains(t, err.Error(), "value not conserved")
}

func TestByronValidateValueConserved_MultipleInputsOutputs(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	input2 := newTestInput(0x02, 0)
	ls.addUtxo(input1, newTestOutput(5_000_000))
	ls.addUtxo(input2, newTestOutput(3_000_000))

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{input1, input2},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(4_000_000),
			newTestOutput(3_500_000),
		},
	}
	// Consumed=8M, Produced=7.5M, fee=0.5M -- valid
	err := byronValidateValueConserved(tx, 0, ls, nil)
	assert.NoError(t, err)
}

func TestByronValidateValueConserved_SkipsMissingInputs(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	ls.addUtxo(input1, newTestOutput(2_000_000))
	// input2 is not in UTxO set -- skipped in value calc
	input2 := newTestInput(0x02, 0)

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{input1, input2},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	// Only input1 counted: 2M consumed vs 1M produced -> ok
	err := byronValidateValueConserved(tx, 0, ls, nil)
	assert.NoError(t, err)
}

func TestValidateTxByron_WithLedgerState_Valid(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	ls.addUtxo(input1, newTestOutput(5_000_000))

	tx := &testByronTx{
		inputs:  []lcommon.TransactionInput{input1},
		outputs: []lcommon.TransactionOutput{newTestOutput(4_800_000)},
	}
	err := ValidateTxByron(tx, 0, ls, nil)
	assert.NoError(t, err)
}

func TestValidateTxByron_WithLedgerState_BadInput(
	t *testing.T,
) {
	ls := newMockLedgerState()
	// Do not add any UTxO -- input will be bad
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &BadInputsByronError{})
}

func TestValidateTxByron_WithLedgerState_ValueNotConserved(
	t *testing.T,
) {
	ls := newMockLedgerState()
	input1 := newTestInput(0x01, 0)
	ls.addUtxo(input1, newTestOutput(500_000))

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{input1},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	assert.ErrorAs(t, err, &ValueNotConservedByronError{})
}

func TestValidateTxByron_NilLedgerState_SkipsUtxoRules(
	t *testing.T,
) {
	// With nil LedgerState, only structural rules run.
	// A valid structural TX passes even though we can't
	// check inputs against the UTxO set.
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, nil, nil)
	assert.NoError(t, err)
}

func TestValidateTxByron_CombinedStructuralAndUtxoErrors(
	t *testing.T,
) {
	ls := newMockLedgerState()
	// Duplicate inputs AND bad inputs
	input1 := newTestInput(0x01, 0)
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			input1,
			input1, // duplicate
		},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000_000),
		},
	}
	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	// Both structural and UTxO errors should be reported
	assert.ErrorAs(t, err, &DuplicateInputByronError{})
	assert.ErrorAs(t, err, &BadInputsByronError{})
}
