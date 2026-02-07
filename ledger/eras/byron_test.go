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

func (i testInput) Id() lcommon.Blake2b256             { return i.txId }
func (i testInput) Index() uint32                      { return i.index }
func (i testInput) String() string                     { return fmt.Sprintf("%s#%d", i.txId, i.index) }
func (i testInput) MarshalJSON() ([]byte, error)       { return []byte(`"` + i.String() + `"`), nil }
func (i testInput) Utxorpc() (*utxorpc.TxInput, error) { return &utxorpc.TxInput{}, nil }
func (i testInput) ToPlutusData() data.PlutusData      { return data.NewConstr(0) }

// testOutput implements lcommon.TransactionOutput for testing.
type testOutput struct {
	amount *big.Int
}

func (o testOutput) Address() lcommon.Address                                  { return lcommon.Address{} }
func (o testOutput) Amount() *big.Int                                          { return o.amount }
func (o testOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] { return nil }
func (o testOutput) Datum() *lcommon.Datum                                     { return nil }
func (o testOutput) DatumHash() *lcommon.Blake2b256                            { return nil }
func (o testOutput) Cbor() []byte                                              { return nil }
func (o testOutput) Utxorpc() (*utxorpc.TxOutput, error)                       { return &utxorpc.TxOutput{}, nil }
func (o testOutput) ScriptRef() lcommon.Script                                 { return nil }
func (o testOutput) ToPlutusData() data.PlutusData                             { return data.NewConstr(0) }
func (o testOutput) String() string                                            { return "testOutput" }

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
	utxos     map[string]lcommon.Utxo
	networkId uint
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
	key := fmt.Sprintf("%s#%d", input.Id(), input.Index())
	utxo, ok := m.utxos[key]
	if !ok {
		return lcommon.Utxo{}, errUtxoNotFound
	}
	return utxo, nil
}

func (m *mockLedgerState) NetworkId() uint { return m.networkId }

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
