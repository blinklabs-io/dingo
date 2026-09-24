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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
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
	amount  *big.Int
	address lcommon.Address
}

func (o testOutput) Address() lcommon.Address { return o.address }

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

// newTestOutputWithAddress builds an output carrying an explicit address, for
// rules that classify the address of a consumed UTxO.
func newTestOutputWithAddress(
	amount uint64,
	addr lcommon.Address,
) testOutput {
	return testOutput{
		amount:  new(big.Int).SetUint64(amount),
		address: addr,
	}
}

// newTestByronAddress builds a Byron address of the given Byron address type
// with a deterministic payment key hash.
func newTestByronAddress(
	t *testing.T,
	byronAddrType uint64,
	hashByte byte,
) lcommon.Address {
	t.Helper()
	hash := make([]byte, lcommon.AddressHashSize)
	hash[0] = hashByte
	addr, err := lcommon.NewByronAddressFromParts(
		byronAddrType,
		hash,
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	return addr
}

// testByronTx wraps byron.ByronTransaction to override
// Inputs() and Outputs() for testing.
type testByronTx struct {
	byron.ByronTransaction
	inputs  []lcommon.TransactionInput
	outputs []lcommon.TransactionOutput
	cbor    []byte
}

func (t *testByronTx) Inputs() []lcommon.TransactionInput {
	return t.inputs
}

func (t *testByronTx) Outputs() []lcommon.TransactionOutput {
	return t.outputs
}

func (t *testByronTx) Cbor() []byte {
	return t.cbor
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

func TestValidateTxByron_MainnetZeroValueOutput(t *testing.T) {
	// This transaction is from the canonical mainnet block at slot 4,427,376.
	// Byron consensus permits its first output to contain zero lovelace.
	txCbor, err := hex.DecodeString(
		"839f8200d8185824825820f27d4ccc224c706184fad5cfb38ee067c334f70a1c57f576fab2ad80992e976a01ff9f8282d818582183581c7aef491d0bb12165ecbd33388686fac0c64d17c1c90ab1c84cce1b81a0001abc1cd901008282d818582183581cb3ad626374eb2b751a233fc06e3ab81f10a4069936b218462cba129ca0001a1cbad9181a089105e8ffa0",
	)
	require.NoError(t, err)
	// Koios exposes the canonical Byron transaction body. Wrap it in the
	// transaction envelope with an empty witness set for structural validation.
	fullTxCbor := append([]byte{0x82}, txCbor...)
	fullTxCbor = append(fullTxCbor, 0x9f, 0xff)
	tx, err := byron.NewByronTransactionFromCbor(fullTxCbor)
	require.NoError(t, err)
	require.Len(t, tx.Outputs(), 2)
	assert.Zero(t, tx.Outputs()[0].Amount().Sign())
	assert.NoError(t, ValidateTxByron(tx, 4_427_376, nil, nil))
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
	assert.ErrorAs(t, err, &OutputNegativeByronError{})
}

func TestValidateTxByron_MainnetRedeemWitness(t *testing.T) {
	// This is the transaction that failed at Mainnet slot 3313. Its witness
	// is a constructor-2 redeem witness with the [vkey, signature] payload
	// wrapped in CBOR tag 24.
	txCbor, err := hex.DecodeString(
		"82839f8200d8185824825820a12a839c25a01fa5d118167db5acdbd9e38172ae8f00e5ac0a4997ef792a200700ff9f8282d818584283581c6c9982e7f2b6dcc5eaa880e8014568913c8868d9f0f86eb687b2633ca101581e581c010d876783fb2b4d0d17c86df29af8d35356ed3d1827bf4744f06700001a8dc672c11a000f4240ffa0818202d81858658258208c0bdedfbbab26a1308300512ffb1b220f068ee13f7612afb076c22de3fb764158406cc41635a9794234966629ccfa2a5b089a20ae392f0e92154ff97eda30ff7a082a65fc4b362c24cf58c27f30103b1f1345e15479cf4b80cd4134c0f9dca83109",
	)
	require.NoError(t, err)
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
	require.NoError(t, err)
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

// TestValidateTxByron_RejectsExtraMalformedWitness is the regression for
// issue #4384: a transaction with a genuine, matching witness must not
// validate merely because that one witness resolves every input. A second,
// unrecognized witness entry appended after it must reject the whole
// transaction, mirroring the reference decoder's lack of a catch-all case.
func TestValidateTxByron_RejectsExtraMalformedWitness(t *testing.T) {
	t.Parallel()

	txCbor, err := hex.DecodeString(
		"82839f8200d8185824825820a12a839c25a01fa5d118167db5acdbd9e38172ae8f00e5ac0a4997ef792a200700ff9f8282d818584283581c6c9982e7f2b6dcc5eaa880e8014568913c8868d9f0f86eb687b2633ca101581e581c010d876783fb2b4d0d17c86df29af8d35356ed3d1827bf4744f06700001a8dc672c11a000f4240ffa0818202d81858658258208c0bdedfbbab26a1308300512ffb1b220f068ee13f7612afb076c22de3fb764158406cc41635a9794234966629ccfa2a5b089a20ae392f0e92154ff97eda30ff7a082a65fc4b362c24cf58c27f30103b1f1345e15479cf4b80cd4134c0f9dca83109",
	)
	require.NoError(t, err)
	redeemTx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)

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

	// Confirm the unmodified transaction is genuinely valid before tampering,
	// so the failure below is attributable to the appended witness.
	require.NoError(t, ValidateTxByron(redeemTx, 3313, ls, nil))

	redeemTx.Twit = append(
		redeemTx.Twit,
		byronTestWitness(t, 1, []any{[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}}),
	)
	err = ValidateTxByron(redeemTx, 3313, ls, nil)
	require.Error(t, err)
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

// TestValidateTxByron_DuplicateInputs covers #4401: the reference keeps
// inputs as a list and has no duplicate-input rejection.
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
	require.NoError(t, ValidateTxByron(tx, 0, nil, nil))
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
	utxos         map[string]lcommon.Utxo
	networkId     uint
	protocolMagic uint32
	// protocolMagicErr, when set, makes ByronProtocolMagic fail, so a test
	// can prove a rule surfaces the lookup failure rather than skipping.
	protocolMagicErr   error
	byronFeeSummand    int64
	byronFeeMultiplier int64
	// byronMaxTxSize backs ByronMaxTxSize; zero means no limit.
	byronMaxTxSize       uint64
	skipPhase2Validation bool
	utxoLookups          int
	// slotToTime, when set, replaces the zero-time default so a test can
	// supply a real network's slot-to-time mapping.
	slotToTime func(uint64) (time.Time, error)
	// slotToTimeCalls counts SlotToTime invocations, the same way utxoLookups
	// counts UtxoById ones. validityRangeInfo -- called once per TxInfo build,
	// via NewTxInfoV1FromTransaction/NewTxInfoV2FromTransaction/
	// NewTxInfoV3FromTransaction -- calls SlotToTime once per validity bound
	// present, so this is a direct proxy for how many times a transaction's
	// TxInfo was (re)built.
	slotToTimeCalls int
	// syntheticV2CostModel backs SyntheticV2CostModelInEffect, so a test can
	// exercise ValidateTxBabbage/EvaluateTxBabbage's ErrNoCostModelForPlutusV2
	// check (blinklabs-io/dingo#3962) without a real *ledger.LedgerView.
	syntheticV2CostModel bool
	// pendingMIR seeds the Pending map MIRDelegState reports, letting a test
	// simulate InstantaneousRewards already accumulated earlier in the
	// current epoch without a real *ledger.LedgerView or database.
	pendingMIR map[MIRCredentialKey]*big.Int
	// mirState, when set, replaces the default MIRDelegState: unbounded pots
	// and no cutoff, so tests unrelated to MIR capacity or timing are not
	// rejected by it.
	mirState *MIRDelegState
	// stakeRegistered backs IsStakeCredentialRegistered, and rewardBalances
	// the balance RewardAccountBalance reports for a registered credential.
	stakeRegistered map[lcommon.Blake2b224]bool
	rewardBalances  map[lcommon.Blake2b224]uint64
}

// MIRDelegState implements eras.MIRDelegStateProvider for tests. The real
// implementation (*ledger.LedgerView) derives this from the database. The
// Pending map is copied because the caller folds certificates into it.
func (m *mockLedgerState) MIRDelegState(
	_ uint64,
	_ bool,
) (MIRDelegState, error) {
	state := MIRDelegState{
		Reserves: math.MaxUint64,
		Treasury: math.MaxUint64,
		Pending:  m.pendingMIR,
		Cutoff:   math.MaxUint64,
	}
	if m.mirState != nil {
		state = *m.mirState
		if state.DeltaReserves != nil {
			state.DeltaReserves = new(big.Int).Set(state.DeltaReserves)
		}
		if state.DeltaTreasury != nil {
			state.DeltaTreasury = new(big.Int).Set(state.DeltaTreasury)
		}
	}
	pending := make(map[MIRCredentialKey]*big.Int, len(state.Pending))
	for key, amount := range state.Pending {
		pending[key] = new(big.Int).Set(amount)
	}
	state.Pending = pending
	return state, nil
}

// SyntheticV2CostModelInEffect implements the eras package's local
// syntheticV2CostModelReporter interface (see eras.go), the same way
// *ledger.LedgerView does in production.
func (m *mockLedgerState) SyntheticV2CostModelInEffect() bool {
	return m.syntheticV2CostModel
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
	if m.protocolMagicErr != nil {
		return 0, m.protocolMagicErr
	}
	return m.protocolMagic, nil
}

func (m *mockLedgerState) ByronFeePolicy() (int64, int64, error) {
	return m.byronFeeSummand, m.byronFeeMultiplier, nil
}

func (m *mockLedgerState) ByronMaxTxSize() (uint64, error) {
	if m.byronMaxTxSize == 0 {
		return math.MaxUint64, nil
	}
	return m.byronMaxTxSize, nil
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
	cred lcommon.Credential,
) bool {
	return m.stakeRegistered[cred.Credential]
}

func (m *mockLedgerState) SlotToTime(
	slot uint64,
) (time.Time, error) {
	m.slotToTimeCalls++
	if m.slotToTime != nil {
		return m.slotToTime(slot)
	}
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
	cred lcommon.Credential,
) (*uint64, error) {
	if !m.stakeRegistered[cred.Credential] {
		return nil, nil
	}
	balance := m.rewardBalances[cred.Credential]
	return &balance, nil
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
	_ lcommon.Credential,
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

func TestValidateTxByron_MinimumFee(t *testing.T) {
	t.Parallel()

	const txSize = 10

	tests := []struct {
		name      string
		output    uint64
		wantError bool
	}{
		{
			name:   "fee equals minimum",
			output: 989,
		},
		{
			name:      "fee is one lovelace below minimum",
			output:    990,
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := newTestInput(0x01, 0)
			ls := newMockLedgerState()
			ls.byronFeeSummand = 1_000_000_001
			ls.byronFeeMultiplier = 1_000_000_000
			ls.addUtxo(input, newTestOutput(1_000))
			tx := &testByronTx{
				inputs: []lcommon.TransactionInput{input},
				outputs: []lcommon.TransactionOutput{
					newTestOutput(test.output),
				},
				cbor: make([]byte, txSize),
			}

			err := ValidateTxByron(tx, 0, ls, nil)
			if test.wantError {
				require.Error(t, err)
				var feeErr FeeTooLowByronError
				require.ErrorAs(t, err, &feeErr)
				// 1_000_000_001 div 10^9 + ceiling(1 * 10), not one
				// ceiling over the scaled sum (12).
				assert.Equal(t, big.NewInt(10), feeErr.Actual)
				assert.Equal(t, big.NewInt(11), feeErr.Required)
				assert.Equal(t, uint64(txSize), feeErr.Size)
				return
			}
			assert.NoError(t, err)
		})
	}
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
	// Negative output AND bad inputs
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{
			newTestInput(0x01, 0),
		},
		outputs: []lcommon.TransactionOutput{
			testOutput{amount: big.NewInt(-1)},
		},
	}
	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	// Both structural and UTxO errors should be reported
	assert.ErrorAs(t, err, &OutputNegativeByronError{})
	assert.ErrorAs(t, err, &BadInputsByronError{})
}

// TestByronValidateMinFee_RedeemOnlyExemption covers the Byron reference
// isRedeemUTxO exemption: when every consumed input is a redeem address, the
// required minimum fee is zero. The presence of a single non-redeem input is
// not sufficient, so a mixed transaction pays the normal linear fee.
//
// This exercises byronValidateMinFee directly so the assertion isolates the
// fee predicate from the independent redeem-witness requirement.
func TestByronValidateMinFee_RedeemOnlyExemption(t *testing.T) {
	t.Parallel()

	const txSize = 10

	redeemA := newTestByronAddress(t, lcommon.ByronAddressTypeRedeem, 0xAA)
	redeemB := newTestByronAddress(t, lcommon.ByronAddressTypeRedeem, 0xBB)
	ordinary := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xCC)

	tests := []struct {
		name      string
		addresses []lcommon.Address
		wantError bool
	}{
		{
			name:      "single redeem input pays zero fee",
			addresses: []lcommon.Address{redeemA},
		},
		{
			name:      "multiple redeem inputs pay zero fee",
			addresses: []lcommon.Address{redeemA, redeemB},
		},
		{
			name:      "redeem plus ordinary input pays normal fee",
			addresses: []lcommon.Address{redeemA, ordinary},
			wantError: true,
		},
		{
			name:      "ordinary input only pays normal fee",
			addresses: []lcommon.Address{ordinary},
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ls := newMockLedgerState()
			ls.byronFeeSummand = 1_000_000_001
			ls.byronFeeMultiplier = 1_000_000_000

			// Each input supplies 1000 lovelace and the single output
			// reproduces the whole consumed value, so the implicit fee is
			// exactly zero. Value conservation holds independently.
			inputs := make([]lcommon.TransactionInput, 0, len(test.addresses))
			var consumed uint64
			for i, addr := range test.addresses {
				input := newTestInput(byte(i+1), 0)
				ls.addUtxo(input, newTestOutputWithAddress(1_000, addr))
				inputs = append(inputs, input)
				consumed += 1_000
			}

			tx := &testByronTx{
				inputs:  inputs,
				outputs: []lcommon.TransactionOutput{newTestOutput(consumed)},
				cbor:    make([]byte, txSize),
			}

			err := byronValidateMinFee(tx, 0, ls, nil)
			if test.wantError {
				require.Error(t, err)
				var feeErr FeeTooLowByronError
				require.ErrorAs(t, err, &feeErr)
				assert.Zero(t, feeErr.Actual.Sign())
				assert.Equal(t, 0, feeErr.Required.Cmp(big.NewInt(11)))
				assert.Equal(t, uint64(txSize), feeErr.Size)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestByronValidateMinFee_RedeemExemptionRequiresResolvableInputs ensures an
// input that cannot be resolved does not make a transaction look redeem-only.
// The bad-input rule reports its own error, but the fee rule must still
// require the normal fee rather than exempting an unresolvable input set.
func TestByronValidateMinFee_RedeemExemptionRequiresResolvableInputs(
	t *testing.T,
) {
	t.Parallel()

	ls := newMockLedgerState()
	ls.byronFeeSummand = 1_000_000_001
	ls.byronFeeMultiplier = 1_000_000_000

	redeem := newTestByronAddress(t, lcommon.ByronAddressTypeRedeem, 0xAA)
	resolved := newTestInput(0x01, 0)
	ls.addUtxo(resolved, newTestOutputWithAddress(1_000, redeem))
	// missing is never added to the UTxO set.
	missing := newTestInput(0x02, 0)

	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{resolved, missing},
		outputs: []lcommon.TransactionOutput{
			newTestOutput(1_000),
		},
		cbor: make([]byte, 10),
	}

	err := byronValidateMinFee(tx, 0, ls, nil)
	require.Error(t, err)
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, err, &feeErr)
}

// TestByronValidateMinFee_NoInputsNotRedeemOnly ensures an empty input set is
// not treated as vacuously redeem-only, where the reference all would be
// vacuously true. The empty-input set has its own structural rule, so this
// guard is unobservable in production, but it pins the intended semantics.
//
// The output must be zero so the implicit fee is exactly zero rather than
// negative. A negative fee is below every requirement including zero, which
// would make this pass whether or not the guard exists.
func TestByronValidateMinFee_NoInputsNotRedeemOnly(t *testing.T) {
	t.Parallel()

	ls := newMockLedgerState()
	ls.byronFeeSummand = 1_000_000_001
	ls.byronFeeMultiplier = 1_000_000_000

	tx := &testByronTx{
		inputs:  []lcommon.TransactionInput{},
		outputs: []lcommon.TransactionOutput{newTestOutput(0)},
		cbor:    make([]byte, 10),
	}

	err := byronValidateMinFee(tx, 0, ls, nil)
	require.Error(t, err)
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, err, &feeErr)
	// Exactly zero, so the assertion discriminates the guard rather than
	// riding on a negative fee.
	assert.Zero(t, feeErr.Actual.Sign())
	assert.Positive(t, feeErr.Required.Sign())
}

// byronTestWitness builds a single Byron TxInWitness value --
// [ctor, #6.24(bytes .cbor fields)] -- as the cbor.Value byronDecodeWitnesses
// expects, for exercising its constructor handling directly.
func byronTestWitness(t *testing.T, ctor uint64, fields []any) cbor.Value {
	t.Helper()
	inner, err := cbor.Encode(fields)
	require.NoError(t, err)
	outer, err := cbor.Encode([]any{ctor, cbor.WrappedCbor(inner)})
	require.NoError(t, err)
	var v cbor.Value
	require.NoError(t, v.UnmarshalCBOR(outer))
	return v
}

func TestByronDecodeWitnesses(t *testing.T) {
	t.Parallel()

	pk := []byte{1, 2, 3, 4}
	sig := []byte{5, 6, 7, 8}
	pk64 := bytes.Repeat([]byte{0xAB}, 64)
	sig64 := bytes.Repeat([]byte{0xCD}, 64)
	redeemPk32 := bytes.Repeat([]byte{0xEF}, 32)

	t.Run("valid constructor 0 and constructor 2 decode", func(t *testing.T) {
		t.Parallel()
		witnesses := []cbor.Value{
			byronTestWitness(
				t,
				lcommon.ByronAddressTypePubkey,
				[]any{pk64, sig64},
			),
			byronTestWitness(
				t,
				lcommon.ByronAddressTypeRedeem,
				[]any{redeemPk32, sig64},
			),
		}
		decoded, err := byronDecodeWitnesses(witnesses)
		require.NoError(t, err)
		require.Len(t, decoded, 2)
		assert.False(t, decoded[0].redeem)
		assert.Equal(t, pk64[:32], decoded[0].publicKey)
		assert.Equal(t, pk64[32:], decoded[0].chainCode)
		assert.Equal(t, sig64, decoded[0].signature)
		assert.True(t, decoded[1].redeem)
		assert.Equal(t, redeemPk32, decoded[1].publicKey)
		assert.Equal(t, sig64, decoded[1].signature)
	})

	t.Run("unknown constructor 1 is rejected", func(t *testing.T) {
		t.Parallel()
		witnesses := []cbor.Value{
			byronTestWitness(t, 1, []any{pk, sig}),
		}
		_, err := byronDecodeWitnesses(witnesses)
		require.Error(t, err)
	})

	t.Run("unknown constructor 3 is rejected", func(t *testing.T) {
		t.Parallel()
		witnesses := []cbor.Value{
			byronTestWitness(t, 3, []any{pk64, sig64, pk64, sig64}),
		}
		_, err := byronDecodeWitnesses(witnesses)
		require.Error(t, err)
	})

	t.Run(
		"malformed constructor-0 field count is rejected",
		func(t *testing.T) {
			t.Parallel()
			witnesses := []cbor.Value{
				byronTestWitness(
					t,
					lcommon.ByronAddressTypePubkey,
					[]any{pk64},
				),
			}
			_, err := byronDecodeWitnesses(witnesses)
			require.Error(t, err)
		},
	)

	t.Run(
		"malformed constructor-0 field length is rejected",
		func(t *testing.T) {
			t.Parallel()
			witnesses := []cbor.Value{
				byronTestWitness(
					t,
					lcommon.ByronAddressTypePubkey,
					[]any{pk, sig},
				),
			}
			_, err := byronDecodeWitnesses(witnesses)
			require.Error(t, err)
		},
	)

	t.Run(
		"malformed constructor-2 field count is rejected",
		func(t *testing.T) {
			t.Parallel()
			witnesses := []cbor.Value{
				byronTestWitness(
					t,
					lcommon.ByronAddressTypeRedeem,
					[]any{redeemPk32, sig64, sig64},
				),
			}
			_, err := byronDecodeWitnesses(witnesses)
			require.Error(t, err)
		},
	)

	t.Run(
		"malformed constructor-2 field length is rejected",
		func(t *testing.T) {
			t.Parallel()
			// A redeem key and signature that are the wrong length for Byron's
			// Ed25519-based redeem crypto (32-byte key, 64-byte signature) must
			// be rejected as a malformed witness during decoding, not accepted
			// here and left to fail later as a signature-verification error.
			witnesses := []cbor.Value{
				byronTestWitness(
					t,
					lcommon.ByronAddressTypeRedeem,
					[]any{pk, sig},
				),
			}
			_, err := byronDecodeWitnesses(witnesses)
			require.Error(t, err)
		},
	)

	t.Run("untagged witness is rejected", func(t *testing.T) {
		t.Parallel()
		outer, err := cbor.Encode(
			[]any{uint64(lcommon.ByronAddressTypePubkey), []any{pk64, sig64}},
		)
		require.NoError(t, err)
		var v cbor.Value
		require.NoError(t, v.UnmarshalCBOR(outer))
		_, err = byronDecodeWitnesses([]cbor.Value{v})
		require.Error(t, err)
	})

	t.Run("trailing bytes after nested cbor are rejected", func(t *testing.T) {
		t.Parallel()
		inner, err := cbor.Encode([]any{pk64, sig64})
		require.NoError(t, err)
		withTrailingGarbage := append(append([]byte{}, inner...), 0xFF, 0xFF)
		outer, err := cbor.Encode(
			[]any{
				uint64(lcommon.ByronAddressTypePubkey),
				cbor.WrappedCbor(withTrailingGarbage),
			},
		)
		require.NoError(t, err)
		var v cbor.Value
		require.NoError(t, v.UnmarshalCBOR(outer))
		_, err = byronDecodeWitnesses([]cbor.Value{v})
		require.Error(t, err)
	})

	t.Run(
		"valid witness plus a malformed extra witness rejects both",
		func(t *testing.T) {
			t.Parallel()
			witnesses := []cbor.Value{
				byronTestWitness(
					t,
					lcommon.ByronAddressTypeRedeem,
					[]any{redeemPk32, sig64},
				),
				byronTestWitness(t, 1, []any{pk, sig}),
			}
			_, err := byronDecodeWitnesses(witnesses)
			require.Error(t, err)
		},
	)
}
