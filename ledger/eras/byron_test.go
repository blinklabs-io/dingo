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
	"crypto/ed25519"
	"crypto/sha3"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// Two distinct custom-network magics. The pair matters: a rule that only
// distinguished "mainnet" from "not mainnet" would treat these as the same
// network and accept an address minted for one on the other.
const (
	customNetworkA uint32 = 42
	customNetworkB uint32 = 43
)

// addressedOutput is a transaction output carrying a real address, which the
// shared testOutput does not.
type addressedOutput struct {
	amount  *big.Int
	address lcommon.Address
}

func (o addressedOutput) Address() lcommon.Address { return o.address }

func (o addressedOutput) Amount() *big.Int { return o.amount }

func (o addressedOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	return nil
}

func (o addressedOutput) Datum() *lcommon.Datum { return nil }

func (o addressedOutput) DatumHash() *lcommon.Blake2b256 { return nil }

func (o addressedOutput) Cbor() []byte { return nil }

func (o addressedOutput) Utxorpc() (*utxorpc.TxOutput, error) {
	return &utxorpc.TxOutput{}, nil
}

func (o addressedOutput) ScriptRef() lcommon.Script { return nil }

func (o addressedOutput) ToPlutusData() data.PlutusData {
	return data.NewConstr(0)
}

func (o addressedOutput) String() string { return "addressedOutput" }

// newByronAddressForNetwork builds a Byron address belonging to the given
// network. A nil magic produces the mainnet encoding, which carries no
// network attribute at all; a non-nil one carries that magic explicitly.
func newByronAddressForNetwork(
	t *testing.T,
	magic *uint32,
	hashByte byte,
) lcommon.Address {
	t.Helper()
	hash := make([]byte, lcommon.AddressHashSize)
	hash[0] = hashByte
	attr := lcommon.ByronAddressAttributes{}
	if magic != nil {
		value := *magic
		attr.Network = &value
	}
	addr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		hash,
		attr,
	)
	require.NoError(t, err)
	return addr
}

// newNetworkTestLedgerState builds a ledger state validating for the given
// protocol magic, with a fee policy that demands nothing so the network rule
// is what decides each case.
func newNetworkTestLedgerState(protocolMagic uint32) *mockLedgerState {
	ls := newMockLedgerState()
	ls.protocolMagic = protocolMagic
	ls.byronFeeSummand = 0
	ls.byronFeeMultiplier = 0
	return ls
}

// newOutputNetworkTx funds one input and pays the whole value to the supplied
// output addresses.
func newOutputNetworkTx(
	ls *mockLedgerState,
	addresses []lcommon.Address,
) *testByronTx {
	input := newTestInput(0x01, 0)
	const perOutput = 1_000_000
	ls.addUtxo(input, newTestOutput(uint64(len(addresses))*perOutput))
	outputs := make([]lcommon.TransactionOutput, 0, len(addresses))
	for _, addr := range addresses {
		outputs = append(outputs, addressedOutput{
			amount:  new(big.Int).SetUint64(perOutput),
			address: addr,
		})
	}
	return &testByronTx{
		inputs:  []lcommon.TransactionInput{input},
		outputs: outputs,
	}
}

// TestValidateTxByron_OutputNetworkMagic covers the case matrix the issue
// calls for, through the ValidateTxByron entry point.
func TestValidateTxByron_OutputNetworkMagic(t *testing.T) {
	t.Parallel()

	magicA := customNetworkA
	magicB := customNetworkB

	tests := []struct {
		name          string
		protocolMagic uint32
		// outputs is one entry per output: nil means the mainnet encoding.
		outputs      []*uint32
		wantError    bool
		wantIndex    int
		wantExpected *uint32
		wantActual   *uint32
	}{
		{
			name:          "mainnet to mainnet",
			protocolMagic: byron.MainnetProtocolMagic,
			outputs:       []*uint32{nil},
		},
		{
			name:          "mainnet to testnet",
			protocolMagic: byron.MainnetProtocolMagic,
			outputs:       []*uint32{&magicA},
			wantError:     true,
			wantActual:    &magicA,
		},
		{
			name:          "custom network to its own network",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicA},
		},
		{
			name:          "custom network A to network B",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicB},
			wantError:     true,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
		{
			name:          "custom network to mainnet encoding",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{nil},
			wantError:     true,
			wantExpected:  &magicA,
		},
		{
			// The three multi-output cases below pin the bad output at each
			// position in turn. A validator that only inspected the last
			// output (or only the first) would still pass a suite that put
			// the mismatch in just one place; PR review on found this
			// gap, since the original matrix only exercised "bad output
			// last".
			name:          "multi-output, bad output last",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicA, &magicA, &magicB},
			wantError:     true,
			wantIndex:     2,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
		{
			name:          "multi-output, bad output first",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicB, &magicA, &magicA},
			wantError:     true,
			wantIndex:     0,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
		{
			name:          "multi-output, bad output in the middle",
			protocolMagic: customNetworkA,
			outputs:       []*uint32{&magicA, &magicB, &magicA},
			wantError:     true,
			wantIndex:     1,
			wantExpected:  &magicA,
			wantActual:    &magicB,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ls := newNetworkTestLedgerState(test.protocolMagic)
			addresses := make([]lcommon.Address, 0, len(test.outputs))
			for i, magic := range test.outputs {
				addresses = append(
					addresses,
					newByronAddressForNetwork(t, magic, byte(0xB0+i)),
				)
			}
			tx := newOutputNetworkTx(ls, addresses)

			err := byronValidateOutputNetwork(tx, 0, ls, nil)
			if !test.wantError {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var mismatch NetworkMagicMismatchByronError
			require.ErrorAs(t, err, &mismatch)
			assert.Equal(t, test.wantIndex, mismatch.OutputIndex)
			if test.wantExpected == nil {
				assert.Nil(t, mismatch.Expected)
			} else {
				require.NotNil(t, mismatch.Expected)
				assert.Equal(t, *test.wantExpected, *mismatch.Expected)
			}
			if test.wantActual == nil {
				assert.Nil(t, mismatch.Actual)
			} else {
				require.NotNil(t, mismatch.Actual)
				assert.Equal(t, *test.wantActual, *mismatch.Actual)
			}
		})
	}
}

// TestValidateTxByron_OutputNetworkMagicThroughEntryPoint confirms the rule is
// reachable through ValidateTxByron and not merely as a standalone function,
// since a rule that is never registered enforces nothing.
func TestValidateTxByron_OutputNetworkMagicThroughEntryPoint(t *testing.T) {
	t.Parallel()

	magicB := customNetworkB
	ls := newNetworkTestLedgerState(customNetworkA)
	tx := newOutputNetworkTx(ls, []lcommon.Address{
		newByronAddressForNetwork(t, &magicB, 0xB0),
	})

	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	var mismatch NetworkMagicMismatchByronError
	require.ErrorAs(t, err, &mismatch)
}

// TestByronValidateOutputNetwork_SurfacesLookupFailure proves the rule fails
// closed when the protocol magic cannot be read, rather than skipping the
// network check.
func TestByronValidateOutputNetwork_SurfacesLookupFailure(t *testing.T) {
	t.Parallel()

	ls := newNetworkTestLedgerState(customNetworkA)
	ls.protocolMagicErr = errors.New("byron genesis unavailable")
	tx := newOutputNetworkTx(ls, []lcommon.Address{
		newByronAddressForNetwork(t, nil, 0xB0),
	})

	err := byronValidateOutputNetwork(tx, 0, ls, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get Byron protocol magic")
}

// TestByronValidateOutputNetwork_SkipsWithoutProvider preserves the behaviour
// the other Byron rules rely on: a lightweight ledger state that exposes no
// chain configuration must not make every transaction invalid.
func TestByronValidateOutputNetwork_SkipsWithoutProvider(t *testing.T) {
	t.Parallel()

	magicB := customNetworkB
	tx := &testByronTx{
		inputs: []lcommon.TransactionInput{newTestInput(0x01, 0)},
		outputs: []lcommon.TransactionOutput{addressedOutput{
			amount:  big.NewInt(1),
			address: newByronAddressForNetwork(t, &magicB, 0xB0),
		}},
	}

	require.NoError(
		t,
		byronValidateOutputNetwork(
			tx, 0, noProtocolMagicLedgerState{}, nil,
		),
	)
}

// noProtocolMagicLedgerState implements lcommon.LedgerState via the embedded
// nil interface but not ByronProtocolMagicProvider.
type noProtocolMagicLedgerState struct {
	lcommon.LedgerState
}

// The Byron transaction wire shapes below are assembled by hand because
// gouroboros implements UnmarshalCBOR for Byron bodies, inputs, and outputs
// but no matching MarshalCBOR. Building the real wire bytes and decoding them
// through byron.NewByronTransactionFromCbor is what lets these tests run
// against the production *byron.ByronTransaction type, which is the only type
// for which byronValidateWitnesses extracts redeem witnesses.

// byronInputInner is the tag-24 wrapped payload of a Byron transaction input.
type byronInputInner struct {
	cbor.StructAsArray
	TxId  lcommon.Blake2b256
	Index uint32
}

// byronInputWire is the outer [0, tag24(...)] Byron input encoding.
type byronInputWire struct {
	cbor.StructAsArray
	Id   int
	Cbor cbor.WrappedCbor
}

// byronOutputWire is the [address, amount] Byron output encoding.
type byronOutputWire struct {
	cbor.StructAsArray
	Address cbor.RawMessage
	Amount  uint64
}

// byronBodyWire is the [inputs, outputs, attributes] Byron body encoding.
type byronBodyWire struct {
	cbor.StructAsArray
	Inputs     []byronInputWire
	Outputs    []byronOutputWire
	Attributes cbor.RawMessage
}

// byronTxWire is the [body, witnesses] Byron transaction encoding.
type byronTxWire struct {
	cbor.StructAsArray
	Body cbor.RawMessage
	Twit cbor.IndefLengthList
}

// byronRedeemKey is an ed25519 keypair together with the Byron redeem address
// derived from its public key.
type byronRedeemKey struct {
	public  ed25519.PublicKey
	private ed25519.PrivateKey
	address lcommon.Address
}

// newByronRedeemKey derives a Byron redeem address from a deterministic
// ed25519 keypair, so a witness signed by the private key satisfies the
// address match that validateByronInputWitnesses performs.
func newByronRedeemKey(t *testing.T, seedByte byte) byronRedeemKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	private := ed25519.NewKeyFromSeed(seed)
	public, ok := private.Public().(ed25519.PublicKey)
	require.True(t, ok)
	address, err := lcommon.NewByronAddressRedeem(
		public,
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	return byronRedeemKey{
		public:  public,
		private: private,
		address: address,
	}
}

// byronEncodeBody assembles the Byron transaction body wire bytes for the
// given inputs and outputs.
func byronEncodeBody(
	t *testing.T,
	inputs []lcommon.TransactionInput,
	outputs []byronOutputWire,
) []byte {
	t.Helper()
	wireInputs := make([]byronInputWire, 0, len(inputs))
	for _, input := range inputs {
		inner, err := cbor.Encode(&byronInputInner{
			TxId:  input.Id(),
			Index: input.Index(),
		})
		require.NoError(t, err)
		wireInputs = append(wireInputs, byronInputWire{
			Id:   0,
			Cbor: cbor.WrappedCbor(inner),
		})
	}
	body, err := cbor.Encode(&byronBodyWire{
		Inputs:  wireInputs,
		Outputs: outputs,
		// An empty attributes map, the ordinary Byron case.
		Attributes: cbor.RawMessage{0xa0},
	})
	require.NoError(t, err)
	return body
}

// byronEncodeOutput builds a Byron output paying the given address.
func byronEncodeOutput(
	t *testing.T,
	addr lcommon.Address,
	amount uint64,
) byronOutputWire {
	t.Helper()
	addrCbor, err := cbor.Encode(&addr)
	require.NoError(t, err)
	return byronOutputWire{
		Address: addrCbor,
		Amount:  amount,
	}
}

// byronRedeemWitness builds a constructor-2 Byron redeem witness signing the
// canonical redeem message for the supplied body hash. When sign is false the
// signature is left invalid, so a caller can prove the witness requirement is
// still enforced.
func byronRedeemWitness(
	t *testing.T,
	key byronRedeemKey,
	protocolMagic uint32,
	bodyHash lcommon.Blake2b256,
	sign bool,
) cbor.Value {
	t.Helper()
	message, err := byronSignatureMessage(0x02, protocolMagic, bodyHash)
	require.NoError(t, err)
	signature := ed25519.Sign(key.private, message)
	if !sign {
		// Flip a bit so the signature is well-formed but does not verify.
		signature[0] ^= 0xff
	}
	payload, err := cbor.Encode([][]byte{key.public, signature})
	require.NoError(t, err)
	witness, err := cbor.Encode(
		[]any{uint64(lcommon.ByronAddressTypeRedeem), cbor.WrappedCbor(payload)},
	)
	require.NoError(t, err)
	var value cbor.Value
	_, err = cbor.Decode(witness, &value)
	require.NoError(t, err)
	return value
}

// byronRedeemTxCase describes one redeem transaction to assemble.
type byronRedeemTxCase struct {
	// keys supply one input each, and every input is funded at the key's
	// redeem address unless overridden by ordinary.
	keys []byronRedeemKey
	// ordinary, when set, funds the input at this index with a non-redeem
	// Byron address instead of its redeem address.
	ordinary map[int]lcommon.Address
	// fee is subtracted from the produced output, so fee 0 means the
	// transaction pays no fee at all.
	fee uint64
	// validSignatures controls whether the redeem witnesses verify.
	validSignatures bool
}

// buildByronRedeemTx assembles a real *byron.ByronTransaction with redeem
// witnesses and registers its inputs in the supplied ledger state. It returns
// the decoded transaction.
func buildByronRedeemTx(
	t *testing.T,
	ls *mockLedgerState,
	testCase byronRedeemTxCase,
) *byron.ByronTransaction {
	t.Helper()

	const perInput = 1_000_000

	inputs := make([]lcommon.TransactionInput, 0, len(testCase.keys))
	var consumed uint64
	for i, key := range testCase.keys {
		input := newTestInput(byte(i+1), 0)
		fundingAddr := key.address
		if override, ok := testCase.ordinary[i]; ok {
			fundingAddr = override
		}
		ls.addUtxo(
			input,
			newTestOutputWithAddress(perInput, fundingAddr),
		)
		inputs = append(inputs, input)
		consumed += perInput
	}

	// Pay the whole consumed value, less the requested fee, to an ordinary
	// Byron address. The Byron reference exempts the transaction based on the
	// addresses it consumes, not the address it produces.
	payTo := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xF1)
	outputs := []byronOutputWire{
		byronEncodeOutput(t, payTo, consumed-testCase.fee),
	}

	body := byronEncodeBody(t, inputs, outputs)
	bodyHash := lcommon.Blake2b256Hash(body)

	witnesses := make(cbor.IndefLengthList, 0, len(testCase.keys))
	for _, key := range testCase.keys {
		witnesses = append(
			witnesses,
			byronRedeemWitness(
				t,
				key,
				ls.protocolMagic,
				bodyHash,
				testCase.validSignatures,
			),
		)
	}

	txCbor, err := cbor.Encode(&byronTxWire{
		Body: body,
		Twit: witnesses,
	})
	require.NoError(t, err)

	tx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)
	// The signature covers the body hash, so the assembled body must be the
	// one the decoded transaction reports.
	require.Equal(t, bodyHash, tx.WireId())
	return tx
}

// newRedeemFeeLedgerState builds a ledger state with a non-zero Byron linear
// fee policy, so an unexempted zero-fee transaction is rejected.
func newRedeemFeeLedgerState() *mockLedgerState {
	ls := newMockLedgerState()
	ls.protocolMagic = 764824073
	ls.byronFeeSummand = 155_381_000_000_000
	ls.byronFeeMultiplier = 43_946_000_000
	return ls
}

// TestValidateTxByron_RedeemOnlyZeroFeeAccepted proves the redeem exemption
// through the ValidateTxByron application path on a real Byron transaction
// carrying valid redeem witnesses. This is the behavior that from-genesis
// replay of canonical Byron redemption transactions depends on.
func TestValidateTxByron_RedeemOnlyZeroFeeAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		keys int
	}{
		{name: "one redeem input with zero fee", keys: 1},
		{name: "multiple redeem inputs with zero fee", keys: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ls := newRedeemFeeLedgerState()
			keys := make([]byronRedeemKey, 0, test.keys)
			for i := range test.keys {
				keys = append(keys, newByronRedeemKey(t, byte(0xA0+i)))
			}
			tx := buildByronRedeemTx(t, ls, byronRedeemTxCase{
				keys:            keys,
				fee:             0,
				validSignatures: true,
			})

			require.NoError(t, ValidateTxByron(tx, 0, ls, nil))
		})
	}
}

// TestValidateTxByron_RedeemPlusOrdinaryZeroFeeRejected proves a single
// non-redeem input removes the exemption on the application path, matching
// isRedeemUTxO rather than "any redeem input".
func TestValidateTxByron_RedeemPlusOrdinaryZeroFeeRejected(t *testing.T) {
	t.Parallel()

	ls := newRedeemFeeLedgerState()
	keys := []byronRedeemKey{
		newByronRedeemKey(t, 0xA0),
		newByronRedeemKey(t, 0xA1),
	}
	// Fund the second input at an ordinary Byron address. Its redeem witness
	// is still supplied, so the rejection is attributable to the fee rule.
	ordinary := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xC1)
	tx := buildByronRedeemTx(t, ls, byronRedeemTxCase{
		keys:            keys,
		ordinary:        map[int]lcommon.Address{1: ordinary},
		fee:             0,
		validSignatures: true,
	})

	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, err, &feeErr)
	assert.Zero(t, feeErr.Actual.Sign())
	assert.Positive(t, feeErr.Required.Sign())
}

// TestValidateTxByron_RedeemOnlyPreservesWitnessRequirement proves the
// exemption does not weaken the redeem-witness requirement: a redeem-only
// zero-fee transaction whose witness signature does not verify is still
// rejected, and not with a fee error.
func TestValidateTxByron_RedeemOnlyPreservesWitnessRequirement(
	t *testing.T,
) {
	t.Parallel()

	ls := newRedeemFeeLedgerState()
	tx := buildByronRedeemTx(t, ls, byronRedeemTxCase{
		keys:            []byronRedeemKey{newByronRedeemKey(t, 0xA0)},
		fee:             0,
		validSignatures: false,
	})

	err := ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	var feeErr FeeTooLowByronError
	require.NotErrorAs(t, err, &feeErr)
	assert.Contains(t, err.Error(), "invalid vkey signature")
}

// TestValidateTxByron_RedeemOnlyNegativeFeeRejected proves the zero
// requirement is still a comparison: a redeem-only transaction that produces
// more than it consumes does not pass the fee rule.
func TestValidateTxByron_RedeemOnlyNegativeFeeRejected(t *testing.T) {
	t.Parallel()

	ls := newRedeemFeeLedgerState()
	key := newByronRedeemKey(t, 0xA0)
	input := newTestInput(0x01, 0)
	ls.addUtxo(input, newTestOutputWithAddress(1_000_000, key.address))

	payTo := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xF1)
	// Produce more than the single input consumes.
	outputs := []byronOutputWire{byronEncodeOutput(t, payTo, 1_500_000)}
	body := byronEncodeBody(
		t,
		[]lcommon.TransactionInput{input},
		outputs,
	)
	bodyHash := lcommon.Blake2b256Hash(body)
	txCbor, err := cbor.Encode(&byronTxWire{
		Body: body,
		Twit: cbor.IndefLengthList{
			byronRedeemWitness(t, key, ls.protocolMagic, bodyHash, true),
		},
	})
	require.NoError(t, err)
	tx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)

	err = ValidateTxByron(tx, 0, ls, nil)
	require.Error(t, err)
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, err, &feeErr)
	assert.Negative(t, feeErr.Actual.Sign())
}

// byronTestKey is a Byron signing key together with the address it controls:
// a VKWitness key (extended with a chain code) or a redeem key.
type byronTestKey struct {
	redeem    bool
	private   ed25519.PrivateKey
	public    ed25519.PublicKey
	chainCode []byte
	address   lcommon.Address
}

func newByronTestKey(t *testing.T, seedByte byte, redeem bool) byronTestKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	private := ed25519.NewKeyFromSeed(seed)
	public, ok := private.Public().(ed25519.PublicKey)
	require.True(t, ok)
	key := byronTestKey{redeem: redeem, private: private, public: public}
	if redeem {
		key.address = byronTestAddress(t, key, []byte{0xa0}, []byte{0xa0})
		return key
	}
	key.chainCode = make([]byte, 32)
	for i := range key.chainCode {
		key.chainCode[i] = seedByte ^ 0x5a
	}
	key.address = byronTestAddress(t, key, []byte{0xa0}, []byte{0xa0})
	return key
}

// byronTestAddress builds a Byron address for key whose root is hashed over
// rootAttrs while the address itself carries addrAttrs on the wire, so a test
// can separate the canonical attribute encoding from the literal one.
func byronTestAddress(
	t *testing.T,
	key byronTestKey,
	rootAttrs []byte,
	addrAttrs []byte,
) lcommon.Address {
	t.Helper()
	addrType := uint64(lcommon.ByronAddressTypePubkey)
	spending := append(append([]byte{}, key.public...), key.chainCode...)
	if key.redeem {
		addrType = lcommon.ByronAddressTypeRedeem
		spending = key.public
	}
	rootCbor, err := cbor.Encode([]any{
		addrType,
		[]any{addrType, spending},
		cbor.RawMessage(rootAttrs),
	})
	require.NoError(t, err)
	digest := sha3.Sum256(rootCbor)
	root := lcommon.Blake2b224Hash(digest[:])
	inner, err := cbor.Encode([]any{
		root.Bytes(),
		cbor.RawMessage(addrAttrs),
		addrType,
	})
	require.NoError(t, err)
	outer, err := cbor.Encode([]any{
		cbor.WrappedCbor(inner),
		uint64(crc32.ChecksumIEEE(inner)),
	})
	require.NoError(t, err)
	addr, err := lcommon.NewAddressFromBytes(outer)
	require.NoError(t, err)
	return addr
}

func (k byronTestKey) witness(
	t *testing.T,
	protocolMagic uint32,
	bodyHash lcommon.Blake2b256,
) cbor.Value {
	t.Helper()
	tag, ctor := byte(0x01), uint64(lcommon.ByronAddressTypePubkey)
	publicKey := append(append([]byte{}, k.public...), k.chainCode...)
	if k.redeem {
		tag, ctor = 0x02, lcommon.ByronAddressTypeRedeem
		publicKey = k.public
	}
	message, err := byronSignatureMessage(tag, protocolMagic, bodyHash)
	require.NoError(t, err)
	payload, err := cbor.Encode(
		[][]byte{publicKey, ed25519.Sign(k.private, message)},
	)
	require.NoError(t, err)
	witness, err := cbor.Encode([]any{ctor, cbor.WrappedCbor(payload)})
	require.NoError(t, err)
	var value cbor.Value
	_, err = cbor.Decode(witness, &value)
	require.NoError(t, err)
	return value
}

// byronTestTx describes a real Byron transaction to assemble.
type byronTestTx struct {
	inputs     []lcommon.TransactionInput
	outputs    []byronOutputWire
	attributes []byte
	signers    []byronTestKey
}

func (b byronTestTx) build(
	t *testing.T,
	protocolMagic uint32,
) *byron.ByronTransaction {
	t.Helper()
	body := byronEncodeBody(t, b.inputs, b.outputs)
	if b.attributes != nil {
		wireInputs := make([]byronInputWire, 0, len(b.inputs))
		for _, input := range b.inputs {
			inner, err := cbor.Encode(&byronInputInner{
				TxId:  input.Id(),
				Index: input.Index(),
			})
			require.NoError(t, err)
			wireInputs = append(wireInputs, byronInputWire{
				Cbor: cbor.WrappedCbor(inner),
			})
		}
		var err error
		body, err = cbor.Encode(&byronBodyWire{
			Inputs:     wireInputs,
			Outputs:    b.outputs,
			Attributes: b.attributes,
		})
		require.NoError(t, err)
	}
	bodyHash := lcommon.Blake2b256Hash(body)
	witnesses := make(cbor.IndefLengthList, 0, len(b.signers))
	for _, signer := range b.signers {
		witnesses = append(
			witnesses,
			signer.witness(t, protocolMagic, bodyHash),
		)
	}
	txCbor, err := cbor.Encode(&byronTxWire{Body: body, Twit: witnesses})
	require.NoError(t, err)
	tx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.Equal(t, bodyHash, tx.WireId())
	return tx
}

func newByronRulesLedgerState() *mockLedgerState {
	ls := newMockLedgerState()
	ls.protocolMagic = 764824073
	return ls
}

func fundByronInput(
	t *testing.T,
	ls *mockLedgerState,
	seed byte,
	key byronTestKey,
	amount uint64,
) lcommon.TransactionInput {
	t.Helper()
	input := newTestInput(seed, 0)
	ls.addUtxo(input, newTestOutputWithAddress(amount, key.address))
	return input
}

// TestValidateTxByron_PositionalWitnesses covers: witness i must
// authorize input i.
func TestValidateTxByron_PositionalWitnesses(t *testing.T) {
	t.Parallel()
	payTo := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xF1)
	pairs := []struct {
		name string
		a, b byronTestKey
	}{
		{
			"two VK witnesses",
			newByronTestKey(t, 0x11, false),
			newByronTestKey(t, 0x12, false),
		},
		{
			"redeem and VK witnesses",
			newByronTestKey(t, 0x13, true),
			newByronTestKey(t, 0x14, false),
		},
		{
			"two redeem witnesses",
			newByronTestKey(t, 0x15, true),
			newByronTestKey(t, 0x16, true),
		},
	}
	for _, pair := range pairs {
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			ls := newByronRulesLedgerState()
			inputs := []lcommon.TransactionInput{
				fundByronInput(t, ls, 0x01, pair.a, 1_000_000),
				fundByronInput(t, ls, 0x02, pair.b, 1_000_000),
			}
			outputs := []byronOutputWire{byronEncodeOutput(t, payTo, 1_800_000)}
			ordered := byronTestTx{
				inputs:  inputs,
				outputs: outputs,
				signers: []byronTestKey{pair.a, pair.b},
			}.build(t, ls.protocolMagic)
			require.NoError(t, ValidateTxByron(ordered, 0, ls, nil))

			swapped := byronTestTx{
				inputs:  inputs,
				outputs: outputs,
				signers: []byronTestKey{pair.b, pair.a},
			}.build(t, ls.protocolMagic)
			var wrongKey WitnessWrongKeyByronError
			require.ErrorAs(t, ValidateTxByron(swapped, 0, ls, nil), &wrongKey)
			assert.Equal(t, 0, wrongKey.InputIndex)
		})
	}
}

// TestValidateTxByron_RepeatedInput covers: a repeated input is valid,
// and the input balance counts it once.
func TestValidateTxByron_RepeatedInput(t *testing.T) {
	t.Parallel()
	payTo := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xF1)
	key := newByronTestKey(t, 0x21, false)
	ls := newByronRulesLedgerState()
	x := fundByronInput(t, ls, 0x01, key, 1_000)
	build := func(output uint64) *byron.ByronTransaction {
		return byronTestTx{
			inputs:  []lcommon.TransactionInput{x, x},
			outputs: []byronOutputWire{byronEncodeOutput(t, payTo, output)},
			signers: []byronTestKey{key, key},
		}.build(t, ls.protocolMagic)
	}
	require.NoError(t, ValidateTxByron(build(800), 0, ls, nil))
	var notConserved ValueNotConservedByronError
	require.ErrorAs(t, ValidateTxByron(build(1_500), 0, ls, nil), &notConserved,
		"a repeated input must not be counted twice")
	assert.Equal(t, big.NewInt(1_000), notConserved.Consumed)
}

// TestValidateTxByron_LovelaceBounds covers: balances are summed as
// bounded Lovelace, so an aggregate above 45e15 fails.
func TestValidateTxByron_LovelaceBounds(t *testing.T) {
	t.Parallel()
	const maxLovelace = byronMaxLovelace
	build := func(inputs []uint64, output uint64) (*testByronTx, *mockLedgerState) {
		ls := newMockLedgerState()
		tx := &testByronTx{
			outputs: []lcommon.TransactionOutput{newTestOutput(output)},
		}
		for i, amount := range inputs {
			input := newTestInput(byte(i+1), 0)
			ls.addUtxo(input, newTestOutput(amount))
			tx.inputs = append(tx.inputs, input)
		}
		return tx, ls
	}
	var bound LovelaceBoundByronError

	tx, ls := build([]uint64{25e15, 20e15}, 44e15)
	require.NoError(t, byronValidateValueConserved(tx, 0, ls, nil))
	tx, ls = build([]uint64{25e15, 20e15 + 1}, 44e15)
	require.ErrorAs(t, byronValidateValueConserved(tx, 0, ls, nil), &bound)
	assert.Equal(t, "input balance", bound.Balance)
	tx, ls = build([]uint64{30e15, 20e15}, 44e15)
	require.ErrorAs(t, ValidateTxByron(tx, 0, ls, nil), &bound)

	tx, ls = build([]uint64{maxLovelace}, maxLovelace)
	require.NoError(t, byronValidateValueConserved(tx, 0, ls, nil))
	tx, ls = build([]uint64{maxLovelace}, maxLovelace+1)
	require.ErrorAs(t, byronValidateValueConserved(tx, 0, ls, nil), &bound)
	assert.Equal(t, "output balance", bound.Balance)
}

// TestByronMinFee covers: summand div 10^9 plus the ceiling of the
// exact rational multiplier times the size.
func TestByronMinFee(t *testing.T) {
	t.Parallel()
	tests := []struct {
		summand, multiplier int64
		size                uint64
		want                int64
	}{
		{0, 0, 100, 0},
		{1, 0, 100, 0},
		{999_999_999, 0, 100, 0},
		{1_000_000_000, 0, 100, 1},
		{1_000_000_001, 0, 100, 1},
		{0, 500_000_000, 3, 2},
		{0, 1, 1, 1},
		{999_999_999, 1, 1, 1},
		{155_381_000_000_000, 43_946_000_000, 200, 164_171},
	}
	for _, test := range tests {
		params, err := byronGenesisTestParams(
			test.summand, test.multiplier, 0,
		)
		require.NoError(t, err)
		got, err := params.MinFee(test.size)
		require.NoError(t, err)
		assert.Zero(t, big.NewInt(test.want).Cmp(got),
			"got %s for summand %d multiplier %d size %d", got,
			test.summand, test.multiplier, test.size)
	}

	// Through ValidateTxByron: summand 1,000,000,001 requires 1, not 2.
	ls := newMockLedgerState()
	ls.byronFeeSummand = 1_000_000_001
	input := newTestInput(0x01, 0)
	ls.addUtxo(input, newTestOutput(1_000))
	for _, test := range []struct {
		output  uint64
		wantErr bool
	}{{999, false}, {1_000, true}} {
		tx := &testByronTx{
			inputs:  []lcommon.TransactionInput{input},
			outputs: []lcommon.TransactionOutput{newTestOutput(test.output)},
			cbor:    make([]byte, 10),
		}
		err := ValidateTxByron(tx, 0, ls, nil)
		if !test.wantErr {
			require.NoError(t, err)
			continue
		}
		var feeErr FeeTooLowByronError
		require.ErrorAs(t, err, &feeErr)
		assert.Equal(t, big.NewInt(1), feeErr.Required)
	}
}

// TestValidateTxByron_MaxTxSize covers: ppMaxTxSize bounds the
// serialized TxAux, whatever fee it pays.
func TestValidateTxByron_MaxTxSize(t *testing.T) {
	t.Parallel()
	const maxTxSize = 64
	for _, test := range []struct {
		size    int
		wantErr bool
	}{{maxTxSize - 1, false}, {maxTxSize, false}, {maxTxSize + 1, true}} {
		ls := newMockLedgerState()
		ls.byronMaxTxSize = maxTxSize
		input := newTestInput(0x01, 0)
		ls.addUtxo(input, newTestOutput(1_000_000))
		tx := &testByronTx{
			inputs: []lcommon.TransactionInput{input},
			// A 999,000 lovelace fee covers any fee policy.
			outputs: []lcommon.TransactionOutput{newTestOutput(1_000)},
			cbor:    make([]byte, test.size),
		}
		err := ValidateTxByron(tx, 0, ls, nil)
		if !test.wantErr {
			require.NoError(t, err, "size %d", test.size)
			continue
		}
		var tooLarge TxTooLargeByronError
		require.ErrorAs(t, err, &tooLarge, "size %d", test.size)
		assert.Equal(t, uint64(maxTxSize+1), tooLarge.Size)
	}
}

// TestValidateTxByron_UnknownAttributes covers for transaction
// attributes: the unknown values must total fewer than 128 bytes.
func TestValidateTxByron_UnknownAttributes(t *testing.T) {
	t.Parallel()
	payTo := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xF1)
	key := newByronTestKey(t, 0x31, false)
	for _, test := range []struct {
		name    string
		attrs   map[uint8][]byte
		wantErr bool
	}{
		{"127 bytes", map[uint8][]byte{5: make([]byte, 127)}, false},
		{"128 bytes", map[uint8][]byte{5: make([]byte, 128)}, true},
		{"entries crossing the limit", map[uint8][]byte{
			5: make([]byte, 64),
			6: make([]byte, 64),
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ls := newByronRulesLedgerState()
			input := fundByronInput(t, ls, 0x01, key, 1_000_000)
			attrs, err := cbor.Encode(test.attrs)
			require.NoError(t, err)
			tx := byronTestTx{
				inputs: []lcommon.TransactionInput{input},
				outputs: []byronOutputWire{
					byronEncodeOutput(t, payTo, 900_000),
				},
				attributes: attrs,
				signers:    []byronTestKey{key},
			}.build(t, ls.protocolMagic)
			err = ValidateTxByron(tx, 0, ls, nil)
			if !test.wantErr {
				require.NoError(t, err)
				return
			}
			var unknown UnknownAttributesByronError
			require.ErrorAs(t, err, &unknown)
		})
	}
}

// TestValidateTxByron_UnknownAddressAttributes covers for output
// addresses. A derivation path is a recognized attribute and never counts.
func TestValidateTxByron_UnknownAddressAttributes(t *testing.T) {
	t.Parallel()
	hash := make([]byte, lcommon.AddressHashSize)
	derivation, err := cbor.Encode(make([]byte, 200))
	require.NoError(t, err)
	for _, test := range []struct {
		name    string
		attrs   lcommon.ByronAddressAttributes
		wantErr bool
	}{
		{"127 bytes", lcommon.ByronAddressAttributes{
			Unparsed: map[uint8][]byte{7: make([]byte, 127)},
		}, false},
		{"128 bytes", lcommon.ByronAddressAttributes{
			Unparsed: map[uint8][]byte{7: make([]byte, 128)},
		}, true},
		{"entries crossing the limit", lcommon.ByronAddressAttributes{
			Unparsed: map[uint8][]byte{7: make([]byte, 100), 8: make([]byte, 28)},
		}, true},
		{"large derivation path", lcommon.ByronAddressAttributes{
			Payload: derivation,
		}, false},
	} {
		addr, err := lcommon.NewByronAddressFromParts(
			lcommon.ByronAddressTypePubkey, hash, test.attrs,
		)
		require.NoError(t, err)
		tx := &testByronTx{
			inputs: []lcommon.TransactionInput{newTestInput(0x01, 0)},
			outputs: []lcommon.TransactionOutput{
				newTestOutput(1),
				newTestOutputWithAddress(1, addr),
			},
		}
		err = ValidateTxByron(tx, 0, nil, nil)
		if !test.wantErr {
			require.NoError(t, err, test.name)
			continue
		}
		var unknown UnknownAddressAttributesByronError
		require.ErrorAs(t, err, &unknown, test.name)
		assert.Equal(t, 1, unknown.OutputIndex, test.name)
	}
}

// TestValidateTxByron_WitnessRootUsesCanonicalAttributes covers: the
// root a witness is checked against is hashed over the canonical encoding of
// the address's decoded attributes, not the bytes it carries on the wire.
func TestValidateTxByron_WitnessRootUsesCanonicalAttributes(t *testing.T) {
	t.Parallel()
	// An empty map with a non-shortest two-byte length.
	nonShortest := []byte{0xb9, 0x00, 0x00}
	payTo := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xF1)
	for _, redeem := range []bool{false, true} {
		for _, test := range []struct {
			name      string
			rootAttrs []byte
			wantErr   bool
		}{
			{"canonical root", []byte{0xa0}, false},
			{"root over the literal bytes", nonShortest, true},
		} {
			key := newByronTestKey(t, 0x41, redeem)
			key.address = byronTestAddress(t, key, test.rootAttrs, nonShortest)
			ls := newByronRulesLedgerState()
			input := fundByronInput(t, ls, 0x01, key, 1_000_000)
			tx := byronTestTx{
				inputs: []lcommon.TransactionInput{input},
				outputs: []byronOutputWire{
					byronEncodeOutput(t, payTo, 900_000),
				},
				signers: []byronTestKey{key},
			}.build(t, ls.protocolMagic)
			err := ValidateTxByron(tx, 0, ls, nil)
			if !test.wantErr {
				require.NoError(t, err, "redeem %v %s", redeem, test.name)
				continue
			}
			var wrongKey WitnessWrongKeyByronError
			require.ErrorAs(
				t,
				err,
				&wrongKey,
				"redeem %v %s",
				redeem,
				test.name,
			)
		}
	}
}

func testByronGenesis(slotDuration string, k int) []byte {
	return []byte(fmt.Sprintf(`{
		"avvmDistr": {},
		"blockVersionData": {
			"heavyDelThd":"300000000000","maxBlockSize":"2000000",
			"maxHeaderSize":"2000000","maxProposalSize":"700",
			"maxTxSize":"4096","mpcThd":"20000000000000",
			"scriptVersion":0,"slotDuration":%q,
			"softforkRule":{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"},
			"txFeePolicy":{"multiplier":"43946000000","summand":"155381000000000"},
			"unlockStakeEpoch":"18446744073709551615","updateImplicit":"10000",
			"updateProposalThd":"100000000000000","updateVoteThd":"1000000000000"
		},
		"protocolConsts":{"k":%d,"protocolMagic":164},"startTime":1506203091,
		"bootStakeholders":{"e551ed0645140f2fc9975a7cee7dd89380d918e091869b19332bcb54":1},
		"heavyDelegation":{"e551ed0645140f2fc9975a7cee7dd89380d918e091869b19332bcb54":{"cert":"0f28871316b43f19773332984976f5d5838ed55b165c5afef4668b9efaee83c45744af47c41cda4ca3579443e3438bc6c6443106a5b2e8f820ab569bc7c1a907","delegatePk":"4yJc1LKn25BXdt5RFiMPKymb2P+V6qnKxbdi8CzHePekiNiMtPIupsmS+TbnZ43NMP6M7QfOEsLou5730/0Dsg==","issuerPk":"U3i0cs1QPT6ajXHJpZj1Aqj0Nkh2bhqkOOEXkd/MmcyH8XDJVZ2TchvZyt2m5PKsrgnqQWIv/dWmWQGwBB065Q==","omega":0}},
		"nonAvvmBalances":{}
	}`, slotDuration, k))
}

// TestEpochLengthByronRejectsNegativeSlotDuration covers: the
// upstream gouroboros parser accepts a signed slotDuration, so a genesis
// carrying slotDuration "-1" must not silently wrap to a huge unsigned
// duration at the uint(...) conversion in EpochLengthByron. This uses
// LoadByronGenesisFromReader, the config package's own test-only bypass of
// the CardanoNodeConfig.loadGenesisConfigs load path, so it exercises the
// guard at EpochLengthByron itself rather than the earlier guard the load
// path now also has (config/cardano.TestLoadGenesisConfigsRejectsNegativeByronSlotDuration).
func TestEpochLengthByronRejectsNegativeSlotDuration(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadByronGenesisFromReader(
		bytes.NewReader(testByronGenesis("20000", 2160)),
	)
	require.NoError(t, err)
	cfg.ByronGenesis().BlockVersionData.SlotDuration = -1

	_, _, err = EpochLengthByron(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "slotDuration")
	require.Contains(t, err.Error(), "negative")
}

// TestEpochLengthByronRejectsNonPositiveK covers the same signed-to-unsigned
// conversion class of bug as TestEpochLengthByronRejectsNegativeSlotDuration,
// but for ProtocolConsts.K: EpochLengthByron's uint(K*10) had no local guard
// of its own, even though three other call sites (config/cardano's
// validateSecurityParameters, internal/node/load.go's
// loadSecurityParamForConfig, and this package's own StabilityWindowForEra)
// already reject a non-positive k before it would reach a production call
// here.
func TestEpochLengthByronRejectsNonPositiveK(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadByronGenesisFromReader(
		bytes.NewReader(testByronGenesis("20000", 2160)),
	)
	require.NoError(t, err)
	cfg.ByronGenesis().ProtocolConsts.K = -1

	_, _, err = EpochLengthByron(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "protocolConsts.k")
}

// TestEpochLengthByronAcceptsNonNegativeSlotDuration is the companion
// positive case: an ordinary genesis must still compute an epoch length, so
// the new guard only rejects negative values.
func TestEpochLengthByronAcceptsNonNegativeSlotDuration(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadByronGenesisFromReader(
		bytes.NewReader(testByronGenesis("20000", 2160)),
	)
	require.NoError(t, err)

	slotDuration, epochLength, err := EpochLengthByron(cfg)
	require.NoError(t, err)
	require.Equal(t, uint(20000), slotDuration)
	require.Equal(t, uint(21600), epochLength)
}

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

// Produced overrides the method promoted from the embedded ByronTransaction,
// which would read its empty body instead of t.outputs.
func (t *testByronTx) Produced() []lcommon.Utxo {
	ret := make([]lcommon.Utxo, 0, len(t.outputs))
	for idx, output := range t.outputs {
		ret = append(ret, lcommon.Utxo{
			//nolint:gosec // G115: test transactions are small
			Id:     newTestInput(0xee, uint32(idx)),
			Output: output,
		})
	}
	return ret
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
	// Byron witnesses exposed through TransactionWitnessSet still require
	// their constructor-specific signature domain.

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
// this rule: a transaction with a genuine, matching witness must not
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

// TestValidateTxByron_DuplicateInputs pins: the reference keeps
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
	protocolMagicErr error
	// byronFeeSummand, byronFeeMultiplier and byronMaxTxSize are the
	// genesis values ByronProtocolParameters loads, the fee policy scaled by
	// 10^9; a zero byronMaxTxSize means no limit.
	byronFeeSummand      int64
	byronFeeMultiplier   int64
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
	// check without a real *ledger.LedgerView.
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
	// plutusEvalContextCache backs PlutusEvalContextCache, letting a test opt
	// a *mockLedgerState into eras.PlutusEvalContextCacheProvider. nil (the
	// zero value) preserves every existing test's uncached behavior.
	plutusEvalContextCache *PlutusEvalContextCache
}

// PlutusEvalContextCache implements eras.PlutusEvalContextCacheProvider, the
// same way *ledger.LedgerView does in production.
func (m *mockLedgerState) PlutusEvalContextCache() *PlutusEvalContextCache {
	return m.plutusEvalContextCache
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

// ByronProtocolParameters builds genesis-loaded parameters from the mock's
// raw genesis fee policy, scaled by 10^9, and size limit.
func (m *mockLedgerState) ByronProtocolParameters() (
	*ByronProtocolParameters,
	error,
) {
	return byronGenesisTestParams(
		m.byronFeeSummand, m.byronFeeMultiplier, m.byronMaxTxSize,
	)
}

// byronGenesisTestParams loads a Byron genesis carrying the given raw fee
// policy and maxTxSize, zero meaning no practical limit.
func byronGenesisTestParams(
	summandNano, multiplierNano int64,
	maxTxSize uint64,
) (*ByronProtocolParameters, error) {
	genesis := &byron.ByronGenesis{}
	genesis.BlockVersionData.TxFeePolicy.Summand = summandNano
	genesis.BlockVersionData.TxFeePolicy.Multiplier = multiplierNano
	params, err := NewByronProtocolParametersFromGenesis(genesis)
	if err != nil {
		return nil, err
	}
	params.MaxTxSize = new(big.Int).SetUint64(maxTxSize)
	if maxTxSize == 0 {
		params.MaxTxSize = new(big.Int).SetUint64(math.MaxUint64)
	}
	return params, nil
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

// feePolicyUpdate returns the update that adopts a fee policy of summand
// lovelace and multiplier lovelace per byte.
func feePolicyUpdate(
	summandLovelace, multiplierNano int64,
) byron.ByronUpdateProposalBlockVersionMod {
	return byron.ByronUpdateProposalBlockVersionMod{
		TxFeePolicy: []byron.ByronTxFeePolicy{{
			SummandNano:    big.NewInt(summandLovelace * byronFeePolicyScale),
			MultiplierNano: big.NewInt(multiplierNano),
		}},
	}
}

// TestValidateTxByron_AdoptedFeePolicyChangesRequiredFee covers: the
// minimum fee is the summand plus the per-byte multiplier of the policy the
// caller passes, whichever of the two an adopted update changed. Genesis is
// 155,381 + 43.946 * 200 = 164,171 for a 200-byte transaction.
func TestValidateTxByron_AdoptedFeePolicyChangesRequiredFee(t *testing.T) {
	t.Parallel()
	genesis, err := NewByronProtocolParametersFromGenesis(mainnetByronGenesis())
	require.NoError(t, err)
	const size = 200
	input := newTestInput(0x01, 0)
	newLS := func() *mockLedgerState {
		ls := newMockLedgerState()
		ls.byronFeeSummand = 155_381_000_000_000
		ls.byronFeeMultiplier = 43_946_000_000
		ls.byronMaxTxSize = 4_096
		ls.addUtxo(input, newTestOutput(10_000_000))
		return ls
	}
	tx := func(fee uint64) *testByronTx {
		return &testByronTx{
			inputs: []lcommon.TransactionInput{input},
			outputs: []lcommon.TransactionOutput{
				newTestOutput(10_000_000 - fee),
			},
			cbor: make([]byte, size),
		}
	}
	const genesisFee = uint64(164_171)
	require.NoError(t, ValidateTxByron(tx(genesisFee), 0, newLS(), nil))
	var genesisErr FeeTooLowByronError
	require.ErrorAs(
		t, ValidateTxByron(tx(genesisFee-1), 0, newLS(), nil), &genesisErr,
	)
	require.Equal(t, big.NewInt(int64(genesisFee)), genesisErr.Required)

	tests := []struct {
		name            string
		summand         int64
		multiplierNano  int64
		required        uint64
		genesisAccepts  bool
		adoptedAccepts  bool
		probeFeeAtBound uint64
	}{
		{
			"raise summand only",
			200_000,
			43_946_000_000,
			208_790,
			true,
			false,
			genesisFee,
		},
		{
			"lower summand only",
			100_000,
			43_946_000_000,
			108_790,
			false,
			true,
			108_790,
		},
		{
			"raise multiplier only",
			155_381,
			100_000_000_000,
			175_381,
			true,
			false,
			genesisFee,
		},
		{
			"lower multiplier only",
			155_381,
			10_000_000_000,
			157_381,
			false,
			true,
			157_381,
		},
		{
			"raise both",
			300_000,
			200_000_000_000,
			340_000,
			true,
			false,
			genesisFee,
		},
		{"lower both", 50_000, 5_000_000_000, 51_000, false, true, 51_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adopted, err := genesis.ApplyUpdate(
				feePolicyUpdate(test.summand, test.multiplierNano),
			)
			require.NoError(t, err)
			require.NoError(
				t,
				ValidateTxByron(tx(test.required), 0, newLS(), adopted),
				"the adopted minimum is enough",
			)
			var feeErr FeeTooLowByronError
			require.ErrorAs(
				t,
				ValidateTxByron(tx(test.required-1), 0, newLS(), adopted),
				&feeErr,
				"one below the adopted minimum",
			)
			assert.Equal(
				t,
				new(big.Int).SetUint64(test.required),
				feeErr.Required,
			)
			// The same fee is judged by the passed policy, not by genesis.
			genesisVerdict := ValidateTxByron(
				tx(test.probeFeeAtBound),
				0,
				newLS(),
				nil,
			)
			adoptedVerdict := ValidateTxByron(
				tx(test.probeFeeAtBound),
				0,
				newLS(),
				adopted,
			)
			assert.Equal(t, test.genesisAccepts, genesisVerdict == nil)
			assert.Equal(t, test.adoptedAccepts, adoptedVerdict == nil)
		})
	}
}

// TestByronFeePolicySuccessiveAdoptionsUseLatest covers: each adopted
// update replaces the whole policy, so the latest one alone sets the fee.
func TestByronFeePolicySuccessiveAdoptionsUseLatest(t *testing.T) {
	t.Parallel()
	genesis, err := NewByronProtocolParametersFromGenesis(mainnetByronGenesis())
	require.NoError(t, err)
	first, err := genesis.ApplyUpdate(feePolicyUpdate(200_000, 100_000_000_000))
	require.NoError(t, err)
	second, err := first.ApplyUpdate(feePolicyUpdate(120_000, 20_000_000_000))
	require.NoError(t, err)
	// A later update of another parameter keeps the latest policy.
	third, err := second.ApplyUpdate(byron.ByronUpdateProposalBlockVersionMod{
		MaxTxSize: []*big.Int{big.NewInt(8_192)},
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name   string
		params *ByronProtocolParameters
		want   int64
	}{
		{"genesis", genesis, 164_171},
		{"first", first, 220_000},
		{"second", second, 124_000},
		{"unrelated update afterwards", third, 124_000},
	} {
		required, err := test.params.MinFee(200)
		require.NoError(t, err, test.name)
		assert.Equal(t, big.NewInt(test.want), required, test.name)
	}
}

// TestByronGenesisFeePolicyNormalizationUnlikeAdopted pins the two decodings
// keeps apart: genesis truncates the summand to a whole lovelace, while
// an adopted on-chain policy rounds it half to even.
func TestByronGenesisFeePolicyNormalizationUnlikeAdopted(t *testing.T) {
	t.Parallel()
	genesisJSON := mainnetByronGenesis()
	genesisJSON.BlockVersionData.TxFeePolicy.Summand = 155_381_999_999_999
	genesis, err := NewByronProtocolParametersFromGenesis(genesisJSON)
	require.NoError(t, err)
	assert.Equal(t, uint64(155_381), genesis.TxFeeSummand)

	adopted, err := genesis.ApplyUpdate(
		byron.ByronUpdateProposalBlockVersionMod{
			TxFeePolicy: []byron.ByronTxFeePolicy{{
				SummandNano:    big.NewInt(155_381_999_999_999),
				MultiplierNano: big.NewInt(43_946_000_000),
			}},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(155_382), adopted.TxFeeSummand)
}

// TestValidateTxByron_RedeemOnlyExemptionUnderAdoptedPolicy covers: the
// redeem-only zero-fee exception holds however high the adopted policy sets
// the minimum, and a transaction with any ordinary input owes that minimum.
func TestValidateTxByron_RedeemOnlyExemptionUnderAdoptedPolicy(t *testing.T) {
	t.Parallel()
	genesis, err := byronGenesisTestParams(
		155_381_000_000_000, 43_946_000_000, 0,
	)
	require.NoError(t, err)
	adopted, err := genesis.ApplyUpdate(
		feePolicyUpdate(500_000, 200_000_000_000),
	)
	require.NoError(t, err)

	ls := newRedeemFeeLedgerState()
	redeemOnly := buildByronRedeemTx(t, ls, byronRedeemTxCase{
		keys:            []byronRedeemKey{newByronRedeemKey(t, 0xA0)},
		fee:             0,
		validSignatures: true,
	})
	require.NoError(t, ValidateTxByron(redeemOnly, 0, ls, adopted))

	ls = newRedeemFeeLedgerState()
	ordinary := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xC1)
	mixed := buildByronRedeemTx(t, ls, byronRedeemTxCase{
		keys: []byronRedeemKey{
			newByronRedeemKey(t, 0xA0),
			newByronRedeemKey(t, 0xA1),
		},
		ordinary:        map[int]lcommon.Address{1: ordinary},
		fee:             0,
		validSignatures: true,
	})
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, ValidateTxByron(mixed, 0, ls, adopted), &feeErr)
	want, err := adopted.MinFee(TxSizeForFee(mixed))
	require.NoError(t, err)
	assert.Equal(t, want, feeErr.Required)
}

// TestValidateTxByron_UnknownAdoptionSkipsSizeAndFeeRules covers the trusted
// start whose adopted parameters cannot be established: the fee and
// transaction size rules do not run against genesis values, while the same
// parameters marked known still enforce them.
func TestValidateTxByron_UnknownAdoptionSkipsSizeAndFeeRules(t *testing.T) {
	t.Parallel()
	params, err := byronGenesisTestParams(
		155_381_000_000_000, 43_946_000_000, 100,
	)
	require.NoError(t, err)
	input := newTestInput(0x01, 0)
	newLS := func() *mockLedgerState {
		ls := newMockLedgerState()
		ls.addUtxo(input, newTestOutput(1_000_000))
		return ls
	}
	tx := &testByronTx{
		inputs:  []lcommon.TransactionInput{input},
		outputs: []lcommon.TransactionOutput{newTestOutput(1_000_000)},
		cbor:    make([]byte, 200),
	}
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, ValidateTxByron(tx, 0, newLS(), params), &feeErr)
	var sizeErr TxTooLargeByronError
	require.ErrorAs(t, ValidateTxByron(tx, 0, newLS(), params), &sizeErr)

	unknown := params.Clone()
	unknown.AdoptionUnknown = true
	require.NoError(t, ValidateTxByron(tx, 0, newLS(), unknown))
}
