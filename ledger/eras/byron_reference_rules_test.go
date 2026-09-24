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
	"crypto/ed25519"
	"crypto/sha3"
	"hash/crc32"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, bodyHash, tx.Hash())
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

// TestValidateTxByron_PositionalWitnesses covers #4394: witness i must
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

// TestValidateTxByron_RepeatedInput covers #4401: a repeated input is valid,
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

// TestValidateTxByron_LovelaceBounds covers #4405: balances are summed as
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

// TestByronMinFee covers #4403: summand div 10^9 plus the ceiling of the
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
		got, err := byronMinFee(test.summand, test.multiplier, test.size)
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

// TestValidateTxByron_MaxTxSize covers #4379: ppMaxTxSize bounds the
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

// TestValidateTxByron_UnknownAttributes covers #4381 for transaction
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

// TestValidateTxByron_UnknownAddressAttributes covers #4381 for output
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

// TestValidateTxByron_WitnessRootUsesCanonicalAttributes covers #4414: the
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
