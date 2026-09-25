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

package ledger

import (
	"crypto/ed25519"
	"crypto/sha3"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// byronBlockTestKey is a VKWitness signing key and the Byron address it
// controls.
type byronBlockTestKey struct {
	private ed25519.PrivateKey
	xpub    []byte
	address lcommon.Address
}

func newByronBlockTestKey(t *testing.T, seedByte byte) byronBlockTestKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	private := ed25519.NewKeyFromSeed(seed)
	public, ok := private.Public().(ed25519.PublicKey)
	require.True(t, ok)
	xpub := append(append([]byte{}, public...), make([]byte, 32)...)
	rootCbor, err := cbor.Encode([]any{
		uint64(lcommon.ByronAddressTypePubkey),
		[]any{uint64(lcommon.ByronAddressTypePubkey), xpub},
		cbor.RawMessage{0xa0},
	})
	require.NoError(t, err)
	digest := sha3.Sum256(rootCbor)
	root := lcommon.Blake2b224Hash(digest[:])
	address, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		root.Bytes(),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	return byronBlockTestKey{private: private, xpub: xpub, address: address}
}

type byronBlockTestInput struct {
	txId  []byte
	index uint32
}

type byronBlockTestOutput struct {
	address lcommon.Address
	amount  uint64
}

// buildByronBlockTestTx assembles and decodes a real Byron transaction whose
// witnesses sign its body under protocolMagic, in signer order.
func buildByronBlockTestTx(
	t *testing.T,
	protocolMagic uint32,
	inputs []byronBlockTestInput,
	outputs []byronBlockTestOutput,
	attributes []byte,
	signers []byronBlockTestKey,
) *byron.ByronTransaction {
	t.Helper()
	wireInputs := make([]any, 0, len(inputs))
	for _, input := range inputs {
		inner, err := cbor.Encode([]any{input.txId, input.index})
		require.NoError(t, err)
		wireInputs = append(wireInputs, []any{0, cbor.WrappedCbor(inner)})
	}
	wireOutputs := make([]any, 0, len(outputs))
	for _, output := range outputs {
		addrCbor, err := cbor.Encode(&output.address)
		require.NoError(t, err)
		wireOutputs = append(
			wireOutputs,
			[]any{cbor.RawMessage(addrCbor), output.amount},
		)
	}
	if attributes == nil {
		attributes = []byte{0xa0}
	}
	body, err := cbor.Encode(
		[]any{wireInputs, wireOutputs, cbor.RawMessage(attributes)},
	)
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(body)
	magicCbor, err := cbor.Encode(protocolMagic)
	require.NoError(t, err)
	message := append([]byte{0x01}, magicCbor...)
	message = append(message, 0x58, 0x20)
	message = append(message, bodyHash[:]...)
	witnesses := make([]any, 0, len(signers))
	for _, signer := range signers {
		payload, err := cbor.Encode(
			[][]byte{signer.xpub, ed25519.Sign(signer.private, message)},
		)
		require.NoError(t, err)
		witnesses = append(
			witnesses,
			[]any{uint64(0), cbor.WrappedCbor(payload)},
		)
	}
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(body), witnesses},
	)
	require.NoError(t, err)
	tx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.Equal(t, bodyHash, tx.WireId())
	return tx
}

// processByronReferenceRuleBlock applies one Byron block holding tx through
// ledgerProcessBlock with a real LedgerState, database and Byron genesis.
func processByronReferenceRuleBlock(
	t *testing.T,
	db *database.Database,
	nodeConfig *cardano.CardanoNodeConfig,
	tx lcommon.Transaction,
) error {
	t.Helper()
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	block := &envelopeTestBlock{
		header: &envelopeTestHeader{
			cbor:   []byte{0x80},
			slot:   1,
			number: 1,
			era:    byron.EraByron,
		},
		cbor: []byte{0x82, 0x80, 0x80},
		txs:  []lcommon.Transaction{tx},
	}
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  1,
				ByteLength: uint32(len(tx.Cbor())), // #nosec G115
			},
		},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	for _, utxo := range tx.Produced() {
		offsets.UtxoOffsets[database.UtxoRef{
			TxId:      txHash,
			OutputIdx: utxo.Id.Index(),
		}] = database.CborOffset{BlockSlot: 1, ByteLength: 1}
	}
	return db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 1, Hash: block.Hash().Bytes()},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			offsets,
			eras.ByronEraDesc,
			&shelley.ShelleyProtocolParameters{},
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

// TestLedgerProcessBlockByronReferenceRules drives real, correctly signed
// Byron transactions through block application for #4379, #4381, #4394,
// #4401 and #4405. The genesis supplies ppMaxTxSize, a zero fee policy and
// the mainnet protocol magic the witnesses sign under, so each case isolates
// one rule.
func TestLedgerProcessBlockByronReferenceRules(t *testing.T) {
	t.Parallel()

	nodeConfig := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, nodeConfig, strings.NewReader(`{
		"blockVersionData": {
			"slotDuration": "20000",
			"maxTxSize": "600",
			"txFeePolicy": {"summand": "0", "multiplier": "0"}
		},
		"protocolConsts": {"k": 2160, "protocolMagic": 764824073}
	}`)),
	)
	const protocolMagic = 764824073
	keyA := newByronBlockTestKey(t, 0x61)
	keyB := newByronBlockTestKey(t, 0x62)
	payTo := newByronBlockTestKey(t, 0x63).address

	tests := []struct {
		name  string
		build func(t *testing.T, db *database.Database) *byron.ByronTransaction
		check func(t *testing.T, err error)
	}{
		{
			name: "witnesses in input order are accepted",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				b := seedByronUtxoWithAmount(t, db, 0x02, keyB.address, 1_000)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}, {b, 0}},
					[]byronBlockTestOutput{{payTo, 1_500}},
					nil, []byronBlockTestKey{keyA, keyB})
			},
			check: func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			name: "swapped witnesses are rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				b := seedByronUtxoWithAmount(t, db, 0x02, keyB.address, 1_000)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}, {b, 0}},
					[]byronBlockTestOutput{{payTo, 1_500}},
					nil, []byronBlockTestKey{keyB, keyA})
			},
			check: func(t *testing.T, err error) {
				var wrongKey eras.WitnessWrongKeyByronError
				require.ErrorAs(t, err, &wrongKey)
			},
		},
		{
			name: "repeated input is accepted",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				x := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{x, 0}, {x, 0}},
					[]byronBlockTestOutput{{payTo, 800}},
					nil, []byronBlockTestKey{keyA, keyA})
			},
			check: func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			name: "input balance above maxLovelaceVal is rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 30e15)
				b := seedByronUtxoWithAmount(t, db, 0x02, keyB.address, 20e15)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}, {b, 0}},
					[]byronBlockTestOutput{{payTo, 44e15}},
					nil, []byronBlockTestKey{keyA, keyB})
			},
			check: func(t *testing.T, err error) {
				var bound eras.LovelaceBoundByronError
				require.ErrorAs(t, err, &bound)
			},
		},
		{
			name: "transaction above ppMaxTxSize is rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				outputs := make([]byronBlockTestOutput, 0, 20)
				for range 20 {
					outputs = append(outputs, byronBlockTestOutput{payTo, 1})
				}
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}},
					outputs, nil, []byronBlockTestKey{keyA})
			},
			check: func(t *testing.T, err error) {
				var tooLarge eras.TxTooLargeByronError
				require.ErrorAs(t, err, &tooLarge)
			},
		},
		{
			name: "unknown transaction attributes at the limit are rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				attrs, err := cbor.Encode(
					map[uint8][]byte{9: make([]byte, 128)},
				)
				require.NoError(t, err)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}},
					[]byronBlockTestOutput{{payTo, 900}},
					attrs, []byronBlockTestKey{keyA})
			},
			check: func(t *testing.T, err error) {
				var unknown eras.UnknownAttributesByronError
				require.ErrorAs(t, err, &unknown)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			tx := test.build(t, db)
			test.check(t, processByronReferenceRuleBlock(t, db, nodeConfig, tx))
		})
	}
}
