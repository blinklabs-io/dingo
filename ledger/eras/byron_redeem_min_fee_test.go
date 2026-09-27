package eras

import (
	"crypto/ed25519"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
