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

package mithril

import (
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"testing"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signerFixture holds data captured from the preprod aggregator: a
// MithrilStakeDistribution certificate for epoch 316, the signers (with
// stakes) that produced it and the signers registered for the next epoch,
// plus one signer's full registration as published.
type signerFixture struct {
	Certificate      Certificate                     `json:"certificate"`
	CurrentSigners   []MithrilStakeDistributionParty `json:"current_signers"`
	NextSigners      []MithrilStakeDistributionParty `json:"next_signers"`
	RegisteredSigner json.RawMessage                 `json:"registered_signer"`
}

func loadSignerFixture(t *testing.T) *signerFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/signer/preprod_epoch316.json")
	require.NoError(t, err)
	var fx signerFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return &fx
}

// verifySTMProofOfPossession checks both halves of a proof of possession
// with the pairing equations of the reference verifier.
func verifySTMProofOfPossession(t *testing.T, vkRaw, pop []byte) {
	t.Helper()
	require.Len(t, pop, 96)
	vk, err := decodeSTMVerificationKey(vkRaw)
	require.NoError(t, err)
	k1, err := decodeSTMSignature(pop[:48])
	require.NoError(t, err)
	k2, err := decodeSTMSignature(pop[48:])
	require.NoError(t, err)
	h, err := bls12381.HashToG1(
		stmProofOfPossessionMessage,
		stmBLSDomainSeparationTag,
	)
	require.NoError(t, err)
	_, _, g1, g2 := bls12381.Generators()
	var negH, negK2 bls12381.G1Affine
	negH.Neg(&h)
	negK2.Neg(&k2)
	ok, err := bls12381.PairingCheck(
		[]bls12381.G1Affine{k1, negH},
		[]bls12381.G2Affine{g2, vk},
	)
	require.NoError(t, err)
	assert.True(t, ok, "k1 does not prove possession of the key")
	ok, err = bls12381.PairingCheck(
		[]bls12381.G1Affine{g1, negK2},
		[]bls12381.G2Affine{vk, g2},
	)
	require.NoError(t, err)
	assert.True(t, ok, "k2 does not match the key")
}

func TestSTMClosedRegistrationMatchesReference(t *testing.T) {
	t.Parallel()
	fx := loadSignerFixture(t)
	cert := fx.Certificate

	// The signers that produced the certificate commit to its
	// aggregate verification key.
	current, err := NewSTMClosedRegistration(fx.CurrentSigners)
	require.NoError(t, err)
	wantAVK, err := parseSTMAggregateVerificationKey(
		cert.AggregateVerificationKey,
	)
	require.NoError(t, err)
	assert.Equal(t, wantAVK.MTCommitment.Root, current.root)
	assert.Equal(t, wantAVK.MTCommitment.NrLeaves, len(current.entries))
	assert.Equal(t, wantAVK.TotalStake, current.totalStake)

	// The next epoch's signers are committed to byte-for-byte in the
	// certified message, and the whole message hashes to the signed value.
	next, err := NewSTMClosedRegistration(fx.NextSigners)
	require.NoError(t, err)
	nextAVK, err := next.AggregateVerificationKey()
	require.NoError(t, err)
	assert.Equal(
		t,
		cert.ProtocolMessage.MessageParts["next_aggregate_verification_key"],
		nextAVK,
	)
	msg := ProtocolMessage{MessageParts: map[string]string{
		"next_aggregate_verification_key": nextAVK,
		"next_protocol_parameters":        cert.Metadata.Parameters.ComputeHash(),
		"current_epoch":                   strconv.FormatUint(cert.Epoch, 10),
	}}
	assert.Equal(t, cert.SignedMessage, msg.ComputeHash())
}

func TestSTMSignerIndexAndLotteryMatchReferenceSignature(t *testing.T) {
	t.Parallel()
	fx := loadSignerFixture(t)
	cert := fx.Certificate
	reg, err := NewSTMClosedRegistration(fx.CurrentSigners)
	require.NoError(t, err)
	avk, err := parseSTMAggregateVerificationKey(cert.AggregateVerificationKey)
	require.NoError(t, err)
	aggregate, err := parseSTMAggregateSignature(cert.MultiSignature)
	require.NoError(t, err)
	require.NotEmpty(t, aggregate.Signatures)
	msgp := stmConcatenateWithMessage(avk, []byte(cert.SignedMessage))

	for _, signed := range aggregate.Signatures {
		index, stake, ok := reg.SignerIndex(signed.RegParty.VerificationKey)
		require.True(t, ok)
		assert.Equal(t, signed.Sig.SignerIndex, index)
		assert.Equal(t, signed.RegParty.Stake, stake)

		// The aggregator keeps a subset of each signer's won indexes, so
		// every index it kept must be one the signer computes as won.
		won := stmWonIndexes(
			signed.Sig.Sigma,
			msgp,
			cert.Metadata.Parameters,
			stake,
			reg.totalStake,
		)
		require.NotEmpty(t, signed.Sig.Indexes)
		for _, kept := range signed.Sig.Indexes {
			assert.Contains(t, won, kept)
		}
	}
}

func TestSTMVerificationKeyProofOfPossession(t *testing.T) {
	t.Parallel()
	fx := loadSignerFixture(t)

	// The reference signer's key passes the check, so the check is the
	// reference one; ours must pass it too.
	var ref struct {
		VerificationKey string `json:"verification_key"`
	}
	require.NoError(t, json.Unmarshal(fx.RegisteredSigner, &ref))
	raw, ok := decodePrimaryEncodedBytes(ref.VerificationKey)
	require.True(t, ok)
	var refKey stmSignerVerificationKey
	require.NoError(t, json.Unmarshal(raw, &refKey))
	verifySTMProofOfPossession(t, refKey.VK, refKey.Pop)

	key, err := NewSTMSigningKey()
	require.NoError(t, err)
	vk, err := key.VerificationKey()
	require.NoError(t, err)
	verifySTMProofOfPossession(t, vk.VK, vk.PoP)
	assert.Equal(t, slices.Concat(vk.VK, vk.PoP), vk.Bytes())

	encoded, err := vk.Encode()
	require.NoError(t, err)
	parsed, err := parseSTMSignerVerificationKey(encoded)
	require.NoError(t, err)
	assert.Equal(t, vk.VK, parsed)
}

func TestSTMSignProducesVerifiableSignature(t *testing.T) {
	t.Parallel()
	fx := loadSignerFixture(t)
	key, err := NewSTMSigningKey()
	require.NoError(t, err)
	vk, err := key.VerificationKey()
	require.NoError(t, err)
	encodedVK, err := vk.Encode()
	require.NoError(t, err)
	// A dominant stake makes winning several lottery indexes certain.
	own := MithrilStakeDistributionParty{
		PartyID:         "own",
		Stake:           fx.CurrentSigners[0].Stake * 50,
		VerificationKey: encodedVK,
	}
	reg, err := NewSTMClosedRegistration(
		append(slices.Clone(fx.CurrentSigners), own),
	)
	require.NoError(t, err)
	params := fx.Certificate.Metadata.Parameters
	msg := []byte(fx.Certificate.SignedMessage)

	sig, err := key.Sign(msg, reg, params)
	require.NoError(t, err)

	wantIndex, wantStake, ok := reg.SignerIndex(vk.VK)
	require.True(t, ok)
	assert.Equal(t, wantIndex, sig.SignerIndex)
	require.NotEmpty(t, sig.Indexes)

	msgp := slices.Concat(msg, reg.root)
	sigma, err := decodeSTMSignature(sig.Sigma)
	require.NoError(t, err)
	vkPoint, err := decodeSTMVerificationKey(vk.VK)
	require.NoError(t, err)
	require.NoError(
		t,
		stmVerifyBLSSignatureAggregate(
			msgp,
			[]bls12381.G2Affine{vkPoint},
			[]bls12381.G1Affine{sigma},
		),
	)
	// The signature must not verify for any other message.
	require.Error(
		t,
		stmVerifyBLSSignatureAggregate(
			slices.Concat([]byte("other"), reg.root),
			[]bls12381.G2Affine{vkPoint},
			[]bls12381.G1Affine{sigma},
		),
	)
	for _, index := range sig.Indexes {
		require.Less(t, index, params.M)
		ev := stmDenseMapping(sig.Sigma, msgp, index)
		assert.True(
			t,
			stmIsLotteryWon(params.PhiF, ev, wantStake, reg.totalStake),
		)
	}

	encoded, err := sig.Encode()
	require.NoError(t, err)
	raw, ok := decodePrimaryEncodedBytes(encoded)
	require.True(t, ok)
	var decoded STMSingleSignature
	require.NoError(t, json.Unmarshal(raw, &decoded))
	assert.Equal(t, *sig, decoded)
}

func TestSTMSignRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	fx := loadSignerFixture(t)
	reg, err := NewSTMClosedRegistration(fx.CurrentSigners)
	require.NoError(t, err)
	key, err := NewSTMSigningKey()
	require.NoError(t, err)

	_, err = key.Sign([]byte("msg"), reg, fx.Certificate.Metadata.Parameters)
	require.ErrorContains(t, err, "not in the closed registration")

	_, err = key.Sign(
		[]byte("msg"),
		reg,
		ProtocolParameters{K: 1, M: 0, PhiF: 0.5},
	)
	require.ErrorContains(t, err, "M=0")

	_, err = NewSTMClosedRegistration(nil)
	require.Error(t, err)
	_, err = NewSTMClosedRegistration(
		[]MithrilStakeDistributionParty{
			{PartyID: "bad", VerificationKey: "zz"},
		},
	)
	require.ErrorContains(t, err, "bad")
}

func TestSTMSigningKeyBytes(t *testing.T) {
	t.Parallel()
	key, err := NewSTMSigningKey()
	require.NoError(t, err)
	restored, err := STMSigningKeyFromBytes(key.Bytes())
	require.NoError(t, err)
	assert.Equal(t, key.Bytes(), restored.Bytes())

	_, err = STMSigningKeyFromBytes(make([]byte, 32))
	require.ErrorContains(t, err, "zero")
	_, err = STMSigningKeyFromBytes(make([]byte, 31))
	require.Error(t, err)
	over := make([]byte, 32)
	for i := range over {
		over[i] = 0xff
	}
	_, err = STMSigningKeyFromBytes(over)
	require.Error(t, err)
}
