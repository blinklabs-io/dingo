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

package leios

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLeiosPopDSTDiffersFromVoteDST pins both DST literals against the
// IETF BLS-signature draft's values for this ciphersuite (hash_to_point vs
// hash_pubkey_to_point), so reusing one for the other -- the bug this pair
// of constants exists to prevent -- fails a test immediately rather than
// only showing up as every real pool's PoP getting rejected.
func TestLeiosPopDSTDiffersFromVoteDST(t *testing.T) {
	assert.Equal(
		t,
		"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_",
		LeiosVoteDST,
	)
	assert.Equal(
		t,
		"BLS_POP_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_",
		leiosPopDST,
	)
	assert.NotEqual(t, LeiosVoteDST, leiosPopDST)
}

// testSigningKey returns a signing key built from a small non-zero scalar.
func testSigningKey(t *testing.T, scalar byte) *VoteSigningKey {
	t.Helper()
	require.NotZero(t, scalar)
	key, err := ParseVoteSigningKey(
		fmt.Sprintf("%064x", scalar),
	)
	require.NoError(t, err)
	return key
}

func TestVoteMessageBytes(t *testing.T) {
	ebHash := lcommon.NewBlake2b256([]byte("endorser block"))
	msg := VoteMessageBytes(0x0102030405060708, ebHash)
	require.Len(t, msg, 40)
	assert.Equal(
		t,
		uint64(0x0102030405060708),
		binary.BigEndian.Uint64(msg[:8]),
	)
	assert.Equal(t, ebHash.Bytes(), msg[8:])
}

// TestPrototypeVoteMessageBytesIsCborByteString pins the signed preimage to
// the reference's SignableRepresentation for RbHash, which is the CBOR
// byte-string encoding of the 32-byte hash (0x58 0x20 followed by the hash)
// rather than the bare hash.
func TestPrototypeVoteMessageBytesIsCborByteString(t *testing.T) {
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	msg := PrototypeVoteMessageBytes(rbHash)
	require.Len(t, msg, 34)
	assert.Equal(t, []byte{0x58, 0x20}, msg[:2])
	assert.Equal(t, rbHash.Bytes(), msg[2:])
	// The constant header must stay identical to what the CBOR encoder
	// produces for the same 32-byte payload.
	encoded, err := cbor.Encode(rbHash.Bytes())
	require.NoError(t, err)
	assert.Equal(t, encoded, msg)
}

func TestSignVoteVerifyRoundTrip(t *testing.T) {
	key := testSigningKey(t, 42)
	msg := VoteMessageBytes(
		1234,
		lcommon.NewBlake2b256([]byte("eb")),
	)
	sig, err := SignVote(key, msg)
	require.NoError(t, err)
	require.Len(t, sig, lcommon.LeiosBlsSignatureSize)
	require.NoError(t, VerifyVoteSignature(key.PublicKey(), msg, sig))
}

func TestPrototypeVoteMusashiVector(t *testing.T) {
	// Fixed MinSig/PoP vector cross-checked with supranational/blst, the
	// independent BLS12-381 implementation the reference signs with.
	// Keeping the expected bytes literal makes this test fail if either the
	// Musashi message shape or cardano-crypto-leios ciphersuite DST changes
	// accidentally.
	rbHashBytes, err := hex.DecodeString(
		"000102030405060708090a0b0c0d0e0f" +
			"101112131415161718191a1b1c1d1e1f",
	)
	require.NoError(t, err)
	var rbHash lcommon.Blake2b256
	copy(rbHash[:], rbHashBytes)
	msg := PrototypeVoteMessageBytes(rbHash)
	// The reference signs the CBOR byte-string encoding of the hash, not the
	// bare hash bytes.
	assert.Equal(t, append([]byte{0x58, 0x20}, rbHashBytes...), msg)

	key := testSigningKey(t, 42)
	sig, err := SignVote(key, msg)
	require.NoError(t, err)
	expectedSig, err := hex.DecodeString(
		"807cbcaf3514d73af215e7734202ab7f08e912d9b10fe8b7" +
			"96a948c0b196856568912818135bcd23d70b0faba48ad214",
	)
	require.NoError(t, err)
	assert.Equal(t, expectedSig, sig)

	publicKey, err := ParseVoterPublicKey(
		"ac7fa63dfc38bbf3712e27a180391bca4ccabf609c5967a0" +
			"592eff420b6235f3f2b323051cb099acc3969aca310f7ff4" +
			"191b2d6db43fafc2c9592f7e5f73981107975d3d92b8438" +
			"91e724dbc9f05b5eee5a3b2b1fc782ede8149f30830b84444",
	)
	require.NoError(t, err)
	require.NoError(t, VerifyVoteSignature(publicKey, msg, expectedSig))
}

func TestVerifyVoteSignatureWrongMessage(t *testing.T) {
	key := testSigningKey(t, 42)
	msg := VoteMessageBytes(1234, lcommon.NewBlake2b256([]byte("eb")))
	sig, err := SignVote(key, msg)
	require.NoError(t, err)
	otherMsg := VoteMessageBytes(
		1235,
		lcommon.NewBlake2b256([]byte("eb")),
	)
	assert.ErrorIs(
		t,
		VerifyVoteSignature(key.PublicKey(), otherMsg, sig),
		ErrInvalidSignature,
	)
}

func TestVerifyVoteSignatureWrongKey(t *testing.T) {
	key := testSigningKey(t, 42)
	otherKey := testSigningKey(t, 43)
	msg := VoteMessageBytes(1234, lcommon.NewBlake2b256([]byte("eb")))
	sig, err := SignVote(key, msg)
	require.NoError(t, err)
	assert.ErrorIs(
		t,
		VerifyVoteSignature(otherKey.PublicKey(), msg, sig),
		ErrInvalidSignature,
	)
}

func TestVerifyVoteSignatureMalformed(t *testing.T) {
	key := testSigningKey(t, 42)
	msg := VoteMessageBytes(1234, lcommon.NewBlake2b256([]byte("eb")))
	// Wrong size
	assert.Error(t, VerifyVoteSignature(key.PublicKey(), msg, []byte{1}))
	// Garbage bytes of the right size
	garbage := make([]byte, lcommon.LeiosBlsSignatureSize)
	for i := range garbage {
		garbage[i] = 0xff
	}
	assert.Error(t, VerifyVoteSignature(key.PublicKey(), msg, garbage))
	// Point at infinity (compressed: 0xc0 then zeros) must be rejected
	infinity := make([]byte, lcommon.LeiosBlsSignatureSize)
	infinity[0] = 0xc0
	assert.Error(t, VerifyVoteSignature(key.PublicKey(), msg, infinity))
}

func TestAggregateSignaturesVerify(t *testing.T) {
	msg := VoteMessageBytes(99, lcommon.NewBlake2b256([]byte("eb")))
	sigs := make([][]byte, 0, 3)
	pubs := make([]*bls12381.G2Affine, 0, 3)
	for _, scalar := range []byte{11, 22, 33} {
		key := testSigningKey(t, scalar)
		sig, err := SignVote(key, msg)
		require.NoError(t, err)
		sigs = append(sigs, sig)
		pubs = append(pubs, key.PublicKey())
	}
	aggSig, err := AggregateSignatures(sigs)
	require.NoError(t, err)
	require.Len(t, aggSig, lcommon.LeiosBlsSignatureSize)
	require.NoError(t, VerifyAggregateSignature(pubs, msg, aggSig))
}

func TestVerifyAggregateSignatureCorrupted(t *testing.T) {
	msg := VoteMessageBytes(99, lcommon.NewBlake2b256([]byte("eb")))
	key1 := testSigningKey(t, 11)
	key2 := testSigningKey(t, 22)
	sig1, err := SignVote(key1, msg)
	require.NoError(t, err)
	sig2, err := SignVote(key2, msg)
	require.NoError(t, err)
	aggSig, err := AggregateSignatures([][]byte{sig1, sig2})
	require.NoError(t, err)
	pubs := []*bls12381.G2Affine{key1.PublicKey(), key2.PublicKey()}

	// Aggregate of a different signer set must not verify
	assert.ErrorIs(
		t,
		VerifyAggregateSignature(pubs, msg, sig1),
		ErrInvalidSignature,
	)
	// Subset of public keys must not verify the full aggregate
	assert.ErrorIs(
		t,
		VerifyAggregateSignature(
			[]*bls12381.G2Affine{key1.PublicKey()}, msg, aggSig,
		),
		ErrInvalidSignature,
	)
}

func TestAggregateSignaturesInvalidInput(t *testing.T) {
	_, err := AggregateSignatures(nil)
	assert.Error(t, err)
	_, err = AggregateSignatures([][]byte{{1, 2, 3}})
	assert.Error(t, err)
}

func TestVerifyAggregateSignatureNoKeys(t *testing.T) {
	msg := VoteMessageBytes(99, lcommon.NewBlake2b256([]byte("eb")))
	key := testSigningKey(t, 11)
	sig, err := SignVote(key, msg)
	require.NoError(t, err)
	assert.Error(t, VerifyAggregateSignature(nil, msg, sig))
}

// testLeiosKey builds a LeiosKey whose possession proof is a genuine
// PopProve signature over its own public key under leiosPopDST -- the same
// DST VerifyLeiosKeyProofOfPossession checks against, not LeiosVoteDST.
func testLeiosKey(t *testing.T, scalar byte) *lcommon.LeiosKey {
	t.Helper()
	key := testSigningKey(t, scalar)
	pub := key.PublicKeyBytes()
	proof, err := signWithDST(key, pub, leiosPopDST)
	require.NoError(t, err)
	return &lcommon.LeiosKey{PublicKey: pub, PossessionProof: proof}
}

func TestVerifyLeiosKeyProofOfPossession(t *testing.T) {
	require.NoError(t, VerifyLeiosKeyProofOfPossession(testLeiosKey(t, 7)))
}

func TestVerifyLeiosKeyProofOfPossessionNil(t *testing.T) {
	assert.Error(t, VerifyLeiosKeyProofOfPossession(nil))
}

func TestVerifyLeiosKeyProofOfPossessionWrongPublicKeyLength(t *testing.T) {
	key := testLeiosKey(t, 7)
	key.PublicKey = key.PublicKey[:len(key.PublicKey)-1]
	assert.Error(t, VerifyLeiosKeyProofOfPossession(key))
}

// TestVerifyLeiosKeyProofOfPossessionMismatchedKeyAndProof covers a
// possession proof genuinely produced by a different key: upstream's
// "invalid proofs are treated as absent" rule depends on this failing.
func TestVerifyLeiosKeyProofOfPossessionMismatchedKeyAndProof(t *testing.T) {
	honest := testLeiosKey(t, 7)
	other := testLeiosKey(t, 9)
	tampered := &lcommon.LeiosKey{
		PublicKey:       honest.PublicKey,
		PossessionProof: other.PossessionProof,
	}
	assert.ErrorIs(
		t,
		VerifyLeiosKeyProofOfPossession(tampered),
		ErrInvalidSignature,
	)
}

// TestVerifyLeiosKeyProofOfPossessionRejectsVoteSignatureAsProof guards
// against reusing an ordinary vote signature (over an RB hash, under
// LeiosVoteDST) as if it were a possession proof (over the public key
// itself, under leiosPopDST): the two are now separated by both a
// different DST and a different message, either of which alone would
// already make this fail.
func TestVerifyLeiosKeyProofOfPossessionRejectsVoteSignatureAsProof(
	t *testing.T,
) {
	key := testSigningKey(t, 7)
	pub := key.PublicKeyBytes()
	rbHash := lcommon.NewBlake2b256([]byte("not-a-pubkey-message"))
	voteSig, err := SignVote(key, PrototypeVoteMessageBytes(rbHash))
	require.NoError(t, err)
	assert.Error(t, VerifyLeiosKeyProofOfPossession(&lcommon.LeiosKey{
		PublicKey:       pub,
		PossessionProof: voteSig,
	}))
}
