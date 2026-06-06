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
	"fmt"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
