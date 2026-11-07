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
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/stretchr/testify/require"
)

// sequentialAggregate is AggregateSignatures as it was: decode each
// signature in turn and sum in affine coordinates.
func sequentialAggregate(t *testing.T, sigs [][]byte) []byte {
	t.Helper()
	var agg bls12381.G1Affine
	for i, sig := range sigs {
		point, err := decodeSignaturePoint(sig)
		require.NoError(t, err)
		if i == 0 {
			agg = *point
			continue
		}
		agg.Add(&agg, point)
	}
	out := agg.Bytes()
	return out[:]
}

func TestAggregateSignaturesMatchesSequentialSum(t *testing.T) {
	msg := VoteMessageBytes(7, lcommon.Blake2b256{7})
	_, pubs, votes := benchCommittee(t, 40, msg)
	for _, n := range []int{1, 2, 3, 17, 40} {
		got, err := AggregateSignatures(votes[:n])
		require.NoError(t, err)
		require.Equal(t, sequentialAggregate(t, votes[:n]), got, "n=%d", n)
		require.NoError(t, VerifyAggregateSignature(pubs[:n], msg, got))
	}
	// A signer missing from the key set fails the aggregate check.
	aggregate, err := AggregateSignatures(votes)
	require.NoError(t, err)
	require.ErrorIs(t,
		VerifyAggregateSignature(pubs[1:], msg, aggregate),
		ErrInvalidSignature,
	)
}

func TestAggregateSignaturesReportsTheBadSignature(t *testing.T) {
	msg := VoteMessageBytes(8, lcommon.Blake2b256{8})
	_, _, votes := benchCommittee(t, 20, msg)
	bad := make([][]byte, len(votes))
	copy(bad, votes)
	bad[13] = make([]byte, lcommon.LeiosBlsSignatureSize)
	bad[17] = make([]byte, lcommon.LeiosBlsSignatureSize)
	_, err := AggregateSignatures(bad)
	require.ErrorContains(t, err, "signature 13:")
}

// Tests below touch the package-level verified-key cache and do not run in
// parallel.

func TestVerifiedLeiosKeyNeverCachesAForgedProof(t *testing.T) {
	msg := VoteMessageBytes(9, lcommon.Blake2b256{9})
	keys, _, _ := benchCommittee(t, 2, msg)
	forged := &lcommon.LeiosKey{
		PublicKey:       keys[0].PublicKey,
		PossessionProof: keys[1].PossessionProof,
	}
	for range 2 {
		require.Error(t, VerifyLeiosKeyProofOfPossession(forged))
		require.NoError(t, VerifyLeiosKeyProofOfPossession(keys[0]))
	}
	pub, err := verifiedLeiosKey(keys[0])
	require.NoError(t, err)
	encoded := pub.Bytes()
	require.True(t, bytes.Equal(encoded[:], keys[0].PublicKey))
}

func TestVerifiedLeiosKeyCacheStaysBounded(t *testing.T) {
	verifiedLeiosKeys.Lock()
	saved := verifiedLeiosKeys.keys
	full := make(map[verifiedLeiosKeyID]*bls12381.G2Affine, verifiedLeiosKeyCacheMax)
	for i := range verifiedLeiosKeyCacheMax {
		var id verifiedLeiosKeyID
		id.publicKey[0], id.publicKey[1] = byte(i), byte(i>>8)
		full[id] = &bls12381.G2Affine{}
	}
	verifiedLeiosKeys.keys = full
	verifiedLeiosKeys.Unlock()
	t.Cleanup(func() {
		verifiedLeiosKeys.Lock()
		verifiedLeiosKeys.keys = saved
		verifiedLeiosKeys.Unlock()
	})
	msg := VoteMessageBytes(10, lcommon.Blake2b256{10})
	keys, _, _ := benchCommittee(t, 1, msg)
	require.NoError(t, VerifyLeiosKeyProofOfPossession(keys[0]))
	verifiedLeiosKeys.RLock()
	defer verifiedLeiosKeys.RUnlock()
	require.Len(t, verifiedLeiosKeys.keys, 1)
}
