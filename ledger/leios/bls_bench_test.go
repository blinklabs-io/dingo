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
	"fmt"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

// benchCommittee returns seats signing keys with their registered Leios
// keys, and each key's vote over msg.
func benchCommittee(
	tb testing.TB,
	seats int,
	msg []byte,
) ([]*lcommon.LeiosKey, []*bls12381.G2Affine, [][]byte) {
	tb.Helper()
	keys := make([]*lcommon.LeiosKey, seats)
	pubs := make([]*bls12381.G2Affine, seats)
	votes := make([][]byte, seats)
	for i := range seats {
		key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", i+1_000_003))
		if err != nil {
			tb.Fatal(err)
		}
		proof, err := SignLeiosKeyProofOfPossession(key)
		if err != nil {
			tb.Fatal(err)
		}
		keys[i] = &lcommon.LeiosKey{
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: proof,
		}
		pubs[i] = key.PublicKey()
		if votes[i], err = SignVote(key, msg); err != nil {
			tb.Fatal(err)
		}
	}
	return keys, pubs, votes
}

// resetVerifiedLeiosKeys empties the verified-key cache so each measured
// verification runs cold.
func resetVerifiedLeiosKeys() {
	verifiedLeiosKeys.Lock()
	verifiedLeiosKeys.keys = make(map[verifiedLeiosKeyID]*bls12381.G2Affine)
	verifiedLeiosKeys.Unlock()
}

func BenchmarkLeiosCommitteeCrypto(b *testing.B) {
	const seats = 900
	msg := VoteMessageBytes(1, lcommon.Blake2b256{1})
	keys, pubs, votes := benchCommittee(b, seats, msg)
	aggregate, err := AggregateSignatures(votes)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("VerifyLeiosKeyProofOfPossession", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			resetVerifiedLeiosKeys()
			b.StartTimer()
			if err := VerifyLeiosKeyProofOfPossession(keys[0]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run(fmt.Sprintf("AggregateSignatures/%d", seats), func(b *testing.B) {
		for b.Loop() {
			if _, err := AggregateSignatures(votes); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run(fmt.Sprintf("VerifyAggregateSignature/%d", seats), func(b *testing.B) {
		for b.Loop() {
			if err := VerifyAggregateSignature(pubs, msg, aggregate); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run(fmt.Sprintf("VerifyCommitteeKeysCold/%d", seats), func(b *testing.B) {
		for b.Loop() {
			resetVerifiedLeiosKeys()
			errs := make([]error, seats)
			parallelFor(seats, func(i int) {
				_, errs[i] = verifiedLeiosKey(keys[i])
			})
			for _, err := range errs {
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
