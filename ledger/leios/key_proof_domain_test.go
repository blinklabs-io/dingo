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
	"encoding/hex"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gleios "github.com/blinklabs-io/gouroboros/ledger/leios"
	"github.com/stretchr/testify/require"
)

var keyProofDomains = []struct {
	name   string
	domain lcommon.DijkstraBlsProofDomain
	dst    string
}{
	{"pop", lcommon.DijkstraBlsProofDomainPop, LeiosPoPDST},
	{"legacy_sig", lcommon.DijkstraBlsProofDomainLegacySig, LeiosVoteDST},
}

func TestVerifyLeiosKeyProofForDomain(t *testing.T) {
	t.Parallel()
	for _, signed := range keyProofDomains {
		key := testSigningKey(t, 9)
		proof, err := signWithDST(key, key.PublicKeyBytes(), signed.dst)
		require.NoError(t, err)
		leiosKey := &lcommon.LeiosKey{
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: proof,
		}
		for _, verified := range keyProofDomains {
			t.Run(
				signed.name+"_verified_as_"+verified.name,
				func(t *testing.T) {
					t.Parallel()
					err := VerifyLeiosKeyProofForDomain(
						leiosKey,
						verified.domain,
					)
					oracle := gleios.VerifyPossessionProofForDomain(
						*leiosKey,
						verified.domain,
					)
					if signed.domain == verified.domain {
						require.NoError(t, err)
						require.NoError(t, oracle)
						return
					}
					require.Error(t, err)
					require.Error(t, oracle)
				},
			)
		}
	}
	require.Error(t, VerifyLeiosKeyProofForDomain(
		testLeiosKey(t, 9),
		lcommon.DijkstraBlsProofDomain(99),
	))
}

// TestVoteManagerCertificateKeyProofDomain drives the certificate path the
// node uses: a committee whose registered keys carry proofs under one domain
// validates only when the manager is configured for that same domain.
func TestVoteManagerCertificateKeyProofDomain(t *testing.T) {
	t.Parallel()
	for _, signed := range keyProofDomains {
		for _, configured := range keyProofDomains {
			t.Run(
				signed.name+"_configured_"+configured.name,
				func(t *testing.T) {
					t.Parallel()
					fixture := newManagerFixture(
						t,
						func(f *managerFixture, cfg *VoteManagerConfig) {
							keys := make(
								map[string]*lcommon.LeiosKey,
								len(f.members),
							)
							for _, member := range f.members {
								key := f.keys[member.VoterId]
								proof, err := signWithDST(
									key,
									key.PublicKeyBytes(),
									signed.dst,
								)
								require.NoError(t, err)
								keys[hex.EncodeToString(member.PoolKeyHash)] = &lcommon.LeiosKey{
									PublicKey:       key.PublicKeyBytes(),
									PossessionProof: proof,
								}
							}
							cfg.KeyProvider = &fakeLeiosKeyProvider{keys: keys}
							cfg.KeyProofDomain = configured.domain
						},
					)
					message := []byte("certificate message")
					signers := make([]byte, lcommon.LeiosSignerBitfieldSize(10))
					signatures := make([][]byte, 0, 10)
					for voterID := range uint64(10) {
						signature, err := SignVote(
							fixture.keys[voterID],
							message,
						)
						require.NoError(t, err)
						signatures = append(signatures, signature)
						signers[voterID/8] |= 1 << (7 - voterID%8)
					}
					aggregate, err := AggregateSignatures(signatures)
					require.NoError(t, err)
					err = fixture.mgr.ValidateDijkstraCertificate(
						5,
						signers,
						aggregate,
						message,
					)
					if signed.domain == configured.domain {
						require.NoError(t, err)
						return
					}
					require.ErrorIs(t, err, ErrInvalidCertificate)
					require.ErrorContains(t, err, "no usable key")
				},
			)
		}
	}
}

// TestVerifiedLeiosKeyCacheIsPerDomain verifies a key under the domain its
// proof was made in, which caches it, and then requires the other domain to
// fail. The steps run in order so the cached entry exists before the lookup.
func TestVerifiedLeiosKeyCacheIsPerDomain(t *testing.T) {
	t.Parallel()
	for i, signed := range keyProofDomains {
		other := keyProofDomains[1-i]
		key := testSigningKey(t, byte(230+i))
		proof, err := signWithDST(key, key.PublicKeyBytes(), signed.dst)
		require.NoError(t, err)
		leiosKey := &lcommon.LeiosKey{
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: proof,
		}
		require.NoError(
			t,
			VerifyLeiosKeyProofForDomain(leiosKey, signed.domain),
		)
		require.Error(
			t,
			VerifyLeiosKeyProofForDomain(leiosKey, other.domain),
			"a %s proof cached under its domain must not verify as %s",
			signed.name,
			other.name,
		)
	}
}
