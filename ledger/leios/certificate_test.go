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
	"fmt"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCommitteeFixture is a committee with signing keys and a registry for
// certificate tests. Pools are sized so member i has stake (10-i)*10 with
// 10 members and total active stake 550 (committee covers all of it).
type testCommitteeFixture struct {
	committee *Committee
	keys      map[uint64]*VoteSigningKey // voter id -> signing key
	registry  *VoterRegistry
}

func newTestCommitteeFixture(t *testing.T, size byte) *testCommitteeFixture {
	t.Helper()
	require.NotZero(t, size)
	poolStakes := make(map[string]uint64)
	var total uint64
	for i := range size {
		stake := uint64(size-i) * 10
		poolStakes[testPoolHash(i+1)] = stake
		total += stake
	}
	committee, err := ComputeCommittee(
		5, 3, poolStakes, total, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, int(size))

	keys := make(map[uint64]*VoteSigningKey)
	registryEntries := make(map[string]string)
	for _, member := range committee.Members {
		key, err := ParseVoteSigningKey(
			fmt.Sprintf("%064x", member.VoterId+1),
		)
		require.NoError(t, err)
		keys[member.VoterId] = key
		registryEntries[hex.EncodeToString(member.PoolKeyHash)] =
			hex.EncodeToString(key.PublicKeyBytes())
	}
	registry, err := NewVoterRegistry(registryEntries)
	require.NoError(t, err)
	return &testCommitteeFixture{
		committee: committee,
		keys:      keys,
		registry:  registry,
	}
}

// signVotes produces verified votes for the given voter ids over the
// given slot and EB hash.
func (f *testCommitteeFixture) signVotes(
	t *testing.T,
	slotNo uint64,
	ebHash lcommon.Blake2b256,
	voterIds ...uint64,
) []VerifiedVote {
	t.Helper()
	msg := VoteMessageBytes(slotNo, ebHash)
	votes := make([]VerifiedVote, 0, len(voterIds))
	for _, voterId := range voterIds {
		key, ok := f.keys[voterId]
		require.True(t, ok)
		sig, err := SignVote(key, msg)
		require.NoError(t, err)
		votes = append(
			votes,
			VerifiedVote{VoterId: voterId, Signature: sig},
		)
	}
	return votes
}

func TestBuildEbCertificateBitfieldMapping(t *testing.T) {
	fixture := newTestCommitteeFixture(t, 10)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	votes := fixture.signVotes(t, 77, ebHash, 0, 2, 9)
	cert, err := BuildEbCertificate(77, ebHash, fixture.committee, votes)
	require.NoError(t, err)

	assert.Equal(t, uint64(77), cert.SlotNo)
	assert.Equal(t, ebHash, cert.EndorserBlockHash)
	// Committee of 10 -> 2 bitfield bytes; bits are MSB-first per
	// lcommon.LeiosSignerBit: voter 0 -> 0x80, voter 2 -> 0x20 in byte
	// 0; voter 9 -> 0x40 in byte 1.
	require.Len(
		t,
		cert.Signers,
		int(lcommon.LeiosSignerBitfieldSize(10)),
	)
	assert.Equal(t, []byte{0xa0, 0x40}, cert.Signers)
	for voterId := range uint64(10) {
		expected := voterId == 0 || voterId == 2 || voterId == 9
		assert.Equal(
			t, expected, cert.Signer(voterId),
			"voter %d", voterId,
		)
	}
	// gouroboros committee-dependent validation must accept the result
	require.NoError(t, cert.Validate(fixture.committee.Size()))
}

func TestBuildEbCertificateUnusedBitsZero(t *testing.T) {
	// Committee size not divisible by 8 leaves unused bits in the last
	// byte that must remain zero.
	fixture := newTestCommitteeFixture(t, 5)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	votes := fixture.signVotes(t, 77, ebHash, 0, 1, 2, 3, 4)
	cert, err := BuildEbCertificate(77, ebHash, fixture.committee, votes)
	require.NoError(t, err)
	require.Len(t, cert.Signers, 1)
	assert.Equal(t, byte(0xf8), cert.Signers[0])
	require.NoError(t, cert.Validate(fixture.committee.Size()))
}

func TestBuildEbCertificateRejectsInvalidVotes(t *testing.T) {
	fixture := newTestCommitteeFixture(t, 10)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))

	// No votes
	_, err := BuildEbCertificate(77, ebHash, fixture.committee, nil)
	assert.Error(t, err)

	// Out-of-range voter id
	votes := fixture.signVotes(t, 77, ebHash, 0)
	votes[0].VoterId = 10
	_, err = BuildEbCertificate(77, ebHash, fixture.committee, votes)
	assert.Error(t, err)

	// Duplicate voter id
	votes = fixture.signVotes(t, 77, ebHash, 3, 3)
	_, err = BuildEbCertificate(77, ebHash, fixture.committee, votes)
	assert.Error(t, err)

	// Malformed signature
	votes = fixture.signVotes(t, 77, ebHash, 0)
	votes[0].Signature = []byte{1, 2, 3}
	_, err = BuildEbCertificate(77, ebHash, fixture.committee, votes)
	assert.Error(t, err)
}

func TestValidateEbCertificateRoundTrip(t *testing.T) {
	fixture := newTestCommitteeFixture(t, 10)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// Top 5 members hold 100+90+80+70+60 = 400 of 550 total stake;
	// tau = 7/10 needs 385.
	votes := fixture.signVotes(t, 77, ebHash, 0, 1, 2, 3, 4)
	cert, err := BuildEbCertificate(77, ebHash, fixture.committee, votes)
	require.NoError(t, err)

	sigChecked, err := ValidateEbCertificate(
		cert, fixture.committee, big.NewRat(7, 10), fixture.registry,
	)
	require.NoError(t, err)
	assert.True(t, sigChecked)
}

func TestValidateEbCertificateQuorumFailure(t *testing.T) {
	fixture := newTestCommitteeFixture(t, 10)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// Bottom 2 members hold 20+10 = 30 of 550: far below tau = 3/4.
	votes := fixture.signVotes(t, 77, ebHash, 8, 9)
	cert, err := BuildEbCertificate(77, ebHash, fixture.committee, votes)
	require.NoError(t, err)

	_, err = ValidateEbCertificate(
		cert, fixture.committee, big.NewRat(3, 4), fixture.registry,
	)
	assert.ErrorIs(t, err, ErrQuorumNotMet)
}

func TestValidateEbCertificateUnknownSignerSkipsSigCheck(t *testing.T) {
	fixture := newTestCommitteeFixture(t, 10)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	votes := fixture.signVotes(t, 77, ebHash, 0, 1, 2, 3, 4)
	cert, err := BuildEbCertificate(77, ebHash, fixture.committee, votes)
	require.NoError(t, err)

	// Registry missing one signer's key: quorum still validates but the
	// aggregate signature cannot be checked.
	partialEntries := make(map[string]string)
	for _, member := range fixture.committee.Members[1:] {
		key := fixture.keys[member.VoterId]
		require.NotNil(t, key)
		partialEntries[hex.EncodeToString(member.PoolKeyHash)] =
			hex.EncodeToString(key.PublicKeyBytes())
	}
	partialRegistry, err := NewVoterRegistry(partialEntries)
	require.NoError(t, err)

	sigChecked, err := ValidateEbCertificate(
		cert, fixture.committee, big.NewRat(7, 10), partialRegistry,
	)
	require.NoError(t, err)
	assert.False(t, sigChecked)
}

func TestValidateEbCertificateTamperedSignature(t *testing.T) {
	fixture := newTestCommitteeFixture(t, 10)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	votes := fixture.signVotes(t, 77, ebHash, 0, 1, 2, 3, 4)
	cert, err := BuildEbCertificate(77, ebHash, fixture.committee, votes)
	require.NoError(t, err)

	// Replace the aggregate with a single member's signature
	require.NoError(
		t,
		cert.SetAggregatedSignature(votes[0].Signature),
	)
	_, err = ValidateEbCertificate(
		cert, fixture.committee, big.NewRat(7, 10), fixture.registry,
	)
	assert.ErrorIs(t, err, ErrInvalidSignature)
}

func TestValidateEbCertificateWrongBitfieldSize(t *testing.T) {
	fixture := newTestCommitteeFixture(t, 10)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	votes := fixture.signVotes(t, 77, ebHash, 0, 1, 2, 3, 4)
	cert, err := BuildEbCertificate(77, ebHash, fixture.committee, votes)
	require.NoError(t, err)

	// Validate against a committee of a different size
	smallFixture := newTestCommitteeFixture(t, 3)
	_, err = ValidateEbCertificate(
		cert,
		smallFixture.committee,
		big.NewRat(1, 100),
		fixture.registry,
	)
	assert.Error(t, err)
}
