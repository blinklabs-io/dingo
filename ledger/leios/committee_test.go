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
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testPoolHash returns a deterministic 28-byte pool key hash whose first
// byte is id, as a lowercase hex string.
func testPoolHash(id byte) string {
	hash := make([]byte, 28)
	hash[0] = id
	return hex.EncodeToString(hash)
}

func TestComputeCommitteeSelectsTopNByStake(t *testing.T) {
	t.Parallel()

	poolStakes := map[string]uint64{
		testPoolHash(1): 15,
		testPoolHash(2): 50,
		testPoolHash(3): 30,
		testPoolHash(4): 5,
	}
	committee, err := ComputeCommittee(10, 8, poolStakes, 100, 2)
	require.NoError(t, err)
	require.Len(t, committee.Members, 2)
	assert.Equal(t, uint64(10), committee.Epoch)
	assert.Equal(t, uint64(8), committee.SnapshotEpoch)
	assert.Equal(t, uint64(100), committee.TotalActiveStake)
	assert.Equal(t, uint64(80), committee.CommitteeStake)
	assert.Equal(t, byte(2), committee.Members[0].PoolKeyHash[0])
	assert.Equal(t, byte(3), committee.Members[1].PoolKeyHash[0])
}

func TestComputeCommitteeSizeExceedsCandidateCount(t *testing.T) {
	t.Parallel()

	committee, err := ComputeCommittee(
		1, 0,
		map[string]uint64{testPoolHash(1): 60, testPoolHash(2): 40},
		100,
		9,
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 2)
}

func TestComputeCommitteeIncludesZeroStakeSeatsInPoolIDOrder(t *testing.T) {
	t.Parallel()

	poolStakes := map[string]uint64{
		testPoolHash(1): 60,
		testPoolHash(2): 0,
		testPoolHash(3): 0,
		testPoolHash(4): 40,
	}
	committee, err := ComputeCommittee(1, 0, poolStakes, 100, 4)
	require.NoError(t, err)
	require.Len(t, committee.Members, 4)
	assert.Equal(t, []byte{1, 4, 2, 3}, []byte{
		committee.Members[0].PoolKeyHash[0],
		committee.Members[1].PoolKeyHash[0],
		committee.Members[2].PoolKeyHash[0],
		committee.Members[3].PoolKeyHash[0],
	})
	assert.Equal(t, uint64(100), committee.CommitteeStake)
	for i, member := range committee.Members {
		assert.Equal(t, uint64(i), member.VoterId)
	}
}

func TestComputeCommitteeZeroStakeSeatsDoNotAddQuorumWeight(t *testing.T) {
	t.Parallel()

	committee, err := ComputeCommittee(
		1, 0,
		map[string]uint64{testPoolHash(1): 100, testPoolHash(2): 0},
		100,
		2,
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 2)
	assert.Equal(t, uint64(0), committee.Members[1].Stake)
	met, err := MeetsStakeQuorum(committee.Members[1].Stake, committee.TotalActiveStake, big.NewRat(1, 2))
	require.NoError(t, err)
	assert.False(t, met)
}

func TestComputeCommitteeBreaksEqualStakeTiesByPoolID(t *testing.T) {
	t.Parallel()

	committee, err := ComputeCommittee(1, 0, map[string]uint64{
		testPoolHash(7): 25,
		testPoolHash(3): 25,
		testPoolHash(9): 25,
		testPoolHash(5): 25,
	}, 100, 3)
	require.NoError(t, err)
	require.Len(t, committee.Members, 3)
	assert.Equal(t, []byte{3, 5, 7}, []byte{
		committee.Members[0].PoolKeyHash[0],
		committee.Members[1].PoolKeyHash[0],
		committee.Members[2].PoolKeyHash[0],
	})
}

func TestComputeCommitteeRejectsInvalidSize(t *testing.T) {
	t.Parallel()

	_, err := ComputeCommittee(1, 0, map[string]uint64{testPoolHash(1): 100}, 100, 0)
	assert.ErrorIs(t, err, ErrInvalidCommitteeSize)
}

func TestComputeCommitteeRejectsEmptyDistribution(t *testing.T) {
	t.Parallel()

	_, err := ComputeCommittee(1, 0, map[string]uint64{}, 100, 1)
	assert.ErrorIs(t, err, ErrEmptyStakeDistribution)
}

func TestComputeCommitteeMalformedPoolKeyHash(t *testing.T) {
	t.Parallel()

	_, err := ComputeCommittee(1, 0, map[string]uint64{"not-hex": 100}, 100, 1)
	assert.Error(t, err)
}

func TestComputeCommitteeRejectsWrongPoolKeyHashLength(t *testing.T) {
	t.Parallel()

	poolStakes := map[string]uint64{hex.EncodeToString(make([]byte, 27)): 100}
	_, err := ComputeCommittee(1, 0, poolStakes, 100, 1)
	require.ErrorContains(t, err, "must be 28 bytes")
}

func TestComputeCommitteeDeterministic(t *testing.T) {
	t.Parallel()

	poolStakes := make(map[string]uint64)
	for i := range byte(100) {
		poolStakes[testPoolHash(i+1)] = uint64((i*37)%50) + 1
	}
	var total uint64
	for _, stake := range poolStakes {
		total += stake
	}
	first, err := ComputeCommittee(3, 1, poolStakes, total, 10)
	require.NoError(t, err)
	for range 10 {
		next, err := ComputeCommittee(3, 1, poolStakes, total, 10)
		require.NoError(t, err)
		require.Equal(t, first.Members, next.Members)
	}
}

func TestCommitteeMemberLookups(t *testing.T) {
	t.Parallel()

	committee, err := ComputeCommittee(1, 0, map[string]uint64{
		testPoolHash(1): 60,
		testPoolHash(2): 40,
	}, 100, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), committee.Size())

	member, ok := committee.Member(0)
	require.True(t, ok)
	assert.Equal(t, uint64(60), member.Stake)
	_, ok = committee.Member(2)
	assert.False(t, ok)

	voterId, ok := committee.VoterIdFor(member.PoolKeyHash)
	require.True(t, ok)
	assert.Equal(t, uint64(0), voterId)
	_, ok = committee.VoterIdFor(make([]byte, voterPoolKeyHashSize))
	assert.False(t, ok)
}
