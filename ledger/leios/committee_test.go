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
	"math"
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

func TestComputeCommitteeOrdersByStakeDescending(t *testing.T) {
	poolStakes := map[string]uint64{
		testPoolHash(1): 15,
		testPoolHash(2): 50,
		testPoolHash(3): 30,
		testPoolHash(4): 5,
	}
	committee, err := ComputeCommittee(
		10, 8, poolStakes, 100, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 4)
	assert.Equal(t, uint64(10), committee.Epoch)
	assert.Equal(t, uint64(8), committee.SnapshotEpoch)
	assert.Equal(t, uint64(100), committee.TotalActiveStake)
	assert.Equal(t, uint64(100), committee.CommitteeStake)
	stakes := make([]uint64, 0, len(committee.Members))
	for i, member := range committee.Members {
		// VoterId is the position in the selected order
		assert.Equal(t, uint64(i), member.VoterId) // #nosec G115
		stakes = append(stakes, member.Stake)
	}
	assert.Equal(t, []uint64{50, 30, 15, 5}, stakes)
	assert.Equal(t, byte(2), committee.Members[0].PoolKeyHash[0])
	assert.Equal(t, byte(3), committee.Members[1].PoolKeyHash[0])
	assert.Equal(t, byte(1), committee.Members[2].PoolKeyHash[0])
	assert.Equal(t, byte(4), committee.Members[3].PoolKeyHash[0])
}

func TestComputeCommitteeBreaksTiesByPoolKeyHashAscending(t *testing.T) {
	poolStakes := map[string]uint64{
		testPoolHash(7): 25,
		testPoolHash(3): 25,
		testPoolHash(9): 25,
		testPoolHash(5): 25,
	}
	committee, err := ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 4)
	assert.Equal(t, byte(3), committee.Members[0].PoolKeyHash[0])
	assert.Equal(t, byte(5), committee.Members[1].PoolKeyHash[0])
	assert.Equal(t, byte(7), committee.Members[2].PoolKeyHash[0])
	assert.Equal(t, byte(9), committee.Members[3].PoolKeyHash[0])
}

func TestComputeCommitteeStopsAtThresholdCrossing(t *testing.T) {
	poolStakes := map[string]uint64{
		testPoolHash(1): 50,
		testPoolHash(2): 30,
		testPoolHash(3): 15,
		testPoolHash(4): 5,
	}
	// 50+30 = 80 meets sigma_c = 80/100 exactly: the crossing pool is
	// included and selection stops after it.
	committee, err := ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(80, 100),
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 2)
	assert.Equal(t, uint64(80), committee.CommitteeStake)

	// 80 < 81: the third pool is needed.
	committee, err = ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(81, 100),
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 3)
	assert.Equal(t, uint64(95), committee.CommitteeStake)

	// A single pool already crossing the threshold forms the committee.
	committee, err = ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(1, 2),
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 1)
	assert.Equal(t, uint64(50), committee.Members[0].Stake)
}

func TestComputeCommitteeExcludesZeroStakePools(t *testing.T) {
	poolStakes := map[string]uint64{
		testPoolHash(1): 60,
		testPoolHash(2): 0,
		testPoolHash(3): 40,
	}
	committee, err := ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	require.Len(t, committee.Members, 2)
	for _, member := range committee.Members {
		assert.NotEqual(t, byte(2), member.PoolKeyHash[0])
		assert.NotZero(t, member.Stake)
	}
}

func TestComputeCommitteeFullCoverageSelectsAllPools(t *testing.T) {
	poolStakes := make(map[string]uint64)
	for i := range byte(50) {
		poolStakes[testPoolHash(i+1)] = uint64(i+1) * 100
	}
	var total uint64
	for _, stake := range poolStakes {
		total += stake
	}
	committee, err := ComputeCommittee(
		1, 0, poolStakes, total, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	assert.Len(t, committee.Members, 50)
	assert.Equal(t, total, committee.CommitteeStake)
}

func TestComputeCommitteeEmptyDistribution(t *testing.T) {
	_, err := ComputeCommittee(
		1, 0, map[string]uint64{}, 100, big.NewRat(1, 1),
	)
	assert.ErrorIs(t, err, ErrEmptyStakeDistribution)

	// All-zero stakes are equivalent to an empty distribution
	_, err = ComputeCommittee(
		1, 0,
		map[string]uint64{testPoolHash(1): 0},
		100, big.NewRat(1, 1),
	)
	assert.ErrorIs(t, err, ErrEmptyStakeDistribution)

	// Zero total active stake cannot form a committee
	_, err = ComputeCommittee(
		1, 0,
		map[string]uint64{testPoolHash(1): 10},
		0, big.NewRat(1, 1),
	)
	assert.ErrorIs(t, err, ErrEmptyStakeDistribution)
}

func TestComputeCommitteeInvalidCoverage(t *testing.T) {
	poolStakes := map[string]uint64{testPoolHash(1): 100}
	for _, sigmaC := range []*big.Rat{
		nil,
		big.NewRat(0, 1),
		big.NewRat(-1, 2),
		big.NewRat(101, 100),
	} {
		_, err := ComputeCommittee(1, 0, poolStakes, 100, sigmaC)
		assert.ErrorIs(
			t, err, ErrInvalidCommitteeStakeCoverage,
			"sigma_c=%v", sigmaC,
		)
	}
}

func TestComputeCommitteeMalformedPoolKeyHash(t *testing.T) {
	_, err := ComputeCommittee(
		1, 0,
		map[string]uint64{"not-hex": 100},
		100, big.NewRat(1, 1),
	)
	assert.Error(t, err)
}

func TestComputeCommitteeLargeStakesNoOverflow(t *testing.T) {
	// Products of stake and rational components overflow uint64; the
	// comparison must be exact in big.Int.
	const huge = math.MaxUint64 / 2
	poolStakes := map[string]uint64{
		testPoolHash(1): huge,
		testPoolHash(2): huge,
	}
	committee, err := ComputeCommittee(
		1, 0, poolStakes, math.MaxUint64-1, big.NewRat(99, 100),
	)
	require.NoError(t, err)
	assert.Len(t, committee.Members, 2)
}

func TestComputeCommitteeDeterministic(t *testing.T) {
	poolStakes := make(map[string]uint64)
	for i := range byte(100) {
		poolStakes[testPoolHash(i+1)] = uint64((i*37)%50) + 1
	}
	var total uint64
	for _, stake := range poolStakes {
		total += stake
	}
	first, err := ComputeCommittee(
		3, 1, poolStakes, total, big.NewRat(9, 10),
	)
	require.NoError(t, err)
	for range 10 {
		next, err := ComputeCommittee(
			3, 1, poolStakes, total, big.NewRat(9, 10),
		)
		require.NoError(t, err)
		require.Equal(t, first.Members, next.Members)
	}
}

func TestCommitteeMemberLookups(t *testing.T) {
	poolStakes := map[string]uint64{
		testPoolHash(1): 60,
		testPoolHash(2): 40,
	}
	committee, err := ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), committee.Size())

	member, ok := committee.Member(0)
	require.True(t, ok)
	assert.Equal(t, uint64(60), member.Stake)
	_, ok = committee.Member(2)
	assert.False(t, ok)

	poolHash, err := hex.DecodeString(testPoolHash(2))
	require.NoError(t, err)
	voterId, ok := committee.VoterIdFor(poolHash)
	require.True(t, ok)
	assert.Equal(t, uint64(1), voterId)

	unknown := make([]byte, 28)
	unknown[0] = 99
	_, ok = committee.VoterIdFor(unknown)
	assert.False(t, ok)
}

func TestCommitteeSnapshotEpoch(t *testing.T) {
	for _, tc := range []struct{ epoch, want uint64 }{
		{0, 0}, {1, 0}, {2, 0}, {3, 1}, {10, 8},
	} {
		assert.Equal(
			t, tc.want, CommitteeSnapshotEpoch(tc.epoch),
			fmt.Sprintf("epoch %d", tc.epoch),
		)
	}
}

func TestComputeCommitteeUnreachableCoverage(t *testing.T) {
	// Inconsistent inputs: the pools sum to less than total active
	// stake, so the coverage target can never be reached. Returning a
	// partial committee would break downstream stake-quorum
	// assumptions.
	poolStakes := map[string]uint64{
		testPoolHash(1): 30,
		testPoolHash(2): 20,
	}
	_, err := ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(9, 10),
	)
	assert.Error(t, err)

	// The same pools satisfy a reachable target
	committee, err := ComputeCommittee(
		1, 0, poolStakes, 100, big.NewRat(1, 2),
	)
	require.NoError(t, err)
	assert.Len(t, committee.Members, 2)
}
