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
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// quorumCommittee seats pools 1..5 with stakes 100, 90, 80, 70 and 10 out of
// a total active stake of 550, so some stake is not seated at all.
func quorumCommittee() *Committee {
	stakes := []uint64{100, 90, 80, 70, 10}
	committee := &Committee{TotalActiveStake: 550}
	for i, stake := range stakes {
		committee.Members = append(committee.Members, CommitteeMember{
			VoterId:     uint64(i),
			PoolKeyHash: []byte{byte(i + 1)},
			Stake:       stake,
		})
		committee.CommitteeStake += stake
	}
	return committee
}

func keyedPools(pools ...byte) func([]byte) bool {
	keyed := make(map[byte]struct{}, len(pools))
	for _, pool := range pools {
		keyed[pool] = struct{}{}
	}
	return func(hash []byte) bool {
		_, ok := keyed[hash[0]]
		return ok
	}
}

func TestCommitteeKeysReachQuorum(t *testing.T) {
	t.Parallel()
	// 7/11 of 550 is exactly 350, the stake of every seat together.
	tau := big.NewRat(7, 11)
	cases := []struct {
		name string
		keys func([]byte) bool
		tau  *big.Rat
		want bool
	}{
		{"exactly the threshold", keyedPools(1, 2, 3, 4, 5), tau, true},
		{"one stake unit below the threshold", keyedPools(1, 2, 3, 4), tau, false},
		{"above the threshold", keyedPools(1, 2, 3, 4, 5), big.NewRat(6, 11), true},
		{"keyless seats count nothing", keyedPools(1, 2, 3, 5), tau, false},
		{"no seat keyed", keyedPools(), tau, false},
		{"zero threshold needs no key", keyedPools(), new(big.Rat), true},
		{"full threshold needs all active stake", keyedPools(1, 2, 3, 4, 5), big.NewRat(1, 1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := CommitteeKeysReachQuorum(quorumCommittee(), tc.keys, tc.tau)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// The denominator is the total active stake, so stake that holds no seat
// never counts towards the threshold.
func TestCommitteeKeysReachQuorumCountsSeatedStakeOnly(t *testing.T) {
	t.Parallel()
	committee := quorumCommittee()
	got, err := CommitteeKeysReachQuorum(
		committee, keyedPools(1, 2, 3, 4, 5), big.NewRat(351, 550),
	)
	require.NoError(t, err)
	require.False(t, got, "350 keyed stake is below 351/550")

	got, err = CommitteeKeysReachQuorum(
		committee, keyedPools(1, 2, 3, 4, 5), big.NewRat(350, 550),
	)
	require.NoError(t, err)
	require.True(t, got, "350 keyed stake meets 350/550")
}

func TestCommitteeKeysReachQuorumEmptyCommittee(t *testing.T) {
	t.Parallel()
	for _, committee := range []*Committee{nil, {}} {
		got, err := CommitteeKeysReachQuorum(committee, keyedPools(), new(big.Rat))
		require.NoError(t, err)
		require.True(t, got, "no stake reaches a zero threshold")

		got, err = CommitteeKeysReachQuorum(committee, keyedPools(), big.NewRat(1, 100))
		require.NoError(t, err)
		require.False(t, got, "no stake cannot reach a positive threshold")
	}
}

func TestCommitteeKeysReachQuorumRejectsInvalidThreshold(t *testing.T) {
	t.Parallel()
	for _, tau := range []*big.Rat{nil, big.NewRat(-1, 2), big.NewRat(3, 2)} {
		_, err := CommitteeKeysReachQuorum(quorumCommittee(), keyedPools(1), tau)
		require.ErrorIs(t, err, ErrInvalidQuorumStakeThreshold)
	}
}

// keyedFixture seats ten pools of stakes 100, 90, ..., 10 and registers a key
// for the first keyedPools of them, so 340 of the 550 active stake is keyed
// when four are.
func keyedFixture(t *testing.T, keyedPools int) *managerFixture {
	t.Helper()
	return newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			entries := make(map[string]string, keyedPools)
			for i := range keyedPools {
				hash := testPoolHash(byte(i + 1))
				entries[hash] = f.registryEntries[hash]
			}
			registry, err := NewVoterRegistry(entries)
			require.NoError(t, err)
			cfg.Registry = registry
		},
	)
}

func TestVoteManagerCommitteeCanReachQuorum(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		keyed int
		tau   *big.Rat
		want  bool
	}{
		{"every seat keyed", 10, big.NewRat(7, 10), true},
		{"keyed stake above the threshold", 4, big.NewRat(6, 10), true},
		{"keyed stake exactly the threshold", 4, big.NewRat(340, 550), true},
		{"keyed stake just below the threshold", 4, big.NewRat(341, 550), false},
		{"keyless seats hold the rest", 4, big.NewRat(7, 10), false},
		{"no key registered", 0, big.NewRat(1, 100), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := keyedFixture(t, tc.keyed)
			got, err := fixture.mgr.CommitteeCanReachQuorum(5, tc.tau)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// The threshold is the caller's: the one the committee was selected under
// does not decide the answer.
func TestVoteManagerCommitteeCanReachQuorumUsesTheGivenThreshold(t *testing.T) {
	t.Parallel()
	fixture := keyedFixture(t, 4)
	require.Zero(
		t,
		big.NewRat(7, 10).Cmp(fixture.params.tau),
		"the committee's own threshold is 7/10",
	)
	got, err := fixture.mgr.CommitteeCanReachQuorum(5, big.NewRat(6, 10))
	require.NoError(t, err)
	require.True(t, got)
	got, err = fixture.mgr.CommitteeCanReachQuorum(5, big.NewRat(7, 10))
	require.NoError(t, err)
	require.False(t, got)
}

func TestVoteManagerCommitteeCanReachQuorumEmptyStake(t *testing.T) {
	t.Parallel()
	fixture := keyedFixture(t, 4)
	fixture.stake.setError(ErrEmptyStakeDistribution)

	got, err := fixture.mgr.CommitteeCanReachQuorum(6, big.NewRat(1, 10))
	require.NoError(t, err)
	require.False(t, got)
	got, err = fixture.mgr.CommitteeCanReachQuorum(6, new(big.Rat))
	require.NoError(t, err)
	require.True(t, got)
}

func TestVoteManagerCommitteeCanReachQuorumPropagatesFailure(t *testing.T) {
	t.Parallel()
	fixture := keyedFixture(t, 4)
	snapshotErr := errors.New("snapshot not ready")
	fixture.stake.setError(snapshotErr)

	got, err := fixture.mgr.CommitteeCanReachQuorum(7, big.NewRat(1, 10))
	require.ErrorIs(t, err, snapshotErr)
	require.False(t, got)

	// A failure is not memoized: the next call recovers.
	fixture.stake.setError(nil)
	got, err = fixture.mgr.CommitteeCanReachQuorum(7, big.NewRat(1, 10))
	require.NoError(t, err)
	require.True(t, got)
}
