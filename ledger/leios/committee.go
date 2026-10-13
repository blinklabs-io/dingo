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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/consensus/praos"
)

var (
	// ErrEmptyStakeDistribution is returned when no registered pools are
	// available to form a committee.
	ErrEmptyStakeDistribution = errors.New("empty stake distribution")
	// ErrInvalidCommitteeSize is returned when committee size is zero.
	ErrInvalidCommitteeSize = errors.New(
		"committee size must be greater than zero",
	)
)

// CommitteeMember is one selected pool in an epoch's voting committee.
type CommitteeMember struct {
	VoterId     uint64
	PoolKeyHash []byte // 28-byte pool key hash
	Stake       uint64 // active stake (lovelace) from the snapshot
}

// Committee is a deterministic voting committee for an epoch, computed by
// ComputeCommittee: the first N snapshot pools ordered by stake descending,
// breaking equal-stake ties by pool key hash ascending, with VoterId assigned
// from a member's index in that order.
type Committee struct {
	Epoch            uint64
	SnapshotEpoch    uint64
	Members          []CommitteeMember // index == VoterId
	TotalActiveStake uint64            // stake-quorum denominator
	CommitteeStake   uint64            // sum of member stakes
	byPoolHex        map[string]uint64 // hex pool key hash -> VoterId
}

type poolStake struct {
	hash  []byte
	stake uint64
}

// prepareCommitteePools parses and validates the stake distribution passed
// to ComputeCommittee, decoding every registered pool key hash, including
// pools with zero active stake.
func prepareCommitteePools(
	poolStakes map[string]uint64,
) ([]poolStake, error) {
	pools := make([]poolStake, 0, len(poolStakes))
	for hashHex, stake := range poolStakes {
		hash, err := hex.DecodeString(hashHex)
		if err != nil {
			return nil, fmt.Errorf(
				"malformed pool key hash %q: %w",
				hashHex,
				err,
			)
		}
		if len(hash) != voterPoolKeyHashSize {
			return nil, fmt.Errorf(
				"malformed pool key hash %q: must be %d bytes",
				hashHex,
				voterPoolKeyHashSize,
			)
		}
		pools = append(pools, poolStake{hash: hash, stake: stake})
	}
	if len(pools) == 0 {
		return nil, ErrEmptyStakeDistribution
	}
	return pools, nil
}

// buildCommittee assigns member indices and the lookup map after
// ComputeCommittee has applied its top-N ordering and selection.
func buildCommittee(
	epoch uint64,
	snapshotEpoch uint64,
	totalActiveStake uint64,
	pools []poolStake,
) *Committee {
	committee := &Committee{
		Epoch:            epoch,
		SnapshotEpoch:    snapshotEpoch,
		Members:          make([]CommitteeMember, 0, len(pools)),
		TotalActiveStake: totalActiveStake,
		byPoolHex:        make(map[string]uint64, len(pools)),
	}
	for _, pool := range pools {
		voterId := uint64(len(committee.Members))
		committee.Members = append(committee.Members, CommitteeMember{
			VoterId:     voterId,
			PoolKeyHash: pool.hash,
			Stake:       pool.stake,
		})
		committee.byPoolHex[hex.EncodeToString(pool.hash)] = voterId
		committee.CommitteeStake += pool.stake
	}
	return committee
}

// CommitteeSnapshotEpoch returns the epoch whose mark stake snapshot is active
// for the given epoch. This must stay in lockstep with Praos leader election.
func CommitteeSnapshotEpoch(epoch uint64) uint64 {
	return praos.StakeSnapshotEpoch(epoch)
}

// ComputeCommittee selects the first committeeSize pools from the snapshot,
// ordered by stake descending and pool key hash ascending for equal stake.
// Zero-stake registered pools remain candidates and receive seats when the
// configured size reaches them. poolStakes maps lowercase-hex pool key hashes
// to stake in lovelace.
func ComputeCommittee(
	epoch uint64,
	snapshotEpoch uint64,
	poolStakes map[string]uint64,
	totalActiveStake uint64,
	committeeSize uint64,
) (*Committee, error) {
	if committeeSize == 0 {
		return nil, ErrInvalidCommitteeSize
	}
	pools, err := prepareCommitteePools(poolStakes)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(pools, func(a, b poolStake) int {
		// Stake descending, pool key hash ascending for equal stake
		if a.stake != b.stake {
			if a.stake > b.stake {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.hash, b.hash)
	})
	selectedCount := len(pools)
	if committeeSize < uint64(selectedCount) {
		// This branch bounds the conversion by the slice length.
		//nolint:gosec // committeeSize is strictly less than len(pools).
		selectedCount = int(committeeSize)
	}
	return buildCommittee(
		epoch,
		snapshotEpoch,
		totalActiveStake,
		pools[:selectedCount],
	), nil
}

// Size returns the number of committee members.
func (c *Committee) Size() uint64 {
	return uint64(len(c.Members))
}

// Member returns the committee member with the given voter id.
func (c *Committee) Member(voterId uint64) (CommitteeMember, bool) {
	if voterId >= uint64(len(c.Members)) {
		return CommitteeMember{}, false
	}
	return c.Members[voterId], true
}

// VoterIdFor returns the voter id assigned to the given pool key hash.
func (c *Committee) VoterIdFor(poolKeyHash []byte) (uint64, bool) {
	voterId, ok := c.byPoolHex[hex.EncodeToString(poolKeyHash)]
	return voterId, ok
}
