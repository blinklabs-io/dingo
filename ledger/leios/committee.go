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
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/consensus/praos"
)

var (
	// ErrEmptyStakeDistribution is returned when no pools with non-zero
	// stake are available to form a committee.
	ErrEmptyStakeDistribution = errors.New("empty stake distribution")
	// ErrInvalidCommitteeStakeCoverage is returned when the cumulative
	// stake coverage target is not in (0, 1].
	ErrInvalidCommitteeStakeCoverage = errors.New(
		"committee stake coverage must be in (0, 1]",
	)
)

// CommitteeMember is one selected pool in an epoch's voting committee.
type CommitteeMember struct {
	VoterId     uint64
	PoolKeyHash []byte // 28-byte pool key hash
	Stake       uint64 // active stake (lovelace) from the snapshot
}

// Committee is a deterministic voting committee for an epoch. Its membership
// and ordering depend on the constructor: ComputeCommittee selects a
// stake-coverage prefix ordered by stake descending, while
// ComputePrototypeCommittee includes every non-zero-stake pool ordered by
// stake ascending. Both constructors break equal-stake ties by pool key hash
// ascending and assign VoterId from the member's index in the resulting order.
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

// prepareCommitteePools parses and validates the stake distribution shared by
// both committee constructors. Keeping this in one place ensures they accept
// the same pool identifiers and exclude the same zero-stake entries.
func prepareCommitteePools(
	poolStakes map[string]uint64,
	totalActiveStake uint64,
) ([]poolStake, error) {
	pools := make([]poolStake, 0, len(poolStakes))
	for hashHex, stake := range poolStakes {
		if stake == 0 {
			continue
		}
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
	if len(pools) == 0 || totalActiveStake == 0 {
		return nil, ErrEmptyStakeDistribution
	}
	return pools, nil
}

// buildCommittee assigns the member indices and lookup map shared by both
// constructors after they have applied their protocol-specific ordering and
// selection rules.
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

// ComputeCommittee selects the voting committee for an epoch from the
// active stake distribution: pools are ordered by stake descending (pool
// key hash ascending breaks ties), then selected in order until their
// cumulative stake reaches committeeStakeCoverage (sigma_c) of
// totalActiveStake. The pool that crosses the threshold is included.
// Zero-stake pools are excluded. poolStakes maps lowercase-hex pool key
// hashes to stake in lovelace.
func ComputeCommittee(
	epoch uint64,
	snapshotEpoch uint64,
	poolStakes map[string]uint64,
	totalActiveStake uint64,
	committeeStakeCoverage *big.Rat,
) (*Committee, error) {
	one := big.NewRat(1, 1)
	if committeeStakeCoverage == nil ||
		committeeStakeCoverage.Sign() <= 0 ||
		committeeStakeCoverage.Cmp(one) > 0 {
		return nil, ErrInvalidCommitteeStakeCoverage
	}
	pools, err := prepareCommitteePools(poolStakes, totalActiveStake)
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
	// Selection threshold: cumStake * sigmaC.Denom() >= sigmaC.Num() *
	// totalActiveStake, evaluated in big.Int since the products overflow
	// uint64.
	target := new(big.Int).Mul(
		committeeStakeCoverage.Num(),
		new(big.Int).SetUint64(totalActiveStake),
	)
	cumStake := new(big.Int)
	scaledCum := new(big.Int)
	selectedCount := 0
	for _, pool := range pools {
		selectedCount++
		cumStake.Add(cumStake, new(big.Int).SetUint64(pool.stake))
		scaledCum.Mul(cumStake, committeeStakeCoverage.Denom())
		if scaledCum.Cmp(target) >= 0 {
			break
		}
	}
	if scaledCum.Cmp(target) < 0 {
		// Inconsistent inputs: the pools cannot cover the target
		// fraction of total active stake. A partial committee would
		// break downstream stake-quorum assumptions.
		return nil, fmt.Errorf(
			"committee stake coverage target %s unreachable: pool stake %d of total active stake %d",
			committeeStakeCoverage.RatString(),
			cumStake.Uint64(),
			totalActiveStake,
		)
	}
	return buildCommittee(
		epoch,
		snapshotEpoch,
		totalActiveStake,
		pools[:selectedCount],
	), nil
}

// ComputePrototypeCommittee reproduces the committee construction used by
// the current interoperable Leios prototype: every non-zero-stake pool votes,
// ordered by stake ascending with pool key hash ascending as the stable tie
// break. VoterId is the index in that order.
func ComputePrototypeCommittee(
	epoch uint64,
	snapshotEpoch uint64,
	poolStakes map[string]uint64,
	totalActiveStake uint64,
) (*Committee, error) {
	pools, err := prepareCommitteePools(poolStakes, totalActiveStake)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(pools, func(a, b poolStake) int {
		if a.stake != b.stake {
			if a.stake < b.stake {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.hash, b.hash)
	})
	return buildCommittee(epoch, snapshotEpoch, totalActiveStake, pools), nil
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
