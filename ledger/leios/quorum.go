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
)

var (
	// ErrInvalidQuorumStakeThreshold is returned when the quorum stake
	// threshold is not in [0, 1].
	ErrInvalidQuorumStakeThreshold = errors.New(
		"quorum stake threshold must be in [0, 1]",
	)
	// ErrZeroTotalActiveStake is returned when the quorum denominator is
	// zero.
	ErrZeroTotalActiveStake = errors.New("total active stake is zero")
)

// MeetsStakeQuorum reports whether votedStake represents at least
// quorumStakeThreshold (tau) of totalActiveStake. The comparison is exact:
// votedStake * tau.Denom() >= tau.Num() * totalActiveStake evaluated in
// big.Int, since the products overflow uint64. The denominator is total
// active stake, not committee stake, per CIP-0164.
func MeetsStakeQuorum(
	votedStake uint64,
	totalActiveStake uint64,
	quorumStakeThreshold *big.Rat,
) (bool, error) {
	one := big.NewRat(1, 1)
	if quorumStakeThreshold == nil ||
		quorumStakeThreshold.Sign() < 0 ||
		quorumStakeThreshold.Cmp(one) > 0 {
		return false, ErrInvalidQuorumStakeThreshold
	}
	if totalActiveStake == 0 {
		return false, ErrZeroTotalActiveStake
	}
	// votedStake * tau.Denom() >= tau.Num() * totalActiveStake
	lhs := new(big.Int).Mul(
		new(big.Int).SetUint64(votedStake),
		quorumStakeThreshold.Denom(),
	)
	rhs := new(big.Int).Mul(
		quorumStakeThreshold.Num(),
		new(big.Int).SetUint64(totalActiveStake),
	)
	return lhs.Cmp(rhs) >= 0, nil
}
