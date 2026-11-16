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

// CommitteeKeysReachQuorum reports whether the seats of committee that have a
// usable voting key hold at least tau of the total active stake. Without
// stake behind the committee nothing is keyed, so only a zero threshold is
// reached. A seat without a key counts nothing: it cannot sign, so no
// certificate can lean on it.
//
// A committee that cannot reach this threshold can never certify an
// endorser block, so nothing that sizes itself for endorser blocks is worth
// enlarging until it can. hasKey reports whether the pool seated at the
// given pool key hash has a usable key.
func CommitteeKeysReachQuorum(
	committee *Committee,
	hasKey func(poolKeyHash []byte) bool,
	tau *big.Rat,
) (bool, error) {
	var keyedStake, totalActiveStake uint64
	if committee != nil {
		totalActiveStake = committee.TotalActiveStake
		for _, member := range committee.Members {
			if hasKey(member.PoolKeyHash) {
				keyedStake += member.Stake
			}
		}
	}
	if totalActiveStake == 0 {
		if tau == nil || tau.Sign() < 0 || tau.Cmp(big.NewRat(1, 1)) > 0 {
			return false, ErrInvalidQuorumStakeThreshold
		}
		return tau.Sign() == 0, nil
	}
	return MeetsStakeQuorum(keyedStake, totalActiveStake, tau)
}

// CommitteeCanReachQuorum reports whether the committee seated for epoch can
// reach tau with the voting keys resolved for it. tau is the threshold in
// force for epoch, not the one the committee was selected under.
//
// A committee that cannot be built because the stake snapshot holds no stake
// has no keyed seat, which a zero tau still satisfies. Any other failure is
// returned and is not memoized, so a later call retries.
func (m *VoteManager) CommitteeCanReachQuorum(
	epoch uint64,
	tau *big.Rat,
) (bool, error) {
	entry, err := m.committeeAndParamsForEpoch(epoch)
	if err != nil {
		if errors.Is(err, ErrEmptyStakeDistribution) ||
			errors.Is(err, ErrZeroTotalActiveStake) {
			return CommitteeKeysReachQuorum(nil, nil, tau)
		}
		return false, err
	}
	if entry == nil || entry.committee == nil {
		return false, errors.New("leios committee unavailable")
	}
	return CommitteeKeysReachQuorum(
		entry.committee,
		func(poolKeyHash []byte) bool {
			pub, ok := m.resolveVoterKey(entry, poolKeyHash)
			return ok && pub != nil
		},
		tau,
	)
}
