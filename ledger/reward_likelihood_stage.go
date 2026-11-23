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

package ledger

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

func (s stagingLedgerDB) SaveRewardLikelihoodState(
	state *models.RewardLikelihoodState,
	pools []*models.RewardPoolLikelihood,
	txn ledgerdb.Txn,
) error {
	if state == nil {
		return errors.New("save reward likelihood state: state is nil")
	}
	cp := *state
	rows := make([]*models.RewardPoolLikelihood, 0, len(pools))
	for _, pool := range pools {
		if pool == nil {
			return errors.New("save reward pool likelihood: pool is nil")
		}
		if pool.Epoch != state.Epoch {
			return fmt.Errorf(
				"save reward pool likelihood: pool epoch %d does not match state epoch %d",
				pool.Epoch,
				state.Epoch,
			)
		}
		p := *pool
		rows = append(rows, &p)
	}
	s.stageRewardInput(txn, RewardInputChange{
		Kind:            RewardInputLikelihood,
		Epoch:           state.Epoch,
		Likelihood:      &cp,
		LikelihoodPools: rows,
	})
	return nil
}

// GetRewardLikelihoodStateBeforeEpoch returns the state of the newest epoch
// below epoch, or nil.
func (s stagingLedgerDB) GetRewardLikelihoodStateBeforeEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.RewardLikelihoodState, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return nil, err
	}
	states, err := v.likelihoodStates()
	if err != nil {
		return nil, err
	}
	var out *models.RewardLikelihoodState
	for _, st := range states {
		if st.Epoch < epoch {
			out = st
		}
	}
	return out, nil
}

// GetRewardLikelihoodStateAtSlot returns the state visible at slot: the one
// installed at the latest boundary at or before it, the newest epoch among
// those.
func (s stagingLedgerDB) GetRewardLikelihoodStateAtSlot(
	slot uint64,
	txn ledgerdb.Txn,
) (*models.RewardLikelihoodState, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return nil, err
	}
	states, err := v.likelihoodStates()
	if err != nil {
		return nil, err
	}
	var out *models.RewardLikelihoodState
	for _, st := range states {
		if st.BoundarySlot > slot {
			continue
		}
		if out == nil || st.BoundarySlot >= out.BoundarySlot {
			out = st
		}
	}
	return out, nil
}

func (s stagingLedgerDB) GetRewardPoolLikelihoods(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolLikelihood, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return nil, err
	}
	return v.poolLikelihoods(epoch)
}

// likelihoodStates returns the likelihood states as txn sees them, oldest
// epoch first.
func (v rewardInputsView) likelihoodStates() (
	[]*models.RewardLikelihoodState,
	error,
) {
	states, err := v.r.RewardLikelihoodStates(v.at)
	if err != nil {
		return nil, err
	}
	for _, c := range v.st.changes {
		if c.Kind != RewardInputLikelihood {
			continue
		}
		cp := *c.Likelihood
		i, found := slices.BinarySearchFunc(
			states, cp.Epoch,
			func(st *models.RewardLikelihoodState, e uint64) int {
				switch {
				case st.Epoch < e:
					return -1
				case st.Epoch > e:
					return 1
				}
				return 0
			},
		)
		if found {
			states[i] = &cp
		} else {
			states = slices.Insert(states, i, &cp)
		}
	}
	return states, nil
}

// poolLikelihoods returns the pool likelihoods of epoch's state as txn sees
// them, ordered by pool. A staged save replaces the epoch's rows.
func (v rewardInputsView) poolLikelihoods(
	epoch uint64,
) ([]*models.RewardPoolLikelihood, error) {
	var staged []*models.RewardPoolLikelihood
	found := false
	for _, c := range v.st.changes {
		if c.Kind == RewardInputLikelihood && c.Likelihood.Epoch == epoch {
			staged, found = c.LikelihoodPools, true
		}
	}
	if !found {
		return v.r.RewardPoolLikelihoodRows(epoch, v.at)
	}
	out := make([]*models.RewardPoolLikelihood, 0, len(staged))
	for _, p := range staged {
		cp := *p
		out = append(out, &cp)
	}
	slices.SortFunc(out, func(a, b *models.RewardPoolLikelihood) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	return out, nil
}
