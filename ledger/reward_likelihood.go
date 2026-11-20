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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/rewardlikelihood"
)

const rewardLikelihoodSampleCount = rewardlikelihood.SampleCount

func encodeRewardLikelihood(values []float32) ([]byte, error) {
	return rewardlikelihood.Encode(values)
}

func decodeRewardLikelihood(data []byte) ([]float32, error) {
	return rewardlikelihood.Decode(data)
}

func rewardLeaderProbability(activeSlotsCoeff, relativeStake, decentralization float64) float64 {
	return rewardlikelihood.LeaderProbability(
		activeSlotsCoeff,
		relativeStake,
		decentralization,
	)
}

func rewardPoolLikelihood(
	blocks uint64,
	slots uint64,
	leaderProbability float64,
) []float32 {
	return rewardlikelihood.Pool(blocks, slots, leaderProbability)
}

func combineRewardLikelihood(previous, current []float32) ([]float32, error) {
	return rewardlikelihood.Combine(previous, current)
}

func rewardPerformanceEstimate(likelihood []float32) (float64, error) {
	return rewardlikelihood.Performance(likelihood)
}

func rewardRatFloat64(r interface{ Float64() (float64, bool) }) float64 {
	value, _ := r.Float64()
	return value
}

func rewardLikelihoodHistoryComplete(
	previous *models.RewardLikelihoodState,
	epoch uint64,
	epochs stakeRewardEpochs,
) bool {
	if previous != nil {
		return previous.Complete
	}
	return epoch == 1 ||
		(epochs.bootstrap && epochs.snapshot == epochs.pots)
}

func (ls *LedgerState) saveRewardLikelihoodForApplication(
	app *stakeRewardApplication,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) error {
	if app == nil {
		return errors.New("save reward likelihood: application is nil")
	}
	ldb := ls.ledgerDB()
	epoch := app.epochs.pots + 1
	previousState, err := ldb.GetRewardLikelihoodStateBeforeEpoch(epoch, txn)
	if err != nil {
		return fmt.Errorf("get previous reward likelihood state: %w", err)
	}
	previous := make(map[string][]float32)
	if previousState != nil {
		rows, err := ldb.GetRewardPoolLikelihoods(previousState.Epoch, txn)
		if err != nil {
			return fmt.Errorf("get previous reward pool likelihoods: %w", err)
		}
		for _, row := range rows {
			values, err := decodeRewardLikelihood(row.LogWeights)
			if err != nil {
				return fmt.Errorf("decode likelihood for pool %x: %w", row.PoolKeyHash, err)
			}
			previous[string(row.PoolKeyHash)] = values
		}
	}
	activeSlotsCoeff := rewardRatFloat64(app.params.ActiveSlotsCoeff)
	decentralization := rewardRatFloat64(app.params.Decentralization)
	rows := make([]*models.RewardPoolLikelihood, 0, len(app.likelihoodPoolInputs))
	for _, pool := range app.likelihoodPoolInputs {
		if pool == nil {
			continue
		}
		relativeStake := 0.0
		if app.snapshotTotalActiveStake != 0 {
			relativeStake = float64(pool.DelegatedStake) /
				float64(app.snapshotTotalActiveStake)
		}
		probability := rewardLeaderProbability(
			activeSlotsCoeff,
			relativeStake,
			decentralization,
		)
		blocks := app.likelihoodBlockCounts[string(pool.PoolKeyHash)]
		if blocks > app.params.EpochLength {
			return fmt.Errorf(
				"reward likelihood for pool %x: %d blocks exceed %d epoch slots",
				pool.PoolKeyHash,
				blocks,
				app.params.EpochLength,
			)
		}
		current := rewardPoolLikelihood(
			blocks,
			app.params.EpochLength,
			probability,
		)
		old := previous[string(pool.PoolKeyHash)]
		if old == nil {
			old = make([]float32, rewardLikelihoodSampleCount)
		}
		combined, err := combineRewardLikelihood(old, current)
		if err != nil {
			return err
		}
		encoded, err := encodeRewardLikelihood(combined)
		if err != nil {
			return err
		}
		estimate, err := rewardPerformanceEstimate(combined)
		if err != nil {
			return err
		}
		rows = append(rows, &models.RewardPoolLikelihood{
			Epoch:               epoch,
			PoolKeyHash:         pool.PoolKeyHash,
			LogWeights:          encoded,
			PerformanceEstimate: estimate,
		})
	}
	state := &models.RewardLikelihoodState{
		Epoch:        epoch,
		RewardPot:    types.Uint64(app.availableRewards),
		CapturedSlot: app.pots.CapturedSlot,
		BoundarySlot: boundarySlot,
		Complete: rewardLikelihoodHistoryComplete(
			previousState,
			epoch,
			app.epochs,
		),
	}
	if err := ldb.SaveRewardLikelihoodState(state, rows, txn); err != nil {
		return fmt.Errorf("save reward likelihood state: %w", err)
	}
	return nil
}
