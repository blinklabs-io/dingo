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

package epochengine

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/rewardlikelihood"
)

const RewardLikelihoodSampleCount = rewardlikelihood.SampleCount

func EncodeRewardLikelihood(values []float32) ([]byte, error) {
	return rewardlikelihood.Encode(values)
}

func DecodeRewardLikelihood(data []byte) ([]float32, error) {
	return rewardlikelihood.Decode(data)
}

func RewardLeaderProbability(
	activeSlotsCoeff, relativeStake, decentralization float64,
) float64 {
	return rewardlikelihood.LeaderProbability(
		activeSlotsCoeff,
		relativeStake,
		decentralization,
	)
}

func RewardPoolLikelihood(
	blocks uint64,
	slots uint64,
	leaderProbability float64,
) []float32 {
	return rewardlikelihood.Pool(blocks, slots, leaderProbability)
}

func CombineRewardLikelihood(previous, current []float32) ([]float32, error) {
	return rewardlikelihood.Combine(previous, current)
}

func RewardPerformanceEstimate(likelihood []float32) (float64, error) {
	return rewardlikelihood.Performance(likelihood)
}

func rewardRatFloat64(r interface{ Float64() (float64, bool) }) float64 {
	value, _ := r.Float64()
	return value
}

func RewardLikelihoodHistoryComplete(
	previous *models.RewardLikelihoodState,
	epoch uint64,
	epochs StakeRewardEpochs,
) bool {
	if previous != nil {
		return previous.Complete
	}
	return epoch == 1 ||
		(epochs.Bootstrap && epochs.Snapshot == epochs.Pots)
}

func (e *Engine) SaveRewardLikelihoodForApplication(
	app *StakeRewardApplication,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) error {
	if app == nil {
		return errors.New("save reward likelihood: application is nil")
	}
	ldb := e.host.RewardLikelihoodDB()
	epoch := app.Epochs.Pots + 1
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
			values, err := DecodeRewardLikelihood(row.LogWeights)
			if err != nil {
				return fmt.Errorf(
					"decode likelihood for pool %x: %w",
					row.PoolKeyHash,
					err,
				)
			}
			previous[string(row.PoolKeyHash)] = values
		}
	}
	activeSlotsCoeff := rewardRatFloat64(app.Params.ActiveSlotsCoeff)
	decentralization := rewardRatFloat64(app.Params.Decentralization)
	rows := make(
		[]*models.RewardPoolLikelihood,
		0,
		len(app.LikelihoodPoolInputs),
	)
	for _, pool := range app.LikelihoodPoolInputs {
		if pool == nil {
			continue
		}
		relativeStake := 0.0
		if app.TotalCirculation != 0 {
			relativeStake = float64(pool.DelegatedStake) /
				float64(app.TotalCirculation)
		}
		probability := RewardLeaderProbability(
			activeSlotsCoeff,
			relativeStake,
			decentralization,
		)
		blocks := app.LikelihoodBlockCounts[string(pool.PoolKeyHash)]
		if blocks > app.Params.EpochLength {
			return fmt.Errorf(
				"reward likelihood for pool %x: %d blocks exceed %d epoch slots",
				pool.PoolKeyHash,
				blocks,
				app.Params.EpochLength,
			)
		}
		current := RewardPoolLikelihood(
			blocks,
			app.Params.EpochLength,
			probability,
		)
		old := previous[string(pool.PoolKeyHash)]
		if old == nil {
			old = make([]float32, RewardLikelihoodSampleCount)
		}
		combined, err := CombineRewardLikelihood(old, current)
		if err != nil {
			return err
		}
		encoded, err := EncodeRewardLikelihood(combined)
		if err != nil {
			return err
		}
		estimate, err := RewardPerformanceEstimate(combined)
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
		RewardPot:    types.Uint64(app.AvailableRewards),
		CapturedSlot: app.Pots.CapturedSlot,
		BoundarySlot: boundarySlot,
		Complete: RewardLikelihoodHistoryComplete(
			previousState,
			epoch,
			app.Epochs,
		),
	}
	if err := ldb.SaveRewardLikelihoodState(state, rows, txn); err != nil {
		return fmt.Errorf("save reward likelihood state: %w", err)
	}
	return nil
}
