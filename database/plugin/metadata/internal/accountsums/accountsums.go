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

package accountsums

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
)

// MIR source pot codes, matching models.MoveInstantaneousRewards.Pot.
const (
	mirPotReserves uint = 0
	mirPotTreasury uint = 1
)

// QueryAccountSums aggregates the lovelace totals Blockfrost reports on the
// account endpoint that are not derivable from the live account row:
//   - withdrawals_sum: every reward withdrawal recorded for the credential
//   - reserves_sum / treasury_sum: every MIR distribution to the credential,
//     split by the source pot
//
// All totals are reconstructed from persisted state, so they remain correct
// across rollbacks (the underlying rows are themselves rollback-aware).
func QueryAccountSums(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (models.AccountSums, error) {
	ret := models.AccountSums{}
	if len(stakingKey) == 0 {
		return ret, nil
	}

	withdrawals, err := sumWithdrawals(db, credentialTag, stakingKey)
	if err != nil {
		return models.AccountSums{}, err
	}
	ret.WithdrawalsSum = withdrawals

	reserves, err := sumMIRByPot(db, mirPotReserves, credentialTag, stakingKey)
	if err != nil {
		return models.AccountSums{}, err
	}
	ret.ReservesSum = reserves

	treasury, err := sumMIRByPot(db, mirPotTreasury, credentialTag, stakingKey)
	if err != nil {
		return models.AccountSums{}, err
	}
	ret.TreasurySum = treasury

	return ret, nil
}

func sumWithdrawals(
	db *gorm.DB,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	var total uint64
	if err := db.Model(&models.AccountRewardDelta{}).
		Where(
			"withdrawal = ? AND credential_tag = ? AND staking_key = ?",
			true,
			credentialTag,
			stakingKey,
		).
		Select("COALESCE(SUM(amount), 0)").
		Scan(&total).Error; err != nil {
		return 0, fmt.Errorf("sum account withdrawals: %w", err)
	}
	return total, nil
}

func sumMIRByPot(
	db *gorm.DB,
	pot uint,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	var total uint64
	if err := db.Model(&models.MoveInstantaneousRewardsReward{}).
		Joins(
			"INNER JOIN move_instantaneous_rewards mir"+
				" ON mir.id = move_instantaneous_rewards_reward.mir_id",
		).
		Where(
			"mir.pot = ?"+
				" AND move_instantaneous_rewards_reward.credential_tag = ?"+
				" AND move_instantaneous_rewards_reward.credential = ?",
			pot,
			credentialTag,
			stakingKey,
		).
		Select(
			"COALESCE(SUM(move_instantaneous_rewards_reward.amount), 0)",
		).
		Scan(&total).Error; err != nil {
		return 0, fmt.Errorf("sum account MIR for pot %d: %w", pot, err)
	}
	return total, nil
}
