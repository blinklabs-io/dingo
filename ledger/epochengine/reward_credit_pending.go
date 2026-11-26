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
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

func registerAppliedRewardCreditRound(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	round models.RewardCreditRound,
) error {
	return meta.AddAppliedRewardCreditRound(round, metaTxn)
}

// RewardCreditsForOutputs returns the credits a boundary writes for outputs:
// every spendable, unguarded output, journaled at the round's boundary slot.
func RewardCreditsForOutputs(
	round models.RewardCreditRound,
	outputs []*models.RewardAccountOutput,
) ([]models.AccountRewardCredit, error) {
	credits := make([]models.AccountRewardCredit, 0, len(outputs))
	for _, output := range outputs {
		if output == nil || !output.Spendable || output.Guarded {
			continue
		}
		reward, err := rewardFromAccountOutput(output)
		if err != nil {
			return nil, err
		}
		credits = append(credits, models.AccountRewardCredit{
			CredentialTag: reward.Credential.Tag,
			StakingKey:    reward.Credential.Hash[:],
			Amount:        reward.Amount,
			Slot:          round.BoundarySlot,
			SourceHash:    stakeRewardSourceHash(round.SnapshotEpoch, reward),
		})
	}
	return credits, nil
}

// RewardRoundCredited reports whether the round applied at the boundary into
// newEpoch is credited. Its outputs are then account balances, so nothing may
// recompute or replace them.
func (e *Engine) RewardRoundCredited(
	txn ledgerdb.Txn,
	newEpoch uint64,
) (bool, error) {
	epochs, ok := StakeRewardEpochsForApplication(newEpoch)
	if !ok {
		return false, nil
	}
	return e.ledgerDB().HasAppliedRewardCreditRound(
		epochs.Snapshot,
		txn,
	)
}
