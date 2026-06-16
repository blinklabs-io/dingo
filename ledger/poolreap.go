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
	"encoding/hex"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/governance"
)

// applyPoolRetirements applies the Shelley-era POOLREAP transition at an epoch
// boundary, embedded in the Conway EPOCH rule before governance enactment and
// treasury accounting (cardano-ledger Conway Epoch.hs). A pool retirement
// certificate names the epoch at which the pool retires; its deposit is not
// refunded in the submitting transaction but at the boundary into that epoch.
//
// For each pool whose effective retirement epoch is newEpoch, the deposit
// recorded with its active registration is refunded to the pool's reward
// account when that account is registered and active; otherwise the deposit is
// added to the treasury. This reuses the governance refund helpers so deposit
// returns follow identical registered-vs-unclaimed accounting.
//
// The refunded amount and reward account come from the pool's latest
// registration. This equals the originally-held deposit whenever the
// poolDeposit parameter is stable across the pool's lifetime, which is the case
// on all live networks; a governance change to poolDeposit between a pool's
// first and last registration is the only scenario where the two could differ.
//
// Removal from active pool state is not a separate write: dingo derives the
// active pool set from the latest registration/retirement certificates
// (GetActivePoolKeyHashesAtSlot excludes pools once retirement.epoch <=
// epochAtSlot), so once the boundary into newEpoch is crossed the pool is no
// longer active. Keeping the certificate rows — rather than deleting pool
// state — is what makes the transition rollback-safe: the reward-account
// credits (AccountRewardDelta journal) and treasury writes (NetworkState) are
// slot-keyed and reverted by the normal rollback path, after which re-applying
// the boundary re-derives the same refunds.
func (ls *LedgerState) applyPoolRetirements(
	txn *database.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) error {
	refunds, err := ls.db.GetPoolsRetiringAtEpoch(
		newEpoch, boundarySlot, txn,
	)
	if err != nil {
		return fmt.Errorf(
			"get pools retiring at epoch %d: %w", newEpoch, err,
		)
	}
	if len(refunds) == 0 {
		return nil
	}
	for _, refund := range refunds {
		deposit := uint64(refund.DepositAmount)
		// The reward account on a pool registration is the 28-byte stake
		// credential hash, the same form AddAccountReward looks up.
		credited, err := governance.CreditRegisteredRewardAccount(
			ls.db,
			txn,
			refund.RewardAccount,
			deposit,
			boundarySlot,
		)
		if err != nil {
			return fmt.Errorf(
				"refund pool %x deposit: %w",
				refund.PoolKeyHash, err,
			)
		}
		if !credited {
			if err := governance.AddUnclaimedToTreasury(
				ls.db,
				txn,
				deposit,
				boundarySlot,
			); err != nil {
				return fmt.Errorf(
					"return unclaimed pool %x deposit to treasury: %w",
					refund.PoolKeyHash, err,
				)
			}
		}
		ls.config.Logger.Debug(
			"applied pool retirement deposit refund",
			"pool", hex.EncodeToString(refund.PoolKeyHash),
			"epoch", newEpoch,
			"deposit", deposit,
			"credited_reward_account", credited,
			"component", "ledger",
		)
	}
	return nil
}
