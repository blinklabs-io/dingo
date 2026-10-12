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
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// A reward round a boundary applies from the per-pool precompute is recorded
// as credited (models.RewardCreditRound) instead of being written account by
// account. Its spendable, unguarded reward_account_output rows are the
// credits: a credential's balance is its account reward plus its unfolded
// outputs in credited rounds. The store adds those outputs wherever it reads
// balances in aggregate, pendingRewardCredit adds them for one credential,
// and foldRewardCreditFor writes them to the account, with the journal rows
// the boundary would have written, only where a stored balance must change.

func registerAppliedRewardCreditRound(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	round models.RewardCreditRound,
) error {
	return meta.AddAppliedRewardCreditRound(round, metaTxn)
}

// rewardCreditsForOutputs returns the credits a boundary writes for outputs:
// every spendable, unguarded output, journaled at the round's boundary slot.
func rewardCreditsForOutputs(
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

// rewardRoundCredited reports whether the round applied at the boundary into
// newEpoch is credited. Its outputs are then account balances, so nothing may
// recompute or replace them.
func (ls *LedgerState) rewardRoundCredited(
	txn *database.Txn,
	newEpoch uint64,
) (bool, error) {
	epochs, ok := stakeRewardEpochsForApplication(newEpoch)
	if !ok {
		return false, nil
	}
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	return ls.db.Metadata().HasAppliedRewardCreditRound(
		epochs.snapshot,
		metaTxn,
	)
}

// pendingRewardCreditOutputs returns one credential's unfolded credits in the
// credited rounds.
func (ls *LedgerState) pendingRewardCreditOutputs(
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) ([]models.AccountRewardCredit, error) {
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	meta := ls.db.Metadata()
	outputs, err := meta.GetPendingRewardAccountOutputsForCredential(
		credentialTag, stakingKey, metaTxn,
	)
	if err != nil {
		return nil, err
	}
	var credits []models.AccountRewardCredit
	for _, output := range outputs {
		roundCredits, err := rewardCreditsForOutputs(
			models.RewardCreditRound{
				SnapshotEpoch: output.Epoch,
				BoundarySlot:  output.BoundarySlot,
			},
			[]*models.RewardAccountOutput{output},
		)
		if err != nil {
			return nil, err
		}
		credits = append(credits, roundCredits...)
	}
	return credits, nil
}

// pendingRewardCredit is the part of a credential's reward balance a pending
// round has not written to its account yet.
func (ls *LedgerState) pendingRewardCredit(
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	credits, err := ls.pendingRewardCreditOutputs(
		txn,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, credit := range credits {
		sum, overflow := addRewardUint64(total, credit.Amount)
		if overflow {
			return 0, fmt.Errorf(
				"pending reward credit overflow for %x", stakingKey,
			)
		}
		total = sum
	}
	return total, nil
}

// PendingRewardCredit returns the part of a credential's reward balance that
// an applied reward round has not written to its account row yet. A reader of
// the account row's reward adds it, read in the same txn, to report the
// balance.
func (ls *LedgerState) PendingRewardCredit(
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	return ls.pendingRewardCredit(txn, credentialTag, stakingKey)
}

// foldRewardCreditFor writes one credential's unfolded credits to its account
// in txn, before a write that reads its stored balance, and marks them folded
// so no balance read counts them again.
func (ls *LedgerState) foldRewardCreditFor(
	ctx context.Context,
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) error {
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	outputs, err := ls.db.Metadata().ClaimPendingRewardCreditsForCredential(
		credentialTag, stakingKey, metaTxn,
	)
	if err != nil {
		return err
	}
	return ls.writeClaimedRewardCredits(ctx, txn, outputs)
}

// writeClaimedRewardCredits writes claimed credits to their accounts with the
// journal rows the boundary would have written: each at its round's boundary
// slot, under its source hash.
func (ls *LedgerState) writeClaimedRewardCredits(
	ctx context.Context,
	txn *database.Txn,
	outputs []*models.RewardAccountOutput,
) error {
	var credits []models.AccountRewardCredit
	for _, output := range outputs {
		roundCredits, err := rewardCreditsForOutputs(
			models.RewardCreditRound{
				SnapshotEpoch: output.Epoch,
				BoundarySlot:  output.BoundarySlot,
			},
			[]*models.RewardAccountOutput{output},
		)
		if err != nil {
			return err
		}
		credits = append(credits, roundCredits...)
	}
	if len(credits) == 0 {
		return nil
	}
	return ls.db.AddAccountRewardsByCredential(ctx, credits, txn)
}

// foldRewardCreditsForWithdrawals writes the pending reward credits of every
// credential tx withdraws from, before the withdrawal reads its balance.
func (ls *LedgerState) foldRewardCreditsForWithdrawals(
	ctx context.Context,
	tx lcommon.Transaction,
	txn *database.Txn,
) error {
	withdrawals := tx.Withdrawals()
	if len(withdrawals) == 0 {
		return nil
	}
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	hasPending, err := ls.db.Metadata().HasPendingRewardCreditRounds(metaTxn)
	if err != nil || !hasPending {
		return err
	}
	for address := range withdrawals {
		if address == nil {
			continue
		}
		tag, ok := models.StakeCredentialTagFromAddress(*address)
		if !ok {
			continue
		}
		stakeKey := address.StakeKeyHash()
		if err := ls.foldRewardCreditFor(ctx, txn, tag, stakeKey.Bytes()); err != nil {
			return fmt.Errorf(
				"credit pending rewards before withdrawal: %w",
				err,
			)
		}
	}
	return nil
}
