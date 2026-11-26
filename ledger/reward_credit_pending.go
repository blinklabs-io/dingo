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
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// pendingRewardCreditOutputsFrom returns one credential's unfolded credits in
// the credited rounds, read from ldb.
func pendingRewardCreditOutputsFrom(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	credentialTag uint8,
	stakingKey []byte,
) ([]models.AccountRewardCredit, error) {
	outputs, err := ldb.GetPendingRewardAccountOutputsForCredential(
		credentialTag, stakingKey, txn,
	)
	if err != nil {
		return nil, err
	}
	var credits []models.AccountRewardCredit
	for _, output := range outputs {
		roundCredits, err := epochengine.RewardCreditsForOutputs(
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
	txn ledgerdb.Txn,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	return pendingRewardCreditFrom(
		ls.ledgerDB(), txn, credentialTag, stakingKey,
	)
}

// PendingRewardCreditIn is PendingRewardCredit over db, the database whose
// rows and transaction the caller reads: the ledger's own, or a split
// IndexerDB holding the copied reward outputs.
func PendingRewardCreditIn(
	db *database.Database,
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	return pendingRewardCreditFrom(
		dbadapter.New(db), dbadapter.Txn(txn), credentialTag, stakingKey,
	)
}

func pendingRewardCreditFrom(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	credits, err := pendingRewardCreditOutputsFrom(
		ldb,
		txn,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, credit := range credits {
		sum, overflow := epochengine.AddRewardUint64(total, credit.Amount)
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
	txn ledgerdb.Txn,
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
	txn ledgerdb.Txn,
	credentialTag uint8,
	stakingKey []byte,
) error {
	outputs, err := ls.ledgerDB().ClaimPendingRewardCreditsForCredential(
		credentialTag, stakingKey, txn,
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
	txn ledgerdb.Txn,
	outputs []*models.RewardAccountOutput,
) error {
	var credits []models.AccountRewardCredit
	for _, output := range outputs {
		roundCredits, err := epochengine.RewardCreditsForOutputs(
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
	return ls.ledgerDB().AddAccountRewardsByCredential(ctx, credits, txn)
}

// foldRewardCreditsForWithdrawals writes the pending reward credits of every
// credential tx withdraws from, before the withdrawal reads its balance.
func (ls *LedgerState) foldRewardCreditsForWithdrawals(
	ctx context.Context,
	tx lcommon.Transaction,
	txn ledgerdb.Txn,
) error {
	withdrawals := tx.Withdrawals()
	if len(withdrawals) == 0 {
		return nil
	}
	hasPending, err := ls.ledgerDB().HasPendingRewardCreditRounds(txn)
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
