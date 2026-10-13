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

package governance

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
)

type activeProposalDeposit struct {
	Credential models.StakeCredentialRef
	Amount     uint64
}

func activeProposalDepositsByReturnCredential(
	ctx context.Context,
	db *database.Database,
	txn *database.Txn,
	currentEpoch uint64,
) (map[string]activeProposalDeposit, error) {
	proposals, err := db.GetActiveGovernanceProposals(ctx, currentEpoch, txn)
	if err != nil {
		return nil, fmt.Errorf("get active governance proposals: %w", err)
	}

	deposits := make(map[string]activeProposalDeposit)
	for _, proposal := range proposals {
		if proposal == nil || proposal.Deposit == 0 {
			continue
		}
		credentialTag, stakeHash, err := rewardAccountStakeCredential(
			proposal.ReturnAddress,
		)
		if err != nil {
			// A malformed return address already fails this proposal's own
			// ratification precondition. It must not fail every tally in the
			// epoch over otherwise unusable deposit data.
			continue
		}
		ref := models.NewStakeCredentialRef(credentialTag, stakeHash)
		key := ref.MapKey()
		entry := deposits[key]
		entry.Credential = ref
		entry.Amount, err = addUint64(entry.Amount, proposal.Deposit)
		if err != nil {
			return nil, fmt.Errorf("sum active proposal deposits: %w", err)
		}
		deposits[key] = entry
	}
	return deposits, nil
}

func accountsForProposalDeposits(
	ctx context.Context,
	db *database.Database,
	txn *database.Txn,
	deposits map[string]activeProposalDeposit,
) (map[string]*models.Account, error) {
	refs := make([]models.StakeCredentialRef, 0, len(deposits))
	for _, deposit := range deposits {
		refs = append(refs, deposit.Credential)
	}
	// An inactive reward account has no DRep or SPO voting power, deposits
	// included, matching the account-state checks in Conway's pulser.
	accounts, err := db.GetAccountsByCredential(ctx, refs, false, txn)
	if err != nil {
		return nil, fmt.Errorf("get proposal return accounts: %w", err)
	}
	return accounts, nil
}

func proposalDepositAccountActive(
	account *models.Account,
	expiryEpoch uint64,
) bool {
	return account != nil && (expiryEpoch == 0 ||
		account.ExpirationEpoch == 0 || account.ExpirationEpoch >= expiryEpoch)
}

// ActiveProposalDepositDRepPower sums every active governance proposal's own
// deposit into its return account's delegated DRep voting power. Per
// CIP-1694, a proposal's deposit is escrowed but still counts as part of the
// depositor's active voting stake for as long as the proposal remains
// active, so it must be added on top of the depositor's ordinary UTxO and
// reward-account stake -- exactly what VotingPowerBatchSQL/
// VotingPowerByTypeSQL compute without it. The conformance harness
// (internal/test/conformance/state_manager.go,
// activeProposalDeposits/credentialVotingStake) implements the same rule
// locally for NoConfidence/UpdateCommittee ratification; this is the production
// counterpart that LoadDRepVotingState folds into every DRep-gated action's
// tally.
//
// The return account's own active/CIP-0163 expiry gates are applied the
// same way VotingPowerBatchSQL applies them to ordinary stake, so a return
// account already excluded from the ordinary tally by those gates does not
// have its deposit counted either. AlwaysAbstain delegators are excluded
// entirely, mirroring tallyDRepVotes' treatment of Abstain stake as outside
// every DRep's power (and every other bucket).
func ActiveProposalDepositDRepPower(
	ctx context.Context,
	db *database.Database,
	txn *database.Txn,
	currentEpoch uint64,
	expiryEpoch uint64,
) (map[string]uint64, uint64, error) {
	return activeProposalDepositDRepPowerAtEpochs(ctx,
		db, txn, currentEpoch, expiryEpoch,
	)
}

func activeProposalDepositDRepPowerAtEpochs(
	ctx context.Context,
	db *database.Database,
	txn *database.Txn,
	activeProposalEpoch uint64,
	expiryEpoch uint64,
) (map[string]uint64, uint64, error) {
	deposits, err := activeProposalDepositsByReturnCredential(ctx,
		db, txn, activeProposalEpoch,
	)
	if err != nil {
		return nil, 0, err
	}
	if len(deposits) == 0 {
		return nil, 0, nil
	}

	accounts, err := accountsForProposalDeposits(ctx, db, txn, deposits)
	if err != nil {
		return nil, 0, err
	}

	drepPower := make(map[string]uint64, len(accounts))
	var noConfidencePower uint64
	for key, deposit := range deposits {
		account, ok := accounts[key]
		if !ok {
			continue
		}
		// Mirrors expiryClauses' `expiration_epoch = 0 OR
		// expiration_epoch >= expiryEpoch`: expiryEpoch == 0 means the
		// CIP-0163 gate is off (delegatorInactivityOn false), so every
		// account counts regardless of ExpirationEpoch.
		if !proposalDepositAccountActive(account, expiryEpoch) {
			continue
		}
		amount := deposit.Amount
		switch account.DrepType {
		case models.DrepTypeAlwaysNoConfidence:
			noConfidencePower, err = addUint64(noConfidencePower, amount)
			if err != nil {
				return nil, 0, fmt.Errorf(
					"active proposal deposit no-confidence power: %w", err,
				)
			}
		case models.DrepTypeAddrKeyHash, models.DrepTypeScriptHash:
			if len(account.Drep) == 0 {
				// DrepType's zero value also means "no delegation set" when
				// Drep is nil; see models.Account.DrepType's doc comment.
				continue
			}
			drepRef := models.NewStakeCredentialRef(
				uint8(account.DrepType), //nolint:gosec // switch case bounds DrepType to {0,1}
				account.Drep,
			)
			mapKey := drepRef.MapKey()
			sum, err := addUint64(drepPower[mapKey], amount)
			if err != nil {
				return nil, 0, fmt.Errorf(
					"active proposal deposit drep power: %w", err,
				)
			}
			drepPower[mapKey] = sum
		}
	}
	return drepPower, noConfidencePower, nil
}
