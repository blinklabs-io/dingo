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
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
)

// ActiveProposalDepositDRepPower sums every active governance proposal's own
// deposit into its return account's delegated DRep voting power. Per
// CIP-1694, a proposal's deposit is escrowed but still counts as part of the
// depositor's active voting stake for as long as the proposal remains
// active, so it must be added on top of the depositor's ordinary UTxO and
// reward-account stake -- exactly what VotingPowerBatchSQL/
// VotingPowerByTypeSQL compute without it (blinklabs-io/dingo#4355). The
// conformance harness (internal/test/conformance/state_manager.go,
// activeProposalDeposits/credentialVotingStake) implements the same rule
// locally for NoConfidence/UpdateCommittee ratification; this is the
// production counterpart that LoadDRepVotingState folds into every
// DRep-gated action's tally.
//
// The return account's own active/CIP-0163 expiry gates are applied the
// same way VotingPowerBatchSQL applies them to ordinary stake, so a return
// account already excluded from the ordinary tally by those gates does not
// have its deposit counted either. AlwaysAbstain delegators are excluded
// entirely, mirroring tallyDRepVotes' treatment of Abstain stake as outside
// every DRep's power (and every other bucket).
func ActiveProposalDepositDRepPower(
	db *database.Database,
	txn *database.Txn,
	currentEpoch uint64,
	expiryEpoch uint64,
) (map[string]uint64, uint64, error) {
	proposals, err := db.GetActiveGovernanceProposals(currentEpoch, txn)
	if err != nil {
		return nil, 0, fmt.Errorf("get active governance proposals: %w", err)
	}

	deposits := make(map[string]uint64)
	refs := make([]models.StakeCredentialRef, 0, len(proposals))
	for _, proposal := range proposals {
		if proposal == nil || proposal.Deposit == 0 {
			continue
		}
		credentialTag, stakeHash, err := rewardAccountStakeCredential(
			proposal.ReturnAddress,
		)
		if err != nil {
			// A malformed return address already fails this proposal's own
			// ratification precondition (ratificationEnactmentPrecondition);
			// skip it here rather than failing every proposal's DRep tally
			// for this epoch tick over one proposal's bad data.
			continue
		}
		ref := models.NewStakeCredentialRef(credentialTag, stakeHash)
		key := ref.MapKey()
		if _, seen := deposits[key]; !seen {
			refs = append(refs, ref)
		}
		deposits[key], err = addUint64(deposits[key], proposal.Deposit)
		if err != nil {
			return nil, 0, fmt.Errorf("sum active proposal deposits: %w", err)
		}
	}
	if len(deposits) == 0 {
		return nil, 0, nil
	}

	// includeInactive=false mirrors the outer `a.active = true` conjunct in
	// VotingPowerBatchSQL: a deregistered return account contributes no
	// voting power, deposit included.
	accounts, err := db.GetAccountsByCredential(refs, false, txn)
	if err != nil {
		return nil, 0, fmt.Errorf("get proposal return accounts: %w", err)
	}

	drepPower := make(map[string]uint64, len(accounts))
	var noConfidencePower uint64
	for key, amount := range deposits {
		account, ok := accounts[key]
		if !ok {
			continue
		}
		// Mirrors expiryClauses' `expiration_epoch = 0 OR
		// expiration_epoch >= expiryEpoch`: expiryEpoch == 0 means the
		// CIP-0163 gate is off (delegatorInactivityOn false), so every
		// account counts regardless of ExpirationEpoch.
		if expiryEpoch > 0 && account.ExpirationEpoch != 0 &&
			account.ExpirationEpoch < expiryEpoch {
			continue
		}
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
