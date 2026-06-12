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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ResolvePoolRewardAccountAutoVotes classifies each PoolStakeSnapshot
// with the CIP-1694 reward-account DRep-delegation outcome and writes
// the result onto snapshot.RewardAccountAutoVote in place. Callers
// invoke this immediately before persisting the snapshots so the
// auto-vote signal is frozen with the snapshot rather than re-derived
// from live state at tally time.
//
// Resolution proceeds in two batched lookups:
//  1. Pool rows yield each pool's reward-account stake credential.
//  2. Account rows yield the DRep delegation type for each credential.
//
// Only ACTIVE accounts are considered. A deregistered reward account
// — even one whose row still carries an AlwaysAbstain or
// AlwaysNoConfidence delegation flag — yields PoolRewardAccountAutoVoteNone,
// since CIP-1694 treats unregistered reward accounts as implicit no.
func (d *Database) ResolvePoolRewardAccountAutoVotes(
	snapshots []*models.PoolStakeSnapshot,
	txn *Txn,
) error {
	if len(snapshots) == 0 {
		return nil
	}
	if txn == nil {
		txn = d.Transaction(false)
		defer txn.Release()
	}

	// Group snapshots per pool key so a single pool with multiple
	// snapshot rows (e.g. mark/set/go imported together) is resolved
	// once and then fanned out.
	snapshotsByPool := make(map[string][]*models.PoolStakeSnapshot, len(snapshots))
	pkhs := make([]lcommon.PoolKeyHash, 0, len(snapshots))
	for _, s := range snapshots {
		// Reset to ensure callers can re-run resolution without
		// stale values leaking through from a previous attempt.
		// Mark the row as resolved up-front: every snapshot passed
		// to this resolver runs against snapshot-era state by
		// contract, so the absence of an Always{Abstain,NoConfidence}
		// delegation is a real "none" answer, not "unknown".
		s.RewardAccountAutoVote = models.PoolRewardAccountAutoVoteNone
		s.RewardAccountAutoVoteResolved = true
		key := string(s.PoolKeyHash)
		if _, seen := snapshotsByPool[key]; !seen {
			pkhs = append(pkhs, lcommon.PoolKeyHash(s.PoolKeyHash))
		}
		snapshotsByPool[key] = append(snapshotsByPool[key], s)
	}

	pools, err := d.GetPools(pkhs, txn)
	if err != nil {
		return fmt.Errorf("get pools: %w", err)
	}

	rewardAcctByPool := make(map[string]models.StakeCredentialRef, len(pools))
	seenRefs := make(map[string]struct{}, len(pools))
	rewardAccountRefs := make([]models.StakeCredentialRef, 0, len(pools))
	for i := range pools {
		ra := pools[i].RewardAccount
		if len(ra) == 0 {
			continue
		}
		poolKey := string(pools[i].PoolKeyHash)
		if _, dup := rewardAcctByPool[poolKey]; dup {
			continue
		}
		ref := models.StakeCredentialRef{
			Tag: pools[i].RewardAccountCredentialTag,
			Key: ra,
		}
		rewardAcctByPool[poolKey] = ref
		mk := ref.MapKey()
		if _, seen := seenRefs[mk]; !seen {
			seenRefs[mk] = struct{}{}
			rewardAccountRefs = append(rewardAccountRefs, ref)
		}
	}
	if len(rewardAccountRefs) == 0 {
		return nil
	}

	// includeInactive=false so a deregistered reward account that
	// still carries a stale predefined-DRep flag does not auto-vote.
	accounts, err := d.GetAccountsByCredential(
		rewardAccountRefs,
		false,
		txn,
	)
	if err != nil {
		return fmt.Errorf("get reward accounts: %w", err)
	}

	for poolKey, ref := range rewardAcctByPool {
		acct, ok := accounts[ref.MapKey()]
		if !ok {
			continue
		}
		var autoVote uint8
		switch acct.DrepType {
		case models.DrepTypeAlwaysAbstain:
			autoVote = models.PoolRewardAccountAutoVoteAbstain
		case models.DrepTypeAlwaysNoConfidence:
			autoVote = models.PoolRewardAccountAutoVoteNoConfidence
		default:
			continue
		}
		for _, s := range snapshotsByPool[poolKey] {
			s.RewardAccountAutoVote = autoVote
		}
	}
	return nil
}
