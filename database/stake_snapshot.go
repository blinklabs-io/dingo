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
//  2. Account rows (active and inactive) yield the DRep delegation type
//     for each credential.
//
// RewardAccountAutoVoteResolved is set true only when the outcome is
// determined from real data, at exactly three terminal conditions:
//   - The pool row exists and carries no reward account → confirmed None.
//   - The pool row exists, its reward-account row is present and ACTIVE →
//     classified by DRep delegation (Abstain / NoConfidence / None).
//   - The pool row exists, its reward-account row is present but INACTIVE
//     (deregistered) → confirmed None, since CIP-1694 treats unregistered
//     reward accounts as implicit no.
//
// Resolved is left false (so the tally falls back to implicit no without
// freezing a value) when:
//   - The pool row is absent from the DB (cannot determine the reward
//     account at all), or
//   - The pool's reward-account credential has no row in the account table
//     at all. An absent row is ambiguous: it may mean the account was never
//     registered, OR that account data has not yet been imported (e.g. a
//     Mithril restore that imported pools from the snapshot fallback before
//     cert-state accounts were loaded). Persisting Resolved=true here would
//     conflate "account input unavailable" with "confirmed none".
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
		s.RewardAccountAutoVote = models.PoolRewardAccountAutoVoteNone
		s.RewardAccountAutoVoteResolved = false
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
		poolKey := string(pools[i].PoolKeyHash)
		if _, dup := rewardAcctByPool[poolKey]; dup {
			continue
		}
		ra := pools[i].RewardAccount
		if len(ra) == 0 {
			// Pool row exists but carries no reward account: this is a
			// confirmed None outcome — mark resolved immediately.
			for _, s := range snapshotsByPool[poolKey] {
				s.RewardAccountAutoVoteResolved = true
			}
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

	// includeInactive=true so a present-but-deregistered reward account
	// is distinguishable from an account row that is entirely absent.
	// Deregistered accounts are a confirmed None; absent rows stay
	// unresolved (see the doc comment).
	accounts, err := d.GetAccountsByCredential(
		rewardAccountRefs,
		true,
		txn,
	)
	if err != nil {
		return fmt.Errorf("get reward accounts: %w", err)
	}

	for poolKey, ref := range rewardAcctByPool {
		acct, ok := accounts[ref.MapKey()]
		if !ok {
			// Account row not in DB: data may not have been imported yet.
			// Leave Resolved=false rather than persisting a false None.
			continue
		}
		// Account row exists. A deregistered (inactive) account does not
		// auto-vote per CIP-1694; an active account auto-votes only when
		// delegated to AlwaysAbstain or AlwaysNoConfidence.
		var autoVote uint8
		if acct.Active {
			switch acct.DrepType {
			case models.DrepTypeAlwaysAbstain:
				autoVote = models.PoolRewardAccountAutoVoteAbstain
			case models.DrepTypeAlwaysNoConfidence:
				autoVote = models.PoolRewardAccountAutoVoteNoConfidence
			}
		}
		for _, s := range snapshotsByPool[poolKey] {
			s.RewardAccountAutoVoteResolved = true
			s.RewardAccountAutoVote = autoVote
		}
	}
	return nil
}
