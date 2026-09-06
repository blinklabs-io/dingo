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

package sqlstore

import (
	"context"
	"fmt"
)

// auth_committee_hot records one row per AuthCommitteeHot certificate and
// never overwrites, so a committee member that re-authorizes a hot key on a
// schedule adds rows forever. On preprod at slot ~79.48M the table held
// 648,758 rows for 35 distinct cold credentials. Only the newest
// authorization per cold credential is ever read back
// (GetActiveCommitteeMembers and GetCommitteeMember both select the maximum
// by (added_slot, certificate_id)), so every older row is dead weight -- with
// one exception, which is what the retention window below exists for.
//
// Retention rule, applied per (cold_credential_tag, cold_credential):
//
//	keep every row with added_slot > horizon,
//	plus the single newest row with added_slot <= horizon,
//	where horizon = tipSlot - retentionSlots.
//
// Rollback safety. A chain rollback deletes committee certificate rows with
// "DELETE FROM auth_committee_hot WHERE added_slot > S" (see
// DeleteCertificatesAfterSlot), so after a rollback to S the newest surviving
// row is the answer the readers need. Ouroboros bounds S from below: a
// rollback cannot cross the immutable tip, so S >= tipSlot - stabilityWindow
// for every reachable rollback target. Choosing retentionSlots >= the
// stability window therefore gives horizon <= S always, and:
//
//   - if any row exists in (horizon, S], it is retained (everything above the
//     horizon is retained) and it dominates every row at or below the
//     horizon, so the reader's answer is unchanged;
//   - if no row exists in (horizon, S], the answer is the newest row at or
//     below the horizon, which is exactly the one row the rule retains.
//
// So the post-rollback query result is identical whether or not pruning ran.
// The rule also never removes a credential's last row, so a credential that
// has an authorization can never be turned into one that has none.
//
// The partition is the tagged credential, matching the readers' PARTITION BY
// and the fact that a key-hash and a script-hash credential sharing 28 bytes
// are different identities. A script-hash row can never prune a key-hash row.
//
// Only auth_committee_hot is pruned. committee_member (the seated-committee
// table that CommitteeStateAvailable reads include-deleted, to tell an
// authoritatively empty committee from an unpopulated one) is untouched.
const (
	// DefaultCommitteeAuthRetentionSlots is the rollback window pruning keeps
	// history for, in slots. 129600 = 3k/f for k=2160, f=0.05: the Shelley-era
	// stability window on mainnet and preprod, and the same bound
	// internal/historyexpiry already uses to decide that block history is
	// immutable enough to expire locally. Networks with a smaller k (preview,
	// devnets) have a smaller true window, so this over-retains there, which
	// is the safe direction. Conway is the only era that has committee
	// certificates at all, so the smaller Byron 2k window never applies.
	DefaultCommitteeAuthRetentionSlots uint64 = 129600

	// committeeAuthPruneBatch bounds how many rows one prune call deletes.
	// Pruning runs inside the block-application transaction that writes a new
	// authorization, so it must not turn a single block into a 648k-row
	// delete. Growth is one row per certificate, so a per-certificate budget
	// well above one drains an existing backlog while keeping any single
	// block's extra work bounded.
	committeeAuthPruneBatch = 512
)

// committeeAuthRetentionSlots returns the configured rollback window, falling
// back to the default. Zero means "unset", not "disabled", so a Store built
// without the field still prunes safely rather than silently growing.
func (s *Store) committeeAuthRetention() uint64 {
	if s.committeeAuthRetentionSlots == 0 {
		return DefaultCommitteeAuthRetentionSlots
	}
	return s.committeeAuthRetentionSlots
}

// pruneCommitteeHotAuthorizations deletes superseded auth_committee_hot rows
// for one cold credential, up to committeeAuthPruneBatch rows per call. It is
// called from the certificate write path right after a new authorization row
// is inserted, so the work is proportional to the growth that caused it.
//
// tipSlot is the slot of the block being applied, which is the node's tip:
// blocks are applied in order, so a lower value only lowers the horizon,
// which is the conservative direction.
func (s *Store) pruneCommitteeHotAuthorizations(
	ctx context.Context,
	db queryer,
	coldCredentialTag uint8,
	coldCredential []byte,
	tipSlot uint64,
) (int64, error) {
	retention := s.committeeAuthRetention()
	if tipSlot <= retention {
		// The whole chain so far is inside the rollback window.
		return 0, nil
	}
	horizon := tipSlot - retention
	// The inner ORDER BY ... LIMIT ? OFFSET 1 is the rule: skip the single
	// newest row at or below the horizon, take up to a batch of the rest. The
	// extra SELECT wrapper materializes a derived table, which MySQL requires
	// both to reference the table being deleted from and to allow LIMIT
	// inside an IN subquery.
	result, err := db.ExecContext(ctx, `
DELETE FROM auth_committee_hot
WHERE id IN (
    SELECT id FROM (
        SELECT id
        FROM auth_committee_hot
        WHERE cold_credential_tag = ?
          AND cold_credential = ?
          AND added_slot <= ?
        ORDER BY added_slot DESC, certificate_id DESC
        LIMIT ? OFFSET 1
    ) superseded
)`,
		coldCredentialTag,
		coldCredential,
		horizon,
		committeeAuthPruneBatch,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"prune superseded committee hot authorizations: %w",
			err,
		)
	}
	pruned, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf(
			"prune superseded committee hot authorizations: row count: %w",
			err,
		)
	}
	return pruned, nil
}
