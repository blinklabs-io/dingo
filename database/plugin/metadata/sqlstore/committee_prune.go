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
	"time"
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
//	where horizon = min(tipSlot - retentionSlots, liveImmutableSlot),
//	and pruning does not run at all while a live syncer is wired but has
//	no current value (see "Suspension" below).
//
// Rollback safety. A chain rollback deletes committee certificate rows with
// "DELETE FROM auth_committee_hot WHERE added_slot > S" (see
// DeleteCertificatesAfterSlot), so after a rollback to S the newest surviving
// row is the answer the readers need. The node's own rollback check
// (Chain.rollbackForkDepth vs. securityParam) bounds S from below in BLOCKS,
// not slots: S is the slot of the block securityParam blocks behind the tip,
// whatever that slot is. tipSlot - retentionSlots only approximates that
// bound by assuming mainnet/preprod-typical block density (retentionSlots is
// 3k/f); a sparse chain -- fewer blocks per slot than that assumption, which
// low-activeSlotCoefficient devnets hit routinely and which nothing stops a
// sparse span of the honest chain from doing occasionally -- can have a
// legal S below tipSlot - retentionSlots, and pruning to the wrong horizon
// permanently deletes the row a legal rollback needs (issue #4353).
//
// liveImmutableSlot is that actual bound when it is fresh: the slot
// securityParam blocks behind the tip, as observed on the live chain (see
// SetCommitteeAuthImmutableSlot), refreshed periodically from outside this
// package because sqlstore cannot import chain to compute it directly. A
// fresh liveImmutableSlot is always <= S: the node's own rollback check
// forbids any legal target shallower than securityParam blocks from the
// tip it was resolved against, and every block after that tip -- on any
// chain the node subsequently adopts -- has a slot no earlier than that
// tip's own, so the bound only moves forward as long as it is refreshed
// after every tip change (see "Suspension" for why a stale one is not
// trustworthy). Nothing is claimed about tipSlot - retentionSlots relative
// to S beyond the assumption above; the minimum is safe because
// liveImmutableSlot alone is, not because both candidates are.
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
// Suspension. A cached liveImmutableSlot can be safe at the moment it is
// resolved and still be unsafe by the time it is used, because "at most
// securityParam blocks behind the tip" is a statement about one instant,
// not a ratchet: a rollback to depth d1 <= securityParam, a little regrowth,
// and a second rollback to depth d2 <= securityParam measured from the new,
// shorter tip can legally reach a point *before* a liveImmutableSlot cached
// from before the first rollback (small oscillating reorgs near the tip are
// ordinary, not adversarial). DeleteCertificatesAfterSlot therefore
// invalidates the cached value on every rollback, and committeeAuthHorizon
// treats "a live syncer is wired (SetCommitteeAuthImmutableSlot has been
// called at least once) but currently has no value" -- bootstrap before the
// first resolution, a resolution failure, or a post-rollback invalidation
// awaiting the next sync -- as a reason to skip pruning entirely rather than
// fall back to the slot-window assumption, which the whole point of this
// mechanism is to not rely on alone. A Store nothing has ever pushed a live
// value to is a different state: no live syncer is wired at all (every test
// in this file, and any non-node caller such as a backfill or inspection
// tool), and that keeps the pre-live-sync slot-window-only behavior
// unchanged, exactly as it shipped before this mechanism existed.
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

	// committeeAuthMaintenanceInterval is deliberately long enough that the
	// background sweep cannot compete with block application, while ensuring a
	// credential that never re-authorizes still eventually drains.
	committeeAuthMaintenanceInterval = 24 * time.Hour
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

// SetCommitteeAuthImmutableSlot records the live rollback-safe immutable
// slot -- the slot of the block securityParam blocks behind the current tip,
// as resolved by Chain.PointAtDepth -- for committeeAuthHorizon to fold into
// the retention decision. known false means no current value is available
// (the depth lookup failed, the security parameter is not yet known, or the
// chain does not yet have securityParam blocks) and pruning suspends rather
// than fall back to the slot-window assumption; callers must pass
// known=false rather than a stale slot in that case, since a wrong slot is
// not distinguishable from a valid one once stored. Every call, known or
// not, marks a live syncer as wired for this Store -- see the package-level
// comment's "Suspension" section for why that first call changes
// committeeAuthHorizon's behavior even before any value is ever known.
//
// The caller -- a periodic sync outside this package, since sqlstore cannot
// import chain -- may call this from a goroutine independent of any
// certificate write or the maintenance sweep, so this is lock-free and never
// blocks.
func (s *Store) SetCommitteeAuthImmutableSlot(slot uint64, known bool) {
	s.committeeAuthImmutableSlotEverSet.Store(true)
	if !known {
		s.committeeAuthImmutableSlotKnown.Store(false)
		return
	}
	s.committeeAuthImmutableSlot.Store(slot)
	s.committeeAuthImmutableSlotKnown.Store(true)
}

// committeeAuthHorizon returns the retention horizon for a prune call at
// tipSlot, and whether pruning should run at all. See the package-level
// comment above for the retention rule and why pruning suspends rather than
// use the slot-window assumption once a live syncer is wired but currently
// has no value.
func (s *Store) committeeAuthHorizon(tipSlot uint64) (uint64, bool) {
	if s.committeeAuthImmutableSlotEverSet.Load() &&
		!s.committeeAuthImmutableSlotKnown.Load() {
		return 0, false
	}
	retention := s.committeeAuthRetention()
	if tipSlot <= retention {
		// The whole chain so far is inside the rollback window.
		return 0, false
	}
	horizon := tipSlot - retention
	if s.committeeAuthImmutableSlotKnown.Load() {
		if live := s.committeeAuthImmutableSlot.Load(); live < horizon {
			horizon = live
		}
	}
	return horizon, true
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
	horizon, ok := s.committeeAuthHorizon(tipSlot)
	if !ok {
		return 0, nil
	}
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

// pruneCommitteeHotAuthorizationsMaintenance removes superseded
// authorizations across all credentials. The certificate write path can only
// visit credentials that re-authorize; this periodic sweep is the liveness
// path for inactive credentials. Each delete is bounded, while one maintenance
// cycle repeats those deletes until the table has no more eligible rows.
func (s *Store) pruneCommitteeHotAuthorizationsMaintenance(
	ctx context.Context,
) error {
	tip, err := s.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read tip for committee hot maintenance: %w", err)
	}
	horizon, ok := s.committeeAuthHorizon(tip.Point.Slot)
	if !ok {
		return nil
	}
	db := s.instrumentedQueryer(s.writeDB)
	for {
		result, err := db.ExecContext(ctx, `
DELETE FROM auth_committee_hot
WHERE id IN (
    SELECT id FROM (
        SELECT old.id
        FROM auth_committee_hot old
        WHERE old.added_slot <= ?
          AND EXISTS (
              SELECT 1
              FROM auth_committee_hot newer
              WHERE newer.cold_credential_tag = old.cold_credential_tag
                AND newer.cold_credential = old.cold_credential
                AND newer.added_slot <= ?
                AND (
                    newer.added_slot > old.added_slot
                    OR (
                        newer.added_slot = old.added_slot
                        AND newer.certificate_id > old.certificate_id
                    )
                    OR (
                        newer.added_slot = old.added_slot
                        AND newer.certificate_id = old.certificate_id
                        AND newer.id > old.id
                    )
                )
          )
        ORDER BY old.added_slot ASC, old.certificate_id ASC, old.id ASC
        LIMIT ?
    ) superseded
)`, horizon, horizon, committeeAuthPruneBatch)
		if err != nil {
			return fmt.Errorf(
				"prune superseded committee hot authorizations in maintenance: %w",
				err,
			)
		}
		pruned, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf(
				"prune superseded committee hot authorizations in maintenance: row count: %w",
				err,
			)
		}
		if pruned == 0 {
			return nil
		}
	}
}
