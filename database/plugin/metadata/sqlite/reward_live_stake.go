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

package sqlite

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/rewardstate"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
)

// rewardCredentialSlotRef pairs a stake credential with the slot at which
// its reward-live-stake aggregate should be recomputed. This type (and the
// three functions immediately below) is kept local rather than moved to the
// shared internal/rewardstate package: account.go, batch_accumulator.go,
// transaction.go, and utxo.go destructure map values of this exact type via
// its unexported ref/slot fields, and the merge logic has no dialect
// variance to consolidate. See internal/rewardstate.CredentialSlotRef for
// the equivalent shared type used by the query helpers below that do carry
// real (dialect-bearing) logic.
type rewardCredentialSlotRef struct {
	ref  models.StakeCredentialRef
	slot uint64
}

func rewardStakeRefsFromUtxos(
	utxos []models.Utxo,
) map[string]rewardCredentialSlotRef {
	refs := make(map[string]rewardCredentialSlotRef)
	addRewardStakeRefsFromUtxos(refs, utxos)
	return refs
}

func addRewardStakeRefsFromUtxos(
	refs map[string]rewardCredentialSlotRef,
	utxos []models.Utxo,
) {
	for i := range utxos {
		u := &utxos[i]
		addRewardStakeRef(
			refs,
			models.NewStakeCredentialRef(u.CredentialTag, u.StakingKey),
			u.AddedSlot,
		)
	}
}

func addRewardStakeRef(
	refs map[string]rewardCredentialSlotRef,
	ref models.StakeCredentialRef,
	slot uint64,
) {
	if len(ref.Key) == 0 {
		return
	}
	key := ref.MapKey()
	if existing, ok := refs[key]; ok {
		if slot > existing.slot {
			existing.slot = slot
			refs[key] = existing
		}
		return
	}
	refs[key] = rewardCredentialSlotRef{ref: ref, slot: slot}
}

// pinRewardStakeRefsToSlot rewrites every ref's recompute slot to slot. The
// rollback paths (DeleteUtxosAfterSlot / SetUtxosNotDeletedAfterSlot) use this
// to pin the live-stake recompute to the rollback boundary rather than the
// affected UTxOs' own slots.
func pinRewardStakeRefsToSlot(
	refs map[string]rewardCredentialSlotRef,
	slot uint64,
) {
	for key, item := range refs {
		item.slot = slot
		refs[key] = item
	}
}

// toLocalRefs converts the shared package's credential/slot map (returned by
// the DB-query helpers, which have no reason to know this package's local
// rewardCredentialSlotRef type) into this package's local map type.
func toLocalRefs(
	shared map[string]rewardstate.CredentialSlotRef,
) map[string]rewardCredentialSlotRef {
	refs := make(map[string]rewardCredentialSlotRef, len(shared))
	for k, v := range shared {
		refs[k] = rewardCredentialSlotRef{ref: v.Ref, slot: v.Slot}
	}
	return refs
}

func rewardStakeRefsFromUtxoIDs(
	db *gorm.DB,
	utxos []models.UtxoId,
	slot uint64,
) (map[string]rewardCredentialSlotRef, error) {
	shared, err := rewardstate.StakeRefsFromUtxoIDs(
		db,
		utxos,
		slot,
		batchChunkSize,
	)
	if err != nil {
		return nil, err
	}
	return toLocalRefs(shared), nil
}

func rewardStakeRefsFromUtxoSpends(
	db *gorm.DB,
	spends []utxoSpend,
) (map[string]rewardCredentialSlotRef, error) {
	sharedSpends := make([]rewardstate.UtxoSpendRef, len(spends))
	for i, s := range spends {
		sharedSpends[i] = rewardstate.UtxoSpendRef{
			TxId:      s.TxId,
			OutputIdx: s.OutputIdx,
			Slot:      s.Slot,
		}
	}
	shared, err := rewardstate.StakeRefsFromUtxoSpends(
		db,
		sharedSpends,
		batchChunkSize,
	)
	if err != nil {
		return nil, err
	}
	return toLocalRefs(shared), nil
}

// flattenPoolDelegationCache converts this package's local certificate cache
// (populated by batchFetchCerts in account.go) into the shared package's
// PoolDelegationCache. account.go's accountCertCache/certRecord types carry
// registration/deregistration/DRep-delegation data used elsewhere in that
// file; only the pool-delegation slice is relevant to the reward-live-stake
// refresh, so only that slice is copied out.
func flattenPoolDelegationCache(
	cache *accountCertCache,
) rewardstate.PoolDelegationCache {
	if cache == nil {
		return nil
	}
	flat := make(rewardstate.PoolDelegationCache, len(cache.poolDelegation))
	for key, rec := range cache.poolDelegation {
		flat[key] = rewardstate.PoolDelegationRecord{
			Pool:       rec.pool,
			AddedSlot:  rec.addedSlot,
			BlockIndex: rec.blockIndex,
			CertIndex:  rec.certIndex,
		}
	}
	return flat
}

// refreshRewardLiveStakeAggregates batch-fetches certificate state once per
// distinct slot (via batchFetchCerts, which is local to this package because
// it shares account.go's certificate-batching machinery) and then refreshes
// each credential's reward_live_stake row through the shared package's
// per-credential engine.
func refreshRewardLiveStakeAggregates(
	db *gorm.DB,
	refs map[string]rewardCredentialSlotRef,
) error {
	sharedRefs := make(map[string]rewardstate.CredentialSlotRef, len(refs))
	for k, v := range refs {
		sharedRefs[k] = rewardstate.CredentialSlotRef{Ref: v.ref, Slot: v.slot}
	}
	refsBySlot, err := rewardstate.GroupRefsBySlot(sharedRefs)
	if err != nil {
		return err
	}
	// Flatten each slot's certificate cache exactly once here, not once per
	// credential in the loop below: a batch can hold many refs sharing one
	// slot, and flattening per-credential made refresh work O(refs*cacheSize).
	// The flattened map is read-only downstream, so sharing one instance
	// across all refs at a slot is safe.
	cachesBySlot := make(
		map[uint64]rewardstate.PoolDelegationCache,
		len(refsBySlot),
	)
	for slot, slotRefs := range refsBySlot {
		cache, err := batchFetchCerts(db, slotRefs, slot)
		if err != nil {
			return fmt.Errorf(
				"query reward live stake pool delegations: %w",
				err,
			)
		}
		cachesBySlot[slot] = flattenPoolDelegationCache(cache)
	}
	for _, item := range refs {
		if err := rewardstate.RefreshLiveStakeAggregate(
			db,
			item.ref,
			item.slot,
			cachesBySlot[item.slot],
		); err != nil {
			return err
		}
	}
	return nil
}

func refreshRewardLiveStakeAggregate(
	db *gorm.DB,
	ref models.StakeCredentialRef,
	slot uint64,
) error {
	if err := rewardstate.ValidateLiveStakeRef(ref); err != nil {
		return err
	}
	if len(ref.Key) == 0 {
		return nil
	}
	cache, err := batchFetchCerts(db, []models.StakeCredentialRef{ref}, slot)
	if err != nil {
		return fmt.Errorf(
			"query reward live stake pool delegation: %w",
			err,
		)
	}
	return rewardstate.RefreshLiveStakeAggregate(
		db,
		ref,
		slot,
		flattenPoolDelegationCache(cache),
	)
}

// creditRewardLiveStakeDelta adds amount to an existing reward_live_stake
// row's reward_stake and total_stake without the full account SELECT + UTxO
// SUM + upsert that refreshRewardLiveStakeAggregate performs. See
// rewardstate.CreditLiveStakeDelta for the shared implementation and its
// safety invariant.
func creditRewardLiveStakeDelta(
	db *gorm.DB,
	ref models.StakeCredentialRef,
	amount uint64,
	slot uint64,
) (bool, error) {
	return rewardstate.CreditLiveStakeDelta(db, ref, amount, slot)
}

// RebuildRewardLiveStake fully repopulates the reward_live_stake table from
// current account/UTxO/certificate state. See rewardstate.RebuildLiveStake
// for the shared SQL.
func (d *MetadataStoreSqlite) RebuildRewardLiveStake(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("rebuild reward live stake: resolve db: %w", err)
	}
	rebuild := func(tx *gorm.DB) error {
		return rewardstate.RebuildLiveStake(
			tx,
			slot,
			"INDEXED BY "+utxoStakingLiveAmountIndex,
		)
	}
	if txn != nil {
		return rebuild(db)
	}
	return db.Transaction(rebuild)
}

// RewardLiveStakeNeedsBackfill reports whether the reward_live_stake
// aggregate needs a one-time RebuildRewardLiveStake pass: true when the
// account table already has rows (this database has synced past genesis)
// but the reward_live_stake aggregate is completely empty. This detects an
// upgraded database that predates the aggregate and never received the
// backfill (see #1959) without misfiring on a legitimately fresh, empty
// database, where both tables are empty and there is nothing to rebuild yet.
func (d *MetadataStoreSqlite) RewardLiveStakeNeedsBackfill(
	txn types.Txn,
) (bool, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return false, fmt.Errorf(
			"reward live stake needs backfill: resolve db: %w",
			err,
		)
	}
	return rewardstate.LiveStakeNeedsBackfill(db)
}
