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

package ledgerstate

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// RewardInputRepairStore is the metadata boundary needed to reconcile one basis.
type RewardInputRepairStore interface {
	GetSyncState(string, types.Txn) (string, error)
	SetSyncState(string, string, types.Txn) error
	GetRewardSnapshot(uint64, string, types.Txn) (*models.RewardSnapshot, error)
	GetEpoch(uint64, types.Txn) (*models.Epoch, error)
	GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash,
		uint64,
		uint64,
		uint64,
		types.Txn,
	) ([]models.PoolRegistration, error)
	GetRewardPoolInputs(uint64, types.Txn) ([]*models.RewardPoolInput, error)
	GetRewardStakeInputs(uint64, types.Txn) ([]*models.RewardStakeInput, error)
	HasAppliedRewardCreditRound(uint64, types.Txn) (bool, error)
	DeleteRewardOutputsForEpoch(uint64, types.Txn) error
	DeleteSyncState(string, types.Txn) error
	SaveRewardStakeInputs([]*models.RewardStakeInput, types.Txn) error
}

// RewardInputRepairReport describes a canonical snapshot comparison before any write.
type RewardInputRepairReport struct {
	Epoch         uint64 `json:"epoch"`
	StoredRows    int    `json:"stored_rows"`
	CanonicalRows int    `json:"canonical_rows"`
	ChangedRows   int    `json:"changed_rows"`
}

// RepairRewardStakeInputs completes one authoritative basis from an imported
// snapshot. The caller must hold an offline database lock and own the transaction.
// Every retained pool total must reconcile before existing rows may be upserted.
func RepairRewardStakeInputs(
	store RewardInputRepairStore,
	txn types.Txn,
	state *RawLedgerState,
	epoch uint64,
	apply bool,
) (*RewardInputRepairReport, error) {
	if state == nil || state.Tip == nil || epoch > state.Epoch ||
		state.Epoch-epoch > 2 {
		return nil, errors.New("epoch outside snapshot window")
	}
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	if err != nil {
		return nil, err
	}
	selected := &snapshots.Mark
	if state.Epoch-epoch == 1 {
		selected = &snapshots.Set
	}
	if state.Epoch-epoch == 2 {
		selected = &snapshots.Go
	}
	retained, err := store.GetRewardSnapshot(epoch, "mark", txn)
	if err != nil {
		return nil, err
	}
	if retained == nil || !retained.Authoritative {
		return nil, errors.New("authoritative snapshot required")
	}
	params, err := authoritativeRewardInputPoolParams(
		store,
		txn,
		selected,
		retained,
	)
	if err != nil {
		return nil, err
	}
	bundle := deriveRewardInputs(
		selected,
		params,
		epoch,
		retained.CapturedSlot,
		retained.BoundarySlot,
	)
	if err := bundle.validate(); err != nil {
		return nil, err
	}
	return repairRewardStakeInputBundle(store, txn, bundle, retained, apply)
}

func repairRewardStakeInputBundle(
	store RewardInputRepairStore,
	txn types.Txn,
	bundle *rewardInputBundle,
	retained *models.RewardSnapshot,
	apply bool,
) (*RewardInputRepairReport, error) {
	if apply && txn == nil {
		return nil, errors.New("repair requires an explicit transaction")
	}
	epoch := bundle.epoch
	credited, err := store.HasAppliedRewardCreditRound(epoch, txn)
	if err != nil {
		return nil, err
	}
	if credited {
		return nil, errors.New("cannot repair credited reward epoch")
	}
	pending, err := loadImportedRewardStakeInputsPending(store, txn)
	if err != nil {
		return nil, err
	}
	if bundle.snapshot.TotalActiveStake != retained.TotalActiveStake ||
		bundle.snapshot.TotalDelegators != retained.TotalDelegators ||
		bundle.snapshot.TotalPoolCount != retained.TotalPoolCount {
		return nil, errors.New(
			"canonical snapshot totals differ from retained authoritative totals",
		)
	}
	pools, err := store.GetRewardPoolInputs(epoch, txn)
	if err != nil {
		return nil, err
	}
	if len(pools) != len(bundle.poolInputs) {
		return nil, errors.New("pool count mismatch")
	}
	canonicalPools := map[string]int{}
	for i, p := range bundle.poolInputs {
		canonicalPools[string(p.PoolKeyHash)] = i
	}
	for _, p := range pools {
		i, ok := canonicalPools[string(p.PoolKeyHash)]
		if !ok {
			return nil, errors.New("unexpected retained pool")
		}
		c := bundle.poolInputs[i]
		if p.DelegatedStake != c.DelegatedStake ||
			p.OwnerStake != c.OwnerStake ||
			p.DelegatorCount != c.DelegatorCount {
			return nil, fmt.Errorf(
				"pool %x canonical stake/owners/parameters disagree: stored/canonical delegated=%v/%v owner=%v/%v delegators=%d/%d pledge=%v/%v cost=%v/%v margin=%v/%v reward_tag=%d/%d reward_account=%x/%x",
				p.PoolKeyHash,
				p.DelegatedStake,
				c.DelegatedStake,
				p.OwnerStake,
				c.OwnerStake,
				p.DelegatorCount,
				c.DelegatorCount,
				p.Pledge,
				c.Pledge,
				p.Cost,
				c.Cost,
				p.Margin,
				c.Margin,
				p.RewardAccountCredentialTag,
				c.RewardAccountCredentialTag,
				p.RewardAccount,
				c.RewardAccount,
			)
		}
	}
	rows, err := store.GetRewardStakeInputs(epoch, txn)
	if err != nil {
		return nil, err
	}
	report := &RewardInputRepairReport{
		Epoch:         epoch,
		StoredRows:    len(rows),
		CanonicalRows: len(bundle.stakeInputs),
	}
	key := func(pool, cred []byte, tag uint8) string { return string(pool) + string([]byte{tag}) + string(cred) }
	canonical := map[string]int{}
	for i, row := range bundle.stakeInputs {
		canonical[key(row.PoolKeyHash, row.StakingKey, row.CredentialTag)] = i
	}
	updates := make(
		[]*models.RewardStakeInput,
		0,
		max(0, len(bundle.stakeInputs)-len(rows)),
	)
	for _, row := range rows {
		i, ok := canonical[key(row.PoolKeyHash, row.StakingKey, row.CredentialTag)]
		if !ok {
			return nil, errors.New(
				"retained stake input absent from canonical snapshot")
		}
		c := bundle.stakeInputs[i]
		if row.Stake != c.Stake || row.Owner != c.Owner ||
			row.Registered != c.Registered {
			report.ChangedRows++
			updates = append(updates, c)
		}
		delete(
			canonical,
			key(row.PoolKeyHash, row.StakingKey, row.CredentialTag),
		)
	}
	for _, i := range canonical {
		updates = append(updates, bundle.stakeInputs[i])
	}
	if apply &&
		(report.StoredRows != report.CanonicalRows || report.ChangedRows != 0) {
		if err := store.DeleteRewardOutputsForEpoch(epoch, txn); err != nil {
			return nil, err
		}
		if err := store.DeleteSyncState(fmt.Sprintf(models.StakeRewardSourcePrefix+"precompute-cursor:%d", epoch), txn); err != nil {
			return nil, err
		}
		if err := store.SaveRewardStakeInputs(updates, txn); err != nil {
			return nil, err
		}
	}
	if apply {
		remaining := make(
			[]importedRewardStakeInputsPendingEntry,
			0,
			len(pending.Entries),
		)
		for _, entry := range pending.Entries {
			if entry.Epoch != epoch {
				remaining = append(remaining, entry)
			}
		}
		if len(remaining) != len(pending.Entries) {
			pending.Entries = remaining
			encoded, err := json.Marshal(pending)
			if err != nil {
				return nil, err
			}
			if err := store.SetSyncState(models.StakeRewardSourcePrefix+"stake-inputs-pending", string(encoded), txn); err != nil {
				return nil, err
			}
		}
	}
	return report, nil
}

func authoritativeRewardInputPoolParams(
	store RewardInputRepairStore,
	txn types.Txn,
	selected *ParsedSnapShot,
	retained *models.RewardSnapshot,
) (map[string]*ParsedPool, error) {
	previousEpoch := retained.Epoch
	if previousEpoch > 0 {
		previousEpoch--
	}
	history, err := store.GetEpoch(previousEpoch, txn)
	if err != nil {
		return nil, err
	}
	if history == nil {
		return nil, errors.New("epoch history missing")
	}
	ids := make([]lcommon.PoolKeyHash, 0, len(selected.PoolParams))
	seen := make(map[string]bool, len(selected.PoolParams))
	for _, key := range selected.Delegations {
		if !seen[string(key)] {
			var id lcommon.PoolKeyHash
			copy(id[:], key)
			ids = append(ids, id)
			seen[string(key)] = true
		}
	}
	registrationStartSlot, registrationEpoch := snapshot.PoolRegistrationSnapshotWindow(
		event.EpochTransitionEvent{
			NewEpoch:        retained.Epoch,
			PreviousEpoch:   previousEpoch,
			BoundarySlot:    retained.BoundarySlot,
			ProtocolVersion: retained.ProtocolVersion,
		},
		history.StartSlot,
	)
	registrations, err := store.GetPoolRegistrationsEffectiveForEpoch(
		ids,
		registrationStartSlot,
		registrationEpoch,
		retained.CapturedSlot,
		txn,
	)
	if err != nil {
		return nil, err
	}

	return retainedCaptureRewardPoolParams(
		selected,
		rewardPoolParamsFromRegistrations(registrations),
	), nil
}

func retainedCaptureRewardPoolParams(
	selected *ParsedSnapShot,
	historical map[string]*ParsedPool,
) map[string]*ParsedPool {
	params := effectiveRewardPoolParams(selected, nil)
	referenced := referencedRewardPools(selected)
	for key, pool := range historical {
		if _, ok := referenced[key]; !ok {
			continue
		}
		params[key] = pool
	}
	return params
}
