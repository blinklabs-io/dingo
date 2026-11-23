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
	"bytes"
	"cmp"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// accountFactRows exports the key and Patch columns of each account row
// whose slot is in range: a patch table's rows only update rows the reader
// already holds, so no other column is carried.
func (ls *LedgerState) accountFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r := ls.accountReader(nil)
	lister, ok := r.(accountLister)
	if r == nil || !ok {
		return metadata.FactRows{}, drainedUnserved(nodesettings.FamilyAccounts)
	}
	var accounts []models.Account
	err := lister.Accounts(func(a *models.Account) error {
		if inSlotRange(a.AddedSlot, from, through) {
			accounts = append(accounts, *a)
		}
		return nil
	})
	if err != nil {
		return metadata.FactRows{}, err
	}
	slices.SortFunc(accounts, func(a, b models.Account) int {
		return cmp.Or(
			cmp.Compare(a.CredentialTag, b.CredentialTag),
			bytes.Compare(a.StakingKey, b.StakingKey),
		)
	})
	out := metadata.FactRows{Columns: accountFactColumns}
	for i := range accounts {
		out.Rows = append(out.Rows, accountFactRow(&accounts[i]))
	}
	return out, nil
}

var accountFactColumns = []string{
	"staking_key", "credential_tag", "pool", "drep", "drep_type",
	"active", "expiration_epoch", "added_slot",
}

func accountFactRow(a *models.Account) []any {
	return []any{
		nilBytes(a.StakingKey), sqlInt(uint64(a.CredentialTag)),
		nilBytes(a.Pool), nilBytes(a.Drep), sqlInt(a.DrepType),
		boolInt(a.Active), sqlInt(a.ExpirationEpoch), sqlInt(a.AddedSlot),
	}
}

// drepFactRows exports the key and Patch columns of each DRep row whose slot
// is in range.
func (ls *LedgerState) drepFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r := ls.drepReader(nil)
	if r == nil {
		return metadata.FactRows{}, drainedUnserved(nodesettings.FamilyDReps)
	}
	var dreps []models.Drep
	err := r.Dreps(func(d *models.Drep) error {
		if inSlotRange(d.AddedSlot, from, through) {
			dreps = append(dreps, *d)
		}
		return nil
	})
	if err != nil {
		return metadata.FactRows{}, err
	}
	slices.SortFunc(dreps, func(a, b models.Drep) int {
		return cmp.Or(
			cmp.Compare(a.CredentialTag, b.CredentialTag),
			bytes.Compare(a.Credential, b.Credential),
		)
	})
	out := metadata.FactRows{Columns: drepFactColumns}
	for i := range dreps {
		out.Rows = append(out.Rows, drepFactRow(&dreps[i]))
	}
	return out, nil
}

var drepFactColumns = []string{
	"credential", "credential_tag", "active", "added_slot",
}

func drepFactRow(d *models.Drep) []any {
	return []any{
		nilBytes(d.Credential), sqlInt(uint64(d.CredentialTag)),
		boolInt(d.Active), sqlInt(d.AddedSlot),
	}
}

func inOptSlotRange(slot *uint64, from, through uint64) bool {
	return slot != nil && inSlotRange(*slot, from, through)
}

func govProposalLess(a, b models.GovernanceProposal) int {
	return cmp.Or(
		bytes.Compare(a.TxHash, b.TxHash),
		cmp.Compare(a.ActionIndex, b.ActionIndex),
	)
}

// governanceProposals returns the proposals matching keep, in natural-key
// order.
func (ls *LedgerState) governanceProposals(
	keep func(models.GovernanceProposal) bool,
) ([]models.GovernanceProposal, error) {
	r := ls.governanceReader(nil)
	if r == nil {
		return nil, drainedUnserved(nodesettings.FamilyGovernance)
	}
	all, err := r.ProposalsAt(nil)
	if err != nil {
		return nil, err
	}
	out := slices.DeleteFunc(all, func(p models.GovernanceProposal) bool {
		return !keep(p)
	})
	slices.SortFunc(out, govProposalLess)
	return out, nil
}

// asOfSlot is a slot column as of through: a slot past it has not happened.
func asOfSlot(slot *uint64, through uint64) (keep bool) {
	return slot != nil && *slot <= through
}

// governanceProposalFactRows exports the key and Patch columns of every
// proposal with a lifecycle slot in range, each lifecycle group null past
// through, as the metadata store reads them.
func (ls *LedgerState) governanceProposalFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	props, err := ls.governanceProposals(func(p models.GovernanceProposal) bool {
		return inOptSlotRange(p.RatifiedSlot, from, through) ||
			inOptSlotRange(p.EnactedSlot, from, through) ||
			inOptSlotRange(p.ExpiredSlot, from, through) ||
			inOptSlotRange(p.DeletedSlot, from, through)
	})
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"tx_hash", "action_index", "ratified_epoch", "ratified_slot",
		"enacted_epoch", "enacted_slot", "expired_epoch", "expired_slot",
		"deleted_slot",
	}}
	group := func(epoch, slot *uint64) (any, any) {
		if !asOfSlot(slot, through) {
			return nil, nil
		}
		return optSQLInt(epoch), sqlInt(*slot)
	}
	for _, p := range props {
		rE, rS := group(p.RatifiedEpoch, p.RatifiedSlot)
		eE, eS := group(p.EnactedEpoch, p.EnactedSlot)
		xE, xS := group(p.ExpiredEpoch, p.ExpiredSlot)
		var del any
		if asOfSlot(p.DeletedSlot, through) {
			del = sqlInt(*p.DeletedSlot)
		}
		out.Rows = append(out.Rows, []any{
			nilBytes(p.TxHash), sqlInt(uint64(p.ActionIndex)), rE, rS, eE, eS,
			xE, xS, del,
		})
	}
	return out, nil
}

func (ls *LedgerState) governanceProposalDropFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	props, err := ls.governanceProposals(func(p models.GovernanceProposal) bool {
		return inOptSlotRange(p.DroppedSlot, from, through)
	})
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"tx_hash", "action_index", "dropped_epoch", "dropped_slot",
	}}
	for _, p := range props {
		out.Rows = append(out.Rows, []any{
			nilBytes(p.TxHash), sqlInt(uint64(p.ActionIndex)),
			optSQLInt(p.DroppedEpoch), sqlInt(*p.DroppedSlot),
		})
	}
	return out, nil
}

// poolStakeSnapshotFactRows exports the pool stake snapshots of the epochs the
// set holds that were captured in [from, through], by epoch, type and pool.
func (ls *LedgerState) poolStakeSnapshotFactRows(
	_ metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	r, _ := ls.epochStateReader(nil)
	if r == nil {
		return metadata.FactRows{}, drainedUnserved(nodesettings.FamilyEpochs)
	}
	view, err := ls.epochStateView(r, nil)
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{
		"epoch", "snapshot_type", "pool_key_hash", "total_stake",
		"stake_denominator", "delegator_count", "captured_slot",
		"vrf_key_hash", "leios_key_public", "leios_key_possession_proof",
		"leios_key_registration_epoch", "calculation_version",
		"reward_account_auto_vote", "reward_account_auto_vote_resolved",
	}}
	held := make(map[uint64]struct{}, len(view))
	for id := range view {
		held[id] = struct{}{}
	}
	snapshots, err := ls.snapshotsCapturedInView(r, nil, from, through)
	if err != nil {
		return metadata.FactRows{}, err
	}
	for _, s := range snapshots {
		if _, ok := held[s.Epoch]; !ok ||
			!slices.Contains(rewardSnapshotTypes, s.SnapshotType) {
			continue
		}
		out.Rows = append(out.Rows, []any{
			sqlInt(s.Epoch), s.SnapshotType, nilBytes(s.PoolKeyHash),
			uint64Text(s.TotalStake), uint64Text(s.StakeDenominator),
			sqlInt(s.DelegatorCount), sqlInt(s.CapturedSlot),
			nilBytes(s.VrfKeyHash), nilBytes(s.LeiosKeyPublic),
			nilBytes(s.LeiosKeyPossessionProof),
			optSQLInt(s.LeiosKeyRegistrationEpoch),
			sqlInt(s.CalculationVersion),
			sqlInt(uint64(s.RewardAccountAutoVote)),
			boolInt(s.RewardAccountAutoVoteResolved),
		})
	}
	return out, nil
}
