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
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// SetDrainedFamilies records the ledger-state families whose rows were
// deleted. The mask only widens.
func (s *Store) SetDrainedFamilies(mask nodesettings.FamilyMask) {
	for {
		cur := s.drained.Load()
		next := cur | uint64(mask)
		if cur == next || s.drained.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (s *Store) familyDrained(f nodesettings.Family) bool {
	return nodesettings.FamilyMask(s.drained.Load()).Has(f)
}

// tableFamilies maps a table that belongs to a drainable ledger-state family
// to that family, for the code paths that serve several families' tables.
var tableFamilies = map[string]nodesettings.Family{
	"auth_committee_hot":    nodesettings.FamilyCommitteeConstitution,
	"resign_committee_cold": nodesettings.FamilyCommitteeConstitution,
	"committee_member":      nodesettings.FamilyCommitteeConstitution,
	"committee_quorum":      nodesettings.FamilyCommitteeConstitution,
	"constitution":          nodesettings.FamilyCommitteeConstitution,
	"pool_opcert_sequence":  nodesettings.FamilyOpCerts,
	"genesis_delegation":    nodesettings.FamilyMIRGenesis,
	"registration":          nodesettings.FamilyAccounts,
	"deregistration":        nodesettings.FamilyAccounts,
	"stake_registration":    nodesettings.FamilyAccounts,
	"stake_deregistration":  nodesettings.FamilyAccounts,
	"pool_registration":     nodesettings.FamilyPoolsHistory,
	"pool_retirement":       nodesettings.FamilyPoolsHistory,
	"pool":                  nodesettings.FamilyPoolsHistory,

	"account":                    nodesettings.FamilyAccounts,
	"account_reward_delta":       nodesettings.FamilyAccounts,
	"account_withdrawal_witness": nodesettings.FamilyAccounts,
	"account_import_baseline":    nodesettings.FamilyAccounts,

	"account_inactivity_activation": nodesettings.FamilyAccounts,

	"drep":                    nodesettings.FamilyDReps,
	"drep_delegator":          nodesettings.FamilyDReps,
	"drep_expiry_history":     nodesettings.FamilyDReps,
	"drep_dormancy_history":   nodesettings.FamilyDReps,
	"drep_dormancy_state":     nodesettings.FamilyDReps,
	"drep_expiry_epoch_event": nodesettings.FamilyDReps,
	"account_drep_clear":      nodesettings.FamilyDReps,

	"reward_live_stake": nodesettings.FamilyLiveStake,

	"reward_stake_input": nodesettings.FamilyRewardInputs,

	"imported_pool_block_count":  nodesettings.FamilyRewardBlocksFees,
	"imported_epoch_block_total": nodesettings.FamilyRewardBlocksFees,
	"reward_account_output":      nodesettings.FamilyRewardOutputs,
	"reward_pool_output":         nodesettings.FamilyRewardOutputs,
	"reward_credit_round":        nodesettings.FamilyRewardOutputs,
}

// rewardReadTables are the tables the historical stake and live stake reads
// join: the accounts and their reward journal, the pool registrations, the
// live stake aggregate and the credited rounds with their outputs.
var rewardReadTables = []string{
	"account", "account_reward_delta", "account_withdrawal_witness",
	"pool_registration", "reward_live_stake", "reward_account_output",
	"reward_credit_round",
}

// tableDrained reports whether table belongs to a drained family.
func (s *Store) tableDrained(table string) bool {
	f, ok := tableFamilies[table]
	return ok && s.familyDrained(f)
}

// liveCertificateTables returns tables without the ones whose family is
// drained.
func (s *Store) liveCertificateTables(tables []string) []string {
	if s.drained.Load() == 0 {
		return tables
	}
	return slices.DeleteFunc(slices.Clone(tables), s.tableDrained)
}

// errTableDrained is returned by a read that needs the rows of a drained
// family's table.
func errTableDrained(table string) error {
	return fmt.Errorf("%w: %s", nodesettings.ErrFamilyDrained, table)
}

// poolsDrained reports whether the pool, registration, owner, relay and
// retirement tables are deleted.
func (s *Store) poolsDrained() bool {
	return s.familyDrained(nodesettings.FamilyPoolsHistory)
}

// refuseDrained returns errTableDrained for the first of tables that belongs
// to a drained family. A read that answers from those rows calls it first, so
// it cannot return the emptied table as an answer.
func (s *Store) refuseDrained(tables ...string) error {
	if s.drained.Load() == 0 {
		return nil
	}
	for _, table := range tables {
		if s.tableDrained(table) {
			return errTableDrained(table)
		}
	}
	return nil
}
