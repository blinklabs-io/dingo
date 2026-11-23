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

package mcp

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/blinklabs-io/dingo/database/nodesettings"
)

var errReadsBusy = errors.New("too many concurrent SQLite queries")

var errOffsetTooLarge = errors.New("offset exceeds the configured maximum")

// readLimits holds optional resource limits shared by the MCP tools that
// run database reads. A zero or nil field disables that limit.
type readLimits struct {
	slots       chan struct{}
	callerSlots chan struct{}
	parseSlots  chan struct{}
	// maxAssetRows is the most live asset rows one get_asset_info call may
	// scan; zero means unlimited.
	maxAssetRows int
	// maxOffset is the largest OFFSET a paginated tool accepts; zero means
	// unlimited.
	maxOffset int
	// ledgerStateOutsideSQL is set when the ledger state is read from the
	// LedgerDB, so the SQL tables that held it are empty.
	ledgerStateOutsideSQL bool
}

// ErrLedgerStateNotInSQL is wrapped by the error a tool returns for a read of
// SQL tables that held ledger state the node now keeps in its LedgerDB.
var ErrLedgerStateNotInSQL = errors.New(
	"the metadata database does not hold this ledger state",
)

// ledgerStateTables are the SQL tables of each ledger-state family. On a node
// that keeps its ledger state in the LedgerDB they hold no rows, and an empty
// answer would read as the truth.
var ledgerStateTables = []struct {
	name   string
	family nodesettings.Family
}{
	{"block_nonce", nodesettings.FamilyNonces},
	{"network_donation", nodesettings.FamilyNetworkDonations},
	{"network_state", nodesettings.FamilyNetworkDonations},
	{"reward_live_stake", nodesettings.FamilyLiveStake},
	{"auth_committee_hot", nodesettings.FamilyCommitteeConstitution},
	{"resign_committee_cold", nodesettings.FamilyCommitteeConstitution},
	{"committee_member", nodesettings.FamilyCommitteeConstitution},
	{"committee_quorum", nodesettings.FamilyCommitteeConstitution},
	{"constitution", nodesettings.FamilyCommitteeConstitution},
	{"governance_vote_history", nodesettings.FamilyGovernance},
	{"governance_vote", nodesettings.FamilyGovernance},
	{"governance_proposal_ratification_history", nodesettings.FamilyGovernance},
	{"governance_proposal_order", nodesettings.FamilyGovernance},
	{"governance_proposal_drop", nodesettings.FamilyGovernance},
	{"governance_proposal", nodesettings.FamilyGovernance},
	{"pparam_update", nodesettings.FamilyPParamsUpdates},
	{"pparams", nodesettings.FamilyPParamsUpdates},
	{"genesis_delegation", nodesettings.FamilyMIRGenesis},
	{"imported_pool_block_count", nodesettings.FamilyRewardBlocksFees},
	{"imported_epoch_block_total", nodesettings.FamilyRewardBlocksFees},
	{"reward_stake_input", nodesettings.FamilyRewardInputs},
	{"reward_pool_input", nodesettings.FamilyRewardInputs},
	{"reward_seed_failure", nodesettings.FamilyRewardInputs},
	{"reward_snapshot", nodesettings.FamilyRewardInputs},
	{"reward_ada_pots", nodesettings.FamilyRewardInputs},
	{"pool_opcert_sequence", nodesettings.FamilyOpCerts},
	{"drep_delegator", nodesettings.FamilyDReps},
	{"drep_expiry_history", nodesettings.FamilyDReps},
	{"drep_dormancy_history", nodesettings.FamilyDReps},
	{"drep_dormancy_state", nodesettings.FamilyDReps},
	{"drep_expiry_epoch_event", nodesettings.FamilyDReps},
	{"account_drep_clear", nodesettings.FamilyDReps},
	{"drep", nodesettings.FamilyDReps},
	{"account_reward_delta", nodesettings.FamilyAccounts},
	{"account_withdrawal_witness", nodesettings.FamilyAccounts},
	{"account_import_baseline", nodesettings.FamilyAccounts},
	{"registration", nodesettings.FamilyAccounts},
	{"deregistration", nodesettings.FamilyAccounts},
	{"stake_registration", nodesettings.FamilyAccounts},
	{"stake_deregistration", nodesettings.FamilyAccounts},
	{"account_inactivity_activation", nodesettings.FamilyAccounts},
	{"account", nodesettings.FamilyAccounts},
	{"pool_registration_owner", nodesettings.FamilyPoolsHistory},
	{"pool_registration_relay", nodesettings.FamilyPoolsHistory},
	{"pool_retirement", nodesettings.FamilyPoolsHistory},
	{"pool_registration", nodesettings.FamilyPoolsHistory},
	{"pool", nodesettings.FamilyPoolsHistory},
	{"reward_account_output", nodesettings.FamilyRewardOutputs},
	{"reward_pool_output", nodesettings.FamilyRewardOutputs},
	{"reward_credit_round", nodesettings.FamilyRewardOutputs},
	{"epoch_summary", nodesettings.FamilySummaries},
	{"epoch", nodesettings.FamilyEpochs},
}

// refuseLedgerStateTables refuses a caller's SQL that names a ledger-state
// table.
func (l *readLimits) refuseLedgerStateTables(query string) error {
	if l == nil || !l.ledgerStateOutsideSQL {
		return nil
	}
	for _, table := range ledgerStateTables {
		ref := regexp.MustCompile(
			`(?i)(^|[^a-z0-9_])` + regexp.QuoteMeta(table.name) +
				`($|[^a-z0-9_])`,
		)
		if ref.MatchString(query) {
			return fmt.Errorf(
				"%w: table %s belongs to family %s; use a tool that reads the ledger state",
				ErrLedgerStateNotInSQL,
				table.name,
				table.family,
			)
		}
	}
	return nil
}

// refuseLedgerStateFamilies refuses a tool path that reads the SQL tables of
// families directly.
func (l *readLimits) refuseLedgerStateFamilies(
	families ...nodesettings.Family,
) error {
	if l == nil || !l.ledgerStateOutsideSQL || len(families) == 0 {
		return nil
	}
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.String())
	}
	return fmt.Errorf(
		"%w: families %s; the tool needs the ledger state",
		ErrLedgerStateNotInSQL, strings.Join(names, ", "),
	)
}

func newReadLimits() *readLimits {
	return &readLimits{}
}

func (l *readLimits) admit() error {
	if !l.acquire() {
		return errReadsBusy
	}
	return nil
}

// admitCallerSQL takes a caller-SQL slot and a shared query slot. A
// successful call must be paired with releaseCallerSQL.
func (l *readLimits) admitCallerSQL() error {
	if l.callerSlots != nil {
		select {
		case l.callerSlots <- struct{}{}:
		default:
			return errReadsBusy
		}
	}
	if !l.acquire() {
		if l.callerSlots != nil {
			<-l.callerSlots
		}
		return errReadsBusy
	}
	return nil
}

func (l *readLimits) releaseCallerSQL() {
	l.release()
	if l.callerSlots != nil {
		<-l.callerSlots
	}
}

// acquire takes a query slot without blocking and reports whether it got one.
// With no slot budget configured it always succeeds.
func (l *readLimits) acquire() bool {
	if l.slots == nil {
		return true
	}
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *readLimits) release() {
	if l.slots != nil {
		<-l.slots
	}
}

// offsetLimitNote returns a notice for a full page whose next offset would
// exceed maxOffset, or "" otherwise. Without it, a caller paging until a
// short page cannot tell a complete result from one cut off by the bound.
func offsetLimitNote(offset, limit, returned, maxOffset int) string {
	if maxOffset <= 0 || returned < limit || offset+limit <= maxOffset {
		return ""
	}
	return fmt.Sprintf(
		"**Offset limit reached**: the next page would start past "+
			"offset %d, which this tool rejects. Matching rows beyond "+
			"this page are not retrievable here; treat the result as "+
			"truncated.\n\n",
		maxOffset,
	)
}

// assetScanLimit returns the SQL LIMIT for the asset-row scan: one row past
// maxAssetRows so an over-cap policy is detected, or -1 (no limit).
func (l *readLimits) assetScanLimit() int {
	if l.maxAssetRows <= 0 {
		return -1
	}
	return l.maxAssetRows + 1
}
