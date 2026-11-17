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

package badgerutxo

import "github.com/blinklabs-io/dingo/database/nodesettings"

// tagFamilies maps a row family tag to the ledger-state family whose metadata
// tables a load of it reads. A tag in neither this map nor undrainedTags
// counts as drained whenever any family is.
var tagFamilies = map[byte]nodesettings.Family{
	NonceTag:             nodesettings.FamilyNonces,
	EpochTag:             nodesettings.FamilyEpochs,
	SnapshotTag:          nodesettings.FamilyEpochs,
	SummaryTag:           nodesettings.FamilySummaries,
	NetworkStateTag:      nodesettings.FamilyNetworkDonations,
	DonationTag:          nodesettings.FamilyNetworkDonations,
	PParamsTag:           nodesettings.FamilyPParamsUpdates,
	PParamsSlotTag:       nodesettings.FamilyPParamsUpdates,
	PParamUpdateTag:      nodesettings.FamilyPParamsUpdates,
	AccountTag:           nodesettings.FamilyAccounts,
	AccountPoolTag:       nodesettings.FamilyAccounts,
	AccountDRepTag:       nodesettings.FamilyAccounts,
	RewardRecordTag:      nodesettings.FamilyAccounts,
	StakeHistoryTag:      nodesettings.FamilyAccounts,
	ImportBaselineTag:    nodesettings.FamilyAccounts,
	WitnessTag:           nodesettings.FamilyAccounts,
	DRepTag:              nodesettings.FamilyDReps,
	DRepLinkTag:          nodesettings.FamilyDReps,
	DormancyTag:          nodesettings.FamilyDReps,
	DRepHistoryTag:       nodesettings.FamilyDReps,
	DRepDepositTag:       nodesettings.FamilyDReps,
	PoolTag:              nodesettings.FamilyPoolsHistory,
	PoolHistoryTag:       nodesettings.FamilyPoolsHistory,
	OpCertTag:            nodesettings.FamilyOpCerts,
	GenesisDelegationTag: nodesettings.FamilyMIRGenesis,
	MIRCertTag:           nodesettings.FamilyMIRGenesis,
	CommitteeMemberTag:   nodesettings.FamilyCommitteeConstitution,
	CommitteeQuorumTag:   nodesettings.FamilyCommitteeConstitution,
	ConstitutionTag:      nodesettings.FamilyCommitteeConstitution,
	HotAuthTag:           nodesettings.FamilyCommitteeConstitution,
	ResignTag:            nodesettings.FamilyCommitteeConstitution,
	ProposalTag:          nodesettings.FamilyGovernance,
	VoteTag:              nodesettings.FamilyGovernance,
	RewardOutputTag:      nodesettings.FamilyRewardOutputs,
	RewardEpochTag:       nodesettings.FamilyRewardOutputs,
	RewardRoundTag:       nodesettings.FamilyRewardOutputs,
	RewardPostSnapTag:    nodesettings.FamilyRewardOutputs,
	RewardDerivedTag:     nodesettings.FamilyRewardOutputs,
	RewardCursorTag:      nodesettings.FamilyRewardOutputs,
	RewardBlocksTag:      nodesettings.FamilyRewardBlocksFees,
	ImportedBlocksTag:    nodesettings.FamilyRewardBlocksFees,
	RewardStakeInputTag:  nodesettings.FamilyRewardInputs,
	RewardPoolInputTag:   nodesettings.FamilyRewardInputs,
	RewardSnapshotTag:    nodesettings.FamilyRewardInputs,
	RewardPotsTag:        nodesettings.FamilyRewardInputs,
}

// undrainedTags are the row families loaded from tables no drain empties
// (certificate and transaction rows), so a load of them never reads a drained
// family.
var undrainedTags = map[byte]bool{
	DelegationTag:  true,
	ClosureUtxoTag: true,
}

// tagDrained reports whether a load of the rows under prefix reads a family
// in drained.
func tagDrained(drained nodesettings.FamilyMask, prefix []byte) bool {
	if len(prefix) == 0 {
		return drained != 0
	}
	if undrainedTags[prefix[0]] {
		return false
	}
	family, ok := tagFamilies[prefix[0]]
	if !ok {
		return drained != 0
	}
	return drained.Has(family)
}
