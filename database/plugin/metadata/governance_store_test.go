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

package metadata_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// governanceStoreMethods is the agreed surface of the governance domain: the
// methods whose subject is a governance entity owned by the Conway governance
// tables (governance_proposal, governance_vote, auth_committee_hot,
// committee_member, committee quorum, constitution, drep).
//
// Spelling it out here rather than deriving it from the interface is the
// point of the test: a method that drifts into or out of GovernanceStore has
// to be argued for by editing this list, which is where the domain boundary
// is reviewed.
//
// Deliberately excluded, with the reason, so a later reader does not "fix"
// the omission by reflex:
//
//   - ImportDrep: belongs to the snapshot bulk-import cluster alongside
//     ImportAccount and ImportPool, which migrates as its own domain.
//   - ClearDanglingDRepDelegations: mutates the account table (it clears an
//     account's DRep delegation), not the drep table.
//   - SetNetworkState / GetNetworkState / DeleteNetworkStateAfterSlot and the
//     network-donation methods: treasury and reserves are ledger economics
//     consumed by reward calculation, not governance state, even though they
//     sit inside the governance run of section comments in store.go.
var governanceStoreMethods = []string{
	// Proposals and votes
	"GetGovernanceProposal",
	"GetActiveGovernanceProposals",
	"GetRatifiedGovernanceProposals",
	"GetEnactedGovernanceProposalsAt",
	"GetExpiringGovernanceProposals",
	"GetExpiredGovernanceProposalsAt",
	"GetLastEnactedGovernanceProposal",
	"SetGovernanceProposal",
	"GetChildGovernanceProposals",
	"GetGovernanceVotes",
	"SetGovernanceVote",
	"DeleteGovernanceProposalsAfterSlot",
	"DeleteGovernanceVotesAfterSlot",

	// Committee
	"GetCommitteeMember",
	"GetActiveCommitteeMembers",
	"IsCommitteeMemberResigned",
	"GetResignedCommitteeMembers",
	"GetCommitteeActiveCount",
	"SetCommitteeMembers",
	"SetCommitteeQuorum",
	"ClearCommitteeQuorum",
	"GetCommitteeQuorum",
	"GetCommitteeMembers",
	"GetCommitteeMembersIncludeDeleted",
	"DeleteCommitteeMembersAfterSlot",
	"SoftDeleteCommitteeMembers",
	"SoftDeleteAllCommitteeMembers",

	// DReps
	"CreateDrep",
	"GetDrep",
	"GetDrepByCredential",
	"GetActiveDreps",
	"GetDreps",
	"GetPredefinedDrepFirstSeenSlots",
	"GetDrepLastRegistrationSlot",
	"InsertDrepIfAbsent",
	"GetDRepVotingPower",
	"GetDRepDelegators",
	"GetDRepVotingPowerBatch",
	"GetDRepVotingPowerByType",
	"UpdateDRepActivity",
	"GetExpiredDReps",
	"RestoreDrepStateAtSlot",

	// Constitution
	"GetConstitution",
	"SetConstitution",
	"DeleteConstitutionsAfterSlot",
}

func TestGovernanceStoreSurface(t *testing.T) {
	got := interfaceMethodNames(reflect.TypeFor[metadata.GovernanceStore]())

	want := append([]string(nil), governanceStoreMethods...)
	sort.Strings(want)

	require.Equal(t, want, got)
}

// TestMetadataStoreComposesGovernanceStore is the "backend implementations
// still compile against the composed interface" half of the split: extracting
// a domain must not remove anything from MetadataStore's surface, or every
// existing caller of the composed interface breaks.
func TestMetadataStoreComposesGovernanceStore(t *testing.T) {
	gov := reflect.TypeFor[metadata.GovernanceStore]()
	composed := reflect.TypeFor[metadata.MetadataStore]()

	for m := range gov.Methods() {
		onComposed, ok := composed.MethodByName(m.Name)
		require.Truef(
			t,
			ok,
			"MetadataStore no longer exposes %s; the composed interface "+
				"must keep every method the extracted domain took",
			m.Name,
		)
		require.Equalf(
			t,
			m.Type,
			onComposed.Type,
			"signature of %s drifted between GovernanceStore and "+
				"MetadataStore",
			m.Name,
		)
	}
}

// TestGovernanceStoreIsNarrowerThanMetadataStore guards the reason the split
// exists: a caller depending on GovernanceStore must not transitively reach
// the whole ~280-method surface again.
func TestGovernanceStoreIsNarrowerThanMetadataStore(t *testing.T) {
	gov := reflect.TypeFor[metadata.GovernanceStore]()
	composed := reflect.TypeFor[metadata.MetadataStore]()

	require.Less(t, gov.NumMethod(), composed.NumMethod())

	// A MetadataStore satisfies GovernanceStore, but not the reverse.
	require.True(t, composed.Implements(gov))
	require.False(t, gov.Implements(composed))
}

func interfaceMethodNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumMethod())
	for m := range t.Methods() {
		names = append(names, m.Name)
	}
	sort.Strings(names)
	return names
}
