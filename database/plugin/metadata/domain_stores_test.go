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
	"ClearGovernanceProposalRatification",
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

// utxoStoreMethods is the UTxO domain: every method sqlstore implements in
// utxo.go, which owns the utxo table and nothing else.
var utxoStoreMethods = []string{
	"CreateUtxo",
	"DeleteUtxo",
	"DeleteUtxos",
	"DeleteUtxosAfterSlot",
	"SetUtxoDeletedAtSlot",
	"SetUtxosNotDeletedAfterSlot",
	"MarkUtxosDeletedAtSlot",
	"AddUtxos",
	"ImportUtxos",
	"GetUtxoBalanceByAddress",
	"GetUtxo",
	"GetUtxoIncludingSpent",
	"GetUtxosByRefs",
	"GetUtxosAddedAfterSlot",
	"GetLiveUtxosBySlot",
	"GetUtxosBySlot",
	"GetUtxosDeletedBeforeSlot",
	"GetUtxosByAddress",
	"GetUtxosByAddressWithOrdering",
	"CountUtxosByAddressWithOrdering",
	"GetUtxosByAddressAtSlot",
	"GetControlledAmountByCredential",
	"GetUtxoPaymentScriptByCredential",
	"GetScriptLockedSupply",
	"GetUtxosByAssets",
	"IterateLiveUtxos",
}

// transactionStoreMethods is the chain-transaction domain: sqlstore's
// transaction_read.go and transaction_write.go, which own the transaction
// table and the address/metadata-label indexes derived from it.
//
// Not to be confused with TxnStore, which creates database transactions.
// CountTransactionsInSlotRange and GetBlockSlotRangeStats are implemented in
// transaction_read.go but belong to SlotRangeStore, which already extracted
// them for the API adapters.
var transactionStoreMethods = []string{
	"GetTransactionByHash",
	"GetTransactionSlotByHash",
	"GetTransactionIDByHash",
	"GetTransactionMetadataByHash",
	"SumTransactionFeesInSlotRange",
	"GetTransactionsByBlockHash",
	"GetTransactionsByHashes",
	"GetTransactionHashesAfterSlot",
	"CountTransactionsByPaymentCred",
	"CountTransactionsByMetadataLabel",
	"DeleteAddressTransactionsAfterSlot",
	"DeleteTransactionMetadataLabelsAfterSlot",
	"CountTransactionsByAddress",
	"GetTransactionsByAddress",
	"GetTransactionsByMetadataLabel",
	"GetAddressesByCredential",
	"CountAddressesByCredential",
	"GetAddressTransactionsByCredential",
	"CountAddressTransactionsByCredential",
	"NewBatchAccumulator",
	"FlushBatch",
	"SetTransactionBatched",
	"SetTransaction",
	"SetGapBlockTransaction",
	"RecomputeGapCollateralFee",
	"SetGenesisTransaction",
	"DeleteTransactionsAfterSlot",
}

// epochStoreMethods is the epoch domain. Unlike the others this one is
// defined by its table rather than by an implementation file: sqlstore's
// operational.go is a grab-bag holding tip, nonces, datums, scripts,
// protocol parameters, network state, and sync state alongside the epoch
// methods, so taking the file wholesale would produce an interface that is
// not a domain at all.
var epochStoreMethods = []string{
	"SetEpoch",
	"GetEpochsByEra",
	"GetEpoch",
	"GetEpochs",
	"GetEpochBySlot",
	"DeleteEpochsAfterSlot",
}

// stakeSnapshotStoreMethods is the stake-snapshot domain: sqlstore's
// stake_snapshot.go (the pool_stake_snapshot and epoch-summary tables) and
// historical_stake.go (per-epoch-boundary stake off active_delegator_stake).
//
// live_stake.go is deliberately not here: it rebuilds reward_live_stake from
// the live utxo, account, and certificate tables to feed reward
// calculation, so its subject is live stake rather than a snapshot.
// GetRewardStakeInputsForPools is implemented in historical_stake.go but was
// never on MetadataStore, so there is nothing to move.
var stakeSnapshotStoreMethods = []string{
	"SavePoolStakeSnapshot",
	"SavePoolStakeSnapshots",
	"GetPoolStakeSnapshot",
	"GetPoolStakeSnapshotsByEpoch",
	"GetPoolStakeSnapshotsForPools",
	"GetTotalActiveStake",
	"SaveEpochSummary",
	"GetEpochSummary",
	"GetLatestEpochSummary",
	"DeletePoolStakeSnapshotsForEpoch",
	"DeletePoolStakeSnapshotsAfterEpoch",
	"DeletePoolStakeSnapshotsBeforeEpoch",
	"DeleteEpochSummariesAfterEpoch",
	"GetStakeByPoolsAtSlot",
	"GetEpochBoundaryStakeByPools",
	"GetPoolOwnerStakeAtSlot",
	"GetEpochBoundaryRewardStakeInputsForPools",
}

// certificateStoreMethods is the certificate domain: sqlstore's
// certificates.go (certs, MIR, genesis delegation, and the rollback delete)
// plus the certificate-history readers that live in account_history.go --
// each of those joins certs to a stake_* certificate table, unlike the
// withdrawal-history and witness readers beside them, which read
// account_reward_delta and the account witness tables and stay with the
// account domain.
var certificateStoreMethods = []string{
	"DeleteCertificatesAfterSlot",
	"GetMIRCertsInSlotRange",
	"GetGenesisDelegationForSlot",
	"GetStakeRegistrationsByCredential",
	"GetAccountDelegationHistoryByCredential",
	"CountAccountDelegationHistoryByCredential",
	"GetAccountRegistrationHistoryByCredential",
	"CountAccountRegistrationHistoryByCredential",
}

// domains is every interface split out of MetadataStore so far, checked as
// one table so a new extraction cannot skip any of the checks below.
var domains = []struct {
	name    string
	typ     reflect.Type
	methods []string
}{
	{
		"GovernanceStore",
		reflect.TypeFor[metadata.GovernanceStore](),
		governanceStoreMethods,
	},
	{
		"UtxoStore",
		reflect.TypeFor[metadata.UtxoStore](),
		utxoStoreMethods,
	},
	{
		"TransactionStore",
		reflect.TypeFor[metadata.TransactionStore](),
		transactionStoreMethods,
	},
	{
		"EpochStore",
		reflect.TypeFor[metadata.EpochStore](),
		epochStoreMethods,
	},
	{
		"StakeSnapshotStore",
		reflect.TypeFor[metadata.StakeSnapshotStore](),
		stakeSnapshotStoreMethods,
	},
	{
		"CertificateStore",
		reflect.TypeFor[metadata.CertificateStore](),
		certificateStoreMethods,
	},
}

func TestDomainStoreSurfaces(t *testing.T) {
	for _, d := range domains {
		t.Run(d.name, func(t *testing.T) {
			want := append([]string(nil), d.methods...)
			sort.Strings(want)
			require.Equal(t, want, interfaceMethodNames(d.typ))
		})
	}
}

// TestMetadataStoreComposesDomains is the "backend implementations still
// compile against the composed interface" half of the split: extracting a
// domain must not remove anything from MetadataStore's surface, or every
// existing caller of the composed interface breaks.
func TestMetadataStoreComposesDomains(t *testing.T) {
	composed := reflect.TypeFor[metadata.MetadataStore]()
	for _, d := range domains {
		t.Run(d.name, func(t *testing.T) {
			for m := range d.typ.Methods() {
				onComposed, ok := composed.MethodByName(m.Name)
				require.Truef(
					t,
					ok,
					"MetadataStore no longer exposes %s; the composed "+
						"interface must keep every method the extracted "+
						"domain took",
					m.Name,
				)
				require.Equalf(
					t,
					m.Type,
					onComposed.Type,
					"signature of %s drifted between %s and MetadataStore",
					m.Name,
					d.name,
				)
			}
		})
	}
}

// TestDomainStoresAreNarrowerThanMetadataStore guards the reason the split
// exists: a caller depending on one domain must not transitively reach the
// whole surface again.
func TestDomainStoresAreNarrowerThanMetadataStore(t *testing.T) {
	composed := reflect.TypeFor[metadata.MetadataStore]()
	for _, d := range domains {
		t.Run(d.name, func(t *testing.T) {
			require.Less(t, d.typ.NumMethod(), composed.NumMethod())

			// A MetadataStore satisfies the domain, but not the reverse.
			require.True(t, composed.Implements(d.typ))
			require.False(t, d.typ.Implements(composed))
		})
	}
}

// TestDomainsDoNotOverlap keeps the split a partition rather than a set of
// overlapping views. A method on two domain interfaces has an ambiguous
// owner, and the next extraction has no way to tell which one to take it
// from.
func TestDomainsDoNotOverlap(t *testing.T) {
	owner := map[string]string{}
	for _, d := range domains {
		for m := range d.typ.Methods() {
			if prev, dup := owner[m.Name]; dup {
				t.Errorf(
					"%s is on both %s and %s; a method needs exactly one "+
						"owning domain",
					m.Name, prev, d.name,
				)
				continue
			}
			owner[m.Name] = d.name
		}
	}
}

func interfaceMethodNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumMethod())
	for m := range t.Methods() {
		names = append(names, m.Name)
	}
	sort.Strings(names)
	return names
}
