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

package ledger_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// setModeScenario builds the boundary scenario from the live set over a loaded
// set. The store is the metadata store read around the ledger.
func setModeScenario(
	t *testing.T,
) (ledger.BoundaryEpochStateScenario, *utxoset.Set, ledgerdb.LedgerDB) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	return scenario, set, store
}

func accountRow(a *models.Account) string {
	c := *a
	c.ID, c.CertificateID, c.ImportDeposit = 0, 0, nil
	c.InactivityActivation, c.InactivityActivationEpoch = false, 0
	if len(c.Pool) == 0 {
		c.Pool = nil
	}
	if len(c.Drep) == 0 {
		c.Drep = nil
	}
	return fmt.Sprintf("%+v", c)
}

func drepRow(d *models.Drep) string {
	c := *d
	c.ID, c.Delegators = 0, nil
	return fmt.Sprintf("%+v", c)
}

func poolRegistrationRow(r *models.PoolRegistration) string {
	c := ledger.CleanPoolRegistration(r)
	margin := "nil"
	if c.Margin != nil && c.Margin.Rat != nil {
		margin = c.Margin.Rat.String()
	}
	var owners []string
	for _, o := range c.Owners {
		owners = append(owners, fmt.Sprintf("%x", o.KeyHash))
	}
	slices.Sort(owners)
	relays := fmt.Sprintf("%d", len(c.Relays))
	return fmt.Sprintf(
		"%x %x %d %d %d %x/%d %q %x %s %v %s slot=%d",
		c.PoolKeyHash,
		c.VrfKeyHash,
		c.Pledge,
		c.Cost,
		c.RewardAccountCredentialTag,
		c.RewardAccount,
		len(c.RewardAccount),
		c.MetadataUrl,
		c.MetadataHash,
		margin,
		owners,
		relays,
		c.AddedSlot,
	)
}

var opCertReadNames = []string{
	"LatestPoolOpCertSequence", "LatestPoolOpCertSequenceAfter",
	"LatestPoolOpCertSequenceAtOrBefore", "LatestPoolOpCertSequenceInRange",
	"LatestPoolOpCertSequences",
}

type opCertAnswer struct {
	sequence uint64
	found    bool
}

// TestOpCertReadsFromTheLiveUtxoSet reads each pool's counter, latest, after
// a slot, as of a slot and within a range, over counters that rise, repeat,
// drop and are batched. A range the set cannot answer fails closed.
func TestOpCertReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	ldb := scenario.LedgerDB()
	slots := scenario.OpCerts()
	ctx := t.Context()
	none := opCertAnswer{}
	seq := func(n uint64) opCertAnswer { return opCertAnswer{n, true} }
	answer := func(sequence uint64, found bool, err error) opCertAnswer {
		t.Helper()
		require.NoError(t, err)
		return opCertAnswer{sequence, found}
	}
	type slotWant struct {
		slot uint64
		want opCertAnswer
	}
	type rangeWant struct {
		start, end uint64
		want       opCertAnswer
	}
	for _, tc := range []struct {
		name     string
		pool     []byte
		latest   opCertAnswer
		after    []slotWant
		atOrUpTo []slotWant
		ranges   []rangeWant
	}{
		{
			name: "pool", pool: slots.Pool, latest: seq(6),
			after: []slotWant{
				{slots.Base, seq(6)}, {slots.Raised, seq(6)},
				{slots.Lowered, seq(6)}, {slots.Batched, none},
			},
			atOrUpTo: []slotWant{
				{slots.Base - 1, none}, {slots.Base, seq(3)},
				{slots.Raised, seq(5)}, {slots.Repeated, seq(5)},
				{slots.Lowered, seq(5)}, {slots.Batched, seq(6)},
				{slots.Batched + 1, seq(6)},
			},
			ranges: []rangeWant{
				{slots.Base, slots.Batched, seq(6)},
				{slots.Raised, slots.Lowered, seq(5)},
				{slots.Lowered + 1, slots.Batched, seq(6)},
				{slots.Batched, slots.Base, none},
			},
		},
		{
			name: "other", pool: slots.Other, latest: seq(2),
			after: []slotWant{
				{slots.Base, seq(2)}, {slots.Raised, seq(2)},
				{slots.Lowered, none}, {slots.Batched, none},
			},
			atOrUpTo: []slotWant{
				{slots.Base - 1, none}, {slots.Base, none},
				{slots.Raised, none}, {slots.Repeated, seq(2)},
				{slots.Lowered, seq(2)}, {slots.Batched, seq(2)},
				{slots.Batched + 1, seq(2)},
			},
			ranges: []rangeWant{
				{slots.Base, slots.Batched, seq(2)},
				{slots.Raised, slots.Lowered, seq(2)},
				{slots.Repeated, slots.Lowered, seq(2)},
				{slots.Lowered + 1, slots.Batched, none},
				{slots.Batched, slots.Base, none},
			},
		},
	} {
		pkh := lcommon.PoolKeyHash(tc.pool)
		require.Equal(t, tc.latest,
			answer(ldb.LatestPoolOpCertSequence(ctx, pkh, nil)), tc.name)
		for _, w := range tc.after {
			require.Equal(t, w.want,
				answer(ldb.LatestPoolOpCertSequenceAfter(ctx, pkh, w.slot, nil)),
				"%s after %d", tc.name, w.slot)
		}
		for _, w := range tc.atOrUpTo {
			require.Equal(t, w.want,
				answer(ldb.LatestPoolOpCertSequenceAtOrBefore(
					ctx, pkh, w.slot, nil,
				)),
				"%s at or before %d", tc.name, w.slot)
		}
		for _, w := range tc.ranges {
			require.Equal(t, w.want,
				answer(ldb.LatestPoolOpCertSequenceInRange(
					ctx, pkh, w.start, w.end, nil,
				)),
				"%s in %d..%d", tc.name, w.start, w.end)
		}
	}
	// The pool's highest counter was reached before this range and a lower
	// one follows inside it, which the set does not record.
	_, _, err := ldb.LatestPoolOpCertSequenceInRange(
		ctx, lcommon.PoolKeyHash(slots.Pool),
		slots.Repeated, slots.Lowered, nil,
	)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	all, err := ldb.LatestPoolOpCertSequences(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{
		string(slots.Pool): 6, string(slots.Other): 2,
	}, all)

	// A counter reached and repeated: after its first observation the only
	// later one holds the same sequence, which the set knows because the
	// pool's latest observation repeats its highest counter.
	repeated := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6f}, 28))
	for _, slot := range []uint64{slots.Batched + 2, slots.Batched + 3} {
		txn := ldb.Begin(ctx, true)
		require.NoError(t, ldb.UpdatePoolOpCertSequence(
			ctx, repeated, 7, slot, txn,
		))
		require.NoError(t, txn.Commit())
		txn.Release()
	}
	require.Equal(t, seq(7), answer(ldb.LatestPoolOpCertSequenceAfter(
		ctx, repeated, slots.Batched+2, nil,
	)), "the latest observation repeats the highest")
	require.Equal(t, seq(7), answer(ldb.LatestPoolOpCertSequenceAfter(
		ctx, repeated, slots.Batched+1, nil,
	)), "the counter first reached after the slot")

	scenario.RollbackBeforeBoundary()
	pkh := lcommon.PoolKeyHash(slots.Pool)
	require.Equal(t, seq(3),
		answer(ldb.LatestPoolOpCertSequence(ctx, pkh, nil)),
		"after the rollback the counter from before it is the only one")
	require.Equal(t, none, answer(ldb.LatestPoolOpCertSequenceAfter(
		ctx, pkh, slots.Base, nil,
	)))
	require.Equal(t, seq(3), answer(ldb.LatestPoolOpCertSequenceInRange(
		ctx, pkh, slots.Base, slots.Lowered, nil,
	)))
}

// TestOpCertReadsBeforeTheSetsRecordUseItsBaseline requires reads of slots
// before the set's recorded observations to be answered from the counters it
// was loaded with.
func TestOpCertReadsBeforeTheSetsRecordUseItsBaseline(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	poolA := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x41}, 28))
	poolB := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x42}, 28))
	// Counters an earlier release left in its tables: the set takes over
	// each pool's latest.
	scenario := ledger.NewSeededBoundaryEpochStateScenarioForTest(
		t, set, nil, func(db *database.Database) {
			for _, w := range []struct {
				pool           lcommon.PoolKeyHash
				sequence, slot uint64
			}{{poolA, 4, 100}, {poolB, 3, 150}, {poolA, 6, 200}} {
				require.NoError(t, dbadapter.New(db).UpdatePoolOpCertSequence(
					t.Context(), w.pool, w.sequence, w.slot, nil,
				))
			}
		},
	)
	reached, _ := scenario.HideStoreReads(opCertReadNames...)
	ldb := scenario.LedgerDB()
	ctx := t.Context()

	latest, found, err := ldb.LatestPoolOpCertSequence(ctx, poolA, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(6), latest)
	at250, _, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		ctx,
		poolA,
		250,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6), at250)
	_, found, err = ldb.LatestPoolOpCertSequenceAfter(ctx, poolA, 200, nil)
	require.NoError(t, err)
	require.False(t, found, "no observation after the latest")
	require.Zero(t, reached())

	at150, found, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		ctx, poolA, 150, nil,
	)
	require.NoError(t, err, "slot 150 is before the set's record")
	require.True(t, found)
	require.Equal(t, uint64(6), at150)
	after100, found, err := ldb.LatestPoolOpCertSequenceAfter(
		ctx, poolA, 100, nil,
	)
	require.NoError(t, err, "the latest observation repeats the highest")
	require.True(t, found)
	require.Equal(t, uint64(6), after100)
	require.Zero(t, reached(), "historical reads did not use the metadata store")
}

func committeeMemberRows(ms []*models.CommitteeMember) []string {
	var out []string
	for _, m := range ms {
		deleted := "-"
		if m.DeletedSlot != nil {
			deleted = fmt.Sprint(*m.DeletedSlot)
		}
		out = append(out, fmt.Sprintf("%d/%x/%d/%d/%t/%d/%s",
			m.ColdCredentialTag, m.ColdCredHash, m.ExpiresEpoch,
			m.TermStartSlot, m.TermStartSlotSet, m.AddedSlot, deleted))
	}
	slices.Sort(out)
	return out
}

func committeeAuthRows(as []*models.AuthCommitteeHot) []string {
	var out []string
	for _, a := range as {
		out = append(out, fmt.Sprintf("%d/%x/%d/%x/%d",
			a.ColdCredentialTag, a.ColdCredential,
			a.HotCredentialTag, a.HotCredential, a.AddedSlot))
	}
	slices.Sort(out)
	return out
}

var liveStakeReadNames = []string{
	"GetLiveStakeInputsForPools", "GetDelegatedPoolKeyHashes",
	"GetStakeByPools", "GetDRepVotingPower", "GetDRepVotingPowerBatch",
	"GetDRepVotingPowerByType",
}

// TestLiveStakeReadsCountPendingCreditsFromTheLiveUtxoSet requires the reads
// that include pending reward credits to be answered from the set once it
// holds the reward rows, with the same figures as the metadata store, which
// adds the credits itself.
func TestLiveStakeReadsCountPendingCreditsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, "set")
	round := h.round(11)
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		require.NoError(t, ldb.AddAppliedRewardCreditRound(round, txn))
		require.NoError(t, ldb.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{
				h.output(h.deleg, 11, 9, h.base, round.BoundarySlot),
			}, txn))
	})
	reached, _ := h.scenario.HideStoreReads(slices.Concat(
		liveStakeReadNames, rewardOutputReadNames,
	)...)
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()
	pools := [][]byte{h.pool}
	in, err := ldb.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	got, _ := stakeOf(in, h.deleg)
	require.Equal(t, uint64(20_000_009), got, "the set adds the pending credit")
	_, err = ldb.GetDRepVotingPower(ctx, 0, h.owner.Key, 0, nil)
	require.NoError(t, err)
	_, err = ldb.GetDRepVotingPowerBatch(ctx, nil, 0, nil)
	require.NoError(t, err)
	_, err = ldb.GetDRepVotingPowerByType(ctx, []uint64{2, 3}, 0, nil)
	require.NoError(t, err)
	require.Zero(t, reached(), "a live stake read reached the metadata store")
}

// boundaryOutcome returns the durable state after each phase. When hideStore
// is true, every read the live set serves fails against the metadata store.
func boundaryOutcome(t *testing.T, hideStore bool) map[string][]string {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	// The harness's stake is rows the set never saw; a node's set holds the
	// outputs the rows came from.
	scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
	var reached func() int64
	if hideStore {
		// BumpDormantDRepExpiries reads the dormancy counter from the store
		// around its own write, so that read is not hidden.
		reached, _ = scenario.HideStoreReads(slices.Concat(
			[]string{
				"GetAccountByCredential", "GetAccountsByCredential",
				"GetDrepByCredential", "GetActiveDreps",
				"GetConstitution", "GetCommitteeQuorum", "GetCommitteeMembers",
				"GetCommitteeMembersIncludeDeleted", "GetCommitteeMember",
				"IsCommitteeMemberResigned", "GetResignedCommitteeMembers",
				"GetActiveCommitteeMembers", "GetCommitteeAuthorizedCount",
				"GetCommitteeHotAuthorizationsSince",
			},
			opCertReadNames, liveStakeReadNames, governanceReadNames,
			accountListReadNames, poolListReadNames, rewardOutputReadNames,
			opCertSlotReadNames, registrationReadNames,
			pparamUpdateReadNames,
		)...)
	}
	out := map[string][]string{}
	sorted := func(rows []string) []string {
		slices.Sort(rows)
		return rows
	}
	snapshot := func(phase string) {
		t.Helper()
		var rows []string
		require.NoError(
			t,
			store.IterateAccounts(nil, func(a *models.Account) error {
				rows = append(rows, accountRow(a))
				return nil
			}),
		)
		out[phase+"/accounts"] = sorted(rows)
		rows = nil
		require.NoError(t, store.IterateDreps(nil, func(d *models.Drep) error {
			rows = append(rows, drepRow(d))
			return nil
		}))
		out[phase+"/dreps"] = sorted(rows)
		rows = nil
		epochs, err := store.GetEpochs(nil)
		require.NoError(t, err)
		for _, e := range epochs {
			rows = append(
				rows,
				fmt.Sprintf("epoch %d %d %d", e.EpochId, e.StartSlot, e.EraId),
			)
			for _, typ := range []string{"mark", "set", "go"} {
				snaps, err := store.GetPoolStakeSnapshotsByEpoch(
					e.EpochId,
					typ,
					nil,
				)
				require.NoError(t, err)
				for _, sn := range snaps {
					rows = append(rows, fmt.Sprintf(
						"snapshot %d %s %x %d %d",
						e.EpochId,
						typ,
						sn.PoolKeyHash,
						sn.TotalStake,
						sn.DelegatorCount,
					))
				}
			}
			donations, err := store.SumNetworkDonationsForEpoch(e.EpochId, nil)
			require.NoError(t, err)
			rows = append(
				rows,
				fmt.Sprintf("donations %d %d", e.EpochId, donations),
			)
		}
		state, err := store.GetNetworkState(nil)
		require.NoError(t, err)
		rows = append(rows, fmt.Sprintf("network %d %d %d",
			state.Slot, state.Treasury, state.Reserves))
		out[phase+"/epochs"] = sorted(rows)
		rows = nil
		pools, err := store.ListPoolKeyHashes(nil)
		require.NoError(t, err)
		for _, p := range pools {
			rows = append(rows, fmt.Sprintf("pool %x", p))
		}
		counters, err := store.LatestPoolOpCertSequences(t.Context(), nil)
		require.NoError(t, err)
		for p, n := range counters {
			rows = append(rows, fmt.Sprintf("counter %x %d", p, n))
		}
		out[phase+"/pools"] = sorted(rows)
		rows = nil
		members, err := store.GetCommitteeMembersIncludeDeleted(
			t.Context(),
			nil,
		)
		require.NoError(t, err)
		rows = append(rows, committeeMemberRows(members)...)
		auths, err := store.GetCommitteeHotAuthorizationsSince(
			t.Context(),
			0,
			nil,
		)
		require.NoError(t, err)
		rows = append(rows, committeeAuthRows(auths)...)
		quorum, err := store.GetCommitteeQuorum(t.Context(), nil)
		require.NoError(t, err)
		if quorum != nil {
			rows = append(rows, "quorum "+quorum.String())
		}
		out[phase+"/committee"] = sorted(rows)
		rows = nil
		proposals, err := store.GetGovernanceProposalSet(t.Context(), nil)
		require.NoError(t, err)
		for _, p := range proposals {
			rows = append(
				rows,
				fmt.Sprintf("proposal %x/%d", p.TxHash, p.ActionIndex),
			)
		}
		out[phase+"/proposals"] = sorted(rows)
		rows = nil
		allProposals, err := store.GetAllGovernanceProposals(t.Context(), nil)
		require.NoError(t, err)
		rows = append(rows, proposalRows(allProposals, true)...)
		allVotes, err := store.GetAllGovernanceVotes(t.Context(), nil)
		require.NoError(t, err)
		for _, v := range allVotes {
			rows = append(rows, fmt.Sprintf(
				"%x#%d %s", v.TxHash, v.ActionIndex, voteRow(&v.Vote),
			))
		}
		out[phase+"/governance rows"] = sorted(rows)
	}
	snapshot("start")
	scenario.ApplyBoundary()
	snapshot("boundary")
	var ref models.StakeCredentialRef
	var pool []byte
	require.NoError(
		t,
		store.IterateAccounts(nil, func(a *models.Account) error {
			if a.Active && ref.Key == nil {
				ref = models.NewStakeCredentialRef(
					a.CredentialTag,
					a.StakingKey,
				)
			}
			if a.Active && len(a.Pool) > 0 && pool == nil {
				pool = a.Pool
			}
			return nil
		}),
	)
	scenario.Donate(40)
	scenario.Credit(ref, 7)
	require.NoError(t, scenario.Withdraw(ref, 5))
	snapshot("rewards")
	scenario.Reap(pool)
	scenario.Expire(ref)
	scenario.Governance(ref, 9)
	snapshot("governance")
	scenario.Pools(func() {})
	scenario.OpCerts()
	snapshot("pools and op-certs")
	scenario.Committee()
	scenario.Proposals()
	snapshot("committee and proposals")
	scenario.RollbackBeforeBoundary()
	snapshot("rollback")
	if reached != nil {
		require.Zero(t, reached(), "a read the set serves reached the store")
	}
	return out
}
