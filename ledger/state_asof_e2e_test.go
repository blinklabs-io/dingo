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
	"sort"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// asOfReadNames are the LedgerDB reads as of a slot the set answers from its
// journals.
var asOfReadNames = []string{
	"GetDrepsAtSlot", "GetDRepDelegatorsAtSlot",
	"GetDrepRegistrationDepositsAtSlot", "GetGovernanceVotesAtSlot",
	"GetPoolRegistrationsAtSlot", "GetRetiringPoolsAtSlot",
	"GetPoolDepositsHeldAtSlot", "GetAccountsByCredentialAtSlot",
}

func drepAtRows(ds []*models.Drep) []string {
	rows := make([]string, 0, len(ds))
	for _, d := range ds {
		rows = append(rows, fmt.Sprintf(
			"%d/%x active=%t added=%d last=%d expiry=%d anchor=%s/%x",
			d.CredentialTag, d.Credential, d.Active, d.AddedSlot,
			d.LastActivityEpoch, d.ExpiryEpoch, d.AnchorURL, d.AnchorHash,
		))
	}
	sort.Strings(rows)
	return rows
}

func delegatorAtRows(m map[string][]models.StakeCredentialRef) []string {
	var rows []string
	for k, stakes := range m {
		for _, s := range stakes {
			rows = append(rows, fmt.Sprintf("%x<-%d/%x", k, s.Tag, s.Key))
		}
	}
	sort.Strings(rows)
	return rows
}

func voteAtRows(vs []*models.GovernanceVote) []string {
	rows := make([]string, 0, len(vs))
	for _, v := range vs {
		rows = append(rows, fmt.Sprintf(
			"id=%d proposal=%d voter=%d/%d/%x vote=%d anchor=%s/%x added=%d",
			v.ID, v.ProposalID, v.VoterType, v.VoterCredentialTag,
			v.VoterCredential, v.Vote, v.AnchorURL, v.AnchorHash, v.AddedSlot,
		))
	}
	sort.Strings(rows)
	return rows
}

func accountAtRows(m map[string]*models.Account) []string {
	rows := make([]string, 0, len(m))
	for k, a := range m {
		rows = append(rows, fmt.Sprintf(
			"%x %d/%x pool=%x drep=%d/%x active=%t reward=%d added=%d created=%d",
			k,
			a.CredentialTag,
			a.StakingKey,
			a.Pool,
			a.DrepType,
			a.Drep,
			a.Active,
			a.Reward,
			a.AddedSlot,
			a.CreatedSlot,
		))
	}
	sort.Strings(rows)
	return rows
}

func poolRegistrationAtRows(rs []models.PoolRegistration) []string {
	rows := make([]string, 0, len(rs))
	for i := range rs {
		rows = append(rows, poolRegistrationRow(&rs[i]))
	}
	sort.Strings(rows)
	return rows
}

func retiringAtRows(rs []models.PoolRetiringRow) []string {
	rows := make([]string, 0, len(rs))
	for _, r := range rs {
		rows = append(rows, fmt.Sprintf("%x@%d", r.PoolKeyHash, r.Epoch))
	}
	sort.Strings(rows)
	return rows
}

func depositAtRows(m map[string]uint64) []string {
	rows := make([]string, 0, len(m))
	for k, v := range m {
		rows = append(rows, fmt.Sprintf("%x=%d", k, v))
	}
	sort.Strings(rows)
	return rows
}

// asOfProbe names what the as-of reads are asked about.
type asOfProbe struct {
	dreps     []models.StakeCredentialRef
	accounts  []models.StakeCredentialRef
	proposals []uint
	pools     []lcommon.PoolKeyHash
	// depositPools are the pools whose held deposit is read: the ones
	// registered at every probed slot. The store keeps a reaped pool's last
	// deposit, where the set holds none for a pool it has removed.
	depositPools []lcommon.PoolKeyHash
	epochs       []uint64
}

// requireAsOfReads reads every as-of read at slot through ldb and requires
// each answer to be the metadata store's.
func requireAsOfReads(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	p asOfProbe,
	slot uint64,
) {
	t.Helper()
	ctx := t.Context()
	label := fmt.Sprintf("slot %d", slot)
	for _, refs := range [][]models.StakeCredentialRef{nil, p.dreps} {
		want, err := store.GetDrepsAtSlot(ctx, refs, slot, nil)
		require.NoError(t, err, label)
		got, err := ldb.GetDrepsAtSlot(ctx, refs, slot, nil)
		require.NoError(t, err, label)
		require.Equal(
			t,
			drepAtRows(want),
			drepAtRows(got),
			"dreps at %s",
			label,
		)
		wantD, err := store.GetDRepDelegatorsAtSlot(ctx, refs, slot, nil)
		require.NoError(t, err, label)
		gotD, err := ldb.GetDRepDelegatorsAtSlot(ctx, refs, slot, nil)
		require.NoError(t, err, label)
		require.Equal(
			t, delegatorAtRows(wantD), delegatorAtRows(gotD),
			"delegators at %s", label,
		)
		wantDep, err := store.GetDrepRegistrationDepositsAtSlot(
			ctx,
			refs,
			slot,
			nil,
		)
		require.NoError(t, err, label)
		gotDep, err := ldb.GetDrepRegistrationDepositsAtSlot(
			ctx,
			refs,
			slot,
			nil,
		)
		require.NoError(t, err, label)
		require.Equal(
			t, depositAtRows(wantDep), depositAtRows(gotDep),
			"DRep deposits at %s", label,
		)
	}
	if len(p.accounts) > 0 {
		want, err := store.GetAccountsByCredentialAtSlot(
			ctx,
			p.accounts,
			slot,
			nil,
		)
		require.NoError(t, err, label)
		got, err := ldb.GetAccountsByCredentialAtSlot(
			ctx,
			p.accounts,
			slot,
			nil,
		)
		require.NoError(t, err, label)
		require.Equal(
			t,
			accountAtRows(want),
			accountAtRows(got),
			"accounts at %s",
			label,
		)
	}
	for _, id := range p.proposals {
		want, err := store.GetGovernanceVotesAtSlot(ctx, id, slot, nil)
		require.NoError(t, err, label)
		got, err := ldb.GetGovernanceVotesAtSlot(ctx, id, slot, nil)
		require.NoError(t, err, label)
		require.Equal(
			t, voteAtRows(want), voteAtRows(got),
			"votes on %d at %s", id, label,
		)
	}
	if len(p.depositPools) > 0 {
		wantH, err := store.GetPoolDepositsHeldAtSlot(p.depositPools, slot, nil)
		require.NoError(t, err, label)
		gotH, err := ldb.GetPoolDepositsHeldAtSlot(p.depositPools, slot, nil)
		require.NoError(t, err, label)
		require.Equal(
			t, depositAtRows(wantH), depositAtRows(gotH),
			"pool deposits at %s", label,
		)
	}
	if len(p.pools) > 0 {
		want, err := store.GetPoolRegistrationsAtSlot(p.pools, slot, nil)
		require.NoError(t, err, label)
		got, err := ldb.GetPoolRegistrationsAtSlot(p.pools, slot, nil)
		require.NoError(t, err, label)
		require.Equal(
			t, poolRegistrationAtRows(want), poolRegistrationAtRows(got),
			"pool registrations at %s", label,
		)
	}
	for _, epoch := range p.epochs {
		want, err := store.GetRetiringPoolsAtSlot(epoch, slot, nil)
		require.NoError(t, err, label)
		got, err := ldb.GetRetiringPoolsAtSlot(epoch, slot, nil)
		require.NoError(t, err, label)
		require.Equal(
			t, retiringAtRows(want), retiringAtRows(got),
			"retiring pools at epoch %d, %s", epoch, label,
		)
	}
}

// asOfScenario is setModeScenario over a set that has a tip, as a node's
// does once it has applied a block: the set's history starts there.
func asOfScenario(
	t *testing.T,
	reads string,
) (ledger.BoundaryEpochStateScenario, uint64, ledgerdb.LedgerDB) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set, reads)
	store := dbadapter.New(scenario.DB)
	txn := store.Begin(t.Context(), false)
	tip, err := store.GetTip(txn)
	txn.Release()
	require.NoError(t, err)
	point := ocommon.Point{Slot: tip.Point.Slot, Hash: tip.Point.Hash}
	require.NoError(t, set.ApplyUtxoChanges(&point, nil))
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	return scenario, point.Slot, store
}

func slotsFrom(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

// TestAsOfDRepAndAccountReadsFromTheLiveUtxoSet reads DReps, their delegators
// and deposits, and accounts as of every slot a scenario of registrations,
// vote delegations, activity and a deregistration wrote, in set mode with the
// metadata store's as-of reads failing, and requires the answers to be the
// store's, before and after a rollback.
func TestAsOfDRepAndAccountReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, tipSlot, store := asOfScenario(t, ledger.UtxoSetReadsSet)
	reached, _ := scenario.HideStoreReads(asOfReadNames...)
	ldb := scenario.LedgerDB()
	start := scenario.EpochStartSlot()
	var ref models.StakeCredentialRef
	require.NoError(
		t,
		store.IterateAccounts(nil, func(a *models.Account) error {
			if a.Active && ref.Key == nil {
				ref = models.NewStakeCredentialRef(
					a.CredentialTag,
					a.StakingKey,
				)
			}
			return nil
		}),
	)
	require.NotNil(t, ref.Key)
	drepKey := bytes.Repeat([]byte{0x5e}, 28)
	scenario.ApplyBoundary()
	scenario.VoteDelegate(ref, drepKey, 0, 3)
	scenario.Governance(ref, 9)
	slots := scenario.Registrations()
	probe := asOfProbe{
		dreps: []models.StakeCredentialRef{
			models.NewStakeCredentialRef(0, drepKey),
			models.NewStakeCredentialRef(0, slots.D),
			models.NewStakeCredentialRef(0, slots.E),
			models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x01}, 28)),
		},
		accounts: []models.StakeCredentialRef{
			ref, slots.Imported,
			models.NewStakeCredentialRef(0, slots.K),
			models.NewStakeCredentialRef(0, slots.L),
			models.NewStakeCredentialRef(0, slots.M),
		},
	}
	for _, slot := range slotsFrom(max(start, tipSlot), slots.DReReg+2) {
		requireAsOfReads(t, ldb, store, probe, slot)
	}
	require.Zero(t, reached(), "an as-of read reached the metadata store")
	scenario.RollbackBeforeBoundary()
	for _, slot := range slotsFrom(max(start, tipSlot), scenario.RollbackSlot()) {
		requireAsOfReads(t, ldb, store, probe, slot)
	}
	require.Zero(t, reached(), "an as-of read reached the metadata store")
}

// TestAsOfVoteReadsFromTheLiveUtxoSet reads the votes on both proposals of
// the governance scenario as of every slot it wrote, through votes cast,
// replaced, deleted with their DRep and cast again, with the metadata
// store's as-of vote read failing, and requires the answers to be the
// store's, before and after a rollback.
func TestAsOfVoteReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, tipSlot, store := asOfScenario(t, ledger.UtxoSetReadsSet)
	reached, _ := scenario.HideStoreReads(asOfReadNames...)
	ldb := scenario.LedgerDB()
	slots := scenario.Proposals()
	stored, err := store.GetAllGovernanceProposals(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, stored, 2)
	probe := asOfProbe{}
	for _, p := range stored {
		probe.proposals = append(probe.proposals, p.ID)
	}
	counted := 0
	for _, slot := range slotsFrom(max(slots.Base, tipSlot), slots.Enact+1) {
		if slot == slots.Deregister {
			continue
		}
		requireAsOfReads(t, ldb, store, probe, slot)
		for _, id := range probe.proposals {
			votes, err := ldb.GetGovernanceVotesAtSlot(
				t.Context(),
				id,
				slot,
				nil,
			)
			require.NoError(t, err)
			counted += len(votes)
		}
	}
	require.NotZero(t, counted, "the scenario's votes were never read")
	// The votes deleted with their DRep at Deregister are gone at it and live
	// the slot before. The store records the deletion as a tombstone, so its
	// answer agrees with the set's, which follows the journal.
	before, err := ldb.GetGovernanceVotesAtSlot(
		t.Context(),
		stored[0].ID,
		slots.Deregister-1,
		nil,
	)
	require.NoError(t, err)
	after, err := ldb.GetGovernanceVotesAtSlot(
		t.Context(),
		stored[0].ID,
		slots.Deregister,
		nil,
	)
	require.NoError(t, err)
	storeAfter, err := store.GetGovernanceVotesAtSlot(
		t.Context(),
		stored[0].ID,
		slots.Deregister,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, before, 2)
	require.Len(t, after, 1, "the deregistration removed a vote")
	require.Len(t, storeAfter, 1, "the store keeps the deletion")
	require.Equal(t, voteAtRows(before)[:1], voteAtRows(after))
	require.Equal(t, voteAtRows(after), voteAtRows(storeAfter))
	require.Zero(t, reached(), "an as-of read reached the metadata store")
	scenario.RollbackBeforeBoundary()
	for _, slot := range slotsFrom(max(slots.Base, tipSlot), scenario.RollbackSlot()) {
		requireAsOfReads(t, ldb, store, probe, slot)
	}
	require.Zero(t, reached(), "an as-of read reached the metadata store")
}

// TestAsOfPoolReadsFromTheLiveUtxoSet reads pool registrations, deposits and
// retirements as of the slots of a pool that registers and re-registers and
// of a pool that retires, in set mode with the metadata store's as-of reads
// failing, and requires the answers to be the store's, before and after a
// rollback.
func TestAsOfPoolReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, tipSlot, store := asOfScenario(t, ledger.UtxoSetReadsSet)
	reached, _ := scenario.HideStoreReads(asOfReadNames...)
	ldb := scenario.LedgerDB()
	start := max(scenario.EpochStartSlot(), tipSlot)
	harnessPools, err := store.ListPoolKeyHashes(nil)
	require.NoError(t, err)
	newPool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6a}, 28))
	probe := asOfProbe{
		pools: []lcommon.PoolKeyHash{
			newPool, lcommon.PoolKeyHash(bytes.Repeat([]byte{0x9a}, 28)),
		},
		depositPools: []lcommon.PoolKeyHash{newPool},
		epochs:       []uint64{4, 5, 6, 7},
	}
	for _, p := range harnessPools {
		probe.pools = append(probe.pools, lcommon.PoolKeyHash(p))
	}
	// The scenario writes its boundary, which reaps the retired pool, at a
	// slot before its own certificates, so the pool states and retirements
	// are read before it: every slot of the certificates.
	var before []uint64
	slots := scenario.Pools(func() {
		for _, slot := range slotsFrom(start, start+40) {
			requireAsOfReads(t, ldb, store, probe, slot)
			before = append(before, slot)
		}
		retiring, err := ldb.GetRetiringPoolsAtSlot(5, start+40, nil)
		require.NoError(t, err)
		require.NotEmpty(t, retiring, "the retirement was never read")
	})
	require.NotEmpty(t, before)
	require.Equal(t, slots.Retired, harnessPools[0])
	// The registrations come from the pool's certificate history, which the
	// reap does not touch.
	probe.depositPools, probe.epochs = nil, nil
	for _, slot := range slotsFrom(start, start+40) {
		requireAsOfReads(t, ldb, store, probe, slot)
	}
	require.Zero(t, reached(), "an as-of read reached the metadata store")
	scenario.RollbackBeforeBoundary()
	for _, slot := range slotsFrom(start, scenario.RollbackSlot()) {
		requireAsOfReads(t, ldb, store, probe, slot)
	}
	require.Zero(t, reached(), "an as-of read reached the metadata store")
}

// TestAsOfReadsOutsideTheJournalWindow requires a read as of a slot before
// the set's history to be the metadata store's in set mode, and a clear
// ErrLedgerStateAsOfUnavailable, never the store, when the set is the only
// copy; a slot the set reaches is still the set's.
func TestAsOfReadsOutsideTheJournalWindow(t *testing.T) {
	t.Parallel()
	scenario, _, store := asOfScenario(t, ledger.UtxoSetReadsSet)
	reached, hide := scenario.HideStoreReads(asOfReadNames...)
	ldb := scenario.LedgerDB()
	slots := scenario.Proposals()
	reads := asOfReadFuncs(t, ldb, store)
	names := make([]string, 0, len(reads))
	for name := range reads {
		names = append(names, name)
	}
	slices.Sort(names)
	require.ElementsMatch(t, asOfReadNames, names)
	const outside = 0
	inside := slots.Vote
	for _, name := range names {
		// Set mode: a slot before the set's history is the store's.
		hide(true)
		before := reached()
		err := reads[name](outside)
		require.Error(t, err, name)
		require.NotErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable, name)
		require.Greater(
			t,
			reached(),
			before,
			"%s did not fall back to the store",
			name,
		)
		hide(false)
		require.NoError(t, reads[name](outside), name)
	}
	hide(true)
	scenario.LedgerStateSetOnly(true)
	before := reached()
	for _, name := range names {
		require.ErrorIs(
			t, reads[name](outside), ledger.ErrLedgerStateAsOfUnavailable, name,
		)
		require.NoError(t, reads[name](inside), "%s inside the window", name)
	}
	require.Equal(
		t,
		before,
		reached(),
		"a set-only read reached the metadata store",
	)
}

// TestAsOfReadsFailClosedOnlyForADrainedFamily turns the set-only switch on
// and requires a read outside the journal window to keep falling back to the
// metadata store until a family the read needs is drained, and to fail with
// ErrLedgerStateAsOfUnavailable, never reaching the store, once one is.
func TestAsOfReadsFailClosedOnlyForADrainedFamily(t *testing.T) {
	t.Parallel()
	needs := map[string]nodesettings.FamilyMask{
		"GetDrepsAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyDReps),
		"GetDRepDelegatorsAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyAccounts),
		"GetDrepRegistrationDepositsAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyDReps),
		"GetGovernanceVotesAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyGovernance),
		"GetPoolRegistrationsAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyPoolsHistory),
		"GetRetiringPoolsAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyPoolsHistory),
		"GetPoolDepositsHeldAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyPoolsHistory),
		"GetAccountsByCredentialAtSlot": nodesettings.FamilyMask(0).
			With(nodesettings.FamilyAccounts),
	}
	require.Len(t, needs, len(asOfReadNames))
	const outside = 0
	for _, name := range asOfReadNames {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			scenario, _, store := asOfScenario(t, ledger.UtxoSetReadsSet)
			reached, hide := scenario.HideStoreReads(asOfReadNames...)
			scenario.Proposals()
			read := asOfReadFuncs(t, scenario.LedgerDB(), store)[name]
			hide(true)
			scenario.SetOnlySwitch(true)

			// The switch alone, and a family the read does not need,
			// leave the metadata store's rows in place.
			for _, drained := range []nodesettings.FamilyMask{
				0,
				nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces),
			} {
				scenario.DrainFamilies(drained)
				before := reached()
				err := read(outside)
				require.Error(t, err)
				require.NotErrorIs(
					t, err, ledger.ErrLedgerStateAsOfUnavailable,
				)
				require.Greater(
					t, reached(), before,
					"the read did not fall back to the store",
				)
			}

			scenario.DrainFamilies(needs[name])
			before := reached()
			require.ErrorIs(
				t, read(outside), ledger.ErrLedgerStateAsOfUnavailable,
			)
			require.Equal(
				t, before, reached(),
				"a read of a drained family reached the metadata store",
			)
		})
	}
}

// asOfReadFuncs returns each as-of LedgerDB read in asOfReadNames, keyed by
// name, as a function of the slot it reads at.
func asOfReadFuncs(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	store ledgerdb.LedgerDB,
) map[string]func(slot uint64) error {
	t.Helper()
	ctx := t.Context()
	stored, err := store.GetAllGovernanceProposals(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stored)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x9a}, 28))
	return map[string]func(slot uint64) error{
		"GetDrepsAtSlot": func(slot uint64) error {
			_, err := ldb.GetDrepsAtSlot(ctx, nil, slot, nil)
			return err
		},
		"GetDRepDelegatorsAtSlot": func(slot uint64) error {
			_, err := ldb.GetDRepDelegatorsAtSlot(ctx, nil, slot, nil)
			return err
		},
		"GetDrepRegistrationDepositsAtSlot": func(slot uint64) error {
			_, err := ldb.GetDrepRegistrationDepositsAtSlot(ctx, nil, slot, nil)
			return err
		},
		"GetGovernanceVotesAtSlot": func(slot uint64) error {
			_, err := ldb.GetGovernanceVotesAtSlot(ctx, stored[0].ID, slot, nil)
			return err
		},
		"GetPoolRegistrationsAtSlot": func(slot uint64) error {
			_, err := ldb.GetPoolRegistrationsAtSlot(
				[]lcommon.PoolKeyHash{pool},
				slot,
				nil,
			)
			return err
		},
		"GetRetiringPoolsAtSlot": func(slot uint64) error {
			_, err := ldb.GetRetiringPoolsAtSlot(1, slot, nil)
			return err
		},
		"GetPoolDepositsHeldAtSlot": func(slot uint64) error {
			_, err := ldb.GetPoolDepositsHeldAtSlot(
				[]lcommon.PoolKeyHash{pool},
				slot,
				nil,
			)
			return err
		},
		"GetAccountsByCredentialAtSlot": func(slot uint64) error {
			_, err := ldb.GetAccountsByCredentialAtSlot(
				ctx,
				[]models.StakeCredentialRef{
					models.NewStakeCredentialRef(0, pool[:]),
				},
				slot,
				nil,
			)
			return err
		},
	}
}

// TestAsOfReadsAreComparedInVerifyMode runs the as-of reads in verify mode:
// every read the set reaches is compared, agreeing reads add to the verified
// count alone, a read outside the window is not compared, and a DRep the
// metadata store holds that the set never saw is counted as a mismatch while
// the store's answer is the one returned.
func TestAsOfReadsAreComparedInVerifyMode(t *testing.T) {
	t.Parallel()
	scenario, _, store := asOfScenario(t, ledger.UtxoSetReadsVerify)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	start := scenario.EpochStartSlot()
	slots := scenario.Registrations()
	gov := scenario.Proposals()
	stored, err := store.GetAllGovernanceProposals(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stored)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x9a}, 28))
	counts := func() (verified, mismatches float64) {
		var v, m float64
		for _, f := range []func() (float64, float64){
			scenario.DrepReads, scenario.RegistrationReads, scenario.GovernanceReads,
			scenario.PoolReads, scenario.AccountReads,
		} {
			dv, dm := f()
			v += dv
			m += dm
		}
		return v, m
	}
	readAll := func(slot uint64) {
		_, err := ldb.GetDrepsAtSlot(ctx, nil, slot, nil)
		require.NoError(t, err)
		_, err = ldb.GetDRepDelegatorsAtSlot(ctx, nil, slot, nil)
		require.NoError(t, err)
		_, err = ldb.GetDrepRegistrationDepositsAtSlot(ctx, nil, slot, nil)
		require.NoError(t, err)
		_, err = ldb.GetGovernanceVotesAtSlot(ctx, stored[0].ID, slot, nil)
		require.NoError(t, err)
		_, err = ldb.GetPoolRegistrationsAtSlot(
			[]lcommon.PoolKeyHash{pool},
			slot,
			nil,
		)
		require.NoError(t, err)
		_, err = ldb.GetPoolDepositsHeldAtSlot(
			[]lcommon.PoolKeyHash{pool},
			slot,
			nil,
		)
		require.NoError(t, err)
		_, err = ldb.GetRetiringPoolsAtSlot(1, slot, nil)
		require.NoError(t, err)
		_, err = ldb.GetAccountsByCredentialAtSlot(
			ctx,
			[]models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, slots.K),
			},
			slot,
			nil,
		)
		require.NoError(t, err)
	}
	verified0, mismatches0 := counts()
	for _, slot := range []uint64{start, slots.DReReg, gov.Vote, gov.Enact} {
		readAll(slot)
	}
	verified1, mismatches1 := counts()
	require.Greater(t, verified1, verified0, "no as-of read was compared")
	require.Equal(
		t,
		mismatches0,
		mismatches1,
		"agreeing reads were counted as mismatches",
	)

	readAll(0)
	verified2, mismatches2 := counts()
	require.Equal(
		t,
		verified1,
		verified2,
		"a read outside the window was compared",
	)
	require.Equal(t, mismatches1, mismatches2)

	// A DRep written around the ledger exists for the store alone.
	around := bytes.Repeat([]byte{0xe3}, 28)
	require.NoError(t, scenario.DB.Metadata().InsertDrepIfAbsent(
		0, around, gov.Enact+1, "", nil, true, nil,
	))
	dreps, err := ldb.GetDrepsAtSlot(ctx, nil, gov.Enact+1, nil)
	require.NoError(t, err)
	require.Contains(t, drepAtRows(dreps), fmt.Sprintf(
		"0/%x active=true added=%d last=0 expiry=0 anchor=/",
		around,
		gov.Enact+1,
	), "the store's answer is returned")
	_, mismatches3 := counts()
	require.Equal(
		t,
		mismatches2+1,
		mismatches3,
		"the divergence was not counted",
	)
}
