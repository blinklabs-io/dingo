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

// asOfScenario is setModeScenario over a set that has a tip, as a node's
// does once it has applied a block: the set's history starts there.
func asOfScenario(
	t *testing.T,
	reads string,
) (ledger.BoundaryEpochStateScenario, uint64, ledgerdb.LedgerDB) {
	t.Helper()
	scenario, tip, store, _ := asOfScenarioSet(t)
	return scenario, tip, store
}

// asOfJournalFloorScenario is asOfScenario with the set's journal pruned
// below the scenario's tip, so the slots before it are outside the journal.
func asOfJournalFloorScenario(t *testing.T) ledger.BoundaryEpochStateScenario {
	t.Helper()
	scenario, tip, _, set := asOfScenarioSet(t)
	require.NotZero(t, tip)
	require.NoError(t, set.PruneHistory(tip))
	return scenario
}

func asOfScenarioSet(
	t *testing.T,
) (ledger.BoundaryEpochStateScenario, uint64, ledgerdb.LedgerDB, *utxoset.Set) {
	t.Helper()
	set, err := utxoset.Open(
		utxoset.Config{Dir: t.TempDir(), HistorySlots: 1_000},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	store := dbadapter.New(scenario.DB)
	txn := store.Begin(t.Context(), false)
	tip, err := store.GetTip(txn)
	txn.Release()
	require.NoError(t, err)
	point := ocommon.Point{Slot: tip.Point.Slot, Hash: tip.Point.Hash}
	// The families load at the scenario's rollback target, below the tip, so
	// the set's journal reaches back to it.
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	require.NoError(t, set.ApplyUtxoChanges(&point, nil))
	return scenario, point.Slot, store, set
}

func slotsFrom(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

// TestAsOfDRepReadsFromTheLiveUtxoSet reads DReps and their deposits as of
// the slots at which two DReps register, one updates, both retire and one
// registers again, and after a rollback below all of them.
func TestAsOfDRepReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := asOfScenario(t, "set")
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	slots := scenario.Registrations()
	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, slots.D),
		models.NewStakeCredentialRef(0, slots.E),
	}
	registered := func(slot uint64) []string {
		t.Helper()
		dreps, err := ldb.GetDrepsAtSlot(ctx, refs, slot, nil)
		require.NoError(t, err, "slot %d", slot)
		var out []string
		for _, d := range dreps {
			if d.Active {
				out = append(out, string(d.Credential))
			}
		}
		sort.Strings(out)
		return out
	}
	deposits := func(slot uint64) map[string]uint64 {
		t.Helper()
		got, err := ldb.GetDrepRegistrationDepositsAtSlot(ctx, refs, slot, nil)
		require.NoError(t, err, "slot %d", slot)
		return got
	}
	d, e := string(slots.D), string(slots.E)
	both := []string{d, e}
	sort.Strings(both)
	keyD := models.DrepDepositKey(0, slots.D)
	keyE := models.DrepDepositKey(0, slots.E)

	require.Empty(t, registered(slots.DRegister-1))
	require.Equal(t, both, registered(slots.DRegister))
	require.Equal(t, map[string]uint64{
		keyD: 500_000_000, keyE: 700_000_000,
	}, deposits(slots.DRegister))
	require.Equal(t, both, registered(slots.DUpdate))
	require.Equal(t, both, registered(slots.DRetire-1))
	require.Empty(t, registered(slots.DRetire))
	require.Equal(t, []string{d}, registered(slots.DReReg))
	require.Equal(t, uint64(600_000_000), deposits(slots.DReReg)[keyD])

	scenario.RollbackBeforeBoundary()
	require.Empty(t, registered(scenario.RollbackSlot()),
		"the rollback undid every registration")
}

// TestAsOfVoteReadsFromTheLiveUtxoSet reads the votes on a proposal as of
// the slots at which two DReps vote, one votes again, the other is
// deregistered with its votes, and votes once more.
func TestAsOfVoteReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := asOfScenario(t, "set")
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	slots := scenario.Proposals()
	proposals, err := ldb.GetAllGovernanceProposals(ctx, nil)
	require.NoError(t, err)
	require.Len(t, proposals, 2)
	var p1 uint
	for _, p := range proposals {
		if bytes.Equal(p.TxHash, slots.P1) {
			p1 = p.ID
		}
	}
	require.NotZero(t, p1, "the first proposal is not stored")
	voters := func(slot uint64) []string {
		t.Helper()
		votes, err := ldb.GetGovernanceVotesAtSlot(ctx, p1, slot, nil)
		require.NoError(t, err, "slot %d", slot)
		var out []string
		for _, v := range votes {
			out = append(out, fmt.Sprintf(
				"%x added=%d", v.VoterCredential, v.AddedSlot,
			))
		}
		sort.Strings(out)
		return out
	}
	vote := func(drep []byte, slot uint64) string {
		return fmt.Sprintf("%x added=%d", drep, slot)
	}
	sorted := func(rows ...string) []string {
		sort.Strings(rows)
		return rows
	}
	require.Empty(t, voters(slots.Vote-1))
	require.Equal(t,
		sorted(vote(slots.D1, slots.Vote), vote(slots.D2, slots.Vote)),
		voters(slots.Vote))
	require.Len(t, voters(slots.Deregister-1), 2)
	require.Len(t, voters(slots.Deregister), 1,
		"the deregistration removed the DRep's vote")
	for _, row := range voters(slots.Deregister) {
		require.Contains(t, row, fmt.Sprintf("%x", slots.D1))
	}
	require.Len(t, voters(slots.Revote2), 2, "the DRep voted again")

	scenario.RollbackBeforeBoundary()
	require.Empty(t, voters(scenario.RollbackSlot()),
		"the rollback undid every vote")
}

// TestAsOfPoolReadsFromTheLiveUtxoSet reads a pool's registration as of the
// slots at which it registers and re-registers with another VRF key, and the
// retiring pools as of the slot another pool announces its retirement.
func TestAsOfPoolReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, tipSlot, _ := asOfScenario(t, "set")
	ldb := scenario.LedgerDB()
	start := max(scenario.EpochStartSlot(), tipSlot)
	newPool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x6a}, 28))
	// The scenario's boundary reaps the retired pool, so the reads as of
	// the certificates' slots are taken before it.
	vrfAt := map[uint64][]byte{}
	retiringAt := map[uint64]map[uint64][]string{}
	slots := scenario.Pools(func() {
		for _, slot := range slotsFrom(start, start+40) {
			regs, err := ldb.GetPoolRegistrationsAtSlot(
				[]lcommon.PoolKeyHash{newPool}, slot, nil,
			)
			require.NoError(t, err, "slot %d", slot)
			require.LessOrEqual(t, len(regs), 1)
			if len(regs) == 1 {
				vrfAt[slot] = regs[0].VrfKeyHash
			}
			retiringAt[slot] = map[uint64][]string{}
			for _, epoch := range []uint64{4, 5, 6, 7} {
				rows, err := ldb.GetRetiringPoolsAtSlot(epoch, slot, nil)
				require.NoError(t, err, "slot %d", slot)
				for _, r := range rows {
					retiringAt[slot][epoch] = append(
						retiringAt[slot][epoch], string(r.PoolKeyHash),
					)
				}
			}
		}
	})
	require.Equal(t, newPool[:], slots.Pool)
	require.GreaterOrEqual(t, slots.Register, start)
	require.LessOrEqual(t, slots.Retire, start+40)
	require.NotContains(t, vrfAt, slots.Register-1)
	require.Equal(t, slots.VrfA, vrfAt[slots.Register])
	require.Equal(t, slots.VrfA, vrfAt[slots.Rotate-1])
	require.Equal(t, slots.VrfB, vrfAt[slots.Rotate])
	// The pool retires for RetireEpoch, so it is retiring as seen from the
	// epoch before, from the slot of its certificate on.
	require.Empty(t, retiringAt[slots.Retire-1][slots.RetireEpoch-1])
	require.Equal(t,
		[]string{string(slots.Retired)},
		retiringAt[slots.Retire][slots.RetireEpoch-1])
	require.Empty(t, retiringAt[slots.Retire][slots.RetireEpoch])
}

// TestAsOfReadsOutsideTheJournalWindow requires reads before the set's
// journal history to fail closed, while a slot it reaches remains readable.
func TestAsOfReadsOutsideTheJournalWindow(t *testing.T) {
	t.Parallel()
	scenario := asOfJournalFloorScenario(t)
	reached, hide := scenario.HideStoreReads(asOfReadNames...)
	ldb := scenario.LedgerDB()
	slots := scenario.Proposals()
	reads := asOfReadFuncs(t, ldb)
	names := make([]string, 0, len(reads))
	for name := range reads {
		names = append(names, name)
	}
	slices.Sort(names)
	require.ElementsMatch(t, asOfReadNames, names)
	const outside = 0
	inside := slots.Vote
	hide(true)
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
		"an as-of read reached the metadata store",
	)

}

// TestAsOfReadsFailClosedOutsideTheSetJournal requires as-of reads outside
// an attached set's journal to fail without consulting metadata.
func TestAsOfReadsFailClosedOutsideTheSetJournal(t *testing.T) {
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
			scenario := asOfJournalFloorScenario(t)
			reached, hide := scenario.HideStoreReads(asOfReadNames...)
			scenario.Proposals()
			read := asOfReadFuncs(t, scenario.LedgerDB())[name]
			hide(true)

			before := reached()
			require.ErrorIs(
				t, read(outside), ledger.ErrLedgerStateAsOfUnavailable,
			)
			require.Equal(t, before, reached(), "the read reached the metadata store")
		})
	}
}

// asOfReadFuncs returns each as-of LedgerDB read in asOfReadNames, keyed by
// name, as a function of the slot it reads at.
func asOfReadFuncs(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
) map[string]func(slot uint64) error {
	t.Helper()
	ctx := t.Context()
	stored, err := ldb.GetAllGovernanceProposals(ctx, nil)
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
