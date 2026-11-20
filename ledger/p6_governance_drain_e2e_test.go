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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// governanceTables are the SQL tables of the governance family.
var governanceTables = []string{
	"governance_proposal", "governance_proposal_order",
	"governance_proposal_drop", "governance_proposal_ratification_history",
	"governance_vote", "governance_vote_history",
}

// govEntries returns the governance drain entry over a harness.
func govEntries(
	set *utxoset.Set,
	db *database.Database,
) []statedrain.Entry {
	src := drainTestSource{set: set, db: db}
	var entries []statedrain.Entry
	for _, e := range statedrain.Graduated(src) {
		if e.Family == nodesettings.FamilyGovernance {
			entries = append(entries, e)
		}
	}
	return entries
}

// renderGovernance renders every proposal and vote read the LedgerDB serves,
// row IDs included, so a run that allocates different IDs or answers a read
// differently reads differently.
func renderGovernance(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	epoch uint64,
	slots ...uint64,
) govSnapshot {
	t.Helper()
	ctx := t.Context()
	var out []string
	add := func(name string, ps []*models.GovernanceProposal, err error) {
		require.NoError(t, err, name)
		out = append(out, name)
		out = append(out, proposalRows(ps, false)...)
	}
	ps, err := ldb.GetGovernanceProposalSet(ctx, nil)
	add("set", ps, err)
	ps, err = ldb.GetRatifiedGovernanceProposals(ctx, nil)
	add("ratified", ps, err)
	for _, e := range []uint64{epoch, epoch + 1, epoch + 6, epoch + 7} {
		ps, err = ldb.GetActiveGovernanceProposals(ctx, e, nil)
		add(fmt.Sprintf("active %d", e), ps, err)
		ps, err = ldb.GetExpiringGovernanceProposals(ctx, e, nil)
		add(fmt.Sprintf("expiring %d", e), ps, err)
		ps, err = ldb.GetExpiredAwaitingDropGovernanceProposals(ctx, e, nil)
		add(fmt.Sprintf("awaiting drop %d", e), ps, err)
		ps, err = ldb.GetGovernanceProposalsRefundedAt(e, nil)
		add(fmt.Sprintf("refunded %d", e), ps, err)
		for _, s := range slots {
			ps, err = ldb.GetExpiredGovernanceProposalsAt(ctx, e, s, nil)
			add(fmt.Sprintf("expired %d/%d", e, s), ps, err)
			ps, err = ldb.GetDroppedGovernanceProposalsAt(ctx, e, s, nil)
			add(fmt.Sprintf("dropped %d/%d", e, s), ps, err)
			ps, err = ldb.GetEnactedGovernanceProposalsAt(ctx, e, s, nil)
			add(fmt.Sprintf("enacted %d/%d", e, s), ps, err)
		}
	}
	for _, s := range slots {
		ps, err = ldb.GetGovernanceProposalSetAtSlot(ctx, s, nil)
		add(fmt.Sprintf("set at %d", s), ps, err)
	}
	for _, purpose := range [][]uint8{{0}, {1}, {6}, {1, 6}} {
		p, err := ldb.GetLastEnactedGovernanceProposal(ctx, purpose, nil)
		require.NoError(t, err)
		out = append(
			out,
			fmt.Sprintf("last enacted %v", purpose),
			proposalRow(p, false),
		)
	}
	all, err := ldb.GetAllGovernanceProposals(ctx, nil)
	require.NoError(t, err)
	slices.SortFunc(all, func(a, b *models.GovernanceProposal) int {
		return int(a.ID) - int(b.ID)
	})
	add("all", all, nil)
	for _, p := range all {
		got, err := ldb.GetGovernanceProposal(ctx, p.TxHash, p.ActionIndex, nil)
		if p.DeletedSlot != nil {
			require.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
			continue
		}
		require.NoError(t, err)
		out = append(out, "proposal "+proposalRow(got, false))
		votes, err := ldb.GetGovernanceVotes(ctx, p.ID, nil)
		require.NoError(t, err)
		slices.SortFunc(votes, func(a, b *models.GovernanceVote) int {
			return int(a.ID) - int(b.ID)
		})
		for _, v := range votes {
			out = append(out, fmt.Sprintf("votes on %d: %s", p.ID, voteRow(v)))
		}
		for _, s := range slots {
			at, err := ldb.GetGovernanceVotesAtSlot(ctx, p.ID, s, nil)
			if err != nil {
				out = append(
					out,
					fmt.Sprintf("votes on %d at %d: %v", p.ID, s, err),
				)
				continue
			}
			for _, v := range at {
				out = append(
					out,
					fmt.Sprintf(
						"votes on %d at %d: id=%d %d/%x vote=%d added=%d",
						p.ID,
						s,
						v.ID,
						v.VoterType,
						v.VoterCredential,
						v.Vote,
						v.AddedSlot,
					),
				)
			}
		}
	}
	refs, err := ldb.GetAllGovernanceVotes(ctx, nil)
	require.NoError(t, err)
	var voteRows []string
	for _, r := range refs {
		voteRows = append(
			voteRows,
			fmt.Sprintf("%x#%d %s", r.TxHash, r.ActionIndex, voteRow(&r.Vote)),
		)
	}
	slices.Sort(voteRows)
	snap := govSnapshot{
		proposals: map[string]uint{}, votes: map[string]uint{},
	}
	names := map[uint]string{}
	for _, p := range all {
		key := fmt.Sprintf("%x#%d", p.TxHash[:4], p.ActionIndex)
		snap.proposals[key] = p.ID
		names[p.ID] = key
	}
	for _, r := range refs {
		key := fmt.Sprintf(
			"%x#%d/%d/%d/%x",
			r.TxHash[:4],
			r.ActionIndex,
			r.Vote.VoterType,
			r.Vote.VoterCredentialTag,
			r.Vote.VoterCredential[:2],
		)
		snap.votes[key] = r.Vote.ID
	}
	snap.lines = canonicalGovernance(
		append(append(out, "all votes"), voteRows...), names,
	)
	return snap
}

// govSnapshot is a render with the row IDs taken out, and the IDs by row key.
// A node that drains allocates the IDs of the rows it writes afterwards
// itself, densely above the highest one, where the metadata store also
// numbers the upserts that found their row; the IDs of rows that exist
// before the drain are the store's and compare equal.
type govSnapshot struct {
	lines     []string
	proposals map[string]uint
	votes     map[string]uint
}

var (
	proposalIDPattern = regexp.MustCompile(`\bid=\d+ ([0-9a-f]{64}#)`)
	voteIDPattern     = regexp.MustCompile(`\bid=\d+ proposal=(\d+) `)
	votesOnPattern    = regexp.MustCompile(`votes on (\d+)`)
	voteAtIDPattern   = regexp.MustCompile(`: id=\d+ (\d)/`)
)

// canonicalGovernance replaces every row ID in lines by the key of its row,
// and names each proposal by its ID as names gives it.
func canonicalGovernance(lines []string, names map[uint]string) []string {
	name := func(id string) string {
		n, _ := strconv.Atoi(id)
		return names[uint(n)]
	}
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		l = proposalIDPattern.ReplaceAllString(l, "id=_ $1")
		l = voteIDPattern.ReplaceAllStringFunc(l, func(m string) string {
			return "id=_ proposal=" + name(
				voteIDPattern.FindStringSubmatch(m)[1],
			) + " "
		})
		l = votesOnPattern.ReplaceAllStringFunc(l, func(m string) string {
			return "votes on " + name(votesOnPattern.FindStringSubmatch(m)[1])
		})
		l = voteAtIDPattern.ReplaceAllString(l, ": id=_ $1/")
		out = append(out, l)
	}
	return out
}

// govRun is what p6GovernanceRun observed.
type govRun struct {
	reads  map[string]govSnapshot
	guard  *statedrain.TripGuard
	db     *database.Database
	set    *utxoset.Set
	err    error
	drains int
}

// governanceScript is the writes of one P6 run past the proposal scenario:
// proposals and votes of every voter type, a DRep whose votes are deleted
// and who votes again, and the lifecycle of the new proposals.
type governanceScript struct {
	p3, p4, p5 []byte
	slots      ledger.GovernanceSlots
}

func (s *governanceScript) proposal(
	hash []byte,
	slot uint64,
	index uint32,
	epoch uint64,
) *models.GovernanceProposal {
	return &models.GovernanceProposal{
		TxHash: hash, ActionType: 6, ProposedEpoch: epoch,
		ExpiresEpoch: epoch + 5, AnchorURL: "https://example.test/info",
		AnchorHash: bytes.Repeat([]byte{9}, 32), Deposit: 2000,
		ReturnAddress: bytes.Repeat([]byte{0xe1}, 29),
		GovActionCbor: []byte{0x06}, AddedSlot: slot, TxIndex: &index,
	}
}

func govVote(
	id uint,
	voterType uint8,
	tag uint8,
	voter []byte,
	choice uint8,
	slot uint64,
) *models.GovernanceVote {
	return &models.GovernanceVote{
		ProposalID: id, VoterType: voterType, VoterCredentialTag: tag,
		VoterCredential: voter, Vote: choice, AddedSlot: slot,
	}
}

func commit(t *testing.T, ldb ledgerdb.LedgerDB, fn func(ledgerdb.Txn)) {
	t.Helper()
	txn := ldb.Begin(t.Context(), true)
	defer txn.Release()
	fn(txn)
	require.NoError(t, txn.Commit())
}

// writeNewProposals submits proposals p3 and p4, gets votes of every voter
// type on p3, deletes and restores a DRep's votes, ratifies and enacts p3
// and expires and drops p4.
func (s *governanceScript) writeNewProposals(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	base uint64,
) {
	t.Helper()
	ctx := t.Context()
	epoch := s.slots.Epoch
	u64 := func(v uint64) *uint64 { return &v }
	var p3, p4 *models.GovernanceProposal
	commit(t, ldb, func(txn ledgerdb.Txn) {
		p3 = s.proposal(s.p3, base+1, 0, epoch)
		require.NoError(t, ldb.SetGovernanceProposal(ctx, p3, txn))
		p4 = s.proposal(s.p4, base+1, 1, epoch)
		require.NoError(t, ldb.SetGovernanceProposal(ctx, p4, txn))
		require.NotZero(t, p3.ID)
		require.NotEqual(t, p3.ID, p4.ID)
	})
	drep := bytes.Repeat([]byte{0xd3}, 28)
	commit(t, ldb, func(txn ledgerdb.Txn) {
		for _, v := range []*models.GovernanceVote{
			govVote(p3.ID, models.VoterTypeDRep, 0, drep, models.VoteYes, base+2),
			govVote(p3.ID, models.VoterTypeDRep, 1, drep, models.VoteNo, base+2),
			govVote(p3.ID, models.VoterTypeSPO, 0, bytes.Repeat([]byte{0x51}, 28), models.VoteNo, base+2),
			govVote(p3.ID, models.VoterTypeCC, 0, bytes.Repeat([]byte{0xcc}, 28), models.VoteYes, base+2),
			govVote(p3.ID, models.VoterTypeCC, 1, bytes.Repeat([]byte{0xcd}, 28), models.VoteAbstain, base+2),
			govVote(p4.ID, models.VoterTypeSPO, 0, bytes.Repeat([]byte{0x52}, 28), models.VoteYes, base+2),
			govVote(p4.ID, models.VoterTypeDRep, 0, drep, models.VoteAbstain, base+2),
		} {
			require.NoError(t, ldb.SetGovernanceVote(ctx, v, txn))
			require.NotZero(t, v.ID)
		}
	})
	commit(t, ldb, func(txn ledgerdb.Txn) {
		n, err := ldb.DeleteGovernanceVotesForDrep(
			ctx,
			0,
			drep,
			epoch,
			base+3,
			txn,
		)
		require.NoError(t, err)
		require.Equal(t, 2, n, "the DRep's key-hash votes on both proposals")
	})
	commit(t, ldb, func(txn ledgerdb.Txn) {
		require.NoError(t, ldb.SetGovernanceVote(
			ctx,
			govVote(
				p3.ID,
				models.VoterTypeDRep,
				0,
				drep,
				models.VoteNo,
				base+4,
			),
			txn,
		))
	})
	commit(t, ldb, func(txn ledgerdb.Txn) {
		p := s.proposal(s.p3, base+1, 0, epoch)
		p.RatifiedEpoch, p.RatifiedSlot = u64(epoch), u64(base+5)
		require.NoError(t, ldb.SetGovernanceProposal(ctx, p, txn))
		q := s.proposal(s.p4, base+1, 1, epoch)
		q.ExpiredEpoch, q.ExpiredSlot = u64(epoch), u64(base+5)
		require.NoError(t, ldb.SetGovernanceProposal(ctx, q, txn))
	})
	commit(t, ldb, func(txn ledgerdb.Txn) {
		p := s.proposal(s.p3, base+1, 0, epoch)
		p.RatifiedEpoch, p.RatifiedSlot = u64(epoch), u64(base+5)
		p.EnactedEpoch, p.EnactedSlot = u64(epoch), u64(base+6)
		require.NoError(t, ldb.SetGovernanceProposal(ctx, p, txn))
		q := s.proposal(s.p4, base+1, 1, epoch)
		q.DroppedEpoch, q.DroppedSlot = u64(epoch), u64(base+6)
		require.NoError(t, ldb.SetGovernanceProposal(ctx, q, txn))
	})
}

// p6GovernanceRun runs the proposal scenario and the governance script,
// draining the family after the scenario's drainAfter-th write when drain is
// set, and reads everything the LedgerDB serves after each phase.
func p6GovernanceRun(
	t *testing.T,
	drain bool,
	drainAfter int,
	corrupt func(*database.Database),
) govRun {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
		t, set, guard,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	run := govRun{
		reads: map[string]govSnapshot{}, guard: guard, db: scenario.DB, set: set,
	}
	ldb := scenario.LedgerDB()
	var probe []uint64
	phase := func(name string, epoch uint64) {
		run.reads[name] = renderGovernance(t, ldb, epoch, probe...)
	}
	drainNow := func() {
		if corrupt != nil {
			corrupt(scenario.DB)
		}
		run.err = statedrain.Run(t.Context(), statedrain.Env{
			Store: drainStore{scenario.DB, scenario.DrainFamilies},
			Set:   drainTestSource{set: set, db: scenario.DB}, Guard: guard,
		}, govEntries(set, scenario.DB))
		run.drains++
	}
	var scenarioEpoch uint64
	scenario.OnProposalWrite(func(step int) {
		if drain && step == drainAfter && run.err == nil {
			drainNow()
		}
	})
	slots := scenario.Proposals()
	scenarioEpoch = slots.Epoch
	if run.err != nil {
		return run
	}
	for _, s := range []uint64{
		slots.Base, slots.Submit, slots.Vote, slots.Ratify, slots.Deregister,
		slots.Enact,
	} {
		probe = append(probe, s, s+1)
	}
	phase("scenario", scenarioEpoch)

	script := &governanceScript{
		p3:    bytes.Repeat([]byte{0xa3}, 32),
		p4:    bytes.Repeat([]byte{0xa4}, 32),
		p5:    bytes.Repeat([]byte{0xa5}, 32),
		slots: slots,
	}
	// An abandoned write transaction allocates IDs it never commits.
	scenario.ProposalsInsideTxn(slots.Enact + 20)
	script.writeNewProposals(t, ldb, slots.Enact+30)
	probe = append(probe, slots.Enact+31, slots.Enact+33, slots.Enact+35)
	phase("script", scenarioEpoch)

	scenario.RollbackBeforeBoundary()
	phase("rollback", scenarioEpoch)

	// Replay the rolled back writes: the proposals come back with new IDs.
	script.writeNewProposals(t, ldb, slots.Enact+30)
	commit(t, ldb, func(txn ledgerdb.Txn) {
		p := script.proposal(script.p5, slots.Enact+40, 0, scenarioEpoch)
		require.NoError(t, ldb.SetGovernanceProposal(t.Context(), p, txn))
		require.NoError(t, ldb.SetGovernanceVote(
			t.Context(),
			govVote(
				p.ID,
				models.VoterTypeDRep,
				0,
				slots.D1,
				models.VoteYes,
				slots.Enact+41,
			),
			txn,
		))
	})
	probe = append(probe, slots.Enact+40, slots.Enact+41)
	phase("replay", scenarioEpoch)
	return run
}

func requireNoGovernanceRows(t *testing.T, db *database.Database) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	for _, table := range governanceTables {
		require.Zero(t, tableRows(t, raw, table), "drained %s", table)
	}
}

// A drained governance family answers the same reads as an undrained one,
// row IDs included, and the metadata store keeps none of its rows or writes
// any new ones.
func TestGovernanceDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	undrained := p6GovernanceRun(t, false, 0, nil)
	require.NoError(t, undrained.err)
	raw, err := dbtest.RawSQLiteMetadata(t, undrained.db)
	require.NoError(t, err)
	for _, table := range []string{
		"governance_proposal", "governance_proposal_order",
		"governance_proposal_drop", "governance_proposal_ratification_history",
		"governance_vote", "governance_vote_history",
	} {
		require.Positive(t, tableRows(t, raw, table), "undrained %s", table)
	}
	// Draining after the sixth write drains the family while the SQL rows are
	// mid-lifecycle, and the rest of the run is drained.
	drained := p6GovernanceRun(t, true, 6, nil)
	require.NoError(t, drained.err)
	require.Equal(t, 1, drained.drains)
	require.Empty(
		t,
		drained.guard.Trips(),
		"a drained table was read or written",
	)
	requireNoGovernanceRows(t, drained.db)
	mask, err := drained.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyGovernance))
	for name, want := range undrained.reads {
		require.NotEmpty(t, want.lines, name)
		require.Equal(t, want.lines, drained.reads[name].lines, name)
	}
	require.Len(t, drained.reads, len(undrained.reads))

	// Rows that exist when the family drains keep the IDs the store gave.
	before := undrained.reads["scenario"]
	after := drained.reads["scenario"]
	for _, key := range []string{"91919191#0", "92929292#0"} {
		require.NotZero(t, before.proposals[key], key)
		require.Equal(t, before.proposals[key], after.proposals[key], key)
	}
	for key, id := range before.votes {
		if strings.HasPrefix(key, "91919191#0/1/") {
			require.Equal(t, id, after.votes[key], key)
		}
	}
	// Rows written after it take set-allocated IDs: every one is unique, and
	// the replay of a rolled back proposal gets a new, higher one.
	for name, snap := range drained.reads {
		seen := map[uint]string{}
		for key, id := range snap.proposals {
			require.NotZero(t, id, name+" "+key)
			require.NotContains(t, seen, id, name+" "+key+" shares an ID")
			seen[id] = key
		}
		seenVotes := map[uint]string{}
		for key, id := range snap.votes {
			require.NotZero(t, id, name+" "+key)
			require.NotContains(t, seenVotes, id, name+" "+key+" shares an ID")
			seenVotes[id] = key
		}
	}
	for _, key := range []string{"a3a3a3a3#0", "a4a4a4a4#0"} {
		require.Greater(
			t, drained.reads["replay"].proposals[key],
			drained.reads["script"].proposals[key], key,
		)
	}
	nextProposal, nextVote, err := drained.set.NextGovernanceIDs()
	require.NoError(t, err)
	for _, id := range drained.reads["replay"].proposals {
		require.Less(t, uint64(id), nextProposal)
	}
	for _, id := range drained.reads["replay"].votes {
		require.Less(t, uint64(id), nextVote)
	}
}

// With the family drained and the set not holding it, the LedgerDB fails
// every read and write rather than answer from the empty SQL tables.
func TestDrainedGovernanceReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// The set never loads the metadata store's rows.
	require.NoError(t, scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyGovernance,
	))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyGovernance),
	)
	ctx := t.Context()
	ldb := scenario.LedgerDB()
	hash := bytes.Repeat([]byte{0x77}, 32)
	_, err = ldb.GetGovernanceProposal(ctx, hash, 0, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetGovernanceProposalSet(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetActiveGovernanceProposals(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetRatifiedGovernanceProposals(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetLastEnactedGovernanceProposal(ctx, []uint8{1}, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetGovernanceVotes(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetAllGovernanceProposals(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetAllGovernanceVotes(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetGovernanceProposalsRefundedAt(1, nil)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.GetGovernanceVotesAtSlot(ctx, 1, 10, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable)
	txn := ldb.Begin(ctx, true)
	defer txn.Release()
	require.ErrorIs(
		t,
		ldb.SetGovernanceProposal(ctx, &models.GovernanceProposal{
			TxHash: hash, AnchorURL: "u",
		}, txn),
		ledger.ErrDrainedFamilyUnserved,
	)
	require.ErrorIs(t, ldb.SetGovernanceVote(ctx, &models.GovernanceVote{
		ProposalID: 1, VoterCredential: hash[:28],
	}, txn), ledger.ErrDrainedFamilyUnserved)
	require.ErrorIs(t, ldb.ClearGovernanceProposalRatification(
		ctx, hash, 0, 5, txn,
	), ledger.ErrDrainedFamilyUnserved)
	_, err = ldb.DeleteGovernanceVotesForDrep(ctx, 0, hash[:28], 1, 5, txn)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}

// A read transaction pinned before a later commit, as the deferred
// ratification job pins its own, reads the votes of its snapshot from the set
// once the family is drained, and the drained tables are never read.
func TestDrainedGovernancePinnedReadsSeeTheirSnapshot(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	guard := statedrain.NewTripGuard()
	scenario := ledger.NewGuardedBoundaryEpochStateScenarioForTest(
		t, set, guard,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
		Store: drainStore{scenario.DB, scenario.DrainFamilies},
		Set:   drainTestSource{set: set, db: scenario.DB}, Guard: guard,
	}, govEntries(set, scenario.DB)))

	require.Empty(t, guard.Trips(), "a drained table was read or written")
	requireNoGovernanceRows(t, scenario.DB)
}
