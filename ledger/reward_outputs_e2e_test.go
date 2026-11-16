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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var rewardDerivedReadNames = []string{
	"GetRewardPoolOutputs", "GetRewardAccountOutputs",
	"GetRewardAccountOutputsForEligibility", "GetSyncState",
}

// newRewardOutputsRun is newRewardBlocksRun over a ledger whose boundary into
// epoch 6 applies a real reward round, with the Leios validator configured as
// every node does.
func newRewardOutputsRun(
	t *testing.T,
	reads string,
	guard *statedrain.TripGuard,
) *rewardBlocksRun {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	var g metadata.StatementGuard
	if guard != nil {
		g = guard
	}
	scenario := ledger.NewRewardBlocksScenarioForTest(
		t, set, reads, g, ledger.RewardBlocksOptions{Round: true},
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	scenario.ConfigureLeiosValidator()
	r := &rewardBlocksRun{scenario: scenario, set: set}
	r.write()
	stakeUp(r)
	scenario.Tip(11)
	return r
}

func requireOutputs(t *testing.T, a ledger.RewardOutputsAnswer) {
	t.Helper()
	require.NoError(t, a.Err)
	require.Len(t, a.Pools, 1)
	require.Len(t, a.Accounts, 2)
	require.NotEmpty(t, a.Cursor)
}

// The set answers every read of the round's derived rows, with the store's
// reads of them failing, and its answer is the rows the store holds.
func TestRewardOutputsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, ledger.UtxoSetReadsSet, nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	want := s.StoreOutputs(3)
	requireOutputs(t, want)
	reached, _ := s.HideStoreReads(rewardDerivedReadNames...)
	got := s.Outputs(3)
	want.Rounds = nil
	got.Rounds = nil
	require.Equal(t, want, got)
	require.Zero(t, reached(), "a read reached the metadata store")
}

// In verify mode the store answers and the set's rows are compared with
// them.
func TestRewardOutputsMatchTheStoreInVerifyMode(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, ledger.UtxoSetReadsVerify, nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	s.Boundary()
	require.NoError(t, s.Outputs(3).Err)
	for _, result := range []string{
		"reward_pool_outputs", "reward_account_outputs",
		"reward_precompute_cursor",
	} {
		verified, mismatches := s.Computed(result)
		require.Positive(t, verified, result)
		require.Zero(t, mismatches, result)
	}
}

func TestRewardOutputsVerifyModeReportsADifference(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, ledger.UtxoSetReadsVerify, nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	s.OutputsAroundTheSet(3, false)
	_ = s.Outputs(3)
	_, mismatches := s.Computed("reward_pool_outputs")
	require.Positive(t, mismatches)
}

// A rollback to the round's captured slot keeps the precomputed outputs, and
// the replayed boundary credits the same rewards the first pass did.
func TestRewardOutputsAcrossARollbackAndReplay(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, ledger.UtxoSetReadsSet, nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	reached, _ := s.HideStoreReads(rewardDerivedReadNames...)
	owner, member := bytes.Repeat(
		[]byte{0x71},
		28,
	), bytes.Repeat(
		[]byte{0x72},
		28,
	)
	s.Boundary()
	first := s.Outputs(3)
	requireOutputs(t, first)
	require.Len(t, first.Rounds, 1)
	_, ownerPending := s.Balance(owner)
	_, memberPending := s.Balance(member)
	require.Equal(t, uint64(336), ownerPending)
	require.Equal(t, uint64(329), memberPending)

	s.RollbackBeforeBoundary()
	kept := s.Outputs(3)
	require.NoError(t, kept.Err)
	require.Empty(t, kept.Rounds, "the rollback removes the credited round")
	require.Equal(
		t,
		first.Accounts,
		kept.Accounts,
		"outputs captured at the target stay",
	)
	_, pending := s.Balance(owner)
	require.Zero(t, pending)

	s.Tip(11)
	s.Boundary()
	replay := s.Outputs(3)
	require.Equal(t, first, replay)
	_, ownerPending = s.Balance(owner)
	_, memberPending = s.Balance(member)
	require.Equal(t, uint64(336), ownerPending)
	require.Equal(t, uint64(329), memberPending)
	require.Zero(t, reached(), "a read reached the metadata store")
}

// A credential deregistered after its chunk ran, with a rollback between the
// precompute and the boundary, is found by the rollback counter: no
// certificate row and no restored account names it.
func TestRewardOutputsRecheckAfterARollbackFindsAnUnlistedCredential(
	t *testing.T,
) {
	t.Parallel()
	r := newRewardOutputsRun(t, ledger.UtxoSetReadsSet, nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	s.RollbackBeforeBoundary()
	s.DeregisterMember()
	s.Tip(11)
	s.Boundary()
	out := s.Outputs(3)
	require.NoError(t, out.Err)
	var spendable, unspendable int
	for _, o := range out.Accounts {
		if o.Spendable {
			spendable++
		} else {
			unspendable++
			require.Equal(t, "member", o.RewardType)
		}
	}
	require.Equal(t, 1, spendable)
	require.Equal(
		t,
		1,
		unspendable,
		"the deregistered member's reward is unspendable",
	)
	require.Len(t, out.Pools, 1)
	require.Equal(t, uint64(329), uint64(out.Pools[0].Unspendable))
	_, pending := s.Balance(bytes.Repeat([]byte{0x72}, 28))
	require.Zero(t, pending)
}

// Without a rollback no recheck of an unlisted credential is owed, so the
// deregistered member keeps its precomputed flag: the recheck follows the
// rollback counter, not every boundary.
func TestRewardOutputsRecheckIsSkippedWithoutARollback(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, ledger.UtxoSetReadsSet, nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	s.DeregisterMember()
	s.Boundary()
	for _, o := range s.Outputs(3).Accounts {
		require.True(t, o.Spendable, o.RewardType)
	}
}

func rewardOutputEntries(
	t *testing.T,
	src statedrain.Source,
) []statedrain.Entry {
	t.Helper()
	var entries []statedrain.Entry
	for _, e := range statedrain.Graduated(src) {
		if e.Family == nodesettings.FamilyRewardOutputs {
			entries = append(entries, e)
		}
	}
	require.Len(t, entries, 1)
	return entries
}

// The same run with the family drained, its tables and sync keys gone and the
// statement guard armed gives the same rewards as the undrained run, across a
// precompute, a boundary, a rollback and a replay, and no statement reaches a
// drained table.
func TestRewardOutputsDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, drain bool) (map[string]string, *statedrain.TripGuard, *rewardBlocksRun) {
		guard := statedrain.NewTripGuard()
		r := newRewardOutputsRun(t, ledger.UtxoSetReadsSet, guard)
		s := r.scenario
		out := map[string]string{}
		owner, member := bytes.Repeat(
			[]byte{0x71},
			28,
		), bytes.Repeat(
			[]byte{0x72},
			28,
		)
		if drain {
			src := rewardBlocksDrainSource{r: r}
			require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
				Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
				Guard: guard, VerifyBeforeDrain: true,
			}, rewardOutputEntries(t, src)))
		}
		require.NoError(t, s.PrecomputeRound())
		read := func(name string) {
			a := s.Outputs(3)
			require.NoError(t, a.Err, name)
			ob, op := s.Balance(owner)
			mb, mp := s.Balance(member)
			raw, err := json.Marshal(a)
			require.NoError(t, err, name)
			out[name] = string(raw) + fmt.Sprint(ob, op, mb, mp)
		}
		read("precompute")
		s.Boundary()
		read("boundary")
		s.RollbackBeforeBoundary()
		read("rollback")
		s.Tip(11)
		s.Boundary()
		read("replay")
		return out, guard, r
	}
	undrained, _, plain := run(t, false)
	drained, guard, r := run(t, true)
	require.Empty(t, guard.Trips(), "a drained table was touched")
	rawPlain, err := dbtest.RawSQLiteMetadata(t, plain.scenario.DB)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, r.scenario.DB)
	require.NoError(t, err)
	for _, table := range []string{
		"reward_pool_output", "reward_account_output", "reward_credit_round",
	} {
		require.Positive(
			t,
			tableRows(t, rawPlain, table),
			"undrained %s",
			table,
		)
		require.Zero(t, tableRows(t, raw, table), "drained %s", table)
	}
	mask, err := r.scenario.DB.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyRewardOutputs))
	require.Equal(t, undrained, drained)
	require.Equal(t, drained["boundary"], drained["replay"])
	require.Contains(t, drained["boundary"], "336")
}

// A set that differs from the metadata store refuses the drain and leaves
// the rows, whether the difference is in the derived pool outputs or in the
// account outputs.
func TestRewardOutputsVerifyMismatchRefusesTheDrain(t *testing.T) {
	t.Parallel()
	for _, accounts := range []bool{false, true} {
		t.Run(fmt.Sprintf("accounts=%t", accounts), func(t *testing.T) {
			t.Parallel()
			guard := statedrain.NewTripGuard()
			r := newRewardOutputsRun(t, ledger.UtxoSetReadsSet, guard)
			s := r.scenario
			require.NoError(t, s.PrecomputeRound())
			s.OutputsAroundTheSet(3, accounts)
			src := rewardBlocksDrainSource{r: r}
			err := statedrain.Run(t.Context(), statedrain.Env{
				Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
				Guard: guard, VerifyBeforeDrain: true,
			}, rewardOutputEntries(t, src))
			require.ErrorIs(t, err, statedrain.ErrGateRefused)
			mask, err := s.DB.DrainedFamilies(t.Context())
			require.NoError(t, err)
			require.False(t, mask.Has(nodesettings.FamilyRewardOutputs))
		})
	}
}

// With the family drained and the set serving nothing, the reward reads fail
// rather than answer from the emptied tables.
func TestRewardOutputsFailWhenTheDrainedSetServesNothing(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsOff, nil, false)
	require.NoError(t, r.scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyRewardOutputs,
	))
	r.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardOutputs),
	)
	a := r.scenario.Outputs(3)
	require.ErrorIs(t, a.Err, ledger.ErrDrainedFamilyUnserved)
}

// The boundary's prune drops an old epoch's derived rows from the set.
func TestRewardOutputsPruneReachesTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, ledger.UtxoSetReadsSet, nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	requireOutputs(t, s.Outputs(3))
	s.PruneBefore(4)
	a := s.Outputs(3)
	require.NoError(t, a.Err)
	require.Empty(t, a.Pools)
	require.Empty(t, a.Accounts)
	require.Empty(t, a.Cursor)
}
