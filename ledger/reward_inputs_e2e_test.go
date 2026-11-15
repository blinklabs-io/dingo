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
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var rewardInputReadNames = []string{
	"GetRewardAdaPots", "GetRewardSnapshot", "GetRewardSeedFailure",
	"GetRewardPoolInputs", "GetRewardStakeInputs",
	"GetRewardStakeInputsInPoolKeyHashRange", "CountEpochPoolBlocks",
	"CountPoolBlocksInSlotRange", "MergeImportedPoolBlockCounts",
}

// stakeUp gives the harness's two stake credentials, which it created in the
// metadata store only, the same stake in the set.
func stakeUp(r *rewardBlocksRun) {
	r.scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	r.scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
}

func requireCaptured(t *testing.T, a ledger.RewardInputsAnswer) {
	t.Helper()
	require.NoError(t, a.Err)
	require.NotNil(t, a.Pots, "the boundary stores the epoch's pots")
	require.NotNil(t, a.Snapshot, "the boundary captures the snapshot")
	require.True(t, a.Snapshot.Authoritative)
	require.NotEmpty(t, a.Pools)
	require.NotEmpty(t, a.Stakes)
}

// After a boundary the set answers every read of the epoch's reward inputs,
// and its answer is the rows the metadata store holds, with the store's
// reads of them failing.
func TestRewardInputsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	s.Boundary()
	s.WaitDeferred()
	want := s.StoreInputs(6)
	requireCaptured(t, want)
	reached, _ := s.HideStoreReads(rewardInputReadNames...)
	got := s.Inputs(6)
	require.Equal(t, want, got)
	require.Zero(t, reached(), "a read reached the metadata store")
	require.Equal(t, s.StoreInputs(5), s.Inputs(5))
}

// In verify mode the store answers and the set's rows are compared with
// them: a boundary leaves the two equal, and a row written around the set is
// a difference.
func TestRewardInputsMatchTheStoreInVerifyMode(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsVerify, nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	s.Boundary()
	s.WaitDeferred()
	requireCaptured(t, s.Inputs(6))
	for _, result := range []string{
		"reward_ada_pots", "reward_snapshot", "reward_pool_inputs",
		"reward_stake_inputs", "reward_seed_failure",
	} {
		verified, mismatches := s.Computed(result)
		require.Positive(t, verified, result)
		require.Zero(t, mismatches, result)
	}
}

func TestRewardInputsVerifyModeReportsADifference(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsVerify, nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	s.Boundary()
	s.WaitDeferred()
	s.InputsAroundTheSet(6)
	_ = s.Inputs(6)
	for _, result := range []string{
		"reward_ada_pots", "reward_pool_inputs", "reward_stake_inputs",
	} {
		_, mismatches := s.Computed(result)
		require.Positive(t, mismatches, result)
	}
}

// A rollback across the boundary leaves the set with none of the epoch's
// inputs, as the store has none, and the replayed boundary writes the rows
// the first pass did.
func TestRewardInputsAcrossABoundaryRollbackAndReplay(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	reached, _ := s.HideStoreReads(rewardInputReadNames[:7]...)
	s.Boundary()
	s.WaitDeferred()
	first := s.Inputs(6)
	requireCaptured(t, first)

	s.RollbackBeforeBoundary()
	s.WaitDeferred()
	gone := s.Inputs(6)
	require.NoError(t, gone.Err)
	require.Nil(t, gone.Pots)
	require.Nil(t, gone.Snapshot)
	require.Empty(t, gone.Pools)
	require.Empty(t, gone.Stakes, "one tombstone drops the epoch's stake rows")
	require.Equal(t, s.StoreInputs(6), gone)

	s.Block(11, s.PoolB)
	s.Fee(11, 25)
	s.Boundary()
	s.WaitDeferred()
	require.Equal(t, first, s.Inputs(6))
	require.Zero(t, reached(), "a read reached the metadata store")
}

func rewardInputEntries(
	t *testing.T,
	src statedrain.Source,
) []statedrain.Entry {
	t.Helper()
	var entries []statedrain.Entry
	for _, e := range statedrain.Graduated(src) {
		if e.Family == nodesettings.FamilyRewardInputs {
			entries = append(entries, e)
		}
	}
	require.Len(t, entries, 1)
	return entries
}

// The same run with the family drained, its tables and pending key gone and
// the statement guard armed gives the same answers as the undrained run,
// across a boundary, a rollback and a replay, and the guard sees no statement
// on a drained table.
func TestRewardInputsDrainMatchesAnUndrainedRun(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, drain bool) (map[string]string, *statedrain.TripGuard, *rewardBlocksRun) {
		guard := statedrain.NewTripGuard()
		r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, guard, false)
		s := r.scenario
		out := map[string]string{}
		r.write()
		stakeUp(r)
		s.Boundary()
		s.WaitDeferred()
		if drain {
			src := rewardBlocksDrainSource{r: r}
			require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
				Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
				Guard: guard, VerifyBeforeDrain: true,
			}, rewardInputEntries(t, src)))
		}
		read := func(name string) {
			a := s.Inputs(6)
			require.NoError(t, a.Err, name)
			raw, err := json.Marshal(a)
			require.NoError(t, err, name)
			out[name] = string(raw)
		}
		read("boundary")
		s.RollbackBeforeBoundary()
		s.WaitDeferred()
		read("rollback")
		s.Block(11, s.PoolB)
		s.Fee(11, 25)
		s.Boundary()
		s.WaitDeferred()
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
		"reward_ada_pots", "reward_snapshot", "reward_pool_input",
		"reward_stake_input",
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
	require.True(t, mask.Has(nodesettings.FamilyRewardInputs))
	require.Equal(t, undrained, drained)
	require.NotContains(t, drained["boundary"], `"Snapshot":null`)
	require.Contains(t, drained["rollback"], `"Snapshot":null`)
	require.Equal(t, drained["boundary"], drained["replay"])
}

// A set that differs from the metadata store refuses the drain and leaves
// the rows.
func TestRewardInputsVerifyMismatchRefusesTheDrain(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, guard, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	s.Boundary()
	s.WaitDeferred()
	s.InputsAroundTheSet(6)
	src := rewardBlocksDrainSource{r: r}
	err := statedrain.Run(t.Context(), statedrain.Env{
		Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
		Guard: guard, VerifyBeforeDrain: true,
	}, rewardInputEntries(t, src))
	require.ErrorIs(t, err, statedrain.ErrGateRefused)
	mask, err := s.DB.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.False(t, mask.Has(nodesettings.FamilyRewardInputs))
	raw, err := dbtest.RawSQLiteMetadata(t, s.DB)
	require.NoError(t, err)
	require.Positive(t, tableRows(t, raw, "reward_stake_input"))
}

// With the family drained and the set holding no rows for an epoch, the
// reward round's reads fail rather than answer from the emptied tables, and
// a fallback snapshot capture is refused.
func TestRewardInputsFailWhenTheDrainedSetServesNothing(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsOff, nil, false)
	require.NoError(t, r.scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyRewardInputs,
	))
	r.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardInputs),
	)
	a := r.scenario.Inputs(6)
	require.ErrorIs(t, a.Err, ledger.ErrDrainedFamilyUnserved)
}

// The boundary capture resolves each pool's reward-account auto-vote and
// reads its registration from the set when the accounts and pool families are
// drained too, and no statement reaches a drained table.
func TestRewardCaptureRunsWithItsInputFamiliesDrained(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, guard, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	src := rewardBlocksDrainSource{r: r}
	want := map[nodesettings.Family]bool{
		nodesettings.FamilyAccounts:     true,
		nodesettings.FamilyPoolsHistory: true,
	}
	var entries []statedrain.Entry
	for _, e := range append(
		statedrain.Graduated(src), statedrain.Staged(src)...,
	) {
		if want[e.Family] {
			entries = append(entries, e)
		}
	}
	require.Len(t, entries, len(want))
	require.NoError(t, statedrain.Run(t.Context(), statedrain.Env{
		Store: drainStore{s.DB, s.DrainFamilies}, Set: src,
		Guard: guard, VerifyBeforeDrain: true,
	}, entries))
	s.Boundary()
	s.WaitDeferred()
	require.Empty(t, guard.Trips(), "a drained table was touched")
	a := s.Inputs(6)
	requireCaptured(t, a)
}

// The boundary's prune drops an old epoch's stake inputs from the set as it
// does from the store, and leaves its pots, snapshot and pool inputs.
func TestRewardStakeInputPruneReachesTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsSet, nil, false)
	s := r.scenario
	r.write()
	stakeUp(r)
	s.Boundary()
	s.WaitDeferred()
	before := s.Inputs(6)
	requireCaptured(t, before)
	s.PruneBefore(7)
	after := s.Inputs(6)
	require.NoError(t, after.Err)
	require.Empty(t, after.Stakes)
	require.Equal(t, before.Pots, after.Pots)
	require.Equal(t, before.Snapshot, after.Snapshot)
	require.Equal(t, before.Pools, after.Pools)
	require.Equal(t, s.StoreInputs(6), after)
}
