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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
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
	g metadata.StatementGuard,
) *rewardBlocksRun {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
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
// reads of them failing: the one pool's output and the reward of its owner
// and of its member.
func TestRewardOutputsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, "set", nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	reached, _ := s.HideStoreReads(rewardDerivedReadNames...)
	requireOutputs(t, s.Outputs(3))
	require.Zero(t, reached(), "a read reached the metadata store")
}

// A rollback to the round's captured slot keeps the precomputed outputs, and
// the replayed boundary credits the same rewards the first pass did.
func TestRewardOutputsAcrossARollbackAndReplay(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, "set", nil)
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
	r := newRewardOutputsRun(t, "set", nil)
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
	r := newRewardOutputsRun(t, "set", nil)
	s := r.scenario
	require.NoError(t, s.PrecomputeRound())
	s.DeregisterMember()
	s.Boundary()
	for _, o := range s.Outputs(3).Accounts {
		require.True(t, o.Spendable, o.RewardType)
	}
}

// With the family drained and the set serving nothing, the reward reads fail
// rather than answer from the emptied tables.
func TestRewardOutputsFailWhenTheDrainedSetServesNothing(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, false)
	r.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardOutputs),
	)
	reached, hide := r.scenario.HideStoreReads(rewardDerivedReadNames...)
	a := r.scenario.Outputs(3)
	hide(false)
	require.NoError(t, a.Err)
	require.Empty(t, a.Pools)
	require.Empty(t, a.Accounts)
	require.Zero(t, reached(), "loaded empty reward outputs reached metadata")
}

// The boundary's prune drops an old epoch's derived rows from the set.
func TestRewardOutputsPruneReachesTheSet(t *testing.T) {
	t.Parallel()
	r := newRewardOutputsRun(t, "set", nil)
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
