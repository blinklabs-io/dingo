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
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/stretchr/testify/require"
)

// TestGovernanceTallyFromTheLiveUtxoSet tallies the scenario's first
// proposal through the ledger's LedgerDB after each of its writes. Two DReps
// of 100 and 50 lovelace vote on it; the second is deregistered with its
// vote and votes again, so the stake the tally counts is that of the votes
// the set holds at each write.
func TestGovernanceTallyFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	d1 := bytes.Repeat([]byte{0xd1}, 28)
	d2 := bytes.Repeat([]byte{0xd2}, 28)
	drepState := &governance.DRepVotingState{
		Dreps: []*models.Drep{
			{CredentialTag: 0, Credential: d1},
			{CredentialTag: 0, Credential: d2},
		},
		Powers: map[string]uint64{
			models.NewStakeCredentialRef(0, d1).MapKey(): 100,
			models.NewStakeCredentialRef(0, d2).MapKey(): 50,
		},
	}
	voted := func() uint64 {
		t.Helper()
		stored, err := ldb.GetAllGovernanceProposals(ctx, nil)
		require.NoError(t, err)
		require.NotEmpty(t, stored)
		slices.SortFunc(stored, func(a, b *models.GovernanceProposal) int {
			return int(a.AddedSlot) - int(b.AddedSlot) //nolint:gosec // test slots are small
		})
		tally, err := governance.TallyProposal(ctx, &governance.TallyContext{
			DB: ldb, DRepState: drepState,
			SPOState:       &governance.SPOVotingState{},
			CommitteeState: &governance.CommitteeVotingState{},
		}, stored[0])
		require.NoError(t, err)
		require.Equal(t, stored[0].ID, tally.ProposalID)
		return tally.DRepYesStake + tally.DRepNoStake + tally.DRepAbstainStake
	}
	var perWrite []uint64
	scenario.OnProposalWrite(func(step int) {
		// The first proposal is voted on by write 3.
		if step >= 3 && step <= 8 {
			perWrite = append(perWrite, voted())
		}
	})
	slots := scenario.Proposals()
	require.Equal(t, d1, slots.D1)
	require.Equal(t, d2, slots.D2)
	require.NotEmpty(t, perWrite)
	require.Equal(t, uint64(150), perWrite[0], "both DReps have voted")
	require.Contains(t, perWrite, uint64(100),
		"the deregistered DRep's vote is not counted")
	require.Equal(t, uint64(150), voted(), "the DRep voted again")
}

// TestCommitteeLineageFromTheLiveUtxoSet reads the committee, its quorum,
// its hot key authorizations and the constitution through the ledger's
// LedgerDB after a committee is seated, authorizes, resigns, is updated and
// is removed by a no-confidence action, and after the rollback of all of it.
func TestCommitteeLineageFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	slots := scenario.Committee()

	seated, err := ldb.GetCommitteeMembers(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, seated, "the no-confidence action removed every member")
	quorum, err := ldb.GetCommitteeQuorum(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, quorum, "the no-confidence action cleared the quorum")
	ever, err := ldb.GetCommitteeMembersIncludeDeleted(ctx, nil)
	require.NoError(t, err)
	var colds []string
	for _, m := range ever {
		require.NotNil(t, m.DeletedSlot, "member %x", m.ColdCredHash)
		colds = append(colds, fmt.Sprintf("%x", m.ColdCredHash))
	}
	slices.Sort(colds)
	colds = slices.Compact(colds)
	want := []string{
		fmt.Sprintf("%x", slots.A), fmt.Sprintf("%x", slots.B),
		fmt.Sprintf("%x", slots.C),
	}
	slices.Sort(want)
	require.Equal(t, want, colds)

	constitution, err := ldb.GetConstitution(nil)
	require.NoError(t, err)
	require.NotNil(t, constitution)
	require.Equal(t, slots.Constitution, constitution.AddedSlot)

	// Each cold credential's latest authorization: A's is the one at
	// Reauth, B's the one at Auth.
	auths := func(since uint64) map[string]uint64 {
		t.Helper()
		rows, err := ldb.GetCommitteeHotAuthorizationsSince(ctx, since, nil)
		require.NoError(t, err)
		out := map[string]uint64{}
		for _, a := range rows {
			out[string(a.ColdCredential)] = a.AddedSlot
		}
		return out
	}
	require.Equal(t, map[string]uint64{
		string(slots.A): slots.Reauth, string(slots.B): slots.Auth,
	}, auths(0))
	require.Equal(t, map[string]uint64{string(slots.A): slots.Reauth},
		auths(slots.Reauth))
	require.Empty(t, auths(slots.Reauth+1))
	resigned, err := ldb.IsCommitteeMemberResigned(ctx, 0, slots.B, 0, nil)
	require.NoError(t, err)
	require.True(t, resigned, "B resigned")
	resigned, err = ldb.IsCommitteeMemberResigned(ctx, 0, slots.A, 0, nil)
	require.NoError(t, err)
	require.False(t, resigned, "A did not resign")

	scenario.RollbackBeforeBoundary()
	require.Empty(t, auths(slots.Auth),
		"the rollback undid every authorization")
	after, err := ldb.GetConstitution(nil)
	require.NoError(t, err)
	if after != nil {
		require.Less(t, after.AddedSlot, slots.Constitution)
	}
	quorum, err = ldb.GetCommitteeQuorum(ctx, nil)
	require.NoError(t, err)
	if slots.Base <= scenario.RollbackSlot() {
		require.Equal(t, big.NewRat(1, 2), quorum,
			"the rollback restored the first committee's quorum")
	}
}

// TestLastEnactedProposalFromTheLiveUtxoSet reads the last enacted proposal
// of a purpose through the ledger's LedgerDB before the scenario's proposal
// is enacted, after it, and after the rollback.
func TestLastEnactedProposalFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	ldb := scenario.LedgerDB()
	last := func(types ...uint8) *models.GovernanceProposal {
		t.Helper()
		got, err := ldb.GetLastEnactedGovernanceProposal(t.Context(), types, nil)
		require.NoError(t, err)
		return got
	}
	require.Nil(t, last(1), "nothing is enacted yet")
	slots := scenario.Proposals()
	enacted := last(1)
	require.NotNil(t, enacted)
	require.Equal(t, slots.P1, enacted.TxHash)
	require.NotNil(t, enacted.EnactedSlot)
	require.Equal(t, slots.Enact, *enacted.EnactedSlot)
	require.Nil(t, last(0, 2), "no proposal of another purpose is enacted")
	scenario.RollbackBeforeBoundary()
	require.Nil(t, last(1), "the rollback undid the enactment")
}
