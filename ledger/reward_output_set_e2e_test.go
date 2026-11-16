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
	"fmt"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var rewardOutputReadNames = []string{
	"HasAppliedRewardCreditRound", "GetPendingRewardCreditRounds",
	"GetPendingRewardAccountOutputsForCredential",
	"HasPendingRewardCreditRounds", "HasUnfoldedRewardCreditsThroughEpoch",
	"GetPostSnapshotRewardCredits",
}

// rewardRows renders every reward state read of db, and the live stake of
// the harness's credentials, row IDs aside.
func (h *rewardOutputHarness) rewardRows(
	t *testing.T,
	db ledgerdb.LedgerDB,
) []string {
	t.Helper()
	var out []string
	rounds, err := db.GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	out = append(out, fmt.Sprintf("rounds %v", rounds))
	has, err := db.HasPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	out = append(out, fmt.Sprintf("has pending %t", has))
	for _, epoch := range []uint64{11, 12, 13, 14} {
		applied, err := db.HasAppliedRewardCreditRound(epoch, nil)
		require.NoError(t, err)
		unfolded, err := db.HasUnfoldedRewardCreditsThroughEpoch(epoch, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("epoch %d applied %t unfolded %t",
			epoch, applied, unfolded))
	}
	for name, ref := range map[string]models.StakeCredentialRef{
		"owner": h.owner, "deleg": h.deleg,
	} {
		pending, err := db.GetPendingRewardAccountOutputsForCredential(
			ref.Tag, ref.Key, nil,
		)
		require.NoError(t, err)
		for _, o := range pending {
			c := *o
			c.ID = 0
			out = append(out, fmt.Sprintf("%s pending %+v", name, c))
		}
	}
	for _, slot := range []uint64{h.start, h.start + 3} {
		credits, err := db.GetPostSnapshotRewardCredits(slot, nil)
		require.NoError(t, err)
		for _, c := range credits {
			out = append(out, fmt.Sprintf("slot %d credit %+v", slot, *c))
		}
	}
	inputs, err := db.GetLiveStakeInputsForPools([][]byte{h.pool}, 0, nil)
	require.NoError(t, err)
	for _, in := range inputs {
		out = append(out, fmt.Sprintf("stake %d/%x %d",
			in.CredentialTag, in.StakingKey, in.Stake))
	}
	power, err := db.GetDRepVotingPowerByType(
		t.Context(), []uint64{2, 3}, 0, nil,
	)
	require.NoError(t, err)
	out = append(out, fmt.Sprintf("power %v", power))
	slices.Sort(out)
	return out
}

// requireRewardReadsMatchStore requires the ledger's answers to every reward
// state read to be the metadata store's.
func (h *rewardOutputHarness) requireRewardReadsMatchStore(
	t *testing.T,
	stage string,
) {
	t.Helper()
	require.Equal(t,
		h.rewardRows(t, h.store), h.rewardRows(t, h.scenario.LedgerDB()), stage)
}

// TestRewardOutputReadsFromTheLiveUtxoSet reads the reward state through the
// ledger's LedgerDB in set mode with the store's reward output and live stake
// reads failing, through post-snapshot credits, saved outputs, credited
// rounds, a withdrawal fold, a compaction and a rollback, and requires every
// answer to be the store's.
func TestRewardOutputReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, ledger.UtxoSetReadsSet)
	reached, _ := h.scenario.HideStoreReads(slices.Concat(
		rewardOutputReadNames, liveStakeReadNames,
	)...)
	h.requireRewardReadsMatchStore(t, "before any reward row")

	h.creditPostSnapshot(t)
	h.creditLater(t)
	h.requireRewardReadsMatchStore(t, "after post-snapshot credits")

	h.saveRounds(t)
	h.requireRewardReadsMatchStore(t, "after saving outputs")

	h.applyRound(t, 11)
	h.applyRound(t, 12)
	h.requireRewardReadsMatchStore(t, "after crediting rounds 11 and 12")
	require.Equal(t, uint64(20_000_086), h.liveStake(t, h.deleg),
		"live stake adds the pending credits")

	h.scenario.FoldForWithdrawal(h.deleg)
	h.requireRewardReadsMatchStore(t, "after the withdrawal fold")

	h.applyRound(t, 13)
	require.False(t, h.scenario.CompactRewardCredits(1000), "round 11 is due")
	require.True(t, h.scenario.CompactRewardCredits(1000), "then nothing is")
	h.requireRewardReadsMatchStore(t, "after the compaction")

	h.scenario.RollbackBeforeBoundary()
	h.requireRewardReadsMatchStore(t, "after the rollback")
	require.Zero(t, reached(), "a reward read reached the metadata store")
	verified, mismatches := h.scenario.RewardOutputReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

// TestRewardOutputReadsSeeTheirTransactionsStagedChangesInSetMode requires a
// pending output read in a write transaction to see an output that
// transaction saved under a credited round, with the store's reads failing.
func TestRewardOutputReadsSeeTheirTransactionsStagedChangesInSetMode(
	t *testing.T,
) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, ledger.UtxoSetReadsSet)
	reached, _ := h.scenario.HideStoreReads(rewardOutputReadNames...)
	ldb := h.scenario.LedgerDB()
	txn := ldb.Begin(t.Context(), true)
	defer txn.Release()
	round := h.round(11)
	require.NoError(t, ldb.AddAppliedRewardCreditRound(round, txn))
	require.NoError(t, ldb.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{
			h.output(h.deleg, 11, 7, h.base, round.BoundarySlot),
		}, txn))
	has, err := ldb.HasPendingRewardCreditRounds(txn)
	require.NoError(t, err)
	require.True(t, has)
	pending, err := ldb.GetPendingRewardAccountOutputsForCredential(
		h.deleg.Tag, h.deleg.Key, txn,
	)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, uint64(7), uint64(pending[0].Amount))
	applied, err := ldb.HasAppliedRewardCreditRound(11, txn)
	require.NoError(t, err)
	require.True(t, applied)
	require.Zero(t, reached(), "a reward read reached the metadata store")
}

// TestRewardOutputReadsStayOnTheStoreWhenTheSetLacksTheRows requires the
// reward reads to reach the metadata store while the set has not loaded its
// reward rows.
func TestRewardOutputReadsStayOnTheStoreWhenTheSetLacksTheRows(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t,
		func(s *utxoset.Set) ledger.UtxoSetWriter {
			return unloadedRewardSet{s}
		},
		ledger.UtxoSetReadsSet,
	)
	reached, _ := h.scenario.HideStoreReads(rewardOutputReadNames...)
	ldb := h.scenario.LedgerDB()
	_, err := ldb.HasAppliedRewardCreditRound(11, nil)
	require.Error(t, err)
	_, err = ldb.GetPendingRewardCreditRounds(nil)
	require.Error(t, err)
	_, err = ldb.GetPendingRewardAccountOutputsForCredential(
		h.deleg.Tag, h.deleg.Key, nil,
	)
	require.Error(t, err)
	_, err = ldb.HasPendingRewardCreditRounds(nil)
	require.Error(t, err)
	_, err = ldb.HasUnfoldedRewardCreditsThroughEpoch(11, nil)
	require.Error(t, err)
	_, err = ldb.GetPostSnapshotRewardCredits(h.start, nil)
	require.Error(t, err)
	require.Equal(t, int64(6), reached(), "every read reached the store")
}
