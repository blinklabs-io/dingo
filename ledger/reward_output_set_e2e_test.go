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
	"testing"

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

// TestRewardOutputReadsSeeTheirTransactionsStagedChangesInSetMode requires a
// pending output read in a write transaction to see an output that
// transaction saved under a credited round, with the store's reads failing.
func TestRewardOutputReadsSeeTheirTransactionsStagedChangesInSetMode(
	t *testing.T,
) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, "set")
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

// TestRewardOutputReadsFailWhenTheSetLacksTheRows requires reward reads
// without set rows to fail rather than mix in metadata reads.
func TestRewardOutputReadsFailWhenTheSetLacksTheRows(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t,
		func(s *utxoset.Set) ledger.UtxoSetWriter {
			return unloadedRewardSet{s}
		},
		"set",
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
	require.Zero(t, reached(), "every read uses only the set")
}
