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

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

func rewardsAt(
	t *testing.T,
	db ledgerdb.LedgerDB,
	refs []models.StakeCredentialRef,
	slot uint64,
) map[string]uint64 {
	t.Helper()
	accts, err := db.GetAccountsByCredentialAtSlot(t.Context(), refs, slot, nil)
	require.NoError(t, err)
	out := make(map[string]uint64, len(accts))
	for k, a := range accts {
		out[k] = uint64(a.Reward)
	}
	return out
}

// TestPinnedAccountRewardsCarryUnfoldedRewardOutputs requires the set's
// as-of account read to add the credited rounds' unfolded reward outputs, as
// the metadata store's does, before any fold, after a withdrawal fold and
// after the compaction.
func TestPinnedAccountRewardsCarryUnfoldedRewardOutputs(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, ledger.UtxoSetReadsSet)
	refs := []models.StakeCredentialRef{h.owner, h.deleg}
	h.saveRounds(t)
	h.applyRound(t, 11)
	h.applyRound(t, 12)
	ldb := h.scenario.LedgerDB()
	slots := []uint64{
		h.round(11).BoundarySlot - 1,
		h.round(11).BoundarySlot,
		h.round(12).BoundarySlot,
		h.round(12).BoundarySlot + 5,
	}
	check := func(label string) {
		t.Helper()
		for _, slot := range slots {
			want := rewardsAt(t, h.store, refs, slot)
			got := rewardsAt(t, ldb, refs, slot)
			require.Equal(t, want, got, "%s: rewards at slot %d", label, slot)
		}
	}
	check("unfolded")
	want := rewardsAt(t, h.store, refs, slots[3])
	require.Equal(t, uint64(77), want[h.deleg.MapKey()], "rounds 11 and 12 are credited")

	h.scenario.FoldForWithdrawal(h.deleg)
	check("after the withdrawal fold")
	h.scenario.CompactRewardCredits(1000)
	check("after the compaction")

	h.scenario.DrainFamilies(nodesettings.FamilyMask(0).
		With(nodesettings.FamilyAccounts).
		With(nodesettings.FamilyRewardOutputs))
	check("with the account and reward output rows drained")
}
