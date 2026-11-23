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
// as-of account read to add the credited rounds' unfolded reward outputs:
// the balance as of a slot is the same before any fold, after a withdrawal
// fold and after the compaction.
func TestPinnedAccountRewardsCarryUnfoldedRewardOutputs(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, "set")
	refs := []models.StakeCredentialRef{h.owner, h.deleg}
	h.saveRounds(t)
	h.applyRound(t, 11)
	h.applyRound(t, 12)
	ldb := h.scenario.LedgerDB()
	// Rounds 11 and 12 credit the delegator 7 and 70 and the owner 3 and 30
	// at their boundary slots.
	balances := func(owner, deleg uint64) map[string]uint64 {
		return map[string]uint64{
			h.owner.MapKey(): owner, h.deleg.MapKey(): deleg,
		}
	}
	want := []struct {
		slot uint64
		at   map[string]uint64
	}{
		{h.round(11).BoundarySlot - 1, balances(0, 0)},
		{h.round(11).BoundarySlot, balances(3, 7)},
		{h.round(12).BoundarySlot, balances(33, 77)},
		{h.round(12).BoundarySlot + 5, balances(33, 77)},
	}
	check := func(label string) {
		t.Helper()
		for _, w := range want {
			require.Equal(t, w.at, rewardsAt(t, ldb, refs, w.slot),
				"%s: rewards at slot %d", label, w.slot)
		}
	}
	check("unfolded")
	h.scenario.FoldForWithdrawal(h.deleg)
	check("after the withdrawal fold")
	h.scenario.CompactRewardCredits(1000)
	check("after the compaction")
}
