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
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// creditColumns are the columns of the reward journal the writer applies.
var creditColumns = []string{
	"staking_key", "credential_tag", "tx_hash", "amount", "added_slot",
	"post_snapshot",
}

func creditRows(rows metadata.FactRows) metadata.FactRows {
	return projectFactRows(rows, creditColumns)
}

// The live UTxO set exports the reward credits the ledger journaled: each
// source once at its slot, the post-snapshot ones flagged, and a credit written
// later under an older slot follows the position a reader holds.
func TestSetExportsTheRewardCreditJournal(t *testing.T) {
	t.Parallel()
	var h0 *setHarness
	out, _ := releasedFactsRun(t, func(h *setHarness) func() []string {
		h0 = h
		ref := h.refs[0]
		h.scenario.Credit(ref, 40)
		ldb := h.scenario.LedgerDB()
		txn := ldb.Begin(t.Context(), true)
		slot := h.scenario.CurrentPoint().Slot
		require.NoError(t, ldb.AddPostSnapshotAccountRewardByCredential(
			t.Context(), ref.Tag, ref.Key, 6, slot, []byte{0x32}, txn,
		))
		require.NoError(t, txn.Commit())
		// Read again after the rollback the harness performs next: the
		// credits it undoes leave the journal.
		return func() []string {
			rows, _, err := h0.scenario.ReadFactRows(
				metadata.AccountRewardCreditTable, 0, math.MaxInt64, 0, 100,
			)
			require.NoError(t, err)
			lines := renderFactRows(creditRows(rows))
			slices.Sort(lines)
			return append([]string{"credits"}, lines...)
		}
	})
	after := func(lines []string) []string {
		return lines[slices.Index(lines, "credits")+1:]
	}
	applied := after(out["applied"])
	require.Len(t, applied, 3,
		"the single credit, the batch's second source and the post-snapshot credit")
	flagged := slices.DeleteFunc(slices.Clone(applied), func(l string) bool {
		return !strings.Contains(l, "post_snapshot=1")
	})
	require.Len(t, flagged, 1, "only the post-snapshot credit is flagged")
	require.Contains(t, flagged[0], "amount=6")
	require.Contains(t, flagged[0], "tx_hash=32")
	require.Empty(t, after(out["rolled back"]), "the rollback undoes the credits")
}

// A reader that holds the position of its last read gets the credit journaled
// after it even when the credit is dated before everything it read.
func TestRewardCreditJournalResumesAfterALateCreditAtAnOldSlot(
	t *testing.T,
) {
	t.Parallel()
	releasedFactsRun(t, func(h *setHarness) func() []string {
		ref := h.refs[0]
		credit := func(slot uint64, source byte) {
			ldb := h.scenario.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.AddPostSnapshotAccountRewardByCredential(
				t.Context(), ref.Tag, ref.Key, 6, slot, []byte{source}, txn,
			))
			require.NoError(t, txn.Commit())
		}
		// The second credit is dated before the first, so the order the
		// credits were written in is not the order of their slots.
		const old = 1
		h.scenario.Credit(ref, 40)
		credit(old, 0x32)
		rows, _, err := h.scenario.ReadFactRows(
			metadata.AccountRewardCreditTable, 0, math.MaxInt64, 0, 100,
		)
		require.NoError(t, err)
		require.Len(t, rows.Rows, 3)
		mark := rows.LastID
		require.NotZero(t, mark)

		credit(old, 0x33)
		later, _, err := h.scenario.ReadFactRows(
			metadata.AccountRewardCreditTable, old, old, mark, 100,
		)
		require.NoError(t, err)
		require.Equal(t, [][]any{{
			ref.Key, int64(ref.Tag), []byte{0x33}, "6", int64(old), int64(1),
		}}, creditRows(later).Rows)
		again, _, err := h.scenario.ReadFactRows(
			metadata.AccountRewardCreditTable, 0, math.MaxInt64, later.LastID,
			100,
		)
		require.NoError(t, err)
		require.Empty(t, again.Rows, "nothing is served twice")
		return func() []string { return nil }
	})
}
