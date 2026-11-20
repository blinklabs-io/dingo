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

// With the accounts family drained, the live UTxO set exports the reward
// credits the metadata store journaled: each source once at its slot, the
// post-snapshot ones flagged, and a credit written later under an older slot
// follows the position a reader holds.
func TestDrainedSetExportsTheRewardCreditJournal(t *testing.T) {
	t.Parallel()
	read := func(
		drain bool,
		h *p5Harness,
		from, through uint64,
		cursor int64,
	) (metadata.FactRows, int64) {
		if drain {
			rows, next, err := h.scenario.ReadFactRows(
				metadata.AccountRewardCreditTable, from, through, cursor, 100,
			)
			require.NoError(t, err)
			return creditRows(rows), next
		}
		store, ok := h.scenario.DB.Metadata().(metadata.FactRowStore)
		require.True(t, ok)
		rows, next, err := store.ReadFactRows(
			t.Context(), metadata.AccountRewardCreditTable, from, through,
			cursor, 100, nil,
		)
		require.NoError(t, err)
		return creditRows(rows), next
	}
	run := func(drain bool) map[string][]string {
		var h0 *p5Harness
		out, _ := allGraduatedRun(t, drain, func(h *p5Harness) func() []string {
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
			// credits it undoes leave the journal as they leave the store.
			return func() []string {
				rows, _ := read(drain, h0, 0, math.MaxInt64, 0)
				lines := renderFactRows(rows)
				slices.Sort(lines)
				return append([]string{"credits"}, lines...)
			}
		})
		after := func(lines []string) []string {
			return lines[slices.Index(lines, "credits")+1:]
		}
		return map[string][]string{
			"applied": after(out["applied"]), "rolled back": after(out["rolled back"]),
		}
	}
	want := run(false)
	require.Len(t, want["applied"], 3,
		"the single credit, the batch's second source and the post-snapshot credit")
	require.Empty(t, want["rolled back"], "the rollback undoes the credits")
	got := run(true)
	require.Equal(t, want["applied"], got["applied"])
	require.Equal(t, want["rolled back"], got["rolled back"])
}

// A reader that holds the position of its last read gets the credit journaled
// after it even when the credit is dated before everything it read.
func TestDrainedRewardCreditJournalResumesAfterALateCreditAtAnOldSlot(
	t *testing.T,
) {
	t.Parallel()
	allGraduatedRun(t, true, func(h *p5Harness) func() []string {
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
