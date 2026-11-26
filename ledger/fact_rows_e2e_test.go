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
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// servedFactTables are the fact tables the live UTxO set exports.
var servedFactTables = []string{
	"epoch", "epoch_summary", "network_state", "network_donation", "pparams",
	"reward_ada_pots", "reward_snapshot", "reward_pool_input",
	"reward_seed_failure", "reward_stake_input", "reward_pool_output",
	"reward_account_output", "reward_credit_round", "account", "drep",
	"governance_proposal", "governance_proposal_drop", "pool_stake_snapshot",
	"imported_pool_block_count", "imported_epoch_block_total",
	"drep_expiry_history", "drep_dormancy_history", "drep_expiry_epoch_event",
	"account_drep_clear",
}

// patchFactColumns are the columns the set exports of a patch table: its key
// and patch columns, which are all the writer applies to a row it already
// holds. The metadata store's other columns are compared away.
var patchFactColumns = map[string][]string{
	"account": {
		"staking_key", "credential_tag", "pool", "drep", "drep_type",
		"active", "expiration_epoch", "added_slot",
	},
	"drep": {"credential", "credential_tag", "active", "added_slot"},
	"governance_proposal": {
		"tx_hash", "action_index", "ratified_epoch", "ratified_slot",
		"enacted_epoch", "enacted_slot", "expired_epoch", "expired_slot",
		"deleted_slot",
	},
}

func factTableByName(t *testing.T, name string) metadata.FactTable {
	t.Helper()
	for _, table := range metadata.FactTables {
		if table.Name == name {
			return table
		}
	}
	require.FailNow(t, "no such fact table", name)
	return metadata.FactTable{}
}

// renderFactRows renders every row as column=value pairs in column order, so
// two exports compare regardless of the order their columns came in or of the
// integer width a driver returned.
func renderFactRows(rows metadata.FactRows) []string {
	// folded is the shared store's credit-fold state of an output; a split
	// IndexerDB derives its own from its credit journal.
	cols := slices.DeleteFunc(
		slices.Clone(rows.Columns),
		func(c string) bool { return c == "folded" },
	)
	slices.Sort(cols)
	out := make([]string, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		parts := make([]string, 0, len(cols))
		for _, c := range cols {
			v := row[slices.Index(rows.Columns, c)]
			switch x := v.(type) {
			case []byte:
				v = fmt.Sprintf("%x", x)
			case int64, int, uint64, uint:
				v = fmt.Sprintf("%d", x)
			case bool:
				if x {
					v = "1"
				} else {
					v = "0"
				}
			}
			parts = append(parts, fmt.Sprintf("%s=%v", c, v))
		}
		out = append(out, strings.Join(parts, " "))
	}
	slices.Sort(out)
	return out
}

func projectFactRows(rows metadata.FactRows, keep []string) metadata.FactRows {
	out := metadata.FactRows{Columns: keep}
	for _, row := range rows.Rows {
		var r []any
		for _, c := range keep {
			r = append(r, row[slices.Index(rows.Columns, c)])
		}
		out.Rows = append(out.Rows, r)
	}
	return out
}

func readAllFactRows(
	t *testing.T,
	read func(metadata.FactTable, uint64, uint64, int64, int) (metadata.FactRows, int64, error),
	name string,
	from, through uint64,
) []string {
	t.Helper()
	table := factTableByName(t, name)
	var all metadata.FactRows
	var cursor int64
	for {
		rows, next, err := read(table, from, through, cursor, 2)
		require.NoError(t, err, name)
		all.Columns = rows.Columns
		if keep := patchFactColumns[name]; keep != nil {
			rows = projectFactRows(rows, keep)
			all.Columns = rows.Columns
		}
		all.Rows = append(all.Rows, rows.Rows...)
		if next == 0 {
			break
		}
		cursor = next
	}
	return renderFactRows(all)
}

// The live UTxO set exports each served
// fact table by its natural keys: every table holds rows over the whole slot
// range, and a narrower range returns rows of the whole range only.
func TestSetExportsTheFactRows(t *testing.T) {
	t.Parallel()
	type window struct{ from, through uint64 }
	windows := []window{{0, math.MaxInt64}, {0, 0}}
	out := map[string][]string{}
	_, h := releasedFactsRun(t, func(h *setHarness) func() []string {
		// DRep registrations, activity, a dormant expiry bump, a reset
		// and a clear of dangling delegations write the DRep history.
		h.scenario.Governance(h.refs[0], 10)
		cur := h.scenario.CurrentPoint().Slot
		windows = append(windows, window{cur, cur + 1})
		for _, name := range servedFactTables {
			for _, w := range windows {
				key := fmt.Sprintf("%s[%d,%d]", name, w.from, w.through)
				out[key] = readAllFactRows(
					t, h.scenario.ReadFactRows, name, w.from, w.through,
				)
			}
		}
		return func() []string { return nil }
	})
	for _, name := range servedFactTables {
		require.NotContains(t, h.scenario.UnservedFactTables(), name)
		whole := out[fmt.Sprintf("%s[%d,%d]", name, 0, uint64(math.MaxInt64))]
		require.NotEmpty(t, whole, "%s holds rows", name)
		for _, w := range windows[1:] {
			key := fmt.Sprintf("%s[%d,%d]", name, w.from, w.through)
			require.Subset(t, whole, out[key], key)
		}
	}
}

// Every ledger-state fact table is exported by the set: no other store holds
// its rows.
func TestSetServesEveryLedgerStateFactTable(t *testing.T) {
	t.Parallel()
	_, h := releasedFactsRun(t, func(*setHarness) func() []string {
		return func() []string { return nil }
	})
	require.Empty(t, h.scenario.UnservedFactTables())
}

// A rollback drops the DRep history rows written after its target from the
// live UTxO set as it drops them from the metadata store, and the rows before
// it stay.
func TestSetDRepHistoryFollowsARollback(t *testing.T) {
	t.Parallel()
	names := []string{
		"drep_expiry_history", "drep_dormancy_history",
		"drep_expiry_epoch_event", "account_drep_clear",
	}
	rows, _ := releasedFactsRun(t, func(h *setHarness) func() []string {
		h.scenario.Governance(h.refs[0], 10)
		return func() []string {
			var lines []string
			for _, name := range names {
				for _, row := range readAllFactRows(
					t, h.scenario.ReadFactRows, name, 0, math.MaxInt64,
				) {
					lines = append(lines, name+" "+row)
				}
			}
			return lines
		}
	})
	grew := 0
	for _, name := range names {
		count := func(lines []string) int {
			n := 0
			for _, l := range lines {
				if strings.HasPrefix(l, name+" ") {
					n++
				}
			}
			return n
		}
		if count(rows["applied"]) > count(rows["rolled back"]) {
			grew++
		}
	}
	require.Equal(
		t, len(names), grew,
		"every DRep history table lost rows to the rollback",
	)
}

// The set serves the tables that grow with the chain page by page from a key.
// Read one row at a time, every page continues where the last stopped: the
// pages are the table, each row once, as one large read returns it.
func TestSeekedFactPagesAreTheWholeTable(t *testing.T) {
	t.Parallel()
	tables := []string{
		"account", "drep", "drep_expiry_history", "drep_dormancy_history",
	}
	releasedFactsRun(t, func(h *setHarness) func() []string {
		for _, name := range tables {
			idx := slices.IndexFunc(metadata.FactTables, func(ft metadata.FactTable) bool {
				return ft.Name == name
			})
			require.GreaterOrEqual(t, idx, 0, name)
			table := metadata.FactTables[idx]

			whole, _, err := h.scenario.ReadFactRows(table, 0, math.MaxInt64, 0, 10_000)
			require.NoError(t, err, name)
			var paged [][]any
			cursor := int64(0)
			pages := 0
			for {
				page, next, err := h.scenario.ReadFactRows(
					table, 0, math.MaxInt64, cursor, 1,
				)
				require.NoError(t, err, name)
				paged = append(paged, page.Rows...)
				pages++
				require.LessOrEqual(t, pages, len(whole.Rows)+2,
					"%s: the pages do not end", name)
				if next == 0 {
					break
				}
				cursor = next
			}
			got := renderFactRows(metadata.FactRows{Columns: whole.Columns, Rows: paged})
			want := renderFactRows(whole)
			slices.Sort(got)
			slices.Sort(want)
			require.Equal(t, want, got, name)
			if name == "account" {
				require.GreaterOrEqual(t, len(want), 2, "the harness has accounts")
			}
		}
		return func() []string { return nil }
	})
}

// An export of the accounts and DReps added in a slot range costs the rows
// changed in that range, whatever the size of the table, and a row whose
// slot leaves the range between two pages is not served while every other row
// is served once. A range below the journal's reach reads the whole table.
func TestAccountAndDrepExportCostFollowsTheRowsChangedInRange(t *testing.T) {
	t.Parallel()
	const rowsHeld = 3000
	tableOf := func(name string) metadata.FactTable {
		i := slices.IndexFunc(metadata.FactTables, func(ft metadata.FactTable) bool {
			return ft.Name == name
		})
		require.GreaterOrEqual(t, i, 0, name)
		return metadata.FactTables[i]
	}
	releasedFactsRun(t, func(h *setHarness) func() []string {
		base := h.scenario.CurrentPoint().Slot + 10
		cred := func(i int) []byte {
			return []byte{0x90, byte(i >> 16), byte(i >> 8), byte(i), 1, 2, 3, 4}
		}
		for lo := 0; lo < rowsHeld; lo += 500 {
			var lc ledger.LedgerChanges
			for i := lo; i < lo+500; i++ {
				slot := base + uint64(i) //nolint:gosec // a small index
				lc.Accounts = append(lc.Accounts, ledger.AccountChange{
					Slot: slot, Account: models.Account{
						StakingKey: cred(i), AddedSlot: slot, Active: true,
					},
				})
				lc.DReps = append(lc.DReps, ledger.DRepChange{
					Slot: slot, Drep: models.Drep{
						Credential: cred(i), AddedSlot: slot, Active: true,
					},
				})
			}
			require.NoError(t, h.set.ApplyLedgerChanges(nil, lc))
		}

		const first, last = rowsHeld / 2, rowsHeld/2 + 4
		from, through := base+uint64(first), base+uint64(last)
		for _, name := range []string{"account", "drep"} {
			table := tableOf(name)
			var got []string
			cursor := int64(0)
			before := h.set.RangeRowsVisited()
			for {
				page, next, err := h.scenario.ReadFactRows(
					table, from, through, cursor, 2,
				)
				require.NoError(t, err, name)
				for _, r := range page.Rows {
					got = append(got, fmt.Sprintf("%x",
						r[slices.Index(page.Columns, "added_slot")]))
				}
				if next == 0 {
					break
				}
				cursor = next
			}
			visited := h.set.RangeRowsVisited() - before
			require.Len(t, got, last-first+1, name)
			// Each slot of the range holds one account entry and one DRep
			// entry; the pages add a step or two at their edges.
			require.LessOrEqual(t, visited, uint64(3*(last-first+1)+6),
				"%s: the export stepped over rows outside the range", name)
		}

		// An account whose slot moves out of the range after the first page is
		// not served; the rest are, once each.
		from2, through2 := base+100, base+104
		table := tableOf("account")
		page, cursor, err := h.scenario.ReadFactRows(table, from2, through2, 0, 2)
		require.NoError(t, err)
		require.Len(t, page.Rows, 2)
		require.NotZero(t, cursor)
		moved := ledger.LedgerChanges{Accounts: []ledger.AccountChange{{
			Slot: through2 + 5000, Account: models.Account{
				StakingKey: cred(103), AddedSlot: through2 + 5000, Active: true,
			},
		}}}
		require.NoError(t, h.set.ApplyLedgerChanges(nil, moved))
		served := len(page.Rows)
		for cursor != 0 {
			page, cursor, err = h.scenario.ReadFactRows(
				table, from2, through2, cursor, 2,
			)
			require.NoError(t, err)
			served += len(page.Rows)
		}
		require.Equal(t, 4, served)

		// A range from slot zero is below the journal's reach: the whole
		// table is read, and the held rows are among its rows.
		whole, _, err := h.scenario.ReadFactRows(
			tableOf("account"), 0, through, 0, 100_000,
		)
		require.NoError(t, err)
		// The moved account is past through now; the harness holds two more.
		require.Len(t, whole.Rows, last+1-1+2)
		return func() []string { return nil }
	})
}
