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

package metadata

import (
	"context"

	"github.com/blinklabs-io/dingo/database/types"
)

// FactTable describes a table of ledger facts: rows the epoch-boundary,
// governance and reward rules write, which block indexing alone does not.
// Rows are read and removed by slot, never by a store's own row ids.
type FactTable struct {
	// Name is the SQL table name.
	Name string
	// SlotColumns lists the columns that date a row. A row is in a slot range
	// when any of them lies inside it.
	SlotColumns []string
	// BlockPatched marks a table whose fact columns sit on rows that block
	// indexing writes. Rollback leaves such a table alone: its rows go with
	// the block-derived state.
	BlockPatched bool
	// Paged marks a table with a surrogate id column, used only as the read
	// cursor and never returned. A table without one is read in a single
	// call.
	Paged bool
	// FilterColumn and FilterValue, when FilterColumn is set, restrict reads
	// to rows whose column equals the value.
	FilterColumn string
	FilterValue  any
}

// FactRows is a batch of fact rows: Columns names the values of each row.
type FactRows struct {
	Columns []string
	Rows    [][]any
	// LastID is the largest row id a Paged read returned, set even when the
	// read reached the end of the range and the returned cursor is zero.
	LastID int64
}

// CommitSyncer is an optional capability of a metadata store whose commits
// are not durable when they return. SyncCommits returns only once every commit
// that returned before it began survives a crash of the host. A store without
// it commits durably.
type CommitSyncer interface {
	SyncCommits(ctx context.Context) error
}

// FactRowStore reads and removes fact rows. It is an optional capability of a
// metadata store: an IndexerDB's rollback removes its derived facts through it,
// and the ledger's SQL LedgerDB reads rows through it.
type FactRowStore interface {
	// ReadFactRows returns rows of table with a slot column in [from,
	// through], at most limit of them for a Paged table, after cursor (zero
	// to start). The returned cursor continues the read; it is zero when the
	// table has no more rows in range.
	ReadFactRows(
		ctx context.Context,
		table FactTable,
		from, through uint64,
		cursor int64,
		limit int,
		txn types.Txn,
	) (rows FactRows, next int64, err error)
	// DeleteFactRowsAfter removes rows of table dated after slot, for a
	// rollback. A Patch table is not touched: its rows are rolled back with
	// the block-derived state they sit on.
	DeleteFactRowsAfter(
		table FactTable,
		slot uint64,
		txn types.Txn,
	) error
}

// FactTables lists the ledger-fact tables an IndexerDB derives, which its
// rollback removes after the target. Rollback of the ones whose block-derived
// counterpart the rollback sweep already removes is covered there as well;
// deleting again is harmless.
var FactTables = []FactTable{
	{
		Name:        "epoch",
		SlotColumns: []string{"start_slot"},
		Paged:       true,
	},
	{
		Name:        "epoch_summary",
		SlotColumns: []string{"boundary_slot"},
		Paged:       true,
	},
	{
		Name: "pool_stake_snapshot", SlotColumns: []string{"captured_slot"},
		Paged: true,
	},
	{
		Name:        "reward_ada_pots",
		SlotColumns: []string{"captured_slot"},
		Paged:       true,
	},
	{
		Name: "reward_snapshot", SlotColumns: []string{"captured_slot"}, Paged: true,
	},
	{
		Name: "reward_pool_input", SlotColumns: []string{"captured_slot"}, Paged: true,
	},
	{
		Name: "reward_stake_input", SlotColumns: []string{"captured_slot"},
		Paged: true,
	},
	{
		Name: "reward_pool_output", SlotColumns: []string{"captured_slot"}, Paged: true,
	},
	{
		Name: "reward_account_output", SlotColumns: []string{"captured_slot"},
		Paged: true,
	},
	{
		Name: "reward_seed_failure", SlotColumns: []string{"captured_slot"},
	},
	{
		Name: "imported_pool_block_count", SlotColumns: []string{"captured_slot"},
	},
	{
		Name: "imported_epoch_block_total", SlotColumns: []string{"captured_slot"},
	},
	{
		Name:        "reward_credit_round",
		SlotColumns: []string{"boundary_slot"},
	},
	{
		Name:        "network_state",
		SlotColumns: []string{"slot"},
		Paged:       true,
	},
	{
		Name:        "network_donation",
		SlotColumns: []string{"slot"},
		Paged:       true,
	},
	{
		Name: "pparams", SlotColumns: []string{"added_slot"}, Paged: true,
	},
	{
		Name: "drep_expiry_history", SlotColumns: []string{"added_slot"},
		Paged: true,
	},
	{
		// A dormant expiry bump and a dormancy reset can fall in one slot, and
		// the counter before each differs.
		Name: "drep_dormancy_history", SlotColumns: []string{"added_slot"}, Paged: true,
	},
	{
		Name:        "drep_expiry_epoch_event",
		SlotColumns: []string{"added_slot"},
	},
	{
		Name: "account_drep_clear", SlotColumns: []string{"added_slot"},
	},
	{
		Name: "governance_proposal",
		SlotColumns: []string{
			"ratified_slot", "enacted_slot", "expired_slot", "deleted_slot",
		},
		BlockPatched: true,
		Paged:        true,
	},
	{
		// The ledger drops an expired proposal one epoch after marking it
		// expired, and records when in this companion row.
		Name:        "governance_proposal_drop",
		SlotColumns: []string{"dropped_slot"},
		Paged:       true,
	},
	{
		Name: "account", SlotColumns: []string{"added_slot"},
		BlockPatched: true,
		Paged:        true,
	},
	{
		Name: "drep", SlotColumns: []string{"added_slot"},
		BlockPatched: true,
		Paged:        true,
	},
}

// AccountRewardCreditTable is the reward-credit journal. It is not copied as
// rows: each credit is applied through AddAccountRewardsByCredential, which
// also moves the account balance and is idempotent per credit.
var AccountRewardCreditTable = FactTable{
	Name: "account_reward_delta", SlotColumns: []string{"added_slot"},
	Paged:        true,
	FilterColumn: "withdrawal", FilterValue: false,
}
