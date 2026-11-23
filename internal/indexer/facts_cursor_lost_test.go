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

package indexer

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// pagedPots serves five reward_ada_pots rows two at a time. When lose is set,
// the first continuation fails as a ledger that restarted between pages does.
type pagedPots struct {
	lose    bool
	cursors []int64
}

func (p *pagedPots) ReadFactRows(
	_ context.Context,
	_ metadata.FactTable,
	_, _ uint64,
	cursor int64,
	_ int,
	_ types.Txn,
) (metadata.FactRows, int64, error) {
	p.cursors = append(p.cursors, cursor)
	if p.lose && cursor > 0 {
		p.lose = false
		return metadata.FactRows{}, 0, metadata.ErrFactCursorLost
	}
	out := metadata.FactRows{Columns: []string{
		"epoch", "treasury", "reserves", "fees", "rewards", "captured_slot",
		"imported_epoch_fees",
	}}
	for e := cursor * 2; e < min(cursor*2+2, 5); e++ {
		out.Rows = append(out.Rows, []any{e, "1", "2", "3", "4", int64(10), nil})
	}
	if cursor*2+2 >= 5 {
		return out, 0, nil
	}
	return out, cursor + 1, nil
}

// A copy whose cursor the ledger lost starts that table again from the start
// and finishes it: the IndexerDB ends with the rows it holds after an
// uninterrupted copy.
func TestCopyTableRestartsAfterTheLedgerLosesItsCursor(t *testing.T) {
	t.Parallel()
	copyPots := func(lose bool) (rows []int, cursors []int64) {
		src, _ := newFactsDB(t)
		dst, dstRaw := newFactsDB(t)
		reader := &pagedPots{lose: lose}
		ledger := newStoreLedger(t, src)
		ledger.FactRowReader = reader
		f, err := NewMetadataFacts(ledger, dst)
		require.NoError(t, err)
		var table metadata.FactTable
		for _, ft := range metadata.FactTables {
			if ft.Name == "reward_ada_pots" {
				table = ft
			}
		}
		writeFacts(t, f, func(txn *database.Txn) error {
			return f.copyTable(context.Background(), txn, table, 0, 100)
		})
		r, err := dstRaw.Query(`SELECT epoch FROM reward_ada_pots ORDER BY epoch`)
		require.NoError(t, err)
		defer r.Close()
		for r.Next() {
			var e int
			require.NoError(t, r.Scan(&e))
			rows = append(rows, e)
		}
		return rows, reader.cursors
	}
	want, _ := copyPots(false)
	require.Equal(t, []int{0, 1, 2, 3, 4}, want)
	got, cursors := copyPots(true)
	require.Equal(t, want, got, "the table is the same after a lost cursor")
	require.Equal(t, []int64{0, 1, 0, 1, 2}, cursors,
		"the copy went back to the first page after the lost cursor")
}
