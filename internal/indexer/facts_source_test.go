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

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// pendingReader reads fact rows from a store and keeps the pending stake-input
// record where the ledger does, not in the store's sync state.
type pendingReader struct {
	metadata.FactRowReader
	pending string
}

func (p pendingReader) RewardStakeInputsPendingState() (string, error) {
	return p.pending, nil
}

type plainReader struct{ metadata.FactRowReader }

// A copy over a fact reader takes the snapshots still being written from the
// reader's own pending record, since a ledger whose families are drained keeps
// that record outside the metadata store.
func TestFactsOverAReaderUseItsPendingStakeInputs(t *testing.T) {
	t.Parallel()
	src, _ := newFactsDB(t)
	dst, _ := newFactsDB(t)
	store, ok := src.Metadata().(metadata.FactRowStore)
	require.True(t, ok)

	f, err := NewFactsOver(src, pendingReader{
		FactRowReader: store,
		pending:       `{"entries":[{"epoch":7,"boundary_slot":70}]}`,
	}, dst)
	require.NoError(t, err)
	pending, err := f.pendingStakeInputEpochs()
	require.NoError(t, err)
	require.Contains(t, pending, uint64(7))

	// A reader without the capability falls back to the store's sync state,
	// which holds no record here.
	g, err := NewFactsOver(src, plainReader{FactRowReader: store}, dst)
	require.NoError(t, err)
	pending, err = g.pendingStakeInputEpochs()
	require.NoError(t, err)
	require.Empty(t, pending)
}

// The source of a copy is never written, whichever reader supplies it.
func TestFactsOverAReaderNeverWriteItsSource(t *testing.T) {
	t.Parallel()
	src, _ := newFactsDB(t)
	dst, _ := newFactsDB(t)
	store, ok := src.Metadata().(metadata.FactRowStore)
	require.True(t, ok)
	f, err := NewFactsOver(src, store, dst)
	require.NoError(t, err)
	require.ErrorIs(
		t,
		f.srcRows.WriteFactRows(metadata.FactTables[0], metadata.FactRows{}, nil),
		errFactSourceReadOnly,
	)
	require.ErrorIs(
		t, f.srcRows.DeleteFactRowsAfter(metadata.FactTables[0], 0, nil),
		errFactSourceReadOnly,
	)
	_, _, err = f.srcRows.ReadFactRows(
		context.Background(), metadata.FactTables[0], 0, 1, 0, 10, nil,
	)
	require.NoError(t, err)
}

// A dormant expiry bump and a dormancy reset at one slot leave two dormancy
// history rows, which differ by the counter before each; a copy keeps both.
func TestDormancyHistoryRowsOfOneSlotAreBothCopied(t *testing.T) {
	t.Parallel()
	dst, _ := newFactsDB(t)
	store, ok := dst.Metadata().(metadata.FactRowStore)
	require.True(t, ok)
	var table metadata.FactTable
	for _, tbl := range metadata.FactTables {
		if tbl.Name == "drep_dormancy_history" {
			table = tbl
		}
	}
	txn := dst.Metadata().Transaction(t.Context())
	require.NoError(t, store.WriteFactRows(table, metadata.FactRows{
		Columns: []string{"added_slot", "previous_dormant_epochs"},
		Rows:    [][]any{{int64(7), int64(1)}, {int64(7), int64(2)}},
	}, txn))
	require.NoError(t, txn.Commit())

	rows, _, err := store.ReadFactRows(
		t.Context(), table, 0, 100, 0, 10, nil,
	)
	require.NoError(t, err)
	require.Len(t, rows.Rows, 2)
}
