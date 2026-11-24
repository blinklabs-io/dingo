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

package ledger

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// ledgerDBDigest exports every fact table and the transaction index from the
// LedgerDB set as sorted text rows, so two ledgers' state can be compared by
// value. The SQL store
// holds no ledger state, so its dump says nothing about it.
func ledgerDBDigest(t *testing.T, ls *LedgerState) string {
	t.Helper()
	var out strings.Builder
	for _, table := range metadata.FactTables {
		var lines []string
		var cursor int64
		for {
			page, next, err := ls.ReadFactRows(
				t.Context(), table, 0, math.MaxInt64, cursor, 10_000, nil,
			)
			require.NoError(t, err, "export %s", table.Name)
			for _, row := range page.Rows {
				fields := make([]string, len(row))
				for i, v := range row {
					fields[i] = fmt.Sprintf("%s=%v", page.Columns[i], v)
				}
				lines = append(lines, strings.Join(fields, "|"))
			}
			if next == 0 {
				break
			}
			cursor = next
		}
		sort.Strings(lines)
		fmt.Fprintf(&out, "## %s\n%s\n", table.Name, strings.Join(lines, "\n"))
	}
	// The transaction index is no fact table: no IndexerDB copies it.
	if index, ok := ls.config.UtxoSet.(TxIndexReader); ok {
		var lines []string
		require.NoError(t, index.EachTxIndexEntry(func(e TxIndexEntry) error {
			lines = append(lines, fmt.Sprintf(
				"hash=%x|block=%x|slot=%d|index=%d|era=%d|valid=%t",
				e.Hash, e.BlockHash, e.Slot, e.BlockIndex, e.Type, e.Valid,
			))
			return nil
		}))
		fmt.Fprintf(&out, "## tx_index\n%s\n", strings.Join(lines, "\n"))
	}
	return out.String()
}
