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

package sqlstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMetadataLabelPageUsesSlotIndex pins the access path of the metadata
// label page in both directions: it must search the (label, slot,
// transaction_id) index and sort only the tie-break terms, so a page stops
// after its first rows instead of sorting every transaction with the label.
func TestMetadataLabelPageUsesSlotIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	_, err := store.writeDB.Exec(`
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<2000)
INSERT INTO "transaction"
  (hash, block_hash, slot, type, id, fee, collateral_fee, ttl, block_index, valid)
SELECT randomblob(32), randomblob(32), i*20, 0, i, '0', '0', '0', i%10, 1 FROM n`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<2000)
INSERT INTO transaction_metadata_label (transaction_id, label, slot)
SELECT i, '674', i*20 FROM n`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec("ANALYZE")
	require.NoError(t, err)

	for _, direction := range []string{"ASC", "DESC"} {
		rows, err := store.writeDB.Query(
			"EXPLAIN QUERY PLAN "+metadataLabelPageQuery(direction)+
				" LIMIT 100 OFFSET 0",
			"674",
		)
		require.NoError(t, err, direction)
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			plan = append(plan, detail)
		}
		require.NoError(t, rows.Err(), direction)
		require.NoError(t, rows.Close(), direction)
		joined := strings.Join(plan, "; ")

		require.Contains(t, joined,
			"USING COVERING INDEX idx_transaction_metadata_label_label_slot",
			"%s page must search the slot index: %s", direction, joined)
		require.Contains(t, joined, "TEMP B-TREE FOR LAST 2 TERMS OF ORDER BY",
			"%s page must sort only the tie-break terms: %s", direction, joined)
		require.NotContains(t, joined, "SCAN",
			"%s page must not scan a table: %s", direction, joined)
		require.NotContains(t, joined, "USE TEMP B-TREE FOR ORDER BY",
			"%s page must not sort every label row: %s", direction, joined)
	}
}
