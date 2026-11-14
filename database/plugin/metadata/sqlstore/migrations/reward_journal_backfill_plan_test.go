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

package migrations

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRewardJournalCopySlotsPlan pins the SQLite plan of the backfill's
// DISTINCT slot copy to the batch's rowid range. Through an
// account_reward_delta index the copy can read every legacy row once per batch.
func TestRewardJournalCopySlotsPlan(t *testing.T) {
	t.Parallel()
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	index := -1
	for i, migration := range registry {
		if migration.Name == rewardJournalTablesSchemaRelease {
			index = i
		}
	}
	require.Positive(t, index)
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	SeedMigratedPrefix(t, path, index)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, statement := range registry[index].SQL["sqlite"].Expand {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}

	query := newRewardJournalStatements("sqlite").copySlots
	rows, err := db.Query(
		"EXPLAIN QUERY PLAN "+query,
		make([]any, strings.Count(query, "?"))...,
	)
	require.NoError(t, err)
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan.WriteString(detail + "\n")
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Contains(
		t, plan.String(),
		"SEARCH d USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)",
	)
	require.NotContains(t, plan.String(), "idx_account_reward_delta")
}
