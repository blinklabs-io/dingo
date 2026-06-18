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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// insertTestUtxos inserts n UTxO rows so ANALYZE has data to measure.
func insertTestUtxos(t *testing.T, store *MetadataStoreSqlite, n int) {
	t.Helper()
	for i := range n {
		txID := bytes.Repeat([]byte{byte(i + 1)}, 32)
		row := models.Utxo{
			TxId:      txID,
			OutputIdx: 0,
			AddedSlot: uint64(i + 1),
			Amount:    types.Uint64(uint64(i+1) * 1_000_000),
		}
		require.NoError(t, store.DB().Create(&row).Error)
	}
}

func sqliteStatCount(t *testing.T, store *MetadataStoreSqlite, table string) int64 {
	t.Helper()
	var count int64
	err := store.DB().Raw(
		"SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = ?",
		table,
	).Scan(&count).Error
	require.NoError(t, err)
	return count
}

// TestUpdatePlannerStats_PopulatesStat1 verifies that after inserting rows and
// calling UpdatePlannerStats, sqlite_stat1 is populated with at least one entry.
func TestUpdatePlannerStats_PopulatesStat1(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	insertTestUtxos(t, store, 5)

	require.NoError(t, store.UpdatePlannerStats())

	var count int64
	err := store.DB().Raw("SELECT COUNT(*) FROM sqlite_stat1").Scan(&count).Error
	require.NoError(t, err)
	assert.Greater(t, count, int64(0), "sqlite_stat1 should have rows after ANALYZE")
}

// TestUpdatePlannerStats_CollectsUtxoTableStats verifies that the utxo table
// specifically gets stats. This is the table that caused bad query plans.
func TestUpdatePlannerStats_CollectsUtxoTableStats(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	insertTestUtxos(t, store, 3)

	require.NoError(t, store.UpdatePlannerStats())

	tableName := (&models.Utxo{}).TableName()
	assert.Positive(
		t,
		sqliteStatCount(t, store, tableName),
		"sqlite_stat1 should have an entry for the utxo table",
	)
}

// TestUpdatePlannerStats_EmptyDB verifies UpdatePlannerStats does not error
// on a freshly created database with no rows.
func TestUpdatePlannerStats_EmptyDB(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	assert.NoError(t, store.UpdatePlannerStats())
}

// TestUpdatePlannerStats_Idempotent verifies that calling UpdatePlannerStats
// more than once does not error. This matters for resume/restart paths.
func TestUpdatePlannerStats_Idempotent(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	insertTestUtxos(t, store, 3)

	require.NoError(t, store.UpdatePlannerStats())
	require.NoError(t, store.UpdatePlannerStats())
	assert.Positive(
		t,
		sqliteStatCount(t, store, (&models.Utxo{}).TableName()),
		"sqlite_stat1 should remain populated after repeated ANALYZE",
	)
}

func TestUpdatePlannerStats_ReturnsErrorWhenDatabaseClosed(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	require.NoError(t, store.Close())

	err := store.UpdatePlannerStats()
	require.Error(t, err)
	assert.ErrorContains(t, err, "ANALYZE")
}
