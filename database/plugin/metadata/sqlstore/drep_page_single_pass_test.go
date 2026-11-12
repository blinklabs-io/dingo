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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestGetDrepsPageAggregatesDelegationsOnce covers the list query reading the
// delegation tables a single time per request: the page and its total come
// from one statement, so a non-empty page costs one aggregation pass rather
// than a count pass plus a page pass.
func TestGetDrepsPageAggregatesDelegationsOnce(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	for i := range 3 {
		_, err := store.writeDB.Exec(`
INSERT INTO drep (credential, added_slot, last_activity_epoch, expiry_epoch, active)
VALUES (?, ?, 0, 0, TRUE)`, bytes.Repeat([]byte{byte(i + 1)}, 28), i+10)
		require.NoError(t, err)
	}
	_, err := store.writeDB.Exec(
		"INSERT INTO vote_delegation (drep_type, added_slot) VALUES (?, 1)",
		models.DrepTypeAlwaysAbstain,
	)
	require.NoError(t, err)

	statements := func() float64 {
		return counterValue(t, reg, "select") + counterValue(t, reg, "other")
	}
	for _, query := range []models.DrepListPageQuery{
		{Limit: 2, Offset: 1},
		{Limit: 2, Offset: 1, Descending: true},
		{Limit: 2, OrderByAmount: true},
	} {
		before := statements()
		page, total, err := store.GetDrepsPage(query, nil)
		require.NoError(t, err)
		require.Equal(t, 4, total)
		require.NotEmpty(t, page)
		require.Equal(
			t, float64(1), statements()-before,
			"statements per non-empty list page",
		)
	}
}
