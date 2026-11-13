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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasPlannerStatsReadsTheDatabase(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	ctx := t.Context()

	present, err := store.HasPlannerStats(ctx)
	require.NoError(t, err)
	require.False(t, present, "an unanalyzed database has no statistics")

	_, err = store.writeDB.ExecContext(ctx, `
CREATE TABLE has_stats_probe (id INTEGER PRIMARY KEY, value INTEGER);
CREATE INDEX has_stats_value ON has_stats_probe(value);
INSERT INTO has_stats_probe VALUES (1, 1), (2, 2);
ANALYZE`)
	require.NoError(t, err)
	present, err = store.HasPlannerStats(ctx)
	require.NoError(t, err)
	require.True(t, present)

	_, err = store.writeDB.ExecContext(ctx, "DELETE FROM sqlite_stat1")
	require.NoError(t, err)
	present, err = store.HasPlannerStats(ctx)
	require.NoError(t, err)
	require.False(t, present, "an empty sqlite_stat1 is not statistics")
}
