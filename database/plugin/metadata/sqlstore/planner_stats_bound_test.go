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
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func plannerProbeStat4Rows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var present int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_stat4'",
	).Scan(&present))
	if present == 0 {
		return 0
	}
	var samples int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_stat4 WHERE tbl = 'planner_analysis_probe'",
	).Scan(&samples))
	return samples
}

func TestOptimizePlannerStatsBoundsAnalysisAndRestoresLimit(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, 100, 5000} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			t.Parallel()
			store := newFilePlannerStatsStore(t, nil)
			ctx := t.Context()
			_, err := store.writeDB.ExecContext(ctx, `
CREATE TABLE planner_analysis_probe (id INTEGER PRIMARY KEY, value INTEGER);
CREATE INDEX planner_analysis_value ON planner_analysis_probe(value);
CREATE INDEX planner_analysis_empty ON planner_analysis_probe(value) WHERE value < 0;
WITH RECURSIVE seq(id) AS (
 SELECT 1 UNION ALL SELECT id + 1 FROM seq WHERE id < 10000
)
INSERT INTO planner_analysis_probe SELECT id, id % 127 FROM seq;`)
			require.NoError(t, err)
			_, err = store.writeDB.ExecContext(
				ctx,
				fmt.Sprintf("PRAGMA analysis_limit = %d", limit),
			)
			require.NoError(t, err)
			for range 2 {
				result, err := store.OptimizePlannerStatsContext(
					ctx,
					PlannerStatsTriggerPeriodic,
				)
				require.NoError(t, err)
				require.True(t, result.Supported)
				var stat string
				require.NoError(t, store.writeDB.QueryRowContext(
					ctx,
					"SELECT stat FROM sqlite_stat1 WHERE idx = 'planner_analysis_value'",
				).Scan(&stat))
				require.NotEmpty(
					t,
					stat,
					"bounded analysis still populates planner statistics",
				)
				// STAT4 histograms require a full scan; limited ANALYZE cannot produce them.
				require.Zero(
					t,
					plannerProbeStat4Rows(t, store.writeDB),
					"periodic maintenance must use sampled analysis",
				)
				var restored int
				require.NoError(
					t,
					store.writeDB.QueryRowContext(ctx, "PRAGMA analysis_limit").
						Scan(&restored),
				)
				require.Equal(
					t,
					limit,
					restored,
					"the optimizer's analysis limit must be temporary",
				)
			}
			_, err = store.writeDB.ExecContext(ctx, "PRAGMA analysis_limit = 0")
			require.NoError(t, err)
			require.NoError(
				t,
				store.dialect.UpdatePlannerStats(
					context.Background(),
					store.writeDB,
				),
			)
			require.Positive(
				t,
				plannerProbeStat4Rows(t, store.writeDB),
				"explicit full analysis must still collect histograms",
			)
		})
	}
}
