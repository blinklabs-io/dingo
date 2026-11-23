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
	"sync"
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
					PlannerStatsTriggerStartup,
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

// A table that keeps growing between periodic passes must have its statistics
// retaken each pass, however small the growth since the last one.
func TestOptimizePlannerStatsRefreshesStatsOfAGrowingTable(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	ctx := t.Context()
	_, err := store.writeDB.ExecContext(ctx, `
CREATE TABLE planner_growth_probe (id INTEGER PRIMARY KEY, value INTEGER);
CREATE INDEX planner_growth_value ON planner_growth_probe(value);`)
	require.NoError(t, err)
	grow := func(from, to int) {
		_, err := store.writeDB.ExecContext(ctx, `
WITH RECURSIVE seq(id) AS (
 SELECT ? UNION ALL SELECT id + 1 FROM seq WHERE id < ?
)
INSERT INTO planner_growth_probe SELECT id, id % 7 FROM seq`, from, to)
		require.NoError(t, err)
	}
	statRows := func() int {
		var stat string
		require.NoError(t, store.writeDB.QueryRowContext(ctx,
			"SELECT stat FROM sqlite_stat1 WHERE idx = 'planner_growth_value'",
		).Scan(&stat))
		var n int
		_, err := fmt.Sscanf(stat, "%d", &n)
		require.NoError(t, err)
		return n
	}

	grow(1, 100)
	_, err = store.OptimizePlannerStatsContext(ctx, PlannerStatsTriggerStartup)
	require.NoError(t, err)
	require.Equal(t, 100, statRows())

	grow(101, 400)
	_, err = store.OptimizePlannerStatsContext(ctx, PlannerStatsTriggerStartup)
	require.NoError(t, err)
	require.Equal(
		t, 400, statRows(),
		"statistics must describe the table as it is now, not as it was when "+
			"it was a quarter of its size",
	)
}

// On a pooled write connection (the in-memory topology, where readDB is
// writeDB) every statement of the bounded pass must run on one connection:
// analysis_limit is per connection, so a limit set on one and restored on
// another leaves the first bounded for every later explicit ANALYZE.
func TestOptimizePlannerStatsRestoresLimitOnAPooledWriteConnection(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.Same(t, store.readDB, store.writeDB)
	const conns = 4
	store.writeDB.SetMaxOpenConns(conns)
	store.writeDB.SetMaxIdleConns(conns)
	ctx := t.Context()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range conns - 1 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				var one int
				_ = store.writeDB.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			}
		})
	}
	for range 50 {
		_, err := store.OptimizePlannerStatsContext(
			ctx, PlannerStatsTriggerStartup,
		)
		require.NoError(t, err)
	}
	close(stop)
	readers.Wait()

	held := make([]*sql.Conn, 0, conns)
	defer func() {
		for _, c := range held {
			require.NoError(t, c.Close())
		}
	}()
	for i := range conns {
		c, err := store.writeDB.Conn(ctx)
		require.NoError(t, err)
		held = append(held, c)
		var limit int
		require.NoError(t, c.QueryRowContext(
			ctx, "PRAGMA analysis_limit",
		).Scan(&limit))
		require.Zero(t, limit, "connection %d kept the temporary limit", i)
	}
}

// cancelOnAnalyze cancels the run's context as ANALYZE starts, the way a
// shutdown landing mid-pass does.
type cancelOnAnalyze struct {
	*sql.Conn
	cancel context.CancelFunc
}

func (c cancelOnAnalyze) ExecContext(
	ctx context.Context, query string, args ...any,
) (sql.Result, error) {
	if query == "ANALYZE" {
		c.cancel()
		return nil, context.Canceled
	}
	return c.Conn.ExecContext(ctx, query, args...)
}

// A pass cut short by cancellation must still put the connection's limit
// back, or the write connection samples every later explicit ANALYZE.
func TestBoundedAnalyzeRestoresLimitWhenCanceled(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	conn, err := store.writeDB.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err = sqliteBoundedAnalyze(ctx, cancelOnAnalyze{Conn: conn, cancel: cancel})
	require.ErrorIs(t, err, context.Canceled)

	var limit int
	require.NoError(t, conn.QueryRowContext(
		t.Context(), "PRAGMA analysis_limit",
	).Scan(&limit))
	require.Zero(t, limit, "the temporary limit must not outlive the pass")
}
