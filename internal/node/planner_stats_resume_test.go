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

package node

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

type fullOnlyStatsStore struct{ full int }

func (f *fullOnlyStatsStore) UpdatePlannerStats() error {
	f.full++
	return nil
}

type incrementalStatsStore struct {
	fullOnlyStatsStore
	present     bool
	presentErr  error
	result      types.PlannerStatsResult
	optimizeErr error
	optimized   int
}

func (f *incrementalStatsStore) HasPlannerStats(
	context.Context,
) (bool, error) {
	return f.present, f.presentErr
}

func (f *incrementalStatsStore) OptimizePlannerStatsContext(
	context.Context,
	string,
) (types.PlannerStatsResult, error) {
	f.optimized++
	return f.result, f.optimizeErr
}

// incrementalNoCheckerStore has an incremental form but cannot tell whether
// statistics exist, so it must keep the full refresh.
type incrementalNoCheckerStore struct {
	fullOnlyStatsStore
	optimized int
}

func (f *incrementalNoCheckerStore) OptimizePlannerStatsContext(
	context.Context,
	string,
) (types.PlannerStatsResult, error) {
	f.optimized++
	return types.PlannerStatsResult{Supported: true}, nil
}

func TestRunPlannerStatsChoosesRefreshFromExistingStatistics(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	supported := types.PlannerStatsResult{Supported: true}

	t.Run("no statistics runs a full analyze", func(t *testing.T) {
		t.Parallel()
		s := &incrementalStatsStore{result: supported}
		require.NoError(t, runPlannerStats(t.Context(), s, logger))
		require.Equal(t, 1, s.full)
		require.Zero(t, s.optimized)
	})
	t.Run("existing statistics skip the full analyze", func(t *testing.T) {
		t.Parallel()
		s := &incrementalStatsStore{present: true, result: supported}
		require.NoError(t, runPlannerStats(t.Context(), s, logger))
		require.Zero(t, s.full, "a full ANALYZE must not run")
		require.Equal(t, 1, s.optimized)
	})
	t.Run("skipped incremental pass falls back to full", func(t *testing.T) {
		t.Parallel()
		s := &incrementalStatsStore{
			present: true,
			result:  types.PlannerStatsResult{Supported: true, Skipped: true},
		}
		require.NoError(t, runPlannerStats(t.Context(), s, logger))
		require.Equal(t, 1, s.full)
		require.Equal(t, 1, s.optimized)
	})
	t.Run("unsupported incremental pass falls back to full", func(t *testing.T) {
		t.Parallel()
		s := &incrementalStatsStore{present: true}
		require.NoError(t, runPlannerStats(t.Context(), s, logger))
		require.Equal(t, 1, s.full)
	})
	t.Run("backend without incremental form is unchanged", func(t *testing.T) {
		t.Parallel()
		s := &fullOnlyStatsStore{}
		require.NoError(t, runPlannerStats(t.Context(), s, logger))
		require.Equal(t, 1, s.full)
	})
	t.Run("incremental store without presence check is unchanged", func(t *testing.T) {
		t.Parallel()
		s := &incrementalNoCheckerStore{}
		require.NoError(t, runPlannerStats(t.Context(), s, logger))
		require.Equal(t, 1, s.full)
		require.Zero(t, s.optimized)
	})
	t.Run("presence error is returned", func(t *testing.T) {
		t.Parallel()
		s := &incrementalStatsStore{presentErr: errors.New("boom")}
		err := runPlannerStats(t.Context(), s, logger)
		require.ErrorContains(t, err, "boom")
		require.Zero(t, s.full)
	})
	t.Run("optimize error is returned", func(t *testing.T) {
		t.Parallel()
		s := &incrementalStatsStore{
			present: true, optimizeErr: errors.New("boom"),
		}
		err := runPlannerStats(t.Context(), s, logger)
		require.ErrorContains(t, err, "boom")
		require.Zero(t, s.full)
	})
}

const probeStatsSetup = `
CREATE TABLE resume_probe (id INTEGER PRIMARY KEY, value INTEGER);
CREATE INDEX resume_probe_value ON resume_probe(value);
WITH RECURSIVE seq(id) AS (
 SELECT 1 UNION ALL SELECT id + 1 FROM seq WHERE id < 5000
)
INSERT INTO resume_probe SELECT id, id % 97 FROM seq;`

func probeIndexStat(t *testing.T, raw *sql.DB) string {
	t.Helper()
	var stat string
	require.NoError(t, raw.QueryRow(
		"SELECT stat FROM sqlite_stat1 WHERE idx = 'resume_probe_value'",
	).Scan(&stat))
	return stat
}

func TestRunPlannerStatsOnSQLite(t *testing.T) {
	t.Parallel()

	t.Run("fresh database gets a full analyze", func(t *testing.T) {
		t.Parallel()
		db := newFileTestDB(t)
		raw, err := dbtest.RawSQLiteMetadata(t, db)
		require.NoError(t, err)
		_, err = raw.Exec(probeStatsSetup)
		require.NoError(t, err)

		require.NoError(t, RunPlannerStats(db, slog.Default()))
		require.NotEmpty(t, probeIndexStat(t, raw))
	})

	t.Run("existing statistics are not rebuilt", func(t *testing.T) {
		t.Parallel()
		db := newFileTestDB(t)
		raw, err := dbtest.RawSQLiteMetadata(t, db)
		require.NoError(t, err)
		_, err = raw.Exec(probeStatsSetup)
		require.NoError(t, err)
		_, err = raw.Exec("ANALYZE")
		require.NoError(t, err)
		// A full ANALYZE recomputes every row; the incremental pass leaves
		// an unchanged table's row alone.
		const sentinel = "5000 12345"
		_, err = raw.Exec(
			"UPDATE sqlite_stat1 SET stat = ? WHERE idx = 'resume_probe_value'",
			sentinel,
		)
		require.NoError(t, err)

		require.NoError(t, RunPlannerStats(db, slog.Default()))
		require.Equal(t, sentinel, probeIndexStat(t, raw))
	})
}

// Hot backfill statements must plan the same after a resume as after a full
// ANALYZE.
func TestRunPlannerStatsResumeKeepsBackfillQueryPlans(t *testing.T) {
	t.Parallel()
	db := newFileTestDB(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<2000)
INSERT INTO "transaction"(id,hash) SELECT i,randomblob(32) FROM n;
WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<2000)
INSERT INTO applied_transaction(tx_id,slot,fee) SELECT i,i,'0' FROM n;
WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<2000)
INSERT INTO witness_scripts(script_hash,transaction_id,type) SELECT randomblob(28),i,0 FROM n;
WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<2000)
INSERT INTO plutus_data(data,transaction_id) SELECT randomblob(8),i FROM n;`)
	require.NoError(t, err)
	_, err = raw.Exec("ANALYZE")
	require.NoError(t, err)

	queries := []string{
		`SELECT hash FROM "transaction" WHERE id >= 10 AND id < 500`,
		`SELECT tx_id FROM applied_transaction WHERE slot >= 10 AND slot < 500`,
		`DELETE FROM witness_scripts WHERE transaction_id >= 10 AND transaction_id < 500`,
		`DELETE FROM plutus_data WHERE transaction_id >= 10 AND transaction_id < 500`,
	}
	plans := func() []string {
		out := make([]string, 0, len(queries))
		for _, q := range queries {
			rows, err := raw.Query("EXPLAIN QUERY PLAN " + q)
			require.NoError(t, err)
			var sb strings.Builder
			for rows.Next() {
				var id, parent, notUsed int
				var detail string
				require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
				sb.WriteString(detail + "\n")
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.NotEmpty(t, sb.String())
			out = append(out, sb.String())
		}
		return out
	}
	afterFull := plans()

	require.NoError(t, RunPlannerStats(db, slog.Default()))
	require.Equal(t, afterFull, plans())
}
