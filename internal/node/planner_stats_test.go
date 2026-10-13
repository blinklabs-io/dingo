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
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestFinalizeBackfillPlannerStatsRepairsAndResumes(t *testing.T) {
	t.Parallel()
	db := newFileTestDB(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO "transaction"(id,hash,slot,fee) VALUES(1,randomblob(32),1,'0'); ANALYZE; WITH RECURSIVE n(i) AS (VALUES(2) UNION ALL SELECT i+1 FROM n WHERE i<1000) INSERT INTO "transaction"(id,hash,slot,fee) SELECT i,randomblob(32),i,'0' FROM n;`,
	)
	require.NoError(t, err)
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   1000,
		TotalSlots: 1000,
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
		Completed:  true,
	}
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(cp, nil))
	logger := slog.New(slog.DiscardHandler)
	stat := func() string {
		var value string
		require.NoError(
			t,
			raw.QueryRow("SELECT stat FROM sqlite_stat1 WHERE idx='idx_transaction_hash'").
				Scan(&value),
		)
		return value
	}
	require.Equal(t, "1 1", stat())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(
		t,
		FinalizeBackfillPlannerStats(ctx, db, logger),
		context.Canceled,
	)
	require.Equal(t, "1 1", stat())
	_, err = raw.Exec(
		`CREATE TRIGGER fail_stats_marker BEFORE INSERT ON sync_state WHEN NEW.sync_key='metadata_planner_stats_backfill' BEGIN SELECT RAISE(ABORT,'marker interrupted'); END`,
	)
	require.NoError(t, err)
	require.ErrorContains(
		t,
		FinalizeBackfillPlannerStats(t.Context(), db, logger),
		"marker interrupted",
	)
	marker, err := db.GetSyncState(metadata.PlannerStatsBackfillSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, marker)
	require.Equal(t, "1000 1", stat())
	_, err = raw.Exec("DROP TRIGGER fail_stats_marker")
	require.NoError(t, err)
	require.NoError(t, FinalizeBackfillPlannerStats(t.Context(), db, logger))
	marker, err = db.GetSyncState(metadata.PlannerStatsBackfillSyncKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, marker)
	// A normal restart must not re-analyze the entire database.
	_, err = raw.Exec(
		`UPDATE sqlite_stat1 SET stat='17 1' WHERE idx='idx_transaction_hash'`,
	)
	require.NoError(t, err)
	require.NoError(t, FinalizeBackfillPlannerStats(t.Context(), db, logger))
	require.Equal(t, "17 1", stat())
	cp.UpdatedAt = cp.UpdatedAt.Add(time.Second)
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(cp, nil))
	require.NoError(t, FinalizeBackfillPlannerStats(t.Context(), db, logger))
	require.Equal(t, "1000 1", stat())
}

func TestFinalizeBackfillPlannerStatsSkipsIncompleteBackfill(t *testing.T) {
	t.Parallel()
	db := newFileTestDB(t)
	logger := slog.New(slog.DiscardHandler)
	require.NoError(t, FinalizeBackfillPlannerStats(t.Context(), db, logger))
	require.NoError(
		t,
		db.Metadata().
			SetBackfillCheckpoint(&models.BackfillCheckpoint{Phase: BackfillPhase, LastSlot: 1, TotalSlots: 2, UpdatedAt: time.Now()}, nil),
	)
	require.NoError(t, FinalizeBackfillPlannerStats(t.Context(), db, logger))
	marker, err := db.GetSyncState(metadata.PlannerStatsBackfillSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, marker)
}
