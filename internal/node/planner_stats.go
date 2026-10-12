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
	"fmt"
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// FinalizeBackfillPlannerStats refreshes statistics once per completed backfill.
// Call after critical index repair and before declaring the database ready.
// A missing completion marker also repairs databases created by older releases.
func FinalizeBackfillPlannerStats(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	updater, ok := db.Metadata().(metadata.PlannerStatsUpdater)
	if !ok {
		return nil
	}
	cp, err := db.Metadata().GetBackfillCheckpoint(BackfillPhase, nil)
	if err != nil {
		return fmt.Errorf("read planner statistics checkpoint: %w", err)
	}
	if cp == nil || !cp.Completed {
		return nil
	}
	version := fmt.Sprintf(
		"v1:%d:%d:%s",
		cp.LastSlot,
		cp.TotalSlots,
		cp.UpdatedAt.UTC().Format(time.RFC3339Nano),
	)
	previous, err := db.GetSyncState(metadata.PlannerStatsBackfillSyncKey, nil)
	if err != nil {
		return err
	}
	if previous == version {
		return nil
	}
	logger.Info(
		"refreshing planner statistics after metadata backfill",
		"slot",
		cp.LastSlot,
	)
	started := time.Now()
	if contextual, ok := updater.(metadata.ContextPlannerStatsUpdater); ok {
		err = contextual.UpdatePlannerStatsContext(ctx)
	} else {
		err = updater.UpdatePlannerStats()
	}
	if err != nil {
		return fmt.Errorf("refresh planner statistics after backfill: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The marker follows successful ANALYZE. A crash before this write safely
	// repeats analysis instead of allowing stale statistics through readiness.
	if err := db.SetSyncState(metadata.PlannerStatsBackfillSyncKey, version, nil); err != nil {
		return err
	}
	logger.Info(
		"planner statistics refreshed after metadata backfill",
		"slot",
		cp.LastSlot,
		"duration",
		time.Since(started),
	)
	return nil
}
