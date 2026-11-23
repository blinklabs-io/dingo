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

package dingo

import (
	"context"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/plannerstats"
)

// newPlannerStatsManager builds the planner-statistics manager for db's
// metadata store, or returns nil when refresh is disabled or the store keeps
// no planner statistics. Only the IndexerDB keeps a metadata store.
func (n *Node) newPlannerStatsManager(db *database.Database) *plannerstats.Manager {
	if !n.config.PlannerStatsRefreshEnabled() || db == nil {
		return nil
	}
	updater, ok := db.Metadata().(metadata.IncrementalPlannerStatsUpdater)
	if !ok {
		return nil
	}
	return plannerstats.NewManager(updater, n.eventBus, n.config.logger)
}

// startIndexerPlannerStatsManager starts the IndexerDB's manager with a
// queued startup pass. The IndexerDB holds the API history, which can take
// minutes to analyze, so callers place it after the API listeners start and
// planner maintenance cannot delay availability. withStartup is false when the
// manager restarts after a storage reinitialization.
func (n *Node) startIndexerPlannerStatsManager(
	ctx context.Context,
	withStartup bool,
) error {
	n.indexerPlannerStatsMgr = n.newPlannerStatsManager(n.indexerDB)
	if n.indexerPlannerStatsMgr == nil {
		return nil
	}
	if withStartup {
		return n.indexerPlannerStatsMgr.StartWithStartup(ctx)
	}
	return n.indexerPlannerStatsMgr.Start(ctx)
}
