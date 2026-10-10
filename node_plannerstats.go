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
	"errors"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/plannerstats"
)

// newPlannerStatsManager builds the planner-statistics manager for the
// current database handle, or returns nil when the refresh is disabled or
// the metadata store has no incremental form. A restore or truncate replaces
// the handle, so the manager is rebuilt with it rather than reused.
func (n *Node) newPlannerStatsManager() *plannerstats.Manager {
	if !n.config.plannerStatsRefreshEnabled() || n.db == nil {
		return nil
	}
	updater, ok := n.db.Metadata().(metadata.IncrementalPlannerStatsUpdater)
	if !ok {
		return nil
	}
	return plannerstats.NewManager(updater, n.eventBus, n.config.logger)
}

// runPlannerStatsStartup runs the startup pass before any block is
// processed. A failure is logged and does not stop the node: the node still
// works, only with the planner statistics it had.
func (n *Node) runPlannerStatsStartup(ctx context.Context) {
	n.plannerStatsMgr = n.newPlannerStatsManager()
	if n.plannerStatsMgr == nil {
		return
	}
	n.config.logger.Info(
		"optimizing planner statistics (a first run on a large database can take minutes)",
		"component", "plannerstats",
	)
	if _, err := n.plannerStatsMgr.RunStartup(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		n.config.logger.Warn(
			"planner statistics startup run failed",
			"component", "plannerstats",
			"error", err,
		)
	}
}

// startPlannerStatsManager subscribes the manager to epoch rollovers,
// creating it first when the database handle was replaced.
func (n *Node) startPlannerStatsManager(ctx context.Context) error {
	if n.plannerStatsMgr == nil {
		n.plannerStatsMgr = n.newPlannerStatsManager()
	}
	if n.plannerStatsMgr == nil {
		return nil
	}
	return n.plannerStatsMgr.Start(ctx)
}
