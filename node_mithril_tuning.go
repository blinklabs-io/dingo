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
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/plannerstats"
)

// mithrilTunedCatchUpMinChunks is the smallest gap, in immutable files, for
// which a stream catch-up drops the lazy deferred indexes and tunes the write
// connection. The rebuild after the run reads every row of the tables those
// indexes cover, a cost set by the size of the database rather than by the
// gap, so a run of a few files would spend longer rebuilding than it saved.
// A hundred files is upwards of twenty thousand blocks on every network,
// which takes the trusted apply path several minutes.
const mithrilTunedCatchUpMinChunks = 100

// mithrilCatchUpIndexMinGapFraction is the smallest gap, as a fraction of the
// blocks the ledger already holds, for which a large-gap run also drops the
// catch-up droppable indexes. Those are rebuilt inside the hold, so their
// cost is paid with Ouroboros held off: the rebuild reads every existing row
// of utxo, transaction and asset, while the drop saves only index
// maintenance on the rows the gap adds. Measured on preview, the drop made
// the trusted apply about 1.2x faster, and the in-hold rebuild took 14.3 s
// for 640K utxo and 1.07M transaction rows, against roughly 11 minutes of
// apply for a 100-file mainnet gap. A gap at least half the existing chain
// adds rows on the order of those it rebuilds, so the drop still pays; a
// node a few weeks behind on a mature chain keeps the indexes and drops only
// the lazy ones, which are rebuilt after the release.
const mithrilCatchUpIndexMinGapFraction = 0.5

// mithrilCatchUpIndexesWorthDropping reports whether a gap of gapFiles
// immutable files, above a ledger whose tip is in file localFile at block
// tipBlock, is large enough relative to the existing chain to drop the
// catch-up droppable indexes. An empty ledger always is.
//
// The gap is estimated in blocks from the local chain's own density: its
// tipBlock blocks over localFile+1 files. Later files hold more blocks than
// earlier ones (Byron's are sparse), so this underestimates a gap above an
// older chain and errs towards keeping the indexes.
func mithrilCatchUpIndexesWorthDropping(
	gapFiles, localFile, tipBlock uint64,
) bool {
	if tipBlock == 0 {
		return true
	}
	gapBlocks := float64(gapFiles) * float64(tipBlock) /
		float64(localFile+1)
	return gapBlocks >= mithrilCatchUpIndexMinGapFraction*float64(tipBlock)
}

// mithrilTuningScope is what one run's tuning may do.
type mithrilTuningScope struct {
	// largeGap tunes the connection and drops the lazy indexes.
	largeGap bool
	// catchUpIndexes, with largeGap, also drops the catch-up droppable
	// indexes the hold's release rebuilds.
	catchUpIndexes bool
}

// mithrilCatchUpTuningBackend reports whether the metadata plugin named
// plugin gets the large-gap tuning. Only SQLite does: it builds each missing
// index in its own transaction and its pragmas are per connection, while
// PostgreSQL and MySQL rebuild the manifest in one transaction whose locks on
// the rebuilt tables would stall the writes Ouroboros resumes, so they keep
// every index and their settings. An empty name is the default provider,
// which is SQLite.
func mithrilCatchUpTuningBackend(plugin string) bool {
	return plugin == "" || plugin == "sqlite"
}

// mithrilCatchUpTuning prepares the database for one stream catch-up run.
// The coordinator calls begin once the run holds the ledger. It calls the
// returned restore inside the hold, before releasing it, and the returned end
// after the release, on every exit. Nothing end does counts against the
// stream's no-progress timeout or keeps Ouroboros off the ledger; restore
// runs after the stream's watchdog has stopped, but does keep Ouroboros off.
type mithrilCatchUpTuning interface {
	// begin defers planner-statistics maintenance, and for a large gap also
	// tunes the write connection and drops the lazy deferred indexes, plus
	// the catch-up droppable ones on a core-mode database.
	begin(ctx context.Context, scope mithrilTuningScope) mithrilTuned
}

// mithrilTuned undoes one run's tuning in two steps.
type mithrilTuned struct {
	// restore rebuilds what Ouroboros needs the moment it resumes: the
	// catch-up droppable indexes rollback and chain selection read.
	restore func(context.Context)
	// end restores the connection settings, rebuilds the lazy indexes and
	// lets planner maintenance run again.
	end func(context.Context)
}

// nodeMithrilTuning tunes the node's live database and planner-statistics
// manager.
type nodeMithrilTuning struct {
	db *database.Database
	// planner is nil when the refresh is disabled; Defer is nil-safe.
	planner *plannerstats.Manager
	logger  *slog.Logger
}

func (t nodeMithrilTuning) begin(
	ctx context.Context,
	scope mithrilTuningScope,
) mithrilTuned {
	if t.logger == nil {
		t.logger = slog.New(slog.DiscardHandler)
	}
	resumePlanner := t.planner.Defer()
	if !scope.largeGap || t.db == nil ||
		!mithrilCatchUpTuningBackend(t.db.Config().MetadataPlugin) {
		return mithrilTuned{
			restore: func(context.Context) {},
			end:     func(context.Context) { resumePlanner() },
		}
	}
	store := t.db.Metadata()
	tuner, tuned := store.(metadata.CatchUpPragmaTuner)
	if tuned {
		if err := tuner.SetCatchUpPragmas(); err != nil {
			t.logger.Warn(
				"mithril: catch-up database tuning failed, continuing untuned",
				"component", "mithril",
				"error", err,
			)
		}
	}
	// The drop is one transaction, but the rebuilds are idempotent, so they
	// run whether or not it succeeded.
	var dropped bool
	catchUp, catchUpDropped := store.(metadata.CatchUpIndexManager)
	// API storage keeps the catch-up droppable indexes: they serve its
	// address and history queries.
	catchUpDropped = catchUpDropped && scope.catchUpIndexes &&
		t.db.Config().StorageMode != types.StorageModeAPI
	if catchUpDropped {
		dropped = true
		if err := catchUp.DropCatchUpIndexesContext(ctx); err != nil {
			t.logger.Warn(
				"mithril: dropping catch-up metadata indexes failed",
				"component", "mithril",
				"error", err,
			)
		} else {
			t.logger.Info(
				"mithril: lazy and catch-up metadata indexes dropped for a "+
					"large catch-up; address and stake queries are slower "+
					"until it ends",
				"component", "mithril",
			)
		}
	} else if dropper, ok := store.(metadata.LazyDeferredIndexDropper); ok {
		dropped = true
		if err := dropper.DropLazyDeferredIndexesContext(ctx); err != nil {
			t.logger.Warn(
				"mithril: dropping lazy metadata indexes failed",
				"component", "mithril",
				"error", err,
			)
		} else {
			t.logger.Info(
				"mithril: lazy metadata indexes dropped for a large catch-up",
				"component", "mithril",
			)
		}
	}
	return mithrilTuned{
		restore: func(ctx context.Context) {
			if catchUpDropped {
				t.rebuildCatchUpIndexes(ctx, catchUp)
			}
		},
		end: func(ctx context.Context) {
			defer resumePlanner()
			if tuned {
				if err := tuner.RestoreNormalPragmas(); err != nil {
					t.logger.Error(
						"mithril: restoring normal database settings failed",
						"component", "mithril",
						"error", err,
					)
				}
			}
			if dropped {
				t.rebuildLazyIndexes(ctx, store)
			}
		},
	}
}

// rebuildCatchUpIndexes rebuilds the catch-up droppable indexes while the run
// still holds the ledger. It has no timeout of its own: releasing with them
// missing would turn the first rollback into a scan of the utxo table per
// rolled-back transaction. A failure, or a node shutdown cancelling ctx,
// leaves the recovery marker set; end's full rebuild retries at once, and
// startup rebuilds every critical index before the node ingests again.
func (t nodeMithrilTuning) rebuildCatchUpIndexes(
	ctx context.Context,
	manager metadata.CatchUpIndexManager,
) {
	start := time.Now()
	t.logger.Info(
		"mithril: rebuilding catch-up metadata indexes before resuming "+
			"Ouroboros",
		"component", "mithril",
	)
	if err := manager.BuildCatchUpIndexesContext(ctx); err != nil {
		if ctx.Err() != nil {
			t.logger.Warn(
				"mithril: catch-up metadata index rebuild cancelled by "+
					"shutdown or a database lifecycle operation; the "+
					"next reinitialization or startup finishes it before "+
					"ingest",
				"component", "mithril",
				"error", err,
			)
			return
		}
		t.logger.Error(
			"mithril: rebuilding catch-up metadata indexes failed; "+
				"the full rebuild after the release retries them, and "+
				"rollbacks scan the utxo table until one succeeds",
			"component", "mithril",
			"error", err,
		)
		return
	}
	t.logger.Info(
		"mithril: catch-up metadata indexes rebuilt",
		"component", "mithril",
		"duration", time.Since(start),
	)
}

// rebuildLazyIndexes rebuilds the deferred-index manifest and clears its
// recovery marker. A failure, a cancelled run included, leaves the marker
// set, so the node's startup maintenance finishes the rebuild.
func (t nodeMithrilTuning) rebuildLazyIndexes(
	ctx context.Context,
	store metadata.MetadataStore,
) {
	manager, ok := store.(metadata.DeferredIndexManager)
	if !ok {
		return
	}
	start := time.Now()
	var err error
	if builder, ok := store.(metadata.ContextDeferredIndexBuilder); ok {
		err = builder.BuildDeferredIndexesContext(ctx)
	} else {
		err = manager.BuildDeferredIndexes()
	}
	if err != nil {
		t.logger.Warn(
			"mithril: rebuilding lazy metadata indexes failed; "+
				"the next startup finishes it",
			"component", "mithril",
			"error", err,
		)
		return
	}
	t.logger.Info(
		"mithril: lazy metadata indexes rebuilt",
		"component", "mithril",
		"duration", time.Since(start),
	)
}
