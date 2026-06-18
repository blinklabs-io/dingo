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

package sqlite

import (
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"gorm.io/gorm"
)

// DropDeferredIndexes drops every index in deferred.Manifest that
// currently exists in the schema and records a sync_state marker so
// that an interrupted run is detectable on the next startup.
//
// The marker is written BEFORE any DROP runs. That ordering matters:
// if the process dies between writing the marker and finishing the
// drops, the marker is still set and HasDeferredIndexesPending will
// return true. If the marker write fails, no DROP has happened yet,
// so the schema is unchanged.
func (d *MetadataStoreSqlite) DropDeferredIndexes() error {
	if err := d.markIndexesPending(); err != nil {
		return fmt.Errorf("marking indexes pending: %w", err)
	}
	migrator := d.DB().Migrator()
	start := time.Now()
	dropped := 0
	for _, idx := range deferred.Manifest {
		name := idx.ResolvedName()
		// HasIndex tolerates either field or literal index name
		// on the GORM SQLite migrator; check before dropping so
		// the operation is idempotent across re-runs.
		if !migrator.HasIndex(idx.Model, name) {
			continue
		}
		if err := migrator.DropIndex(idx.Model, name); err != nil {
			return fmt.Errorf(
				"drop index %q on %q: %w",
				name, idx.Table, err,
			)
		}
		dropped++
	}
	d.logger.Info(
		"dropped deferred metadata indexes for bulk load",
		"component", "database",
		"dropped", dropped,
		"manifest_size", len(deferred.Manifest),
		"duration", time.Since(start),
	)
	return nil
}

// BuildCriticalDeferredIndexes recreates only the manifest entries
// marked Critical=true. It does NOT clear the sync_state marker,
// because the lazy remainder still needs to be built.
// Call this before marking the API ready; follow up with
// BuildDeferredIndexes to finish the remaining lazy indexes.
func (d *MetadataStoreSqlite) BuildCriticalDeferredIndexes() error {
	return d.buildIndexSubset(deferred.CriticalManifest(), false)
}

// BuildDeferredIndexes recreates every index in deferred.Manifest
// that is missing from the schema and, once every manifest entry is
// present, clears the sync_state marker. The operation is
// idempotent: re-running it after a successful build is a no-op.
//
// Index creation is logged per-entry so operators can correlate
// slow builds with specific indexes (the utxo composite index, in
// particular, can take several minutes on mainnet).
func (d *MetadataStoreSqlite) BuildDeferredIndexes() error {
	return d.buildIndexSubset(deferred.Manifest, true)
}

func (d *MetadataStoreSqlite) buildIndexSubset(subset []deferred.Index, clearMarker bool) error {
	migrator := d.DB().Migrator()
	overallStart := time.Now()
	built := 0
	for _, idx := range subset {
		name := idx.ResolvedName()
		if migrator.HasIndex(idx.Model, name) {
			continue
		}
		entryStart := time.Now()
		d.logger.Info(
			"building deferred metadata index",
			"component", "database",
			"table", idx.Table,
			"index", name,
		)
		if err := migrator.CreateIndex(idx.Model, name); err != nil {
			return fmt.Errorf(
				"create index %q on %q: %w",
				name, idx.Table, err,
			)
		}
		built++
		d.logger.Info(
			"deferred metadata index built",
			"component", "database",
			"table", idx.Table,
			"index", name,
			"duration", time.Since(entryStart),
		)
	}
	for _, idx := range subset {
		name := idx.ResolvedName()
		if !migrator.HasIndex(idx.Model, name) {
			return fmt.Errorf(
				"deferred index %q on %q missing after build "+
					"— manifest may be stale or schema diverged",
				name, idx.Table,
			)
		}
	}
	if clearMarker {
		if err := d.clearIndexesPending(); err != nil {
			return fmt.Errorf("clearing indexes-pending marker: %w", err)
		}
	}
	d.logger.Info(
		"rebuilt deferred metadata indexes",
		"component", "database",
		"built", built,
		"subset_size", len(subset),
		"manifest_size", len(deferred.Manifest),
		"duration", time.Since(overallStart),
	)
	return nil
}

// HasDeferredIndexesPending returns true when DropDeferredIndexes
// ran without a matching BuildDeferredIndexes. Callers use this to
// detect crash recovery scenarios before serving API traffic.
func (d *MetadataStoreSqlite) HasDeferredIndexesPending() (bool, error) {
	val, err := d.GetSyncState(deferred.SyncStateKey, nil)
	if err != nil {
		return false, fmt.Errorf(
			"read %s: %w", deferred.SyncStateKey, err,
		)
	}
	return val == deferred.SyncStateValue, nil
}

func (d *MetadataStoreSqlite) markIndexesPending() error {
	return d.SetSyncState(
		deferred.SyncStateKey,
		deferred.SyncStateValue,
		nil,
	)
}

func (d *MetadataStoreSqlite) clearIndexesPending() error {
	// Use Delete to remove the row entirely so a future
	// HasDeferredIndexesPending check returns false without
	// needing to parse the empty-string value semantics.
	if err := d.DeleteSyncState(
		deferred.SyncStateKey, nil,
	); err != nil {
		// Tolerate "not found" defensively; the sqlite plugin
		// already returns nil for a no-op delete, but we surface
		// any other error.
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	return nil
}
