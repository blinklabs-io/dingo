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

package metadata

// DeferredIndexManager is an optional interface that metadata stores
// implement to participate in bulk-load index deferral. The Mithril
// sync orchestrator probes the store with a type assertion and skips
// deferral entirely when the store does not implement this
// interface.
//
// The manifest lives in
// github.com/blinklabs-io/dingo/database/plugin/metadata/deferred —
// kept in a sub-package so each plugin (sqlite, mysql, postgres) can
// import it without pulling in the parent metadata package, which
// side-effect-imports every plugin and would create an import
// cycle.
//
// Crash recovery contract:
//
//   - DropDeferredIndexes records the deferred.SyncStateKey marker
//     in sync_state BEFORE any DROP runs, so an interrupted run is
//     detectable on the next startup.
//   - BuildDeferredIndexes is idempotent: it skips indexes that
//     already exist and clears the sync_state marker only after
//     every manifest entry is present in the schema.
//   - HasDeferredIndexesPending answers the recovery question
//     without touching DDL, so callers like `dingo serve` can
//     decide whether to refuse startup or auto-repair.
type DeferredIndexManager interface {
	DropDeferredIndexes() error
	// BuildCriticalDeferredIndexes rebuilds only the manifest
	// entries marked Critical=true. Call this before marking the
	// API ready so queries and rollbacks work immediately; follow
	// up with BuildDeferredIndexes to finish the lazy remainder.
	BuildCriticalDeferredIndexes() error
	BuildDeferredIndexes() error
	HasDeferredIndexesPending() (bool, error)
}
