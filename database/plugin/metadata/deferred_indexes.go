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

import (
	"context"
	"time"
)

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

// ContextDeferredIndexDropper is the cancellable form of the deferred-index
// drop used by bulk-load operations.
type ContextDeferredIndexDropper interface {
	DropDeferredIndexesContext(ctx context.Context) error
}

// LazyDeferredIndexDropper is an optional companion to DeferredIndexManager
// for callers that keep serving while they write in bulk: it drops only the
// non-critical manifest entries (deferred.LazyManifest), so every index the
// API, rollback and block application read stays in place.
//
// It records the deferred.SyncStateKey marker before any DROP runs, exactly
// as DropDeferredIndexes does, so an interrupted cycle is repaired by the
// same startup maintenance, and BuildDeferredIndexes ends the cycle.
type LazyDeferredIndexDropper interface {
	DropLazyDeferredIndexesContext(ctx context.Context) error
}

// CatchUpIndexManager is an optional companion to DeferredIndexManager for a
// node-managed Mithril stream catch-up that holds network and chain ingest
// while it applies certified blocks. Its drop goes further than
// LazyDeferredIndexDropper: it also drops deferred.CatchUpDroppableManifest,
// the critical entries only rollback, the API and LocalStateQuery read.
//
// The caller must rebuild those with BuildCatchUpIndexesContext before it
// lets ingest resume. The drop records the deferred.SyncStateKey marker first,
// as DropDeferredIndexes does, so a process that dies before the rebuild
// leaves startup maintenance to restore every critical entry before the node
// ingests again.
type CatchUpIndexManager interface {
	// DropCatchUpIndexesContext records the marker and drops the lazy and
	// catch-up droppable manifest entries in one transaction.
	DropCatchUpIndexesContext(ctx context.Context) error
	// BuildCatchUpIndexesContext rebuilds any missing catch-up droppable
	// entry. It leaves the marker set: the lazy entries the drop removed are
	// still missing, and BuildDeferredIndexes ends the cycle.
	BuildCatchUpIndexesContext(ctx context.Context) error
}

// MissingCriticalDeferredIndexLister is an optional companion to
// DeferredIndexManager: it answers which critical manifest entries are absent
// without building anything.
//
// BuildCriticalDeferredIndexes is silent while it runs, and building one index
// on a multi-million-row table takes long enough that an operator watching
// startup sees nothing at all until it finishes. A caller that can name the
// missing entries first can attribute that wait while it is happening.
// Stores that do not implement this report nothing and fall back to logging
// after the fact.
type MissingCriticalDeferredIndexLister interface {
	// MissingCriticalDeferredIndexes returns the names of missing
	// Critical=true manifest entries, in manifest order. It performs no DDL.
	MissingCriticalDeferredIndexes() ([]string, error)
}

// ContextMissingCriticalDeferredIndexLister is the cancellable form of
// MissingCriticalDeferredIndexLister for startup and restore operations.
type ContextMissingCriticalDeferredIndexLister interface {
	MissingCriticalDeferredIndexesContext(
		ctx context.Context,
	) ([]string, error)
}

// MissingDeferredIndexLister lists missing entries in the complete deferred
// index manifest, for restore paths that rebuild the full manifest.
type MissingDeferredIndexLister interface {
	// MissingDeferredIndexes returns the names of the manifest entries
	// missing from the schema, in manifest order. It performs no DDL.
	MissingDeferredIndexes() ([]string, error)
}

// ContextMissingDeferredIndexLister is an optional companion to
// MissingDeferredIndexLister for cancellable operations such as restore.
type ContextMissingDeferredIndexLister interface {
	MissingDeferredIndexesContext(ctx context.Context) ([]string, error)
}

// ContextDeferredIndexBuilder is an optional companion to
// DeferredIndexManager for callers that own a cancellable context.
//
// BuildDeferredIndexes runs its DDL on a context the store creates, so a
// caller whose own context is cancelled still waits for the build to finish.
// Restore stages a full rebuild inside a cancellable operation and uses this
// instead; stores that do not implement it fall back to the uninterruptible
// call.
type ContextDeferredIndexBuilder interface {
	// BuildDeferredIndexesContext is BuildDeferredIndexes bound to ctx.
	BuildDeferredIndexesContext(ctx context.Context) error
}

// ContextCriticalDeferredIndexBuilder rebuilds the startup-critical subset
// using the caller's cancellation context.
type ContextCriticalDeferredIndexBuilder interface {
	BuildCriticalDeferredIndexesContext(ctx context.Context) error
}

// CriticalDeferredIndexProgressBuilder is the critical-subset counterpart of
// DeferredIndexProgressBuilder, for startup and reinitialization repairs that
// block chain ingest while they run and so must say what they are doing.
type CriticalDeferredIndexProgressBuilder interface {
	// BuildCriticalDeferredIndexesContextWithProgress rebuilds missing
	// critical entries with ctx and leaves the recovery marker alone. before
	// runs immediately before each DDL statement; after runs after a
	// successful build with its elapsed duration.
	BuildCriticalDeferredIndexesContextWithProgress(
		ctx context.Context,
		before func(string),
		after func(string, time.Duration),
	) error
}

// DeferredIndexProgressBuilder is an optional companion used by restore
// paths that need progress while a large index build is running.
type DeferredIndexProgressBuilder interface {
	// BuildDeferredIndexesContextWithProgress rebuilds missing full-manifest
	// indexes with ctx. before runs immediately before each DDL statement;
	// after runs after a successful build with its elapsed duration.
	BuildDeferredIndexesContextWithProgress(
		ctx context.Context,
		before func(string),
		after func(string, time.Duration),
	) error
}
