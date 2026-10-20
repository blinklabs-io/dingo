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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/stretchr/testify/require"
)

// TestBuildDeferredIndexesContextHonoursCancellation proves the store's
// context-aware rebuild actually reaches the database driver with the
// caller's context, rather than accepting one and building on
// context.Background() anyway. Restore relies on this: an uninterruptible
// full-manifest rebuild outlives a cancelled restore for as long as its
// largest index takes.
func TestBuildDeferredIndexesContextHonoursCancellation(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, store.DropDeferredIndexes())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(
		t,
		store.BuildDeferredIndexesContext(ctx),
		context.Canceled,
	)

	// The cancelled attempt must leave the recovery marker in place so the
	// next run still knows a rebuild is outstanding.
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)

	require.NoError(t, store.BuildDeferredIndexesContext(context.Background()))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, missing)
	require.NotEmpty(t, deferred.Manifest)
}

func TestDropDeferredIndexesContextCancelsWhileWriteLocked(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	store.writeDB.SetMaxOpenConns(1)

	holder, err := store.writeDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = holder.ExecContext(
		t.Context(),
		`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)`,
		"deferred-index-drop-lock",
		"held",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback() })
	forceRelease := time.AfterFunc(time.Second, func() { _ = holder.Rollback() })
	defer forceRelease.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = store.DropDeferredIndexesContext(ctx)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(
		t,
		elapsed,
		2*time.Second,
		"drop must return on cancellation instead of waiting out busy_timeout",
	)

	forceRelease.Stop()
	require.NoError(t, holder.Rollback())
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, missing)
}

// TestMissingDeferredIndexesNamesDroppedEntries covers the full-manifest
// lister the repair paths use to announce a rebuild before it starts.
func TestMissingDeferredIndexesNamesDroppedEntries(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, missing)

	require.NoError(t, store.DropDeferredIndexes())
	missing, err = store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.NotEmpty(t, missing)
	critical, err := store.MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.Subset(t, missing, critical)
}

func TestBuildDeferredIndexesContextWithProgressReportsOnlyMissingIndexes(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	var before, after []string
	require.NoError(t, store.BuildDeferredIndexesContextWithProgress(
		t.Context(),
		func(name string) { before = append(before, name) },
		func(name string, _ time.Duration) { after = append(after, name) },
	))
	require.Empty(t, before, "present indexes need no build progress events")
	require.Empty(t, after, "present indexes need no build progress events")

	missing := deferred.Manifest[0]
	_, err := store.writeDB.ExecContext(
		t.Context(),
		store.dialect.DropIndexSQL(missing.Name, missing.Table),
	)
	require.NoError(t, err)

	before = nil
	after = nil
	require.NoError(t, store.BuildDeferredIndexesContextWithProgress(
		t.Context(),
		func(name string) { before = append(before, name) },
		func(name string, _ time.Duration) {
			after = append(after, name)
		},
	))
	require.Equal(t, []string{missing.Name}, before)
	require.Equal(t, []string{missing.Name}, after)
	exists, err := store.deferredIndexExists(
		t.Context(),
		store.readDB,
		missing,
	)
	require.NoError(t, err)
	require.True(t, exists, "the after callback follows the CREATE INDEX")
}

func TestBuildDeferredIndexesContextCommitsEachSQLiteIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, store.DropDeferredIndexes())
	missingBefore, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(missingBefore), 2)

	ctx, cancel := context.WithCancel(t.Context())
	var firstBuilt string
	err = store.BuildDeferredIndexesContextWithProgress(
		ctx,
		nil,
		func(name string, _ time.Duration) {
			if firstBuilt != "" {
				return
			}
			firstBuilt = name
			cancel()
		},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, missingBefore[0], firstBuilt)

	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)
	missingAfter, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.NotContains(
		t,
		missingAfter,
		firstBuilt,
		"the completed index remains committed after cancellation",
	)
	require.NotEmpty(t, missingAfter, "cancellation leaves unfinished indexes to resume")

	require.NoError(t, store.BuildDeferredIndexesContext(t.Context()))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, missing)
}

// TestDropLazyDeferredIndexesKeepsCriticalIndexes drops only the lazy
// manifest entries, behind the same recovery marker a full drop records, and
// BuildDeferredIndexes ends that cycle.
func TestDropLazyDeferredIndexesKeepsCriticalIndexes(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, store.DropLazyDeferredIndexesContext(t.Context()))

	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending, "a lazy drop must record the recovery marker")
	critical, err := store.MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, critical, "a lazy drop removed a critical index")
	var lazy []string
	for _, index := range deferred.LazyManifest() {
		lazy = append(lazy, index.Name)
	}
	require.NotEmpty(t, lazy)
	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.ElementsMatch(t, lazy, missing)

	require.NoError(t, store.BuildDeferredIndexesContext(t.Context()))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
	missing, err = store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, missing)
}

// TestCatchUpIndexDropAndRebuild drops the lazy and catch-up droppable
// entries behind the recovery marker. The catch-up rebuild restores the
// droppable ones and keeps the marker, which only the full rebuild clears.
func TestCatchUpIndexDropAndRebuild(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	var lazy, droppable []string
	for _, index := range deferred.LazyManifest() {
		lazy = append(lazy, index.Name)
	}
	for _, index := range deferred.CatchUpDroppableManifest() {
		droppable = append(droppable, index.Name)
	}
	require.NotEmpty(t, droppable)
	require.NoError(t, store.DropCatchUpIndexesContext(t.Context()))

	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending, "a catch-up drop must record the recovery marker")
	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.ElementsMatch(t, append(append([]string(nil), lazy...),
		droppable...), missing)
	critical, err := store.MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.ElementsMatch(t, droppable, critical)

	require.NoError(t, store.BuildCatchUpIndexesContext(t.Context()))
	missing, err = store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.ElementsMatch(t, lazy, missing,
		"the catch-up rebuild must restore exactly the droppable entries")
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending, "the lazy entries are still missing")

	require.NoError(t, store.BuildDeferredIndexesContext(t.Context()))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
}

// TestSetCatchUpPragmasLeavesDurabilityAlone checks the catch-up tuning
// changes only memory settings on the write connection and that
// RestoreNormalPragmas puts them back.
func TestSetCatchUpPragmasLeavesDurabilityAlone(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	store.writeDB.SetMaxOpenConns(1)
	pragma := func(name string) int64 {
		t.Helper()
		var value int64
		require.NoError(t, store.writeDB.QueryRowContext(
			t.Context(), "PRAGMA "+name,
		).Scan(&value))
		return value
	}
	synchronous := pragma("synchronous")
	journal := func() string {
		t.Helper()
		var mode string
		require.NoError(t, store.writeDB.QueryRowContext(
			t.Context(), "PRAGMA journal_mode",
		).Scan(&mode))
		return mode
	}
	journalMode := journal()

	require.NoError(t, store.SetCatchUpPragmas())
	require.Equal(t, int64(-200000), pragma("cache_size"))
	require.Equal(t, int64(2), pragma("temp_store"), "temp_store = MEMORY")
	require.Equal(t, synchronous, pragma("synchronous"),
		"catch-up tuning must not relax durability")
	require.Equal(t, journalMode, journal())
	require.False(t, store.bulkMode, "catch-up tuning is not bulk mode")

	require.NoError(t, store.RestoreNormalPragmas())
	require.Equal(t, int64(-50000), pragma("cache_size"))
	require.Equal(t, int64(0), pragma("temp_store"), "temp_store = DEFAULT")
}

// A bulk-load cycle drops the spender indexes and the rebuild recreates them.
// The rebuild must restore the partial definition the schema migration
// installed, not a full-table index over the mostly-NULL columns.
func TestDeferredRebuildKeepsUtxoSpenderIndexesPartial(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildDeferredIndexes())

	for _, column := range []string{
		"spent_at_tx_id",
		"collateral_by_tx_id",
		"referenced_by_tx_id",
	} {
		var definition string
		require.NoError(t, store.readDB.QueryRow(
			"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
			"idx_utxo_"+column,
		).Scan(&definition))
		require.Contains(t, definition, "WHERE "+column+" IS NOT NULL",
			"rebuilt idx_utxo_%s must stay partial: %s", column, definition)
	}
}

// MySQL has no partial indexes and needs a full index on each foreign-key
// column, so a deferred index predicate applies only to SQLite and Postgres.
func TestDeferredIndexSQLPartialPredicateByDialect(t *testing.T) {
	t.Parallel()
	index := deferred.Index{
		Name:    "idx_utxo_spent_at_tx_id",
		Table:   "utxo",
		Columns: []string{"spent_at_tx_id"},
		Where:   "spent_at_tx_id IS NOT NULL",
	}
	require.Equal(
		t,
		`CREATE INDEX IF NOT EXISTS "idx_utxo_spent_at_tx_id" ON "utxo" ("spent_at_tx_id") WHERE spent_at_tx_id IS NOT NULL`,
		deferredIndexCreateSQL(SQLiteDialect(), index),
	)
	require.Equal(
		t,
		`CREATE INDEX IF NOT EXISTS "idx_utxo_spent_at_tx_id" ON "utxo" ("spent_at_tx_id") WHERE spent_at_tx_id IS NOT NULL`,
		deferredIndexCreateSQL(PostgresDialect(), index),
	)
	require.Equal(
		t,
		"CREATE INDEX `idx_utxo_spent_at_tx_id` ON `utxo` (`spent_at_tx_id`(255))",
		deferredIndexCreateSQL(MySQLDialect(), index),
	)
}
