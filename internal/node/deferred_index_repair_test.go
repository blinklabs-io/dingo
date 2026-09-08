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
	"bytes"
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// rollbackCascadeIndex is the child index for fk_transaction_outputs, the
// foreign key the rollback's DELETE FROM "transaction" cascades through.
const rollbackCascadeIndex = "idx_utxo_transaction_id"

// seedClearedMarkerWithMissingCriticalIndex leaves the database in the state
// two Mithril-bootstrapped preview nodes were found in: a critical manifest
// index absent, and no pending marker to say so.
//
// Mithril sync produced it: BuildCritical leaves the marker set for the lazy
// remainder, and updateMithrilReadyState then runs ClearSyncState, an
// unqualified DELETE FROM sync_state, which removes it (fixed for new syncs in
// mithril/sync_import.go; databases bootstrapped before that fix stay in this
// state). The migration that created the index is recorded complete, so its
// CREATE INDEX IF NOT EXISTS never runs again. Dropping the index directly is
// the only way to reproduce it here: no store method can express "manifest
// incomplete, cycle finished".
func seedClearedMarkerWithMissingCriticalIndex(
	t *testing.T,
	db *database.Database,
) *sql.DB {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DROP INDEX IF EXISTS " + rollbackCascadeIndex)
	require.NoError(t, err)
	require.False(
		t,
		metadataIndexExists(t, raw, rollbackCascadeIndex),
		"%s must be absent for this to test the repair",
		rollbackCascadeIndex,
	)
	_, err = raw.Exec(
		"DELETE FROM sync_state WHERE sync_key = ?",
		deferred.SyncStateKey,
	)
	require.NoError(t, err)
	return raw
}

func metadataIndexExists(t *testing.T, raw *sql.DB, name string) bool {
	t.Helper()
	var count int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
		name,
	).Scan(&count))
	return count == 1
}

// TestRepairDeferredIndexesRestoresCriticalIndexWithoutMarker covers the
// core-mode serve path: cmd/dingo/serve.go calls RepairDeferredIndexes for
// every non-API storage mode.
//
// The pending marker records that a bulk-load cycle was interrupted. It does
// not record which indexes exist, so it cannot answer the question the
// rollback path asks. A database whose marker a Mithril sync wiped after the
// critical rebuild carries the gap permanently: nothing else recreates an
// index the schema migration already claims to have built.
func TestRepairDeferredIndexesRestoresCriticalIndexWithoutMarker(
	t *testing.T,
) {
	db := newFileTestDB(t)
	raw := seedClearedMarkerWithMissingCriticalIndex(t, db)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	require.NoError(t, RepairDeferredIndexes(db, logger))

	require.True(
		t,
		metadataIndexExists(t, raw, rollbackCascadeIndex),
		"core-mode serve must restore %s before the node can roll back",
		rollbackCascadeIndex,
	)
	requireNoPendingMarker(t, raw)
}

// TestRepairCriticalDeferredIndexesRestoresCriticalIndexWithoutMarker covers
// the API-mode path: resumeBackfill calls RepairCriticalDeferredIndexes both
// when it runs a backfill and when it finds none needed, and a node in either
// state begins rolling back as soon as live sync resumes.
func TestRepairCriticalDeferredIndexesRestoresCriticalIndexWithoutMarker(
	t *testing.T,
) {
	db := newFileTestDB(t)
	raw := seedClearedMarkerWithMissingCriticalIndex(t, db)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	require.NoError(t, RepairCriticalDeferredIndexes(db, logger))

	require.True(
		t,
		metadataIndexExists(t, raw, rollbackCascadeIndex),
		"the critical repair must restore %s",
		rollbackCascadeIndex,
	)
	requireNoPendingMarker(t, raw)
}

// requireNoPendingMarker asserts the repair did not invent a bulk-load cycle:
// the marker means "a drop/rebuild is outstanding", and setting it on a
// database with a complete manifest would send the next startup through a
// rebuild it does not need.
func requireNoPendingMarker(t *testing.T, raw *sql.DB) {
	t.Helper()
	var count int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM sync_state WHERE sync_key = ?",
		deferred.SyncStateKey,
	).Scan(&count))
	require.Zero(
		t,
		count,
		"repairing a missing index must not mark a rebuild pending",
	)
}

// TestRepairDeferredIndexesFinishesPendingCycle keeps the marker's existing
// meaning intact: when a cycle really was interrupted, the core-mode repair
// still rebuilds the whole manifest and clears it.
func TestRepairDeferredIndexesFinishesPendingCycle(t *testing.T) {
	db := newFileTestDB(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	lazy := lazyManifestIndex(t)
	for _, name := range []string{rollbackCascadeIndex, lazy} {
		_, err = raw.Exec("DROP INDEX IF EXISTS " + name)
		require.NoError(t, err)
	}
	_, err = raw.Exec(
		`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
		 ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`,
		deferred.SyncStateKey,
		deferred.SyncStateValue,
	)
	require.NoError(t, err)

	require.NoError(t, RepairDeferredIndexes(
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))

	require.True(t, metadataIndexExists(t, raw, rollbackCascadeIndex))
	require.True(
		t,
		metadataIndexExists(t, raw, lazy),
		"a pending cycle must still finish the lazy remainder",
	)
	requireNoPendingMarker(t, raw)
}

// lazyManifestIndex returns the name of a non-critical manifest entry, so the
// test above distinguishes the full rebuild from the critical subset without
// pinning a specific index the manifest may reclassify later.
func lazyManifestIndex(t *testing.T) string {
	t.Helper()
	for _, index := range deferred.Manifest {
		if !index.Critical {
			return index.Name
		}
	}
	t.Fatal("the manifest must keep at least one lazy entry")
	return ""
}

// namedMissingManager is a DeferredIndexManager that also lists its missing
// critical entries, and records what had already been logged when the rebuild
// was entered.
type namedMissingManager struct {
	missing []string
	logged  string
	log     *bytes.Buffer
}

func (m *namedMissingManager) DropDeferredIndexes() error { return nil }

func (m *namedMissingManager) BuildCriticalDeferredIndexes() error {
	m.logged = m.log.String()
	return nil
}

func (m *namedMissingManager) BuildDeferredIndexes() error { return nil }

func (m *namedMissingManager) HasDeferredIndexesPending() (bool, error) {
	return false, nil
}

func (m *namedMissingManager) MissingCriticalDeferredIndexes() (
	[]string,
	error,
) {
	return m.missing, nil
}

// TestEnsureCriticalDeferredIndexesNamesMissingBeforeBuilding pins the
// ordering: the names are logged before the rebuild is entered, not after it
// returns.
//
// A rebuild of one index on a multi-million-row table takes minutes and emits
// nothing while it runs, which is the silence the reported incident opened
// with. The assertion reads the log as the rebuild sees it, so a message moved
// back below the build fails here.
func TestEnsureCriticalDeferredIndexesNamesMissingBeforeBuilding(
	t *testing.T,
) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	manager := &namedMissingManager{
		missing: []string{rollbackCascadeIndex, "idx_utxo_added_slot"},
		log:     &buf,
	}

	require.NoError(t, ensureCriticalDeferredIndexes(manager, logger))

	require.Contains(
		t,
		manager.logged,
		"rebuilding missing critical deferred metadata indexes",
		"the rebuild must be announced before it starts",
	)
	require.Contains(t, manager.logged, rollbackCascadeIndex)
	require.Contains(t, manager.logged, "idx_utxo_added_slot")
	require.Contains(
		t,
		buf.String(),
		"critical deferred metadata index check complete",
		"the completion line still reports the outcome",
	)
	require.Contains(
		t,
		buf.String(),
		"duration_covers",
		"the reported duration must say what it covers: the rebuild "+
			"runs inside withDeferredIndexWrite, which restores missing "+
			"retained indexes in the same write transaction",
	)
}

// TestEnsureCriticalDeferredIndexesQuietWhenManifestComplete keeps the healthy
// path silent: a complete manifest is one catalog lookup per entry and must
// not add a startup log line.
func TestEnsureCriticalDeferredIndexesQuietWhenManifestComplete(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	manager := &namedMissingManager{log: &buf}

	require.NoError(t, ensureCriticalDeferredIndexes(manager, logger))

	require.Empty(t, buf.String())
}

// TestRepairDeferredIndexesNamesMissingIndexAgainstRealStore runs the same
// path against the SQLite store, so the listing is exercised through
// MissingCriticalDeferredIndexes and the real catalog rather than a fake.
func TestRepairDeferredIndexesNamesMissingIndexAgainstRealStore(t *testing.T) {
	db := newFileTestDB(t)
	raw := seedClearedMarkerWithMissingCriticalIndex(t, db)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	require.NoError(t, RepairDeferredIndexes(db, logger))

	require.True(t, metadataIndexExists(t, raw, rollbackCascadeIndex))
	require.Contains(
		t,
		buf.String(),
		"rebuilding missing critical deferred metadata indexes",
	)
	require.Contains(
		t,
		buf.String(),
		rollbackCascadeIndex,
		"the operator must be told which index the wait is for",
	)
}
