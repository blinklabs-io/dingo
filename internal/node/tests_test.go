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
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
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
		dbtest.MetadataIndexExists(t, raw, rollbackCascadeIndex),
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
		dbtest.MetadataIndexExists(t, raw, rollbackCascadeIndex),
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
		dbtest.MetadataIndexExists(t, raw, rollbackCascadeIndex),
		"the critical repair must restore %s",
		rollbackCascadeIndex,
	)
	requireNoPendingMarker(t, raw)
}

// TestRepairDeferredIndexesRestoresLazyIndexWithoutMarker covers a restored
// database whose complete migration history hides a missing lazy index.
func TestRepairDeferredIndexesRestoresLazyIndexWithoutMarker(t *testing.T) {
	db := newFileTestDB(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	lazy := dbtest.LazyManifestIndex(t)
	_, err = raw.Exec("DROP INDEX IF EXISTS " + lazy)
	require.NoError(t, err)
	_, err = raw.Exec(
		"DELETE FROM sync_state WHERE sync_key = ?", deferred.SyncStateKey,
	)
	require.NoError(t, err)

	require.NoError(t, RepairDeferredIndexes(
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))

	require.True(t, dbtest.MetadataIndexExists(t, raw, lazy))
	requireNoPendingMarker(t, raw)
}

// TestRepairCriticalDeferredIndexesRestoresLazyIndexWithoutMarker covers the
// API-mode startup path, which must not leave a restored database missing its
// non-critical manifest entries once no bulk-load cycle is active.
func TestRepairCriticalDeferredIndexesRestoresLazyIndexWithoutMarker(
	t *testing.T,
) {
	db := newFileTestDB(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	lazy := dbtest.LazyManifestIndex(t)
	_, err = raw.Exec("DROP INDEX IF EXISTS " + lazy)
	require.NoError(t, err)
	_, err = raw.Exec(
		"DELETE FROM sync_state WHERE sync_key = ?", deferred.SyncStateKey,
	)
	require.NoError(t, err)

	require.NoError(t, RepairCriticalDeferredIndexes(
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))

	require.True(t, dbtest.MetadataIndexExists(t, raw, lazy))
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
	lazy := dbtest.LazyManifestIndex(t)
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

	require.True(t, dbtest.MetadataIndexExists(t, raw, rollbackCascadeIndex))
	require.True(
		t,
		dbtest.MetadataIndexExists(t, raw, lazy),
		"a pending cycle must still finish the lazy remainder",
	)
	requireNoPendingMarker(t, raw)
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

	require.True(t, dbtest.MetadataIndexExists(t, raw, rollbackCascadeIndex))
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

// namedMissingAllManager answers the full-manifest lister and records the log
// as BuildDeferredIndexes sees it, so the ordering assertion below reads the
// same "before the build" evidence its critical-subset sibling does.
type namedMissingAllManager struct {
	missing []string
	logged  string
	log     *bytes.Buffer
}

func (m *namedMissingAllManager) DropDeferredIndexes() error { return nil }

func (m *namedMissingAllManager) BuildCriticalDeferredIndexes() error {
	return nil
}

func (m *namedMissingAllManager) BuildDeferredIndexes() error {
	m.logged = m.log.String()
	return nil
}

func (m *namedMissingAllManager) HasDeferredIndexesPending() (bool, error) {
	return false, nil
}

func (m *namedMissingAllManager) MissingDeferredIndexes() ([]string, error) {
	return m.missing, nil
}

// TestEnsureAllDeferredIndexesNamesMissingBeforeBuilding covers the repair a
// restored database takes: the full manifest is rebuilt, and that rebuild is
// as silent while it runs as the critical subset's, with more entries to get
// through. The names must reach the log before the build is entered.
func TestEnsureAllDeferredIndexesNamesMissingBeforeBuilding(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	manager := &namedMissingAllManager{
		missing: []string{"idx_asset_fingerprint", "idx_utxo_added_slot"},
		log:     &buf,
	}

	require.NoError(t, ensureAllDeferredIndexes(manager, logger))

	require.Contains(
		t,
		manager.logged,
		"rebuilding missing deferred metadata indexes",
		"the rebuild must be announced before it starts",
	)
	require.Contains(t, manager.logged, "idx_asset_fingerprint")
	require.Contains(t, manager.logged, "idx_utxo_added_slot")
	require.Contains(
		t,
		buf.String(),
		"deferred metadata index check complete",
	)
	require.Contains(t, buf.String(), "duration")
}

// TestEnsureAllDeferredIndexesQuietWhenManifestComplete keeps the healthy
// path silent, matching the critical-subset check.
func TestEnsureAllDeferredIndexesQuietWhenManifestComplete(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	manager := &namedMissingAllManager{log: &buf}

	require.NoError(t, ensureAllDeferredIndexes(manager, logger))

	require.Empty(t, buf.String())
}

// TestRepairDeferredIndexesAnnouncesLazyRebuild drives the real store through
// the startup repair path and requires the lazy entry it rebuilds to be named
// in the log, not rebuilt silently.
func TestRepairDeferredIndexesAnnouncesLazyRebuild(t *testing.T) {
	db := newFileTestDB(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	lazy := dbtest.LazyManifestIndex(t)
	_, err = raw.Exec("DROP INDEX IF EXISTS " + lazy)
	require.NoError(t, err)
	_, err = raw.Exec(
		"DELETE FROM sync_state WHERE sync_key = ?", deferred.SyncStateKey,
	)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, RepairDeferredIndexes(
		db,
		slog.New(slog.NewTextHandler(&buf, nil)),
	))

	require.Contains(t, buf.String(), "rebuilding missing deferred metadata")
	require.Contains(t, buf.String(), lazy)
	require.True(t, dbtest.MetadataIndexExists(t, raw, lazy))
}

// TestBuildDingoConfigWiresForgeEBCaps follows the composition path the
// binary actually takes -- buildDingoConfig -> dingo.NewConfig -> the
// With... option -> the dingo.Config field the forger is built from. A yaml
// tag, a default and a getter prove nothing on their own: a missing With...
// call here silently drops the field and the cap never reaches the forge
// loop.
func TestBuildDingoConfigWiresForgeEBCaps(t *testing.T) {
	t.Parallel()

	refs, maxBytes := uint64(1234), uint64(567890)
	cfg := &config.Config{
		ForgeEBMaxTxRefs: &refs,
		ForgeEBMaxBytes:  &maxBytes,
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBMaxTxRefs(); got == nil || *got != 1234 {
		t.Fatalf("expected forgeEbMaxTxRefs 1234 to flow through, got %v", got)
	}
	if got := built.ForgeEBMaxBytes(); got == nil || *got != 567890 {
		t.Fatalf("expected forgeEbMaxBytes 567890 to flow through, got %v", got)
	}
}

func TestBuildDingoConfigWiresLeiosPersistenceRetention(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	built := buildDingoConfig(
		&config.Config{LeiosPersistenceRetentionSlots: 12345},
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)
	require.Equal(t, uint64(12345), built.LeiosPersistenceRetentionSlots())
}

// TestBuildDingoConfigWiresLocalStateQueryViewMaxLifetime follows the
// composition path for the LocalStateQuery snapshot lifetime: a missing
// With... call would drop the operator's value and leave the default.
func TestBuildDingoConfigWiresLocalStateQueryViewMaxLifetime(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{LocalStateQueryViewMaxLifetime: "7m"}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	require.Equal(
		t,
		7*time.Minute,
		built.LocalStateQueryViewMaxLifetimeDuration(),
	)
}

// TestBuildDingoConfigPreservesExplicitZeroForgeEBCaps carries the
// zero-means-disabled contract through the composition path: an operator
// who wrote 0 must not have it replaced by the default on the way to the
// forger.
func TestBuildDingoConfigPreservesExplicitZeroForgeEBCaps(t *testing.T) {
	t.Parallel()

	zero := uint64(0)
	cfg := &config.Config{ForgeEBMaxTxRefs: &zero, ForgeEBMaxBytes: &zero}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBMaxTxRefs(); got == nil || *got != 0 {
		t.Fatalf("explicit zero forgeEbMaxTxRefs must survive, got %v", got)
	}
	if got := built.ForgeEBMaxBytes(); got == nil || *got != 0 {
		t.Fatalf("explicit zero forgeEbMaxBytes must survive, got %v", got)
	}
}

// TestBuildDingoConfigDefaultsUnsetForgeEBCaps covers a Config built
// directly rather than loaded: Load applies the defaults, so nil here
// means nobody did, and the cap must still be the backstop rather than
// zero (which would mean "disabled").
func TestBuildDingoConfigDefaultsUnsetForgeEBCaps(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	built := buildDingoConfig(
		&config.Config{},
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBMaxTxRefs(); got == nil ||
		*got != config.DefaultForgeEBMaxTxRefs {
		t.Fatalf("unset forgeEbMaxTxRefs must take the default, got %v", got)
	}
	if got := built.ForgeEBMaxBytes(); got == nil ||
		*got != config.DefaultForgeEBMaxBytes {
		t.Fatalf("unset forgeEbMaxBytes must take the default, got %v", got)
	}
}

// TestBuildDingoConfigWiresForgeEBSelectionReserve follows the same
// composition path for the selection reserve. Without the With... call
// here the field is dropped between the loaded configuration and the
// forger, so every deployment silently runs the built-in default however
// the operator set it.
func TestBuildDingoConfigWiresForgeEBSelectionReserve(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{ForgeEBSelectionReserve: 750 * time.Millisecond}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.ForgeEBSelectionReserve(); got != 750*time.Millisecond {
		t.Fatalf("expected forgeEbSelectionReserve 750ms to flow through, got %s", got)
	}
}

// TestBuildDingoConfigCarriesTokenRegistry exercises the composition the node
// actually uses. buildDingoConfig maps internal config onto dingo.With*
// options field by field, so a family with no With* call here is silently
// dropped no matter how well it parses -- an operator's tokenRegistry block
// would be read, validated, and then ignored.
//
// dingo.NewConfigFromInternal is a different path and cannot catch this.
func TestBuildDingoConfigCarriesTokenRegistry(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		TokenRegistry: config.TokenRegistryConfig{
			Enabled:               true,
			SourceURL:             "https://mirror.example.test/reg.tar.gz",
			Interval:              2 * time.Hour,
			RequestTimeout:        9 * time.Minute,
			UserAgent:             "custom-agent/9",
			MaxBytes:              123,
			MaxDecompressedBytes:  456,
			MaxEntryBytes:         45,
			MaxArchiveEntries:     67,
			MaxAcceptedEntries:    34,
			MaxBatchBytes:         89,
			StoreLogos:            true,
			AllowPrivateAddresses: true,
		},
	}

	built := buildDingoConfig(
		cfg,
		nil,
		nil,
		nil,
		false,
		dingo.StorageModeAPI,
		time.Minute,
		time.Minute,
		0,
	)

	got := built.TokenRegistry()
	require.True(t, got.Enabled, "tokenRegistry.enabled must reach the node")
	require.Equal(t, "https://mirror.example.test/reg.tar.gz", got.SourceURL)
	require.Equal(t, 2*time.Hour, got.Interval)
	require.Equal(t, 9*time.Minute, got.RequestTimeout)
	require.Equal(t, "custom-agent/9", got.UserAgent)
	require.Equal(t, int64(123), got.MaxBytes)
	require.Equal(t, int64(456), got.MaxDecompressedBytes)
	require.Equal(t, int64(45), got.MaxEntryBytes)
	require.Equal(t, 67, got.MaxArchiveEntries)
	require.Equal(t, 34, got.MaxAcceptedEntries)
	require.Equal(t, int64(89), got.MaxBatchBytes)
	require.True(t, got.StoreLogos)
	require.True(t, got.AllowPrivateAddresses)
}

// TestBuildDingoConfigTokenRegistryDefaultsOff pins the deliberate default
// through the same production path.
func TestBuildDingoConfigTokenRegistryDefaultsOff(t *testing.T) {
	t.Parallel()

	built := buildDingoConfig(
		&config.Config{},
		nil,
		nil,
		nil,
		false,
		dingo.StorageModeAPI,
		time.Minute,
		time.Minute,
		0,
	)

	require.False(t, built.TokenRegistry().Enabled)
}
