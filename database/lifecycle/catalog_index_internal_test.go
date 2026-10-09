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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	driversqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

type catalogRepairDestination struct {
	dir string
}

type catalogScriptedLister struct {
	entries []SnapshotEntry
	err     error
}

func (*catalogScriptedLister) UploadDir(context.Context, string) error { return nil }
func (*catalogScriptedLister) DownloadDir(context.Context, string) error {
	return nil
}
func (d *catalogScriptedLister) ListSnapshots(context.Context) ([]SnapshotEntry, error) {
	return d.entries, d.err
}

type catalogUnsupportedDestination struct{}

func (*catalogUnsupportedDestination) UploadDir(context.Context, string) error { return nil }
func (*catalogUnsupportedDestination) DownloadDir(context.Context, string) error {
	return nil
}

type blockingCatalogLister struct {
	entries  []SnapshotEntry
	captured chan struct{}
	release  chan struct{}
}

func (*blockingCatalogLister) UploadDir(context.Context, string) error { return nil }
func (*blockingCatalogLister) DownloadDir(context.Context, string) error {
	return nil
}
func (d *blockingCatalogLister) ListSnapshots(ctx context.Context) ([]SnapshotEntry, error) {
	entries := append([]SnapshotEntry(nil), d.entries...)
	close(d.captured)
	select {
	case <-d.release:
		return entries, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *catalogRepairDestination) UploadDir(_ context.Context, localDir string) error {
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(localDir, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(d.dir, entry.Name()), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (*catalogRepairDestination) DownloadDir(context.Context, string) error {
	return nil
}

func (d *catalogRepairDestination) ListSnapshots(context.Context) ([]SnapshotEntry, error) {
	return ListSnapshots(d.dir)
}

func (d *catalogRepairDestination) Delete(context.Context) error {
	return os.RemoveAll(d.dir)
}

func TestSnapshotCatalogUsesCanonicalSQLiteConfiguration(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	db, err := openSnapshotCatalog(t.Context(), path, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var journalMode string
	require.NoError(t, db.QueryRow("PRAGMA journal_mode").Scan(&journalMode))
	require.Equal(t, "wal", strings.ToLower(journalMode))
	var busyTimeout int
	require.NoError(t, db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout))
	require.Equal(t, snapshotCatalogBusyMS, busyTimeout)
}

func TestSnapshotCatalogLockHonorsCancellation(t *testing.T) {
	snapshotCatalogMu <- struct{}{}
	t.Cleanup(func() { <-snapshotCatalogMu })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := EnsureSnapshotCatalogContext(ctx, t.TempDir())
	require.ErrorIs(t, err, context.Canceled)
}

func TestSnapshotCatalogContinuationUsesIndexSeek(t *testing.T) {
	t.Parallel()
	db, err := openSnapshotCatalog(t.Context(), ":memory:", true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, createSnapshotCatalogSchema(db))

	rows, err := db.Query(
		"EXPLAIN QUERY PLAN "+nextSnapshotPageQuery,
		int64(1_700_000_000), 123, "snapshot-050000", 101,
	)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	require.Contains(
		t,
		strings.Join(plan, "\n"),
		"SEARCH snapshots USING COVERING INDEX snapshots_created",
	)
}

func TestSnapshotCatalogRejectsTraversalRowBeforeManifestRead(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	base := filepath.Join(root, "snapshots")
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(base, 0o755))
	require.NoError(t, os.MkdirAll(outside, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(outside, ManifestFileName),
		[]byte("outside manifest must not be read"),
		0o644,
	))
	require.NoError(t, EnsureSnapshotCatalog(base))

	db, err := openSnapshotCatalog(
		t.Context(), snapshotCatalogPath(base), false,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO snapshots(id, created_sec, created_ns) VALUES (?, ?, ?)",
		filepath.Join("..", "outside"),
		time.Now().Unix(),
		0,
	)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	entries, _, err := ListSnapshotPage(base, 10, nil)
	require.ErrorIs(t, err, ErrSnapshotCatalogCorrupt)
	require.Empty(t, entries)
}

func TestSnapshotCatalogRejectsTraversalInsertion(t *testing.T) {
	t.Parallel()
	err := updateSnapshotCatalogIfPresent(t.Context(), t.TempDir(), SnapshotEntry{
		ID: filepath.Join("..", "outside"),
	})
	require.ErrorIs(t, err, ErrSnapshotCatalogCorrupt)
}

func TestRemoveSnapshotFinalizesCatalogAfterDeletionStarts(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	olderDir := filepath.Join(base, "older")
	newerDir := filepath.Join(base, "newer")
	require.NoError(t, os.Mkdir(olderDir, 0o755))
	require.NoError(t, os.Mkdir(newerDir, 0o755))
	require.NoError(t, WriteManifest(olderDir, Manifest{
		CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}))
	require.NoError(t, WriteManifest(newerDir, Manifest{
		CreatedAt: time.Unix(1_700_000_001, 0).UTC(),
	}))
	require.NoError(t, EnsureSnapshotCatalog(base))
	_, cursor, err := ListSnapshotPage(base, 1, nil)
	require.NoError(t, err)
	require.NotNil(t, cursor)

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, removeSnapshotContext(
		ctx,
		newerDir,
		func(path string) error {
			err := os.RemoveAll(path)
			cancel()
			return err
		},
	))
	require.NoDirExists(t, newerDir)
	_, _, err = ListSnapshotPage(base, 1, cursor)
	require.ErrorIs(t, err, ErrSnapshotCatalogChanged)
	entries, next, err := ListSnapshotPage(base, 1, nil)
	require.NoError(t, err)
	require.Nil(t, next)
	require.Len(t, entries, 1)
	require.Equal(t, "older", entries[0].ID)
}

func TestRemoveSnapshotRepairsCatalogAfterIndexFailure(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	removedDir := filepath.Join(base, "removed")
	keptDir := filepath.Join(base, "kept")
	for i, dir := range []string{removedDir, keptDir} {
		require.NoError(t, os.Mkdir(dir, 0o755))
		require.NoError(t, WriteManifest(dir, Manifest{
			CreatedAt: time.Unix(1_700_000_000+int64(i), 0).UTC(),
		}))
	}
	require.NoError(t, EnsureSnapshotCatalog(base))
	_, staleCursor, err := ListSnapshotPage(base, 1, nil)
	require.NoError(t, err)
	require.NotNil(t, staleCursor)
	require.NoError(t, os.WriteFile(
		snapshotCatalogPath(base), []byte("not a sqlite database"), 0o600,
	))

	const repairedGeneration = int64(4_242_424_242)
	require.NoError(t, removeSnapshotContextWithGenerationSource(
		t.Context(), removedDir, os.RemoveAll,
		func() (int64, error) { return repairedGeneration, nil },
	))
	require.NoDirExists(t, removedDir)
	_, _, err = ListSnapshotPage(base, 1, staleCursor)
	require.ErrorIs(t, err, ErrSnapshotCatalogChanged)
	entries, next, err := ListSnapshotPage(base, 10, nil)
	require.NoError(t, err)
	require.Nil(t, next)
	require.Len(t, entries, 1)
	require.Equal(t, "kept", entries[0].ID)
}

func TestEnsureSnapshotCatalogRepairsInvalidMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		schema string
		seed   string
	}{
		{
			name: "zero generation",
			schema: `CREATE TABLE catalog_meta (
singleton INTEGER PRIMARY KEY, generation INTEGER)`,
			seed: "INSERT INTO catalog_meta VALUES (1, 0)",
		},
		{
			name: "negative generation",
			schema: `CREATE TABLE catalog_meta (
singleton INTEGER PRIMARY KEY, generation INTEGER)`,
			seed: "INSERT INTO catalog_meta VALUES (1, -1)",
		},
		{
			name: "text generation",
			schema: `CREATE TABLE catalog_meta (
singleton INTEGER PRIMARY KEY, generation TEXT)`,
			seed: "INSERT INTO catalog_meta VALUES (1, 'broken')",
		},
		{
			name: "missing singleton",
			schema: `CREATE TABLE catalog_meta (
singleton INTEGER PRIMARY KEY, generation INTEGER)`,
		},
		{
			name: "wrong schema",
			schema: `CREATE TABLE catalog_meta (
singleton INTEGER PRIMARY KEY, epoch INTEGER)`,
			seed: "INSERT INTO catalog_meta VALUES (1, 7)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			base := t.TempDir()
			db, err := openSnapshotCatalog(
				t.Context(), snapshotCatalogPath(base), true,
			)
			require.NoError(t, err)
			_, err = db.Exec(test.schema)
			require.NoError(t, err)
			if test.seed != "" {
				_, err = db.Exec(test.seed)
				require.NoError(t, err)
			}
			require.NoError(t, db.Close())

			const repairedGeneration = int64(8_181_818_181)
			calls := 0
			require.NoError(t, ensureSnapshotCatalogContext(
				t.Context(), base,
				func() (int64, error) {
					calls++
					return repairedGeneration, nil
				},
			))
			require.Equal(t, 1, calls)
			repaired, err := openSnapshotCatalog(
				t.Context(), snapshotCatalogPath(base), false,
			)
			require.NoError(t, err)
			generation, err := readCatalogGeneration(t.Context(), repaired)
			require.NoError(t, err)
			require.Equal(t, uint64(repairedGeneration), generation)
			require.NoError(t, repaired.Close())
		})
	}
}

func TestEnsureSnapshotCatalogRepairsSQLiteCorruptSchema(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	path := snapshotCatalogPath(base)
	db, err := openSnapshotCatalog(t.Context(), path, true)
	require.NoError(t, err)
	require.NoError(t, createSnapshotCatalogSchema(db))
	_, err = db.Exec("PRAGMA writable_schema=ON")
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE sqlite_schema
SET sql = 'CREATE TABLE catalog_meta ('
WHERE name = 'catalog_meta'`)
	require.NoError(t, err)
	_, err = db.Exec("PRAGMA writable_schema=OFF")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = openSnapshotCatalog(t.Context(), path, false)
	require.Error(t, err)
	var sqliteErr *driversqlite.Error
	require.ErrorAs(t, err, &sqliteErr)
	require.Equal(t, sqlite3.SQLITE_CORRUPT, sqliteErr.Code()&0xff)

	const repairedGeneration = int64(9_191_919_191)
	require.NoError(t, ensureSnapshotCatalogContext(
		t.Context(), base,
		func() (int64, error) { return repairedGeneration, nil },
	))
	repaired, err := openSnapshotCatalog(t.Context(), path, false)
	require.NoError(t, err)
	actual, err := readCatalogGeneration(t.Context(), repaired)
	require.NoError(t, err)
	require.Equal(t, uint64(repairedGeneration), actual)
	require.NoError(t, repaired.Close())
}

func TestEnsureSnapshotCatalogRemovesStaleTargetSidecars(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	catalogPath := snapshotCatalogPath(base)
	for _, suffix := range []string{"-wal", "-shm"} {
		require.NoError(t, os.WriteFile(
			catalogPath+suffix, []byte("stale"), 0o600,
		))
	}

	require.NoError(t, EnsureSnapshotCatalog(base))
	for _, suffix := range []string{"-wal", "-shm"} {
		require.NoFileExists(t, catalogPath+suffix)
	}

	entries, next, err := ListSnapshotPage(base, 10, nil)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.Nil(t, next)
}

func TestWriteManifestReportsIncompleteCatalogRepair(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	brokenDir := filepath.Join(base, "broken")
	dir := filepath.Join(base, "complete")
	require.NoError(t, EnsureSnapshotCatalog(base))
	require.NoError(t, os.Mkdir(brokenDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(brokenDir, ManifestFileName), []byte("broken"), 0o600,
	))
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, os.WriteFile(
		snapshotCatalogPath(base), []byte("not a sqlite database"), 0o600,
	))

	err := writeManifest(t.Context(), dir, Manifest{
		CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
	})
	require.ErrorIs(t, err, ErrSnapshotCatalogUpdate)
	require.ErrorIs(t, err, ErrSnapshotCatalogIncomplete)
	_, readErr := ReadManifest(dir)
	require.NoError(t, readErr)
}

func TestSnapshotCatalogOperationalErrorsAreNotRebuildable(t *testing.T) {
	t.Parallel()
	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		os.ErrPermission,
		errors.New("database is locked"),
		errors.New("disk I/O error"),
	} {
		require.False(t, snapshotCatalogCanRebuild(err), err)
	}
}

func TestSnapshotCatalogRecoveryNeverUsesInitialGeneration(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(t, os.WriteFile(
		snapshotCatalogPath(base), []byte("not a sqlite database"), 0o600,
	))
	err := ensureSnapshotCatalogContext(
		t.Context(), base, func() (int64, error) { return 1, nil },
	)
	require.ErrorContains(t, err, "must be greater than one")
}

func TestListSnapshotPageMissingCatalogDoesNotCreateIt(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	catalogPath := snapshotCatalogPath(base)
	_, _, err := ListSnapshotPage(base, 10, nil)
	require.Error(t, err)
	var sqliteErr *driversqlite.Error
	require.ErrorAs(t, err, &sqliteErr)
	require.Equal(t, sqlite3.SQLITE_CANTOPEN, sqliteErr.Code()&0xff)
	require.False(t, snapshotCatalogCanRebuild(err))
	require.NoFileExists(t, catalogPath)
	require.NoError(t, EnsureSnapshotCatalog(base))
	entries, next, err := ListSnapshotPage(base, 10, nil)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.Nil(t, next)
}

func TestAvailableSnapshotPageOrdersAndDeduplicatesCatalogs(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	localDir := filepath.Join(base, "duplicate")
	require.NoError(t, os.Mkdir(localDir, 0o755))
	require.NoError(t, WriteManifest(localDir, Manifest{
		CreatedAt: time.Unix(20, 0).UTC(),
		Trigger:   TriggerManual,
	}))
	require.NoError(t, EnsureSnapshotCatalog(base))

	manifestAt := func(second int64) Manifest {
		dir := t.TempDir()
		require.NoError(t, WriteManifest(dir, Manifest{
			CreatedAt: time.Unix(second, 0).UTC(),
			Trigger:   TriggerManual,
		}))
		manifest, err := ReadManifest(dir)
		require.NoError(t, err)
		return manifest
	}
	require.NoError(t, RebuildCloudSnapshotCatalogContext(
		t.Context(), base, "s3://bucket/snapshots", []SnapshotEntry{
			{ID: "cloud-new", Manifest: manifestAt(30)},
			{ID: "duplicate", Manifest: manifestAt(40)},
			{ID: "cloud-old", Manifest: manifestAt(10)},
		},
	))

	first, cursor, err := ListAvailableSnapshotPageContext(
		t.Context(), base, "s3://bucket/snapshots", 2, nil,
	)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, "cloud-new", first[0].Entry.ID)
	require.Equal(t, "s3://bucket/snapshots/cloud-new", first[0].Location)
	require.Equal(t, "duplicate", first[1].Entry.ID)
	require.Equal(t, localDir, first[1].Location)
	require.NotNil(t, cursor)

	second, next, err := ListAvailableSnapshotPageContext(
		t.Context(), base, "s3://bucket/snapshots", 2, cursor,
	)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, "cloud-old", second[0].Entry.ID)
	require.Nil(t, next)
}

func TestAvailableSnapshotPageUsesOrderingIndexWithoutTempSort(t *testing.T) {
	t.Parallel()
	db, err := openSnapshotCatalog(t.Context(), ":memory:", true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, createSnapshotCatalogSchema(db))

	assertPlan := func(
		query string,
		wantIndex string,
		forbidden []string,
		args ...any,
	) {
		rows, err := db.QueryContext(
			t.Context(), "EXPLAIN QUERY PLAN "+query, args...,
		)
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			details = append(details, detail)
		}
		require.NoError(t, rows.Err())
		plan := strings.Join(details, "\n")
		require.Contains(t, plan, wantIndex, plan)
		require.NotContains(t, plan, "USE TEMP B-TREE", plan)
		for _, table := range forbidden {
			require.NotContains(t, plan, table, plan)
		}
	}

	assertPlan(
		firstAvailableSnapshotPageQuery, "available_snapshots_created",
		[]string{"cloud_snapshots", "snapshots AS"}, 6,
	)
	assertPlan(
		nextAvailableSnapshotPageQuery, "available_snapshots_created",
		[]string{"cloud_snapshots", "snapshots AS"},
		int64(50), 0, "id", 6,
	)
	assertPlan(
		firstLocalAvailableSnapshotPageQuery, "snapshots_created",
		[]string{"available_snapshots", "cloud_snapshots"}, 6,
	)
	assertPlan(
		nextLocalAvailableSnapshotPageQuery, "snapshots_created",
		[]string{"available_snapshots", "cloud_snapshots"},
		int64(50), 0, "id", 6,
	)
}

func TestCloudCatalogMutationFailureRepairsFromDurableSource(t *testing.T) {
	base := t.TempDir()
	snapshotDir := filepath.Join(base, "snapshot-one")
	require.NoError(t, os.Mkdir(snapshotDir, 0o755))
	require.NoError(t, WriteManifest(snapshotDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))
	require.NoError(t, EnsureSnapshotCatalog(base))

	cloudRoot := t.TempDir()
	registry := NewDestinationRegistry()
	registry.Register("catalogrepair", func(uri *url.URL) (CloudDestination, error) {
		return &catalogRepairDestination{dir: filepath.Join(
			cloudRoot, strings.TrimPrefix(uri.Path, "/"),
		)}, nil
	})
	const cloudDest = "catalogrepair://bucket/snapshots"
	require.NoError(t, os.WriteFile(
		snapshotCatalogPath(base), []byte("not a sqlite database"), 0o600,
	))
	require.NoError(t, MirrorToCloud(
		t.Context(), registry, snapshotDir, cloudDest,
	))
	require.NoError(t, RemoveSnapshotContext(t.Context(), snapshotDir))
	entries, _, err := ListAvailableSnapshotPageContext(
		t.Context(), base, cloudDest, 10, nil,
	)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "snapshot-one", entries[0].Entry.ID)
	require.Equal(t, cloudDest+"/snapshot-one", entries[0].Location)

	cloudURI := JoinCloudURI(cloudDest, "snapshot-one")
	ok, err := DeleteCloudSnapshot(t.Context(), registry, cloudURI)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, os.WriteFile(
		snapshotCatalogPath(base), []byte("not a sqlite database"), 0o600,
	))
	require.NoError(t, RemoveCloudSnapshotCatalogEntryContext(
		t.Context(), base, "snapshot-one", registry, cloudDest,
	))
	entries, _, err = ListAvailableSnapshotPageContext(
		t.Context(), base, cloudDest, 10, nil,
	)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestMirrorMarkerWaitsForCatalogRepair(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	snapshotDir := filepath.Join(base, "snapshot-one")
	require.NoError(t, os.Mkdir(snapshotDir, 0o755))
	require.NoError(t, WriteManifest(snapshotDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))
	require.NoError(t, EnsureSnapshotCatalog(base))
	require.NoError(t, os.WriteFile(
		snapshotCatalogPath(base), []byte("not a sqlite database"), 0o600,
	))
	providerErr := errors.New("provider unavailable")
	registry := NewDestinationRegistry()
	registry.Register("markerrepair", func(*url.URL) (CloudDestination, error) {
		return &catalogScriptedLister{err: providerErr}, nil
	})
	err := MirrorToCloud(
		t.Context(), registry, snapshotDir,
		"markerrepair://user:secret@bucket/snapshots?token=private#fragment",
	)
	require.ErrorIs(t, err, ErrSnapshotCatalogUpdate)
	require.ErrorIs(t, err, providerErr)
	require.NoFileExists(t, CloudMirrorMarkerPath(snapshotDir))
	for _, secret := range []string{"user", "secret", "private", "fragment"} {
		require.NotContains(t, err.Error(), secret)
	}
}

func TestSnapshotCloudRepairRepopulatesCloudRowsAfterTotalCatalogCorruption(
	t *testing.T,
) {
	base := t.TempDir()
	cloudRoot := t.TempDir()
	cloudBase := filepath.Join(cloudRoot, "snapshots")
	cloudOnlyDir := filepath.Join(cloudBase, "cloud-only")
	require.NoError(t, os.MkdirAll(cloudOnlyDir, 0o755))
	require.NoError(t, WriteManifest(cloudOnlyDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))

	registry := NewDestinationRegistry()
	registry.Register("catalogrepairwrite", func(uri *url.URL) (CloudDestination, error) {
		return &catalogRepairDestination{dir: filepath.Join(
			cloudRoot, strings.TrimPrefix(uri.Path, "/"),
		)}, nil
	})
	const cloudDest = "catalogrepairwrite://bucket/snapshots"
	require.NoError(t, EnsureSnapshotCatalog(base))
	require.NoError(t, RepairSnapshotCatalogContext(
		t.Context(), base, registry, cloudDest,
	))
	require.NoError(t, os.WriteFile(
		snapshotCatalogPath(base), []byte("not a sqlite database"), 0o600,
	))

	db := newRestoreInternalTestDB(t)
	localDir := filepath.Join(base, "new-local")
	_, err := SnapshotToCloud(
		t.Context(), registry, db, localDir,
		TriggerManual, "test", "badger", "sqlite", cloudDest, "", "",
	)
	require.NoError(t, err)

	entries, _, err := ListAvailableSnapshotPageContext(
		t.Context(), base, cloudDest, 10, nil,
	)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	byID := make(map[string]AvailableSnapshotCatalogEntry, len(entries))
	for _, entry := range entries {
		byID[entry.Entry.ID] = entry
	}
	require.Contains(t, byID, "cloud-only")
	require.Equal(
		t, cloudDest+"/cloud-only", byID["cloud-only"].Location,
	)
	require.Contains(t, byID, "new-local")
	require.Equal(t, localDir, byID["new-local"].Location)
}

func TestLocalCatalogRepairRetainsValidatedCloudRows(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, EnsureSnapshotCatalog(base))
	manifest := Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}
	manifestDir := t.TempDir()
	require.NoError(t, WriteManifest(manifestDir, manifest))
	manifest, err := ReadManifest(manifestDir)
	require.NoError(t, err)
	require.NoError(t, RebuildCloudSnapshotCatalogContext(
		t.Context(), base, "test://bucket/snapshots", []SnapshotEntry{{
			ID: "cloud-only", Manifest: manifest,
		}},
	))

	db, err := openSnapshotCatalog(
		t.Context(), snapshotCatalogPath(base), false,
	)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "DROP TABLE snapshots")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoError(t, EnsureSnapshotCatalogContext(t.Context(), base))

	entries, _, err := ListAvailableSnapshotPageContext(
		t.Context(), base, "test://bucket/snapshots", 10, nil,
	)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "cloud-only", entries[0].Entry.ID)
	require.Equal(
		t, "test://bucket/snapshots/cloud-only", entries[0].Location,
	)
}

func TestAvailableCatalogExposesOnlyConfiguredCloudSource(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(t, EnsureSnapshotCatalog(base))
	manifestDir := t.TempDir()
	require.NoError(t, WriteManifest(manifestDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))
	manifest, err := ReadManifest(manifestDir)
	require.NoError(t, err)
	const oldDest = "s3://old-bucket/snapshots"
	require.NoError(t, RebuildCloudSnapshotCatalogContext(
		t.Context(), base, oldDest,
		[]SnapshotEntry{{ID: "stale", Manifest: manifest}},
	))

	for _, configured := range []string{
		"", "gcs://new-bucket/snapshots", "unsupported://bucket/snapshots",
	} {
		entries, _, err := ListAvailableSnapshotPageContext(
			t.Context(), base, configured, 10, nil,
		)
		require.NoError(t, err)
		require.Empty(t, entries, configured)
	}
	entries, _, err := ListAvailableSnapshotPageContext(
		t.Context(), base, oldDest, 10, nil,
	)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "stale", entries[0].Entry.ID)
}

func TestCloudCatalogPersistsOnlyDigestAndManifest(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(t, EnsureSnapshotCatalog(base))
	manifestDir := t.TempDir()
	require.NoError(t, WriteManifest(manifestDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))
	manifest, err := ReadManifest(manifestDir)
	require.NoError(t, err)
	const cloudDest = "s3://user:secret@bucket/snapshots?token=private#option"
	require.NoError(t, RebuildCloudSnapshotCatalogContext(
		t.Context(), base, cloudDest,
		[]SnapshotEntry{{ID: "cloud", Manifest: manifest}},
	))
	db, err := openSnapshotCatalog(
		t.Context(), snapshotCatalogPath(base), false,
	)
	require.NoError(t, err)
	var source string
	require.NoError(t, db.QueryRowContext(
		t.Context(), "SELECT cloud_source FROM catalog_meta WHERE singleton = 1",
	).Scan(&source))
	require.True(t, strings.HasPrefix(source, "v1:sha256:"))
	for _, table := range []string{"cloud_snapshots", "available_snapshots"} {
		rows, err := db.QueryContext(t.Context(), "PRAGMA table_info("+table+")")
		require.NoError(t, err)
		for rows.Next() {
			var cid int
			var name, columnType string
			var notNull, primaryKey int
			var defaultValue any
			require.NoError(t, rows.Scan(
				&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey,
			))
			require.NotEqual(t, "location", name)
		}
		require.NoError(t, errors.Join(rows.Err(), rows.Close()))
	}
	var legacyIndex int
	require.NoError(t, db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM sqlite_master
WHERE type = 'index' AND name = 'cloud_snapshots_created'`).Scan(&legacyIndex))
	require.Zero(t, legacyIndex)
	require.NoError(t, db.Close())
	catalogBytes, err := os.ReadFile(snapshotCatalogPath(base))
	require.NoError(t, err)
	for _, secret := range []string{"user", "secret", "private"} {
		require.NotContains(t, source, secret)
		require.NotContains(t, string(catalogBytes), secret)
	}
}

func TestRepairSnapshotCatalogPreservesPartialErrorsAndClearsStaleRows(
	t *testing.T,
) {
	base := t.TempDir()
	broken := filepath.Join(base, "broken")
	require.NoError(t, os.MkdirAll(broken, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(broken, ManifestFileName), []byte("broken"), 0o600,
	))
	require.ErrorIs(t, EnsureSnapshotCatalog(base), ErrSnapshotCatalogIncomplete)
	manifestDir := t.TempDir()
	require.NoError(t, WriteManifest(manifestDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))
	manifest, err := ReadManifest(manifestDir)
	require.NoError(t, err)

	registry := NewDestinationRegistry()
	providerErr := errors.New("partial provider listing")
	registry.Register("partialcatalog", func(*url.URL) (CloudDestination, error) {
		return &catalogScriptedLister{
			entries: []SnapshotEntry{{ID: "cloud-valid", Manifest: manifest}},
			err:     providerErr,
		}, nil
	})
	const cloudDest = "partialcatalog://bucket/snapshots"
	err = RepairSnapshotCatalogContext(t.Context(), base, registry, cloudDest)
	require.ErrorIs(t, err, ErrSnapshotCatalogIncomplete)
	require.ErrorIs(t, err, providerErr)
	entries, _, listErr := ListAvailableSnapshotPageContext(
		t.Context(), base, cloudDest, 10, nil,
	)
	require.NoError(t, listErr)
	require.Len(t, entries, 1)
	require.Equal(t, "cloud-valid", entries[0].Entry.ID)

	registry.Register("unsupportedcatalog", func(*url.URL) (CloudDestination, error) {
		return &catalogUnsupportedDestination{}, nil
	})
	const unsupportedDest = "unsupportedcatalog://bucket/snapshots"
	err = RepairSnapshotCatalogContext(
		t.Context(), base, registry, unsupportedDest,
	)
	require.ErrorIs(t, err, ErrSnapshotCatalogIncomplete)
	entries, _, listErr = ListAvailableSnapshotPageContext(
		t.Context(), base, unsupportedDest, 10, nil,
	)
	require.NoError(t, listErr)
	require.Empty(t, entries)
}

func TestRepairSnapshotCatalogTotalListFailureClearsStaleRows(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(t, EnsureSnapshotCatalog(base))
	manifestDir := t.TempDir()
	require.NoError(t, WriteManifest(manifestDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))
	manifest, err := ReadManifest(manifestDir)
	require.NoError(t, err)
	const cloudDest = "failedcatalog://bucket/snapshots"
	require.NoError(t, RebuildCloudSnapshotCatalogContext(
		t.Context(), base, cloudDest,
		[]SnapshotEntry{{ID: "stale", Manifest: manifest}},
	))
	providerErr := errors.New("provider unavailable")
	registry := NewDestinationRegistry()
	registry.Register("failedcatalog", func(*url.URL) (CloudDestination, error) {
		return &catalogScriptedLister{err: providerErr}, nil
	})

	err = RepairSnapshotCatalogContext(t.Context(), base, registry, cloudDest)
	require.ErrorIs(t, err, providerErr)
	entries, _, listErr := ListAvailableSnapshotPageContext(
		t.Context(), base, cloudDest, 10, nil,
	)
	require.NoError(t, listErr)
	require.Empty(t, entries)
}

func TestCloudCatalogRecoversAfterStartupConstructorFailure(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(t, EnsureSnapshotCatalog(base))
	cloudRoot := t.TempDir()
	failConstruction := true
	registry := NewDestinationRegistry()
	registry.Register("constructorrecovery", func(uri *url.URL) (CloudDestination, error) {
		if failConstruction {
			failConstruction = false
			return nil, errors.New("provider initialization failed")
		}
		return &catalogRepairDestination{dir: filepath.Join(
			cloudRoot, strings.TrimPrefix(uri.Path, "/"),
		)}, nil
	})
	const cloudDest = "constructorrecovery://bucket/snapshots"
	current, err := ReconcileCloudSnapshotCatalogContext(
		t.Context(), base, registry, cloudDest,
	)
	require.ErrorContains(t, err, "provider initialization failed")
	require.True(t, current,
		"successful clear must bind the catalog to the configured source")

	snapshotDir := filepath.Join(base, "mirrored")
	require.NoError(t, os.Mkdir(snapshotDir, 0o755))
	require.NoError(t, WriteManifest(snapshotDir, Manifest{
		CreatedAt: time.Unix(10, 0).UTC(), Trigger: TriggerManual,
	}))
	require.NoError(t, MirrorToCloud(
		t.Context(), registry, snapshotDir, cloudDest,
	))
	require.NoError(t, RemoveSnapshotContext(t.Context(), snapshotDir))
	entries, _, err := ListAvailableSnapshotPageContext(
		t.Context(), base, cloudDest, 10, nil,
	)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "mirrored", entries[0].Entry.ID)
	require.Equal(t, cloudDest+"/mirrored", entries[0].Location)
}

func TestCloudReconciliationSerializesConcurrentIncrementalMirror(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, EnsureSnapshotCatalog(base))
	manifestAt := func(second int64) Manifest {
		dir := t.TempDir()
		require.NoError(t, WriteManifest(dir, Manifest{
			CreatedAt: time.Unix(second, 0).UTC(), Trigger: TriggerManual,
		}))
		manifest, err := ReadManifest(dir)
		require.NoError(t, err)
		return manifest
	}
	provider := &blockingCatalogLister{
		entries:  []SnapshotEntry{{ID: "scanned", Manifest: manifestAt(1)}},
		captured: make(chan struct{}), release: make(chan struct{}),
	}
	registry := NewDestinationRegistry()
	registry.Register("interleavecatalog", func(*url.URL) (CloudDestination, error) {
		return provider, nil
	})
	const cloudDest = "interleavecatalog://bucket/snapshots"
	mirroredManifest := manifestAt(2)
	reconcileDone := make(chan error, 1)
	go func() {
		_, err := ReconcileCloudSnapshotCatalogContext(
			t.Context(), base, registry, cloudDest,
		)
		reconcileDone <- err
	}()
	<-provider.captured
	lockCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	lockErr := lockSnapshotCatalog(lockCtx)
	if lockErr == nil {
		unlockSnapshotCatalog()
	}
	require.ErrorIs(t, lockErr, context.DeadlineExceeded,
		"provider listing and replacement must hold the reconciliation gate")

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- updateCloudSnapshotCatalogIfPresent(
			t.Context(), base,
			SnapshotEntry{ID: "mirrored", Manifest: mirroredManifest}, cloudDest,
		)
	}()
	close(provider.release)
	require.NoError(t, <-reconcileDone)
	require.NoError(t, <-updateDone)

	entries, _, err := ListAvailableSnapshotPageContext(
		t.Context(), base, cloudDest, 10, nil,
	)
	require.NoError(t, err)
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.Entry.ID)
	}
	require.ElementsMatch(t, []string{"scanned", "mirrored"}, ids)
}

func TestCloudReconciliationConstructsProviderBeforeGate(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, EnsureSnapshotCatalog(base))
	registry := NewDestinationRegistry()
	registry.Register("constructorgate", func(*url.URL) (CloudDestination, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if err := lockSnapshotCatalog(ctx); err != nil {
			return nil, fmt.Errorf("constructor blocked on catalog gate: %w", err)
		}
		unlockSnapshotCatalog()
		return &catalogScriptedLister{}, nil
	})
	current, err := ReconcileCloudSnapshotCatalogContext(
		t.Context(), base, registry, "constructorgate://bucket/snapshots",
	)
	require.NoError(t, err)
	require.True(t, current)
}
