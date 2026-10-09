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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	driversqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

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
