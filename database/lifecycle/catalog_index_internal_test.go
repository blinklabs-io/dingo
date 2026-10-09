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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotCatalogContinuationUsesIndexSeek(t *testing.T) {
	t.Parallel()
	db, err := openSnapshotCatalog(":memory:")
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

	db, err := openSnapshotCatalog(snapshotCatalogPath(base))
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
