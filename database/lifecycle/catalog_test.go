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

package lifecycle_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

// TestListSnapshotsMissingBaseDirReturnsEmpty verifies that a
// non-existent base directory returns an empty list, not an error.
func TestListSnapshotsMissingBaseDirReturnsEmpty(t *testing.T) {
	t.Parallel()

	entries, err := lifecycle.ListSnapshots(
		filepath.Join(t.TempDir(), "does-not-exist"),
	)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestListSnapshotsSkipsEntriesWithoutValidManifest verifies that a
// partial (manifest-less) directory and a stray file are both skipped.
func TestListSnapshotsSkipsEntriesWithoutValidManifest(t *testing.T) {
	t.Parallel()

	base := t.TempDir()

	// A real, valid snapshot directory.
	good := testManifest()
	good.CreatedAt = time.Unix(1700000100, 0).UTC()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-good"), 0o755))
	require.NoError(
		t,
		lifecycle.WriteManifest(filepath.Join(base, "snap-good"), good),
	)

	// A directory that exists but has no manifest.json yet (snapshot
	// still in progress, or failed partway through).
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-partial"), 0o755))

	// A regular file directly under baseDir (not a directory) must be
	// ignored entirely, not mistaken for a snapshot.
	require.NoError(
		t,
		os.WriteFile(filepath.Join(base, "not-a-dir"), []byte("x"), 0o644),
	)

	entries, err := lifecycle.ListSnapshots(base)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "snap-good", entries[0].ID)
}

// TestListSnapshotsSurfacesCorruptedManifestWithoutHidingGoodEntries guards
// against a real bug: the same blanket "continue" ListSnapshots
// used for a snapshot still being written (no manifest.json yet — an
// expected transient state, covered by
// TestListSnapshotsSkipsEntriesWithoutValidManifest above) used to also
// silently swallow a manifest.json that exists but is corrupted — a
// checksum mismatch, here — with no way for a caller to ever learn about
// it. ListSnapshots must still return every other, valid snapshot (one
// broken directory must not hide the rest of the catalog), but it must
// also report the corruption via its error return rather than pretending
// the corrupted snapshot was simply never taken.
func TestListSnapshotsSurfacesCorruptedManifestWithoutHidingGoodEntries(
	t *testing.T,
) {
	t.Parallel()

	base := t.TempDir()

	good := testManifest()
	good.CreatedAt = time.Unix(1700000100, 0).UTC()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-good"), 0o755))
	require.NoError(
		t,
		lifecycle.WriteManifest(filepath.Join(base, "snap-good"), good),
	)

	corrupted := testManifest()
	require.NoError(
		t,
		os.MkdirAll(filepath.Join(base, "snap-corrupted"), 0o755),
	)
	require.NoError(
		t,
		lifecycle.WriteManifest(
			filepath.Join(base, "snap-corrupted"),
			corrupted,
		),
	)
	// Tamper with the manifest after writing it so its checksum no longer
	// matches — same class of on-disk corruption ErrManifestCorrupted
	// exists to detect.
	manifestPath := filepath.Join(
		base,
		"snap-corrupted",
		lifecycle.ManifestFileName,
	)
	data, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	tampered := bytes.Replace(data, []byte(`"mainnet"`), []byte(`"testnet"`), 1)
	require.NotEqual(t, data, tampered, "tamper must actually change the file")
	require.NoError(t, os.WriteFile(manifestPath, tampered, 0o644))

	// Still an expected in-progress snapshot: must remain silently skipped.
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-partial"), 0o755))

	entries, err := lifecycle.ListSnapshots(base)
	require.Error(t, err, "a corrupted manifest must be reported, not hidden")
	require.ErrorIs(t, err, lifecycle.ErrManifestCorrupted)
	require.Len(t, entries, 1, "the good snapshot must still be listed")
	require.Equal(t, "snap-good", entries[0].ID)
}

// TestListSnapshotsOrdersNewestFirst verifies that entries are sorted by
// CreatedAt descending, newest snapshot first.
func TestListSnapshotsOrdersNewestFirst(t *testing.T) {
	t.Parallel()

	base := t.TempDir()

	older := testManifest()
	older.CreatedAt = time.Unix(1700000000, 0).UTC()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-older"), 0o755))
	require.NoError(
		t,
		lifecycle.WriteManifest(filepath.Join(base, "snap-older"), older),
	)

	newer := testManifest()
	newer.CreatedAt = time.Unix(1700000999, 0).UTC()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-newer"), 0o755))
	require.NoError(
		t,
		lifecycle.WriteManifest(filepath.Join(base, "snap-newer"), newer),
	)

	entries, err := lifecycle.ListSnapshots(base)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "snap-newer", entries[0].ID)
	require.Equal(t, "snap-older", entries[1].ID)
}

func TestListSnapshotPageBoundsManifestReads(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	const catalogSize = 256
	for i := range catalogSize {
		name := fmt.Sprintf("snapshot-%04d", i)
		dir := filepath.Join(base, name)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		manifest := testManifest()
		manifest.CreatedAt = manifest.CreatedAt.Add(time.Duration(i) * time.Second)
		require.NoError(t, lifecycle.WriteManifest(dir, manifest))
	}
	require.NoError(t, lifecycle.EnsureSnapshotCatalog(base))
	for i := range catalogSize - 4 {
		dir := filepath.Join(base, fmt.Sprintf("snapshot-%04d", i))
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, lifecycle.ManifestFileName),
			[]byte("not-json"),
			0o600,
		))
	}

	entries, next, err := lifecycle.ListSnapshotPage(base, 3, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		"snapshot-0255", "snapshot-0254", "snapshot-0253",
	}, []string{entries[0].ID, entries[1].ID, entries[2].ID})
	require.NotNil(t, next)

	entries, next, err = lifecycle.ListSnapshotPage(base, 1, next)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "snapshot-0252", entries[0].ID)
	require.NotNil(t, next)
}

func TestListSnapshotPageRejectsOversizedPage(t *testing.T) {
	t.Parallel()
	_, _, err := lifecycle.ListSnapshotPage(
		t.TempDir(), lifecycle.SnapshotCatalogPageLimit+1, nil,
	)
	require.ErrorContains(t, err, "invalid snapshot page size")
}

func TestSnapshotCatalogTracksWritesDeletesAndGeneration(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(t, lifecycle.EnsureSnapshotCatalog(base))

	write := func(id string, createdAt time.Time) {
		dir := filepath.Join(base, id)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		manifest := testManifest()
		manifest.CreatedAt = createdAt
		require.NoError(t, lifecycle.WriteManifest(dir, manifest))
	}
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	write("older", createdAt)
	write("newer", createdAt.Add(time.Minute))

	entries, cursor, err := lifecycle.ListSnapshotPage(base, 1, nil)
	require.NoError(t, err)
	require.Equal(t, "newer", entries[0].ID)
	require.NotNil(t, cursor)

	write("newest", createdAt.Add(2*time.Minute))
	_, _, err = lifecycle.ListSnapshotPage(base, 1, cursor)
	require.ErrorIs(t, err, lifecycle.ErrSnapshotCatalogChanged)

	require.NoError(t, lifecycle.RemoveSnapshot(filepath.Join(base, "newest")))
	entries, _, err = lifecycle.ListSnapshotPage(base, 10, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"newer", "older"}, []string{
		entries[0].ID, entries[1].ID,
	})
}
