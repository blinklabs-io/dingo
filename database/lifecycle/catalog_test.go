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
	entries, err := lifecycle.ListSnapshots(filepath.Join(t.TempDir(), "does-not-exist"))
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestListSnapshotsSkipsEntriesWithoutValidManifest verifies that a
// partial (manifest-less) directory and a stray file are both skipped.
func TestListSnapshotsSkipsEntriesWithoutValidManifest(t *testing.T) {
	base := t.TempDir()

	// A real, valid snapshot directory.
	good := testManifest()
	good.CreatedAt = time.Unix(1700000100, 0).UTC()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-good"), 0o755))
	require.NoError(t, lifecycle.WriteManifest(filepath.Join(base, "snap-good"), good))

	// A directory that exists but has no manifest.json yet (snapshot
	// still in progress, or failed partway through).
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-partial"), 0o755))

	// A regular file directly under baseDir (not a directory) must be
	// ignored entirely, not mistaken for a snapshot.
	require.NoError(t, os.WriteFile(filepath.Join(base, "not-a-dir"), []byte("x"), 0o644))

	entries, err := lifecycle.ListSnapshots(base)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "snap-good", entries[0].ID)
}

// TestListSnapshotsOrdersNewestFirst verifies that entries are sorted by
// CreatedAt descending, newest snapshot first.
func TestListSnapshotsOrdersNewestFirst(t *testing.T) {
	base := t.TempDir()

	older := testManifest()
	older.CreatedAt = time.Unix(1700000000, 0).UTC()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-older"), 0o755))
	require.NoError(t, lifecycle.WriteManifest(filepath.Join(base, "snap-older"), older))

	newer := testManifest()
	newer.CreatedAt = time.Unix(1700000999, 0).UTC()
	require.NoError(t, os.MkdirAll(filepath.Join(base, "snap-newer"), 0o755))
	require.NoError(t, lifecycle.WriteManifest(filepath.Join(base, "snap-newer"), newer))

	entries, err := lifecycle.ListSnapshots(base)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "snap-newer", entries[0].ID)
	require.Equal(t, "snap-older", entries[1].ID)
}
