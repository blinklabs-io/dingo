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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOrderEntriesManifestLastSortsManifestToEnd guards against
// comment-27: a snapshot's manifest.json must never upload before every
// other backup payload has succeeded, since a concurrent lister/fetcher
// treats a cloud-visible manifest as "this snapshot is fully there" (see
// FetchCloudManifest/ListCloudSnapshots) and could otherwise download or
// restore an incomplete snapshot. os.ReadDir's alphabetical order would
// place "manifest.json" before "metadata.sqlite", so relying on
// directory order alone is not enough.
func TestOrderEntriesManifestLastSortsManifestToEnd(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{ManifestFileName, BlobBackupFileName, MetadataBackupFileName} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644))
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	ordered := orderEntriesManifestLast(entries)
	require.Len(t, ordered, 3)
	require.Equal(
		t, ManifestFileName, ordered[len(ordered)-1].Name(),
		"manifest.json must sort last regardless of directory order",
	)
	var nonManifest []string
	for _, e := range ordered[:len(ordered)-1] {
		nonManifest = append(nonManifest, e.Name())
	}
	require.ElementsMatch(
		t, []string{BlobBackupFileName, MetadataBackupFileName}, nonManifest,
	)
}

// TestOrderEntriesManifestLastNoManifestPresent verifies the function is
// a safe no-op reordering when no manifest.json entry exists at all.
func TestOrderEntriesManifestLastNoManifestPresent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, BlobBackupFileName), []byte("x"), 0o644,
	))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	ordered := orderEntriesManifestLast(entries)
	require.Len(t, ordered, 1)
	require.Equal(t, BlobBackupFileName, ordered[0].Name())
}
