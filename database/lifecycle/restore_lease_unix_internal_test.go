//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/stretchr/testify/require"
)

// An attempt killed before its deferred cleanup ran leaves a populated
// staging directory that no process holds. The next attempt reclaims it
// immediately instead of waiting out the age cutoff.
func TestCleanStaleRestoreWorkDirsReclaimsAbandonedStaging(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	staging := filepath.Join(parent, ".dingo-restore-staging-abandoned")
	require.NoError(t, os.Mkdir(staging, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(staging, "metadata.sqlite"), []byte("partial"), 0o600,
	))

	require.NoError(t, CleanStaleRestoreWorkDirs(parent))

	_, err := os.Lstat(staging)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// A staging directory still held by its running attempt is never reclaimed,
// however old it looks, and becomes reclaimable once that attempt lets go.
func TestCleanStaleRestoreWorkDirsKeepsHeldStaging(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	owned, err := createOwnedRestoreDir(parent, ".dingo-restore-staging-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = owned.close() })
	marker := filepath.Join(owned.dir.path, "metadata.sqlite")
	require.NoError(t, os.WriteFile(marker, []byte("in progress"), 0o600))
	old := time.Now().Add(-2 * staleRestoreWorkDirAge)
	require.NoError(t, os.Chtimes(owned.dir.path, old, old))

	require.NoError(t, CleanStaleRestoreWorkDirs(parent))
	require.FileExists(t, marker)

	require.NoError(t, owned.close())
	require.NoError(t, CleanStaleRestoreWorkDirs(parent))
	_, err = os.Lstat(owned.dir.path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// A directory nobody holds yet may belong to an attempt that has created it
// but not yet taken its hold. Only populated directories are reclaimed early.
func TestCleanStaleRestoreWorkDirsKeepsRecentEmptyStaging(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	staging := filepath.Join(parent, ".dingo-restore-staging-new")
	require.NoError(t, os.Mkdir(staging, 0o700))

	require.NoError(t, CleanStaleRestoreWorkDirs(parent))

	require.DirExists(t, staging)
}
