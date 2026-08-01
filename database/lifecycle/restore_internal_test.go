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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// newRestoreInternalTestDB and newRestoreInternalTestHost duplicate the
// small fixtures restore_test.go/storage_host_test.go build (newTestDB,
// newTestStorageHost) rather than sharing them: those live in the
// external lifecycle_test package, which this white-box test file (needed
// to reach the unexported syncDir var below) cannot see.
func newRestoreInternalTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return db
}

func newRestoreInternalTestHost(t *testing.T) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host
}

func newRestoreInternalTestBlock() models.Block {
	return models.Block{
		ID:     1,
		Slot:   10,
		Hash:   bytes.Repeat([]byte{0x01}, 32),
		Cbor:   []byte{0x80},
		Number: 1,
		Type:   1,
	}
}

// TestSyncDirTreeSyncsEveryDirectory verifies syncDirTree calls syncDir
// for every directory in a nested tree, including the root itself, and
// does not call it for regular files.
func TestSyncDirTreeSyncsEveryDirectory(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "a", "b"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "a", "b", "f"), []byte("x"), 0o644,
	))

	var synced []string
	orig := syncDir
	syncDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() { syncDir = orig })

	require.NoError(t, syncDirTree(root))
	require.Contains(t, synced, root)
	require.Contains(t, synced, filepath.Join(root, "a"))
	require.Contains(t, synced, filepath.Join(root, "a", "b"))
	require.NotContains(
		t, synced, filepath.Join(root, "a", "b", "f"),
		"syncDirTree must only fsync directories, not files",
	)
}

// TestRestoreValidatedFailsClosedWhenStagingSyncFails guards the crash-
// durability gap this closes: if the pre-activation directory sync fails,
// RestoreValidated must not proceed to rename the staging directory into
// place -- targetDataDir must be left exactly as untouched as any other
// failure earlier in the pipeline leaves it, not silently activated
// anyway despite durability being unconfirmed.
func TestRestoreValidatedFailsClosedWhenStagingSyncFails(t *testing.T) {
	db := newRestoreInternalTestDB(t)
	require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := Snapshot(
		context.Background(), db, snapshotDir, TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	injectedErr := errors.New("injected staging sync failure")
	orig := syncDir
	syncDir = func(string) error { return injectedErr }
	t.Cleanup(func() { syncDir = orig })

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = Restore(
		context.Background(), newRestoreInternalTestHost(t), nil, snapshotDir, targetDir,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, injectedErr)

	_, statErr := os.Stat(targetDir)
	require.True(
		t, os.IsNotExist(statErr),
		"targetDataDir must not be activated when the pre-rename sync fails",
	)
}

// TestRestoreValidatedSurfacesPostRenameSyncFailure verifies that a
// failure syncing the parent directory after the activating rename is
// still surfaced as an error -- even though targetDataDir was already
// renamed into place and is perfectly usable, "restore succeeded" must
// not be reported when this durability guarantee could not be confirmed.
func TestRestoreValidatedSurfacesPostRenameSyncFailure(t *testing.T) {
	db := newRestoreInternalTestDB(t)
	require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := Snapshot(
		context.Background(), db, snapshotDir, TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	parentDir := t.TempDir()
	targetDir := filepath.Join(parentDir, "restored")

	injectedErr := errors.New("injected parent sync failure")
	orig := syncDir
	syncDir = func(path string) error {
		if path == parentDir {
			return injectedErr
		}
		return orig(path)
	}
	t.Cleanup(func() { syncDir = orig })

	_, err = Restore(
		context.Background(), newRestoreInternalTestHost(t), nil, snapshotDir, targetDir,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, injectedErr)

	// The rename already happened before the parent sync ran -- the
	// restored data directory is real and usable even though this
	// particular durability guarantee could not be confirmed.
	_, statErr := os.Stat(targetDir)
	require.NoError(t, statErr)
}
