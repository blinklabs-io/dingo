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

package database

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/stretchr/testify/require"
)

// TestWriteDBInfoSidecarOnFirstStart pins that a normal, fully-configured
// first start actually produces the dbinfo sidecar on disk with the
// configured metadata plugin name -- without this, internal/settingsresolve's
// pre-open check would have nothing to read on any real database.
func TestWriteDBInfoSidecarOnFirstStart(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	info, err := dbinfo.Read(dataDir)
	require.NoError(t, err)
	require.Equal(t, "sqlite", info.MetadataPlugin)
}

// TestWriteDBInfoSidecarSkippedOnPartialConfig guards the first of
// writeDBInfoSidecar's two guards: mithril/sync.go and
// database/lifecycle/restore.go reopen an existing database with a Config
// that never sets MetadataPlugin, so writing an empty plugin name would
// poison the pre-open check for every later, complete start against the
// same directory.
func TestWriteDBInfoSidecarSkippedOnPartialConfig(t *testing.T) {
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Network:     "preprod",
		// No MetadataPlugin, mirroring the partial-Config callers.
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	info, err := dbinfo.Read(dataDir)
	require.NoError(t, err)
	require.Empty(t, info.MetadataPlugin)
}

// TestWriteDBInfoSidecarNeverOverwritesExisting guards writeDBInfoSidecar's
// second guard: a sidecar already present, even one naming a different
// plugin than what is about to open successfully, must be left alone --
// overwriting it would erase the exact mismatch signal the file exists to
// carry.
func TestWriteDBInfoSidecarNeverOverwritesExisting(t *testing.T) {
	dataDir := t.TempDir()
	require.NoError(t, dbinfo.Write(dataDir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: "postgres",
	}))

	db, err := newTestDatabase(t, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	info, err := dbinfo.Read(dataDir)
	require.NoError(t, err)
	require.Equal(
		t,
		"postgres",
		info.MetadataPlugin,
		"an existing sidecar naming a different plugin must never be overwritten",
	)
}

// TestWriteDBInfoSidecarRecreatedOnSteadyStateStart is a regression test for
// a bug where evaluateAndPersistGates returned early whenever there was
// nothing new to write to node_settings_gate -- the normal case for every
// start after the first -- without ever reaching writeGateValues, the only
// place that called writeDBInfoSidecar. An operator who deleted the sidecar
// (or lost it to a partial restore) would never get it back on any later
// steady-state start, silently disabling internal/settingsresolve's
// pre-open metadata-plugin check from then on.
func TestWriteDBInfoSidecarRecreatedOnSteadyStateStart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}

	db, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	sidecarPath := filepath.Join(dataDir, dbinfo.FileName)
	_, err = os.Stat(sidecarPath)
	require.NoError(t, err, "sidecar must exist after the first start")
	require.NoError(t, os.Remove(sidecarPath))

	// Reopen with the identical config: every gate already matches what is
	// persisted, so this start has nothing to write to node_settings_gate.
	reopened, err := newTestDatabase(t, cfg)
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(reopened))

	info, err := dbinfo.Read(dataDir)
	require.NoError(
		t,
		err,
		"a deleted sidecar must be recreated even when no gate needed writing",
	)
	require.Equal(t, "sqlite", info.MetadataPlugin)
}

// sidecarTrapPath returns a path that behaves like an unusable data
// directory for dbinfo.Read/Write specifically: a plain file, not a
// directory, so any attempt to read or create a file "inside" it fails with
// ENOTDIR. Config.DataDir is independent of the metadata/blob stores'
// actual directories (newTestDatabaseAt resolves those separately), so this
// lets a test break only the sidecar path without touching the real store.
func sidecarTrapPath(t *testing.T) string {
	t.Helper()
	trap := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(trap, []byte("x"), 0o600))
	return trap
}

// TestNewDatabaseFailsWhenSidecarCannotBeEstablished pins Finding 3's fix:
// for a brand-new database, the sidecar is the only thing that will later
// stop a mistyped provider from silently creating a second, empty database
// beside the real one (there is no metadata_plugin gate row yet for
// settingsresolve to compare against -- this open is what creates it), so
// failing to establish it here must fail the open instead of warning and
// continuing.
func TestNewDatabaseFailsWhenSidecarCannotBeEstablished(t *testing.T) {
	metaDir := t.TempDir()
	blobDir := t.TempDir()

	_, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        sidecarTrapPath(t),
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "dbinfo sidecar")
}

func TestSidecarFailureDoesNotLatchMetadataPluginGate(t *testing.T) {
	metaDir := t.TempDir()
	blobDir := t.TempDir()
	trapDataDir := sidecarTrapPath(t)

	_, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        trapDataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.Error(t, err)

	dataDir := t.TempDir()
	db, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        dataDir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)

	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "sqlite", gates["metadata_plugin"])
}

// TestExistingDatabaseSidecarFailureIsNonFatal pins the other half of
// Finding 3: an already-established database (one with a prior
// writeGateValues call, so a metadata_plugin gate row already exists)
// backfilling a lost or never-written sidecar on a later gate write must
// still warn and continue, not fail the open -- node_settings_gate's own
// metadata_plugin gate is already the real enforcement for it by then.
func TestExistingDatabaseSidecarFailureIsNonFatal(t *testing.T) {
	metaDir := t.TempDir()
	blobDir := t.TempDir()
	realDataDir := t.TempDir()

	db, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        realDataDir,
		StorageMode:    "api",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	// Reopen the same stores with a gate that still needs a genuine write
	// (storage_mode's permitted api -> core move) so this reaches
	// writeGateValues again, but point Config.DataDir at a sidecar trap
	// this time. legacy is already non-nil from the open above, so this is
	// not a new database.
	reopened, err := newTestDatabaseAt(t, metaDir, blobDir, &Config{
		DataDir:        sidecarTrapPath(t),
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(
		t,
		err,
		"a sidecar failure while backfilling an already-established "+
			"database must not fail the open",
	)
	require.NoError(t, closeTestDatabase(reopened))
}
