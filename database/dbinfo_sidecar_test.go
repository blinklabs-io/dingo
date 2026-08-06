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
