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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// nodeSettingsAt reads the node settings a closed ledger database keeps in
// its data directory.
func nodeSettingsAt(t *testing.T, dataDir string) nodesettings.Values {
	t.Helper()
	info, err := dbinfo.Read(dataDir)
	require.NoError(t, err)
	require.True(t, info.HoldsGates(), "the data directory holds no settings")
	return info.Gates
}

// setNodeSettingAt records one setting in a closed ledger database's data
// directory.
func setNodeSettingAt(t *testing.T, dataDir, name, value string) {
	t.Helper()
	values := nodeSettingsAt(t, dataDir)
	values[name] = value
	require.NoError(t, dbinfo.WriteGates(dataDir, values))
}

// deleteNodeSettingAt removes one setting from a closed ledger database's
// data directory.
func deleteNodeSettingAt(t *testing.T, dataDir, name string) {
	t.Helper()
	values := nodeSettingsAt(t, dataDir)
	delete(values, name)
	require.NoError(t, dbinfo.WriteGates(dataDir, values))
}

// releasedNodeSettingsDatabase leaves dir as v0.94.2 does: the node settings
// in the metadata store, paired with the blob store's identity, beside a
// format 1 dbinfo file. pair overrides the recorded blob store identity
// when it is not empty.
func releasedNodeSettingsDatabase(
	t *testing.T,
	dir string,
	cfg *Config,
	extra nodesettings.Values,
	pair string,
) {
	t.Helper()
	db, err := newTestDatabaseAt(t, dir, dir, cfg)
	require.NoError(t, err)
	gates, err := db.NodeSettingsGates(context.Background())
	require.NoError(t, err)
	for name, value := range extra {
		gates[name] = value
	}
	if pair != "" {
		gates["blob_store_id"] = pair
	}
	require.NoError(t, db.Metadata().SetNodeSettingsGates(
		context.Background(), gates, 0, 0,
	))
	require.NoError(t, closeTestDatabase(db))
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  dbinfo.ReleasedFormatVersion,
		MetadataPlugin: cfg.MetadataPlugin,
	}))
}

// The first open of a v0.94.2 data directory moves its node settings out of
// the metadata store, every gate and latch with them, and enforces them from
// there.
func TestOpenMovesReleasedNodeSettings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := &Config{
		DataDir:        dir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		Role:           RoleReleased,
	}
	releasedNodeSettingsDatabase(t, dir, cfg, nodesettings.Values{
		"history_expiry_active": nodesettings.LatchOn,
	}, "")

	db, err := newTestDatabaseAt(t, dir, dir, cfg)
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))
	moved := nodeSettingsAt(t, dir)
	require.Equal(t, "preprod", moved["network"])
	require.Equal(t, nodesettings.LatchOn, moved["history_expiry_active"])
	require.Equal(t, "sqlite", moved["metadata_plugin"])
	require.NotEmpty(t, moved["blob_store_id"])

	partial := *cfg
	partial.Network = "preview"
	_, err = newTestDatabaseAt(t, dir, dir, &partial)
	var settingsErr NodeSettingsError
	require.ErrorAs(t, err, &settingsErr)
	require.Contains(t, settingsErr.Error(), "network")
}

// v0.94.2 settings that name another blob store are not moved: the pairing
// refusal they would have produced in the metadata store is produced here.
func TestOpenRefusesReleasedNodeSettingsOfAnotherBlobStore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := &Config{
		DataDir:        dir,
		StorageMode:    "core",
		Network:        "preprod",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		Role:           RoleReleased,
	}
	releasedNodeSettingsDatabase(t, dir, cfg, nil, "another-blob-store")

	_, err := newTestDatabaseAt(t, dir, dir, cfg)
	var settingsErr NodeSettingsError
	require.ErrorAs(t, err, &settingsErr)
	require.Contains(t, settingsErr.Error(), "blob store ID")
	info, err := dbinfo.Read(dir)
	require.NoError(t, err)
	require.False(t, info.HoldsGates(), "nothing was moved")
}
