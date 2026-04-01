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
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func newSettingsTestDB(t *testing.T, dataDir, storageMode, network string) (*Database, error) {
	t.Helper()
	return New(&Config{
		DataDir:        dataDir,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		StorageMode:    storageMode,
		Network:        network,
		Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
}

func TestNodeSettingsPersistence(t *testing.T) {
	dataDir := t.TempDir()

	// First open: persists settings
	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "preview", s.Network)
	require.NoError(t, db.Close())

	// Reopen with same settings: succeeds
	db, err = newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestNodeSettingsRejectStorageModeChange(t *testing.T) {
	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Change storage mode → error
	db, err = newSettingsTestDB(t, dataDir, "api", "preview")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 1)
	require.Contains(t, nsErr.Mismatches[0], "storage mode")
	if db != nil {
		require.NoError(t, db.Close())
	}
}

func TestNodeSettingsRejectNetworkChange(t *testing.T) {
	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Change network → error
	db, err = newSettingsTestDB(t, dataDir, "core", "mainnet")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 1)
	require.Contains(t, nsErr.Mismatches[0], "network")
	if db != nil {
		require.NoError(t, db.Close())
	}
}

func TestNodeSettingsAllowOpenWithoutConfiguredNetwork(t *testing.T) {
	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = newSettingsTestDB(t, dataDir, "core", "")
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestNodeSettingsAllowDeferredNetworkInitialization(t *testing.T) {
	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "")
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "", s.Network)
	require.NoError(t, db.Close())

	db, err = newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)

	s, err = db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "preview", s.Network)
	require.NoError(t, db.Close())
}

func TestNodeSettingsRejectStorageModeChangeWhenNetworkUnset(t *testing.T) {
	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = newSettingsTestDB(t, dataDir, "api", "")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 1)
	require.Contains(t, nsErr.Mismatches[0], "storage mode")
	if db != nil {
		require.NoError(t, db.Close())
	}
}

func TestNodeSettingsRejectBothChanged(t *testing.T) {
	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Change both → error with 2 mismatches
	db, err = newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	require.Len(t, nsErr.Mismatches, 2)
	if db != nil {
		require.NoError(t, db.Close())
	}
}

func TestNodeSettingsAPIMode(t *testing.T) {
	dataDir := t.TempDir()

	// First open with "api" + "mainnet"
	db, err := newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, "api", s.StorageMode)
	require.Equal(t, "mainnet", s.Network)
	require.NoError(t, db.Close())

	// Reopen: succeeds
	db, err = newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Attempt downgrade → error
	db, err = newSettingsTestDB(t, dataDir, "core", "mainnet")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	if db != nil {
		require.NoError(t, db.Close())
	}
}

func TestNodeSettingsMetadataSetDoesNotOverwrite(t *testing.T) {
	dataDir := t.TempDir()

	db, err := newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)

	err = db.Metadata().SetNodeSettings(&types.NodeSettings{
		StorageMode: "api",
		Network:     "mainnet",
	})
	require.NoError(t, err)

	s, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, "core", s.StorageMode)
	require.Equal(t, "preview", s.Network)
	require.NoError(t, db.Close())

	db, err = newSettingsTestDB(t, dataDir, "core", "preview")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = newSettingsTestDB(t, dataDir, "api", "mainnet")
	require.Error(t, err)
	var nsErr NodeSettingsError
	require.True(t, errors.As(err, &nsErr))
	if db != nil {
		require.NoError(t, db.Close())
	}
}
