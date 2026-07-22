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

package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestMithrilSyncRefusedWithDelegatorInactivity covers Guard 1: the mithril
// sync command (and the shared "sync --mithril" path) must refuse before doing
// any work when the CIP-0163 delegator-inactivity gate is enabled, because a
// Mithril bootstrap cannot carry reward-account expiration state.
func TestMithrilSyncRefusedWithDelegatorInactivity(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.SetContext(config.WithContext(context.Background(), &config.Config{
		DelegatorInactivityEnabled: true,
	}))

	err := mithrilSyncRunE(cmd, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot use Mithril bootstrap")
}

// TestCheckMithrilInactivityCompat covers Guard 2: serving is refused only when
// the gate is enabled AND the database was Mithril-bootstrapped. It is a no-op
// with the gate off, and allowed on a fresh (genesis-synced) database.
func TestCheckMithrilInactivityCompat(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Gate off: no-op, no database access.
	require.NoError(t, checkMithrilInactivityCompat(
		&config.Config{DelegatorInactivityEnabled: false},
		logger,
	))

	dir := t.TempDir()
	cfg := &config.Config{
		DelegatorInactivityEnabled: true,
		DatabasePath:               dir,
		Plugins:                    testStoragePlugins(),
	}

	// Gate on, fresh (non-bootstrapped) database: allowed.
	require.NoError(t, checkMithrilInactivityCompat(cfg, logger))

	// Mark the database as Mithril-bootstrapped, then confirm serving is
	// refused. The key mirrors mithril's durable immutable-import marker
	// (mithril.WasBootstrapped / import_marker.go).
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir,
		Logger:  logger,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetSyncState("mithril_immutable_max", "26887", nil))
	require.NoError(t, dbtest.CloseDatabase(db))

	err = checkMithrilInactivityCompat(cfg, logger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot use Mithril bootstrap")
}

// TestCheckMithrilInactivityCompatLegacyMarker covers the pre-v0.62.0 (#2694)
// bootstrap case: a database Mithril-bootstrapped before the immutable-import
// marker existed carries only the durable mithril_ledger_slot trust boundary.
// Serving with the gate enabled must still be refused, or such a database could
// start with CIP-0163 enabled and diverge from a genesis-synced node.
func TestCheckMithrilInactivityCompatLegacyMarker(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	dir := t.TempDir()
	cfg := &config.Config{
		DelegatorInactivityEnabled: true,
		DatabasePath:               dir,
		Plugins:                    testStoragePlugins(),
	}

	// Legacy bootstrap: only the durable mithril_ledger_slot boundary is set,
	// with no newer mithril_immutable_max marker.
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir,
		Logger:  logger,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetSyncState("mithril_ledger_slot", "12345678", nil))
	require.NoError(t, dbtest.CloseDatabase(db))

	err = checkMithrilInactivityCompat(cfg, logger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot use Mithril bootstrap")
}

func testStoragePlugins() config.PluginsConfig {
	return config.PluginsConfig{
		Storage: config.StoragePluginsConfig{
			Blob: plugin.Selection{
				Provider: "badger",
				Config:   map[string]any{},
			},
			Metadata: plugin.Selection{
				Provider: "sqlite",
				Config:   map[string]any{},
			},
		},
	}
}
