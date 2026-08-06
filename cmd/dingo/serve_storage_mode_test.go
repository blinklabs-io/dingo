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

	dingo "github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestEffectiveStorageMode pins effectiveStorageMode's dev-mode override so
// preflight callers and internal/node.Run's own "dev mode always uses API
// storage" upgrade never disagree about which mode a dev-mode config
// actually runs with.
func TestEffectiveStorageMode(t *testing.T) {
	tests := []struct {
		name    string
		runMode config.RunMode
		mode    string
		want    dingo.StorageMode
	}{
		{
			"dev mode upgrades core to api",
			config.RunModeDev,
			"core",
			dingo.StorageModeAPI,
		},
		{
			"dev mode leaves api as api",
			config.RunModeDev,
			"api",
			dingo.StorageModeAPI,
		},
		{
			"dev mode upgrades unset mode to api",
			config.RunModeDev,
			"",
			dingo.StorageModeAPI,
		},
		{
			"serve mode leaves core alone",
			config.RunModeServe,
			"core",
			dingo.StorageModeCore,
		},
		{
			"serve mode leaves api alone",
			config.RunModeServe,
			"api",
			dingo.StorageModeAPI,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveStorageMode(&config.Config{
				RunMode:     tt.runMode,
				StorageMode: tt.mode,
			})
			require.Equal(t, tt.want, got)
		})
	}
}

// TestCheckSyncStateDevModeAgreesWithLaterAPIModeOpen is a regression test
// for a reachable startup failure: validate.go exempts midnight.enabled
// from its storageMode-must-be-api check in dev mode, on the assumption
// that internal/node.Run's own "dev mode always uses API storage" override
// makes the contradiction moot. But serveRun's preflight (checkSyncState)
// opens the database before node.Run ever runs, and used to do so with the
// raw configured storage mode ("core" here) rather than the mode node.Run
// was about to upgrade to. That latched storage_mode="core" as a node
// settings gate; storage_mode is a LatchEnum that only ever moves
// api-to-core, never back, so node.Run's subsequent api-mode open of the
// same database would then fail enforcement.
//
// With effectiveStorageMode applied consistently, checkSyncState's open
// already uses "api" for a dev-mode config, so the gate it latches matches
// what every later open (including the one this test performs directly,
// standing in for node.Run's) needs.
func TestCheckSyncStateDevModeAgreesWithLaterAPIModeOpen(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	cfg := &config.Config{
		RunMode:      config.RunModeDev,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: dir,
		Plugins:      testStoragePlugins(),
	}

	// Preflight open, exactly as serveRun performs it before node.Run runs.
	require.NoError(t, checkSyncState(cfg, logger))

	// Stand-in for node.Run's own database open, once it has upgraded to
	// API mode. Reuses openConfiguredDatabase (which node.go's real open
	// path also ultimately composes the same storage config through) so
	// this exercises the same effective-mode computation both call sites
	// share.
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.RecoveryError())
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })

	gates, err := runtime.Database.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(
		t,
		"api",
		gates["storage_mode"],
		"dev mode's preflight open must have already latched api, not core",
	)
}
