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
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestCheckSyncStateRepairsLegacyAlonzoPParamsUnit covers the open that
// reaches a legacy database first. serveRun runs checkSyncState before
// node.Run, so the repair's genesis input has to be resolved by
// openConfiguredDatabase from the configuration alone: when it is not, this
// preflight — not the node's own later open, which does resolve it — is what
// aborts with the resync instruction the in-place repair exists to remove.
func TestCheckSyncStateRepairsLegacyAlonzoPParamsUnit(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: dir,
		Plugins:      testStoragePlugins(),
	}

	word := cardano.AlonzoLovelacePerUtxoWord(
		nil,
		cfg.CardanoConfig,
		cfg.Network,
	)
	require.NotZero(
		t,
		word,
		"the embedded preview config must supply an Alonzo genesis word",
	)

	// Seed a database in the shape a pre-gouroboros-v0.205.7 release left
	// behind: one Alonzo row holding the lossy quotient, and the
	// conservative marker migration v20 writes for it.
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	params := alonzo.AlonzoProtocolParameters{AdaPerUtxoByte: word / 8}
	encoded, err := cbor.Encode(&params)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetPParams(
		encoded, 0, 0, alonzo.EraIdAlonzo, nil,
	))
	require.NoError(t, runtime.Close(context.Background()))

	sqlDB, err := sql.Open("sqlite", filepath.Join(dir, "metadata.sqlite"))
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`UPDATE node_settings_gate SET value = ? WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitLegacyByteV0,
		nodesettings.AlonzoPParamsUnitGateName,
	)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	require.NoError(t, checkSyncState(cfg, logger))

	reopened, err := openConfiguredDatabase(
		context.Background(),
		cfg,
		logger,
		1,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	gates, err := reopened.Database.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(
		t,
		nodesettings.AlonzoPParamsUnitWordV1,
		gates[nodesettings.AlonzoPParamsUnitGateName],
	)
}
