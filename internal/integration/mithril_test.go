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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package integration

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/stretchr/testify/require"
)

// TestImportLedgerStateFromMithril downloads a preview network
// Mithril snapshot, extracts the ledger state, and imports it
// into a temporary SQLite database. This is an integration test
// that requires network access and takes several minutes.
func TestImportLedgerStateFromMithril(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if os.Getenv("DINGO_INTEGRATION_TEST") == "" {
		t.Skip(
			"set DINGO_INTEGRATION_TEST=1 to run " +
				"integration tests",
		)
	}

	ctx := context.Background()
	logger := slog.New(
		slog.NewTextHandler(
			os.Stderr,
			&slog.HandlerOptions{Level: slog.LevelInfo},
		),
	)

	// Download and extract a preview snapshot
	aggregatorURL, err := mithril.AggregatorURLForNetwork(
		"preview",
	)
	require.NoError(t, err, "getting aggregator URL")

	downloadDir := t.TempDir()

	result, err := mithril.Bootstrap(
		ctx,
		mithril.BootstrapConfig{
			Network:       "preview",
			AggregatorURL: aggregatorURL,
			DownloadDir:   downloadDir,
			Logger:        logger,
			OnProgress: func(p mithril.DownloadProgress) {
				if p.TotalBytes > 0 &&
					int(p.Percent)%10 == 0 {
					t.Logf(
						"download: %.1f%% (%d/%d)",
						p.Percent,
						p.BytesDownloaded,
						p.TotalBytes,
					)
				}
			},
		},
	)
	require.NoError(t, err, "bootstrapping from Mithril")
	t.Logf(
		"snapshot extracted: epoch=%d, immutable=%s",
		result.Snapshot.Beacon.Epoch,
		result.ImmutableDir,
	)

	// Search for ledger state in ancillary dir first,
	// then extract dir
	var lstatePath string
	var searchDir string
	for _, dir := range []string{
		result.AncillaryDir, result.ExtractDir,
	} {
		if dir == "" {
			continue
		}
		path, findErr := ledgerstate.FindLedgerStateFile(
			dir,
		)
		if findErr == nil {
			lstatePath = path
			searchDir = dir
			break
		}
		t.Logf("no ledger state in %s: %v", dir, findErr)
	}
	require.NotEmpty(
		t, lstatePath,
		"should find ledger state file",
	)
	t.Logf("ledger state file: %s", lstatePath)

	// Parse the snapshot
	state, err := ledgerstate.ParseSnapshot(lstatePath)
	require.NoError(t, err, "parsing snapshot")

	// Check for UTxO-HD tvar file
	tvarPath := ledgerstate.FindUTxOTableFile(searchDir)
	if tvarPath != "" {
		state.UTxOTablePath = tvarPath
		t.Logf("UTxO table file (UTxO-HD): %s", tvarPath)
	}

	require.NotNil(t, state.Tip, "tip should not be nil")
	t.Logf(
		"parsed: era=%s epoch=%d slot=%d",
		ledgerstate.EraName(state.EraIndex),
		state.Epoch,
		state.Tip.Slot,
	)
	require.Greater(
		t, state.Epoch, uint64(0),
		"epoch should be > 0",
	)
	if state.UTxOTablePath == "" {
		require.NotNil(
			t, state.UTxOData,
			"UTxO data should not be nil (legacy format)",
		)
	}
	require.NotNil(
		t, state.CertStateData,
		"cert state data should not be nil",
	)

	// Open a temporary database
	dbDir := t.TempDir()
	db, err := database.New(&database.Config{
		DataDir: dbDir,
		Logger:  logger,
	})
	require.NoError(t, err, "creating database")
	defer db.Close()

	// Import the ledger state
	var lastProgress ledgerstate.ImportProgress
	err = ledgerstate.ImportLedgerState(
		ctx,
		ledgerstate.ImportConfig{
			Database: db,
			State:    state,
			Logger:   logger,
			OnProgress: func(p ledgerstate.ImportProgress) {
				lastProgress = p
				t.Logf("import: %s", p.Description)
			},
		},
	)
	require.NoError(t, err, "importing ledger state")

	t.Logf("final progress: %+v", lastProgress)

	// Verify tip was set
	store := db.Metadata()
	txn := db.MetadataTxn(false)
	defer txn.Release()
	tip, err := store.GetTip(txn.Metadata())
	require.NoError(t, err, "getting tip")
	require.Equal(
		t,
		state.Tip.Slot,
		tip.Point.Slot,
		"tip slot should match snapshot",
	)
	t.Logf(
		"verified tip: slot=%d hash=%x",
		tip.Point.Slot,
		tip.Point.Hash,
	)
}
