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
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
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
	if errors.Is(err, mithril.ErrNoSnapshotsAvailable) {
		t.Skipf("Mithril aggregator has no snapshots: %v", err)
	}
	require.NoError(t, err, "bootstrapping from Mithril")
	t.Logf(
		"snapshot extracted: epoch=%d, immutable=%s",
		result.Snapshot.Beacon.Epoch,
		result.ImmutableDir,
	)

	// Search for ledger state in ancillary dir first, then extract dir,
	// through the directory handles the bootstrap vetted — the same discovery
	// the import performs. Searching by pathname here would leave the
	// integration coverage on a code path production no longer takes.
	var snapshot *ledgerstate.SnapshotFiles
	for _, root := range []*os.Root{
		result.AncillaryRoot, result.ExtractRoot,
	} {
		if root == nil {
			continue
		}
		files, findErr := ledgerstate.OpenSnapshotAtOrBefore(
			root, ^uint64(0),
		)
		if findErr == nil {
			snapshot = files
			break
		}
		t.Logf("no ledger state in %s: %v", root.Name(), findErr)
	}
	require.NotNil(t, snapshot, "should find ledger state file")
	defer snapshot.Close()
	t.Logf("ledger state file: %s", snapshot.StatePath)

	// Parse the snapshot
	state, err := ledgerstate.ParseSnapshotFile(snapshot.State)
	require.NoError(t, err, "parsing snapshot")

	// Check for UTxO-HD tvar file
	if snapshot.Table != nil {
		state.UTxOTablePath = snapshot.TablePath
		state.UTxOTableFile = snapshot.Table
		t.Logf("UTxO table file (UTxO-HD): %s", snapshot.TablePath)
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
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dbDir,
		Logger:  logger,
	})
	require.NoError(t, err, "creating database")
	defer dbtest.CloseDatabase(db)

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
