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

package mithril

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// TestUpdateMithrilReadyStateKeepsTheLedgerStateLatch requires the sync-state
// clear that ends an import to keep the statedrain markers and the latch: an
// import writes no SQL ledger state, so the families stay where they were.
func TestUpdateMithrilReadyStateKeepsTheLedgerStateLatch(t *testing.T) {
	t.Parallel()
	db := newMithrilTestDB(t)
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))
	key := statedrain.MarkerPrefix + "nonces"
	require.NoError(t, db.SetSyncState(key, "1", nil))
	_ = updateMithrilReadyState(
		context.Background(),
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil, 30, nil,
	)
	mask, err := db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, mask.Has(nodesettings.FamilyNonces))
	marker, err := db.GetSyncState(key, nil)
	require.NoError(t, err)
	require.Equal(t, "1", marker)
}

// TestSyncTakesADatabaseWithALatchedFamily requires a catch-up into a
// database with a latch bit to get past the dispatch and to keep the latch.
func TestSyncTakesADatabaseWithALatchedFamily(t *testing.T) {
	t.Parallel()
	fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
	dataDir := t.TempDir()
	seedCompleteDB(t, dataDir, "core", 0, true)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))
	require.NoError(t, dbtest.CloseDatabase(db))
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	_, err = Sync(context.Background(), SyncConfig{
		Network:           "preprod",
		DataDir:           dataDir,
		Backend:           BackendV2,
		AggregatorURL:     fix.server.URL,
		AllowInsecureHTTP: true,
		StoragePlugins:    testStoragePlugins(),
		DatabaseWorkers:   1,
		UtxoSet:           set,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	// The fixture's immutable files are not real blocks, so the run stops
	// reading them, after the dispatch and before it writes any ledger state.
	require.Error(t, err)
	require.NotErrorIs(t, err, database.ErrLedgerStateDrained)

	db, err = dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	latch, err := db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, latch.Has(nodesettings.FamilyNonces), "the latch is kept")
}
