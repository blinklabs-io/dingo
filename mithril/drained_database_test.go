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
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// TestUpdateMithrilReadyStateRefusesADrainedDatabase requires the sync-state
// clear that ends an import to refuse, and to leave the drain markers and the
// latch in place, when the database has begun or finished a drain.
func TestUpdateMithrilReadyStateRefusesADrainedDatabase(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, db *database.Database)
		key     string
	}{
		{
			"latch bit",
			func(t *testing.T, db *database.Database) {
				require.NoError(t, db.RecordDrainedFamily(
					t.Context(), nodesettings.FamilyNonces,
				))
			},
			"",
		},
		{
			"drain begun",
			func(t *testing.T, db *database.Database) {
				require.NoError(t, db.SetSyncState(
					statedrain.MarkerPrefix+"nonces", "1", nil,
				))
			},
			statedrain.MarkerPrefix + "nonces",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newMithrilTestDB(t)
			tc.prepare(t, db)
			err := updateMithrilReadyState(
				context.Background(),
				db,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				nil, 30, nil,
			)
			require.ErrorIs(t, err, database.ErrLedgerStateDrained)
			mask, err := db.DrainedFamilies(t.Context())
			require.NoError(t, err)
			if tc.key == "" {
				require.True(t, mask.Has(nodesettings.FamilyNonces))
				return
			}
			marker, err := db.GetSyncState(tc.key, nil)
			require.NoError(t, err)
			require.Equal(t, "1", marker)
		})
	}
}

// TestProcessGapBlocksRefusesADrainedDatabase requires the gap replay, which
// reads the epoch rows and writes ledger state, to stop before touching a
// database that has drained a family.
func TestProcessGapBlocksRefusesADrainedDatabase(t *testing.T) {
	t.Parallel()
	db := newMithrilTestDB(t)
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyNonces,
	))
	gap := []models.Block{{Slot: 1, Hash: bytes.Repeat([]byte{1}, 32)}}

	err := processGapBlocks(
		t.Context(), db,
		slog.New(slog.NewTextHandler(io.Discard, nil)), gap,
	)

	require.ErrorIs(t, err, database.ErrLedgerStateDrained)
}

// TestSyncTakesADrainedDatabaseWhenTheLiveSetCanLoadIt requires a catch-up
// into a database with a latch bit to get past the dispatch that refuses it
// without a live UTxO set, to mark the import pending and to keep the latch.
func TestSyncTakesADrainedDatabaseWhenTheLiveSetCanLoadIt(t *testing.T) {
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
	mask, pending, err := db.DrainedImportPending(t.Context())
	require.NoError(t, err)
	require.True(t, pending, "the import is marked before anything is downloaded")
	require.True(t, mask.Has(nodesettings.FamilyNonces))
	latch, err := db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.True(t, latch.Has(nodesettings.FamilyNonces), "the latch is kept")
}
