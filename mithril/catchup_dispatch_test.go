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
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// seedCompleteDB creates a database at dataDir that looks like a finished sync:
// one chain block and a clear sync_status, optionally with an immutable-import
// marker. determineSyncMode classifies it as syncModeCatchUp.
func seedCompleteDB(
	t *testing.T,
	dataDir, storageMode string,
	marker uint64,
	setMarker bool,
) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		StorageMode: storageMode,
	})
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     42,
		Hash:     bytes.Repeat([]byte{0xaa}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   7,
		Type:     6,
	}, nil))
	if setMarker {
		require.NoError(t, setImmutableImportMarker(db, marker))
	}
	require.NoError(t, dbtest.CloseDatabase(db))
}

// TestSyncCatchUpDispatch covers the two state-detected catch-up decisions that
// must not mutate the database: an up-to-date core DB is a no-op, and an api-mode
// DB is rejected (api catch-up is unsupported in this version).
func TestSyncCatchUpDispatch(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("up-to-date core DB returns without syncing", func(t *testing.T) {
		fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
		dataDir := t.TempDir()
		// Marker at the artifact's immutable tip → already up to date.
		seedCompleteDB(t, dataDir, "core", 5, true)

		res, err := Sync(context.Background(), SyncConfig{
			Network:           "preview",
			DataDir:           dataDir,
			StorageMode:       "core",
			Backend:           BackendV2,
			AggregatorURL:     fix.server.URL,
			AllowInsecureHTTP: true,
			StoragePlugins:    testStoragePlugins(),
			DatabaseWorkers:   1,
			Logger:            discard,
		})
		require.NoError(t, err)
		require.Nil(t, res.Snapshot, "up-to-date catch-up should not sync")
	})

	t.Run("api mode rejects a divergent chain before mutation", func(t *testing.T) {
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			fallbackLedgerState: true,
			missingAncillary:    true,
		})
		_, anchorHash := validImmutableFiles(t, 1000)
		wrongHash := bytes.Clone(anchorHash)
		wrongHash[0] ^= 0xff
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "api",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1000,
			Hash:     wrongHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, db.SetSyncState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))

		_, err = Sync(context.Background(), SyncConfig{
			Network:           "preprod",
			DataDir:           dataDir,
			StorageMode:       "api",
			Backend:           BackendV2,
			AggregatorURL:     fixture.server.URL,
			AllowInsecureHTTP: true,
			StoragePlugins:    testStoragePlugins(),
			DatabaseWorkers:   1,
			Logger:            discard,
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "diverg")
		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "api",
			Logger:      discard,
		})
		require.NoError(t, err)
		block, err := database.BlockByHash(db, wrongHash)
		require.NoError(t, err)
		require.EqualValues(t, 1000, block.Slot)
		status, err := db.GetSyncState("sync_status", nil)
		require.NoError(t, err)
		require.Empty(t, status)
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	t.Run("legacy reward repair reconciles an existing database in place", func(t *testing.T) {
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			fallbackLedgerState: true,
			missingAncillary:    true,
		})
		_, anchorHash := validImmutableFiles(t, 1000)
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1000,
			Hash:     anchorHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, setImmutableImportMarker(db, 0))
		require.NoError(t, db.SetSyncState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))

		result, err := Sync(context.Background(), SyncConfig{
			Network:                 "preprod",
			DataDir:                 dataDir,
			StorageMode:             "core",
			Backend:                 BackendV2,
			AggregatorURL:           fixture.server.URL,
			AllowInsecureHTTP:       true,
			StoragePlugins:          testStoragePlugins(),
			DatabaseWorkers:         1,
			Logger:                  discard,
			RepairLegacyRewardState: true,
		})
		require.NoError(t, err)
		require.NotNil(t, result.Snapshot)

		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		pending, err := RewardStateRepairPending(db)
		require.NoError(t, err)
		require.False(t, pending)
		block, err := database.BlockByHash(db, anchorHash)
		require.NoError(t, err)
		require.EqualValues(t, 1000, block.Slot,
			"repair must retain the existing chain anchor")
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	t.Run("legacy reward repair rebuilds api metadata in place", func(t *testing.T) {
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			fallbackLedgerState: true,
			missingAncillary:    true,
		})
		files, _ := validImmutableFiles(t, 1000)
		firstHash := bytes.Clone(files["immutable/00000.secondary"][16:48])
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "api",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     999,
			Hash:     firstHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, setImmutableImportMarker(db, 0))
		require.NoError(t, db.Metadata().SetBackfillCheckpoint(
			&models.BackfillCheckpoint{
				Phase:     "metadata",
				LastSlot:  999,
				StartedAt: time.Now().Add(-time.Hour),
				UpdatedAt: time.Now().Add(-time.Minute),
				Completed: true,
			},
			nil,
		))
		require.NoError(t, db.SetSyncState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))

		firstBackfilledSlot := ^uint64(0)
		_, err = Sync(context.Background(), SyncConfig{
			Network:                 "preprod",
			DataDir:                 dataDir,
			StorageMode:             "api",
			Backend:                 BackendV2,
			AggregatorURL:           fixture.server.URL,
			AllowInsecureHTTP:       true,
			StoragePlugins:          testStoragePlugins(),
			DatabaseWorkers:         1,
			BackfillBatchSize:       100,
			Logger:                  discard,
			RepairLegacyRewardState: true,
			OnProgress: func(progress SyncProgress) {
				if progress.Phase == PhaseBackfill &&
					progress.Active && progress.CurrentSlot > 0 &&
					progress.CurrentSlot < firstBackfilledSlot {
					firstBackfilledSlot = progress.CurrentSlot
				}
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1000, firstBackfilledSlot,
			"API repair must run historical metadata backfill")

		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "api",
			Logger:      discard,
		})
		require.NoError(t, err)
		pending, err := RewardStateRepairPending(db)
		require.NoError(t, err)
		require.False(t, pending)
		block, err := database.BlockByHash(db, firstHash)
		require.NoError(t, err)
		require.EqualValues(t, 999, block.Slot)
		checkpoint, err := db.Metadata().GetBackfillCheckpoint("metadata", nil)
		require.NoError(t, err)
		require.NotNil(t, checkpoint)
		require.True(t, checkpoint.Completed)
		require.GreaterOrEqual(t, checkpoint.LastSlot, uint64(1000))
		require.NoError(t, dbtest.CloseDatabase(db))
	})
}

// TestDecideCatchUp pins the dispatch decision that selects catch-up semantics
// (divergence check before mutation + reconcile of stale live rows) for every
// v2 import into a previously-complete database — including interrupted
// catch-ups and databases without an import marker — so no path can re-import
// a snapshot over live state without reconciliation.
//
// The subtests share one database (database.New is expensive) and are
// order-dependent: the fresh-database case runs before any block is seeded,
// and each later subtest sets sync_status and the import marker to exactly
// the state it needs. Run the whole function, not individual subtests.
func TestDecideCatchUp(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()
	// Unroutable aggregator: proves decision paths that must not fetch.
	const noAggregator = "http://127.0.0.1:1"

	db := newSyncModeTestDB(t)

	// setState pins the shared database to exactly the sync_status ("" =
	// clear) and marker (hasMarker=false = absent) a subtest needs.
	setState := func(
		t *testing.T, status string, marker uint64, hasMarker bool,
	) {
		t.Helper()
		if status == "" {
			require.NoError(t, db.DeleteSyncState("sync_status", nil))
		} else {
			require.NoError(t, db.SetSyncState("sync_status", status, nil))
		}
		if hasMarker {
			require.NoError(t, setImmutableImportMarker(db, marker))
		} else {
			require.NoError(t, db.DeleteSyncState(syncKeyImmutableMax, nil))
		}
		require.NoError(t, db.DeleteSyncState(
			RewardStateRepairPendingKey, nil,
		))
		require.NoError(t, db.DeleteSyncState(syncKeyCatchUpActive, nil))
	}
	modeOf := func(t *testing.T) syncMode {
		t.Helper()
		mode, err := determineSyncMode(db)
		require.NoError(t, err)
		return mode
	}

	t.Run("fresh database bootstraps", func(t *testing.T) {
		mode := modeOf(t)
		require.Equal(t, syncModeBootstrap, mode)
		dec, err := decideCatchUp(
			ctx, db, mode, BackendV2, "core", noAggregator, true, discard,
		)
		require.NoError(t, err)
		require.False(t, dec.engage)
		require.False(t, dec.upToDate)
	})

	// Every remaining subtest runs against a database with chain data.
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     42,
		Hash:     bytes.Repeat([]byte{0xaa}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   7,
		Type:     6,
	}, nil))

	t.Run("markerless complete database catches up over the full range",
		func(t *testing.T) {
			setState(t, "", 0, false)
			mode := modeOf(t)
			require.Equal(t, syncModeCatchUp, mode)
			dec, err := decideCatchUp(
				ctx, db, mode, BackendV2, "core", noAggregator, true, discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage,
				"markerless complete DB must reconcile on re-import")
			require.EqualValues(t, 0, dec.start,
				"no marker means the full artifact range")
		})

	t.Run("markerless complete api database keeps the ordinary sync path",
		func(t *testing.T) {
			setState(t, "", 0, false)
			dec, err := decideCatchUp(
				ctx,
				db,
				modeOf(t),
				BackendV2,
				"api",
				noAggregator,
				true,
				discard,
			)
			require.NoError(t, err)
			require.False(t, dec.engage)
			require.False(t, dec.upToDate)
		})

	t.Run("api catch-up with marker is rejected without reward repair", func(t *testing.T) {
		setState(t, "", 1, true)
		_, err := decideCatchUp(
			ctx, db, modeOf(t), BackendV2, "api", noAggregator, true, discard,
		)
		require.ErrorContains(t, err, "API-mode metadata replacement")
	})

	t.Run("v1 backend never engages", func(t *testing.T) {
		setState(t, "", 2, true)
		dec, err := decideCatchUp(
			ctx, db, modeOf(t), BackendV1, "core", noAggregator, true, discard,
		)
		require.NoError(t, err)
		require.False(t, dec.engage)
		require.False(t, dec.upToDate)
	})

	t.Run("marker behind target engages catch-up from the marker",
		func(t *testing.T) {
			fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
			setState(t, "", 2, true)
			dec, err := decideCatchUp(
				ctx, db, modeOf(t), BackendV2, "core", fix.server.URL,
				true, discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage)
			require.EqualValues(t, 2, dec.start)
		})

	t.Run("marker at target is up to date", func(t *testing.T) {
		fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
		setState(t, "", 5, true)
		dec, err := decideCatchUp(
			ctx,
			db,
			modeOf(t),
			BackendV2,
			"core",
			fix.server.URL,
			true,
			discard,
		)
		require.NoError(t, err)
		require.False(t, dec.engage)
		require.True(t, dec.upToDate)
	})

	t.Run("api mode with a newer target engages catch-up", func(t *testing.T) {
		fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
		setState(t, "", 1, true)
		require.NoError(t, db.SetSyncState(
			RewardStateRepairPendingKey, "1", nil,
		))
		dec, err := decideCatchUp(
			ctx, db, modeOf(t), BackendV2, "api", fix.server.URL, true, discard,
		)
		require.NoError(t, err)
		require.True(t, dec.engage)
		require.EqualValues(t, 1, dec.start)
	})

	t.Run("interrupted sync with marker re-runs as catch-up",
		func(t *testing.T) {
			setState(t, syncStatusInProgress, 7, true)
			mode := modeOf(t)
			require.Equal(t, syncModeResume, mode)
			dec, err := decideCatchUp(
				ctx, db, mode, BackendV2, "core", noAggregator, true, discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage,
				"interrupted catch-up must re-run with reconcile")
			require.EqualValues(t, 7, dec.start)
		})

	t.Run("interrupted api sync with marker resumes repair from marker",
		func(t *testing.T) {
			setState(t, syncStatusInProgress, 7, true)
			require.NoError(t, db.SetSyncState(
				RewardStateRepairPendingKey, "1", nil,
			))
			dec, err := decideCatchUp(
				ctx,
				db,
				modeOf(t),
				BackendV2,
				"api",
				noAggregator,
				true,
				discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage)
			require.EqualValues(t, 7, dec.start)
			require.False(t, dec.upToDate)
		})

	t.Run("interrupted sync without marker resumes normally",
		func(t *testing.T) {
			setState(t, syncStatusInProgress, 0, false)
			dec, err := decideCatchUp(
				ctx,
				db,
				modeOf(t),
				BackendV2,
				"core",
				noAggregator,
				true,
				discard,
			)
			require.NoError(t, err)
			require.False(t, dec.engage)
			require.False(t, dec.upToDate)
		})

	t.Run("interrupted markerless catch-up re-runs as catch-up",
		func(t *testing.T) {
			// A markerless catch-up (fix for pre-marker databases) writes no
			// marker until completion; the active flag is its only trace.
			setState(t, syncStatusInProgress, 0, false)
			require.NoError(t, setCatchUpActive(db))
			dec, err := decideCatchUp(
				ctx,
				db,
				modeOf(t),
				BackendV2,
				"core",
				noAggregator,
				true,
				discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage,
				"interrupted markerless catch-up must re-run with reconcile")
			require.EqualValues(t, 0, dec.start)
		})
}
