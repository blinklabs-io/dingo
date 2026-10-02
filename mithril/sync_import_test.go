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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestUpdateMithrilReadyStateKeepsDeferredIndexPendingMarker pins the
// interaction between the deferred-index rebuild and the sync-state clear
// that ends a Mithril sync.
//
// mithril sync rebuilds only the critical subset and deliberately leaves
// deferred.SyncStateKey set so the first serve finishes the lazy manifest.
// updateMithrilReadyState then runs db.ClearSyncState, which is an
// unqualified DELETE FROM sync_state: without carrying the marker across
// the clear, every completed Mithril sync erases it moments after
// BuildCritical set it, and the lazy manifest entries are never built on
// any Mithril-bootstrapped database.
func TestUpdateMithrilReadyStateKeepsDeferredIndexPendingMarker(t *testing.T) {
	t.Parallel()
	db := newMithrilTestDB(t)
	manager, ok := db.Metadata().(metadata.DeferredIndexManager)
	require.True(t, ok, "sqlite metadata store must manage deferred indexes")

	// The Mithril sequence: drop the manifest, bulk load, rebuild the
	// critical subset only.
	require.NoError(t, manager.DropDeferredIndexes())
	require.NoError(t, manager.BuildCriticalDeferredIndexes())
	pending, err := manager.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(
		t, pending,
		"precondition: the critical rebuild leaves the lazy remainder pending",
	)

	ledgerStateHash := bytes.Repeat([]byte{0x55}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, ledgerStateHash),
	}, nil))
	require.NoError(t, db.SetSyncState("sync_status", "bootstrap", nil))

	require.NoError(t, updateMithrilReadyState(
		context.Background(),
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		ledgerStateHash,
		"",
		true,
	))

	pending, err = manager.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(
		t, pending,
		"the deferred-index pending marker must survive ClearSyncState "+
			"so the first serve builds the lazy manifest entries",
	)
	marker, err := db.GetSyncState(deferred.SyncStateKey, nil)
	require.NoError(t, err)
	require.Equal(t, deferred.SyncStateValue, marker)

	// The clear still does its job for everything else.
	status, err := db.GetSyncState("sync_status", nil)
	require.NoError(t, err)
	require.Empty(t, status, "sync_status must still be cleared")
}

// newMithrilFileTestDB is newMithrilTestDB with a data directory, so the
// SQLite catalog can be inspected directly.
func newMithrilFileTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})
	return db
}

// TestMithrilSyncLeavesLazyManifestForTheFirstServe walks the composition a
// Mithril-bootstrapped node actually takes: sync drops the manifest, rebuilds
// the critical subset, and calls updateMithrilReadyState; the first
// `dingo serve` then calls node.RepairDeferredIndexes, which finishes the
// manifest only if the pending marker survived the sync's sync-state clear.
//
// Without the marker carried across the clear, RepairDeferredIndexes takes its
// no-cycle-pending branch, every lazy manifest entry stays absent for the life
// of the database, and Node.startDeferredIndexMaintenance reads the same wiped
// marker and logs "deferred-index maintenance not needed".
func TestMithrilSyncLeavesLazyManifestForTheFirstServe(t *testing.T) {
	t.Parallel()
	db := newMithrilFileTestDB(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	lazy := dbtest.LazyManifestIndex(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// The sync: drop for the bulk load, rebuild the critical subset only.
	deferredIndexes := node.WithDeferredIndexes(db, logger)
	require.NoError(t, deferredIndexes.BuildCritical())
	require.False(
		t,
		dbtest.MetadataIndexExists(t, raw, lazy),
		"precondition: %s is lazy and the critical rebuild skips it",
		lazy,
	)

	ledgerStateHash := bytes.Repeat([]byte{0x66}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, ledgerStateHash),
	}, nil))
	require.NoError(t, updateMithrilReadyState(
		context.Background(),
		db, logger, nil, 30, ledgerStateHash, "", true,
	))

	// The first serve on a core-mode node.
	require.NoError(t, node.RepairDeferredIndexes(db, logger))

	require.True(
		t,
		dbtest.MetadataIndexExists(t, raw, lazy),
		"the first serve after a Mithril sync must finish the lazy "+
			"manifest entry %s",
		lazy,
	)
	manager, ok := db.Metadata().(metadata.DeferredIndexManager)
	require.True(t, ok)
	pending, err := manager.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(
		t, pending,
		"the full rebuild clears the marker it consumed",
	)
}

func newMithrilTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})
	return db
}

func TestEnsureMithrilBackfillCheckpointCreatesMissing(t *testing.T) {
	t.Parallel()

	db := newMithrilTestDB(t)

	require.NoError(t, ensureMithrilBackfillCheckpoint(db))

	cp, err := db.Metadata().GetBackfillCheckpoint(
		node.BackfillPhase, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, uint64(0), cp.LastSlot)
	require.False(t, cp.Completed)
	require.False(t, cp.StartedAt.IsZero())
	require.False(t, cp.UpdatedAt.IsZero())
}

func TestEnsureMithrilBackfillCheckpointPreservesIncomplete(t *testing.T) {
	t.Parallel()

	db := newMithrilTestDB(t)
	startedAt := time.Now().Add(-time.Hour)
	updatedAt := time.Now().Add(-time.Minute)
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      node.BackfillPhase,
			LastSlot:   1042527,
			TotalSlots: 2000000,
			StartedAt:  startedAt,
			UpdatedAt:  updatedAt,
			Completed:  false,
		},
		nil,
	))

	require.NoError(t, ensureMithrilBackfillCheckpoint(db))

	cp, err := db.Metadata().GetBackfillCheckpoint(
		node.BackfillPhase, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, uint64(1042527), cp.LastSlot)
	require.Equal(t, uint64(2000000), cp.TotalSlots)
	require.False(t, cp.Completed)
	require.Equal(t, startedAt.UnixNano(), cp.StartedAt.UnixNano())
	require.Equal(t, updatedAt.UnixNano(), cp.UpdatedAt.UnixNano())
}

func TestEnsureMithrilBackfillCheckpointReopensCompleted(t *testing.T) {
	t.Parallel()

	db := newMithrilTestDB(t)
	startedAt := time.Now().Add(-time.Hour)
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      node.BackfillPhase,
			LastSlot:   5000,
			TotalSlots: 5000,
			StartedAt:  startedAt,
			UpdatedAt:  startedAt,
			Completed:  true,
		},
		nil,
	))

	require.NoError(t, ensureMithrilBackfillCheckpoint(db))

	cp, err := db.Metadata().GetBackfillCheckpoint(
		node.BackfillPhase, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, uint64(5000), cp.LastSlot)
	require.Equal(t, uint64(5000), cp.TotalSlots)
	require.False(t, cp.Completed)
	require.Equal(t, startedAt.UnixNano(), cp.StartedAt.UnixNano())
	require.True(t, cp.UpdatedAt.After(startedAt))
}

func TestResetMithrilBackfillCheckpointReplaysFromFirstBlock(t *testing.T) {
	t.Parallel()

	db := newMithrilTestDB(t)
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      node.BackfillPhase,
			LastSlot:   1042527,
			TotalSlots: 2000000,
			StartedAt:  time.Now().Add(-time.Hour),
			UpdatedAt:  time.Now().Add(-time.Minute),
			Completed:  true,
		},
		nil,
	))

	require.NoError(t, resetMithrilBackfillCheckpoint(db))
	cp, err := db.Metadata().GetBackfillCheckpoint(node.BackfillPhase, nil)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Zero(t, cp.LastSlot)
	require.Zero(t, cp.TotalSlots)
	require.False(t, cp.Completed)
}

func TestUpdateMithrilReadyStateKeepsTrustBoundaryAtStableLedgerTip(
	t *testing.T,
) {
	t.Parallel()

	db := newMithrilTestDB(t)
	tipHash := bytes.Repeat([]byte{0x11}, 32)
	ledgerStateHash := bytes.Repeat([]byte{0x22}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.NewPoint(30, ledgerStateHash),
		BlockNumber: 1,
	}, nil))
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     2,
		Slot:   42,
		Hash:   tipHash,
		Number: 2,
		Type:   1,
		Cbor:   []byte{0x80},
	}, nil))

	require.NoError(t, updateMithrilReadyState(
		context.Background(),
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		ledgerStateHash,
		"",
		true,
	))

	slot, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(ledgerStateHash), hash)
	storedTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), storedTip.Point.Slot)
	require.Equal(t, ledgerStateHash, storedTip.Point.Hash)
}

func TestSetStableMithrilLedgerTipUsesCertifiedBlockNumber(t *testing.T) {
	t.Parallel()

	db := newMithrilTestDB(t)
	ledgerStateHash := bytes.Repeat([]byte{0x23}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     1,
		Slot:   30,
		Hash:   ledgerStateHash,
		Number: 123,
		Type:   1,
		Cbor:   []byte{0x80},
	}, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, ledgerStateHash),
	}, nil))

	require.NoError(t, setStableMithrilLedgerTip(
		context.Background(),
		db,
		30,
		ledgerStateHash,
	))

	storedTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), storedTip.Point.Slot)
	require.Equal(t, ledgerStateHash, storedTip.Point.Hash)
	require.Equal(t, uint64(123), storedTip.BlockNumber)
}

func TestSetStableMithrilLedgerTipRejectsPointOutsideCertifiedChain(
	t *testing.T,
) {
	t.Parallel()

	db := newMithrilTestDB(t)
	certifiedHash := bytes.Repeat([]byte{0x24}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     1,
		Slot:   30,
		Hash:   certifiedHash,
		Number: 123,
		Type:   1,
		Cbor:   []byte{0x80},
	}, nil))

	err := setStableMithrilLedgerTip(
		context.Background(),
		db,
		30,
		bytes.Repeat([]byte{0x25}, 32),
	)
	require.ErrorContains(t, err, "is not present in certified ImmutableDB")
}

func TestUpdateMithrilReadyStateStoresTrustBoundaryFromLedgerState(
	t *testing.T,
) {
	t.Parallel()

	db := newMithrilTestDB(t)
	ledgerStateHash := bytes.Repeat([]byte{0x33}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, ledgerStateHash),
	}, nil))

	require.NoError(t, updateMithrilReadyState(
		context.Background(),
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		ledgerStateHash,
		"",
		true,
	))

	slot, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(ledgerStateHash), hash)
}

func TestUpdateMithrilReadyStateClearsStaleTrustBoundaryHash(
	t *testing.T,
) {
	t.Parallel()

	db := newMithrilTestDB(t)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, nil),
	}, nil))
	require.NoError(t, db.SetSyncState(
		mithrilLedgerHashSyncKey,
		hex.EncodeToString(bytes.Repeat([]byte{0x44}, 32)),
		nil,
	))

	require.NoError(t, updateMithrilReadyState(
		context.Background(),
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		nil,
		"",
		true,
	))

	slot, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, hash)
}
