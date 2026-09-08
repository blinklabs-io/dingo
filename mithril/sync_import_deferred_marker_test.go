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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/node"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
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
