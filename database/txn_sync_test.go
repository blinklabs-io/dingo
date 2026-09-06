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

package database

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	metadataSqlite "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newSyncBarrierTestDB builds a Database pairing the given blob store with a
// real in-memory sqlite metadata store, which is the combination the
// combined-commit durability barrier applies to. The parameter is the
// interface so a case can inject a store whose Sync or SetCommitTimestamp
// fails without changing the shared mockBlobStore.
func newSyncBarrierTestDB(
	t *testing.T,
	store blob.BlobStore,
) *Database {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	sqliteStore, err := metadataSqlite.NewSQLStore(
		metadataSqlite.Config{},
		metadata.ProviderDependencies{Logger: logger},
	)
	require.NoError(t, err)
	require.NoError(t, sqliteStore.Start(context.Background()))
	db := &Database{
		blobRef:  newBlobStoreRef(store),
		metadata: sqliteStore,
		logger:   logger,
		config:   &Config{Logger: logger},
	}
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func syncBarrierTestTip() ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 193600907,
			Hash: bytes.Repeat([]byte{0x41}, 32),
		},
		BlockNumber: 12345,
	}
}

// TestCommitSyncsBlobAfterBlobCommit covers the cross-store durability
// ordering. Committing the blob transaction before the metadata transaction
// only keeps the blob store ahead of the metadata tip in memory: the metadata
// store fsyncs per commit while the blob store buffers, so without a sync
// barrier an unclean shutdown leaves a durable metadata tip referencing blocks
// the blob store discarded. Startup reconciliation can trim a blob store that
// is ahead but cannot rebuild blocks missing beneath the ledger tip, so the
// barrier must run on every combined commit, after the blob commit.
func TestCommitSyncsBlobAfterBlobCommit(t *testing.T) {
	store := &mockBlobStore{}
	db := newSyncBarrierTestDB(t, store)

	txn := db.Transaction(true)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
	require.NoError(t, txn.Commit())

	require.Equal(
		t,
		1,
		store.syncCount,
		"combined commit should sync the blob store exactly once",
	)
	require.Equal(
		t,
		1,
		store.syncAtBlobCommitCount,
		"sync must run after the blob commit, so the commit it makes durable is included",
	)

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, syncBarrierTestTip().Point.Slot, tip.Point.Slot)
}

// TestCommitDoesNotSyncMetadataOnlyTransaction pins that the fsync is scoped to
// transactions that span both stores. A metadata-only transaction has no blob
// write whose durability the metadata commit could outrun, so paying an fsync
// for it would be pure cost.
func TestCommitDoesNotSyncMetadataOnlyTransaction(t *testing.T) {
	store := &mockBlobStore{}
	db := newSyncBarrierTestDB(t, store)

	txn := db.MetadataTxn(true)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
	require.NoError(t, txn.Commit())

	require.Zero(
		t,
		store.syncCount,
		"metadata-only commit should not sync the blob store",
	)
}

// TestCommitFailedBlobSyncDoesNotCommitMetadata is the guarantee that makes the
// barrier worth anything: if the blob store cannot be made durable, the
// metadata tip must not advance past it. The blob transaction is already
// committed and carries the new commit timestamp at that point, which is the
// same inconsistency a failed metadata commit leaves, so it is reported as a
// partial commit to drive the existing blob-trimming recovery.
func TestCommitFailedBlobSyncDoesNotCommitMetadata(t *testing.T) {
	syncErr := errors.New("fsync failed")
	store := &mockBlobStore{syncErr: syncErr}
	db := newSyncBarrierTestDB(t, store)

	txn := db.Transaction(true)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))

	err := txn.Commit()
	require.Error(t, err)
	require.ErrorIs(t, err, syncErr)
	require.ErrorContains(t, err, "blob sync failed")
	require.ErrorIs(
		t,
		err,
		types.ErrPartialCommit,
		"a committed blob with an un-synced tip should route through partial-commit recovery",
	)

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.NotEqual(
		t,
		syncBarrierTestTip().Point.Slot,
		tip.Point.Slot,
		"metadata tip must not advance when the blob store could not be synced",
	)
}
