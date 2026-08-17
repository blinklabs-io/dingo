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
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// newTestDatabaseAt mirrors newTestDatabase (test_database_test.go) but
// resolves the blob and metadata stores from independently supplied
// directories, so a test can pair a metadata store with a blob store it was
// never initialised with -- something a single shared config.DataDir cannot
// express.
func newTestDatabaseAt(
	tb testing.TB,
	metaDir, blobDir string,
	config *Config,
) (*Database, error) {
	tb.Helper()
	if config == nil {
		config = DefaultConfig
	}
	host := plugin.NewHost()
	if err := badger.RegisterProvider(host); err != nil {
		return nil, err
	}
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, err
	}
	blobStore, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{
			DataDir: blobDir, StorageMode: config.StorageMode,
			Logger: config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		return nil, err
	}
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{
			DataDir: metaDir, StorageMode: config.StorageMode,
			Logger: config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	db, err := New(config, Stores{Blob: blobStore, Metadata: metadataStore})
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	testDatabaseHosts.Store(db, host)
	tb.Cleanup(func() {
		if closeErr := closeTestDatabase(db); closeErr != nil {
			tb.Errorf("close test database runtime: %v", closeErr)
		}
	})
	return db, err
}

// storesWithFreshBlob resolves a metadata store from metaDir, which may
// already contain a previously initialised database, paired with a brand
// new blob store from an unrelated temporary directory -- simulating a
// swapped or emptied blob store/bucket. Its host is stopped via tb.Cleanup
// unconditionally, since this helper never calls New itself and so has no
// error path of its own to stop it on.
func storesWithFreshBlob(tb testing.TB, metaDir string) Stores {
	tb.Helper()
	host := plugin.NewHost()
	tb.Cleanup(func() {
		if err := host.Stop(context.Background()); err != nil {
			tb.Errorf("stop plugin host: %v", err)
		}
	})
	require.NoError(tb, badger.RegisterProvider(host))
	require.NoError(tb, sqlite.RegisterProvider(host))
	blobStore, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{DataDir: tb.TempDir()},
	)
	require.NoError(tb, err)
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{DataDir: metaDir},
	)
	require.NoError(tb, err)
	return Stores{Blob: blobStore, Metadata: metadataStore}
}

func TestBlobStoreIDIsMintedOnceAndStable(t *testing.T) {
	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{
		DataDir:     dir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	first := gates["blob_store_id"]
	require.NotEmpty(t, first)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{
		DataDir:     dir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	gates, err = reopened.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, first, gates["blob_store_id"])
	require.NoError(t, closeTestDatabase(reopened))
}

func TestBlobStoreIDMismatchIsFatal(t *testing.T) {
	// A metadata store paired with a blob store it was not initialised
	// with, which is what a swapped or emptied bucket looks like.
	metaDir := t.TempDir()
	db, err := newTestDatabaseAt(t, metaDir, metaDir, &Config{
		DataDir:     metaDir,
		StorageMode: "core",
		Network:     "preprod",
	})
	require.NoError(t, err)
	require.NoError(t, closeTestDatabase(db))

	_, err = New(&Config{
		DataDir:     metaDir,
		StorageMode: "core",
		Network:     "preprod",
	}, storesWithFreshBlob(t, metaDir))
	var settingsErr NodeSettingsError
	require.ErrorAs(t, err, &settingsErr)
	require.Contains(t, settingsErr.Error(), "blob store ID")
}

// TestBlobStoreIDSyncsAfterMint is a regression test for a durability gap:
// blobStoreID committed its mint without ever syncing the blob store, so on
// an unclean host shutdown the id could survive only in Badger's unsynced
// WAL (SyncWrites=false) while a later, already-fsynced metadata write
// latched it into the Frozen blob_store_id gate. Losing the unsynced id
// after that point is permanent -- the next start mints a new one that can
// never match the gate again. Syncing before returning closes that window,
// mirroring the identical blob-then-sync-then-metadata barrier txn.go
// applies to combined commits.
func TestBlobStoreIDSyncsAfterMint(t *testing.T) {
	store := &mockBlobStore{}
	db := &Database{blob: store}

	id, err := db.blobStoreID()
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.Equal(t, 1, store.syncCount)
	// txns[0] is the read-only lookup that finds nothing and rolls back;
	// txns[1] is the mint's write transaction. Sync must observe it already
	// committed.
	require.Len(t, store.txns, 2)
	require.Equal(
		t,
		1,
		store.txns[1].commitCount,
		"sync must run after the mint commit, not before it",
	)
}

// TestPhase1RejectsBlobStoreIDReadFailure ensures a genuine read failure --
// distinct from types.ErrBlobKeyNotFound, the ordinary first-use case that
// mints an id -- fails closed instead of silently omitting the identity gate.
func TestPhase1RejectsBlobStoreIDReadFailure(t *testing.T) {
	readErr := errors.New("simulated corrupted blob store read")
	store := &mockBlobStore{getErr: readErr}
	db := &Database{
		blob: store,
		config: &Config{
			StorageMode: "core",
			Network:     "preprod",
		},
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}

	_, err := db.phase1GateValues()
	require.Error(t, err)
	require.Contains(t, err.Error(), "get blob store id")
}

// TestBlobStoreIDSyncFailurePropagates confirms a failed sync surfaces as an
// error from blobStoreID rather than being swallowed. blobStoreID must never
// claim an id is durable when Sync could not confirm it.
func TestBlobStoreIDSyncFailurePropagates(t *testing.T) {
	syncErr := errors.New("simulated sync failure")
	store := &mockBlobStore{syncErr: syncErr}
	db := &Database{blob: store}

	_, err := db.blobStoreID()
	require.ErrorIs(t, err, syncErr)
}
