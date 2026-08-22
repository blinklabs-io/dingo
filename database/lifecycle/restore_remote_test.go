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

package lifecycle_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

const (
	remoteBlobProviderName     = "remote-test-blob"
	remoteMetadataProviderName = "remote-test-metadata"
)

// remoteTestMetadataStore makes SQLite behave like a client/server provider:
// every resolution ignores ProviderDependencies.DataDir and opens the same
// external directory. Reset is applied by Stop, after SQLite has closed its
// file, matching lifecycle's Reset-then-Stop-then-RestoreFrom ordering without
// deleting an open SQLite database.
type remoteTestMetadataStore struct {
	*sqlstore.Store
	dataDir      string
	resetPending atomic.Bool
}

func (s *remoteTestMetadataStore) HasDestructiveReset() bool { return true }

func (s *remoteTestMetadataStore) Reset(context.Context) error {
	s.resetPending.Store(true)
	return nil
}

func (s *remoteTestMetadataStore) stop(ctx context.Context) error {
	closeErr := s.CloseContext(ctx)
	if !s.resetPending.Swap(false) {
		return closeErr
	}
	removeErr := os.RemoveAll(s.dataDir)
	if removeErr == nil {
		removeErr = os.MkdirAll(s.dataDir, 0o700)
	}
	return errors.Join(closeErr, removeErr)
}

// remoteTestBlobStore gives Badger the same external-target behavior and
// injects one restore failure after Reset. The following Restore is the
// compensating rollback and succeeds.
type remoteTestBlobStore struct {
	*badger.BlobStoreBadger
	control *remoteBlobRestoreControl
}

type remoteBlobRestoreControl struct {
	failRestores    atomic.Int32
	cancelOnRestore atomic.Bool
	cancel          context.CancelFunc
}

var errInjectedRemoteBlobRestore = errors.New(
	"injected remote blob restore failure",
)

func (s *remoteTestBlobStore) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.DB().DropAll()
}

func (s *remoteTestBlobStore) Restore(ctx context.Context, r io.Reader) error {
	if s.control.cancelOnRestore.CompareAndSwap(true, false) {
		s.control.cancel()
		return ctx.Err()
	}
	for {
		remaining := s.control.failRestores.Load()
		if remaining == 0 {
			break
		}
		if s.control.failRestores.CompareAndSwap(remaining, remaining-1) {
			return errInjectedRemoteBlobRestore
		}
	}
	return s.BlobStoreBadger.Restore(ctx, r)
}

func newRemoteRestoreHost(
	t *testing.T,
	dataRoot string,
	control *remoteBlobRestoreControl,
) *plugin.Host {
	t.Helper()
	metadataDir := filepath.Join(dataRoot, "metadata")
	blobDir := filepath.Join(dataRoot, "blob")
	host := plugin.NewHost()
	require.NoError(t, plugin.Register[metadata.MetadataStore](
		host,
		plugin.Descriptor{
			Capability: plugin.CapabilityStorageMetadata,
			Name:       remoteMetadataProviderName,
		},
		func() struct{} { return struct{}{} },
		func(
			_ context.Context,
			_ struct{},
			deps metadata.ProviderDependencies,
		) (metadata.MetadataStore, plugin.Instance, error) {
			store, err := sqlite.NewSQLStore(
				sqlite.Config{DataDir: metadataDir}, deps,
			)
			if err != nil {
				return nil, nil, err
			}
			remote := &remoteTestMetadataStore{
				Store: store, dataDir: metadataDir,
			}
			return remote, plugin.Lifecycle{
				StartFunc: remote.Start,
				StopFunc:  remote.stop,
			}, nil
		},
	))
	require.NoError(t, plugin.Register[blob.BlobStore](
		host,
		plugin.Descriptor{
			Capability: plugin.CapabilityStorageBlob,
			Name:       remoteBlobProviderName,
		},
		func() struct{} { return struct{}{} },
		func(
			_ context.Context,
			_ struct{},
			_ blob.ProviderDependencies,
		) (blob.BlobStore, plugin.Instance, error) {
			store, err := badger.New(
				badger.WithDataDir(blobDir),
				badger.WithDeferOpen(),
				badger.WithGc(false),
			)
			if err != nil {
				return nil, nil, err
			}
			remote := &remoteTestBlobStore{
				BlobStoreBadger: store,
				control:         control,
			}
			return remote, plugin.Lifecycle{
				StartFunc: func(context.Context) error { return remote.Start() },
				StopFunc:  func(context.Context) error { return remote.Stop() },
			}, nil
		},
	))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host
}

type remoteTestDatabase struct {
	db       *database.Database
	blob     *badger.BlobStoreBadger
	metadata *sqlstore.Store
}

func openRemoteTestDatabase(
	t *testing.T,
	dataRoot string,
) *remoteTestDatabase {
	t.Helper()
	blobStore, err := badger.New(
		badger.WithDataDir(filepath.Join(dataRoot, "blob")),
		badger.WithGc(false),
	)
	require.NoError(t, err)
	metadataStore, err := sqlite.NewSQLStore(
		sqlite.Config{DataDir: filepath.Join(dataRoot, "metadata")},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, metadataStore.Start(context.Background()))
	db, err := database.New(
		&database.Config{Network: "preview"},
		database.Stores{Blob: blobStore, Metadata: metadataStore},
	)
	require.NoError(t, err)
	return &remoteTestDatabase{
		db: db, blob: blobStore, metadata: metadataStore,
	}
}

func (d *remoteTestDatabase) close(t *testing.T) {
	t.Helper()
	require.NoError(t, d.db.Close())
	require.NoError(t, d.metadata.Close())
	require.NoError(t, d.blob.Close())
}

func readBlobContents(t *testing.T, db *database.Database) map[string][]byte {
	t.Helper()
	txn := db.Blob().NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck
	it := db.Blob().NewIterator(txn, types.BlobIteratorOptions{})
	require.NotNil(t, it)
	defer it.Close()
	require.NoError(t, it.Err())
	ret := map[string][]byte{}
	for it.Valid() {
		item := it.Item()
		if item != nil {
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			ret[string(item.Key())] = value
		}
		it.Next()
	}
	require.NoError(t, it.Err())
	return ret
}

func runRemoteRestoreFailureRollback(
	t *testing.T,
	configure func(*remoteBlobRestoreControl) context.Context,
	wantError string,
	wantRollbackFailure bool,
) {
	t.Helper()
	ctx := context.Background()
	remoteDir := filepath.Join(t.TempDir(), "remote")
	original := openRemoteTestDatabase(t, remoteDir)
	require.NoError(t, original.db.BlockCreate(testBlock(1, 0x11), nil))
	require.NoError(t, original.db.BlockCreate(testBlock(2, 0x22), nil))
	originalTip, err := original.db.GetTip(nil)
	require.NoError(t, err)
	originalCommitTimestamp, err := original.db.Metadata().GetCommitTimestamp()
	require.NoError(t, err)
	originalGates, err := original.db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	originalBlobs := readBlobContents(t, original.db)
	original.close(t)

	incoming := newTestDB(t)
	require.NoError(t, incoming.BlockCreate(testBlock(1, 0x99), nil))
	snapshotDir := filepath.Join(t.TempDir(), "incoming")
	_, err = lifecycle.Snapshot(
		ctx,
		incoming,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)
	manifest, err := lifecycle.ReadManifest(snapshotDir)
	require.NoError(t, err)
	manifest.BlobPlugin = remoteBlobProviderName
	manifest.MetadataPlugin = remoteMetadataProviderName
	require.NoError(t, lifecycle.WriteManifest(snapshotDir, manifest))

	control := &remoteBlobRestoreControl{}
	restoreCtx := configure(control)
	host := newRemoteRestoreHost(t, remoteDir, control)
	_, err = lifecycle.RestoreValidated(
		metadata.AllowResetOfPopulatedTarget(restoreCtx),
		host,
		nil,
		snapshotDir,
		filepath.Join(t.TempDir(), "local-staging-target"),
		nil,
		lifecycle.RestoreStorageConfig{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), wantError)
	if wantError == errInjectedRemoteBlobRestore.Error() {
		require.ErrorIs(t, err, errInjectedRemoteBlobRestore)
	}
	if wantRollbackFailure {
		require.Contains(t, err.Error(), "automatic restore rollback failed")
		require.Contains(t, err.Error(), "original backups preserved at")
		return
	}
	require.NotContains(t, err.Error(), "automatic restore rollback failed")

	restored := openRemoteTestDatabase(t, remoteDir)
	restoredTip, err := restored.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, originalTip, restoredTip)
	restoredCommitTimestamp, err := restored.db.Metadata().GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, originalCommitTimestamp, restoredCommitTimestamp)
	restoredGates, err := restored.db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, originalGates, restoredGates)
	require.Equal(t, originalBlobs, readBlobContents(t, restored.db))
	for _, id := range []uint64{1, 2} {
		_, err := restored.db.BlockByIndex(id, nil)
		require.NoError(t, err)
	}
	restored.close(t)
}

func TestRestoreFailureRollsBackPopulatedRemoteStoresExactly(t *testing.T) {
	t.Run("provider failure", func(t *testing.T) {
		runRemoteRestoreFailureRollback(
			t,
			func(control *remoteBlobRestoreControl) context.Context {
				control.failRestores.Store(1)
				return context.Background()
			},
			"injected remote blob restore failure",
			false,
		)
	})
	t.Run("cancellation after reset", func(t *testing.T) {
		runRemoteRestoreFailureRollback(
			t,
			func(control *remoteBlobRestoreControl) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				control.cancel = cancel
				control.cancelOnRestore.Store(true)
				return ctx
			},
			context.Canceled.Error(),
			false,
		)
	})
	t.Run("rollback failure joins original error", func(t *testing.T) {
		runRemoteRestoreFailureRollback(
			t,
			func(control *remoteBlobRestoreControl) context.Context {
				control.failRestores.Store(2)
				return context.Background()
			},
			"injected remote blob restore failure",
			true,
		)
	})
}

func TestRestoreSuccessfulRemoteReplacementRemainsRecoverableUntilCommit(
	t *testing.T,
) {
	ctx := context.Background()
	remoteDir := filepath.Join(t.TempDir(), "remote")
	original := openRemoteTestDatabase(t, remoteDir)
	require.NoError(t, original.db.BlockCreate(testBlock(1, 0x11), nil))
	require.NoError(t, original.db.BlockCreate(testBlock(2, 0x22), nil))
	originalTip, err := original.db.GetTip(nil)
	require.NoError(t, err)
	originalCommitTimestamp, err := original.db.Metadata().GetCommitTimestamp()
	require.NoError(t, err)
	originalGates, err := original.db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	originalBlobs := readBlobContents(t, original.db)
	original.close(t)

	incoming := newTestDB(t)
	require.NoError(t, incoming.BlockCreate(testBlock(1, 0x99), nil))
	incomingBlobs := readBlobContents(t, incoming)
	incomingTip, err := incoming.GetTip(nil)
	require.NoError(t, err)
	snapshotDir := filepath.Join(t.TempDir(), "incoming")
	_, err = lifecycle.Snapshot(
		ctx,
		incoming,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)
	manifest, err := lifecycle.ReadManifest(snapshotDir)
	require.NoError(t, err)
	manifest.BlobPlugin = remoteBlobProviderName
	manifest.MetadataPlugin = remoteMetadataProviderName
	require.NoError(t, lifecycle.WriteManifest(snapshotDir, manifest))

	control := &remoteBlobRestoreControl{}
	host := newRemoteRestoreHost(t, remoteDir, control)
	_, recovery, err := lifecycle.RestoreRecoverable(
		metadata.AllowResetOfPopulatedTarget(ctx),
		host,
		nil,
		snapshotDir,
		filepath.Join(t.TempDir(), "local-staging-target"),
		nil,
		lifecycle.RestoreStorageConfig{},
	)
	require.NoError(t, err)

	restored := openRemoteTestDatabase(t, remoteDir)
	restoredTip, err := restored.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, incomingTip, restoredTip)
	require.Equal(t, incomingBlobs, readBlobContents(t, restored.db))
	block, err := restored.db.BlockByIndex(1, nil)
	require.NoError(t, err)
	require.Equal(t, testBlock(1, 0x99).Hash, block.Hash)
	_, err = restored.db.BlockByIndex(2, nil)
	require.Error(t, err)
	restored.close(t)

	// Node.Restore keeps this recovery handle until its local directory swap
	// and reinitialization also succeed. A later failure at either boundary
	// must still be able to restore the exact original external pair.
	require.NoError(t, recovery.Rollback(context.Background()))
	rolledBack := openRemoteTestDatabase(t, remoteDir)
	rolledBackTip, err := rolledBack.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, originalTip, rolledBackTip)
	rolledBackCommitTimestamp, err := rolledBack.db.Metadata().
		GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, originalCommitTimestamp, rolledBackCommitTimestamp)
	rolledBackGates, err := rolledBack.db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, originalGates, rolledBackGates)
	require.Equal(t, originalBlobs, readBlobContents(t, rolledBack.db))
	for _, id := range []uint64{1, 2} {
		_, err := rolledBack.db.BlockByIndex(id, nil)
		require.NoError(t, err)
	}
	rolledBack.close(t)
}
