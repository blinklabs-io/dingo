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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// TestRestoreResolvesBlobStoreWithLoadRunMode verifies that Restore's
// blob-store restore step resolves the badger provider with
// RunMode: "load". Badger's own docs require db.Load to be the only
// thing operating on the store -- no concurrent reads, writes, or GC --
// and the badger provider (see database/plugin/blob/badger/provider.go)
// only skips starting its periodic background value-log GC ticker when
// RunMode == "load". A long-running restore that resolved the store any
// other way would risk that ticker firing mid-Load and violating Load's
// exclusivity requirement.
//
// This registers its own capturing "badger" provider (rather than using
// badger.RegisterProvider directly) so the exact ProviderDependencies
// Restore passes at each resolve can be observed, while still
// constructing a real, working *badger.BlobStoreBadger so the rest of
// Restore's flow (metadata restore, blob restore, post-restore
// validation) runs exactly as it would in production.
func TestRestoreResolvesBlobStoreWithLoadRunMode(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	var gotRunModes []string
	host := plugin.NewHost()
	require.NoError(t, plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityStorageBlob,
			Name:        "badger",
			Description: "badger provider wrapped to capture ProviderDependencies.RunMode",
		},
		func() struct{} { return struct{}{} },
		func(_ context.Context, _ struct{}, deps blob.ProviderDependencies) (*badger.BlobStoreBadger, plugin.Instance, error) {
			gotRunModes = append(gotRunModes, deps.RunMode)
			store, err := badger.New(
				badger.WithDataDir(deps.DataDir),
				badger.WithGc(deps.RunMode != "load"),
				badger.WithDeferOpen(),
				badger.WithValueLogFileSize(
					testutil.TestBadgerValueLogFileSize,
				),
				badger.WithMemTableSize(testutil.TestBadgerMemTableSize),
			)
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{
				StartFunc: func(context.Context) error { return store.Start() },
				StopFunc:  func(context.Context) error { return store.Stop() },
			}, nil
		},
	))
	require.NoError(t, sqlite.RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		host,
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		// The capturing badger provider registered above takes struct{}
		// as its config type and already hardcodes the bounded test
		// sizes itself, so a non-empty storageConfig.Blob here would
		// fail decodeStrict's strict decode into struct{} rather than
		// reach the store.
		lifecycle.RestoreStorageConfig{}, // restoreconfig:zero-value-required
	)
	require.NoError(t, err)

	// restoreBlobStore's resolve (the one that actually calls db.Load) must
	// be the first one, and must carry RunMode "load".
	require.NotEmpty(t, gotRunModes)
	require.Equal(
		t,
		"load",
		gotRunModes[0],
		"restoreBlobStore must resolve the blob plugin with RunMode \"load\" so its background GC ticker does not run concurrently with db.Load",
	)
}

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
				badger.WithValueLogFileSize(
					testutil.TestBadgerValueLogFileSize,
				),
				badger.WithMemTableSize(testutil.TestBadgerMemTableSize),
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
		badger.WithValueLogFileSize(testutil.TestBadgerValueLogFileSize),
		badger.WithMemTableSize(testutil.TestBadgerMemTableSize),
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
	// db.Blob() is non-nil: database.New rejects a nil or typed-nil blob
	// store (database/database.go), so the nil-receiver branch of
	// blobStoreRef.blobStore that nilaway traces is unreachable for any
	// constructed database.
	//nolint:nilaway // database.New requires a non-nil blob store
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
		// newRemoteRestoreHost's blob provider takes struct{} as its
		// config type and hardcodes the bounded test sizes itself, so a
		// non-empty storageConfig.Blob here would fail strict decoding
		// into struct{} rather than reach the store.
		lifecycle.RestoreStorageConfig{}, // restoreconfig:zero-value-required
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), wantError)
	if wantError == errInjectedRemoteBlobRestore.Error() {
		require.ErrorIs(t, err, errInjectedRemoteBlobRestore)
	}
	if wantRollbackFailure {
		require.ErrorIs(t, err, lifecycle.ErrRestoreRollbackPending)
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

func TestRestoreRecoverableRetainsHandleWhenAutomaticRollbackFails(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	remoteDir := filepath.Join(t.TempDir(), "remote")
	original := openRemoteTestDatabase(t, remoteDir)
	require.NoError(t, original.db.BlockCreate(testBlock(1, 0x11), nil))
	original.close(t)

	incoming := newTestDB(t)
	require.NoError(t, incoming.BlockCreate(testBlock(1, 0x99), nil))
	snapshotDir := filepath.Join(t.TempDir(), "incoming")
	_, err := lifecycle.Snapshot(
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
	control.failRestores.Store(2)
	host := newRemoteRestoreHost(t, remoteDir, control)
	_, recovery, err := lifecycle.RestoreRecoverable(
		metadata.AllowResetOfPopulatedTarget(ctx),
		host,
		nil,
		snapshotDir,
		filepath.Join(t.TempDir(), "local-staging-target"),
		nil,
		// newRemoteRestoreHost's blob provider takes struct{} as its
		// config type and hardcodes the bounded test sizes itself, so a
		// non-empty storageConfig.Blob here would fail strict decoding
		// into struct{} rather than reach the store.
		lifecycle.RestoreStorageConfig{}, // restoreconfig:zero-value-required
	)
	require.ErrorIs(t, err, lifecycle.ErrRestoreRollbackPending)
	require.NotNil(t, recovery)
	require.DirExists(t, recovery.BackupDir())
	// The injected provider consumed both failures. The retained handle can
	// retry compensation while its host remains active.
	require.NoError(t, recovery.Rollback(context.Background()))
	_, statErr := os.Stat(recovery.BackupDir())
	require.True(t, os.IsNotExist(statErr))
}

func TestRestoreFailureRollsBackPopulatedRemoteStoresExactly(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
		// newRemoteRestoreHost's blob provider takes struct{} as its
		// config type and hardcodes the bounded test sizes itself, so a
		// non-empty storageConfig.Blob here would fail strict decoding
		// into struct{} rather than reach the store.
		lifecycle.RestoreStorageConfig{}, // restoreconfig:zero-value-required
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

// restoreStorageConfigAllowBareMarker exempts a call site from
// TestRestoreCallSitesUseBoundedBadgerConfig. A same-line trailing comment
// bearing this marker documents why passing the zero-value
// RestoreStorageConfig{} there is deliberate -- e.g. the target host's blob
// provider takes a struct{} config and would fail strict decoding of any
// non-empty map -- rather than the unbounded-default oversight this test
// otherwise guards against.
const restoreStorageConfigAllowBareMarker = "restoreconfig:zero-value-required"

// restoreFuncNames are lifecycle's exported entry points whose final
// parameter is a RestoreStorageConfig (see restore.go). Passing one without
// Blob to any of them resolves the blob plugin with a nil provider config,
// which for badger means the production 1 GiB
// value log / 128 MiB memtable defaults -- badger maps the value log at
// twice that, so 2 GiB is really reserved the moment the store opens. On
// Windows that reservation is not sparse, and it is real for as long as
// Restore holds the store open; enough concurrent test restores exhaust a CI
// runner's disk. testutil.BadgerBlobConfig and dbtest.NewDatabase already
// guard every other on-disk test store in this repository against exactly
// this; this test extends that guard to lifecycle.Restore's own callers.
var restoreFuncNames = map[string]bool{
	"Restore":            true,
	"RestoreValidated":   true,
	"RestoreRecoverable": true,
}

// TestRestoreCallSitesUseBoundedBadgerConfig statically scans every test file
// in this directory for a call to Restore, RestoreValidated, or
// RestoreRecoverable whose RestoreStorageConfig literal sets no Blob --
// including the bare RestoreStorageConfig{} and a Metadata-only literal --
// and fails naming each one found, unless the line carries
// restoreStorageConfigAllowBareMarker.
//
// A runtime assertion cannot make this same distinction: badger truncates its
// value log and memtable files back down when a store is cleanly stopped,
// and every Restore call in this package stops its stores well before
// Restore returns, so measuring reserved file sizes after the fact passes
// whether or not a call site supplies a bounded config (see the
// investigation on dingo#3746). Only a source-level check catches the
// regression of a new or edited call site reintroducing the unbounded
// default.
func TestRestoreCallSitesUseBoundedBadgerConfig(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}

	fset := token.NewFileSet()
	var violations []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		lines := strings.Split(string(src), "\n")

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isRestoreCall(call.Fun) {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.CompositeLit)
				if !ok || !isRestoreStorageConfigType(lit.Type) ||
					setsBlob(lit) {
					continue
				}
				pos := fset.Position(lit.Pos())
				var lineText string
				if pos.Line-1 < len(lines) {
					lineText = lines[pos.Line-1]
				}
				if strings.Contains(
					lineText, restoreStorageConfigAllowBareMarker,
				) {
					continue
				}
				violations = append(violations, fmt.Sprintf(
					"%s:%d: RestoreStorageConfig without Blob resolves "+
						"the restore's blob store with badger's unbounded "+
						"default sizes; set Blob: "+
						"testutil.BadgerBlobConfig(), or mark the line "+
						"with %q if the target host's provider ignores "+
						"its config",
					name, pos.Line, restoreStorageConfigAllowBareMarker,
				))
			}
			return true
		})
	}

	if len(violations) > 0 {
		t.Fatalf(
			"%d call site(s) resolve a Restore blob/metadata plugin with "+
				"an unbounded default config:\n%s",
			len(violations),
			strings.Join(violations, "\n"),
		)
	}
}

func isRestoreCall(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return restoreFuncNames[f.Name]
	case *ast.SelectorExpr:
		return restoreFuncNames[f.Sel.Name]
	}
	return false
}

func isRestoreStorageConfigType(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name == "RestoreStorageConfig"
	case *ast.SelectorExpr:
		return t.Sel.Name == "RestoreStorageConfig"
	}
	return false
}

// setsBlob reports whether lit, a RestoreStorageConfig literal, supplies a
// Blob provider config. An unkeyed literal with elements sets Blob, its
// first field.
func setsBlob(lit *ast.CompositeLit) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Blob" {
			return true
		}
	}
	return false
}

// TestSnapshotRestoreRoundTrip verifies that a snapshotted database
// restores into a fresh directory with the same blocks and tip.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, src.BlockCreate(testBlock(2, 0x02), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	snapMan, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	restoreMan, err := lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.NoError(t, err)
	require.Equal(t, snapMan.CommitTimestamp, restoreMan.CommitTimestamp)
	require.Equal(t, snapMan.TipSlot, restoreMan.TipSlot)

	// Reopen the restored data dir like a normal node startup would and
	// confirm the blocks survived the round trip.
	restored, err := dbtest.NewDatabase(t, &database.Config{DataDir: targetDir})
	require.NoError(t, err)

	block1, err := restored.BlockByIndex(1, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), block1.ID)
	block2, err := restored.BlockByIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block2.ID)
}

func TestRestoreRebuildsDeferredIndexes(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	manager, ok := src.Metadata().(metadata.DeferredIndexManager)
	require.True(t, ok)
	require.NoError(t, manager.DropDeferredIndexes())

	snapshotDir := filepath.Join(t.TempDir(), "snapshot")
	_, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.NoError(t, err)

	restored, err := dbtest.NewDatabase(t, &database.Config{DataDir: targetDir})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, restored)
	require.NoError(t, err)
	for _, index := range []string{
		"idx_utxo_transaction_id",
		dbtest.LazyManifestIndex(t),
	} {
		require.True(t, dbtest.MetadataIndexExists(t, raw, index), index)
	}
}

// TestRestoreRefusesNonEmptyTargetDirectory verifies that Restore errors
// when the target directory already contains a file.
func TestRestoreRefusesNonEmptyTargetDirectory(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	// Target dir already has a file in it.
	targetDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(targetDir, "existing.txt"), []byte("data"), 0o644,
	))

	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.Error(t, err)
}

// TestRestoreRejectsConfiguredDataDirOverrideWithoutTouchingTarget guards a
// real gap: restore always resolved its blob/metadata plugins with a nil
// provider config, silently ignoring a caller's configured per-plugin
// "dataDir" override (plugin.Selection.Config) -- meaning a restore could
// write into targetDataDir's own staging directory while a real subsequent
// startup, which does honor that override, opens a completely different
// directory and sees the old or empty database there instead. Now that
// storageConfig is propagated, such an override must be refused outright
// (not silently honored either, since doing so would write outside the
// staging directory this package's atomic-rename interruption safety
// depends on) -- and, like every other RestoreValidated rejection, before
// targetDataDir is touched at all.
func TestRestoreRejectsConfiguredDataDirOverrideWithoutTouchingTarget(
	t *testing.T,
) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{
			Blob: map[string]any{"dataDir": "/some/other/configured/path"},
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dataDir override")

	_, statErr := os.Stat(targetDir)
	require.True(
		t, os.IsNotExist(statErr),
		"targetDataDir must be untouched when a dataDir override is rejected",
	)
}

func TestRestoreRejectsNonStringDataDirOverride(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	snapshotDir := filepath.Join(t.TempDir(), "snapshot")
	_, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{
			Blob:     testutil.BadgerBlobConfig(),
			Metadata: map[string]any{"dataDir": 42},
		},
	)
	require.ErrorContains(t, err, "non-string metadata provider dataDir")
	require.NoDirExists(t, targetDir)
}

// TestManifestCheckPluginMatch verifies Manifest.CheckPluginMatch itself:
// it accepts the plugins a snapshot was actually taken with, and rejects
// any other combination. This is a unit test of the check in isolation —
// see TestRestoreValidatedRejectsPluginMismatchWithoutTouchingTarget for
// the real call site (internal/dblifecycle.Service.Restore's validate
// hook, via lifecycle.RestoreValidated) that actually enforces it during a
// restore.
func TestManifestCheckPluginMatch(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	m, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)
	require.NoError(t, m.CheckPluginMatch("badger", "sqlite"))
	require.Error(t, m.CheckPluginMatch("gcs", "sqlite"))
}

// TestRestoreValidatedRejectsPluginMismatchWithoutTouchingTarget exercises
// the actual restore call site a plugin mismatch is meant to protect:
// internal/dblifecycle.Service.Restore passes a validate func (calling
// CheckPluginMatch/CheckCompatibility) into lifecycle.RestoreValidated,
// which — per RestoreValidated's own doc comment — must run that check
// before targetDataDir is touched in any way, "not even the empty/absent
// check". Unlike the old, misleadingly-named version of this test (which
// called Manifest.CheckPluginMatch directly and never invoked Restore or
// RestoreValidated at all), this proves the mismatch actually aborts a
// restore attempt, and that it does so before creating targetDir.
func TestRestoreValidatedRejectsPluginMismatchWithoutTouchingTarget(
	t *testing.T,
) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.RestoreValidated(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		func(m lifecycle.Manifest) error {
			return m.CheckPluginMatch("gcs", "sqlite")
		},
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.Error(t, err)

	_, statErr := os.Stat(targetDir)
	require.Truef(
		t, os.IsNotExist(statErr),
		"a rejected validate hook must run before targetDir is created, "+
			"got stat error: %v", statErr,
	)
}

// TestRestoreRejectsMismatchedTipBlockNumber verifies that
// validateRestoredDatabase's post-restore tip check compares
// TipBlockNumber, not just slot/hash, against the restored database's
// actual tip. The manifest's checksum is recomputed over the tampered
// content (via WriteManifest), so this is not caught as a corrupted file —
// only comparing block number as well as slot/hash catches a restored
// database whose recorded chain height disagrees with its own tip point.
func TestRestoreRejectsMismatchedTipBlockNumber(t *testing.T) {
	t.Parallel()

	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, src.BlockCreate(testBlock(2, 0x02), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	m, err := lifecycle.Snapshot(
		context.Background(),
		src,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	m.TipBlockNumber++
	require.NoError(t, lifecycle.WriteManifest(snapshotDir, m))

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		testDestinationRegistry,
		snapshotDir,
		targetDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match manifest tip")
}

// manifestOnlyCloudDestination implements CloudManifestFetcher but fails
// UploadDir/DownloadDir outright -- used to prove a caller went through
// the lightweight FetchManifest path and never attempted a full
// directory download at all, rather than merely happening to succeed
// either way.
type manifestOnlyCloudDestination struct {
	manifest lifecycle.Manifest
}

func (d *manifestOnlyCloudDestination) UploadDir(
	context.Context,
	string,
) error {
	return errors.New(
		"manifestOnlyCloudDestination: UploadDir must never be called",
	)
}

func (d *manifestOnlyCloudDestination) DownloadDir(
	context.Context,
	string,
) error {
	return errors.New(
		"manifestOnlyCloudDestination: DownloadDir must never be called",
	)
}

func (d *manifestOnlyCloudDestination) FetchManifest(
	context.Context,
) (lifecycle.Manifest, error) {
	return d.manifest, nil
}

var _ lifecycle.CloudManifestFetcher = &manifestOnlyCloudDestination{}

// Registered directly on the package's shared testDestinationRegistry
// (defined in destination_test.go) — package-level var initializers all
// complete before any init() runs, regardless of which file they're in, so
// referencing it here is safe.
func init() {
	testDestinationRegistry.Register(
		"faketest-manifestonly",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return &manifestOnlyCloudDestination{
				manifest: manifestOnlyFixture,
			}, nil
		},
	)
}

var manifestOnlyFixture = lifecycle.Manifest{
	BlobPlugin:     "badger",
	MetadataPlugin: "sqlite",
}

// TestPeekManifestUsesLightweightCloudFetchWithoutDownloading guards
// against a real gap: PeekManifest used to always go
// through the full download-based resolveManifest path, even for a
// cloud snapshotDir whose destination type supports fetching just the
// one manifest.json object via CloudManifestFetcher -- downloading the
// (possibly very large) blob/metadata backups alongside it just to read
// its manifest. This uses a destination whose UploadDir/DownloadDir both
// fail outright, so this test only passes if PeekManifest actually took
// the lightweight FetchCloudManifest path and never called DownloadDir
// at all.
func TestPeekManifestUsesLightweightCloudFetchWithoutDownloading(t *testing.T) {
	t.Parallel()

	m, err := lifecycle.PeekManifest(
		context.Background(),
		testDestinationRegistry,
		"faketest-manifestonly://bucket/prefix",
	)
	require.NoError(t, err)
	require.Equal(t, manifestOnlyFixture, m)
}

// noManifestFetcherCloudDestination forwards to a real fakeCloudDestination
// for UploadDir/DownloadDir but deliberately does not embed it or expose a
// FetchManifest method of its own -- unlike this package's "faketest"
// scheme, whose fakeCloudDestination DOES implement CloudManifestFetcher.
// A test resolving a destination through THIS wrapper's scheme instead
// therefore cannot silently take PeekManifest's lightweight
// FetchCloudManifest path (see TestPeekManifestUsesLightweightCloudFetch
// WithoutDownloading): the type assertion for CloudManifestFetcher
// genuinely fails, forcing the full-download resolveManifest fallback the
// test below claims to cover.
type noManifestFetcherCloudDestination struct {
	inner *fakeCloudDestination
}

func (d *noManifestFetcherCloudDestination) UploadDir(
	ctx context.Context,
	localDir string,
) error {
	return d.inner.UploadDir(ctx, localDir)
}

func (d *noManifestFetcherCloudDestination) DownloadDir(
	ctx context.Context,
	localDir string,
) error {
	return d.inner.DownloadDir(ctx, localDir)
}

var _ lifecycle.CloudDestination = &noManifestFetcherCloudDestination{}

// Registered directly on the package's shared testDestinationRegistry, the
// same way manifestOnlyCloudDestination's scheme is above -- resolved
// under the same fakeCloudDir backing directory as "faketest" itself, so
// setFakeCloudBackingDir still applies.
func init() {
	testDestinationRegistry.Register(
		"faketest-nomanifestfetcher",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			fakeCloudMu.Lock()
			base := fakeCloudDir
			fakeCloudMu.Unlock()
			if base == "" {
				// A resolution with no backing directory set would
				// filepath.Join against "" and write the URI path
				// relative to the package directory. Fail instead: the
				// only way to get here is a resolution that outlived the
				// test that set the fixture, and that should error,
				// not leave files in the checkout.
				return nil, errors.New(
					"faketest-nomanifestfetcher: no backing directory set for this test",
				)
			}
			return &noManifestFetcherCloudDestination{
				inner: &fakeCloudDestination{
					dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
				},
			}, nil
		},
	)
}

// TestPeekManifestFallsBackToDownloadWhenCloudDestinationLacksManifestFetcher
// verifies PeekManifest still works correctly against a cloud destination
// type that does NOT implement CloudManifestFetcher, using
// "faketest-nomanifestfetcher" (a wrapper that deliberately omits
// FetchManifest — see its doc comment) rather than this package's plain
// "faketest" scheme, whose fakeCloudDestination DOES implement
// CloudManifestFetcher and would therefore take the lightweight
// FetchCloudManifest path instead of genuinely exercising this fallback.
func TestPeekManifestFallsBackToDownloadWhenCloudDestinationLacksManifestFetcher(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)

	snapshotDir := filepath.Join(t.TempDir(), "snap-peek")
	m, err := lifecycle.SnapshotToCloud(
		context.Background(), testDestinationRegistry, db, snapshotDir,
		lifecycle.TriggerManual, "test-version", "badger", "sqlite",
		"faketest-nomanifestfetcher://bucket/prefix",
		"", "",
	)
	require.NoError(t, err)

	peeked, err := lifecycle.PeekManifest(
		context.Background(), testDestinationRegistry,
		"faketest-nomanifestfetcher://bucket/prefix/snap-peek",
	)
	require.NoError(t, err)
	require.Equal(t, m.CommitTimestamp, peeked.CommitTimestamp)
}
