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

package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

const cancelOnRestoreBlobProviderName = "cancel-on-restore-blob"

// cancelOnRestoreBlobStore embeds an unopened Badger blob store purely to
// satisfy the full blob.BlobStore method set plugin.Resolve's type
// assertion requires -- restoreBlobStore itself never calls any of those
// promoted methods, only Restore (overridden below) and the Restorer/
// Resettable type assertions.
//
// Its own stop method reproduces BlobStoreBadger.CloseContext's exact
// documented contract (see database/plugin/blob/badger/database.go):
// returning is not the completion signal, because a caller-supplied ctx
// can race the real close and return first. Reproducing that contract
// here, rather than depending on Badger's own on-disk directory lock
// timing, isolates what this test proves -- restoreBlobStore's own
// context handling -- from Badger's OS-level lock behavior, which
// database/plugin/blob/badger's own tests (provider_test.go's
// TestProviderStopDeadlineDuringValueLogGC) already cover.
type cancelOnRestoreBlobStore struct {
	*badger.BlobStoreBadger
	cancelRestore context.CancelFunc
	stopEntered   chan struct{}
	release       chan struct{}
	stopFinished  atomic.Bool
}

func (s *cancelOnRestoreBlobStore) Restore(
	ctx context.Context,
	_ io.Reader,
) error {
	s.cancelRestore()
	return ctx.Err()
}

// stop races ctx against a completion signal gated on s.release, exactly
// as CloseContext races ctx against its own closeDone channel.
func (s *cancelOnRestoreBlobStore) stop(ctx context.Context) error {
	close(s.stopEntered)
	done := make(chan struct{})
	go func() {
		<-s.release
		s.stopFinished.Store(true)
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newCancelOnRestoreTestHost(
	t *testing.T,
	store *cancelOnRestoreBlobStore,
) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, plugin.Register[blob.BlobStore](
		host,
		plugin.Descriptor{
			Capability: plugin.CapabilityStorageBlob,
			Name:       cancelOnRestoreBlobProviderName,
		},
		func() struct{} { return struct{}{} },
		func(
			_ context.Context,
			_ struct{},
			_ blob.ProviderDependencies,
		) (blob.BlobStore, plugin.Instance, error) {
			return store, plugin.Lifecycle{
				StartFunc: func(context.Context) error { return nil },
				StopFunc:  store.stop,
			}, nil
		},
	))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host
}

// TestRestoreBlobStoreStopWaitsForProviderAfterCanceledRestore is a
// regression test for the restore-rollback lock race in dingo#4179's
// class: restoreBlobStore's own StopCapability call must not surface as
// "the provider is stopped" before the provider's Stop has genuinely
// finished, even when Restore failed because the operation's own context
// was just canceled -- the ordinary case, since Restore and the cleanup
// Stop that follows it share the same ctx.
//
// If StopCapability races that canceled ctx instead of waiting for the
// real completion, an automatic rollback (restoreRollback.restore) or a
// caller retrying the same restore against the same host can reopen
// targetDataDir while the prior store's close is still in flight and
// lose the race for its directory lock -- restore_remote_test.go's
// TestRestoreFailureRollsBackPopulatedRemoteStoresExactly/
// cancellation_after_reset is the user-visible "automatic restore
// rollback failed ... Cannot acquire directory lock" symptom this
// produces for a provider whose Stop propagates ctx like the real
// on-disk "badger" plugin does (badger.RegisterProvider wires
// StopFunc: store.CloseContext directly, unlike that test's own
// remoteTestBlobStore, whose Stop always uses context.Background and so
// cannot observe this race).
func TestRestoreBlobStoreStopWaitsForProviderAfterCanceledRestore(
	t *testing.T,
) {
	t.Parallel()

	backupPath := filepath.Join(t.TempDir(), "blob.backup")
	require.NoError(t, os.WriteFile(backupPath, nil, 0o600))
	targetDir := t.TempDir()

	embedded, err := badger.New(badger.WithDeferOpen())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnRestoreBlobStore{
		BlobStoreBadger: embedded,
		cancelRestore:   cancel,
		stopEntered:     make(chan struct{}),
		release:         make(chan struct{}),
	}
	host := newCancelOnRestoreTestHost(t, store)
	manifest := Manifest{BlobPlugin: cancelOnRestoreBlobProviderName}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- restoreBlobStore(
			ctx, host, manifest, backupPath, targetDir, nil, false,
		)
	}()

	testutil.RequireReceive(
		t, store.stopEntered, 5*time.Second,
		"restoreBlobStore never reached the post-Restore StopCapability call",
	)

	// restoreBlobStore must not return yet: its StopCapability call is
	// required to wait for the provider's own Stop to actually finish,
	// not race the operation's already-canceled context. 200ms only
	// needs to distinguish "returned without waiting at all" (the bug,
	// which resolves near-instantly since ctx.Done() is already ready)
	// from "genuinely still blocked" -- it is not a narrow race window.
	testutil.RequireNoReceive(
		t, resultCh, 200*time.Millisecond,
		"restoreBlobStore returned before the blob provider finished "+
			"stopping -- a subsequent reopen of the same directory can "+
			"race the still-in-flight close and hit "+
			"\"Cannot acquire directory lock\"",
	)
	require.False(t, store.stopFinished.Load())

	close(store.release)
	err = testutil.RequireReceive(
		t, resultCh, 5*time.Second,
		"restoreBlobStore did not return after the provider finished stopping",
	)
	require.True(
		t,
		store.stopFinished.Load(),
		"blob provider must be fully stopped by the time restoreBlobStore returns",
	)
	require.ErrorContains(t, err, "context canceled")
}

// plainIndexManager implements only metadata.DeferredIndexManager: the
// fallback a store without context support takes.
type plainIndexManager struct {
	built int
}

func (m *plainIndexManager) DropDeferredIndexes() error          { return nil }
func (m *plainIndexManager) BuildCriticalDeferredIndexes() error { return nil }
func (m *plainIndexManager) BuildDeferredIndexes() error {
	m.built++
	return nil
}

func (m *plainIndexManager) HasDeferredIndexesPending() (bool, error) {
	return false, nil
}

// ctxIndexManager also implements metadata.ContextDeferredIndexBuilder and
// metadata.MissingDeferredIndexLister.
type ctxIndexManager struct {
	plainIndexManager
	missing    []string
	ctxBuilds  int
	sawErr     error
	sawListErr error
	loggedAt   string
	logBuffer  *bytes.Buffer
}

func (m *ctxIndexManager) MissingDeferredIndexes() ([]string, error) {
	return m.missing, nil
}

func (m *ctxIndexManager) MissingDeferredIndexesContext(
	ctx context.Context,
) ([]string, error) {
	m.sawListErr = ctx.Err()
	return m.missing, m.sawListErr
}

func (m *ctxIndexManager) BuildDeferredIndexesContext(
	ctx context.Context,
) error {
	m.ctxBuilds++
	if m.logBuffer != nil {
		m.loggedAt = m.logBuffer.String()
	}
	m.sawErr = ctx.Err()
	return ctx.Err()
}

type progressIndexManager struct {
	*ctxIndexManager
	progressBuilds int
}

func (m *progressIndexManager) BuildDeferredIndexesContextWithProgress(
	ctx context.Context,
	before func(string),
	after func(string, time.Duration),
) error {
	m.progressBuilds++
	before("idx_asset_fingerprint")
	after("idx_asset_fingerprint", time.Millisecond)
	return ctx.Err()
}

// TestRebuildRestoredDeferredIndexesHonoursCancellation pins the property the
// restore path's interruption-safety contract depends on: a cancelled restore
// must not be stuck behind a full manifest rebuild.
func TestRebuildRestoredDeferredIndexesHonoursCancellation(t *testing.T) {
	t.Parallel()
	manager := &ctxIndexManager{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rebuildRestoredDeferredIndexes(ctx, manager, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, manager.sawListErr, context.Canceled)
	require.Equal(t, 1, manager.ctxBuilds)
	require.Zero(
		t, manager.built,
		"the context-free build must not run when the store supports ctx",
	)
}

// TestRebuildRestoredDeferredIndexesFallsBackWithoutContextSupport keeps a
// store that implements only DeferredIndexManager working.
func TestRebuildRestoredDeferredIndexesFallsBackWithoutContextSupport(
	t *testing.T,
) {
	t.Parallel()
	manager := &plainIndexManager{}

	require.NoError(
		t,
		rebuildRestoredDeferredIndexes(context.Background(), manager, nil),
	)
	require.Equal(t, 1, manager.built)
}

// TestRebuildRestoredDeferredIndexesNamesMissingBeforeBuilding requires the
// names to reach the log before the build starts, the same ordering the
// startup repair holds: the build itself is silent for as long as it runs.
func TestRebuildRestoredDeferredIndexesNamesMissingBeforeBuilding(
	t *testing.T,
) {
	t.Parallel()
	var buf bytes.Buffer
	manager := &ctxIndexManager{
		missing:   []string{"idx_asset_fingerprint"},
		logBuffer: &buf,
	}

	require.NoError(t, rebuildRestoredDeferredIndexes(
		context.Background(),
		manager,
		slog.New(slog.NewTextHandler(&buf, nil)),
	))

	require.Contains(
		t, manager.loggedAt, "idx_asset_fingerprint",
		"the rebuild must be announced before it starts",
	)
	require.Contains(
		t, buf.String(),
		"deferred metadata index repair complete in the restored database",
	)
}

func TestRebuildRestoredDeferredIndexesLogsProgressCallbacks(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	manager := &progressIndexManager{
		ctxIndexManager: &ctxIndexManager{
			missing:   []string{"idx_asset_fingerprint"},
			logBuffer: &buf,
		},
	}
	err := rebuildRestoredDeferredIndexes(
		context.Background(),
		manager,
		slog.New(slog.NewTextHandler(&buf, nil)),
	)
	require.NoError(t, err)
	require.Equal(t, 1, manager.progressBuilds)
	require.Contains(t, buf.String(), "building deferred metadata index")
	require.Contains(t, buf.String(), "deferred metadata index ready")
}

// TestRebuildRestoredDeferredIndexesTolerateNilLogger keeps the logger
// optional: nothing else in this package logs, so callers may leave it unset.
func TestRebuildRestoredDeferredIndexesTolerateNilLogger(t *testing.T) {
	t.Parallel()
	manager := &ctxIndexManager{missing: []string{"idx_asset_fingerprint"}}

	require.NoError(t, rebuildRestoredDeferredIndexes(
		context.Background(),
		manager,
		nil,
	))
	require.Equal(t, 1, manager.ctxBuilds)
}

// newRestoreInternalTestDB and newRestoreInternalTestHost duplicate the
// small fixtures restore_test.go/storage_host_test.go build (newTestDB,
// newTestStorageHost) rather than sharing them: those live in the
// external lifecycle_test package, which this white-box test file (needed
// to reach the unexported syncDir var below) cannot see.
func newRestoreInternalTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return db
}

func newRestoreInternalTestHost(t *testing.T) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host
}

func newRestoreInternalTestBlock() models.Block {
	return models.Block{
		ID:     1,
		Slot:   10,
		Hash:   bytes.Repeat([]byte{0x01}, 32),
		Cbor:   []byte{0x80},
		Number: 1,
		Type:   1,
	}
}

// TestSyncDirTreeSyncsEveryDirectory verifies syncDirTree calls syncDir
// for every directory in a nested tree, including the root itself, and
// does not call it for regular files.
// Not t.Parallel: this and the two tests below swap the package-level
// syncDir seam, which every concurrent Restore in this package would
// otherwise observe.
func TestSyncDirTreeSyncsEveryDirectory(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "a", "b"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "a", "b", "f"), []byte("x"), 0o644,
	))

	var synced []string
	orig := syncDir
	syncDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() { syncDir = orig })

	require.NoError(t, syncDirTree(root))
	require.Contains(t, synced, root)
	require.Contains(t, synced, filepath.Join(root, "a"))
	require.Contains(t, synced, filepath.Join(root, "a", "b"))
	require.NotContains(
		t, synced, filepath.Join(root, "a", "b", "f"),
		"syncDirTree must only fsync directories, not files",
	)
}

// TestRestoreValidatedFailsClosedWhenStagingSyncFails guards the crash-
// durability gap this closes: if the pre-activation directory sync fails,
// RestoreValidated must not proceed to rename the staging directory into
// place -- targetDataDir must be left exactly as untouched as any other
// failure earlier in the pipeline leaves it, not silently activated
// anyway despite durability being unconfirmed.
func TestRestoreValidatedFailsClosedWhenStagingSyncFails(t *testing.T) {
	db := newRestoreInternalTestDB(t)
	require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := Snapshot(
		context.Background(),
		db,
		snapshotDir,
		TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	injectedErr := errors.New("injected staging sync failure")
	orig := syncDir
	syncDir = func(string) error { return injectedErr }
	t.Cleanup(func() { syncDir = orig })

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = Restore(
		context.Background(),
		newRestoreInternalTestHost(t),
		nil,
		snapshotDir,
		targetDir,
		RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, injectedErr)

	_, statErr := os.Stat(targetDir)
	require.True(
		t, os.IsNotExist(statErr),
		"targetDataDir must not be activated when the pre-rename sync fails",
	)
}

// TestRestoreValidatedSurfacesPostRenameSyncFailure verifies that a
// failure syncing the parent directory after the activating rename is
// still surfaced as an error -- even though targetDataDir was already
// renamed into place and is perfectly usable, "restore succeeded" must
// not be reported when this durability guarantee could not be confirmed.
func TestRestoreValidatedSurfacesPostRenameSyncFailure(t *testing.T) {
	db := newRestoreInternalTestDB(t)
	require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := Snapshot(
		context.Background(),
		db,
		snapshotDir,
		TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)

	parentDir := t.TempDir()
	targetDir := filepath.Join(parentDir, "restored")

	injectedErr := errors.New("injected parent sync failure")
	orig := syncDir
	syncDir = func(path string) error {
		if path == parentDir {
			return injectedErr
		}
		return orig(path)
	}
	t.Cleanup(func() { syncDir = orig })

	_, err = Restore(
		context.Background(),
		newRestoreInternalTestHost(t),
		nil,
		snapshotDir,
		targetDir,
		RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, injectedErr)

	// The rename already happened before the parent sync ran -- the
	// restored data directory is real and usable even though this
	// particular durability guarantee could not be confirmed.
	_, statErr := os.Stat(targetDir)
	require.NoError(t, statErr)
}
