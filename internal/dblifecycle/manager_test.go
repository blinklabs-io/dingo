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

package dblifecycle_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// testDestinationRegistry is this test package's own instance-owned
// registry (mirroring what composition code builds at startup — see
// lifecycle.DestinationRegistry's doc comment), shared by every fake cloud
// scheme this file registers below instead of the removed package-global
// process registry.
var testDestinationRegistry = lifecycle.NewDestinationRegistry()

// testManagerBlobPlugin is deliberately not the production Badger selector:
// the manager rejects automatic snapshots for the real selector until the
// backup path can capture a version without holding the commit barrier. The
// manager tests below exercise event handling and retention with a real
// Badger-backed database; the policy itself has a focused test below.
const testManagerBlobPlugin = "badger-test"

func newManagerTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return db
}

// cloudPrimaryBackupStore is a contract-compatible cloud-primary test double.
// It delegates storage to Badger but owns its Backuper implementation so the
// test proves lifecycle.Snapshot uses the provider selected as s3 or gcs.
type cloudPrimaryBackupStore struct {
	*badger.BlobStoreBadger
	backupCalled *atomic.Bool
}

func (s *cloudPrimaryBackupStore) Backup(
	ctx context.Context,
	w io.Writer,
) error {
	s.backupCalled.Store(true)
	return s.BlobStoreBadger.Backup(ctx, w)
}

func cloudPrimaryTestProvider(
	name string,
	backupCalled *atomic.Bool,
) dbtest.StorageProvider {
	return dbtest.StorageProvider{
		Name: name,
		Register: func(host *plugin.Host) error {
			return plugin.Register[blob.BlobStore](
				host,
				plugin.Descriptor{
					Capability:  plugin.CapabilityStorageBlob,
					Name:        name,
					Description: "cloud-primary snapshot test provider",
				},
				func() struct{} { return struct{}{} },
				func(
					_ context.Context,
					_ struct{},
					deps blob.ProviderDependencies,
				) (blob.BlobStore, plugin.Instance, error) {
					store, err := badger.New(
						badger.WithDataDir(deps.DataDir),
						badger.WithLogger(deps.Logger),
						badger.WithGc(false),
						badger.WithDeferOpen(),
					)
					if err != nil {
						return nil, nil, err
					}
					cloudStore := &cloudPrimaryBackupStore{
						BlobStoreBadger: store,
						backupCalled:    backupCalled,
					}
					return cloudStore, plugin.Lifecycle{
						StartFunc: func(context.Context) error {
							return cloudStore.Start()
						},
						StopFunc: func(context.Context) error {
							return cloudStore.Stop()
						},
					}, nil
				},
			)
		},
	}
}

func publishEpochTransition(eb *event.EventBus, newEpoch uint64) {
	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{
				PreviousEpoch: newEpoch - 1,
				NewEpoch:      newEpoch,
			},
		),
	)
}

// TestManagerDisabledByDefaultDoesNothing verifies that with
// SnapshotEnabled false, an epoch-transition event never triggers a snapshot.
func TestManagerDisabledByDefaultDoesNothing(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := filepath.Join(t.TempDir(), "snapshots")
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled: false,
		SnapshotDir:     snapshotDir,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 1)
	require.Never(t, func() bool {
		_, err := os.Stat(snapshotDir)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond)
}

// TestManagerRejectsCloudPrimaryAutomaticSnapshotsButManualSnapshotsRemainAvailable
// proves that an epoch-boundary snapshot cannot begin an unbounded remote
// object walk while holding the commit barrier. The manager is the automatic
// path only: manual lifecycle snapshots remain available for an operator who
// deliberately accepts their duration.
func TestManagerRejectsCloudPrimaryAutomaticSnapshotsButManualSnapshotsRemainAvailable(
	t *testing.T,
) {
	for _, blobPluginName := range []string{"s3", "gcs"} {
		t.Run(blobPluginName, func(t *testing.T) {
			var backupCalled atomic.Bool
			db, err := dbtest.NewDatabaseWithOptions(
				t,
				dbtest.Options{
					Config: &database.Config{DataDir: t.TempDir()},
					Blob: cloudPrimaryTestProvider(
						blobPluginName,
						&backupCalled,
					),
				},
			)
			require.NoError(t, err)
			eb := event.NewEventBus(nil, nil)
			t.Cleanup(eb.Stop)

			m := dblifecycle.NewManager(
				db,
				eb,
				config.DatabaseLifecycleConfig{
					SnapshotEnabled: true,
					SnapshotDir:     t.TempDir(),
				},
				blobPluginName,
				"sqlite",
				testDestinationRegistry,
				nil,
			)
			err = m.Start(context.Background())
			require.ErrorIs(t, err, dblifecycle.ErrCloudPrimaryAutomaticSnapshots)
			require.ErrorContains(t, err, blobPluginName)

			manualDir := filepath.Join(t.TempDir(), "manual")
			manifest, err := lifecycle.Snapshot(
				context.Background(),
				db,
				manualDir,
				lifecycle.TriggerManual,
				"test",
				blobPluginName,
				"sqlite",
			)
			require.NoError(t, err)
			require.True(t, backupCalled.Load())
			require.Equal(t, lifecycle.TriggerManual, manifest.Trigger)
			require.FileExists(
				t,
				filepath.Join(manualDir, lifecycle.ManifestFileName),
			)
		})
	}
}

// TestManagerRejectsBadgerAutomaticSnapshotsButManualSnapshotsRemainAvailable
// proves that the epoch-boundary path fails closed while Badger's explicitly
// requested manual snapshot path remains available. Badger's native backup
// API does not expose a separate, cheap MVCC-version capture operation, so a
// full automatic backup would otherwise hold the commit barrier for its whole
// production-scale stream.
func TestManagerRejectsBadgerAutomaticSnapshotsButManualSnapshotsRemainAvailable(
	t *testing.T,
) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	t.Cleanup(eb.Stop)

	m := dblifecycle.NewManager(
		db,
		eb,
		config.DatabaseLifecycleConfig{
			SnapshotEnabled: true,
			SnapshotDir:     t.TempDir(),
		},
		"badger",
		"sqlite",
		testDestinationRegistry,
		nil,
	)
	err := m.Start(context.Background())
	require.ErrorIs(t, err, dblifecycle.ErrBadgerAutomaticSnapshots)

	manualDir := filepath.Join(t.TempDir(), "manual")
	manifest, err := lifecycle.Snapshot(
		context.Background(),
		db,
		manualDir,
		lifecycle.TriggerManual,
		"test",
		"badger",
		"sqlite",
	)
	require.NoError(t, err)
	require.Equal(t, lifecycle.TriggerManual, manifest.Trigger)
	require.FileExists(
		t,
		filepath.Join(manualDir, lifecycle.ManifestFileName),
	)
}

// TestManagerCapturesSnapshotOnEpochBoundary verifies that an
// epoch-transition event captures a real snapshot under epoch-<N>.
func TestManagerCapturesSnapshotOnEpochBoundary(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 1,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 5)

	require.Eventually(t, func() bool {
		_, err := os.Stat(
			filepath.Join(snapshotDir, "epoch-5", "manifest.json"),
		)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

// TestManagerRespectsEveryNEpochsGating verifies that with
// SnapshotEveryNEpochs=2, only an epoch divisible by 2 is captured.
func TestManagerRespectsEveryNEpochsGating(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 2,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 3) // 3 % 2 != 0, must be skipped
	publishEpochTransition(eb, 4) // 4 % 2 == 0, must be captured

	require.Eventually(t, func() bool {
		_, err := os.Stat(
			filepath.Join(snapshotDir, "epoch-4", "manifest.json"),
		)
		return err == nil
	}, 30*time.Second, 10*time.Millisecond)

	require.NoDirExists(t, filepath.Join(snapshotDir, "epoch-3"))
}

// TestManagerRedeliveredEventIsNotFatal verifies that publishing the same
// epoch's transition event twice does not crash or stall the manager.
func TestManagerRedeliveredEventIsNotFatal(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 1,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 7)
	require.Eventually(t, func() bool {
		_, err := os.Stat(
			filepath.Join(snapshotDir, "epoch-7", "manifest.json"),
		)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	// Redeliver the same epoch's event (simulating a restart replay) —
	// must not crash the loop or leave it stuck; a later, new epoch must
	// still be captured afterward.
	publishEpochTransition(eb, 7)
	publishEpochTransition(eb, 8)
	require.Eventually(t, func() bool {
		_, err := os.Stat(
			filepath.Join(snapshotDir, "epoch-8", "manifest.json"),
		)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

// TestManagerPrunesOldSnapshotsBeyondRetention verifies that with
// SnapshotRetention=2, capturing a 3rd snapshot deletes the oldest one.
func TestManagerPrunesOldSnapshotsBeyondRetention(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:      true,
		SnapshotDir:          snapshotDir,
		SnapshotEveryNEpochs: 1,
		SnapshotRetention:    2,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	for epoch := uint64(1); epoch <= 3; epoch++ {
		publishEpochTransition(eb, epoch)
		// 30s, not 5s: a real badger+sqlite snapshot capture, matching
		// TestManagerRespectsEveryNEpochsGating's identical allowance for
		// slower Linux and Windows CI runners -- this test captures three
		// in a row, so it needs the same per-capture headroom.
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(
				snapshotDir,
				"epoch-"+strconv.FormatUint(epoch, 10),
				"manifest.json",
			))
			return err == nil
		}, 30*time.Second, 10*time.Millisecond)
	}

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-1"))
		return os.IsNotExist(err)
	}, 30*time.Second, 10*time.Millisecond)
	require.DirExists(t, filepath.Join(snapshotDir, "epoch-2"))
	require.DirExists(t, filepath.Join(snapshotDir, "epoch-3"))
}

// directoryBackedCloudDestination contains the common filesystem behavior
// used by the manager's cloud-destination test doubles. Specialized doubles
// embed it and override only the operation whose failure they inject.
type directoryBackedCloudDestination struct {
	dir      string
	uploaded chan<- string
}

func (d *directoryBackedCloudDestination) UploadDir(
	ctx context.Context,
	localDir string,
) error {
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(localDir, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(d.dir, entry.Name()), data, 0o600); err != nil {
			return err
		}
	}
	if d.uploaded != nil {
		select {
		case d.uploaded <- d.dir:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (d *directoryBackedCloudDestination) DownloadDir(
	context.Context,
	string,
) error {
	return errors.New("not implemented")
}

func (d *directoryBackedCloudDestination) Delete(context.Context) error {
	return os.RemoveAll(d.dir)
}

var (
	_ lifecycle.CloudDestination = &directoryBackedCloudDestination{}
	_ lifecycle.CloudDeleter     = &directoryBackedCloudDestination{}
)

var (
	managerFakeCloudMu  sync.Mutex
	managerFakeCloudDir string
)

func init() {
	testDestinationRegistry.Register(
		"managerfaketest",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			managerFakeCloudMu.Lock()
			base := managerFakeCloudDir
			managerFakeCloudMu.Unlock()
			return &directoryBackedCloudDestination{
				dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
			}, nil
		},
	)
}

func setManagerFakeCloudBackingDir(t *testing.T, dir string) {
	t.Helper()
	managerFakeCloudMu.Lock()
	managerFakeCloudDir = dir
	managerFakeCloudMu.Unlock()
}

// TestManagerPruningDeletesCloudMirror guards against a real
// gap: retention pruning only removed the local snapshot
// directory, leaving every epoch's mirrored cloud copy in object storage
// forever, growing without bound regardless of SnapshotRetention. With a
// working cloud destination configured, pruning an epoch beyond
// retention must also delete that epoch's cloud mirror.
func TestManagerPruningDeletesCloudMirror(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	uploaded := make(chan string)
	registry := lifecycle.NewDestinationRegistry()
	registry.Register(
		"managerprunetest",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			return &directoryBackedCloudDestination{
				dir: filepath.Join(
					cloudBackingDir,
					filepath.FromSlash(strings.TrimPrefix(uri.Path, "/")),
				),
				uploaded: uploaded,
			}, nil
		},
	)
	const cloudDest = "managerprunetest://bucket/prefix"
	cloudPrefixDir := filepath.Join(cloudBackingDir, "prefix")
	// Start launches a retry scan in the background. Give that scan an
	// existing, nonnumeric epoch directory to upload before publishing any
	// real epoch: receiving this upload proves the scan reached the probe,
	// and retryMu orders the first epoch handler after the scan finishes.
	// The nonnumeric suffix keeps the probe out of retention accounting.
	const startupProbeName = "epoch-startup-probe"
	startupProbeDir := filepath.Join(snapshotDir, startupProbeName)
	require.NoError(t, os.Mkdir(startupProbeDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(startupProbeDir, "probe"), []byte("probe"), 0o600,
	))

	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotRetention:        2,
		SnapshotCloudDestination: cloudDest,
	}, testManagerBlobPlugin, "sqlite", registry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()
	require.Equal(t, filepath.Join(cloudPrefixDir, startupProbeName),
		testutil.RequireReceive(
			t,
			uploaded,
			30*time.Second,
			"startup cloud-mirror retry scan did not reach its probe",
		),
	)

	for epoch := uint64(1); epoch <= 3; epoch++ {
		publishEpochTransition(eb, epoch)
		epochDir := filepath.Join(
			cloudPrefixDir,
			"epoch-"+strconv.FormatUint(epoch, 10),
		)
		require.Equal(t, epochDir, testutil.RequireReceive(
			t,
			uploaded,
			30*time.Second,
			"automatic snapshot cloud upload did not complete",
		))
		require.FileExists(t, filepath.Join(epochDir, "manifest.json"))
	}

	// epoch-1 is beyond retention (2): both its local directory and its
	// cloud mirror must be gone. Local removal is the observable completion
	// point shared by both the fixed implementation and the old local-only
	// pruning behavior, so the remote assertion below fails on the actual
	// deletion defect rather than waiting for a notification the old code
	// could never send.
	testutil.WaitForCondition(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-1"))
		return os.IsNotExist(err)
	}, 30*time.Second, "automatic snapshot local prune did not complete")
	// Stop is the manager's drain barrier and proves the same handler has
	// fully completed before inspecting either retained set.
	require.NoError(t, m.Stop())
	require.NoDirExists(t, filepath.Join(snapshotDir, "epoch-1"))
	require.NoDirExists(t, filepath.Join(cloudPrefixDir, "epoch-1"))
	require.DirExists(t, filepath.Join(cloudPrefixDir, "epoch-2"))
	require.DirExists(t, filepath.Join(cloudPrefixDir, "epoch-3"))
}

// failingCloudDestination always fails UploadDir, simulating a cloud
// mirror outage that happens after the local snapshot write has already
// succeeded — the exact case a stat-after-error idempotency check can't
// tell apart from "this epoch's snapshot already exists from an earlier
// successful run."
type failingCloudDestination struct{}

func (failingCloudDestination) UploadDir(context.Context, string) error {
	return errors.New("simulated cloud upload failure")
}

func (failingCloudDestination) DownloadDir(context.Context, string) error {
	return errors.New("not implemented")
}

func init() {
	testDestinationRegistry.Register(
		"faketestfail",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return failingCloudDestination{}, nil
		},
	)
}

// panickingCloudDestination panics on UploadDir for exactly the one
// snapshot directory named panicOnDirBase, simulating a bug in a cloud
// destination plugin (a nil-pointer deref deep in an SDK, for example)
// for that single call, while behaving like an ordinary successful
// upload (a no-op) for every other snapshot. This lets a test prove the
// manager returns to fully normal, panic-free operation for a later
// epoch, not merely that its local-write half kept working while the
// same call kept panicking underneath it.
type panickingCloudDestination struct {
	panicOnDirBase string
}

func (d panickingCloudDestination) UploadDir(
	_ context.Context,
	localDir string,
) error {
	if filepath.Base(localDir) == d.panicOnDirBase {
		panic("simulated cloud destination bug")
	}
	return nil
}

func (panickingCloudDestination) DownloadDir(context.Context, string) error {
	return errors.New("not implemented")
}

func init() {
	testDestinationRegistry.Register(
		"faketestpanic",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return panickingCloudDestination{panicOnDirBase: "epoch-20"}, nil
		},
	)
}

// TestManagerSurvivesHandlerPanic guards against a real
// bug: Manager used to subscribe via the EventBus's raw channel Subscribe
// and run its own hand-rolled per-event loop, bypassing SubscribeFunc's
// safeHandlerCall panic recovery entirely -- a panic anywhere in the
// snapshot path (here, simulating a buggy cloud destination plugin) would
// propagate out of that loop's goroutine and crash the whole process,
// with no way for anything outside that goroutine to catch it. Now that
// Start subscribes via SubscribeFunc, the same panic must be caught and
// logged by the EventBus, and the manager must keep working afterward:
// unaffected by whatever epoch triggered the panic, a later epoch must
// still be captured normally.
func TestManagerSurvivesHandlerPanic(t *testing.T) {
	db := newManagerTestDB(t)
	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	// The panic is recovered and logged by EventBus's own safeHandlerCall
	// (via e.Logger), not by Manager -- so the bus itself, not just the
	// manager, needs this capturing logger for the assertions below to see
	// it.
	eb := event.NewEventBus(nil, logger)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotCloudDestination: "faketestpanic://bucket/prefix",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, logger)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	// Epoch 20's automatic snapshot panics inside UploadDir (see
	// panickingCloudDestination). If that panic is not recovered somewhere
	// between here and the manager's event dispatch, the whole test binary
	// crashes right here -- this is not a normal assertion failure to
	// recover from afterward, it's the actual regression this test exists
	// to catch.
	publishEpochTransition(eb, 20)
	require.Eventually(t, func() bool {
		return strings.Contains(
			logBuf.String(),
			"SubscribeFunc handler panicked",
		)
	}, 5*time.Second, 10*time.Millisecond, "the panic must be caught and logged by the EventBus")

	// The manager's dispatch must still be alive and fully, normally
	// functional afterward -- not stuck, not dead, and not itself still
	// panicking -- for this to have actually recovered rather than merely
	// not-yet-crashed. epoch-21's own cloud "upload" is a clean no-op (see
	// panickingCloudDestination), so its manifest existing, with no
	// further panic logged, proves a complete, ordinary snapshot ran to
	// completion right after the panic.
	publishEpochTransition(eb, 21)
	require.Eventually(t, func() bool {
		_, err := os.Stat(
			filepath.Join(snapshotDir, "epoch-21", "manifest.json"),
		)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(
		t, 1,
		strings.Count(logBuf.String(), "SubscribeFunc handler panicked"),
		"only epoch 20 should have panicked; epoch 21 must run cleanly",
	)
}

// syncBuffer is a bytes.Buffer safe for the concurrent write (the
// manager's background goroutine logging) / read (the test polling for
// that log line) pattern require.Eventually needs here.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestManagerCloudUploadFailureIsNotSwallowed verifies that a cloud-upload
// failure (with the local snapshot write having already succeeded) is
// logged as a real failure, not silently treated as an idempotent
// already-captured-this-epoch skip just because the local directory now
// exists.
//
// handleEpochTransitionEvent logs this directly via the Manager's own
// logger (m.logger), not through the EventBus's internal logging, so
// wiring only the Manager's logger to logBuf below is sufficient on its
// own -- verified by deliberately misrouting the production log call to
// a different logger and confirming this test's first require.Eventually
// then fails instead of passing vacuously. The EventBus is still given
// the same logger here anyway, purely defensively: it costs nothing and
// keeps this test valid even if EventBus's own logging (e.g. a
// SubscribeFunc handler panic) ever became relevant to what it checks.
func TestManagerCloudUploadFailureIsNotSwallowed(t *testing.T) {
	db := newManagerTestDB(t)

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	eb := event.NewEventBus(nil, logger)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotCloudDestination: "faketestfail://bucket/prefix",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, logger)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 9)
	require.Eventually(t, func() bool {
		return strings.Contains(
			logBuf.String(),
			"automatic database snapshot failed",
		)
	}, 15*time.Second, 10*time.Millisecond)

	require.NotContains(
		t,
		logBuf.String(),
		"already exists, skipping",
		"a cloud-upload failure must not be silently treated as idempotent already-exists skip",
	)
}

// blockingCloudDestination's UploadDir blocks unconditionally (ignoring
// ctx) until released is closed, closing started the moment it's
// entered -- simulating a slow, still-running upload so a test can hold
// a snapshot handler "in flight" for exactly as long as it needs,
// independent of any context cancellation the handler's ctx might see.
type blockingCloudDestination struct {
	started  chan struct{}
	released chan struct{}
}

func (d *blockingCloudDestination) UploadDir(context.Context, string) error {
	close(d.started)
	<-d.released
	return nil
}

func (d *blockingCloudDestination) DownloadDir(context.Context, string) error {
	return errors.New("not implemented")
}

var (
	blockingCloudMu   sync.Mutex
	blockingCloudDest *blockingCloudDestination
)

func init() {
	testDestinationRegistry.Register(
		"faketestblocking",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			blockingCloudMu.Lock()
			defer blockingCloudMu.Unlock()
			return blockingCloudDest, nil
		},
	)
}

// TestManagerStopWaitsForInFlightHandlerAfterExternalContextCancellation
// guards against a real bug: Start's cleanup goroutine
// (which reacts to the parent ctx passed to Start being cancelled
// directly, not just to a call to Stop) used to race Stop() to decide
// which of them was responsible for unsubscribing. If the parent ctx was
// cancelled externally first, that cleanup goroutine could already have
// reset the manager's running state by the time a concurrent Stop() call
// checked it, so Stop() returned immediately instead of waiting for the
// SAME goroutine's still-in-flight UnsubscribeAndWait call -- which
// itself was waiting for a snapshot handler that was still actually
// running. A caller relying on Stop() returning to mean "the manager's
// snapshot handler is no longer touching the database" could then
// proceed to close/replace the database while an upload was still
// running against it. This is exactly the shape of a real node shutdown:
// the top-level context gets cancelled, and Stop() is called on every
// subsystem separately with no guaranteed ordering between the two.
func TestManagerStopWaitsForInFlightHandlerAfterExternalContextCancellation(
	t *testing.T,
) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	dest := &blockingCloudDestination{
		started:  make(chan struct{}),
		released: make(chan struct{}),
	}
	blockingCloudMu.Lock()
	blockingCloudDest = dest
	blockingCloudMu.Unlock()
	releaseOnce := sync.Once{}
	releaseDest := func() { releaseOnce.Do(func() { close(dest.released) }) }
	// Registered after "defer eb.Stop()" above, so it unwinds first: if an
	// assertion below fails (exactly what should happen against the buggy
	// version this test guards against) and aborts the test via Goexit,
	// this still releases the blocked dispatch goroutine before eb.Stop()
	// gets a chance to wait on it forever -- without this, a correctly-
	// failing assertion here would manifest as the whole test hanging
	// until the package timeout instead of a clean, fast failure.
	defer releaseDest()

	snapshotDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotCloudDestination: "faketestblocking://bucket/prefix",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(ctx))
	// Registered immediately after Start succeeds (before any assertion
	// that could fail): if this test fails before reaching the explicit
	// cancel()/Stop() below, this still tears the manager down instead of
	// leaving its watcher goroutine (and the blocked snapshot handler)
	// running past the test. Redundant with, and safe alongside, the
	// explicit calls further down and the standalone releaseDest defer
	// above -- cancel/Stop are idempotent and releaseDest is
	// sync.Once-guarded.
	defer func() {
		cancel()
		releaseDest()
		_ = m.Stop()
	}()

	publishEpochTransition(eb, 3)
	testutil.RequireReceive(
		t, dest.started, 5*time.Second,
		"the snapshot handler must have entered UploadDir",
	)

	// Cancel the PARENT context directly (not via Stop) -- the
	// external-cancellation path Start's own cleanup goroutine reacts to.
	cancel()

	// Call Stop concurrently, exactly as node shutdown would.
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		_ = m.Stop()
	}()

	testutil.RequireNoReceive(
		t, stopDone, 150*time.Millisecond,
		"Stop must not return while the snapshot handler it's stopping is "+
			"still in flight, even though the parent ctx was already "+
			"cancelled externally",
	)

	releaseDest()

	testutil.RequireReceive(
		t,
		stopDone,
		time.Second,
		"Stop must return promptly once the in-flight handler actually finishes",
	)
}

// flakyCloudDestination fails UploadDir exactly once (its first call),
// then succeeds on every subsequent call — simulating a transient cloud
// outage that has cleared by the time the operation is retried, as
// opposed to failingCloudDestination's permanent failure.
type flakyCloudDestination struct {
	failed *bool
	mu     *sync.Mutex
}

func (d flakyCloudDestination) UploadDir(context.Context, string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !*d.failed {
		*d.failed = true
		return errors.New("simulated transient cloud upload failure")
	}
	return nil
}

func (flakyCloudDestination) DownloadDir(context.Context, string) error {
	return errors.New("not implemented")
}

var (
	flakyCloudFailed  bool
	flakyCloudMu      sync.Mutex
	flakyCloud2Failed bool
	flakyCloud2Mu     sync.Mutex
	flakyCloud3Failed bool
	flakyCloud3Mu     sync.Mutex
)

func resetFlakyCloudState(failed *bool, mu *sync.Mutex) {
	mu.Lock()
	*failed = false
	mu.Unlock()
}

func init() {
	testDestinationRegistry.Register(
		"faketestflaky",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return flakyCloudDestination{
				failed: &flakyCloudFailed,
				mu:     &flakyCloudMu,
			}, nil
		},
	)
}

// TestManagerRetriesCloudMirrorAfterTransientFailureOnRedeliveredEvent
// guards the actual bug behind this fix: handleEpochTransition's
// idempotency check used to look only at whether the local snapshot
// directory existed, which a transient cloud-upload failure and a fully
// successful run both leave true — so once a cloud upload failed after
// the local write succeeded, a later redelivered epoch-transition event
// (or a node restart) would see the directory, assume "already done", and
// permanently skip the cloud mirror. Combined with retention eventually
// pruning the local-only copy, this could silently lose the only copy of
// that snapshot ever having existed. This proves the opposite: after a
// transient failure, redelivering the same epoch event actually retries
// (and this time succeeds at) the cloud mirror, from the existing local
// copy, without redoing the local snapshot.
func TestManagerRetriesCloudMirrorAfterTransientFailureOnRedeliveredEvent(
	t *testing.T,
) {
	resetFlakyCloudState(&flakyCloudFailed, &flakyCloudMu)
	db := newManagerTestDB(t)

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	eb := event.NewEventBus(nil, logger)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotCloudDestination: "faketestflaky://bucket/prefix",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, logger)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	// First delivery: local write succeeds, cloud upload fails.
	publishEpochTransition(eb, 9)
	require.Eventually(t, func() bool {
		return strings.Contains(
			logBuf.String(),
			"automatic database snapshot failed",
		)
	}, 5*time.Second, 10*time.Millisecond)

	entries, err := os.ReadDir(snapshotDir)
	require.NoError(t, err)
	require.Len(
		t,
		entries,
		1,
		"the local snapshot must still exist despite the cloud failure",
	)
	destDir := filepath.Join(snapshotDir, entries[0].Name())
	require.False(
		t, lifecycle.IsCloudMirrored(destDir),
		"must not be marked mirrored after a failed upload",
	)

	// Redelivery of the SAME epoch event (e.g. a restart, or the EventBus
	// redelivering): the cloud outage has cleared, so this must retry the
	// upload from the existing local copy and succeed, not skip as
	// already-done.
	require.Eventually(t, func() bool {
		// Publish until the asynchronous EventBus accepts a delivery after
		// the first handler has fully unwound. A delivery made in the tiny
		// interval after its error log but before dispatch cleanup may be
		// coalesced by the bus.
		publishEpochTransition(eb, 9)
		return lifecycle.IsCloudMirrored(destDir)
	}, 15*time.Second, 10*time.Millisecond)

	require.True(
		t, lifecycle.IsCloudMirrored(destDir),
		"must be marked mirrored after the retried upload succeeds",
	)
	entries, err = os.ReadDir(snapshotDir)
	require.NoError(t, err)
	require.Len(
		t,
		entries,
		1,
		"the retry must reuse the existing local snapshot directory, not create a second one",
	)
}

func init() {
	testDestinationRegistry.Register(
		"faketestflaky2",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return flakyCloudDestination{
				failed: &flakyCloud2Failed,
				mu:     &flakyCloud2Mu,
			}, nil
		},
	)
}

// TestManagerRetriesUnmirroredSnapshotOnLaterEpochWithoutRedelivery guards
// the gap the redelivery-only retry left: the old epoch whose upload
// failed never gets its own transition event again once a later epoch has
// begun, so a fix that only retries on an exact redelivery never actually
// heals it in the normal case (a node simply keeps running and epochs keep
// advancing). This proves epoch 10's transition notices and retries epoch
// 9's stuck local-only snapshot, without epoch 9's own event ever being
// redelivered.
func TestManagerRetriesUnmirroredSnapshotOnLaterEpochWithoutRedelivery(
	t *testing.T,
) {
	resetFlakyCloudState(&flakyCloud2Failed, &flakyCloud2Mu)
	db := newManagerTestDB(t)

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	eb := event.NewEventBus(nil, logger)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotCloudDestination: "faketestflaky2://bucket/prefix",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, logger)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 9)
	require.Eventually(t, func() bool {
		return strings.Contains(
			logBuf.String(),
			"automatic database snapshot failed",
		)
	}, 15*time.Second, 10*time.Millisecond)

	destDir := filepath.Join(snapshotDir, "epoch-9")
	require.False(
		t, lifecycle.IsCloudMirrored(destDir),
		"must not be marked mirrored after a failed upload",
	)

	// Advance to a genuinely different epoch -- epoch 9's own event is
	// never redelivered.
	publishEpochTransition(eb, 10)
	require.Eventually(t, func() bool {
		return lifecycle.IsCloudMirrored(destDir)
	}, 15*time.Second, 10*time.Millisecond,
		"epoch 9's stuck snapshot must be retried when epoch 10 transitions, "+
			"without epoch 9's own event ever being redelivered",
	)
}

func init() {
	testDestinationRegistry.Register(
		"faketestflaky3",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return flakyCloudDestination{
				failed: &flakyCloud3Failed,
				mu:     &flakyCloud3Mu,
			}, nil
		},
	)
}

// TestManagerRetriesUnmirroredSnapshotOnRestart guards the other half of
// the same gap: a plain node restart never redelivers the failed epoch's
// transition event either, and could otherwise wait indefinitely (up to
// the next epoch boundary, days later on mainnet) before ever retrying.
// This proves a fresh Manager's Start alone -- with no epoch-transition
// event of any kind delivered to it -- notices and retries a snapshot left
// unmirrored by a previous run.
func TestManagerRetriesUnmirroredSnapshotOnRestart(t *testing.T) {
	db := newManagerTestDB(t)

	snapshotDir := t.TempDir()
	cfg := config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotCloudDestination: "faketestflaky3://bucket/prefix",
	}
	destDir := filepath.Join(snapshotDir, "epoch-9")
	// Recreate exactly the durable state a previous process leaves after
	// its local snapshot succeeds and its cloud upload fails: a valid local
	// epoch directory with no cloud-mirrored marker. The later-epoch test
	// above exercises the real transient failure path; constructing the
	// restart fixture directly avoids depending on asynchronous EventBus
	// cleanup from a process that, by definition, no longer exists.
	_, err := lifecycle.Snapshot(
		context.Background(),
		db,
		destDir,
		lifecycle.TriggerEpochBoundary,
		"test-version",
		testManagerBlobPlugin,
		"sqlite",
	)
	require.NoError(t, err)
	require.False(
		t, lifecycle.IsCloudMirrored(destDir),
		"must not be marked mirrored after a failed upload",
	)
	flakyCloud3Mu.Lock()
	flakyCloud3Failed = true // the simulated transient outage has cleared
	flakyCloud3Mu.Unlock()

	// Simulate a restart: a brand new Manager over the same on-disk
	// snapshot dir and cloud destination, with no epoch-transition event
	// delivered to it at all.
	logBuf2 := &syncBuffer{}
	logger2 := slog.New(slog.NewTextHandler(logBuf2, nil))
	eb2 := event.NewEventBus(nil, logger2)
	defer eb2.Stop()
	m2 := dblifecycle.NewManager(
		db,
		eb2,
		cfg,
		testManagerBlobPlugin,
		"sqlite",
		testDestinationRegistry,
		logger2,
	)
	require.NoError(t, m2.Start(context.Background()))
	defer m2.Stop()

	require.Eventually(t, func() bool {
		return lifecycle.IsCloudMirrored(destDir)
	}, 15*time.Second, 10*time.Millisecond,
		"restart's startup scan must retry epoch 9's stuck snapshot "+
			"without any epoch-transition event being delivered",
	)
}

// flakyDeleteCloudDestination uses the shared directory-backed behavior, but
// Delete fails for as long as
// failDelete points at true, then succeeds once it's flipped to false —
// simulating a transient cloud outage specifically on the delete path,
// distinct from flakyCloudDestination's upload-path flakiness above.
type flakyDeleteCloudDestination struct {
	*directoryBackedCloudDestination
	failDelete *bool
	mu         *sync.Mutex
}

func (d *flakyDeleteCloudDestination) Delete(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if *d.failDelete {
		return errors.New("simulated transient cloud delete failure")
	}
	return d.directoryBackedCloudDestination.Delete(ctx)
}

var (
	_ lifecycle.CloudDestination = &flakyDeleteCloudDestination{}
	_ lifecycle.CloudDeleter     = &flakyDeleteCloudDestination{}
)

var (
	flakyDeleteCloudMu      sync.Mutex
	flakyDeleteCloudBaseDir string
	flakyDeleteCloudFail    bool
)

func init() {
	testDestinationRegistry.Register(
		"managerfaketest-flakydelete",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			flakyDeleteCloudMu.Lock()
			base := flakyDeleteCloudBaseDir
			flakyDeleteCloudMu.Unlock()
			return &flakyDeleteCloudDestination{
				directoryBackedCloudDestination: &directoryBackedCloudDestination{
					dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
				},
				failDelete: &flakyDeleteCloudFail,
				mu:         &flakyDeleteCloudMu,
			}, nil
		},
	)
}

func setFlakyDeleteCloudBackingDir(t *testing.T, dir string) {
	t.Helper()
	flakyDeleteCloudMu.Lock()
	flakyDeleteCloudBaseDir = dir
	flakyDeleteCloudFail = true
	flakyDeleteCloudMu.Unlock()
	t.Cleanup(func() {
		flakyDeleteCloudMu.Lock()
		flakyDeleteCloudBaseDir = ""
		flakyDeleteCloudFail = false
		flakyDeleteCloudMu.Unlock()
	})
}

// permanentUploadFailureCloudDestination uses the shared directory-backed
// behavior except that UploadDir permanently fails whenever localDir's base name matches
// the currently configured target (see setPermanentUploadFailureTarget) —
// simulating a snapshot whose cloud upload never completes, as opposed to
// flakyCloudDestination's clears-after-one-retry failure above. The
// target can be changed later to simulate the underlying outage finally
// clearing for that specific snapshot.
type permanentUploadFailureCloudDestination struct {
	*directoryBackedCloudDestination
	mu *sync.Mutex
	// target, guarded by mu, names the one directory basename UploadDir
	// currently fails for; every other directory uploads normally.
	target *string
}

func (d *permanentUploadFailureCloudDestination) UploadDir(
	ctx context.Context,
	localDir string,
) error {
	d.mu.Lock()
	target := *d.target
	d.mu.Unlock()
	if filepath.Base(localDir) == target {
		return errors.New("simulated permanent cloud upload failure")
	}
	return d.directoryBackedCloudDestination.UploadDir(
		ctx,
		localDir,
	)
}

var (
	_ lifecycle.CloudDestination = &permanentUploadFailureCloudDestination{}
	_ lifecycle.CloudDeleter     = &permanentUploadFailureCloudDestination{}
)

var (
	permanentUploadFailureMu      sync.Mutex
	permanentUploadFailureBaseDir string
	permanentUploadFailureTarget  string
)

func init() {
	testDestinationRegistry.Register(
		"managerfaketest-permanentuploadfail",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			permanentUploadFailureMu.Lock()
			base := permanentUploadFailureBaseDir
			permanentUploadFailureMu.Unlock()
			return &permanentUploadFailureCloudDestination{
				directoryBackedCloudDestination: &directoryBackedCloudDestination{
					dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
				},
				mu:     &permanentUploadFailureMu,
				target: &permanentUploadFailureTarget,
			}, nil
		},
	)
}

func setPermanentUploadFailureTarget(t *testing.T, backingDir, target string) {
	t.Helper()
	permanentUploadFailureMu.Lock()
	permanentUploadFailureBaseDir = backingDir
	permanentUploadFailureTarget = target
	permanentUploadFailureMu.Unlock()
	t.Cleanup(func() {
		permanentUploadFailureMu.Lock()
		permanentUploadFailureBaseDir = ""
		permanentUploadFailureTarget = ""
		permanentUploadFailureMu.Unlock()
	})
}

// TestManagerPruningPreservesNeverMirroredSnapshotLocally guards against a
// real bug: DeleteCloudSnapshot's ok=true return doesn't distinguish
// "actually deleted a real remote object" from "there was nothing there
// because this snapshot was never mirrored in the first place" (both an
// S3/GCS delete against an empty/nonexistent prefix and a real deletion
// return ok=true, err=nil). pruneOldSnapshots used to treat that ok=true
// as license to also remove the local directory — permanently destroying
// the only surviving copy of a snapshot that failed to mirror, since
// nothing would ever select an already-deleted local directory for a
// retry again. This proves the opposite: a snapshot beyond retention that
// was never confirmed mirrored to the configured cloud destination keeps
// its local copy during pruning, and once a later retry actually mirrors
// it, a subsequent pruning pass finishes the job for real.
func TestManagerPruningPreservesNeverMirroredSnapshotLocally(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	const cloudDest = "managerfaketest-permanentuploadfail://bucket/prefix"
	cloudPrefixDir := filepath.Join(cloudBackingDir, "prefix")
	setPermanentUploadFailureTarget(t, cloudBackingDir, "epoch-1")

	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotRetention:        1,
		SnapshotCloudDestination: cloudDest,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	// epoch 1: local write succeeds, its cloud upload permanently fails.
	publishEpochTransition(eb, 1)
	epoch1Dir := filepath.Join(snapshotDir, "epoch-1")
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(epoch1Dir, "manifest.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.False(
		t, lifecycle.IsCloudMirrored(epoch1Dir),
		"must not be marked mirrored after a failed upload",
	)

	// epoch 2: its own capture succeeds and mirrors normally, which is
	// what triggers pruning -- retention (1) now puts epoch-1 (never
	// mirrored) up for removal. Without the fix, its local directory
	// would be deleted right here even though it was never backed up to
	// the cloud.
	publishEpochTransition(eb, 2)
	require.Eventually(t, func() bool {
		_, err := os.Stat(
			filepath.Join(cloudPrefixDir, "epoch-2", "manifest.json"),
		)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	require.DirExists(
		t,
		epoch1Dir,
		"epoch-1's local copy must survive pruning since it was never mirrored to the cloud",
	)

	// Once the underlying outage clears, a later epoch's opportunistic
	// retry scan mirrors epoch-1 for real, and a subsequent pruning pass
	// must then actually finish removing both its local and cloud copies
	// -- proving the fix doesn't just leak local storage forever either.
	setPermanentUploadFailureTarget(t, cloudBackingDir, "")
	publishEpochTransition(eb, 3)
	require.Eventually(t, func() bool {
		return lifecycle.IsCloudMirrored(epoch1Dir)
	}, 15*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		_, localErr := os.Stat(epoch1Dir)
		_, cloudErr := os.Stat(filepath.Join(cloudPrefixDir, "epoch-1"))
		return os.IsNotExist(localErr) && os.IsNotExist(cloudErr)
	}, 15*time.Second, 10*time.Millisecond,
		"once mirrored for real, a later pruning pass must finish removing "+
			"both copies of the now-retired epoch-1 snapshot",
	)
}

// TestManagerCloudDestinationPrefixIsIncorporatedIntoUploadPath guards
// against a real gap: automatic epoch-boundary snapshots are named
// deterministically (epoch-<N>), identical across every node, so two
// nodes pointed at the same SnapshotCloudDestination would otherwise
// upload to the exact same remote key at every epoch boundary --
// interleaved uploads could even leave one node's manifest.json paired
// with another node's backup files at the same remote path. This proves
// SnapshotCloudDestinationPrefix, once configured, is actually
// incorporated into the remote upload path as an extra path segment ahead
// of the snapshot's own ID, giving each node using a distinct prefix a
// disjoint remote location.
func TestManagerCloudDestinationPrefixIsIncorporatedIntoUploadPath(
	t *testing.T,
) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setManagerFakeCloudBackingDir(t, cloudBackingDir)
	const baseCloudDest = "managerfaketest://bucket/shared-prefix"

	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:                true,
		SnapshotDir:                    snapshotDir,
		SnapshotEveryNEpochs:           1,
		SnapshotCloudDestination:       baseCloudDest,
		SnapshotCloudDestinationPrefix: "node-a",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 5)

	// With the prefix configured, the upload must land under an extra
	// "node-a" path segment ahead of the snapshot's own epoch-5 ID -- not
	// directly under the shared base destination, where a second node
	// sharing baseCloudDest (with its own distinct prefix) would otherwise
	// collide.
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(
			cloudBackingDir,
			"shared-prefix",
			"node-a",
			"epoch-5",
			"manifest.json",
		))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	require.NoDirExists(
		t,
		filepath.Join(cloudBackingDir, "shared-prefix", "epoch-5"),
		"must not upload directly under the shared base destination, bypassing the per-node prefix",
	)
}

func TestManagerRejectsUnsafeCloudDestinationPrefix(t *testing.T) {
	for _, prefix := range []string{"..", ".", "nodes/a", `nodes\a`} {
		t.Run(prefix, func(t *testing.T) {
			eb := event.NewEventBus(nil, nil)
			t.Cleanup(eb.Stop)
			m := dblifecycle.NewManager(
				newManagerTestDB(t),
				eb,
				config.DatabaseLifecycleConfig{
					SnapshotEnabled:                true,
					SnapshotDir:                    t.TempDir(),
					SnapshotCloudDestination:       "managerfaketest://bucket/prefix",
					SnapshotCloudDestinationPrefix: prefix,
				},
				testManagerBlobPlugin,
				"sqlite",
				testDestinationRegistry,
				nil,
			)
			t.Cleanup(func() {
				_ = m.Stop()
			})
			require.ErrorContains(
				t,
				m.Start(context.Background()),
				"safe path segment",
			)
		})
	}
}

// TestManagerWarnsWhenCloudDestinationConfiguredWithoutPrefix verifies
// Start logs a warning when a cloud destination is configured with no
// distinguishing SnapshotCloudDestinationPrefix -- the situation that lets
// two nodes sharing one destination silently collide at the same
// deterministic epoch-<N> remote key.
func TestManagerWarnsWhenCloudDestinationConfiguredWithoutPrefix(t *testing.T) {
	db := newManagerTestDB(t)
	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	eb := event.NewEventBus(nil, logger)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotCloudDestination: "managerfaketest://bucket/prefix",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, logger)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	require.Contains(
		t,
		logBuf.String(),
		"snapshotCloudDestinationPrefix",
		"must warn when a cloud destination is configured without a distinguishing per-node prefix",
	)
}

// TestManagerDoesNotWarnWhenCloudDestinationPrefixIsSet is the negative
// case: once a distinguishing prefix is configured, Start must not warn
// about the same missing-prefix collision risk.
func TestManagerDoesNotWarnWhenCloudDestinationPrefixIsSet(t *testing.T) {
	db := newManagerTestDB(t)
	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	eb := event.NewEventBus(nil, logger)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:                true,
		SnapshotDir:                    snapshotDir,
		SnapshotEveryNEpochs:           1,
		SnapshotCloudDestination:       "managerfaketest://bucket/prefix",
		SnapshotCloudDestinationPrefix: "node-a",
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, logger)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	require.NotContains(
		t,
		logBuf.String(),
		"snapshotCloudDestinationPrefix",
		"must not warn when a distinguishing per-node prefix is already configured",
	)
}

// TestManagerPruningKeepsLocalCopyUntilCloudDeleteSucceeds guards the
// actual bug this fix addresses: pruneOldSnapshots used to remove
// the local snapshot directory before attempting to delete its cloud
// mirror. If that cloud delete then failed, the local directory (the only
// record a later pruning pass uses to rediscover retry candidates) was
// already gone, so the cloud copy was never revisited and retention no
// longer bounded cloud storage at all -- a permanent orphan. This proves
// the opposite: while the cloud delete keeps failing, the local directory
// must survive so a later pass keeps retrying, and once the transient
// outage clears, that same later pass must actually finish the job (both
// copies gone), not leave the survived local copy behind forever either.
func TestManagerPruningKeepsLocalCopyUntilCloudDeleteSucceeds(t *testing.T) {
	db := newManagerTestDB(t)
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	snapshotDir := t.TempDir()
	cloudBackingDir := t.TempDir()
	setFlakyDeleteCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "managerfaketest-flakydelete://bucket/prefix"
	cloudPrefixDir := filepath.Join(cloudBackingDir, "prefix")

	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotRetention:        2,
		SnapshotCloudDestination: cloudDest,
	}, testManagerBlobPlugin, "sqlite", testDestinationRegistry, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	for epoch := uint64(1); epoch <= 3; epoch++ {
		publishEpochTransition(eb, epoch)
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(
				cloudPrefixDir,
				"epoch-"+strconv.FormatUint(epoch, 10),
				"manifest.json",
			))
			return err == nil
		}, 5*time.Second, 10*time.Millisecond)
	}

	// epoch-1 is beyond retention (2): pruning must have attempted to
	// delete its cloud mirror, failed (flakyDeleteCloudFail starts true),
	// and therefore left the local copy in place -- neither gone.
	require.Eventually(t, func() bool {
		_, localErr := os.Stat(filepath.Join(snapshotDir, "epoch-1"))
		return localErr == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.DirExists(
		t,
		filepath.Join(cloudPrefixDir, "epoch-1"),
		"cloud mirror must survive a failed delete attempt too, not just the local copy",
	)

	// Clear the transient outage and redeliver a later epoch's transition
	// (the same "restart or bus redelivery" trigger
	// TestManagerRetriesCloudMirrorAfterTransientFailureOnRedeliveredEvent
	// above uses) so pruning runs again and retries epoch-1's cloud delete.
	flakyDeleteCloudMu.Lock()
	flakyDeleteCloudFail = false
	flakyDeleteCloudMu.Unlock()

	publishEpochTransition(eb, 4)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(
			cloudPrefixDir,
			"epoch-4",
			"manifest.json",
		))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		_, localErr := os.Stat(filepath.Join(snapshotDir, "epoch-1"))
		_, cloudErr := os.Stat(filepath.Join(cloudPrefixDir, "epoch-1"))
		return os.IsNotExist(localErr) && os.IsNotExist(cloudErr)
	}, 5*time.Second, 10*time.Millisecond,
		"once the cloud outage clears, a later pruning pass must finish "+
			"removing both the local and cloud copies of the retried epoch",
	)
}
