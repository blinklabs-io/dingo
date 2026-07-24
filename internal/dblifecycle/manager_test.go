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
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func newManagerTestDB(t *testing.T) *database.Database {
	t.Helper()
	dir := t.TempDir()
	db, err := database.New(&database.Config{
		DataDir:        dir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func publishEpochTransition(eb *event.EventBus, newEpoch uint64) {
	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, event.EpochTransitionEvent{
			PreviousEpoch: newEpoch - 1,
			NewEpoch:      newEpoch,
		}),
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
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 1)
	require.Never(t, func() bool {
		_, err := os.Stat(snapshotDir)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond)
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
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 5)

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-5", "manifest.json"))
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
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 3) // 3 % 2 != 0, must be skipped
	publishEpochTransition(eb, 4) // 4 % 2 == 0, must be captured

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-4", "manifest.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

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
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 7)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-7", "manifest.json"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	// Redeliver the same epoch's event (simulating a restart replay) —
	// must not crash the loop or leave it stuck; a later, new epoch must
	// still be captured afterward.
	publishEpochTransition(eb, 7)
	publishEpochTransition(eb, 8)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-8", "manifest.json"))
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
	}, nil)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	for epoch := uint64(1); epoch <= 3; epoch++ {
		publishEpochTransition(eb, epoch)
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(
				snapshotDir,
				"epoch-"+strconv.FormatUint(epoch, 10),
				"manifest.json",
			))
			return err == nil
		}, 5*time.Second, 10*time.Millisecond)
	}

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-1"))
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond)
	require.DirExists(t, filepath.Join(snapshotDir, "epoch-2"))
	require.DirExists(t, filepath.Join(snapshotDir, "epoch-3"))
}

// managerFakeCloudDestination is a minimal stand-in for a real cloud
// destination (S3/GCS), backed by an ordinary local directory — the same
// pattern used elsewhere (database/lifecycle/destination_test.go,
// bark/database_cloud_test.go), redeclared here since Go test binaries
// are per-package and this scheme registration only needs to exist in
// this one.
type managerFakeCloudDestination struct {
	dir string
}

func (d *managerFakeCloudDestination) UploadDir(_ context.Context, localDir string) error {
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
	return nil
}

func (d *managerFakeCloudDestination) DownloadDir(context.Context, string) error {
	return errors.New("not implemented")
}

func (d *managerFakeCloudDestination) Delete(context.Context) error {
	return os.RemoveAll(d.dir)
}

var (
	_ lifecycle.CloudDestination = &managerFakeCloudDestination{}
	_ lifecycle.CloudDeleter     = &managerFakeCloudDestination{}
)

var (
	managerFakeCloudMu  sync.Mutex
	managerFakeCloudDir string
)

func init() {
	lifecycle.RegisterCloudDestinationScheme(
		"managerfaketest",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			managerFakeCloudMu.Lock()
			base := managerFakeCloudDir
			managerFakeCloudMu.Unlock()
			return &managerFakeCloudDestination{
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

// TestManagerPruningDeletesCloudMirror guards against comment-30's
// original gap: retention pruning only removed the local snapshot
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
	setManagerFakeCloudBackingDir(t, cloudBackingDir)
	const cloudDest = "managerfaketest://bucket/prefix"
	// managerFakeCloudDestination resolves a URI's path directly onto the
	// backing dir (see its registration func above), and cloudDest's own
	// path component is "/prefix" -- so every snapshot this uploads lands
	// under cloudBackingDir/prefix/<snapshotID>, not cloudBackingDir/<snapshotID>.
	cloudPrefixDir := filepath.Join(cloudBackingDir, "prefix")

	m := dblifecycle.NewManager(db, eb, config.DatabaseLifecycleConfig{
		SnapshotEnabled:          true,
		SnapshotDir:              snapshotDir,
		SnapshotEveryNEpochs:     1,
		SnapshotRetention:        2,
		SnapshotCloudDestination: cloudDest,
	}, nil)
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

	// epoch-1 is beyond retention (2): both its local directory and its
	// cloud mirror must be gone.
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-1"))
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(cloudPrefixDir, "epoch-1"))
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond)
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
	lifecycle.RegisterCloudDestinationScheme(
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

func (d panickingCloudDestination) UploadDir(_ context.Context, localDir string) error {
	if filepath.Base(localDir) == d.panicOnDirBase {
		panic("simulated cloud destination bug")
	}
	return nil
}

func (panickingCloudDestination) DownloadDir(context.Context, string) error {
	return errors.New("not implemented")
}

func init() {
	lifecycle.RegisterCloudDestinationScheme(
		"faketestpanic",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return panickingCloudDestination{panicOnDirBase: "epoch-20"}, nil
		},
	)
}

// TestManagerSurvivesHandlerPanic guards against comment-42's original
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
	}, logger)
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
		return strings.Contains(logBuf.String(), "SubscribeFunc handler panicked")
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
		_, err := os.Stat(filepath.Join(snapshotDir, "epoch-21", "manifest.json"))
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
	}, logger)
	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	publishEpochTransition(eb, 9)
	require.Eventually(t, func() bool {
		return strings.Contains(logBuf.String(), "automatic database snapshot failed")
	}, 5*time.Second, 10*time.Millisecond)

	require.NotContains(
		t, logBuf.String(), "already exists, skipping",
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
	lifecycle.RegisterCloudDestinationScheme(
		"faketestblocking",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			blockingCloudMu.Lock()
			defer blockingCloudMu.Unlock()
			return blockingCloudDest, nil
		},
	)
}

// TestManagerStopWaitsForInFlightHandlerAfterExternalContextCancellation
// guards against comment-50's original bug: Start's cleanup goroutine
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
func TestManagerStopWaitsForInFlightHandlerAfterExternalContextCancellation(t *testing.T) {
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
	}, nil)
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
		t, stopDone, time.Second,
		"Stop must return promptly once the in-flight handler actually finishes",
	)
}
