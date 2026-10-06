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
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

var errInjectedBackup = errors.New("injected backup failure")

// backupHooks replaces the blob and metadata backup calls. A nil hook runs
// the real backup.
type backupHooks struct {
	blob     func(ctx context.Context, w io.Writer) error
	metadata func(ctx context.Context, dstPath string) error
	read     func() error
}

type hookedBlobStore struct {
	blob.BlobStore
	hooks *backupHooks
}

func (s hookedBlobStore) Backup(ctx context.Context, w io.Writer) error {
	if s.hooks.blob != nil {
		return s.hooks.blob(ctx, w)
	}
	return s.BlobStore.(blob.Backuper).Backup(ctx, w)
}

type hookedMetadataStore struct {
	metadata.MetadataStore
	hooks *backupHooks
}

func (s hookedMetadataStore) GetCommitTimestamp() (int64, error) {
	if s.hooks.read != nil {
		if err := s.hooks.read(); err != nil {
			return 0, err
		}
	}
	return s.MetadataStore.GetCommitTimestamp()
}

func (s hookedMetadataStore) BackupTo(ctx context.Context, dst string) error {
	if s.hooks.metadata != nil {
		return s.hooks.metadata(ctx, dst)
	}
	return s.MetadataStore.(metadata.Backuper).BackupTo(ctx, dst)
}

func newHookedDB(
	t *testing.T,
	reg prometheus.Registerer,
	hooks *backupHooks,
) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{
			DataDir: t.TempDir(), PromRegistry: reg,
		}},
		func(m metadata.MetadataStore) metadata.MetadataStore {
			return hookedMetadataStore{MetadataStore: m, hooks: hooks}
		},
	)
	require.NoError(t, err)
	prev, drain := db.SetBlobStore(
		hookedBlobStore{BlobStore: db.Blob(), hooks: hooks},
	)
	_ = prev
	drain()
	return db
}

func snapshotAt(
	ctx context.Context,
	db *database.Database,
	dir string,
	opts ...lifecycle.ManifestOption,
) (lifecycle.Manifest, error) {
	return lifecycle.Snapshot(
		ctx, db, dir, lifecycle.TriggerManual, "test-version",
		"badger", "sqlite", opts...,
	)
}

// requireBarrierReleased fails unless a read-write transaction can start,
// which it cannot while a Snapshot still holds the commit barrier.
func requireBarrierReleased(t *testing.T, db *database.Database) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resume, err := db.PauseCommitsContext(ctx)
	require.NoError(t, err, "commit barrier still held")
	resume()
}

func gatherFamily(
	t *testing.T,
	reg *prometheus.Registry,
	name string,
) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func metricLabel(mt *dto.Metric, name string) string {
	for _, label := range mt.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

func TestSnapshotMaxCommitPauseAbortsAndReleasesBarrier(t *testing.T) {
	t.Parallel()

	hooks := &backupHooks{
		blob: func(ctx context.Context, _ io.Writer) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	db := newHookedDB(t, nil, hooks)
	dir := filepath.Join(t.TempDir(), "snap")

	// The outer deadline only keeps a regression from hanging the run.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := snapshotAt(
		ctx, db, dir,
		lifecycle.WithMaxCommitPause(50*time.Millisecond),
	)
	require.ErrorIs(t, err, lifecycle.ErrCommitPauseExceeded)
	require.NoDirExists(t, dir)
	requireBarrierReleased(t, db)
}

func TestSnapshotWithoutMaxCommitPauseIsUnbounded(t *testing.T) {
	t.Parallel()

	hooks := &backupHooks{
		blob: func(ctx context.Context, w io.Writer) error {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
			_, err := w.Write([]byte("blob"))
			return err
		},
	}
	db := newHookedDB(t, nil, hooks)
	_, err := snapshotAt(t.Context(), db, filepath.Join(t.TempDir(), "snap"))
	require.NoError(t, err)
}

func TestSnapshotMaxCommitPauseExcludesBarrierWait(t *testing.T) {
	t.Parallel()

	// The backup holds the barrier for a known minimum, so the recorded
	// pause must cover it while still excluding the barrier wait.
	const backupHold = 100 * time.Millisecond
	reg := prometheus.NewRegistry()
	db := newHookedDB(t, reg, &backupHooks{
		blob: func(_ context.Context, w io.Writer) error {
			time.Sleep(backupHold)
			_, err := io.WriteString(w, "blob")
			return err
		},
		metadata: func(_ context.Context, dst string) error {
			return os.WriteFile(dst, []byte("metadata"), 0o600)
		},
	})
	writer := database.NewTxn(db, true)
	rolledBack := false
	defer func() {
		if !rolledBack {
			_ = writer.Rollback()
		}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	dir := filepath.Join(t.TempDir(), "snapshot")
	go func() {
		_, err := snapshotAt(
			ctx, db, dir,
			lifecycle.WithMaxCommitPause(2*time.Second),
		)
		result <- err
	}()

	waitStart := time.Now()
	testutil.RequireNoReceive(
		t, result, 2500*time.Millisecond,
		"snapshot returned while a write transaction held the commit barrier",
	)
	barrierWait := time.Since(waitStart)
	require.NoError(t, writer.Rollback())
	rolledBack = true
	require.NoError(
		t, testutil.RequireReceive(t, result, 5*time.Second, "snapshot completion"),
	)

	pause := gatherFamily(t, reg, "dingo_snapshot_commit_pause_seconds")
	require.NotNil(t, pause)
	for _, metric := range pause.GetMetric() {
		if metricLabel(metric, "result") == "ok" {
			require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
			sum := metric.GetHistogram().GetSampleSum()
			require.GreaterOrEqual(t, sum, backupHold.Seconds())
			require.Less(t, sum, barrierWait.Seconds())
			return
		}
	}
	t.Fatal("successful snapshot pause metric was not recorded")
}

func TestSnapshotRejectsNegativeMaxCommitPause(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	dir := filepath.Join(t.TempDir(), "snap")
	_, err := snapshotAt(
		t.Context(), db, dir, lifecycle.WithMaxCommitPause(-time.Second),
	)
	require.Error(t, err)
	require.NotErrorIs(t, err, lifecycle.ErrCommitPauseExceeded)
	require.NoDirExists(t, dir)
}

func TestSnapshotReleasesBarrierWhenBackupFails(t *testing.T) {
	t.Parallel()

	failBlob := func(context.Context, io.Writer) error {
		return errInjectedBackup
	}
	failMetadata := func(context.Context, string) error {
		return errInjectedBackup
	}
	cases := []struct {
		name  string
		hooks backupHooks
	}{
		{"blob", backupHooks{blob: failBlob}},
		{"metadata", backupHooks{metadata: failMetadata}},
		{"both", backupHooks{blob: failBlob, metadata: failMetadata}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hooks := tc.hooks
			reg := prometheus.NewRegistry()
			db := newHookedDB(t, reg, &hooks)
			dir := filepath.Join(t.TempDir(), "snap")
			_, err := snapshotAt(t.Context(), db, dir)
			require.ErrorIs(t, err, errInjectedBackup)
			require.NoDirExists(t, dir)
			requireBarrierReleased(t, db)
			pause := gatherFamily(t, reg, "dingo_snapshot_commit_pause_seconds")
			require.NotNil(t, pause)
			results := map[string]uint64{}
			for _, mt := range pause.GetMetric() {
				results[metricLabel(mt, "result")] = mt.GetHistogram().GetSampleCount()
			}
			require.Equal(t, uint64(1), results["failed"])
		})
	}
}

func TestSnapshotReleasesBarrierOnCancellation(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	hooks := &backupHooks{
		blob: func(ctx context.Context, _ io.Writer) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	db := newHookedDB(t, nil, hooks)
	dir := filepath.Join(t.TempDir(), "snap")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := snapshotAt(ctx, db, dir)
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("blob backup did not start before the deadline")
	}
	cancel()
	var err error
	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not return after cancellation")
	}
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, lifecycle.ErrCommitPauseExceeded)
	require.NoDirExists(t, dir)
	requireBarrierReleased(t, db)
}

func TestSnapshotRecordsCommitPauseAndBytesMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	db := newHookedDB(t, reg, &backupHooks{})
	m, err := snapshotAt(t.Context(), db, filepath.Join(t.TempDir(), "snap"))
	require.NoError(t, err)

	pause := gatherFamily(t, reg, "dingo_snapshot_commit_pause_seconds")
	require.NotNil(t, pause, "pause histogram not registered")
	require.Len(t, pause.GetMetric(), 3)
	results := map[string]uint64{}
	for _, mt := range pause.GetMetric() {
		results[metricLabel(mt, "result")] = mt.GetHistogram().GetSampleCount()
	}
	require.Equal(t, map[string]uint64{"ok": 1, "failed": 0, "exceeded": 0}, results)

	written := gatherFamily(t, reg, "dingo_snapshot_bytes_written_total")
	require.NotNil(t, written, "bytes counter not registered")
	got := map[string]float64{}
	for _, mt := range written.GetMetric() {
		got[mt.GetLabel()[0].GetValue()] = mt.GetCounter().GetValue()
	}
	require.Equal(t, map[string]float64{
		"blob":     float64(m.BlobBytes),
		"metadata": float64(m.MetadataBytes),
	}, got)
}

func TestSnapshotCommitPauseMetricExcludesBarrierWait(t *testing.T) {
	t.Parallel()

	const (
		barrierWait = 10 * time.Minute
		backupHold  = 10 * time.Second
		waitTimeout = 5 * time.Second
	)
	type fakeClock struct {
		sync.Mutex
		now time.Time
	}
	clock := &fakeClock{now: time.Unix(1, 0)}
	now := func() time.Time {
		clock.Lock()
		defer clock.Unlock()
		return clock.now
	}
	advance := func(d time.Duration) {
		clock.Lock()
		defer clock.Unlock()
		clock.now = clock.now.Add(d)
	}

	reg := prometheus.NewRegistry()
	backupStarted := make(chan struct{})
	finishBackup := make(chan struct{})
	hooks := &backupHooks{
		blob: func(_ context.Context, w io.Writer) error {
			close(backupStarted)
			<-finishBackup
			_, err := w.Write([]byte("blob"))
			return err
		},
		metadata: func(_ context.Context, dst string) error {
			return os.WriteFile(dst, []byte("metadata"), 0o600)
		},
	}
	db := newHookedDB(t, reg, hooks)
	dir := filepath.Join(t.TempDir(), "snap")
	resume, err := db.PauseCommitsContext(t.Context())
	require.NoError(t, err)
	var resumeOnce sync.Once
	resumeBarrier := func() { resumeOnce.Do(resume) }
	defer resumeBarrier()
	barrierAttempted := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, snapshotErr := snapshotAt(
			t.Context(), db, dir,
			lifecycle.WithMaxCommitPause(time.Minute),
			lifecycle.WithSnapshotPauseClockForTest(now, func() {
				close(barrierAttempted)
			}),
		)
		result <- snapshotErr
	}()

	testutil.RequireReceive(
		t, barrierAttempted, waitTimeout,
		"snapshot must attempt the held commit barrier",
	)
	advance(barrierWait)
	resumeBarrier()
	testutil.RequireReceive(
		t, backupStarted, waitTimeout,
		"snapshot backup must start after the barrier is acquired",
	)
	advance(backupHold)
	close(finishBackup)
	err = testutil.RequireReceive(
		t, result, waitTimeout, "snapshot completion",
	)
	require.NoError(t, err)

	pause := gatherFamily(t, reg, "dingo_snapshot_commit_pause_seconds")
	require.NotNil(t, pause)
	var success *dto.Histogram
	for _, mt := range pause.GetMetric() {
		if metricLabel(mt, "result") == "ok" {
			success = mt.GetHistogram()
			break
		}
	}
	require.NotNil(t, success)
	require.Equal(t, uint64(1), success.GetSampleCount())
	require.Equal(t, backupHold.Seconds(), success.GetSampleSum())
}

func TestSnapshotRecordsPauseResultOnFailure(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	hooks := &backupHooks{
		blob: func(ctx context.Context, _ io.Writer) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	db := newHookedDB(t, reg, hooks)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := snapshotAt(
		ctx, db, filepath.Join(t.TempDir(), "snap"),
		lifecycle.WithMaxCommitPause(20*time.Millisecond),
	)
	require.ErrorIs(t, err, lifecycle.ErrCommitPauseExceeded)

	pause := gatherFamily(t, reg, "dingo_snapshot_commit_pause_seconds")
	require.NotNil(t, pause)
	require.Len(t, pause.GetMetric(), 3)
	results := map[string]uint64{}
	for _, mt := range pause.GetMetric() {
		results[metricLabel(mt, "result")] = mt.GetHistogram().GetSampleCount()
	}
	require.Equal(t, uint64(1), results["exceeded"])
}

func TestSnapshotReusesMetricsForWrappedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	wrapped := prometheus.WrapRegistererWith(prometheus.Labels{"network": "test"}, reg)
	db := newHookedDB(t, wrapped, &backupHooks{})
	for i := range 2 {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("snap-%d", i))
		_, err := snapshotAt(t.Context(), db, dir)
		require.NoError(t, err)
	}
	pause := gatherFamily(t, reg, "dingo_snapshot_commit_pause_seconds")
	require.NotNil(t, pause)
	var count uint64
	for _, mt := range pause.GetMetric() {
		if metricLabel(mt, "network") == "test" && metricLabel(mt, "result") == "ok" {
			count = mt.GetHistogram().GetSampleCount()
		}
	}
	require.Equal(t, uint64(2), count)
}

func TestSnapshotRejectsSuccessfulBackupAfterPauseDeadline(t *testing.T) {
	t.Parallel()
	db := newHookedDB(t, nil, &backupHooks{blob: func(ctx context.Context, _ io.Writer) error {
		<-ctx.Done()
		return nil
	}})
	dir := filepath.Join(t.TempDir(), "snapshot")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := snapshotAt(ctx, db, dir, lifecycle.WithMaxCommitPause(30*time.Millisecond))
	require.ErrorIs(t, err, lifecycle.ErrCommitPauseExceeded)
	require.NoDirExists(t, dir)
	requireBarrierReleased(t, db)
}

func TestSnapshotBoundsBlockedStateRead(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	finished := make(chan struct{})
	hooks := &backupHooks{}
	db := newHookedDB(t, nil, hooks)
	// The deadline starts after setup: opening the database under a loaded
	// -race run can outlast it, leaving the read hook never entered.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hooks.read = func() error {
		select {
		case <-release:
			close(finished)
			return nil
		case <-ctx.Done():
			close(finished)
			return ctx.Err()
		}
	}
	dir := filepath.Join(t.TempDir(), "snapshot")
	_, err := snapshotAt(ctx, db, dir, lifecycle.WithMaxCommitPause(30*time.Millisecond))
	close(release)
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("snapshot state reader did not exit")
	}
	require.ErrorIs(t, err, lifecycle.ErrCommitPauseExceeded)
	require.NoDirExists(t, dir)
	requireBarrierReleased(t, db)
}
