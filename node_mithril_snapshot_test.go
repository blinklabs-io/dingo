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

package dingo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/health"
	"github.com/blinklabs-io/dingo/plugin"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSnapshotSource is a snapshot-mode Mithril half. Each SnapshotState call
// returns the next scripted state, repeating the last; each RestoreSnapshot
// call runs the next scripted restore, repeating the last.
type fakeSnapshotSource struct {
	*fakeMithrilSource
	mu       sync.Mutex
	states   []MithrilSnapshotState
	restores []func(context.Context) (ocommon.Point, error)
	stateN   int
	restoreN int
	// progress, when set, is the restore's progress token; nil reports
	// none, so no attempt counts as getting further.
	progress func() string
}

func (s *fakeSnapshotSource) SnapshotProgress(
	context.Context, *database.Database,
) (string, error) {
	s.mu.Lock()
	progress := s.progress
	s.mu.Unlock()
	if progress == nil {
		return "", nil
	}
	return progress(), nil
}

func (s *fakeSnapshotSource) SnapshotMode() bool { return true }

func (s *fakeSnapshotSource) SnapshotState(
	context.Context, *database.Database,
) (MithrilSnapshotState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[min(s.stateN, len(s.states)-1)]
	s.stateN++
	return state, nil
}

func (s *fakeSnapshotSource) RestoreSnapshot(
	ctx context.Context, _ *database.Database,
) (ocommon.Point, error) {
	s.mu.Lock()
	restore := s.restores[min(s.restoreN, len(s.restores)-1)]
	s.restoreN++
	s.mu.Unlock()
	return restore(ctx)
}

func (s *fakeSnapshotSource) restoreCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restoreN
}

func restoreReturns(
	p ocommon.Point,
	err error,
) func(context.Context) (ocommon.Point, error) {
	return func(context.Context) (ocommon.Point, error) { return p, err }
}

type snapshotStartupHarness struct {
	s       *mithrilSnapshotStartup
	src     *fakeSnapshotSource
	clock   *fakeClock
	waits   []time.Duration
	reports []*mithrilRestoreStatus
}

func newSnapshotStartupHarness(
	src *fakeSnapshotSource,
	gate bool,
) *snapshotStartupHarness {
	h := &snapshotStartupHarness{
		src:   src,
		clock: &fakeClock{now: time.Unix(1_700_000_000, 0)},
	}
	h.s = &mithrilSnapshotStartup{
		source:              src,
		backoff:             10 * time.Minute,
		delegatorInactivity: gate,
		logger:              slog.New(slog.DiscardHandler),
		now:                 h.clock.Now,
		wait: func(_ context.Context, d time.Duration) error {
			h.waits = append(h.waits, d)
			h.clock.Advance(d)
			return nil
		},
		report: func(status *mithrilRestoreStatus) {
			h.reports = append(h.reports, status)
		},
		dataDir: "/data/dingo",
	}
	return h
}

// A restore that fails before writing, such as on a corrupted ancillary
// manifest signature, leaves the database fresh: the node goes on to sync it
// from genesis, and the coordinator holds off for the retry backoff.
func TestSnapshotModeFallsBackToGenesis(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotFresh},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, errors.New(
				"verifying ancillary manifest: bad signature",
			)),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	until, err := h.s.run(t.Context())
	require.NoError(t, err, "a failed restore must not stop the node")
	assert.Equal(t, 1, src.restoreCount(), "retried before going live")
	assert.Empty(t, h.waits)
	assert.Equal(t, h.clock.Now().Add(10*time.Minute), until)
}

// A fresh-database restore that fails because the aggregator is unavailable
// is retried a few times, a short backoff apart, before snapshot mode is
// given up for genesis: an aggregator blip at first boot must not turn a
// history-expiry node into a full-history replay.
func TestSnapshotModeRetriesFreshRestoreOnOutage(t *testing.T) {
	t.Parallel()
	outage := restoreReturns(ocommon.Point{}, fmt.Errorf(
		"listing artifacts: %w", ErrMithrilRestoreUnavailable,
	))
	t.Run("recovers", func(t *testing.T) {
		t.Parallel()
		tip := ocommon.NewPoint(9, nil)
		src := &fakeSnapshotSource{
			states: []MithrilSnapshotState{MithrilSnapshotFresh},
			restores: []func(context.Context) (ocommon.Point, error){
				outage, outage, restoreReturns(tip, nil),
			},
		}
		h := newSnapshotStartupHarness(src, false)
		until, err := h.s.run(t.Context())
		require.NoError(t, err)
		assert.True(t, until.IsZero(), "a restored node is not suppressed")
		assert.Equal(t, 3, src.restoreCount())
		assert.Equal(t, []time.Duration{
			freshSnapshotRestoreBackoff, freshSnapshotRestoreBackoff,
		}, h.waits)
	})
	t.Run("falls back", func(t *testing.T) {
		t.Parallel()
		src := &fakeSnapshotSource{
			states: []MithrilSnapshotState{MithrilSnapshotFresh},
			restores: []func(context.Context) (ocommon.Point, error){
				outage,
			},
		}
		h := newSnapshotStartupHarness(src, false)
		until, err := h.s.run(t.Context())
		require.NoError(t, err, "an outage must not stop the node")
		assert.Equal(t, freshSnapshotRestoreRetries+1, src.restoreCount())
		assert.Len(t, h.waits, freshSnapshotRestoreRetries)
		assert.Equal(t, h.clock.Now().Add(10*time.Minute), until)
	})
}

// A restore that failed after writing is resumed after the backoff, not
// abandoned to Ouroboros on a partly restored database.
func TestSnapshotModeResumesPartialRestore(t *testing.T) {
	t.Parallel()
	tip := ocommon.NewPoint(999, []byte{1})
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, errors.New("disk full")),
			restoreReturns(tip, nil),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	until, err := h.s.run(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, src.restoreCount())
	assert.Equal(t, []time.Duration{10 * time.Minute}, h.waits)
	assert.True(t, until.IsZero(), "a finished restore suppresses nothing")
}

func TestSnapshotModeLeavesPopulatedDatabase(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotPopulated},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, errors.New("unexpected restore")),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	until, err := h.s.run(t.Context())
	require.NoError(t, err)
	assert.Zero(t, src.restoreCount())
	assert.True(t, until.IsZero())
}

// The CIP-0163 gate keeps a fresh database off the snapshot path, as
// `dingo mithril sync` refuses with it set.
func TestSnapshotModeInactivityGate(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotFresh},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, errors.New("unexpected restore")),
		},
	}
	until, err := newSnapshotStartupHarness(src, true).s.run(t.Context())
	require.NoError(t, err)
	assert.Zero(t, src.restoreCount())
	assert.True(t, until.IsZero())
}

// A resume that keeps failing, such as against an artifact the aggregator
// no longer serves, is retried a bounded number of times, reported as stuck
// meanwhile, and then fails the startup naming the recovery.
func TestSnapshotModeCapsFailedResumes(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, errors.New("artifact gone")),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	_, err := h.s.run(t.Context())
	require.Error(t, err)
	assert.ErrorContains(t, err, "remove the data directory /data/dingo")
	assert.ErrorContains(t, err, "artifact gone")
	assert.Equal(t, maxSnapshotRestoreAttempts, src.restoreCount())
	assert.Len(t, h.waits, maxSnapshotRestoreAttempts-1)

	require.NotEmpty(t, h.reports)
	assert.Zero(t, h.reports[0].failures, "first reported as stuck")
	last := h.reports[len(h.reports)-1]
	assert.Equal(t, maxSnapshotRestoreAttempts, last.failures)

	n := &Node{}
	require.NoError(t, n.MithrilSnapshotRestoreReady())
	n.mithrilRestore.Store(h.reports[0])
	assert.ErrorIs(t, n.MithrilSnapshotRestoreReady(), errMithrilRestoreInProgress)
	n.mithrilRestore.Store(last)
	assert.ErrorIs(t, n.MithrilSnapshotRestoreReady(), errMithrilRestoreStuck)
}

// An aggregator outage during a resume is not the restore being stuck: the
// attempts it fails are retried after the backoff for as long as it lasts,
// never counted toward the cap, and never end the startup with advice to
// discard the database.
func TestSnapshotModeOutageDoesNotCountTowardCap(t *testing.T) {
	t.Parallel()
	outage := restoreReturns(ocommon.Point{}, fmt.Errorf(
		"certificate request timed out: %w", ErrMithrilRestoreUnavailable,
	))
	restores := make(
		[]func(context.Context) (ocommon.Point, error),
		0, 3*maxSnapshotRestoreAttempts+1,
	)
	for range 3 * maxSnapshotRestoreAttempts {
		restores = append(restores, outage)
	}
	tip := ocommon.NewPoint(9, nil)
	restores = append(restores, restoreReturns(tip, nil))
	src := &fakeSnapshotSource{
		states:   []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: restores,
	}
	h := newSnapshotStartupHarness(src, false)
	_, err := h.s.run(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 3*maxSnapshotRestoreAttempts+1, src.restoreCount())
	assert.Len(t, h.waits, 3*maxSnapshotRestoreAttempts)
	for _, status := range h.reports {
		assert.Zero(t, status.failures, "an outage was reported as stuck")
	}
}

// The recovery advice reaches every store: removing the data directory
// empties the local plugins' stores but not a remote database or bucket.
func TestMithrilRestoreRecoveryNamesStores(t *testing.T) {
	t.Parallel()
	local := mithrilRestoreRecovery("/data/dingo", "", "")
	assert.Equal(t, local, mithrilRestoreRecovery(
		"/data/dingo", "sqlite", "badger",
	))
	assert.Contains(t, local, "remove the data directory /data/dingo")

	remote := mithrilRestoreRecovery("/data/dingo", "postgres", "s3")
	assert.Contains(t, remote, "postgres metadata database")
	assert.Contains(t, remote, "s3 blob store")
	assert.Contains(t, remote, "remove the data directory /data/dingo")

	mixed := mithrilRestoreRecovery("/data/dingo", "mysql", "badger")
	assert.Contains(t, mixed, "mysql metadata database")
	assert.NotContains(t, mixed, "blob store")

	h := newSnapshotStartupHarness(&fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, ErrMithrilRestoreUnrecoverable),
		},
	}, false)
	h.s.metadataPlugin, h.s.blobPlugin = "postgres", "gcs"
	_, err := h.s.run(t.Context())
	require.ErrorIs(t, err, ErrMithrilRestoreUnrecoverable)
	assert.ErrorContains(t, err, "gcs blob store")
}

// The cap counts only attempts that got no further: a restore that keeps
// failing but advances each time, as a long restore through flaky downloads
// does, is retried past it until it completes.
func TestSnapshotModeProgressResetsCap(t *testing.T) {
	t.Parallel()
	failing := restoreReturns(ocommon.Point{}, errors.New("download reset"))
	restores := make(
		[]func(context.Context) (ocommon.Point, error),
		0, 2*maxSnapshotRestoreAttempts,
	)
	for range 2*maxSnapshotRestoreAttempts - 1 {
		restores = append(restores, failing)
	}
	restores = append(restores, restoreReturns(ocommon.NewPoint(9, nil), nil))
	src := &fakeSnapshotSource{
		states:   []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: restores,
	}
	// Each attempt stores more: the token is the attempt count.
	src.progress = func() string { return strconv.Itoa(src.restoreN) }
	h := newSnapshotStartupHarness(src, false)
	_, err := h.s.run(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2*maxSnapshotRestoreAttempts, src.restoreCount())
	for _, r := range h.reports {
		assert.Zero(t, r.failures, "a progressing restore reported stuck")
	}
}

// A restore the source reports unrecoverable, such as one whose ledger
// import began on an artifact the aggregator has since pruned, fails the
// startup at once with the recovery, without retrying.
func TestSnapshotModeUnrecoverableStopsAtOnce(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, fmt.Errorf(
				"%w: pinned artifact pruned", ErrMithrilRestoreUnrecoverable,
			)),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	_, err := h.s.run(t.Context())
	require.ErrorIs(t, err, ErrMithrilRestoreUnrecoverable)
	assert.ErrorContains(t, err, "remove the data directory /data/dingo")
	assert.Equal(t, 1, src.restoreCount())
	assert.Empty(t, h.waits)
}

// While Run is held in the startup restore, readiness reports the restore,
// though the tip gap is not yet known, and the restore-state gauge says
// restoring, then stuck once an attempt failed without progress, then idle.
func TestSnapshotModeRestoreReadinessAndGauge(t *testing.T) {
	t.Parallel()
	n := newAPIPluginRuntimeNode(t)
	n.config.cfg.Mithril.RetryBackoff = time.Millisecond
	starting := func() (uint64, bool) { return 0, false }
	type seen struct {
		reason string
		gauge  float64
	}
	var during []seen
	observe := func() {
		status := health.Evaluate(starting, 1000, health.Check{
			Startup: true,
			Reason:  "mithril_snapshot_restore",
			Fn:      n.MithrilSnapshotRestoreReady,
		})
		during = append(during, seen{
			reason: status.Reason,
			gauge:  promtestutil.ToFloat64(n.mithrilRestoreGauge),
		})
	}
	var stuckErr error
	src := &fakeSnapshotSource{
		fakeMithrilSource: &fakeMithrilSource{},
		states:            []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			func(context.Context) (ocommon.Point, error) {
				observe()
				return ocommon.Point{}, errors.New("disk full")
			},
			func(context.Context) (ocommon.Point, error) {
				observe()
				stuckErr = n.MithrilSnapshotRestoreReady()
				return ocommon.NewPoint(1, []byte{1}), nil
			},
		},
	}
	n.config.mithrilCatchUp = func(MithrilCatchUpParams) (MithrilCatchUp, error) {
		return src, nil
	}
	errProbe := errors.New("probe done")
	probe := &apiLifecycleProbe{startErr: errProbe}
	registerAPIProbe(t, n.pluginHost, plugin.CapabilityAPIBlockfrost, "probe", probe)
	selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)

	require.ErrorIs(t, n.Run(context.Background()), errProbe)
	require.Len(t, during, 2)
	assert.Equal(t, seen{"mithril_snapshot_restore", 1}, during[0])
	assert.Equal(t, seen{"mithril_snapshot_restore", 2}, during[1])
	assert.ErrorIs(t, stuckErr, errMithrilRestoreStuck)
	assert.NoError(t, n.MithrilSnapshotRestoreReady())
	assert.Zero(t, promtestutil.ToFloat64(n.mithrilRestoreGauge))
}

// The node's own interrupted restore with the CIP-0163 gate on refuses to
// start before the node settings gate latches the setting, so starting
// again with the gate off is still allowed and resumes the restore.
func TestSnapshotRestoreGateRefusedBeforeSettingsLatch(t *testing.T) {
	t.Parallel()
	n := newAPIPluginRuntimeNode(t)
	dataDir := n.config.dataDir
	src := &fakeSnapshotSource{
		fakeMithrilSource: &fakeMithrilSource{},
		states:            []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.NewPoint(1, []byte{1}), nil),
		},
	}
	n.config.mithrilCatchUp = func(MithrilCatchUpParams) (MithrilCatchUp, error) {
		return src, nil
	}
	n.config.delegatorInactivityEnabled = true
	n.config.delegatorInactivity = 3
	n.config.cfg.DelegatorInactivityEnabled = true
	n.config.cfg.DelegatorInactivity = 3
	err := n.Run(context.Background())
	require.ErrorContains(t, err, "delegatorInactivityEnabled (CIP-0163)")
	require.ErrorContains(t, err, dataDir)
	assert.Zero(t, src.restoreCount())

	m := newAPIPluginRuntimeNode(t)
	m.config.dataDir = dataDir
	m.config.mithrilCatchUp = n.config.mithrilCatchUp
	errProbe := errors.New("probe done")
	probe := &apiLifecycleProbe{startErr: errProbe}
	registerAPIProbe(t, m.pluginHost, plugin.CapabilityAPIBlockfrost, "probe", probe)
	selectAPIProbe(m, plugin.CapabilityAPIBlockfrost, "probe", 13000)
	require.ErrorIs(t, m.Run(context.Background()), errProbe,
		"turning the gate off was refused")
	assert.Equal(t, 1, src.restoreCount(), "the restore was not resumed")
}

func TestStartMithrilCoordinatorKeepsStartupBackoff(t *testing.T) {
	t.Parallel()
	until := time.Now().Add(time.Hour)
	n := &Node{
		config:                 Config{logger: slog.New(slog.DiscardHandler)},
		mithrilSource:          &fakeMithrilSource{},
		mithrilSuppressedUntil: until,
	}
	n.config.cfg = &internalconfig.Config{
		Mithril: internalconfig.MithrilConfig{
			Enabled:       true,
			CheckInterval: time.Hour,
			RetryBackoff:  time.Hour,
		},
	}
	stop := n.startMithrilCoordinator(t.Context())
	defer stop()
	require.NotNil(t, n.mithrilCoordinator)
	assert.Equal(t, until, n.mithrilCoordinator.suppressedUntil)
}

// The restore runs inside Run before the chain manager, the ledger state and
// chainsync exist, so no chainsync client can start until it has returned,
// and the ledger is built on the restored database.
func TestSnapshotModeWaitsForBackfill(t *testing.T) {
	t.Parallel()
	n := newAPIPluginRuntimeNode(t)
	var restored atomic.Bool
	var builtDuringRestore []string
	src := &fakeSnapshotSource{
		fakeMithrilSource: &fakeMithrilSource{},
		states:            []MithrilSnapshotState{MithrilSnapshotFresh},
		restores: []func(context.Context) (ocommon.Point, error){
			func(context.Context) (ocommon.Point, error) {
				// Run's own goroutine: nothing else touches these yet.
				if n.chainManager != nil {
					builtDuringRestore = append(builtDuringRestore, "chain")
				}
				if n.ledgerState != nil {
					builtDuringRestore = append(builtDuringRestore, "ledger")
				}
				if n.chainsyncState != nil {
					builtDuringRestore = append(
						builtDuringRestore, "chainsync",
					)
				}
				restored.Store(true)
				return ocommon.NewPoint(1, []byte{1}), nil
			},
		},
	}
	n.config.mithrilCatchUp = func(MithrilCatchUpParams) (MithrilCatchUp, error) {
		return src, nil
	}

	errProbe := errors.New("probe done")
	var chainsyncAfterRestore atomic.Bool
	probe := &apiLifecycleProbe{onStart: func() error {
		chainsyncAfterRestore.Store(restored.Load() && n.chainsyncState != nil)
		return errProbe
	}}
	registerAPIProbe(t, n.pluginHost, plugin.CapabilityAPIBlockfrost, "probe", probe)
	selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)

	require.ErrorIs(t, n.Run(context.Background()), errProbe)
	assert.Equal(t, 1, src.restoreCount())
	assert.Empty(t, builtDuringRestore,
		"built before the restore returned")
	assert.True(t, chainsyncAfterRestore.Load(),
		"chainsync was not built after the restore")
}

// observedRestore records the gauge and readiness answer after each status
// the startup loop publishes.
type observedRestore struct {
	gauge     []float64
	readiness []string
}

func observeRestoreReports(
	t *testing.T, h *snapshotStartupHarness,
) (*Node, *observedRestore) {
	t.Helper()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "restore_state"})
	n := &Node{mithrilRestoreGauge: gauge}
	obs := &observedRestore{}
	h.s.report = func(status *mithrilRestoreStatus) {
		n.reportMithrilRestore(status)
		obs.gauge = append(obs.gauge, promtestutil.ToFloat64(gauge))
		text := ""
		if err := n.MithrilSnapshotRestoreReady(); err != nil {
			text = err.Error()
		}
		obs.readiness = append(obs.readiness, text)
	}
	return n, obs
}

func TestSnapshotModeSourceUnavailableReportsWaiting(t *testing.T) {
	t.Parallel()
	outageErr := fmt.Errorf(
		"certificate request timed out: %w", ErrMithrilRestoreUnavailable,
	)
	outage := restoreReturns(ocommon.Point{}, outageErr)
	var token atomic.Value
	token.Store("a")
	src := &fakeSnapshotSource{
		states:   []MithrilSnapshotState{MithrilSnapshotInterrupted},
		progress: func() string { return token.Load().(string) },
		restores: []func(context.Context) (ocommon.Point, error){
			outage, outage,
			// Writes something, then the source drops again.
			func(context.Context) (ocommon.Point, error) {
				token.Store("b")
				return ocommon.Point{}, outageErr
			},
			restoreReturns(ocommon.NewPoint(9, nil), nil),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	n, obs := observeRestoreReports(t, h)
	_, err := h.s.run(t.Context())
	require.NoError(t, err)
	n.reportMithrilRestore(nil)

	// start, then failed twice with no progress (waiting holds across the
	// retry), then progress returns to restoring; the last attempt starts at 1.
	require.Equal(t, []float64{1, 3, 3, 1, 1}, obs.gauge)
	assert.Contains(t, obs.readiness[1], "certificate request timed out")
	assert.Contains(t, obs.readiness[1], "waiting on source")
	assert.Contains(t, obs.readiness[1], "next attempt at 2023-11-14T22:23:20Z")
	assert.Contains(t, obs.readiness[2], "waiting on source")
	assert.Equal(t, "mithril snapshot restore in progress", obs.readiness[3])
	assert.Zero(t, promtestutil.ToFloat64(n.mithrilRestoreGauge))
}

func TestSnapshotModeStuckReadinessCarriesErrorAndNextAttempt(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, errors.New("disk full")),
			restoreReturns(ocommon.NewPoint(9, nil), nil),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	_, obs := observeRestoreReports(t, h)
	_, err := h.s.run(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2.0, obs.gauge[1])
	assert.Contains(t, obs.readiness[1], "disk full")
	assert.Contains(t, obs.readiness[1], "next attempt at ")
}

func TestSnapshotModeReadinessRedactsCredentials(t *testing.T) {
	t.Parallel()
	src := &fakeSnapshotSource{
		states: []MithrilSnapshotState{MithrilSnapshotInterrupted},
		restores: []func(context.Context) (ocommon.Point, error){
			restoreReturns(ocommon.Point{}, fmt.Errorf(
				`Get "https://alice:hunter2@agg.example/aggregator/x?token=abc123": `+
					`dial tcp: no such host: %w`, ErrMithrilRestoreUnavailable,
			)),
			restoreReturns(ocommon.NewPoint(9, nil), nil),
		},
	}
	h := newSnapshotStartupHarness(src, false)
	_, obs := observeRestoreReports(t, h)
	_, err := h.s.run(t.Context())
	require.NoError(t, err)
	text := obs.readiness[1]
	assert.Contains(t, text, "agg.example")
	assert.NotContains(t, text, "alice")
	assert.NotContains(t, text, "hunter2")
	assert.NotContains(t, text, "abc123")
}
