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
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ErrCommitPauseExceeded marks a Snapshot aborted because its backups
// kept the commit barrier held longer than the limit set with
// WithMaxCommitPause. The commit barrier is released and the partial
// snapshot directory removed before it is returned.
var ErrCommitPauseExceeded = errors.New(
	"snapshot exceeded maximum commit pause",
)

// WithMaxCommitPause bounds how long Snapshot may hold the commit barrier
// once it has acquired it. When the backups are still running at the limit
// they are cancelled, the barrier is released, the partial snapshot is
// removed, and Snapshot returns ErrCommitPauseExceeded. The barrier is
// released once the cancelled backups return, so the hold can exceed the
// limit by however long a backup takes to observe cancellation. Time spent
// waiting to acquire the barrier is not counted: that wait is bounded by the
// caller's context. Zero (the default) means no limit; negative values are
// rejected before any I/O.
func WithMaxCommitPause(limit time.Duration) ManifestOption {
	return func(cfg *manifestConfig) { cfg.maxPause = limit }
}

func commitPauseLimit(opts []ManifestOption) (time.Duration, error) {
	cfg := manifestConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.maxPause < 0 {
		return 0, errors.New("maximum commit pause must be >= 0")
	}
	return cfg.maxPause, nil
}

// Snapshot outcomes recorded in the commit-pause histogram's result label.
const (
	snapshotResultOK       = "ok"
	snapshotResultFailed   = "failed"
	snapshotResultExceeded = "exceeded"
)

type snapshotMetrics struct {
	pause *prometheus.HistogramVec
	bytes *prometheus.CounterVec
}

// snapshotMetricsFor returns the snapshot collectors registered on reg,
// registering them on first use. Collectors are per registry, so databases
// sharing a registry share one series and tests with their own registries
// stay isolated. A nil reg yields collectors that are observed but exposed
// nowhere.
func snapshotMetricsFor(reg prometheus.Registerer) *snapshotMetrics {
	m := &snapshotMetrics{
		pause: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "dingo_snapshot_commit_pause_seconds",
			Help: "time the commit barrier was held by an explicit snapshot",
			Buckets: []float64{
				0.01, 0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600,
			},
		}, []string{"result"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dingo_snapshot_bytes_written_total",
			Help: "snapshot backup bytes written to disk, by store",
		}, []string{"store"}),
	}
	if reg == nil {
		return m
	}
	if err := reg.Register(m.pause); err != nil {
		if already, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			if existing, ok := already.ExistingCollector.(*prometheus.HistogramVec); ok {
				m.pause = existing
			}
		}
	}
	if err := reg.Register(m.bytes); err != nil {
		if already, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
				m.bytes = existing
			}
		}
	}
	return m
}
