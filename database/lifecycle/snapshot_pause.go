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
	"fmt"
	"reflect"
	"sync"
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
// once it has acquired it, including snapshot-state reads. When work remains at the limit
// backups are cancelled, the barrier is released, the partial snapshot is
// removed, and Snapshot returns ErrCommitPauseExceeded. The barrier is
// released once the cancelled backups return, so the hold can exceed the
// limit by however long a backup takes to observe cancellation. Time spent
// waiting to acquire the barrier is not counted: that wait is bounded by the
// caller's context. Zero (the default) means no limit; negative values are
// rejected before any I/O.
func WithMaxCommitPause(limit time.Duration) ManifestOption {
	return func(cfg *manifestConfig) { cfg.maxPause = limit }
}

func commitPauseConfig(
	opts []ManifestOption,
) (time.Duration, func() time.Time, func(), error) {
	cfg := manifestConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.maxPause < 0 {
		return 0, nil, nil, errors.New("maximum commit pause must be >= 0")
	}
	if cfg.pauseNow == nil {
		cfg.pauseNow = time.Now
	}
	return cfg.maxPause, cfg.pauseNow, cfg.beforePauseAcquire, nil
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

var snapshotMetricRegistryCache = struct {
	sync.Mutex
	byRegistry map[snapshotRegistryIdentity]snapshotMetricsCacheEntry
}{byRegistry: make(map[snapshotRegistryIdentity]snapshotMetricsCacheEntry)}

type snapshotRegistryIdentity struct {
	typ   reflect.Type
	value any
	ptr   uintptr
}

type snapshotMetricsCacheEntry struct {
	registerer prometheus.Registerer
	metrics    *snapshotMetrics
}

func snapshotRegistryKey(reg prometheus.Registerer) (snapshotRegistryIdentity, bool) {
	v := reflect.ValueOf(reg)
	typ := v.Type()
	if v.Comparable() {
		return snapshotRegistryIdentity{typ: typ, value: reg}, true
	}
	if v.Kind() == reflect.Map {
		ptr := v.Pointer()
		if ptr != 0 {
			return snapshotRegistryIdentity{typ: typ, ptr: ptr}, true
		}
	}
	return snapshotRegistryIdentity{}, false
}

func registerSnapshotCollector(
	reg prometheus.Registerer,
	collector prometheus.Collector,
) (prometheus.Collector, bool, error) {
	if err := reg.Register(collector); err != nil {
		already, ok := errors.AsType[prometheus.AlreadyRegisteredError](err)
		if !ok {
			return nil, false, err
		}
		var existing prometheus.Collector
		switch collector.(type) {
		case *prometheus.HistogramVec:
			existing, ok = already.ExistingCollector.(*prometheus.HistogramVec)
		case *prometheus.CounterVec:
			existing, ok = already.ExistingCollector.(*prometheus.CounterVec)
		}
		if !ok {
			return nil, false, fmt.Errorf(
				"existing snapshot collector has unexpected type %T",
				already.ExistingCollector,
			)
		}
		return existing, false, nil
	}
	return collector, true, nil
}

// snapshotMetricsFor returns the snapshot collectors registered on reg,
// registering them on first use. Collectors are per registry, so databases
// sharing a registry share one series and tests with their own registries
// stay isolated. A nil reg yields collectors that are observed but exposed
// nowhere.
func snapshotMetricsFor(reg prometheus.Registerer) (*snapshotMetrics, error) {
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
		return m, nil
	}
	snapshotMetricRegistryCache.Lock()
	defer snapshotMetricRegistryCache.Unlock()
	key, cacheable := snapshotRegistryKey(reg)
	if cacheable {
		if cached, ok := snapshotMetricRegistryCache.byRegistry[key]; ok {
			return cached.metrics, nil
		}
	}
	pause, pauseRegistered, err := registerSnapshotCollector(reg, m.pause)
	if err != nil {
		return nil, fmt.Errorf("register snapshot pause metric: %w", err)
	}
	m.pause = pause.(*prometheus.HistogramVec)
	bytes, _, err := registerSnapshotCollector(reg, m.bytes)
	if err != nil {
		if pauseRegistered {
			reg.Unregister(m.pause)
		}
		return nil, fmt.Errorf("register snapshot bytes metric: %w", err)
	}
	m.bytes = bytes.(*prometheus.CounterVec)
	for _, labels := range []string{snapshotResultOK, snapshotResultFailed, snapshotResultExceeded} {
		m.pause.WithLabelValues(labels)
	}
	for _, store := range []string{"blob", "metadata"} {
		m.bytes.WithLabelValues(store)
	}
	if cacheable {
		snapshotMetricRegistryCache.byRegistry[key] = snapshotMetricsCacheEntry{
			registerer: reg,
			metrics:    m,
		}
	}
	return m, nil
}
