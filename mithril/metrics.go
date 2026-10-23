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

package mithril

import (
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/internal/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

// PipelineMetrics instruments the chunk feed and the stream runner. Every
// method is safe on a nil *PipelineMetrics and then records nothing, so a
// caller without a registry pays only the nil check.
type PipelineMetrics struct {
	windowOccupancy    prometheus.Gauge
	chunksDownloaded   prometheus.Counter
	chunksApplied      prometheus.Counter
	batchCommitSeconds prometheus.Histogram
	staleRestarts      prometheus.Counter
}

// NewPipelineMetrics registers the pipeline metrics on reg. A nil reg returns
// nil metrics. Collectors already registered on reg, by an earlier call on the
// same registry, are reused.
func NewPipelineMetrics(reg prometheus.Registerer) (*PipelineMetrics, error) {
	if reg == nil {
		return nil, nil
	}
	r := promutil.NewRegistration(reg)
	m := &PipelineMetrics{
		windowOccupancy: promutil.Register(r, prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "dingo_mithril_window_chunks",
				Help: "Immutable file trios a windowed Mithril chunk feed " +
					"holds on disk or is downloading, as last reported by " +
					"any open windowed feed (a snapshot probe feed included).",
			},
		)),
		chunksDownloaded: promutil.Register(r, prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dingo_mithril_chunks_downloaded_total",
				Help: "Immutable file trios downloaded and verified against " +
					"the certified digest list, snapshot restore probe " +
					"fetches included.",
			},
		)),
		chunksApplied: promutil.Register(r, prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dingo_mithril_chunks_applied_total",
				Help: "Immutable files whose blocks stream catch-up has " +
					"committed to the ledger.",
			},
		)),
		batchCommitSeconds: promutil.Register(r, prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "dingo_mithril_batch_commit_seconds",
				Help: "Time to apply and commit one stream catch-up batch.",
				Buckets: prometheus.ExponentialBuckets(
					0.05, 2, 12,
				),
			},
		)),
		staleRestarts: promutil.Register(r, prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dingo_mithril_stale_restarts_total",
				Help: "Stream catch-up replays after the applied tip moved " +
					"under a batch.",
			},
		)),
	}
	if err := r.Err(); err != nil {
		r.Rollback()
		return nil, fmt.Errorf("registering mithril pipeline metrics: %w", err)
	}
	return m, nil
}

func (m *PipelineMetrics) setWindowOccupancy(n uint64) {
	if m == nil {
		return
	}
	m.windowOccupancy.Set(float64(n))
}

func (m *PipelineMetrics) chunkDownloaded() {
	if m == nil {
		return
	}
	m.chunksDownloaded.Inc()
}

func (m *PipelineMetrics) chunkApplied() {
	if m == nil {
		return
	}
	m.chunksApplied.Inc()
}

func (m *PipelineMetrics) observeBatchCommit(d time.Duration) {
	if m == nil {
		return
	}
	m.batchCommitSeconds.Observe(d.Seconds())
}

func (m *PipelineMetrics) staleRestart() {
	if m == nil {
		return
	}
	m.staleRestarts.Inc()
}
