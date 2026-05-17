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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	mithrilSyncPhaseBootstrap     = "bootstrap"
	mithrilSyncPhaseLedgerImport  = "ledger_import"
	mithrilSyncPhaseImmutableCopy = "immutable_copy"
	mithrilSyncPhaseGapBlocks     = "gap_blocks"
	mithrilSyncPhasePostLedger    = "post_ledger_state"
	mithrilSyncPhaseBackfill      = "backfill"
	mithrilSyncPhaseComplete      = "complete"
)

type prometheusMetricsServer struct {
	server *http.Server
	errCh  <-chan error
	addr   string
}

func startPrometheusMetricsServerWithHandler(
	logger *slog.Logger,
	bindAddr string,
	port uint,
	component string,
	handler http.Handler,
) (*prometheusMetricsServer, error) {
	addr := net.JoinHostPort(bindAddr, strconv.FormatUint(uint64(port), 10))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("starting metrics listener on %s: %w", addr, err)
	}
	actualAddr := listener.Addr().String()
	logger.Info(
		"serving prometheus metrics on "+actualAddr,
		"component", component,
	)
	server := &http.Server{
		Addr:              actualAddr,
		Handler:           handler,
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		if err := server.Serve(listener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
	}()
	return &prometheusMetricsServer{
		server: server,
		errCh:  errCh,
		addr:   actualAddr,
	}, nil
}

func (s *prometheusMetricsServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *prometheusMetricsServer) Err() <-chan error {
	if s == nil {
		errCh := make(chan error)
		close(errCh)
		return errCh
	}
	return s.errCh
}

type mithrilSyncMetrics struct {
	phaseActive *prometheus.GaugeVec

	startedAt prometheus.Gauge
	completed prometheus.Gauge
	errors    prometheus.Counter

	downloadBytes          prometheus.Gauge
	downloadTotalBytes     prometheus.Gauge
	downloadPercent        prometheus.Gauge
	downloadBytesPerSecond prometheus.Gauge

	snapshotSize                prometheus.Gauge
	snapshotAncillarySize       prometheus.Gauge
	snapshotEpoch               prometheus.Gauge
	snapshotImmutableFileNumber prometheus.Gauge

	ledgerImportCurrent *prometheus.GaugeVec
	ledgerImportTotal   *prometheus.GaugeVec
	ledgerImportPercent *prometheus.GaugeVec
	ledgerStateSlot     *prometheus.GaugeVec

	immutableBlocksCopied prometheus.Gauge
	immutableCurrentSlot  prometheus.Gauge
	immutableTipSlot      prometheus.Gauge
	immutablePercent      prometheus.Gauge
	immutableBlocksPerSec prometheus.Gauge

	gapBlocks prometheus.Gauge
}

func newMithrilSyncMetrics(reg prometheus.Registerer) *mithrilSyncMetrics {
	factory := promauto.With(reg)
	m := &mithrilSyncMetrics{
		phaseActive: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dingo_mithril_sync_phase_active",
				Help: "Whether the Mithril sync phase is currently active.",
			},
			[]string{"phase"},
		),
		startedAt: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_started_at_seconds",
			Help: "Unix timestamp when the Mithril sync command started.",
		}),
		completed: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_completed",
			Help: "Whether the Mithril sync command completed successfully.",
		}),
		errors: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_mithril_sync_errors_total",
			Help: "Total number of Mithril sync command errors.",
		}),
		downloadBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_download_bytes",
			Help: "Bytes downloaded for the current Mithril artifact.",
		}),
		downloadTotalBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_download_total_bytes",
			Help: "Total bytes expected for the current Mithril artifact.",
		}),
		downloadPercent: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_download_percent",
			Help: "Download completion percentage for the current Mithril artifact.",
		}),
		downloadBytesPerSecond: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_download_bytes_per_second",
			Help: "Current Mithril artifact download speed in bytes per second.",
		}),
		snapshotSize: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_snapshot_size_bytes",
			Help: "Selected Mithril snapshot archive size in bytes.",
		}),
		snapshotAncillarySize: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_snapshot_ancillary_size_bytes",
			Help: "Selected Mithril ancillary archive size in bytes.",
		}),
		snapshotEpoch: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_snapshot_epoch",
			Help: "Selected Mithril snapshot epoch.",
		}),
		snapshotImmutableFileNumber: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_snapshot_immutable_file_number",
			Help: "Selected Mithril snapshot immutable file number.",
		}),
		ledgerImportCurrent: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dingo_mithril_sync_ledger_import_current",
				Help: "Current item count for the active ledger-state import stage.",
			},
			[]string{"stage"},
		),
		ledgerImportTotal: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dingo_mithril_sync_ledger_import_total",
				Help: "Total item count for the active ledger-state import stage.",
			},
			[]string{"stage"},
		),
		ledgerImportPercent: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dingo_mithril_sync_ledger_import_percent",
				Help: "Completion percentage for the active ledger-state import stage.",
			},
			[]string{"stage"},
		),
		ledgerStateSlot: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dingo_mithril_sync_ledger_state_slot",
				Help: "Slot of the Mithril ledger state tip.",
			},
			[]string{},
		),
		immutableBlocksCopied: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_immutable_blocks_copied",
			Help: "ImmutableDB blocks copied into the blob store during Mithril sync.",
		}),
		immutableCurrentSlot: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_immutable_current_slot",
			Help: "Current ImmutableDB slot copied into the blob store.",
		}),
		immutableTipSlot: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_immutable_tip_slot",
			Help: "ImmutableDB tip slot for the selected Mithril snapshot.",
		}),
		immutablePercent: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_immutable_copy_percent",
			Help: "Completion percentage for ImmutableDB block copying.",
		}),
		immutableBlocksPerSec: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_immutable_blocks_per_second",
			Help: "ImmutableDB block copy rate during Mithril sync.",
		}),
		gapBlocks: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_mithril_sync_gap_blocks",
			Help: "Volatile gap blocks fetched or reused during Mithril sync.",
		}),
	}
	m.startedAt.SetToCurrentTime()
	return m
}

func newMithrilSyncMetricsHandler(
	network string,
) (*mithrilSyncMetrics, http.Handler) {
	registry := prometheus.NewRegistry()
	metrics := newMithrilSyncMetrics(
		prometheus.WrapRegistererWith(
			prometheus.Labels{"network": network},
			registry,
		),
	)
	handler := promhttp.HandlerFor(
		prometheus.Gatherers{
			prometheus.DefaultGatherer,
			registry,
		},
		promhttp.HandlerOpts{},
	)
	return metrics, handler
}

func (m *mithrilSyncMetrics) setPhaseActive(phase string, active bool) {
	if m == nil {
		return
	}
	value := 0.0
	if active {
		value = 1
	}
	m.phaseActive.WithLabelValues(phase).Set(value)
}

func (m *mithrilSyncMetrics) recordDownloadProgress(
	p mithril.DownloadProgress,
) {
	if m == nil {
		return
	}
	m.downloadBytes.Set(float64(p.BytesDownloaded))
	m.downloadTotalBytes.Set(float64(p.TotalBytes))
	m.downloadPercent.Set(p.Percent)
	m.downloadBytesPerSecond.Set(p.BytesPerSecond)
}

func (m *mithrilSyncMetrics) recordSnapshot(snapshot *mithril.SnapshotListItem) {
	if m == nil || snapshot == nil {
		return
	}
	m.snapshotSize.Set(float64(snapshot.Size))
	m.snapshotAncillarySize.Set(float64(snapshot.AncillarySize))
	m.snapshotEpoch.Set(float64(snapshot.Beacon.Epoch))
	m.snapshotImmutableFileNumber.Set(
		float64(snapshot.Beacon.ImmutableFileNumber),
	)
}

func (m *mithrilSyncMetrics) recordLedgerImportProgress(
	p ledgerstate.ImportProgress,
) {
	if m == nil || p.Stage == "" {
		return
	}
	m.ledgerImportCurrent.WithLabelValues(p.Stage).Set(float64(p.Current))
	m.ledgerImportTotal.WithLabelValues(p.Stage).Set(float64(p.Total))
	percent := p.Percent
	if percent == 0 && p.Total > 0 {
		percent = float64(p.Current) / float64(p.Total) * 100
	}
	m.ledgerImportPercent.WithLabelValues(p.Stage).Set(percent)
}

func (m *mithrilSyncMetrics) recordLedgerStateSlot(slot uint64) {
	if m == nil {
		return
	}
	m.ledgerStateSlot.WithLabelValues().Set(float64(slot))
}

func (m *mithrilSyncMetrics) recordImmutableProgress(
	p node.LoadBlobsProgress,
) {
	if m == nil {
		return
	}
	m.immutableBlocksCopied.Set(float64(p.BlocksCopied))
	m.immutableCurrentSlot.Set(float64(p.CurrentSlot))
	m.immutableTipSlot.Set(float64(p.TipSlot))
	m.immutablePercent.Set(p.Percent)
	m.immutableBlocksPerSec.Set(p.BlocksPerSecond)
}

func (m *mithrilSyncMetrics) recordGapBlocks(count int) {
	if m == nil {
		return
	}
	m.gapBlocks.Set(float64(count))
}

func (m *mithrilSyncMetrics) markComplete() {
	if m == nil {
		return
	}
	m.completed.Set(1)
	m.setPhaseActive(mithrilSyncPhaseComplete, true)
}

func (m *mithrilSyncMetrics) recordError() {
	if m == nil {
		return
	}
	m.errors.Inc()
}
