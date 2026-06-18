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
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMithrilSyncMetricsRecordProgress(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := newMithrilSyncMetrics(reg)

	metrics.setPhaseActive(mithrilSyncPhaseBootstrap, true)
	metrics.recordDownloadProgress(mithril.DownloadProgress{
		BytesDownloaded: 128,
		TotalBytes:      256,
		Percent:         50,
		BytesPerSecond:  64,
	})
	metrics.recordSnapshot(&mithril.SnapshotListItem{
		SnapshotBase: mithril.SnapshotBase{
			Size:          1024,
			AncillarySize: 128,
			Beacon: mithril.Beacon{
				Epoch:               42,
				ImmutableFileNumber: 9001,
			},
		},
	})
	metrics.recordLedgerImportProgress(ledgerstate.ImportProgress{
		Stage:   "utxo",
		Current: 5,
		Total:   10,
		Percent: 50,
	})
	metrics.recordLedgerStateSlot(1234)
	metrics.recordImmutableProgress(node.LoadBlobsProgress{
		BlocksCopied:    7,
		CurrentSlot:     1200,
		TipSlot:         1234,
		BlocksPerSecond: 3.5,
		Percent:         97.2,
	})
	metrics.recordGapBlocks(3)
	metrics.recordBackfillProgress(node.BackfillProgress{
		Slot:            1200,
		TipSlot:         2400,
		BlocksPerSecond: 25,
		ProgressPercent: 50,
		Stats: dbtypes.BackfillHotPathStats{
			Blocks:                10,
			Txs:                   20,
			Utxos:                 30,
			InputRefs:             40,
			AddressTxs:            50,
			Witnesses:             60,
			Scripts:               70,
			Redeemers:             80,
			BlobTxOffsetWrites:    20,
			BlobUtxoOffsetWrites:  30,
			SkippedUtxoOffsets:    5,
			BlockReadDecode:       100 * time.Millisecond,
			OffsetComputation:     200 * time.Millisecond,
			BlobOffsetWrites:      300 * time.Millisecond,
			SetTransactionBatched: 400 * time.Millisecond,
			ConsumedInputRecovery: 500 * time.Millisecond,
			UtxoAddressLookup:     600 * time.Millisecond,
			AddressIndex:          700 * time.Millisecond,
			FlushBatch:            800 * time.Millisecond,
			CheckpointWrites:      900 * time.Millisecond,
		},
	})
	metrics.recordIndexRebuildDuration(1500 * time.Millisecond)
	metrics.markComplete()
	metrics.recordError()

	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			metrics.phaseActive.WithLabelValues(mithrilSyncPhaseBootstrap),
		),
	)
	require.Equal(t, float64(128), promtestutil.ToFloat64(metrics.downloadBytes))
	require.Equal(t, float64(256), promtestutil.ToFloat64(metrics.downloadTotalBytes))
	require.Equal(t, float64(50), promtestutil.ToFloat64(metrics.downloadPercent))
	require.Equal(
		t,
		float64(64),
		promtestutil.ToFloat64(metrics.downloadBytesPerSecond),
	)
	require.Equal(t, float64(1024), promtestutil.ToFloat64(metrics.snapshotSize))
	require.Equal(
		t,
		float64(128),
		promtestutil.ToFloat64(metrics.snapshotAncillarySize),
	)
	require.Equal(t, float64(42), promtestutil.ToFloat64(metrics.snapshotEpoch))
	require.Equal(
		t,
		float64(9001),
		promtestutil.ToFloat64(metrics.snapshotImmutableFileNumber),
	)
	require.Equal(
		t,
		float64(5),
		promtestutil.ToFloat64(
			metrics.ledgerImportCurrent.WithLabelValues("utxo"),
		),
	)
	require.Equal(
		t,
		float64(10),
		promtestutil.ToFloat64(
			metrics.ledgerImportTotal.WithLabelValues("utxo"),
		),
	)
	require.Equal(
		t,
		float64(50),
		promtestutil.ToFloat64(
			metrics.ledgerImportPercent.WithLabelValues("utxo"),
		),
	)
	require.Equal(
		t,
		float64(1234),
		promtestutil.ToFloat64(metrics.ledgerStateSlot.WithLabelValues()),
	)
	require.Equal(
		t,
		float64(7),
		promtestutil.ToFloat64(metrics.immutableBlocksCopied),
	)
	require.Equal(
		t,
		float64(1200),
		promtestutil.ToFloat64(metrics.immutableCurrentSlot),
	)
	require.Equal(t, float64(1234), promtestutil.ToFloat64(metrics.immutableTipSlot))
	require.Equal(t, float64(97.2), promtestutil.ToFloat64(metrics.immutablePercent))
	require.Equal(
		t,
		float64(3.5),
		promtestutil.ToFloat64(metrics.immutableBlocksPerSec),
	)
	require.Equal(t, float64(3), promtestutil.ToFloat64(metrics.gapBlocks))
	require.Equal(t, float64(1200), promtestutil.ToFloat64(metrics.backfillCurrentSlot))
	require.Equal(t, float64(2400), promtestutil.ToFloat64(metrics.backfillTipSlot))
	require.Equal(t, float64(25), promtestutil.ToFloat64(metrics.backfillBlocksPerSec))
	require.Equal(t, float64(50), promtestutil.ToFloat64(metrics.backfillPercent))
	require.Equal(
		t,
		float64(0.4),
		promtestutil.ToFloat64(
			metrics.backfillStageDuration.WithLabelValues(
				"set_transaction_batched",
			),
		),
	)
	require.Equal(
		t,
		float64(50),
		promtestutil.ToFloat64(
			metrics.backfillIntervalCounts.WithLabelValues("address_txs"),
		),
	)
	require.Equal(
		t,
		float64(5),
		promtestutil.ToFloat64(
			metrics.backfillIntervalCounts.WithLabelValues(
				"skipped_utxo_offsets",
			),
		),
	)
	require.Equal(
		t,
		float64(1.5),
		promtestutil.ToFloat64(metrics.indexRebuildDuration),
	)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.completed))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.errors))
}

func TestStartPrometheusMetricsServerWithHandlerServesMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_mithril_metric",
		Help: "Test metric exposed by the command metrics server.",
	})
	reg.MustRegister(gauge)
	gauge.Set(42)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := startPrometheusMetricsServerWithHandler(
		logger,
		"127.0.0.1",
		0,
		"mithril",
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(ctx))
	})

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + server.addr + "/metrics")
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "test_mithril_metric 42")
}

func TestMithrilSyncMetricsLedgerStateSlotAbsentUntilRecorded(t *testing.T) {
	reg := prometheus.NewRegistry()
	newMithrilSyncMetrics(reg)

	metricFamilies, err := reg.Gather()
	require.NoError(t, err)
	for _, metricFamily := range metricFamilies {
		require.NotEqual(
			t,
			"dingo_mithril_sync_ledger_state_slot",
			metricFamily.GetName(),
		)
	}
}
