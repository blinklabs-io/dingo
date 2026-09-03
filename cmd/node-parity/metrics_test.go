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
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestParityMetrics builds a parityMetrics registered into a throwaway
// registry, so tests never collide with each other or with the real
// process-wide default registerer (which only allows one registration per
// metric name per process).
func newTestParityMetrics(t *testing.T) (*parityMetrics, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	return newParityMetricsIn("preview", registry), registry
}

// TestNewParityMetricsIn_LabelsWithNetwork covers the reason this tool
// builds its own wrapped registry instead of registering counters
// directly: every metric it emits must carry a "network" label matching
// the real dingo node's own convention (configWrapPromRegistry), so an
// operator's dashboards and alert rules can filter/group by network the
// same way they already do for cardano_node_metrics_*.
func TestNewParityMetricsIn_LabelsWithNetwork(t *testing.T) {
	metrics, registry := newTestParityMetrics(t)
	metrics.checksTotal.Inc()
	metrics.recordSkip(nodeparity.SkipTipMismatch)
	metrics.recordCheck(nodeparity.Diff{ProtocolParamsDiff: "differs"})
	metrics.recordCheckError()

	families, err := registry.Gather()
	require.NoError(t, err)

	for _, name := range []string{
		"node_parity_checks_total",
		"node_parity_checks_skipped_total",
		"node_parity_divergence_total",
		"node_parity_check_errors_total",
	} {
		found := false
		for _, fam := range families {
			if fam.GetName() != name {
				continue
			}
			for _, m := range fam.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "network" &&
						label.GetValue() == "preview" {
						found = true
					}
				}
			}
		}
		assert.True(t, found, "%s must carry network=\"preview\"", name)
	}
}

// TestParityMetrics_RecordCheck_MatchIncrementsOnlyChecksTotal covers a
// clean match: recordCheck must increment checksTotal, and an empty Diff
// must not increment any divergence field -- a match must never show up
// as a divergence in any field's counter.
func TestParityMetrics_RecordCheck_MatchIncrementsOnlyChecksTotal(
	t *testing.T,
) {
	metrics, _ := newTestParityMetrics(t)
	metrics.recordCheck(nodeparity.Diff{})

	assert.Equal(t, float64(1), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			metrics.divergenceTotal.WithLabelValues("protocol_params"),
		),
	)
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			metrics.divergenceTotal.WithLabelValues("stake_distribution"),
		),
	)
	assert.Equal(
		t, float64(0),
		promtestutil.ToFloat64(metrics.divergenceTotal.WithLabelValues("utxo")),
	)
}

// TestParityMetrics_RecordCheck_DivergenceIncrementsOnlyAffectedFields
// covers a diff that touches only some fields: recordCheck must increment
// checksTotal (a divergence is still a completed check, not a skip) and
// the divergence counter for exactly the fields that actually differed --
// here protocol params and UTxO, but not stake distribution -- so an
// operator reading node_parity_divergence_total{field=...} can tell which
// field regressed without reading logs.
func TestParityMetrics_RecordCheck_DivergenceIncrementsOnlyAffectedFields(
	t *testing.T,
) {
	metrics, _ := newTestParityMetrics(t)
	metrics.recordCheck(nodeparity.Diff{
		ProtocolParamsDiff: "protocol parameters differ",
		UTxO:               []string{"utxo abc#0 differs"},
	})

	assert.Equal(t, float64(1), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			metrics.divergenceTotal.WithLabelValues("protocol_params"),
		),
	)
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			metrics.divergenceTotal.WithLabelValues("stake_distribution"),
		),
	)
	assert.Equal(
		t, float64(1),
		promtestutil.ToFloat64(metrics.divergenceTotal.WithLabelValues("utxo")),
	)
}

// TestParityMetrics_RecordCheck_StakeDistributionDivergenceIncrements
// covers the one divergence field the other recordCheck tests never
// exercise on the positive path (both only assert it stays 0): a
// stake-distribution-only diff must increment
// node_parity_divergence_total{field="stake_distribution"} without
// touching the other two fields.
func TestParityMetrics_RecordCheck_StakeDistributionDivergenceIncrements(
	t *testing.T,
) {
	metrics, _ := newTestParityMetrics(t)
	metrics.recordCheck(nodeparity.Diff{
		StakeDistribution: []string{"pool abc fraction differs"},
	})

	assert.Equal(t, float64(1), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			metrics.divergenceTotal.WithLabelValues("stake_distribution"),
		),
	)
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			metrics.divergenceTotal.WithLabelValues("protocol_params"),
		),
	)
	assert.Equal(
		t, float64(0),
		promtestutil.ToFloat64(metrics.divergenceTotal.WithLabelValues("utxo")),
	)
}

// TestNewParityMetricsIn_PreMaterializesZeroSeries covers the fix for
// NodeParityNotChecking's original blind spot: a CounterVec exposes no
// series at all for a label value until something touches it (increments
// it, or even just looks it up via WithLabelValues) -- so before this fix,
// a network that had never skipped a check reported no
// checks_skipped_total series whatsoever, not a zero one. The alert
// expression sums rate(checks_total) with
// sum(rate(checks_skipped_total)), and a binary operator between two
// instant vectors drops any series missing from either side, so a
// completely healthy run (zero skips, ever) made the whole expression
// return no data instead of a real 0, and the alert could never fire even
// while the tool was dead. newParityMetricsIn must pre-materialize every
// known reason (and, for dashboard consistency, every divergence field) at
// construction time so each one already has a real 0 sample from process
// start, before recordSkip/recordCheck or a test's own WithLabelValues
// call is ever made -- this reads back via registry.Gather() rather than
// metrics.checksSkippedTotal.WithLabelValues(...), since calling
// WithLabelValues from the test would itself lazily create the series and
// mask exactly the bug this test exists to catch (verified: an untouched
// CounterVec's family is absent from Gather() entirely, not merely empty).
func TestNewParityMetricsIn_PreMaterializesZeroSeries(t *testing.T) {
	_, registry := newTestParityMetrics(t)

	families, err := registry.Gather()
	require.NoError(t, err)

	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, fam := range families {
		byName[fam.GetName()] = fam
	}

	skipped := byName["node_parity_checks_skipped_total"]
	require.NotNil(
		t, skipped,
		"node_parity_checks_skipped_total must already be exposed before any skip is recorded",
	)
	gotReasons := make(map[string]float64, len(skipped.GetMetric()))
	for _, m := range skipped.GetMetric() {
		for _, label := range m.GetLabel() {
			if label.GetName() == "reason" {
				gotReasons[label.GetValue()] = m.GetCounter().GetValue()
			}
		}
	}
	for _, reason := range []string{
		nodeparity.SkipTipMismatch, nodeparity.SkipTipAdvanced,
	} {
		value, ok := gotReasons[reason]
		assert.True(t, ok, "reason %q must already be exposed", reason)
		assert.Equal(t, float64(0), value, "reason %q must start at 0", reason)
	}

	divergence := byName["node_parity_divergence_total"]
	require.NotNil(
		t, divergence,
		"node_parity_divergence_total must already be exposed before any divergence is recorded",
	)
	gotFields := make(map[string]float64, len(divergence.GetMetric()))
	for _, m := range divergence.GetMetric() {
		for _, label := range m.GetLabel() {
			if label.GetName() == "field" {
				gotFields[label.GetValue()] = m.GetCounter().GetValue()
			}
		}
	}
	for _, field := range []string{
		"protocol_params", "stake_distribution", "utxo",
	} {
		value, ok := gotFields[field]
		assert.True(t, ok, "field %q must already be exposed", field)
		assert.Equal(t, float64(0), value, "field %q must start at 0", field)
	}
}

// discardLogger returns a logger that writes nowhere, for tests that only
// care about serveMetrics's return value.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestServeMetrics_BindFailureReturnsError covers the fix for a metrics
// server that silently failed to bind: serveMetrics must return the bind
// error synchronously (net.Listen happens before the background goroutine
// starts) rather than only ever logging it from that goroutine while
// watchRun carries on as if --metrics-addr were actually serving. Occupies
// the address first so the second bind genuinely fails on "address already
// in use", not a made-up error.
func TestServeMetrics_BindFailureReturnsError(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()

	_, err = serveMetrics(occupied.Addr().String(), discardLogger())
	require.Error(t, err)
}

// TestServeMetrics_SucceedsAndServesMetrics covers the ordinary path:
// serveMetrics must return a working server whose /metrics endpoint
// actually responds, not only ever be exercised through its failure path.
func TestServeMetrics_SucceedsAndServesMetrics(t *testing.T) {
	srv, err := serveMetrics("127.0.0.1:0", discardLogger())
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	var resp *http.Response
	testutil.WaitForCondition(t, func() bool {
		var getErr error
		resp, getErr = http.Get("http://" + srv.Addr + "/metrics")
		return getErr == nil
	}, 2*time.Second, "metrics server never became reachable")
	require.NotNil(t, resp)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestParityMetrics_RecordCheckError_Increments covers the dedicated
// check-error counter (see NodeParityCheckErrors in
// docs/dashboards/alerts.yaml): recordCheckError must increment
// checkErrorsTotal without touching checksTotal or checksSkippedTotal, so
// a run of persistent check errors (e.g. a misconfigured address) is
// distinguishable both from a completed cycle and from a skipped one.
func TestParityMetrics_RecordCheckError_Increments(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	metrics.recordCheckError()
	metrics.recordCheckError()

	assert.Equal(
		t, float64(2), promtestutil.ToFloat64(metrics.checkErrorsTotal),
	)
	assert.Equal(t, float64(0), promtestutil.ToFloat64(metrics.checksTotal))
}

// TestParityMetrics_RecordSkip_IncrementsByReason covers that skipped
// cycles are tracked separately per reason code (tip_mismatch vs.
// tip_advanced), and that recording one reason does not also bump the
// other -- an operator diagnosing "why do checks keep getting skipped"
// needs the two failure modes distinguishable, not merged into one count.
func TestParityMetrics_RecordSkip_IncrementsByReason(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	metrics.recordSkip(nodeparity.SkipTipMismatch)
	metrics.recordSkip(nodeparity.SkipTipMismatch)
	metrics.recordSkip(nodeparity.SkipTipAdvanced)

	assert.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(
			metrics.checksSkippedTotal.WithLabelValues(
				nodeparity.SkipTipMismatch,
			),
		),
	)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			metrics.checksSkippedTotal.WithLabelValues(
				nodeparity.SkipTipAdvanced,
			),
		),
	)
	assert.Equal(
		t, float64(0),
		promtestutil.ToFloat64(metrics.checksTotal),
		"a skipped cycle must never count as a completed one",
	)
}
