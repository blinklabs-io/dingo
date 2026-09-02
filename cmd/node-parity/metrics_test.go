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
	"testing"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
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

	families, err := registry.Gather()
	require.NoError(t, err)

	found := false
	for _, fam := range families {
		if fam.GetName() != "node_parity_checks_total" {
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
	assert.True(
		t,
		found,
		"node_parity_checks_total must carry network=\"preview\"",
	)
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
