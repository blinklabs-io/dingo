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

package koiosparity

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewMetricsNilRegistererIsSafe confirms newMetrics(nil) -- the standalone
// CLI path, and any Observer built without ObserverConfig.PromRegistry set --
// yields a metrics value whose recordResult is a safe no-op rather than a nil
// dereference, matching ledger's stateMetrics nil-tolerance convention.
func TestNewMetricsNilRegistererIsSafe(t *testing.T) {
	t.Parallel()

	m := newMetrics(nil)
	require.Nil(
		t,
		m,
		"a nil registerer must not construct/register any collector",
	)

	// Must not panic on a nil *metrics receiver.
	m.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   10,
		Status:  StatusFail,
		Mismatches: []CheckMismatch{
			{Category: CategoryValueMismatch},
		},
	})
	m.recordResult(nil)
}

// TestRecordResultPass drives one PASS result through a real registry and
// checks every metric this change adds: the result counter, the
// last-checked-epoch gauge, and the mismatch-count gauge (zero, since PASS
// carries no mismatches). lastFailEpoch/lastErrorEpoch must stay at their
// zero value -- a PASS is not "when did this last happen". The vecs
// themselves carry no "network" label: it is supplied as a constant label by
// the node's shared registry (config.go's configWrapPromRegistry), and
// TestNewMetricsWithNetworkConstantLabel below covers that wiring.
func TestRecordResultPass(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	require.NotNil(t, m)

	m.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   100,
		Status:  StatusPass,
	})

	assert.Equal(
		t,
		1.0,
		testutil.ToFloat64(
			m.epochResultTotal.WithLabelValues(ScopeAggregate, "pass"),
		),
	)
	assert.Equal(
		t,
		100.0,
		testutil.ToFloat64(
			m.lastCheckedEpoch.WithLabelValues(ScopeAggregate),
		),
	)
	assert.Equal(
		t,
		0.0,
		testutil.ToFloat64(
			m.epochMismatchCount.WithLabelValues(ScopeAggregate),
		),
	)
	assert.Equal(
		t,
		0,
		testutil.CollectAndCount(m.mismatchTotal),
		"a PASS result records no mismatch rows",
	)
	assert.Equal(
		t,
		0,
		testutil.CollectAndCount(m.lastFailEpoch),
		"lastFailEpoch must not gain a series from a PASS result",
	)
	assert.Equal(
		t,
		0,
		testutil.CollectAndCount(m.lastErrorEpoch),
		"lastErrorEpoch must not gain a series from a PASS result",
	)
}

// TestRecordResultFailMixedSeverities drives a FAIL result carrying both a
// FAIL-severity mismatch and an informational one, and checks that both are
// counted under their own category/severity labels, epochMismatchCount
// reflects the total row count (not just the significant ones -- it mirrors
// check_epoch_status.mismatch_count exactly), and lastFailEpoch is set to
// this epoch.
func TestRecordResultFailMixedSeverities(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	require.NotNil(t, m)

	m.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   200,
		Status:  StatusFail,
		Mismatches: []CheckMismatch{
			{Category: CategoryValueMismatch},
			{Category: CategoryPoolDeparted}, // informational
		},
	})

	assert.Equal(
		t,
		1.0,
		testutil.ToFloat64(
			m.epochResultTotal.WithLabelValues(ScopeAggregate, "fail"),
		),
	)
	assert.Equal(
		t,
		200.0,
		testutil.ToFloat64(
			m.lastCheckedEpoch.WithLabelValues(ScopeAggregate),
		),
	)
	assert.Equal(
		t,
		2.0,
		testutil.ToFloat64(
			m.epochMismatchCount.WithLabelValues(ScopeAggregate),
		),
		"epochMismatchCount mirrors len(result.Mismatches), including "+
			"informational rows",
	)
	assert.Equal(
		t,
		1.0,
		testutil.ToFloat64(m.mismatchTotal.WithLabelValues(
			ScopeAggregate, CategoryValueMismatch, "fail",
		)),
	)
	assert.Equal(
		t,
		1.0,
		testutil.ToFloat64(m.mismatchTotal.WithLabelValues(
			ScopeAggregate, CategoryPoolDeparted, "informational",
		)),
	)
	assert.Equal(
		t,
		200.0,
		testutil.ToFloat64(
			m.lastFailEpoch.WithLabelValues(ScopeAggregate),
		),
	)
	assert.Equal(
		t,
		0,
		testutil.CollectAndCount(m.lastErrorEpoch),
		"a FAIL result must not touch lastErrorEpoch",
	)
}

// TestRecordResultError drives a synthesized ERROR result (reportError's
// shape: one dingo_db_error mismatch) and checks lastErrorEpoch is set while
// lastFailEpoch is untouched.
func TestRecordResultError(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	require.NotNil(t, m)

	m.recordResult(&EpochCompareResult{
		Network: "preprod",
		Epoch:   300,
		Status:  StatusError,
		Mismatches: []CheckMismatch{
			{Category: CategoryDBError},
		},
	})

	assert.Equal(
		t,
		1.0,
		testutil.ToFloat64(
			m.epochResultTotal.WithLabelValues(ScopeAggregate, "error"),
		),
	)
	assert.Equal(
		t,
		300.0,
		testutil.ToFloat64(
			m.lastErrorEpoch.WithLabelValues(ScopeAggregate),
		),
	)
	assert.Equal(
		t,
		0,
		testutil.CollectAndCount(m.lastFailEpoch),
		"an ERROR result must not touch lastFailEpoch",
	)
}

// TestRecordResultLastFailErrorEpochAreSticky pins the "when did this last
// happen" contract: lastFailEpoch/lastErrorEpoch must NOT
// reset when a later epoch passes. A stale, undiagnosed mismatch must not
// silently disappear from the dashboard just because a later epoch happened
// to pass.
func TestRecordResultLastFailErrorEpochAreSticky(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	require.NotNil(t, m)

	m.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   50,
		Status:  StatusFail,
		Mismatches: []CheckMismatch{
			{Category: CategoryValueMismatch},
		},
	})
	m.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   51,
		Status:  StatusPass,
	})

	assert.Equal(
		t,
		50.0,
		testutil.ToFloat64(
			m.lastFailEpoch.WithLabelValues(ScopeAggregate),
		),
		"lastFailEpoch must stay at the last epoch that failed, "+
			"not reset because epoch 51 passed",
	)
	assert.Equal(
		t,
		51.0,
		testutil.ToFloat64(
			m.lastCheckedEpoch.WithLabelValues(ScopeAggregate),
		),
		"lastCheckedEpoch always advances to the most recently checked epoch",
	)
}

// TestNewMetricsWithNetworkConstantLabel reproduces the node's real wiring
// (config.go's configWrapPromRegistry): every registry the node ever passes
// to newMetrics has already been wrapped with a constant "network" label
// before any collector is registered. A collector descriptor that also
// declares "network" as one of its own variable labels conflicts with that
// constant label, and registerCollector's fallback only recognizes
// AlreadyRegisteredError, so any other registration error hits its
// panic(err) branch (dingo#4723).
func TestNewMetricsWithNetworkConstantLabel(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	wrapped := prometheus.WrapRegistererWith(
		prometheus.Labels{"network": "preview"},
		reg,
	)

	var m *metrics
	require.NotPanics(t, func() {
		m = newMetrics(wrapped)
	}, "newMetrics must not panic against a registry wrapped in a constant "+
		"\"network\" label, which every node registry carries")
	require.NotNil(t, m)

	m.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   100,
		Status:  StatusPass,
	})

	assert.Equal(
		t,
		1.0,
		testutil.ToFloat64(
			m.epochResultTotal.WithLabelValues(ScopeAggregate, "pass"),
		),
	)

	families, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range families {
		if mf.GetName() != "dingo_koiosparity_epoch_result_total" {
			continue
		}
		found = true
		for _, metric := range mf.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, "preview", labels["network"],
				"the constant \"network\" label from the wrapping registry "+
					"must still appear on the gathered series")
			assert.Equal(t, ScopeAggregate, labels["queue"])
			assert.Equal(t, "pass", labels["status"])
		}
	}
	require.True(
		t,
		found,
		"dingo_koiosparity_epoch_result_total not registered",
	)
}
