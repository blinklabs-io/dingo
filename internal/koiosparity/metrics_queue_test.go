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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The aggregate and account queues emit independent results for the same
// epochs, and the account queue can lag arbitrarily far behind. A lagging
// account-queue PASS must not overwrite the aggregate queue's latest verdict:
// the mismatch-count stat would go green over an unresolved FAIL, and the
// last-checked epoch would run backwards.
func TestRecordResultQueuesDoNotClobberEachOther(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	require.NotNil(t, m)

	m.recordResult(&EpochCompareResult{
		Network:       "preview",
		Epoch:         500,
		Status:        StatusFail,
		CheckedScopes: []string{ScopeAggregate},
		Mismatches: []CheckMismatch{
			{Category: CategoryValueMismatch},
			{Category: CategoryValueMismatch},
			{Category: CategoryValueMismatch},
		},
	})
	m.recordResult(&EpochCompareResult{
		Network:       "preview",
		Epoch:         300,
		Status:        StatusPass,
		CheckedScopes: AllMismatchScopes,
	})

	expected := `
# HELP dingo_koiosparity_epoch_mismatch_count mismatch row count for the most recently checked epoch, by network (a constant label from the node's shared registry) and queue (mirrors check_epoch_status.mismatch_count)
# TYPE dingo_koiosparity_epoch_mismatch_count gauge
dingo_koiosparity_epoch_mismatch_count{queue="account"} 0
dingo_koiosparity_epoch_mismatch_count{queue="aggregate"} 3
# HELP dingo_koiosparity_last_checked_epoch most recent epoch the koios-parity observer has completed a check for, by network (a constant label from the node's shared registry) and queue
# TYPE dingo_koiosparity_last_checked_epoch gauge
dingo_koiosparity_last_checked_epoch{queue="account"} 300
dingo_koiosparity_last_checked_epoch{queue="aggregate"} 500
`
	assert.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(expected),
		"dingo_koiosparity_epoch_mismatch_count",
		"dingo_koiosparity_last_checked_epoch",
	))
}

// startKoiosParityObserver runs again against the same node registry on a
// live restore/truncate (node_lifecycle.go). A second registration must reuse
// the first collectors rather than panic, and keep counting into them.
func TestNewMetricsReusesCollectorsOnReregistration(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := newMetrics(reg)
	require.NotNil(t, first)
	first.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   10,
		Status:  StatusPass,
	})

	var second *metrics
	require.NotPanics(t, func() { second = newMetrics(reg) })
	require.NotNil(t, second)
	second.recordResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   11,
		Status:  StatusPass,
	})

	expected := `
# HELP dingo_koiosparity_epoch_result_total koios-parity epoch validation results, by network (a constant label from the node's shared registry), queue and status (pass/fail/error)
# TYPE dingo_koiosparity_epoch_result_total counter
dingo_koiosparity_epoch_result_total{queue="aggregate",status="pass"} 2
`
	assert.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(expected),
		"dingo_koiosparity_epoch_result_total",
	))
}
