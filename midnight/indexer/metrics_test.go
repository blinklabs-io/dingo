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

package indexer

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

// TestRecordBlockEvents_IncrementsBlocksAndLabelsEvents verifies blocksIndexed
// increments once per call and eventsTotal accumulates per type label across
// multiple blocks, including a block with no events at all.
func TestRecordBlockEvents_IncrementsBlocksAndLabelsEvents(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIndexerMetrics(reg)

	m.recordBlockEvents(map[string]int{"create": 2, "spend": 1})
	m.recordBlockEvents(map[string]int{"registration": 1})
	m.recordBlockEvents(map[string]int{}) // a block with no Midnight events

	assert.Equal(t, float64(3), testutil.ToFloat64(m.blocksIndexed))
	assert.Equal(
		t,
		float64(2),
		testutil.ToFloat64(m.eventsTotal.WithLabelValues("create")),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(m.eventsTotal.WithLabelValues("spend")),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(m.eventsTotal.WithLabelValues("registration")),
	)
	// A type never recorded must not appear as a series at all. Checked by
	// count, not by reading WithLabelValues("deregistration") directly --
	// WithLabelValues lazily creates the child series it looks up, so reading
	// one that was never recorded would create it and then trivially observe
	// the zero value it just created, proving nothing.
	assert.Equal(t, 3, testutil.CollectAndCount(m.eventsTotal))
}

// TestRecordBlockEvents_IgnoresNonPositiveCounts verifies a zero count never
// creates a labelled eventsTotal series, while blocksIndexed still increments.
func TestRecordBlockEvents_IgnoresNonPositiveCounts(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newIndexerMetrics(reg)

	// A zero or negative count must not create a labelled series at all --
	// only actually-written events should ever show up under a type label.
	m.recordBlockEvents(map[string]int{"create": 0})

	assert.Equal(t, float64(1), testutil.ToFloat64(m.blocksIndexed))
	assert.Equal(t, 0, testutil.CollectAndCount(m.eventsTotal))
}

// TestNewIndexerMetrics_NilRegistryIsSafe verifies the counters stay usable
// (no panic, correct value) when constructed with a nil Prometheus registry.
func TestNewIndexerMetrics_NilRegistryIsSafe(t *testing.T) {
	m := newIndexerMetrics(nil)
	// Must not panic when there is no registry to register against.
	m.recordBlockEvents(map[string]int{"create": 1})
	assert.Equal(t, float64(1), testutil.ToFloat64(m.blocksIndexed))
}
