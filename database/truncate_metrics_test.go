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

package database

import (
	"bytes"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTruncateAfterSlotObservesDuration verifies the rollback-truncation sweep
// is measured. Without this the only symptom of a multi-second truncation is
// the ledger going silent, which is what made a ~90s chain freeze invisible.
func TestTruncateAfterSlotObservesDuration(t *testing.T) {
	db := newTestDB(t)

	reg := prometheus.NewRegistry()
	require.NoError(t, RegisterTruncateMetrics(reg))
	// Registering a second registry must not panic or error.
	require.NoError(t, RegisterTruncateMetrics(prometheus.NewRegistry()))

	before := collectHistogramCount(t, reg, truncateResultSuccess)

	targetBlock := testIndexedBlock(1500, 1, 0x15)
	require.NoError(t, db.BlockCreate(targetBlock, nil))
	point := ocommon.Point{Slot: 1500, Hash: targetBlock.Hash}
	_, _, err := db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err)

	require.Equal(
		t,
		before+1,
		collectHistogramCount(t, reg, truncateResultSuccess),
		"TruncateAfterSlot must record its duration",
	)
}

// TestTruncateAfterSlotRecordsFailureSeparately verifies a failed sweep is not
// reported as a completed truncation. The duration is still observed -- a sweep
// that failed held the ledger write lock for that long -- but under the failure
// label, and the log line says failed rather than complete.
func TestTruncateAfterSlotRecordsFailureSeparately(t *testing.T) {
	db := newTestDB(t)

	reg := prometheus.NewRegistry()
	require.NoError(t, RegisterTruncateMetrics(reg))
	beforeOK := collectHistogramCount(t, reg, truncateResultSuccess)
	beforeFail := collectHistogramCount(t, reg, truncateResultFailure)

	// A rollback point above slot 0 whose block row does not exist makes
	// TruncateAfterSlot fail when it looks the block up for the new tip.
	point := ocommon.Point{
		Slot: 4242,
		Hash: bytes.Repeat([]byte{0x99}, 32),
	}
	_, _, err := db.TruncateAfterSlot(point, 0, nil)
	require.Error(t, err)

	assert.Equal(
		t,
		beforeOK,
		collectHistogramCount(t, reg, truncateResultSuccess),
		"a failed sweep must not be counted as a success",
	)
	assert.Equal(
		t,
		beforeFail+1,
		collectHistogramCount(t, reg, truncateResultFailure),
		"a failed sweep must still record its duration under the failure label",
	)
}

// TestRegisterTruncateMetricsNilRegistry verifies a nil registry is tolerated,
// matching the other database metric registrations.
func TestRegisterTruncateMetricsNilRegistry(t *testing.T) {
	require.NoError(t, RegisterTruncateMetrics(nil))
}

func collectHistogramCount(
	t *testing.T,
	reg *prometheus.Registry,
	result string,
) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() !=
			"dingo_database_truncate_after_slot_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "result" &&
					label.GetValue() == result {
					return metric.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	// A labelled child that has never been observed is absent from the
	// gathered output, which is a legitimate count of zero.
	return 0
}
