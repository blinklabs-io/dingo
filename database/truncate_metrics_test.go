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
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
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

	before := collectHistogramCount(t, reg)

	targetBlock := testIndexedBlock(1500, 1, 0x15)
	require.NoError(t, db.BlockCreate(targetBlock, nil))
	point := ocommon.Point{Slot: 1500, Hash: targetBlock.Hash}
	_, _, err := db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err)

	require.Equal(
		t,
		before+1,
		collectHistogramCount(t, reg),
		"TruncateAfterSlot must record its duration",
	)
}

// TestRegisterTruncateMetricsNilRegistry verifies a nil registry is tolerated,
// matching the other database metric registrations.
func TestRegisterTruncateMetricsNilRegistry(t *testing.T) {
	require.NoError(t, RegisterTruncateMetrics(nil))
}

func collectHistogramCount(t *testing.T, reg *prometheus.Registry) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() !=
			"dingo_database_truncate_after_slot_duration_seconds" {
			continue
		}
		require.NotEmpty(t, family.GetMetric())
		return family.GetMetric()[0].GetHistogram().GetSampleCount()
	}
	t.Fatal("truncation duration histogram not registered")
	return 0
}
