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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetricsReportTipAndLag(t *testing.T) {
	t.Parallel()
	m, err := NewMetrics(prometheus.NewRegistry(), nil)
	require.NoError(t, err)
	m.Observe(
		indexerdb.Tip{Slot: 1000, BlockNumber: 90},
		ochainsync.Tip{Point: ocommon.Point{Slot: 1260}, BlockNumber: 100},
	)
	require.InDelta(t, 1000, promtestutil.ToFloat64(m.tipSlot), 0)
	require.InDelta(t, 10, promtestutil.ToFloat64(m.lagBlocks), 0)
	require.InDelta(t, 260, promtestutil.ToFloat64(m.lagSeconds), 0,
		"without a slot clock a slot counts as one second")

	// An indexer past the ledger tip, as after a ledger rollback, is not behind.
	m.Observe(
		indexerdb.Tip{Slot: 1300, BlockNumber: 105},
		ochainsync.Tip{Point: ocommon.Point{Slot: 1260}, BlockNumber: 100},
	)
	require.Zero(t, promtestutil.ToFloat64(m.lagBlocks))
	require.Zero(t, promtestutil.ToFloat64(m.lagSeconds))
}

func TestMetricsLagSecondsUseTheSlotClock(t *testing.T) {
	t.Parallel()
	epoch := time.Unix(0, 0)
	clock := func(slot uint64) (time.Time, error) {
		return epoch.Add(time.Duration(slot) * 20 * time.Second), nil
	}
	m, err := NewMetrics(prometheus.NewRegistry(), clock)
	require.NoError(t, err)
	m.Observe(
		indexerdb.Tip{Slot: 10, BlockNumber: 1},
		ochainsync.Tip{Point: ocommon.Point{Slot: 13}, BlockNumber: 4},
	)
	require.InDelta(t, 60, promtestutil.ToFloat64(m.lagSeconds), 0)

	failing := func(uint64) (time.Time, error) { return time.Time{}, errors.New("no clock") }
	m, err = NewMetrics(prometheus.NewRegistry(), failing)
	require.NoError(t, err)
	m.Observe(
		indexerdb.Tip{Slot: 10, BlockNumber: 1},
		ochainsync.Tip{Point: ocommon.Point{Slot: 13}, BlockNumber: 4},
	)
	require.InDelta(t, 3, promtestutil.ToFloat64(m.lagSeconds), 0,
		"a clock that cannot answer falls back to slot counting")
}

func TestMetricsRegisterOnceOnASharedRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	first, err := NewMetrics(reg, nil)
	require.NoError(t, err)
	second, err := NewMetrics(reg, nil)
	require.NoError(t, err, "a restarted writer re-registers the same gauges")
	first.Observe(
		indexerdb.Tip{Slot: 5},
		ochainsync.Tip{Point: ocommon.Point{Slot: 5}},
	)
	require.InDelta(t, 5, promtestutil.ToFloat64(second.tipSlot), 0)
}

func TestMetricsCountLostRollbackAncestors(t *testing.T) {
	t.Parallel()
	m, err := NewMetrics(prometheus.NewRegistry(), nil)
	require.NoError(t, err)
	m.NoRollbackAncestor()
	m.NoRollbackAncestor()
	require.InDelta(t, 2, promtestutil.ToFloat64(m.noAncestor), 0)
}
