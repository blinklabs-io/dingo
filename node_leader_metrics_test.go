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

package dingo

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// leaderProducerSeries are the series a producing pool adds for leader
// schedule observability, as name to the result labels it carries.
var leaderProducerSeries = map[string][]string{
	"dingo_leader_scheduled_slots_elapsed_total":    {""},
	"dingo_leader_schedule_epoch":                   {""},
	"dingo_leader_schedule_leader_slots":            {""},
	"dingo_leader_next_slot":                        {""},
	"dingo_forge_slot_battle_outcomes_total":        {"won", "lost"},
	"dingo_forge_remote_kes_signer_not_ready_total": {""},
}

// registerPoolProducer registers what a pool's election and forger register
// on the pool's metrics registry.
func registerPoolProducer(
	t *testing.T,
	reg prometheus.Registerer,
) *forging.BlockForger {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	election := leader.NewElection(
		lcommon.PoolKeyHash{},
		nil,
		nil,
		nil,
		bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	election.SetPromRegistry(reg)
	forger, err := forging.NewBlockForger(forging.ForgerConfig{
		Mode:         forging.ModeDev,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry: reg,
	})
	require.NoError(t, err)
	return forger
}

// The event bus registerPoolProducer creates is released when its test ends.
// Not t.Parallel: goleak compares the goroutines of the whole process.
func TestRegisterPoolProducerReleasesItsEventBus(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	t.Run("register", func(t *testing.T) {
		registerPoolProducer(t, prometheus.NewRegistry())
	})
	goleak.VerifyNone(t, ignore)
}

// leaderSeries gathers the leader-schedule series of reg, keyed by name and
// then by the pool_id and result label values ("" when absent).
func leaderSeries(
	t *testing.T,
	reg *prometheus.Registry,
) map[string]map[[2]string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]map[[2]string]float64{}
	for _, mf := range families {
		if _, ok := leaderProducerSeries[mf.GetName()]; !ok {
			continue
		}
		out[mf.GetName()] = map[[2]string]float64{}
		for _, m := range mf.GetMetric() {
			var key [2]string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "pool_id":
					key[0] = l.GetValue()
				case "result":
					key[1] = l.GetValue()
				default:
					t.Fatalf("unexpected label %q", l.GetName())
				}
			}
			out[mf.GetName()][key] = metricValue(m)
		}
	}
	return out
}

func metricValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	}
	return 0
}

// With several pools each pool gets its own pool_id series of every new
// metric, seven per pool, and none collide.
func TestLeaderScheduleSeriesCarryPoolIDWithSeveralPools(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	n := &Node{config: Config{
		promRegistry: reg,
		blockProducerPools: []BlockProducerPool{
			{ShelleyOperationalCertificate: "/a"},
			{ShelleyOperationalCertificate: "/b"},
		},
	}}
	for _, poolID := range []string{"pool1aaa", "pool1bbb"} {
		registerPoolProducer(t, n.poolMetricsRegistry(poolID))
	}

	series := leaderSeries(t, reg)
	total := 0
	for name, results := range leaderProducerSeries {
		for _, poolID := range []string{"pool1aaa", "pool1bbb"} {
			for _, result := range results {
				_, ok := series[name][[2]string{poolID, result}]
				require.True(t, ok, "%s{pool_id=%q,result=%q}", name, poolID, result)
				total++
			}
		}
		require.Len(t, series[name], len(results)*2,
			"%s has a series per pool and nothing else", name)
	}
	require.Equal(t, 14, total, "seven series per pool")
}

func TestLeaderScheduleSeriesAreUnlabelledWithOnePool(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	n := &Node{config: Config{
		promRegistry: reg,
		blockProducerPools: []BlockProducerPool{
			{ShelleyOperationalCertificate: "/a"},
		},
	}}
	registerPoolProducer(t, n.poolMetricsRegistry("pool1aaa"))

	series := leaderSeries(t, reg)
	for name, results := range leaderProducerSeries {
		require.Len(t, series[name], len(results), name)
		for _, result := range results {
			_, ok := series[name][[2]string{"", result}]
			require.True(t, ok, "%s{result=%q} without pool_id", name, result)
		}
	}
}

// A settled battle is counted on the pool that forged the block, found by the
// block's hash and not by which pool is asked first.
func TestSlotBattleOutcomeIsCountedForThePoolThatForgedTheBlock(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	n := &Node{config: Config{
		promRegistry: reg,
		blockProducerPools: []BlockProducerPool{
			{ShelleyOperationalCertificate: "/a"},
			{ShelleyOperationalCertificate: "/b"},
		},
	}}
	first := registerPoolProducer(t, n.poolMetricsRegistry("pool1aaa"))
	second := registerPoolProducer(t, n.poolMetricsRegistry("pool1bbb"))
	first.SlotTracker().RecordForgedBlock(10, []byte{0x0a})
	second.SlotTracker().RecordForgedBlock(10, []byte{0x0b})
	own := &ownForgedBlocks{}
	own.set([]*forging.BlockForger{first, second})

	require.Nil(t, own.SlotBattleOutcome(10, []byte{0xff}))
	own.SlotBattleOutcome(10, []byte{0x0b})(false)
	own.SlotBattleOutcome(10, []byte{0x0a})(true)
	own.SlotBattleOutcome(10, []byte{0x0b})(false)

	series := leaderSeries(t, reg)["dingo_forge_slot_battle_outcomes_total"]
	require.Equal(t, map[[2]string]float64{
		{"pool1aaa", "won"}:  1,
		{"pool1aaa", "lost"}: 0,
		{"pool1bbb", "won"}:  0,
		{"pool1bbb", "lost"}: 2,
	}, series)
}
