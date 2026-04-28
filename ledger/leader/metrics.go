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

package leader

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type electionMetrics struct {
	slotChecksTotal          prometheus.Counter
	slotWonTotal             prometheus.Counter
	slotNotWonTotal          prometheus.Counter
	vrfEvalDurationSeconds   prometheus.Histogram
	stakeLookupDuration      prometheus.Histogram
	lastEpochSlotsChecked    prometheus.Gauge
	lastEpochSlotsWon        prometheus.Gauge
	lastEpochSlotsNotWon     prometheus.Gauge
	lastEvaluatedEpochNumber prometheus.Gauge
}

func initElectionMetrics(reg prometheus.Registerer) *electionMetrics {
	factory := promauto.With(reg)
	return &electionMetrics{
		slotChecksTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "cardano_node_metrics_leader_slot_checks_total",
			Help: "number of leader slot checks executed",
		}),
		slotWonTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "cardano_node_metrics_leader_slot_won_total",
			Help: "number of leader checks that won a slot",
		}),
		slotNotWonTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "cardano_node_metrics_leader_slot_not_won_total",
			Help: "number of leader checks that did not win a slot",
		}),
		vrfEvalDurationSeconds: factory.NewHistogram(prometheus.HistogramOpts{
			Name: "cardano_node_metrics_leader_vrf_eval_duration_seconds",
			Help: "duration spent evaluating VRF over an epoch schedule",
			Buckets: prometheus.ExponentialBuckets(
				0.001, 2, 16,
			), // 1ms to ~32s
		}),
		stakeLookupDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name: "cardano_node_metrics_leader_stake_lookup_duration_seconds",
			Help: "duration spent loading stake data for leader schedule computation",
			Buckets: prometheus.ExponentialBuckets(
				0.0001, 2, 16,
			), // 100us to ~3.2s
		}),
		lastEpochSlotsChecked: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_leader_last_epoch_slots_checked_int",
			Help: "slots evaluated in the most recently computed epoch schedule",
		}),
		lastEpochSlotsWon: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_leader_last_epoch_slots_won_int",
			Help: "winning slots in the most recently computed epoch schedule",
		}),
		lastEpochSlotsNotWon: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_leader_last_epoch_slots_not_won_int",
			Help: "non-winning slots in the most recently computed epoch schedule",
		}),
		lastEvaluatedEpochNumber: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_leader_last_evaluated_epoch_int",
			Help: "most recent epoch for which leader schedule was evaluated",
		}),
	}
}
