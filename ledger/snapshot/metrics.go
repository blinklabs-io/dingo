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

package snapshot

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type managerMetrics struct {
	captureDurationSeconds    prometheus.Histogram
	captureSuccessTotal       prometheus.Counter
	captureFailureTotal       prometheus.Counter
	capturePoolsTotal         prometheus.Gauge
	captureTotalStakeLovelace prometheus.Gauge
	lastSuccessfulEpoch       prometheus.Gauge
}

func initManagerMetrics(reg prometheus.Registerer) *managerMetrics {
	factory := promauto.With(reg)
	return &managerMetrics{
		captureDurationSeconds: factory.NewHistogram(prometheus.HistogramOpts{
			Name: "cardano_node_metrics_stake_snapshot_capture_duration_seconds",
			Help: "duration of stake snapshot capture runs",
			Buckets: prometheus.ExponentialBuckets(
				0.001, 2, 16,
			), // 1ms to ~32s
		}),
		captureSuccessTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "cardano_node_metrics_stake_snapshot_capture_success_total",
			Help: "successful stake snapshot captures",
		}),
		captureFailureTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "cardano_node_metrics_stake_snapshot_capture_failure_total",
			Help: "failed stake snapshot captures",
		}),
		capturePoolsTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_stake_snapshot_pool_count_int",
			Help: "number of pools in the latest captured snapshot",
		}),
		captureTotalStakeLovelace: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_stake_snapshot_total_active_stake_lovelace",
			Help: "total active stake in the latest captured snapshot",
		}),
		lastSuccessfulEpoch: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_stake_snapshot_last_successful_epoch_int",
			Help: "epoch of the latest successful snapshot capture",
		}),
	}
}
