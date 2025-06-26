// Copyright 2024 Blink Labs Software
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

package ledger

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type stateMetrics struct {
	blockNum    prometheus.Gauge
	density     prometheus.Gauge
	epochNum    prometheus.Gauge
	slotInEpoch prometheus.Gauge
	slotNum     prometheus.Gauge
	forks       prometheus.Gauge
}

func (m *stateMetrics) init(promRegistry prometheus.Registerer) {
	promautoFactory := promauto.With(promRegistry)
	m.blockNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockNum_int",
		Help: "current block number",
	})
	// TODO: figure out how to calculate this (#390)
	m.density = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_density_real",
		Help: "chain density",
	})
	m.epochNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_epoch_int",
		Help: "current epoch number",
	})
	m.slotInEpoch = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_slotInEpoch_int",
		Help: "current relative slot number in epoch",
	})
	m.slotNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_slotNum_int",
		Help: "current slot number",
	})
	m.forks = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_forks_int",
		Help: "number of forks seen",
	})
}
