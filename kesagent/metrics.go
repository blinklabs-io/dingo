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

package kesagent

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// clientMetrics exposes the agent connection and key state a block-producer
// operator needs to tell a working KES agent from a misconfigured one.
//
// The forging metrics say nothing about it: the node logs a healthy producer
// and reports Forge_about_to_lead on every slot whether or not the agent ever
// answered, so a wrong socket path is invisible until a slot is lost. These
// gauges make that state alertable before the first slot win.
type clientMetrics struct {
	connected       prometheus.Gauge
	keyPresent      prometheus.Gauge
	keyPeriod       prometheus.Gauge
	connectFailures prometheus.Counter
	rejectedPushes  prometheus.Counter
	signFailures    prometheus.Counter
}

func initClientMetrics(reg prometheus.Registerer) *clientMetrics {
	factory := promauto.With(reg)
	return &clientMetrics{
		connected: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_kes_agent_connected",
			Help: "1 while a session with the KES agent is established, 0 otherwise",
		}),
		keyPresent: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_kes_agent_key_present",
			Help: "1 while a KES signing key received from the agent is held (serve-key mode; always 0 in sign mode)",
		}),
		keyPeriod: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_kes_agent_key_period",
			Help: "absolute KES period of the key held from the agent, 0 when none is held",
		}),
		connectFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_kes_agent_connect_failures_total",
			Help: "failed attempts to connect to the KES agent service socket",
		}),
		rejectedPushes: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_kes_agent_rejected_pushes_total",
			Help: "KES key pushes refused by validation against the operational certificate",
		}),
		signFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_kes_agent_sign_failures_total",
			Help: "sign-mode round trips that returned an error (sign mode only)",
		}),
	}
}

// setConnected records whether a session with the agent is currently up.
func (c *Client) setConnectedMetric(up bool) {
	if c.metrics == nil {
		return
	}
	if up {
		c.metrics.connected.Set(1)
		return
	}
	c.metrics.connected.Set(0)
}

// setKeyMetric records the held key's presence and absolute period. period is
// ignored when held is false.
func (c *Client) setKeyMetric(held bool, period uint64) {
	if c.metrics == nil {
		return
	}
	if !held {
		c.metrics.keyPresent.Set(0)
		c.metrics.keyPeriod.Set(0)
		return
	}
	c.metrics.keyPresent.Set(1)
	c.metrics.keyPeriod.Set(float64(period))
}
