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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus metrics for a Client's connection health and
// sign-mode latency. A nil *Metrics is valid everywhere it is used: every
// method is a no-op on a nil receiver, so a Client constructed without
// metrics (Config.Metrics left nil) pays no cost and needs no separate
// guard at every call site.
type Metrics struct {
	connected         prometheus.Gauge
	reconnectFailures prometheus.Counter
	signSuccesses     prometheus.Counter
	signFailures      prometheus.Counter
	signLatency       prometheus.Histogram
}

// NewMetrics registers and returns KES agent client metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)
	return &Metrics{
		connected: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_kes_agent_connected",
			Help: "1 if the KES agent client currently holds a live connection, 0 otherwise",
		}),
		reconnectFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_kes_agent_reconnect_failures_total",
			Help: "KES agent client dial or handshake failures",
		}),
		signSuccesses: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_kes_agent_sign_success_total",
			Help: "sign-mode requests the agent answered with a verified signature",
		}),
		signFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_kes_agent_sign_failure_total",
			Help: "sign-mode requests that errored, timed out, or failed validation",
		}),
		signLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "dingo_kes_agent_sign_latency_seconds",
			Help:    "sign-mode round-trip latency to the KES agent",
			Buckets: prometheus.DefBuckets,
		}),
	}
}

func (m *Metrics) setConnected(connected bool) {
	if m == nil {
		return
	}
	if connected {
		m.connected.Set(1)
	} else {
		m.connected.Set(0)
	}
}

func (m *Metrics) incReconnectFailures() {
	if m == nil {
		return
	}
	m.reconnectFailures.Inc()
}

func (m *Metrics) incSignSuccess() {
	if m == nil {
		return
	}
	m.signSuccesses.Inc()
}

func (m *Metrics) incSignFailure() {
	if m == nil {
		return
	}
	m.signFailures.Inc()
}

func (m *Metrics) observeSignLatency(d time.Duration) {
	if m == nil {
		return
	}
	m.signLatency.Observe(d.Seconds())
}
