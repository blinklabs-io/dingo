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

package ouroboros

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// chainsyncMetrics makes chainsync header parking and resyncs visible, both
// of which were previously only in logs.
type chainsyncMetrics struct {
	// deferredHeaders counts headers parked by markDeferredHeaderRecovery
	// because their producer authorization could not be verified yet and the
	// chain does not already hold their block.
	deferredHeaders prometheus.Counter
	// resyncs counts chainsync resync events handled by
	// SubscribeChainsyncResync, by the event's reason string. Every publisher
	// passes a ChainsyncResyncReason* constant or a fixed literal, so label
	// cardinality is bounded.
	resyncs *prometheus.CounterVec
}

func (o *Ouroboros) initChainsyncMetrics() {
	factory := promauto.With(o.registerer)
	o.chainsyncMetrics = &chainsyncMetrics{
		deferredHeaders: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_chainsync_deferred_headers_total",
			Help: "headers parked because producer authorization could not be verified yet, pending a deferred header validation recovery resync",
		}),
		resyncs: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_chainsync_resyncs_total",
				Help: "chainsync resync events published, by reason",
			},
			[]string{"reason"},
		),
	}
	// Read at scrape time so the gauge cannot drift from the map it counts,
	// however many paths add or remove a parked connection.
	factory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_chainsync_deferred_header_connections",
			Help: "connections currently parked awaiting deferred header validation recovery",
		},
		func() float64 {
			o.deferredHeaderRecoveryMu.Lock()
			defer o.deferredHeaderRecoveryMu.Unlock()
			return float64(len(o.deferredHeaderRecovery))
		},
	)
}

// recordDeferredHeaderParked counts one header parked for deferred producer
// authorization recovery. Safe to call when metrics are not initialized.
func (o *Ouroboros) recordDeferredHeaderParked() {
	if o.chainsyncMetrics == nil {
		return
	}
	o.chainsyncMetrics.deferredHeaders.Inc()
}

// recordChainsyncResync counts one chainsync resync event. Safe to call when
// metrics are not initialized.
func (o *Ouroboros) recordChainsyncResync(reason string) {
	if o.chainsyncMetrics == nil {
		return
	}
	o.chainsyncMetrics.resyncs.WithLabelValues(reason).Inc()
}
