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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// leiosMetrics tracks how the NtC chainsync server resolves the endorser
// block transaction closure for certifying ranking blocks (CertRBs).
type leiosMetrics struct {
	// certRbOutcomes counts CertRBs handled over NtC by outcome:
	//   merged            - closure was cached, spliced without waiting
	//   merged_after_wait - closure arrived during the bounded wait
	//   unresolved        - closure did not arrive; connection closed (not served)
	certRbOutcomes *prometheus.CounterVec
	// certRbWaitSeconds records how long the server waited for a missing
	// closure, labelled resolved (closure arrived) or timeout.
	certRbWaitSeconds *prometheus.HistogramVec
}

func (o *Ouroboros) initLeiosMetrics() {
	factory := promauto.With(o.config.PromRegistry)
	o.leiosMetrics = &leiosMetrics{
		certRbOutcomes: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_leios_ntc_certrb_total",
				Help: "certifying ranking blocks handled for NtC chainsync, by endorser-closure resolution outcome",
			},
			[]string{"outcome"},
		),
		certRbWaitSeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "dingo_leios_ntc_certrb_closure_wait_seconds",
				Help:    "time the NtC chainsync server waited for a CertRB endorser block closure, by outcome",
				Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0},
			},
			[]string{"outcome"},
		),
	}
}

// recordLeiosCertRbOutcome increments the CertRB outcome counter for the given
// outcome. Safe to call when metrics are not initialized.
func (o *Ouroboros) recordLeiosCertRbOutcome(outcome string) {
	if o.leiosMetrics == nil {
		return
	}
	o.leiosMetrics.certRbOutcomes.WithLabelValues(outcome).Inc()
}

// recordLeiosCertRbWait records how long the server waited for a missing
// closure. Safe to call when metrics are not initialized.
func (o *Ouroboros) recordLeiosCertRbWait(outcome string, dur time.Duration) {
	if o.leiosMetrics == nil {
		return
	}
	o.leiosMetrics.certRbWaitSeconds.WithLabelValues(outcome).
		Observe(dur.Seconds())
}
