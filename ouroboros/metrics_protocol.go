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

// Outcomes for the txsubmission reply size mismatch counter.
const (
	// txsubmissionReplySizeAcceptedWire counts bodies whose advertised size
	// was the wrapped wire size rather than the unwrapped body size. This
	// is the normal, spec-correct case for a cardano-node peer.
	txsubmissionReplySizeAcceptedWire = "accepted_wire_size"
	// txsubmissionReplySizeRejected counts the bodies of replies dropped
	// because an advertised size matched neither the body size nor the
	// wire size. The whole reply is dropped, so every body it carried is
	// counted, keeping the unit the same as the accepted outcome.
	txsubmissionReplySizeRejected = "rejected"
)

type protocolMetrics struct {
	messagesReceived              *prometheus.CounterVec
	messageDuration               *prometheus.HistogramVec
	txsubmissionAdmissionRetries  prometheus.Histogram
	txsubmissionReplySizeMismatch *prometheus.CounterVec
}

func (o *Ouroboros) initProtocolMetrics() {
	factory := promauto.With(o.registerer)
	o.protocolMetrics = &protocolMetrics{
		messagesReceived: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_protocol_messages_received_total",
				Help: "total mini-protocol messages received, by protocol and callback outcome",
			},
			[]string{"protocol", "outcome"},
		),
		messageDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "dingo_protocol_message_duration_seconds",
				Help: "duration of mini-protocol callback processing, by protocol and outcome",
				// Default Prometheus buckets start at 5ms, but most
				// callbacks here are sub-ms (event publish, counter Inc,
				// map lookup). Use a sub-ms-to-second range so fast
				// callbacks are distinguishable from slow ones.
				Buckets: []float64{
					0.0001, 0.0005, 0.001, 0.005, 0.01,
					0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
				},
			},
			[]string{"protocol", "outcome"},
		),
		txsubmissionReplySizeMismatch: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_txsubmission_reply_size_mismatch_total",
				Help: "tx-submission reply bodies, counted in bodies not replies, whose length differed from the size the peer advertised: accepted_wire_size counts bodies advertised at the wrapped wire size, rejected counts every body of a reply dropped for a size mismatch since the whole reply is dropped",
			},
			[]string{"outcome"},
		),
		txsubmissionAdmissionRetries: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name: "dingo_txsubmission_admission_retry_streak",
				Help: "consecutive mempool admission losses for a peer transaction under contention",
				Buckets: []float64{
					1, 2, 3, 5, 10, 20, 50,
				},
			},
		),
	}
	// Pre-materialize both outcomes so a dashboard or alert sees a zero
	// series before the first mismatch rather than a missing one.
	o.protocolMetrics.txsubmissionReplySizeMismatch.
		WithLabelValues(txsubmissionReplySizeAcceptedWire).Add(0)
	o.protocolMetrics.txsubmissionReplySizeMismatch.
		WithLabelValues(txsubmissionReplySizeRejected).Add(0)
}

// recordProtocolMessage records one received message and the callback
// duration for the named protocol, labelled with success or error based
// on err. Called by the per-signature instrument helpers in each protocol
// file. Safe to call when metrics are not initialized.
func (o *Ouroboros) recordProtocolMessage(
	protocol string,
	err error,
	dur time.Duration,
) {
	if o.protocolMetrics == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	o.protocolMetrics.messagesReceived.
		WithLabelValues(protocol, outcome).
		Inc()
	o.protocolMetrics.messageDuration.
		WithLabelValues(protocol, outcome).
		Observe(dur.Seconds())
}

// recordTxsubmissionReplySize records count reply bodies resolved with the
// given outcome. Safe to call when metrics are not initialized.
func (o *Ouroboros) recordTxsubmissionReplySize(outcome string, count int) {
	if o.protocolMetrics == nil || count <= 0 {
		return
	}
	o.protocolMetrics.txsubmissionReplySizeMismatch.
		WithLabelValues(outcome).
		Add(float64(count))
}

func (o *Ouroboros) recordTxsubmissionAdmissionRetry(streak int) {
	if o.protocolMetrics == nil {
		return
	}
	o.protocolMetrics.txsubmissionAdmissionRetries.Observe(float64(streak))
}
