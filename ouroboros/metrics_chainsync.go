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
	"github.com/blinklabs-io/dingo/event"
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
	// SubscribeChainsyncResync, labelled by chainsyncResyncReasonLabel.
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

// chainsyncResyncOtherReason labels a resync whose reason is not one of the
// event package's fixed reasons.
const chainsyncResyncOtherReason = "other"

// chainsyncResyncReasons are the fixed reasons a resync is labelled with.
var chainsyncResyncReasons = map[string]struct{}{
	event.ChainsyncResyncReasonBehindLocalTip:                    {},
	event.ChainsyncResyncReasonBlockfetchRangeUnavailable:        {},
	event.ChainsyncResyncReasonBlockfetchTimeoutRetryFailed:      {},
	event.ChainsyncResyncReasonChainSwitchCursorAhead:            {},
	event.ChainsyncResyncReasonDeferredHeaderValidationFailure:   {},
	event.ChainsyncResyncReasonDeterministicTxValidationRecovery: {},
	event.ChainsyncResyncReasonDisconnectedHeaderAdmission:       {},
	event.ChainsyncResyncReasonForecastCorroborationRecovery:     {},
	event.ChainsyncResyncReasonForecastHorizonNoWork:             {},
	event.ChainsyncResyncReasonForkExtensionRestartFailed:        {},
	event.ChainsyncResyncReasonForkQueueOverflowRestartFailed:    {},
	event.ChainsyncResyncReasonForkResolutionExceedsK:            {},
	event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery:     {},
	event.ChainsyncResyncReasonGenesisCorroborationHoldExpired:   {},
	event.ChainsyncResyncReasonHeaderQueueFullRestartFailed:      {},
	event.ChainsyncResyncReasonHeaderAuthorizationChanged:        {},
	event.ChainsyncResyncReasonHeaderValidationRecovery:          {},
	event.ChainsyncResyncReasonLiveTxValidationRecovery:          {},
	event.ChainsyncResyncReasonLocalLedgerRollback:               {},
	event.ChainsyncResyncReasonLocalTipPlateau:                   {},
	event.ChainsyncResyncReasonNetworkIngestReleased:             {},
	event.ChainsyncResyncReasonPeerTipBehindMithril:              {},
	event.ChainsyncResyncReasonPersistentFork:                    {},
	event.ChainsyncResyncReasonPostPlateauRealign:                {},
	event.ChainsyncResyncReasonReplayRecoveryNonConverging:       {},
	event.ChainsyncResyncReasonRollbackAhead:                     {},
	event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor:       {},
	event.ChainsyncResyncReasonRollbackExceedsK:                  {},
	event.ChainsyncResyncReasonRollbackExceedsMithril:            {},
	event.ChainsyncResyncReasonRollbackLoop:                      {},
	event.ChainsyncResyncReasonRollbackNotFound:                  {},
}

// chainsyncResyncReasonLabel bounds the reason label. Some publishers format
// errors or connection IDs into the reason, which would add a retained series
// per connection, so any reason that is not a fixed one is counted as
// chainsyncResyncOtherReason; publishers log the full reason.
func chainsyncResyncReasonLabel(reason string) string {
	if _, ok := chainsyncResyncReasons[reason]; ok {
		return reason
	}
	return chainsyncResyncOtherReason
}

// recordChainsyncResync counts one chainsync resync event. Safe to call when
// metrics are not initialized.
func (o *Ouroboros) recordChainsyncResync(reason string) {
	if o.chainsyncMetrics == nil {
		return
	}
	o.chainsyncMetrics.resyncs.WithLabelValues(
		chainsyncResyncReasonLabel(reason),
	).Inc()
}
