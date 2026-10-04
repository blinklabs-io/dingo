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

package event

import (
	"github.com/blinklabs-io/dingo/internal/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

type eventMetrics struct {
	eventsTotal         *prometheus.CounterVec
	subscribers         *prometheus.GaugeVec
	deliveryErrors      *prometheus.CounterVec
	deliveryTimeouts    *prometheus.CounterVec
	deliveryBlocked     *prometheus.CounterVec
	asyncEnqueueBlocked *prometheus.CounterVec
	handlerStalls       *prometheus.CounterVec
}

// initMetrics registers the bus collectors, reusing compatible collectors
// already on the registry. On failure it unregisters what it added.
func (e *EventBus) initMetrics(promRegistry prometheus.Registerer) error {
	r := promutil.NewRegistration(promRegistry)
	e.metrics = &eventMetrics{}
	e.metrics.eventsTotal = promutil.Register(r, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_total",
			Help: "total events by type",
		},
		[]string{"type"},
	))
	e.metrics.subscribers = promutil.Register(r, prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "event_subscribers",
			Help: "subscribers by event type and kind",
		},
		[]string{"type", "kind"},
	))
	e.metrics.deliveryErrors = promutil.Register(r, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_delivery_errors_total",
			Help: "total delivery errors by event type and kind",
		},
		[]string{"type", "kind"},
	))
	e.metrics.deliveryTimeouts = promutil.Register(r, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_delivery_timeouts_total",
			Help: "total subscriber delivery timeouts by event type",
		},
		[]string{"type"},
	))
	e.metrics.deliveryBlocked = promutil.Register(r, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_delivery_blocked_total",
			Help: "total deliveries that waited for subscriber buffer " +
				"capacity, by event type and kind",
		},
		[]string{"type", "kind"},
	))
	e.metrics.asyncEnqueueBlocked = promutil.Register(r, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_async_enqueue_blocked_total",
			Help: "total async publishes that waited for queue capacity, " +
				"by event type",
		},
		[]string{"type"},
	))
	e.metrics.handlerStalls = promutil.Register(r, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_subscriber_handler_stalled_total",
			Help: "observations of a subscriber handler that had not " +
				"returned within the progress interval, by event type",
		},
		[]string{"type"},
	))
	if err := r.Err(); err != nil {
		r.Rollback()
		return err
	}
	return nil
}
