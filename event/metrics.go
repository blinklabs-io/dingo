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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type eventMetrics struct {
	eventsTotal      *prometheus.CounterVec
	subscribers      *prometheus.GaugeVec
	deliveryErrors   *prometheus.CounterVec
	deliveryTimeouts *prometheus.CounterVec
}

func (e *EventBus) initMetrics(promRegistry prometheus.Registerer) {
	promautoFactory := promauto.With(promRegistry)
	e.metrics = &eventMetrics{}
	e.metrics.eventsTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_total",
			Help: "total events by type",
		},
		[]string{"type"},
	)
	e.metrics.subscribers = promautoFactory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "event_subscribers",
			Help: "subscribers by event type and kind",
		},
		[]string{"type", "kind"},
	)
	e.metrics.deliveryErrors = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_delivery_errors_total",
			Help: "total delivery errors by event type and kind",
		},
		[]string{"type", "kind"},
	)
	e.metrics.deliveryTimeouts = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "event_delivery_timeouts_total",
			Help: "total remote subscriber delivery timeouts by event type",
		},
		[]string{"type"},
	)
}
