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

package event

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestTryNewEventBusRegistrationFailureRollsBack(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	// event_total is registered before event_delivery_blocked_total, so the
	// failure at the latter must unregister it again.
	registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "event_delivery_blocked_total",
		Help: "conflicting caller collector",
	}))

	bus, err := TryNewEventBus(registry, nil)
	require.Error(t, err)
	require.Nil(t, bus)

	probe := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "event_total",
		Help: "total events by type",
	}, []string{"type"})
	require.NoError(t, registry.Register(probe),
		"failed construction left event_total registered")
}

func TestNewEventBusReusesRegisteredMetrics(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	first := NewEventBus(registry, nil)
	defer first.Close()
	var second *EventBus
	require.NotPanics(t, func() { second = NewEventBus(registry, nil) })
	defer second.Close()
	require.Same(t, first.metrics.eventsTotal, second.metrics.eventsTotal)
}
