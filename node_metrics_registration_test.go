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

package dingo

import (
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func metricsRegistrationOptions(registry prometheus.Registerer) []ConfigOptionFunc {
	return []ConfigOptionFunc{
		WithNetworkMagic(1), WithPrometheusRegistry(registry),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
	}
}

func gatheredFamilyNames(t *testing.T, registry *prometheus.Registry) []string {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	sort.Strings(names)
	return names
}

func TestNewReusesCompatibleRegisteredMetrics(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	options := metricsRegistrationOptions(registry)
	var first, second *Node
	var err error
	require.NotPanics(t, func() { first, err = New(NewConfig(options...)) })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Stop()) })
	before := gatheredFamilyNames(t, registry)

	require.NotPanics(t, func() { second, err = New(NewConfig(options...)) })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Stop()) })
	require.Equal(t, before, gatheredFamilyNames(t, registry))
}

func TestNewMetricsRegistrationFailureRollsBack(t *testing.T) {
	t.Parallel()

	// Each name is registered by a later collector than the previous one, so
	// a failure at it must unregister everything registered before it.
	for _, conflict := range []string{
		"dingo_build_info",
		"cardano_node_metrics_RTS_gcMajorNum_int",
		"dingo_chainselection_rollback_registrations_total",
		"event_delivery_blocked_total",
	} {
		t.Run(conflict, func(t *testing.T) {
			t.Parallel()

			registry := prometheus.NewRegistry()
			// The node registers this name with a "network" label, so a
			// different label set makes the registration fail without being
			// an AlreadyRegisteredError.
			blocker := prometheus.NewGauge(prometheus.GaugeOpts{
				Name:        conflict,
				Help:        "conflicting caller collector",
				ConstLabels: prometheus.Labels{"conflict": "true"},
			})
			registry.MustRegister(blocker)
			before := gatheredFamilyNames(t, registry)
			options := metricsRegistrationOptions(registry)

			var node *Node
			var err error
			require.NotPanics(t, func() { node, err = New(NewConfig(options...)) })
			require.Error(t, err)
			require.Nil(t, node)
			require.Equal(t, before, gatheredFamilyNames(t, registry),
				"failed construction left node collectors registered")

			require.NotPanics(t, func() { node, err = New(NewConfig(options...)) })
			require.Error(t, err, "repeated failed construction must stay an error")
			require.Equal(t, before, gatheredFamilyNames(t, registry))
		})
	}
}
