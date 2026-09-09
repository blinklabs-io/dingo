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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInvalidConfigPreservesMetricsRegistry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		option    ConfigOptionFunc
		wantError string
	}{
		{"storage", WithStorageMode("invalid"), "invalid storage mode"},
		{"era", WithStartEra("invalid"), "invalid start era"},
		{"margin", WithMinPoolMargin(10001), "min pool margin"},
		{"leverage", WithPledgeLeverage(true, 0), "pledge leverage"},
		{"listeners", func(c *Config) { c.listeners = nil }, "no listeners defined"},
		{"listener address", WithListeners(ListenerConfig{}), "listener must provide"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			control := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "caller_registry_control", Help: "Collector owned by the caller.",
			})
			registry.MustRegister(control)
			control.Set(1)
			before, err := registry.Gather()
			require.NoError(t, err)
			baseOptions := []ConfigOptionFunc{
				WithNetworkMagic(1), WithPrometheusRegistry(registry),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
				}),
			}
			for range 2 {
				cfg := NewConfig(append(baseOptions, tc.option)...)
				var node *Node
				require.NotPanics(t, func() { node, err = New(cfg) })
				require.Nil(t, node)
				require.ErrorContains(t, err, tc.wantError)
				after, err := registry.Gather()
				require.NoError(t, err)
				require.Len(t, after, len(before), "failed construction changed caller metrics")
				assert.Equal(t, before[0], after[0], "caller collector changed")
			}
			var node *Node
			require.NotPanics(t, func() { node, err = New(NewConfig(baseOptions...)) })
			require.NoError(t, err)
			require.NotNil(t, node)
			t.Cleanup(func() { require.NoError(t, node.Stop()) })
			after, err := registry.Gather()
			require.NoError(t, err)
			require.Greater(t, len(after), len(before), "valid retry must register node metrics")
		})
	}
}
