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
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// stubRewardParitySource satisfies NewObserver's non-nil Source check; the
// observer is never started, so no method is called.
type stubRewardParitySource struct {
	koiosparity.RewardParitySource
}

// TestKoiosParityObserverRegistersThroughNodeRegistry builds the observer
// against the registry New leaves in n.config.promRegistry, which
// configWrapPromRegistry has wrapped with a constant "network" label. It
// builds it twice, as startKoiosParityObserver does again on a live
// restore/truncate, so the reuse path through the wrapper is covered too.
func TestKoiosParityObserverRegistersThroughNodeRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	node, err := New(NewConfig(
		WithNetworkMagic(2),
		WithPrometheusRegistry(registry),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
	))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Stop()) })

	cacheDir := t.TempDir()
	for i := range 2 {
		var observer *koiosparity.Observer
		require.NotPanics(t, func() {
			observer, err = koiosparity.NewObserver(koiosparity.ObserverConfig{
				Network:      "preview",
				CachePath:    filepath.Join(cacheDir, "cache"),
				Source:       stubRewardParitySource{},
				PromRegistry: node.config.promRegistry,
			})
		}, "observer construction %d", i)
		require.NoError(t, err)
		require.NoError(t, observer.Stop(context.Background()))
	}

	err = node.config.promRegistry.Register(prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_koiosparity_epoch_result_total",
			Help: "koios-parity epoch validation results, by network (a constant label from the node's shared registry), queue and status (pass/fail/error)",
		},
		[]string{"queue", "status"},
	))
	_, ok := errors.AsType[prometheus.AlreadyRegisteredError](err)
	require.True(
		t,
		ok,
		"koios-parity collectors were not registered through the node "+
			"registry: %v",
		err,
	)
}
