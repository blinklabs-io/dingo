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

package lifecycle

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type mapRegisterer map[prometheus.Collector]struct{}

type registryRegisterer struct {
	registry *prometheus.Registry
}

func (r registryRegisterer) Register(collector prometheus.Collector) error {
	return r.registry.Register(collector)
}

func (r registryRegisterer) MustRegister(collectors ...prometheus.Collector) {
	r.registry.MustRegister(collectors...)
}

func (r registryRegisterer) Unregister(collector prometheus.Collector) bool {
	return r.registry.Unregister(collector)
}

type structRegisterer struct {
	registryRegisterer
	marker []byte
}

type interfaceFieldRegisterer struct {
	registryRegisterer
	marker any
}

type sliceRegisterer []prometheus.Registerer

func (r sliceRegisterer) Register(collector prometheus.Collector) error {
	return r[len(r)-1].Register(collector)
}

func (r sliceRegisterer) MustRegister(collectors ...prometheus.Collector) {
	r[len(r)-1].MustRegister(collectors...)
}

func (r sliceRegisterer) Unregister(collector prometheus.Collector) bool {
	return r[len(r)-1].Unregister(collector)
}

func (r mapRegisterer) Register(collector prometheus.Collector) error {
	if _, exists := r[collector]; exists {
		return prometheus.AlreadyRegisteredError{
			ExistingCollector: collector,
			NewCollector:      collector,
		}
	}
	r[collector] = struct{}{}
	return nil
}

func (r mapRegisterer) MustRegister(collectors ...prometheus.Collector) {
	for _, collector := range collectors {
		if err := r.Register(collector); err != nil {
			panic(err)
		}
	}
}

func (r mapRegisterer) Unregister(collector prometheus.Collector) bool {
	if _, exists := r[collector]; !exists {
		return false
	}
	delete(r, collector)
	return true
}

func TestSnapshotMetricsReuseNonComparableRegisterer(t *testing.T) {
	t.Parallel()

	reg := make(mapRegisterer)
	first, err := snapshotMetricsFor(reg)
	require.NoError(t, err)
	second, err := snapshotMetricsFor(reg)
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Len(t, reg, 2)

	registry := prometheus.NewRegistry()
	wrapper := structRegisterer{
		registryRegisterer: registryRegisterer{registry: registry},
		marker:             []byte("not comparable"),
	}
	requireSnapshotMetricsReuse(t, wrapper)

	dynamic := interfaceFieldRegisterer{
		registryRegisterer: registryRegisterer{registry: prometheus.NewRegistry()},
		marker:             []string{"not comparable at runtime"},
	}
	requireSnapshotMetricsReuse(t, dynamic)

	firstRegistry := prometheus.NewRegistry()
	secondRegistry := prometheus.NewRegistry()
	registries := []prometheus.Registerer{firstRegistry, secondRegistry}
	short := sliceRegisterer(registries[:1])
	long := sliceRegisterer(registries[:2])
	_, err = snapshotMetricsFor(short)
	require.NoError(t, err)
	_, err = snapshotMetricsFor(long)
	require.NoError(t, err)
	families, err := secondRegistry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)
}

func requireSnapshotMetricsReuse(t *testing.T, reg prometheus.Registerer) {
	t.Helper()
	first, err := snapshotMetricsFor(reg)
	require.NoError(t, err)
	second, err := snapshotMetricsFor(reg)
	require.NoError(t, err)
	require.Same(t, first.pause, second.pause)
	require.Same(t, first.bytes, second.bytes)
}
