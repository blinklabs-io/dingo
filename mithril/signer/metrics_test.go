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

package signer

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewMetricsReusesRegisteredCollectors(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	first := newMetrics(registry)
	second := newMetrics(registry)

	first.rounds.Inc()
	require.Equal(t, float64(1), testutil.ToFloat64(second.rounds))
	collected, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, collected, 3)
}
