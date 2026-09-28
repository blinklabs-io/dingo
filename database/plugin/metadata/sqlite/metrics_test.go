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

package sqlite

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestSafeRegisterGaugeFuncReplacesStaleCollector is the regression test for
// safeRegisterGaugeFunc's duplicate-registration handling: a second call
// under the same metric name must make the new fn observable, not silently
// keep the first store's GaugeFunc. Without the fix, a replacement or
// second store sharing a registry (as some test harnesses do) is
// permanently invisible to every later scrape: the first collector stays
// registered and keeps reading whatever it closed over.
func TestSafeRegisterGaugeFuncReplacesStaleCollector(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	const name = "test_safe_register_gauge_func"

	// Help stays identical across both calls, matching the real scenario
	// this guards: two Store instances (or a closed-then-reopened one)
	// registering the exact same metric name/help with a different fn
	// closed over their own state, not two different metrics colliding by
	// name. prometheus.Registry requires help/labels to stay consistent for
	// a name across its whole lifetime regardless of Unregister, so varying
	// help here would fail for a reason unrelated to this fix.
	const help = "shared help text"
	safeRegisterGaugeFunc(reg, name, help, func() float64 { return 1 })
	safeRegisterGaugeFunc(reg, name, help, func() float64 { return 2 })

	families, err := reg.Gather()
	require.NoError(t, err)
	var found bool
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		found = true
		require.Len(t, family.GetMetric(), 1)
		require.Equal(
			t,
			float64(2),
			family.GetMetric()[0].GetGauge().GetValue(),
			"expected the second registration's fn to be the one actually read",
		)
	}
	require.True(t, found, "expected %s to be registered", name)
}
