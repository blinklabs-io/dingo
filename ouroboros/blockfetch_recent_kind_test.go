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
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/internal/leiosheader"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// gatherRecentBlockKinds returns idx -> kind for the kind metric.
func gatherRecentBlockKinds(
	t *testing.T,
	reg *prometheus.Registry,
) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != recentBlockKindMetricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			require.Len(t, m.GetLabel(), 1)
			require.Equal(t, "idx", m.GetLabel()[0].GetName())
			out[m.GetLabel()[0].GetValue()] = m.GetGauge().GetValue()
		}
	}
	return out
}

// The kind is exported in the same slot as the delay it belongs to, replaced
// together with it, and 0 for a block recorded without a kind.
func TestRecentBlockDelaysExportsLeiosKindPerSlot(t *testing.T) {
	t.Parallel()
	r, reg := newTestRecentDelays(t)
	require.Empty(t, gatherRecentBlockKinds(t, reg))

	both := leiosheader.KindAnnouncesEB | leiosheader.KindCertifiesEB
	r.record(100, testBlockHash(1), 0.3, 0.3, leiosheader.KindAnnouncesEB)
	r.record(101, testBlockHash(2), 0.3, 0.3, leiosheader.KindCertifiesEB)
	r.record(102, testBlockHash(3), 0.3, 0.3, both)
	r.record(103, testBlockHash(4), 0.3, 0.3)

	slot := func(n uint64) string {
		return strconv.FormatUint(n%recentBlockDelaySlots, 10)
	}
	require.Equal(t, map[string]float64{
		slot(100): 1,
		slot(101): 2,
		slot(102): 3,
		slot(103): 0,
	}, gatherRecentBlockKinds(t, reg))

	// A different block at the same height replaces delay and kind together;
	// a repeat of the same block keeps the first delivery's kind.
	r.record(100, testBlockHash(9), 0.5, 0.5)
	r.record(101, testBlockHash(2), 0.9, 0.9, 0)
	got := gatherRecentBlockKinds(t, reg)
	require.Equal(t, 0.0, got[slot(100)])
	require.Equal(t, 2.0, got[slot(101)])
}
