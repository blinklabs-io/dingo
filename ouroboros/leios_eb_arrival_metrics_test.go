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
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// gatherEbArrivalRing returns idx -> value for the named ring metric.
func gatherEbArrivalRing(
	t *testing.T,
	reg *prometheus.Registry,
	name string,
) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != name {
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

func ebArrivalHistogramCount(t *testing.T, o *Ouroboros) uint64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, o.leiosMetrics.ebArrivalDelay.Write(&m))
	return m.GetHistogram().GetSampleCount()
}

func TestRecentEbArrivalsRing(t *testing.T) {
	t.Parallel()

	r := newRecentEbArrivals()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(r))
	require.Empty(t, gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName))

	require.True(t, r.record(17, []byte{1}, 1.5, 4))
	require.True(t, r.record(18, []byte{2}, 2.5, 5))
	require.Equal(t,
		map[string]float64{"1": 17, "2": 18},
		gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName),
	)
	require.Equal(t,
		map[string]float64{"1": 1.5, "2": 2.5},
		gatherEbArrivalRing(t, reg, recentEbArrivalDelayMetricName),
	)
	require.Equal(t,
		map[string]float64{"1": 4, "2": 5},
		gatherEbArrivalRing(t, reg, recentEbArrivalTxCountMetricName),
	)

	// Repeat keeps the first; stale lower slot is dropped; a different block
	// at the same slot and a wrapped newer slot replace.
	require.False(t, r.record(17, []byte{1}, 9, 9))
	require.False(t, r.record(1, []byte{7}, 9, 9))
	require.True(t, r.record(18, []byte{3}, 3.5, 6))
	require.True(t, r.record(33, []byte{4}, 4.5, 7))
	require.Equal(t,
		map[string]float64{"1": 33, "2": 18},
		gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName),
	)
	require.Equal(t,
		map[string]float64{"1": 4.5, "2": 3.5},
		gatherEbArrivalRing(t, reg, recentEbArrivalDelayMetricName),
	)
	require.Equal(t,
		map[string]float64{"1": 7, "2": 6},
		gatherEbArrivalRing(t, reg, recentEbArrivalTxCountMetricName),
	)
}

func newEbArrivalTestOuroboros(
	t *testing.T,
	age time.Duration,
) (*Ouroboros, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{
		EnableLeios:  true,
		PromRegistry: reg,
		LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{
			slotTimeFunc: func(uint64) time.Time {
				return time.Now().Add(-age)
			},
		},
	})
	require.NotNil(t, o.leiosMetrics)
	return o, reg
}

// twoTxEndorserBlock builds a manifest of two transactions and returns the
// point, manifest and transaction bodies.
func twoTxEndorserBlock(
	t *testing.T,
	slot uint64,
) (ocommon.Point, []byte, []cbor.RawMessage) {
	t.Helper()
	tx1, ref1 := testLeiosManifestTx(t, 1)
	tx2, ref2 := testLeiosManifestTx(t, 2)
	manifest, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref1, ref2},
	}.MarshalCBOR()
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(manifest)
	return ocommon.NewPoint(slot, hash.Bytes()),
		manifest,
		[]cbor.RawMessage{tx1, tx2}
}

// The arrival is recorded when the transaction closure completes, not when
// the manifest alone is stored, and a re-store of the complete block does not
// record it again.
func TestLeiosEbArrivalRecordedWhenClosureCompletes(t *testing.T) {
	t.Parallel()

	o, reg := newEbArrivalTestOuroboros(t, 3*time.Second)
	point, manifest, txs := twoTxEndorserBlock(t, 4242)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifest, nil, leiosStoreBackfill,
	))
	require.Zero(t, ebArrivalHistogramCount(t, o))
	require.Empty(t, gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName))

	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifest, txs, leiosStoreBackfill,
	))
	require.EqualValues(t, 1, ebArrivalHistogramCount(t, o))
	slots := gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName)
	require.Len(t, slots, 1)
	for idx, slot := range slots {
		require.EqualValues(t, 4242, slot)
		require.EqualValues(t, 2,
			gatherEbArrivalRing(t, reg, recentEbArrivalTxCountMetricName)[idx])
		delay := gatherEbArrivalRing(t, reg, recentEbArrivalDelayMetricName)[idx]
		require.GreaterOrEqual(t, delay, 3.0)
		require.Less(t, delay, 60.0)
	}

	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifest, txs, leiosStoreBackfill,
	))
	require.EqualValues(t, 1, ebArrivalHistogramCount(t, o))
}

// An endorser block that completes while its slot binding is still
// unverified is recorded once the binding is established.
func TestLeiosEbArrivalWaitsForSlotBinding(t *testing.T) {
	t.Parallel()

	o, reg := newEbArrivalTestOuroboros(t, 2*time.Second)
	point, manifest, txs := twoTxEndorserBlock(t, 5151)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point, manifest, txs, leiosStorePeerOffered,
	))
	require.Zero(t, ebArrivalHistogramCount(t, o))

	publish := o.bindLeiosEndorserBlockSlot(point.Hash, point.Slot)
	require.NotNil(t, publish)
	publish()
	require.EqualValues(t, 1, ebArrivalHistogramCount(t, o))
	require.Len(t,
		gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName), 1)
}

// Backfill of an old endorser block, and an endorser block this node forged
// itself, are not arrivals.
func TestLeiosEbArrivalIgnoresBackfillAndForged(t *testing.T) {
	t.Parallel()

	t.Run("old slot", func(t *testing.T) {
		t.Parallel()
		o, reg := newEbArrivalTestOuroboros(t, time.Hour)
		point, manifest, txs := twoTxEndorserBlock(t, 100)
		require.NoError(t, o.storeLeiosEndorserBlock(
			point, manifest, txs, leiosStoreBackfill,
		))
		require.Zero(t, ebArrivalHistogramCount(t, o))
		require.Empty(t,
			gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName))
	})
	t.Run("forged locally", func(t *testing.T) {
		t.Parallel()
		o, reg := newEbArrivalTestOuroboros(t, time.Second)
		point, manifest, txs := twoTxEndorserBlock(t, 200)
		require.NoError(t, o.storeLeiosEndorserBlock(
			point, manifest, txs, leiosStoreAuthoritative,
		))
		require.Zero(t, ebArrivalHistogramCount(t, o))
		require.Empty(t,
			gatherEbArrivalRing(t, reg, recentEbArrivalSlotMetricName))
	})
}
