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
	"github.com/blinklabs-io/dingo/internal/leiosheader"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
)

const blockDelayByKindMetricName = "dingo_blockfetch_block_delay_seconds"

// blockDelayByKindBuckets has exact edges at 0.25, 0.5, 1, 2, 3 and 5
// seconds because dashboards compute the under-1s/3s/5s shares from the le
// buckets, matching the cardano-node compatible cdf gauges.
var blockDelayByKindBuckets = []float64{
	0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 45, 60, 90, 120, 300,
}

// blockDelayKindLabels is indexed by the leiosheader kind bitmask.
var blockDelayKindLabels = [...]string{
	0:                           "plain",
	leiosheader.KindAnnouncesEB: "announce",
	leiosheader.KindCertifiesEB: "certify",
	leiosheader.KindAnnouncesEB | leiosheader.KindCertifiesEB: "announce_certify",
}

// blockDelayKindLabel maps a leiosheader kind bitmask to the kind label.
// Bits outside the two known flags are ignored.
func blockDelayKindLabel(kind uint8) string {
	return blockDelayKindLabels[kind&(leiosheader.KindAnnouncesEB|
		leiosheader.KindCertifiesEB)]
}

func newBlockDelayByKindHistogram() *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    blockDelayByKindMetricName,
			Help:    "block delay in seconds (wall clock minus slot time) of each at-tip fetched block, by Leios ranking-block kind: plain, announce, certify or announce_certify; observes the same value as " + recentBlockDelayMetricName,
			Buckets: blockDelayByKindBuckets,
		},
		[]string{"kind"},
	)
	// Create every series up front so rate queries over a kind that has not
	// occurred yet see zero rather than an absent series.
	for _, label := range blockDelayKindLabels {
		h.WithLabelValues(label)
	}
	return h
}

// recordAtTipBlock publishes one at-tip fetched block's delay to the
// per-block ring and to the by-kind histogram, so both always carry the same
// value. The cardano-node compatible gauges are updated by the caller.
func (m *blockfetchMetrics) recordAtTipBlock(
	blockNumber uint64,
	blockSlot uint64,
	hash lcommon.Blake2b256,
	kind uint8,
	delaySeconds float64,
	fetchSeconds float64,
) {
	m.recentDelays.record(
		blockNumber,
		hash,
		delaySeconds,
		fetchSeconds,
		withBlockSlot(blockSlot),
		withBlockKind(kind),
	)
	m.blockDelayByKind.WithLabelValues(blockDelayKindLabel(kind)).
		Observe(delaySeconds)
}
