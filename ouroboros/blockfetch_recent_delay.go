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
	"sync"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// recentBlockDelaySlots is the ring size. A block stays exported until
	// recentBlockDelaySlots newer blocks arrive (~5 minutes on mainnet), so
	// every block is seen by many scrapes; one is only lost if more than this
	// many blocks land within a single scrape interval.
	recentBlockDelaySlots = 16

	recentBlockDelayMetricName    = "dingo_blockfetch_recent_delay_seconds"
	recentBlockNumberMetricName   = "dingo_blockfetch_recent_block_number"
	recentFetchDurationMetricName = "dingo_blockfetch_recent_fetch_duration_seconds"
)

var (
	recentBlockDelayDesc = prometheus.NewDesc(
		recentBlockDelayMetricName,
		"block delay in seconds (wall clock minus slot time) of the block held in each slot of a ring of the most recent at-tip fetched blocks; pair with "+recentBlockNumberMetricName+" by idx",
		[]string{"idx"},
		nil,
	)
	recentBlockNumberDesc = prometheus.NewDesc(
		recentBlockNumberMetricName,
		"block number held in each slot of the ring behind "+recentBlockDelayMetricName,
		[]string{"idx"},
		nil,
	)
	recentFetchDurationDesc = prometheus.NewDesc(
		recentFetchDurationMetricName,
		"request-dispatch-to-response duration in seconds for the block held in each slot of "+recentBlockDelayMetricName+"; unlike that metric (wall clock minus slot time), this excludes any delay before the request was dispatched -- e.g. local rollback/chain-selection work that runs before a fork's replacement block is even requested -- so a high "+recentBlockDelayMetricName+" alongside a low value here means the fetch itself was fast and the delay was upstream of it, not in the network transfer",
		[]string{"idx"},
		nil,
	)
)

type recentBlockDelay struct {
	blockNumber  uint64
	hash         lcommon.Blake2b256
	delay        float64
	fetchSeconds float64
	set          bool
}

// recentBlockDelays keeps the block delay of the last recentBlockDelaySlots
// at-tip blocks so a per-block chart can see every block, not just the one
// that happened to be current when Prometheus scraped the cardano-node
// compatible cardano_node_metrics_blockfetchclient_blockdelay_s gauge.
//
// Slot position is blockNumber % recentBlockDelaySlots and the block number
// is exported as a value, so the collector always exposes at most 3N series
// with no label churn. It is a custom collector rather than three GaugeVecs so
// each scrape reads a slot's block number, delay, and fetch duration together
// under one lock; a scrape can never pair one block's number with another
// block's delay or fetch duration. Slots that have never been written are not
// exported.
type recentBlockDelays struct {
	mu    sync.Mutex
	slots [recentBlockDelaySlots]recentBlockDelay
}

func newRecentBlockDelays() *recentBlockDelays {
	return &recentBlockDelays{}
}

// record stores the delay and fetch duration for a fetched block. A repeat
// delivery of the same block (same height and hash, e.g. from another peer)
// keeps the first delivery's values, since the later one overstates the
// delay; a different block at the same height (rollback or slot battle)
// replaces it. A block lower than the one its slot already holds is a stale,
// out-of-order delivery from before the ring wrapped and is dropped, so it
// can never overwrite a newer sample. After a rollback deeper than the ring,
// slots can keep an orphaned higher block until the new chain reaches that
// height and replaces it.
//
// fetchSeconds is the request-dispatch-to-response duration for this specific
// delivery, independent of delay: delay is wall clock minus slot time (when
// the request was dispatched matters, not just how long it took once sent),
// so a block whose request was dispatched late -- e.g. because the ledger
// held off requesting a fork's replacement block until a rollback finished --
// can show a large delay with a small fetchSeconds. Callers that cannot
// measure a genuine fetch duration (e.g. no matching in-flight request) pass
// the same value as delay, matching the pre-existing fallback in
// blockfetchClientBlock.
func (r *recentBlockDelays) record(
	blockNumber uint64,
	hash lcommon.Blake2b256,
	delay float64,
	fetchSeconds float64,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[blockNumber%recentBlockDelaySlots]
	if s.set && blockNumber < s.blockNumber {
		return
	}
	if s.set && s.blockNumber == blockNumber && s.hash == hash {
		return
	}
	*s = recentBlockDelay{
		blockNumber:  blockNumber,
		hash:         hash,
		delay:        delay,
		fetchSeconds: fetchSeconds,
		set:          true,
	}
}

func (r *recentBlockDelays) Describe(ch chan<- *prometheus.Desc) {
	ch <- recentBlockDelayDesc
	ch <- recentBlockNumberDesc
	ch <- recentFetchDurationDesc
}

func (r *recentBlockDelays) Collect(ch chan<- prometheus.Metric) {
	r.mu.Lock()
	slots := r.slots
	r.mu.Unlock()
	for i, s := range slots {
		if !s.set {
			continue
		}
		idx := strconv.Itoa(i)
		ch <- prometheus.MustNewConstMetric(
			recentBlockDelayDesc,
			prometheus.GaugeValue,
			s.delay,
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentBlockNumberDesc,
			prometheus.GaugeValue,
			float64(s.blockNumber),
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentFetchDurationDesc,
			prometheus.GaugeValue,
			s.fetchSeconds,
			idx,
		)
	}
}
