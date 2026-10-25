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
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// recentHeaderArrivalSlots matches the blockfetch recent-delay ring, so
	// a dashboard can pair the two by block number over the same window.
	recentHeaderArrivalSlots = 16

	headerArrivalDelayMetricName       = "dingo_chainsync_header_arrival_delay_seconds"
	recentHeaderDelayMetricName        = "dingo_chainsync_recent_header_delay_seconds"
	recentHeaderBlockNumberMetricName  = "dingo_chainsync_recent_header_block_number"
	recentHeaderBlockSlotMetricName    = "dingo_chainsync_recent_header_block_slot"
	headerArrivalMaxBackfillSeconds    = 300
	headerArrivalHistogramDescription  = "network arrival time minus slot time of the earliest at-tip chainsync header for each block, across peers, observed once the block leaves the recent-header ring so a copy that arrived earlier but finished admission later still counts; Leios voters only vote for an endorser block whose announcing header arrived within L_hdr of its slot; headers more than 5 minutes after their slot (backfill) are not recorded"
	recentHeaderDelayDescriptionSuffix = "; pair with " + recentHeaderBlockNumberMetricName + " by idx"
)

var (
	recentHeaderDelayDesc = prometheus.NewDesc(
		recentHeaderDelayMetricName,
		"network arrival time minus slot time of the earliest at-tip chainsync header, across peers, for the block held in each slot of a ring of recent blocks"+recentHeaderDelayDescriptionSuffix,
		[]string{"idx"},
		nil,
	)
	recentHeaderBlockNumberDesc = prometheus.NewDesc(
		recentHeaderBlockNumberMetricName,
		"block number held in each slot of the ring behind "+recentHeaderDelayMetricName,
		[]string{"idx"},
		nil,
	)
	recentHeaderBlockSlotDesc = prometheus.NewDesc(
		recentHeaderBlockSlotMetricName,
		"slot of the block held in each slot of the ring behind "+recentHeaderDelayMetricName,
		[]string{"idx"},
		nil,
	)
)

// recentHeaderForkLimit bounds the competing headers one ring slot tracks at
// a single height; a further fork at that height is not recorded.
const recentHeaderForkLimit = 4

type recentHeaderCandidate struct {
	slot  uint64
	hash  lcommon.Blake2b256
	delay float64
}

// recentHeaderArrival holds every header seen at one height, each with its
// earliest arrival, and exports the most recently seen one. Fixed-size
// storage keeps a scrape's copy of the ring independent of later writes.
type recentHeaderArrival struct {
	blockNumber uint64
	cands       [recentHeaderForkLimit]recentHeaderCandidate
	n           int
	cur         int
	set         bool
}

// recentHeaderArrivals keeps the header arrival delay of the last
// recentHeaderArrivalSlots blocks. Slot position is
// blockNumber % recentHeaderArrivalSlots and the block number is exported as a
// value, so the collector exposes at most 3N series with no label churn, and
// each scrape reads a slot's values together under one lock.
type recentHeaderArrivals struct {
	mu    sync.Mutex
	slots [recentHeaderArrivalSlots]recentHeaderArrival
}

// record stores a header arrival. Peers run admission independently, so
// copies of one header can finish out of network-arrival order, and copies of
// competing headers at the same height (a fork) can interleave. Each header
// keeps its earliest arrival, the one that bounds when the node could have
// acted on it. When a higher block takes the slot, every header the slot held
// is returned once, with its earliest arrival, so the histogram observes each
// header exactly once. A block lower than the one the slot holds is stale and
// dropped.
func (r *recentHeaderArrivals) record(
	blockNumber uint64,
	slot uint64,
	hash lcommon.Blake2b256,
	delay float64,
) []float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[blockNumber%recentHeaderArrivalSlots]
	if s.set && blockNumber < s.blockNumber {
		return nil
	}
	if s.set && s.blockNumber == blockNumber {
		for i := 0; i < s.n; i++ {
			if s.cands[i].hash == hash {
				s.cands[i].delay = min(s.cands[i].delay, delay)
				s.cur = i
				return nil
			}
		}
		if s.n < recentHeaderForkLimit {
			s.cands[s.n] = recentHeaderCandidate{
				slot: slot, hash: hash, delay: delay,
			}
			s.cur = s.n
			s.n++
		}
		return nil
	}
	var final []float64
	for i := 0; i < s.n; i++ {
		final = append(final, s.cands[i].delay)
	}
	*s = recentHeaderArrival{blockNumber: blockNumber, n: 1, set: true}
	s.cands[0] = recentHeaderCandidate{slot: slot, hash: hash, delay: delay}
	return final
}

func (r *recentHeaderArrivals) Describe(ch chan<- *prometheus.Desc) {
	ch <- recentHeaderDelayDesc
	ch <- recentHeaderBlockNumberDesc
	ch <- recentHeaderBlockSlotDesc
}

func (r *recentHeaderArrivals) Collect(ch chan<- prometheus.Metric) {
	r.mu.Lock()
	slots := r.slots
	r.mu.Unlock()
	for i, s := range slots {
		if !s.set {
			continue
		}
		idx := strconv.Itoa(i)
		c := s.cands[s.cur]
		ch <- prometheus.MustNewConstMetric(
			recentHeaderDelayDesc,
			prometheus.GaugeValue,
			c.delay,
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentHeaderBlockNumberDesc,
			prometheus.GaugeValue,
			float64(s.blockNumber),
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentHeaderBlockSlotDesc,
			prometheus.GaugeValue,
			float64(c.slot),
			idx,
		)
	}
}

type headerArrivalMetrics struct {
	delay  prometheus.Histogram
	recent *recentHeaderArrivals
	// slotClock, when set, replaces the ledger lookup in headerArrivalClock.
	slotClock func(slot uint64) (time.Time, bool, bool)
}

// headerArrivalClock returns a slot's start time and whether the ledger is at
// tip. Tests replace slotClock on their own instance; otherwise it reads the
// ledger state.
func (o *Ouroboros) headerArrivalClock(
	slot uint64,
) (time.Time, bool, bool) {
	if m := o.headerArrivalMetrics; m != nil && m.slotClock != nil {
		return m.slotClock(slot)
	}
	if o.ledgerState == nil {
		return time.Time{}, false, false
	}
	slotTime, err := o.ledgerState.SlotToTime(slot)
	if err != nil {
		return time.Time{}, false, false
	}
	return slotTime, o.ledgerState.IsAtTip(), true
}

func (o *Ouroboros) initHeaderArrivalMetrics() {
	m := &headerArrivalMetrics{
		delay: promauto.With(o.registerer).NewHistogram(
			prometheus.HistogramOpts{
				Name: headerArrivalDelayMetricName,
				Help: headerArrivalHistogramDescription,
				Buckets: []float64{
					0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 20,
					60,
				},
			},
		),
		recent: &recentHeaderArrivals{},
	}
	o.registerer.MustRegister(m.recent)
	o.headerArrivalMetrics = m
}

// observeHeaderArrival records a chainsync header's network arrival against
// its slot time. Callers pass atTip and slotTime so catch-up (where every
// header is naturally late) is excluded, matching the blockfetch delay
// metrics. The ring keeps each header's earliest arrival; the histogram
// observes every header once, when its height leaves the ring.
func (m *headerArrivalMetrics) observeHeaderArrival(
	atTip bool,
	blockNumber uint64,
	slot uint64,
	hash lcommon.Blake2b256,
	slotTime time.Time,
	arrival time.Time,
) {
	if m == nil || !atTip {
		return
	}
	delay := arrival.Sub(slotTime).Seconds()
	if delay > headerArrivalMaxBackfillSeconds {
		return
	}
	for _, final := range m.recent.record(blockNumber, slot, hash, delay) {
		m.delay.Observe(final)
	}
}
