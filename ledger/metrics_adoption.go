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

package ledger

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// recentAdoptionSlots is the ring size, matching the blockfetch
	// recent-delay ring (ouroboros.recentBlockDelaySlots) so the two panels
	// cover the same window.
	recentAdoptionSlots = 16

	recentAdoptionDelayMetricName  = "dingo_ledger_recent_adoption_delay_seconds"
	recentAdoptionNumberMetricName = "dingo_ledger_recent_adoption_block_number"
)

var (
	recentAdoptionDelayDesc = prometheus.NewDesc(
		recentAdoptionDelayMetricName,
		"ledger adoption delay in seconds (wall clock at tip commit minus slot time) of the block held in each slot of a ring of the most recent at-tip applied blocks; pair with "+recentAdoptionNumberMetricName+" by idx",
		[]string{"idx"},
		nil,
	)
	recentAdoptionNumberDesc = prometheus.NewDesc(
		recentAdoptionNumberMetricName,
		"block number held in each slot of the ring behind "+recentAdoptionDelayMetricName,
		[]string{"idx"},
		nil,
	)
)

type recentAdoption struct {
	blockNumber uint64
	hash        string
	delay       float64
	set         bool
}

// recentAdoptions keeps the ledger adoption delay of the last
// recentAdoptionSlots at-tip blocks. It follows the ring collector pattern of
// ouroboros.recentBlockDelays: slot position is blockNumber %
// recentAdoptionSlots, the block number is exported as a value so there is no
// label churn, and a scrape reads a slot's number and delay together under one
// lock. Slots that were never written are not exported.
type recentAdoptions struct {
	mu    sync.Mutex
	slots [recentAdoptionSlots]recentAdoption
}

func newRecentAdoptions() *recentAdoptions {
	return &recentAdoptions{}
}

// record stores the adoption delay for an applied block. A repeat of the same
// block (same height and hash, e.g. an apply retry) keeps the first value; a
// different block at the same height (rollback or slot battle) replaces it. A
// block lower than the one its slot already holds is stale and is dropped.
func (r *recentAdoptions) record(
	blockNumber uint64,
	hash string,
	delay float64,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[blockNumber%recentAdoptionSlots]
	if s.set && blockNumber < s.blockNumber {
		return
	}
	if s.set && s.blockNumber == blockNumber && s.hash == hash {
		return
	}
	*s = recentAdoption{
		blockNumber: blockNumber,
		hash:        hash,
		delay:       delay,
		set:         true,
	}
}

func (r *recentAdoptions) Describe(ch chan<- *prometheus.Desc) {
	ch <- recentAdoptionDelayDesc
	ch <- recentAdoptionNumberDesc
}

func (r *recentAdoptions) Collect(ch chan<- prometheus.Metric) {
	r.mu.Lock()
	slots := r.slots
	r.mu.Unlock()
	for i, s := range slots {
		if !s.set {
			continue
		}
		idx := strconv.Itoa(i)
		ch <- prometheus.MustNewConstMetric(
			recentAdoptionDelayDesc,
			prometheus.GaugeValue,
			s.delay,
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentAdoptionNumberDesc,
			prometheus.GaugeValue,
			float64(s.blockNumber),
			idx,
		)
	}
}

// observeBlockAdoption records the delay between a block's slot time and the
// moment the ledger adopted it as its tip. Callers gate on at-tip so catch-up
// does not poison the history.
func (m *stateMetrics) observeBlockAdoption(
	blockNumber uint64,
	hash []byte,
	delaySeconds float64,
) {
	if m.blockAdoptionDelay != nil {
		m.blockAdoptionDelay.Observe(delaySeconds)
	}
	if m.recentAdoptions != nil {
		m.recentAdoptions.record(blockNumber, string(hash), delaySeconds)
	}
}

// adoptedBlock identifies a block applied in one commit chunk.
type adoptedBlock struct {
	number uint64
	slot   uint64
	hash   []byte
}

// observeBlockAdoptions records the adoption delay of every block in a chunk
// whose new tip was just published. It runs after the chunk's tip is visible
// to readers, which is the moment the ledger "adopts" the block, and only
// while the ledger is at tip: during catch-up every block is naturally late
// relative to wall-clock time and would poison the history. The at-tip signal
// is the same one the blockfetch delay metrics use (IsAtTip), so the
// transition batch that first reaches tip is included.
func (ls *LedgerState) observeBlockAdoptions(blocks []adoptedBlock) {
	if len(blocks) == 0 || !ls.IsAtTip() {
		return
	}
	now := time.Now()
	for _, b := range blocks {
		slotTime, err := ls.SlotToTime(b.slot)
		if err != nil {
			continue
		}
		ls.metrics.observeBlockAdoption(
			b.number,
			b.hash,
			now.Sub(slotTime).Seconds(),
		)
	}
}
