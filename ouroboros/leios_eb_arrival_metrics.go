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
	"bytes"
	"strconv"
	"sync"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// recentEbArrivalSlots is the ring size, matching the blockfetch
	// recent-delay ring.
	recentEbArrivalSlots = 16

	// leiosEbArrivalMaxDelay bounds what counts as an at-tip arrival. An
	// endorser block that completes later than this after its slot time is
	// backfill or a late replay, which would poison the delay history the
	// same way catch-up blocks poison the blockfetch delay.
	leiosEbArrivalMaxDelay = 5 * time.Minute

	recentEbArrivalDelayMetricName   = "dingo_leios_recent_eb_arrival_delay_seconds"
	recentEbArrivalSlotMetricName    = "dingo_leios_recent_eb_slot"
	recentEbArrivalTxCountMetricName = "dingo_leios_recent_eb_tx_count"
)

var (
	recentEbArrivalDelayDesc = prometheus.NewDesc(
		recentEbArrivalDelayMetricName,
		"endorser block arrival delay in seconds (wall clock when its manifest and every transaction became available locally, minus its slot time) of the block held in each slot of a ring of the most recent at-tip endorser blocks; pair with "+recentEbArrivalSlotMetricName+" and "+recentEbArrivalTxCountMetricName+" by idx",
		[]string{"idx"},
		nil,
	)
	recentEbArrivalSlotDesc = prometheus.NewDesc(
		recentEbArrivalSlotMetricName,
		"endorser block slot held in each slot of the ring behind "+recentEbArrivalDelayMetricName,
		[]string{"idx"},
		nil,
	)
	recentEbArrivalTxCountDesc = prometheus.NewDesc(
		recentEbArrivalTxCountMetricName,
		"number of transactions in the endorser block held in each slot of the ring behind "+recentEbArrivalDelayMetricName,
		[]string{"idx"},
		nil,
	)
)

type recentEbArrival struct {
	slot    uint64
	hash    []byte
	delay   float64
	txCount int
	set     bool
}

// recentEbArrivals keeps the arrival delay of the last recentEbArrivalSlots
// at-tip endorser blocks. It follows the ring collector pattern of
// recentBlockDelays: slot position is ebSlot % recentEbArrivalSlots, the EB
// slot is exported as a value so there is no label churn, and a scrape reads a
// slot's EB slot, delay and transaction count together under one lock. Slots
// never written are not exported.
type recentEbArrivals struct {
	mu    sync.Mutex
	slots [recentEbArrivalSlots]recentEbArrival
}

func newRecentEbArrivals() *recentEbArrivals {
	return &recentEbArrivals{}
}

// record stores an endorser block's arrival and reports whether the
// histogram should observe it. The first arrival of an (slot, hash) wins: a
// repeat of the same endorser block keeps the first delay (later ones only
// overstate it) and returns false. A different block at the same slot
// replaces it. A lower slot than the one already held is not stored, since
// the ring keeps the newer slot, but it is still an arrival and returns true.
func (r *recentEbArrivals) record(
	slot uint64,
	hash []byte,
	delay float64,
	txCount int,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[slot%recentEbArrivalSlots]
	if s.set && slot < s.slot {
		return true
	}
	if s.set && s.slot == slot && bytes.Equal(s.hash, hash) {
		return false
	}
	*s = recentEbArrival{
		slot:    slot,
		hash:    bytes.Clone(hash),
		delay:   delay,
		txCount: txCount,
		set:     true,
	}
	return true
}

func (r *recentEbArrivals) Describe(ch chan<- *prometheus.Desc) {
	ch <- recentEbArrivalDelayDesc
	ch <- recentEbArrivalSlotDesc
	ch <- recentEbArrivalTxCountDesc
}

func (r *recentEbArrivals) Collect(ch chan<- prometheus.Metric) {
	r.mu.Lock()
	slots := r.slots
	r.mu.Unlock()
	for i, s := range slots {
		if !s.set {
			continue
		}
		idx := strconv.Itoa(i)
		ch <- prometheus.MustNewConstMetric(
			recentEbArrivalDelayDesc,
			prometheus.GaugeValue,
			s.delay,
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentEbArrivalSlotDesc,
			prometheus.GaugeValue,
			float64(s.slot),
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentEbArrivalTxCountDesc,
			prometheus.GaugeValue,
			float64(s.txCount),
			idx,
		)
	}
}

// leiosEbSlotTime resolves an endorser block's slot to wall-clock time with
// whichever ledger view is wired.
func (o *Ouroboros) leiosEbSlotTime(slot uint64) (time.Time, bool) {
	if o.leiosAnnouncementLedger != nil {
		if t, err := o.leiosAnnouncementLedger.SlotToTime(slot); err == nil {
			return t, true
		}
		return time.Time{}, false
	}
	if o.ledgerState != nil {
		if t, err := o.ledgerState.SlotToTime(slot); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// recordLeiosEbArrival records the wall clock minus slot time of an
// endorser block whose manifest and full transaction set have just become
// available locally with a verified slot binding. Arrivals more than
// leiosEbArrivalMaxDelay after the slot are backfill and are not recorded.
// It resolves the slot through the ledger, so callers must hold none of
// leiosMu or leiosAnnouncementsMu.
func (o *Ouroboros) recordLeiosEbArrival(
	point ocommon.Point,
	txCount int,
) {
	if o.leiosMetrics == nil || o.leiosMetrics.recentEbArrivals == nil {
		return
	}
	slotTime, ok := o.leiosEbSlotTime(point.Slot)
	if !ok {
		return
	}
	delay := time.Since(slotTime)
	if delay > leiosEbArrivalMaxDelay {
		return
	}
	if o.leiosMetrics.recentEbArrivals.record(
		point.Slot,
		point.Hash,
		delay.Seconds(),
		txCount,
	) {
		o.leiosMetrics.ebArrivalDelay.Observe(delay.Seconds())
	}
}
