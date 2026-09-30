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

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	recentForkParticipantsMetricName  = "dingo_blockfetch_recent_fork_participants"
	recentForkDistinctSlotsMetricName = "dingo_blockfetch_recent_fork_distinct_slots"

	// recentForkMaxParticipants bounds memory for a single ring slot. Real
	// battles rarely exceed a handful of competing blocks; this only guards
	// against a pathological number of distinct hashes being reported for
	// one height (e.g. a misbehaving peer), so the set can never grow
	// without bound.
	recentForkMaxParticipants = 8
)

var (
	recentForkParticipantsDesc = prometheus.NewDesc(
		recentForkParticipantsMetricName,
		"distinct competing blocks observed for the height held in each slot of "+recentBlockDelayMetricName+" (1 = no battle, 2 = an ordinary two-way race, 3+ = an N-way battle); pair with "+recentForkDistinctSlotsMetricName+" by idx to tell a slot battle (all participants share one slot) from a height battle (each has its own slot) from a mix of the two",
		[]string{"idx"},
		nil,
	)
	recentForkDistinctSlotsDesc = prometheus.NewDesc(
		recentForkDistinctSlotsMetricName,
		"distinct slot numbers among the participants counted in "+recentForkParticipantsMetricName+" for that idx",
		[]string{"idx"},
		nil,
	)
)

type recentForkBattle struct {
	blockNumber uint64
	hashes      map[lcommon.Blake2b256]struct{}
	slots       map[uint64]struct{}
	set         bool
}

// recentForkBattles tracks, per ring index (blockNumber % recentBlockDelaySlots,
// the same indexing recentBlockDelays uses), the set of distinct blocks
// observed competing for that height -- both the eventual winner and any
// blocks displaced before it. Unlike a single "pending competitor" scalar,
// a set handles battles of arbitrary width (multiple pools winning the same
// slot by VRF chance, or a stacked short fork) without special-casing the
// common two-way case.
type recentForkBattles struct {
	mu    sync.Mutex
	slots [recentBlockDelaySlots]recentForkBattle
}

func newRecentForkBattles() *recentForkBattles {
	return &recentForkBattles{}
}

// recordParticipant registers one block observed at blockNumber/slotNumber
// as a competitor for that height. A block lower than the height its ring
// slot already holds is a stale, out-of-order delivery from before the ring
// wrapped and is dropped, so it can never resurrect or mutate a slot the
// ring has moved past. A block at a new, higher height reusing the slot
// starts a fresh participant set: it is a different battle, or no battle at
// all, not a continuation of the old one.
func (r *recentForkBattles) recordParticipant(
	blockNumber uint64,
	slotNumber uint64,
	hash lcommon.Blake2b256,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.slots[blockNumber%recentBlockDelaySlots]
	if s.set && blockNumber < s.blockNumber {
		return
	}
	if !s.set || s.blockNumber != blockNumber {
		*s = recentForkBattle{
			blockNumber: blockNumber,
			hashes:      make(map[lcommon.Blake2b256]struct{}),
			slots:       make(map[uint64]struct{}),
			set:         true,
		}
	}
	if _, ok := s.hashes[hash]; !ok {
		if len(s.hashes) >= recentForkMaxParticipants {
			return
		}
		s.hashes[hash] = struct{}{}
	}
	s.slots[slotNumber] = struct{}{}
}

// RecordForkBattleParticipants registers each rolled-back block as a
// participant for its own height, so a battle is visible even when the
// eventual winner is a locally forged block that never passes through
// blockfetchClientBlock. A block that fails to decode is skipped: it never
// reached the ledger as a real competitor, so it cannot be identified as
// one. Safe to call with o.blockfetchMetrics == nil (metrics disabled).
func (o *Ouroboros) RecordForkBattleParticipants(rolledBack []models.Block) {
	if o.blockfetchMetrics == nil {
		return
	}
	for _, b := range rolledBack {
		header, err := b.Decode()
		if err != nil {
			continue
		}
		o.blockfetchMetrics.recentForks.recordParticipant(
			header.BlockNumber(),
			header.SlotNumber(),
			header.Hash(),
		)
	}
}

func (r *recentForkBattles) Describe(ch chan<- *prometheus.Desc) {
	ch <- recentForkParticipantsDesc
	ch <- recentForkDistinctSlotsDesc
}

func (r *recentForkBattles) Collect(ch chan<- prometheus.Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, s := range r.slots {
		if !s.set {
			continue
		}
		idx := strconv.Itoa(i)
		ch <- prometheus.MustNewConstMetric(
			recentForkParticipantsDesc,
			prometheus.GaugeValue,
			float64(len(s.hashes)),
			idx,
		)
		ch <- prometheus.MustNewConstMetric(
			recentForkDistinctSlotsDesc,
			prometheus.GaugeValue,
			float64(len(s.slots)),
			idx,
		)
	}
}
