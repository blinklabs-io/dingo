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

package leader

import "maps"

// slotObservation is what ObserveSlot remembers between calls. It lives for
// the Election, not for one Start, so an election that is stopped and started
// again inside a process does not count a slot twice.
type slotObservation struct {
	started bool
	// floor is the first slot ever observed. Slots below it passed before
	// this process was watching and are never counted.
	floor uint64
	// last is the highest slot observed.
	last uint64
	// counted maps an epoch to the highest slot of it already counted.
	counted map[uint64]uint64
	// closed is the oldest epoch still counted. An older epoch's entry has
	// been dropped from counted, so its schedule, should it still be cached,
	// is skipped rather than counted again from the floor.
	closed uint64
}

// ObserveSlot reports the slot the slot clock has reached. It counts the
// scheduled leader slots the clock has reached since the last call, so the
// elapsed-slot counter moves for a slot the forger refused (syncing, stale
// tip) or never evaluated, which the forger's own counters cannot say. It
// then refreshes the current-epoch schedule gauges and the next-leader-slot
// gauge.
//
// Each slot counts once. A reading at or below the highest slot already
// counted for an epoch counts nothing, so a repeated or backwards reading is
// harmless, and a reading several slots ahead counts every scheduled slot it
// skipped. The first reading sets the floor: slots before it are not counted,
// the slot itself is. A schedule that arrives after its slots passed counts
// them from the floor on, once.
//
// Only epochs with a cached schedule are counted. A slot of an epoch whose
// schedule cannot be produced (no nonce yet, as in a deep sync) is not
// counted, because nothing says whether this pool led it.
//
// It does nothing without a metrics registry, and is cheap enough for the
// forger to call every slot.
func (e *Election) ObserveSlot(slot uint64) {
	m := e.metrics
	if m == nil {
		return
	}
	e.mu.RLock()
	schedules := maps.Clone(e.schedules)
	e.mu.RUnlock()

	e.obsMu.Lock()
	obs := &e.obs
	if !obs.started {
		obs.started = true
		obs.floor = slot
		obs.last = slot
		obs.counted = make(map[uint64]uint64)
	}
	obs.last = max(obs.last, slot)
	var elapsed uint64
	for epoch, schedule := range schedules {
		if epoch < obs.closed {
			continue
		}
		from := obs.floor
		if done, ok := obs.counted[epoch]; ok {
			if done >= slot {
				continue
			}
			// A reading behind the floor leaves done below it.
			from = max(from, done+1)
		}
		elapsed += schedule.countLeaderSlotsBetween(from, slot)
		obs.counted[epoch] = slot
	}
	if slotEpoch, err := e.epochProvider.EpochForSlot(slot); err == nil &&
		slotEpoch >= maxCachedSchedules {
		// Close the epochs older than the schedule cache's window and drop
		// their entries. The cache prunes relative to the newest schedule it
		// stored, not to the slot clock, so it can still hold a closed
		// epoch's schedule when no newer one could be produced.
		obs.closed = max(obs.closed, slotEpoch-maxCachedSchedules+1)
		for epoch := range obs.counted {
			if epoch < obs.closed {
				delete(obs.counted, epoch)
			}
		}
	}
	e.obsMu.Unlock()

	if elapsed > 0 {
		m.scheduledSlotsElapsed.Add(float64(elapsed))
	}
	e.publishScheduleGauges()
}

// gaugeEpoch is the epoch the schedule gauges describe: the epoch of the
// slot clock's last reading, or the ledger's current epoch before the first.
func (e *Election) gaugeEpoch() (epoch, lastSlot uint64, observed bool) {
	e.obsMu.Lock()
	observed, lastSlot = e.obs.started, e.obs.last
	e.obsMu.Unlock()
	if observed {
		if epoch, err := e.epochProvider.EpochForSlot(lastSlot); err == nil {
			return epoch, lastSlot, true
		}
	}
	return e.epochProvider.CurrentEpoch(), lastSlot, observed
}

// publishScheduleGauges sets the current-epoch schedule gauges from the
// cached schedule of the current epoch, when there is one, and the
// next-leader-slot gauge when the slot clock has been read. The schedule
// gauges keep their last values until the current epoch's schedule is cached.
func (e *Election) publishScheduleGauges() {
	m := e.metrics
	if m == nil {
		return
	}
	epoch, lastSlot, observed := e.gaugeEpoch()
	e.mu.RLock()
	schedule := e.schedules[epoch]
	e.mu.RUnlock()
	if schedule != nil {
		m.scheduleEpoch.Set(float64(epoch))
		m.scheduleLeaderSlots.Set(float64(schedule.SlotCount()))
	}
	if observed {
		next, _ := e.NextLeaderSlot(lastSlot + 1)
		m.nextLeaderSlot.Set(float64(next))
	}
}
