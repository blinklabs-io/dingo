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

package dingo

import "sync/atomic"

// nodeHealth holds the cheap always-available signals the node's readiness
// probe classifies. It is a value field on Node rather than a pointer, and
// every method is safe on the zero value, so a Node built directly in a
// test needs no extra initialization.
//
// A live database Restore or Truncate replaces n.ledgerState; this struct
// deliberately does not, so the probe never has to chase a pointer another
// goroutine is swapping. The rebuilt ledger reports into the same struct
// because ledgerStateConfig closes over n, not over the ledger.
type nodeHealth struct {
	tipGapSlots atomic.Uint64
	tipGapKnown atomic.Bool
}

// recordTipGap stores the wall-clock-to-tip distance observed on a slot tick.
func (h *nodeHealth) recordTipGap(gapSlots uint64) {
	if h == nil {
		return
	}
	h.tipGapSlots.Store(gapSlots)
	h.tipGapKnown.Store(true)
}

// TipGapSlots reports the distance in slots between the wall-clock slot and
// the node's chain tip, as observed on the most recent slot-clock tick. The
// second return is false until the node has processed its first tick, which
// covers database open, Mithril bootstrap and ledger startup.
//
// This is the same quantity the dingo_tip_gap_slots gauge exports, read
// directly so a health probe does not depend on the Prometheus listener.
func (n *Node) TipGapSlots() (uint64, bool) {
	if n == nil {
		return 0, false
	}
	if !n.health.tipGapKnown.Load() {
		return 0, false
	}
	return n.health.tipGapSlots.Load(), true
}
