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

package chainselection

import (
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// PeerChainTip tracks the chain tip reported by a specific peer.
type PeerChainTip struct {
	ConnectionId  ouroboros.ConnectionId
	Tip           ochainsync.Tip
	ObservedTip   ochainsync.Tip
	VRFOutput     []byte // VRF output from tip block for tie-breaking
	LastUpdated   time.Time
	observedSlots []uint64
}

// NewPeerChainTip creates a new PeerChainTip with the given connection ID,
// tip, and VRF output.
func NewPeerChainTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
) *PeerChainTip {
	return &PeerChainTip{
		ConnectionId: connId,
		Tip:          tip,
		ObservedTip:  tip,
		VRFOutput:    vrfOutput,
		LastUpdated:  time.Now(),
	}
}

// UpdateTip updates the peer's chain tip, VRF output, and last updated timestamp.
func (p *PeerChainTip) UpdateTip(tip ochainsync.Tip, vrfOutput []byte) {
	p.UpdateTipWithObserved(tip, tip, vrfOutput)
}

// UpdateTipWithObserved updates both the remote advertised tip and the latest
// locally observed frontier for the peer.
func (p *PeerChainTip) UpdateTipWithObserved(
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
) {
	p.Tip = tip
	p.ObservedTip = observedTip
	p.VRFOutput = vrfOutput
	p.LastUpdated = time.Now()
}

// ApplyRollback trims observed history at the rollback point and refreshes the
// peer tip to the chainsync tip reported with the rollback.
func (p *PeerChainTip) ApplyRollback(
	point ocommon.Point,
	tip ochainsync.Tip,
) {
	if p == nil {
		return
	}
	p.Tip = tip
	p.ObservedTip = tip
	p.VRFOutput = nil
	p.LastUpdated = time.Now()
	if point.Slot == 0 || len(p.observedSlots) == 0 {
		p.observedSlots = nil
		return
	}

	keepUntil := 0
	for keepUntil < len(p.observedSlots) &&
		p.observedSlots[keepUntil] <= point.Slot {
		keepUntil++
	}
	if keepUntil == 0 {
		p.observedSlots = nil
		return
	}
	p.observedSlots = p.observedSlots[:keepUntil]
}

func (p *PeerChainTip) recordObservedSlot(slot, window uint64) {
	if p == nil {
		return
	}
	if slot == 0 {
		p.observedSlots = nil
		return
	}

	// Keep the history monotonic and bounded even if the observed frontier
	// rolls back. Genesis mode only needs the recent slot frontier, not the
	// full chain history.
	for len(p.observedSlots) > 0 &&
		p.observedSlots[len(p.observedSlots)-1] > slot {
		p.observedSlots = p.observedSlots[:len(p.observedSlots)-1]
	}
	if len(p.observedSlots) == 0 ||
		p.observedSlots[len(p.observedSlots)-1] < slot {
		p.observedSlots = append(p.observedSlots, slot)
	}

	if window == 0 {
		if len(p.observedSlots) > 1 {
			p.observedSlots = p.observedSlots[len(p.observedSlots)-1:]
		}
		return
	}

	var cutoff uint64
	if slot > window {
		cutoff = slot - window + 1
	} else {
		cutoff = 1
	}
	pruneIdx := 0
	for pruneIdx < len(p.observedSlots) &&
		p.observedSlots[pruneIdx] < cutoff {
		pruneIdx++
	}
	if pruneIdx > 0 {
		p.observedSlots = p.observedSlots[pruneIdx:]
	}
}

func (p *PeerChainTip) observedDensity(window uint64) uint64 {
	if p == nil || len(p.observedSlots) == 0 {
		return 0
	}
	if window == 0 {
		return uint64(len(p.observedSlots))
	}

	latestSlot := p.observedSlots[len(p.observedSlots)-1]
	var cutoff uint64
	if latestSlot > window {
		cutoff = latestSlot - window + 1
	} else {
		cutoff = 1
	}
	for i, slot := range p.observedSlots {
		if slot >= cutoff {
			// #nosec G115 -- i < len(p.observedSlots), difference is non-negative
			return uint64(len(p.observedSlots) - i)
		}
	}
	return 0
}

// SelectionTip returns the best locally observed frontier for this peer.
// When available, prefer the latest block the peer has actually delivered to
// us over its remote advertised tip. This avoids switching to peers whose
// far-end tip is high while their chainsync cursor is still lagging.
func (p *PeerChainTip) SelectionTip() ochainsync.Tip {
	if p == nil {
		return ochainsync.Tip{}
	}
	if p.ObservedTip.BlockNumber > 0 || p.ObservedTip.Point.Slot > 0 ||
		len(p.ObservedTip.Point.Hash) > 0 {
		return p.ObservedTip
	}
	return p.Tip
}

// Touch marks the peer as recently active without changing its advertised tip.
func (p *PeerChainTip) Touch() {
	p.LastUpdated = time.Now()
}

// IsStale returns true if the peer's tip hasn't been updated within the given
// duration.
func (p *PeerChainTip) IsStale(threshold time.Duration) bool {
	return time.Since(p.LastUpdated) > threshold
}
