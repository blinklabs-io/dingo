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
)

// PeerChainTip tracks the chain tip reported by a specific peer.
type PeerChainTip struct {
	ConnectionId   ouroboros.ConnectionId
	Tip            ochainsync.Tip
	ObservedTip    ochainsync.Tip
	ObservedTipSet bool   // true when ObservedTip has been explicitly set
	VRFOutput      []byte // VRF output from tip block for tie-breaking
	LastUpdated    time.Time
}

// NewPeerChainTip creates a new PeerChainTip with the given connection ID,
// tip, and VRF output.
func NewPeerChainTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
) *PeerChainTip {
	return &PeerChainTip{
		ConnectionId:   connId,
		Tip:            tip,
		ObservedTip:    tip,
		ObservedTipSet: true,
		VRFOutput:      vrfOutput,
		LastUpdated:    time.Now(),
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
	p.ObservedTipSet = true
	p.VRFOutput = vrfOutput
	p.LastUpdated = time.Now()
}

// SelectionTip returns the best locally observed frontier for this peer.
// When available, prefer the latest block the peer has actually delivered to
// us over its remote advertised tip. This avoids switching to peers whose
// far-end tip is high while their chainsync cursor is still lagging.
func (p *PeerChainTip) SelectionTip() ochainsync.Tip {
	if p == nil {
		return ochainsync.Tip{}
	}
	if p.ObservedTipSet {
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
