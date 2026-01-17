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
	ConnectionId ouroboros.ConnectionId
	Tip          ochainsync.Tip
	LastUpdated  time.Time
}

// NewPeerChainTip creates a new PeerChainTip with the given connection ID and
// tip.
func NewPeerChainTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
) *PeerChainTip {
	return &PeerChainTip{
		ConnectionId: connId,
		Tip:          tip,
		LastUpdated:  time.Now(),
	}
}

// UpdateTip updates the peer's chain tip and last updated timestamp.
func (p *PeerChainTip) UpdateTip(tip ochainsync.Tip) {
	p.Tip = tip
	p.LastUpdated = time.Now()
}

// IsStale returns true if the peer's tip hasn't been updated within the given
// duration.
func (p *PeerChainTip) IsStale(threshold time.Duration) bool {
	return time.Since(p.LastUpdated) > threshold
}
