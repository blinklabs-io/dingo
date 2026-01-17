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
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const (
	PeerTipUpdateEventType  event.EventType = "chainselection.peer_tip_update"
	ChainSwitchEventType    event.EventType = "chainselection.chain_switch"
	ChainSelectionEventType event.EventType = "chainselection.selection"
)

// PeerTipUpdateEvent is published when a peer's chain tip is updated via
// chainsync roll forward.
type PeerTipUpdateEvent struct {
	ConnectionId ouroboros.ConnectionId
	Tip          ochainsync.Tip
}

// ChainSwitchEvent is published when the chain selector decides to switch
// to a different peer's chain.
//
// Fields:
//   - PreviousConnectionId: The connection ID of the peer we were following.
//   - NewConnectionId: The connection ID of the peer we are now following.
//   - NewTip: The chain tip of the new peer.
//   - PreviousTip: The chain tip of the previous peer at the time of the switch.
type ChainSwitchEvent struct {
	PreviousConnectionId ouroboros.ConnectionId
	NewConnectionId      ouroboros.ConnectionId
	NewTip               ochainsync.Tip
	PreviousTip          ochainsync.Tip
}

// ChainSelectionEvent is published when chain selection evaluation completes.
type ChainSelectionEvent struct {
	BestConnectionId ouroboros.ConnectionId
	BestTip          ochainsync.Tip
	PeerCount        int
	SwitchOccurred   bool
}
