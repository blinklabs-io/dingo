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
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	PeerTipUpdateEventType  event.EventType = "chainselection.peer_tip_update"
	PeerActivityEventType   event.EventType = "chainselection.peer_activity"
	PeerRollbackEventType   event.EventType = "chainselection.peer_rollback"
	ChainSwitchEventType    event.EventType = "chainselection.chain_switch"
	ChainSelectionEventType event.EventType = "chainselection.selection"
	PeerEvictedEventType    event.EventType = "chainselection.peer_evicted"

	// GenesisCorroborationFailedEventType is published when the densest
	// Genesis fast source cannot be corroborated by the configured minimum
	// number of independent peers, so it is denied chain selection (stalls)
	// rather than steering the local chain. Subscribers (peer governance,
	// operators) can use this to demote or investigate the source.
	GenesisCorroborationFailedEventType event.EventType = "chainselection.genesis_corroboration_failed"

	// GenesisModeExitedEventType is published when the selector transitions
	// from Genesis density-based selection back to Praos selection because the
	// local tip has caught up to within the Genesis window of the best known
	// peer tip.
	GenesisModeExitedEventType event.EventType = "chainselection.genesis_mode_exited"
)

// PeerTipUpdateEvent is published when a peer's chain tip is updated via
// chainsync roll forward.
type PeerTipUpdateEvent struct {
	ConnectionId ouroboros.ConnectionId
	Tip          ochainsync.Tip
	ObservedTip  ochainsync.Tip
	VRFOutput    []byte // VRF output from observed block header for tie-breaking
	PraosView    PraosTiebreakerView
}

// PeerActivityEvent is published when a peer has recent protocol activity
// (for example, a keepalive response) without a tip change. This refreshes
// selector liveness for healthy but temporarily quiet peers.
type PeerActivityEvent struct {
	ConnectionId ouroboros.ConnectionId
}

// PeerRollbackEvent is published when an ingress-eligible chainsync peer
// reports a rollback. Point is the rollback point; Tip is the peer's current
// chainsync tip after the rollback.
type PeerRollbackEvent struct {
	ConnectionId ouroboros.ConnectionId
	Point        ocommon.Point
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
//   - ComparisonResult: Why the new chain is better than the previous chain.
//   - BlockDifference: NewTip.BlockNumber - PreviousTip.BlockNumber.
type ChainSwitchEvent struct {
	PreviousConnectionId ouroboros.ConnectionId
	NewConnectionId      ouroboros.ConnectionId
	NewTip               ochainsync.Tip
	PreviousTip          ochainsync.Tip
	ComparisonResult     ChainComparisonResult
	BlockDifference      int64
}

// ChainSelectionEvent is published when chain selection evaluation completes.
type ChainSelectionEvent struct {
	BestConnectionId ouroboros.ConnectionId
	BestTip          ochainsync.Tip
	PeerCount        int
	SwitchOccurred   bool
}

// PeerEvictedEvent is published when a tracked peer is evicted from the
// chain selector to make room for a new peer. Subscribers (e.g. connection
// manager) can use this to close the evicted peer's connection.
type PeerEvictedEvent struct {
	ConnectionId ouroboros.ConnectionId
}

// GenesisCorroborationFailedEvent is published when the densest Genesis fast
// source lacks the configured minimum corroboration from independent peers.
//
// Fields:
//   - ConnectionId: the uncorroborated fast source that was denied selection.
//   - ObservedDensity: its observed block density within the Genesis window.
//   - CorroboratingPeers: how many independent peers actually corroborate it.
//   - RequiredPeers: the configured MinCorroboratingPeers threshold.
//   - GenesisWindowSlots: the active Genesis density window in slots.
type GenesisCorroborationFailedEvent struct {
	ConnectionId       ouroboros.ConnectionId
	ObservedDensity    uint64
	CorroboratingPeers int
	RequiredPeers      int
	GenesisWindowSlots uint64
}

// GenesisModeExitedEvent is published when the selector leaves Genesis mode.
//
// Fields:
//   - LocalSlot: the local tip slot at the time of exit.
//   - BestKnownSlot: the best known selectable peer tip slot.
//   - GenesisWindowSlots: the active Genesis density window in slots.
type GenesisModeExitedEvent struct {
	LocalSlot          uint64
	BestKnownSlot      uint64
	GenesisWindowSlots uint64
}
