// Copyright 2024 Blink Labs Software
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

package peergov

import (
	ouroboros "github.com/blinklabs-io/gouroboros"
)

const (
	OutboundConnectionEventType     = "peergov.outbound_conn"
	PeerDemotedEventType            = "peergov.peer_demoted"
	PeerPromotedEventType           = "peergov.peer_promoted"
	PeerRemovedEventType            = "peergov.peer_removed"
	PeerAddedEventType              = "peergov.peer_added"
	PeerEligibilityChangedEventType = "peergov.peer_eligibility_changed"
	PeerPriorityChangedEventType    = "peergov.peer_priority_changed"
	// Phase 7: Enhanced Observability event types
	PeerChurnEventType   = "peergov.peer_churn"
	QuotaStatusEventType = "peergov.quota_status"
	// Phase 5: Bootstrap Peer Lifecycle event types
	BootstrapExitedEventType   = "peergov.bootstrap_exited"
	BootstrapRecoveryEventType = "peergov.bootstrap_recovery"
)

type OutboundConnectionEvent struct {
	ConnectionId ouroboros.ConnectionId
}

type PeerStateChangeEvent struct {
	Address string
	Reason  string
}

type PeerEligibilityChangedEvent struct {
	ConnectionId ouroboros.ConnectionId
	Eligible     bool
}

type PeerPriorityChangedEvent struct {
	ConnectionId ouroboros.ConnectionId
	Priority     int
}

// PeerChurnEvent provides detailed information about a peer state change
// during churn operations. This event contains all the information needed
// for debugging peer selection decisions.
type PeerChurnEvent struct {
	Address  string  // Peer address (host:port)
	Source   string  // Peer source (gossip, ledger, topology-public-root, etc.)
	OldState string  // Previous state (cold, warm, hot)
	NewState string  // New state after churn
	Score    float64 // Performance score at time of churn
	Reason   string  // Reason for state change (gossip churn, public root churn, etc.)
}

// QuotaStatusEvent reports inbound and per-category quota posture.
// This is published during reconciliation to help operators monitor
// configured budgets and current usage.
type QuotaStatusEvent struct {
	// Configured inbound budgets (phase 1 control surface).
	InboundWarmTarget int
	InboundHotQuota   int
	// Current inbound usage. InboundWarm/InboundHot exclude peers still
	// inside the InboundProvisionalWindow so flash-connects cannot
	// inflate reported budget usage.
	InboundWarm   int
	InboundHot    int
	InboundPruned int // Cumulative count since process start
	// Inbound admission metadata (phase 2).
	// InboundTopologyMatched is the current number of peers whose
	// inbound arrival was identified as a configured topology peer.
	// InboundDuplex is the current number of inbound peers on
	// full-duplex connections.
	InboundTopologyMatched int
	InboundDuplex          int
	// Existing active-tier category view.
	TopologyHot int // Hot peers from topology sources (local + public roots)
	GossipHot   int // Hot peers from gossip
	LedgerHot   int // Hot peers from ledger
	OtherHot    int // Hot peers from other sources (unknown)
	TotalHot    int // Total hot peers
}

// BootstrapExitedEvent is published when bootstrap peers are exited.
// This provides details about the exit including the reason and number of demoted peers.
type BootstrapExitedEvent struct {
	Reason       string // Why bootstrap mode was exited
	DemotedPeers int    // Number of bootstrap peers demoted to cold
}

// BootstrapRecoveryEvent is published when bootstrap peers are re-enabled.
// This happens when hot peer count drops below MinHotPeers and no other
// peers are available to promote.
type BootstrapRecoveryEvent struct {
	HotPeerCount int // Current hot peer count at recovery time
	MinHotPeers  int // Configured minimum hot peers threshold
}
