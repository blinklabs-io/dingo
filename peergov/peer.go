// Copyright 2025 Blink Labs Software
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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	oprotocol "github.com/blinklabs-io/gouroboros/protocol"
)

type PeerSource uint16

const (
	PeerSourceUnknown               = 0
	PeerSourceTopologyLocalRoot     = 1
	PeerSourceTopologyPublicRoot    = 2
	PeerSourceTopologyBootstrapPeer = 3
	PeerSourceP2PLedger             = 4
	PeerSourceP2PGossip             = 5
	PeerSourceInboundConn           = 6
)

// String returns a human-readable name for the peer source.
func (s PeerSource) String() string {
	switch s {
	case PeerSourceTopologyLocalRoot:
		return "topology-local-root"
	case PeerSourceTopologyPublicRoot:
		return "topology-public-root"
	case PeerSourceTopologyBootstrapPeer:
		return "topology-bootstrap"
	case PeerSourceP2PLedger:
		return "ledger"
	case PeerSourceP2PGossip:
		return "gossip"
	case PeerSourceInboundConn:
		return "inbound"
	default:
		return "unknown"
	}
}

type PeerState uint16

const (
	PeerStateCold PeerState = iota
	PeerStateWarm
	PeerStateHot
)

// String returns a human-readable name for the peer state.
func (s PeerState) String() string {
	switch s {
	case PeerStateCold:
		return "cold"
	case PeerStateWarm:
		return "warm"
	case PeerStateHot:
		return "hot"
	default:
		return "unknown"
	}
}

type TestResult uint8

const (
	TestResultUnknown TestResult = iota
	TestResultPass
	TestResultFail
)

type Peer struct {
	LastActivity       time.Time
	LastTestTime       time.Time // When peer was last tested for suitability
	LastBlockFetchTime time.Time // Timestamp of last observed block fetch
	FirstSeen          time.Time // When peer was first seen (used for tenure calculation)
	Connection         *PeerConnection
	Address            string
	NormalizedAddress  string // Cached normalized form of Address for deduplication
	// Performance metrics (used by the peer scoring system)
	BlockFetchLatencyMs     float64 // Average block fetch latency in ms (EMA)
	BlockFetchSuccessRate   float64 // Average block fetch success rate 0..1 (EMA)
	ConnectionStability     float64 // Connection stability score 0..1
	ReconnectDelay          time.Duration
	PerformanceScore        float64 // Composite score from the above metrics
	ReconnectCount          int
	State                   PeerState
	Source                  PeerSource
	Sharable                bool
	BlockFetchLatencyInit   bool       // Whether latency has been initialized
	BlockFetchSuccessInit   bool       // Whether success rate has been initialized
	ConnectionStabilityInit bool       // Whether stability has been initialized
	LastTestResult          TestResult // Result of last suitability test

	// ChainSync performance metrics
	HeaderArrivalRate     float64   // Headers received per second during sync (EMA)
	TipSlotDelta          int64     // TipSlotDelta = ourTip - peerTip (negative = peer ahead, positive = we are ahead)
	ChainSyncLastUpdate   time.Time // Last chainsync observation timestamp
	HeaderArrivalRateInit bool      // Whether header rate has been initialized
	TipSlotDeltaInit      bool      // Whether tip delta has been initialized

	// EMA configuration (0 means use default)
	EMAAlpha float64

	// Topology valency configuration (only used for topology-sourced peers)
	// Valency is the target number of hot connections from this peer's group
	// WarmValency is the target number of warm connections from this peer's group
	Valency     uint
	WarmValency uint
	// GroupID identifies the topology group this peer belongs to (for valency tracking)
	GroupID string
}

func (p *Peer) setConnection(conn *ouroboros.Connection, outbound bool) {
	connId := conn.Id()
	protoVersion, versionData := conn.ProtocolVersion()
	p.Connection = &PeerConnection{
		Id:              connId,
		ProtocolVersion: uint(protoVersion),
		VersionData:     versionData,
	}
	// Determine whether connection can be used as a client
	// This should be true for any outbound connections and any inbound
	// connections in full-duplex mode
	if p.Connection != nil && (outbound ||
		versionData.DiffusionMode() == oprotocol.DiffusionModeInitiatorAndResponder) {
		p.Connection.IsClient = true
	}
}

type PeerConnection struct {
	Id              ouroboros.ConnectionId
	VersionData     oprotocol.VersionData
	ProtocolVersion uint
	IsClient        bool
}
