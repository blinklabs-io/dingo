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

type Peer struct {
	Connection     *PeerConnection
	Address        string
	ReconnectCount int
	ReconnectDelay time.Duration
	Source         PeerSource
	Sharable       bool
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
	if outbound ||
		versionData.DiffusionMode() == oprotocol.DiffusionModeInitiatorAndResponder {
		p.Connection.IsClient = true
	}
}

type PeerConnection struct {
	Id              ouroboros.ConnectionId
	VersionData     oprotocol.VersionData
	ProtocolVersion uint
	IsClient        bool
}
