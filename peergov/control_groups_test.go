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

package peergov

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func controlTestConn(n int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000 + n},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("44.0.0.1"), Port: 3001},
	}
}

func TestConfiguredControlGroupsExclusiveAndStable(t *testing.T) {
	t.Parallel()
	groups := []topology.PeerControlGroup{
		{
			Identity: "first",
			AccessPoints: []topology.TopologyConfigP2PAccessPoint{
				{Address: "44.0.0.1", Port: 3001},
				{Address: "44.0.0.2", Port: 3001},
			},
		},
		{
			Identity: "second",
			AccessPoints: []topology.TopologyConfigP2PAccessPoint{
				{Address: "45.0.0.1", Port: 3001},
			},
		},
	}
	pg := NewPeerGovernor(PeerGovernorConfig{ControlGroups: groups})
	groups[0].Identity = "mutated"
	groups[0].AccessPoints[0].Address = "46.0.0.1"
	canonical := &topology.TopologyConfig{}
	pg.LoadTopologyConfig(canonical)
	require.Empty(
		t,
		canonical.LocalRoots,
		"overlay must not mutate callers' topology",
	)
	require.Len(t, pg.peers, 3)
	ids := make(map[string]ouroboros.ConnectionId)
	for i, peer := range pg.peers {
		conn := controlTestConn(i)
		peer.Connection = &PeerConnection{Id: conn, IsClient: true}
		ids[peer.Address] = conn
	}
	first := pg.CorroborationIdentityByConnId(ids["44.0.0.1:3001"])
	require.Equal(t, "configured:first", first)
	require.Equal(
		t,
		first,
		pg.CorroborationIdentityByConnId(ids["44.0.0.2:3001"]),
	)
	require.Equal(
		t,
		"configured:second",
		pg.CorroborationIdentityByConnId(ids["45.0.0.1:3001"]),
	)

	// Discovery refresh may remove ledger evidence; configured roots keep their
	// independent provenance and the unknown pool cannot become a third operator.
	unknown := controlTestConn(20)
	pg.peers = append(
		pg.peers,
		&Peer{
			Address:               "47.0.0.1:3001",
			CorroborationIdentity: "pool:other",
			Connection:            &PeerConnection{Id: unknown, IsClient: true},
		},
	)
	pg.reconcileLedgerCorroborationIdentities(
		map[string]string{"47.0.0.1:3001": "pool:other"},
	)
	require.Empty(t, pg.CorroborationIdentityByConnId(unknown))
	require.Equal(
		t,
		first,
		pg.CorroborationIdentityByConnId(ids["44.0.0.1:3001"]),
	)
	pg.LoadTopologyConfig(canonical)
	require.Equal(
		t,
		first,
		pg.CorroborationIdentityByConnId(ids["44.0.0.1:3001"]),
	)
}

func TestConfiguredControlGroupsBindTrustedOrigin(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(
		PeerGovernorConfig{ControlGroups: []topology.PeerControlGroup{
			{
				Identity: "first",
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "first.example", Port: 3001},
				},
			},
			{
				Identity: "second",
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "second.example", Port: 3001},
				},
			},
		}},
	)
	conn := controlTestConn(1)
	peer := &Peer{
		Address:    "44.0.0.1:3001",
		Connection: &PeerConnection{Id: conn, IsClient: true},
	}
	pg.peers = []*Peer{peer}
	// A connected IP, even equal to a configured endpoint's DNS result, cannot
	// attest its own origin. Only trusted relay discovery supplies that mapping.
	require.Empty(t, pg.CorroborationIdentityByConnId(conn))
	pg.ledgerKnownAddrs[peer.Address] = "first.example:3001"
	require.Equal(t, "configured:first", pg.CorroborationIdentityByConnId(conn))
	delete(pg.ledgerKnownAddrs, peer.Address)
	require.Empty(
		t,
		pg.CorroborationIdentityByConnId(conn),
		"delisting revokes trust",
	)
	pg.ledgerKnownAddrs[peer.Address] = "second.example:3001"
	require.Empty(
		t,
		pg.CorroborationIdentityByConnId(conn),
		"a live bearer cannot change operators",
	)
	pg.ledgerKnownAddrs[peer.Address] = "first.example:3001"
	require.Empty(
		t,
		pg.CorroborationIdentityByConnId(conn),
		"conflict stays closed while peer retained",
	)
}

func TestConfiguredControlGroupsResolveSharedSnapshotEndpoint(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(
		PeerGovernorConfig{ControlGroups: []topology.PeerControlGroup{
			{
				Identity: "operator",
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
				},
			},
		}},
	)
	snapshot := &topology.PeerSnapshotConfig{
		Point: topology.PeerSnapshotPoint{BlockPointHash: "trusted"},
		BigLedgerPools: []topology.PeerSnapshotLedgerPool{
			{
				Relays: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
				},
			},
			{
				Relays: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
				},
			},
			{
				Relays: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
				},
			},
		},
	}
	require.Equal(t, 1, pg.LoadPeerSnapshot(context.Background(), snapshot))
	conn := controlTestConn(0)
	pg.peers[0].Connection = &PeerConnection{Id: conn, IsClient: true}
	require.Empty(
		t,
		pg.peers[0].CorroborationIdentity,
		"ambiguous snapshot pool identity stays closed",
	)
	require.Equal(
		t,
		"configured:operator",
		pg.CorroborationIdentityByConnId(conn),
	)
	// The same source changing to ledger discovery cannot change operator label.
	ip := net.ParseIP("44.0.0.1")
	identities := pg.ledgerRelayCorroborationIdentities(
		[]PoolRelay{
			{IPv4: &ip, Port: 3001, PoolKeyHash: bytes.Repeat([]byte{1}, 28)},
		},
	)
	pg.reconcileLedgerCorroborationIdentities(identities)
	require.Equal(
		t,
		"configured:operator",
		pg.CorroborationIdentityByConnId(conn),
	)
}

func TestConfiguredControlGroupsConflictingDeclarationsFailClosed(
	t *testing.T,
) {
	t.Parallel()
	pg := NewPeerGovernor(
		PeerGovernorConfig{ControlGroups: []topology.PeerControlGroup{
			{
				Identity: "first",
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
				},
			},
			{
				Identity: "second",
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
				},
			},
		}},
	)
	pg.LoadTopologyConfig(&topology.TopologyConfig{})
	conn := controlTestConn(0)
	pg.peers[0].Connection = &PeerConnection{Id: conn, IsClient: true}
	require.Empty(t, pg.CorroborationIdentityByConnId(conn))
}

func TestConfiguredControlGroupsRequireIndependentDeliveredWitness(
	t *testing.T,
) {
	t.Parallel()
	pg := NewPeerGovernor(
		PeerGovernorConfig{ControlGroups: []topology.PeerControlGroup{
			{
				Identity: "source",
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
					{Address: "44.0.0.2", Port: 3001},
				},
			},
			{
				Identity: "witness",
				AccessPoints: []topology.TopologyConfigP2PAccessPoint{
					{Address: "45.0.0.1", Port: 3001},
				},
			},
		}},
	)
	pg.LoadTopologyConfig(&topology.TopologyConfig{})
	ids := make(map[string]ouroboros.ConnectionId)
	for i, peer := range pg.peers {
		conn := controlTestConn(i)
		peer.Connection = &PeerConnection{Id: conn, IsClient: true}
		ids[peer.Address] = conn
	}
	source, alias, witness := ids["44.0.0.1:3001"], ids["44.0.0.2:3001"], ids["45.0.0.1:3001"]
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode: true, SecurityParam: 20, GenesisWindowSlots: 100, MinCorroboratingPeers: 1, PeerIdentity: pg.CorroborationIdentityByConnId,
	})
	deliver := func(conn ouroboros.ConnectionId, prefix string) {
		for slot := uint64(910); slot <= 1000; slot += 10 {
			cs.UpdatePeerTip(
				conn,
				ochainsync.Tip{
					Point: ocommon.Point{
						Slot: slot,
						Hash: []byte(fmt.Sprintf("%s%d", prefix, slot)),
					},
					BlockNumber: slot / 10,
				},
				nil,
			)
		}
	}
	status := func() chainselection.GenesisPeerStatus {
		for _, peer := range cs.GenesisStatus().Peers {
			if peer.ConnectionId == source {
				return peer
			}
		}
		t.Fatal("source not observed")
		return chainselection.GenesisPeerStatus{}
	}
	deliver(source, "honest")
	deliver(alias, "honest")
	require.Zero(
		t,
		status().CorroboratingPeers,
		"aliases cannot corroborate their own operator",
	)
	require.False(t, status().Corroborated)
	deliver(witness, "divergent")
	require.False(
		t,
		status().Corroborated,
		"independent but divergent blocks do not authorize ingress",
	)
	cs.RemovePeer(witness)
	deliver(witness, "honest")
	require.Equal(t, 1, status().CorroboratingPeers)
	require.True(
		t,
		status().Corroborated,
		"independent delivery starts Genesis selection",
	)
	cs.RemovePeer(witness)
	require.False(
		t,
		status().Corroborated,
		"loss of the only witness revokes permission",
	)
}
