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

import (
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/require"
)

func TestGenesisControlGroupsUsePublishedTopologyByDefault(t *testing.T) {
	t.Parallel()
	for _, network := range []string{
		"dijkstra", "musashi", "preview", "preprod", "mainnet",
	} {
		t.Run(network, func(t *testing.T) {
			t.Parallel()
			cardanoCfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
				cardano.EmbeddedConfigFS, cardano.EmbeddedConfigPath(network),
			)
			require.NoError(t, err)
			cfg := NewConfig()
			cfg.cardanoNodeConfig = cardanoCfg
			cfg.networkMagic = cardanoCfg.ShelleyGenesis().NetworkMagic
			n := &Node{config: cfg}
			require.Zero(t, cfg.cfg.GenesisBootstrap.CorroborationPeers)
			for range 2 {
				pg := n.newPeerGovernor()
				pg.LoadTopologyConfig(&topology.TopologyConfig{})
				require.Empty(
					t,
					pg.GetPeers(),
					"only the supplied topology may add roots",
				)
			}
		})
	}
}

func TestGenesisControlGroupsExplicitOverrideIsOwned(t *testing.T) {
	t.Parallel()
	cfg := NewConfig()
	groups := []topology.PeerControlGroup{
		{
			Identity: "custom",
			AccessPoints: []topology.TopologyConfigP2PAccessPoint{
				{Address: "44.0.0.1", Port: 3001},
			},
		},
	}
	option := WithGenesisControlGroups(groups)
	groups[0].AccessPoints[0].Address = "mutated"
	option(&cfg)
	n := &Node{config: cfg}
	require.Equal(
		t,
		"44.0.0.1",
		n.genesisControlGroups()[0].AccessPoints[0].Address,
	)
	n.config.cfg.GenesisBootstrap.ControlGroups[0].Identity = "changed"
	option(&cfg)
	require.Equal(
		t,
		"custom",
		cfg.cfg.GenesisBootstrap.ControlGroups[0].Identity,
	)
	WithGenesisControlGroups([]topology.PeerControlGroup{})(&n.config)
	require.NotNil(t, n.genesisControlGroups())
	require.Empty(t, n.genesisControlGroups())
}

func TestGenesisControlGroupsReachReplacementGovernors(t *testing.T) {
	t.Parallel()
	cfg := NewConfig(WithGenesisControlGroups([]topology.PeerControlGroup{
		{
			Identity: "operator",
			AccessPoints: []topology.TopologyConfigP2PAccessPoint{
				{Address: "44.0.0.1", Port: 3001},
			},
		},
	}))
	n := &Node{config: cfg}
	for range 2 {
		pg := n.newPeerGovernor()
		pg.LoadTopologyConfig(&topology.TopologyConfig{})
		peers := pg.GetPeers()
		require.Len(
			t,
			peers,
			1,
			"startup and replacement must both receive operator roots",
		)
		require.Equal(t, "44.0.0.1:3001", peers[0].Address)
		require.False(
			t,
			peers[0].StakeKnown,
			"configured roots do not invent stake",
		)
	}
}

func TestChainsyncGenesisIntersectionDoesNotRequireOperatorQuorum(
	t *testing.T,
) {
	t.Parallel()
	for _, genesis := range []bool{false, true} {
		cs := chainselection.NewChainSelector(
			chainselection.ChainSelectorConfig{
				GenesisMode: genesis, SecurityParam: 20,
			},
		)
		n := &Node{chainSelector: cs}
		require.False(t, cs.GenesisCorroborationActive())
		require.Equal(t, genesis, n.chainsyncIntersectAtLedgerTip())
	}
	require.False(t, (&Node{}).chainsyncIntersectAtLedgerTip())
}
