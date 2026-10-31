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
	"reflect"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/require"
)

func TestGenesisControlGroupsCanonicalDefaults(t *testing.T) {
	t.Parallel()
	for _, network := range []string{"dijkstra", "musashi"} {
		t.Run(network, func(t *testing.T) {
			t.Parallel()
			cfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
				cardano.EmbeddedConfigFS,
				cardano.EmbeddedConfigPath(network),
			)
			require.NoError(t, err)
			n := &Node{
				config: Config{cardanoNodeConfig: cfg, genesisBootstrap: true},
			}
			n.config.networkMagic = cfg.ShelleyGenesis().NetworkMagic
			groups := n.genesisControlGroups()
			if network != "dijkstra" {
				require.Empty(t, groups)
				return
			}
			require.Len(t, groups, 2)
			require.Len(t, groups[0].AccessPoints, 1)
			require.Len(t, groups[1].AccessPoints, 2)
			for _, g := range groups {
				require.NoError(t, g.Validate())
			}
			groups[0].Identity = "mutated"
			require.Equal(t, "iog", n.genesisControlGroups()[0].Identity)
			for _, name := range []string{"ShelleyGenesisHash", "ByronGenesisHash", "AlonzoGenesisHash", "ConwayGenesisHash", "DijkstraGenesisHash"} {
				field := reflect.ValueOf(cfg).Elem().FieldByName(name)
				original := field.String()
				field.SetString("private genesis")
				require.Empty(
					t,
					n.genesisControlGroups(),
					"private %s cannot inherit public trust",
					name,
				)
				field.SetString(original)
			}
			n.config.cfg = NewConfig().cfg
			WithGenesisControlGroups([]topology.PeerControlGroup{})(&n.config)
			require.Empty(
				t,
				n.genesisControlGroups(),
				"explicit empty list overrides canonical defaults",
			)
			WithGenesisControlGroups(nil)(&n.config)
			require.Len(t, n.genesisControlGroups(), 2, "nil restores defaults")
			n.config.genesisBootstrap = false
			require.Empty(
				t,
				n.genesisControlGroups(),
				"Praos does not add default roots",
			)
		})
	}
}

func TestGenesisControlGroupsPublicNetworkDefaults(t *testing.T) {
	t.Parallel()
	want := map[string][]string{
		"mainnet": {"iog", "cardano-foundation", "blinklabs"},
		"preprod": {"iog", "blinklabs"},
		"preview": {"iog", "blinklabs"},
	}
	for network, identities := range want {
		t.Run(network, func(t *testing.T) {
			t.Parallel()
			cfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
				cardano.EmbeddedConfigFS,
				cardano.EmbeddedConfigPath(network),
			)
			require.NoError(t, err)
			n := &Node{
				config: Config{cardanoNodeConfig: cfg, genesisBootstrap: true},
			}
			n.config.networkMagic = cfg.ShelleyGenesis().NetworkMagic
			groups := n.genesisControlGroups()
			var got []string
			for _, g := range groups {
				require.NoError(t, g.Validate())
				got = append(got, g.Identity)
			}
			require.Equal(t, identities, got)
			for _, name := range []string{"ShelleyGenesisHash", "ByronGenesisHash", "AlonzoGenesisHash", "ConwayGenesisHash"} {
				field := reflect.ValueOf(cfg).Elem().FieldByName(name)
				original := field.String()
				field.SetString("private genesis")
				require.Empty(
					t,
					n.genesisControlGroups(),
					"private %s cannot inherit public trust",
					name,
				)
				field.SetString(original)
			}
			n.config.networkMagic++
			require.Empty(
				t,
				n.genesisControlGroups(),
				"a reused magic cannot inherit public trust",
			)
			n.config.networkMagic--
			n.config.genesisBootstrap = false
			require.Empty(
				t,
				n.genesisControlGroups(),
				"Praos does not add default roots",
			)
		})
	}
}

func TestEffectiveGenesisCorroborationPeers(t *testing.T) {
	t.Parallel()
	newNode := func(t *testing.T, network string, peers int) *Node {
		t.Helper()
		cfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
			cardano.EmbeddedConfigFS,
			cardano.EmbeddedConfigPath(network),
		)
		require.NoError(t, err)
		n := &Node{config: Config{
			cardanoNodeConfig:         cfg,
			genesisBootstrap:          true,
			genesisCorroborationPeers: peers,
		}}
		n.config.networkMagic = cfg.ShelleyGenesis().NetworkMagic
		return n
	}
	t.Run("single operator network is density only", func(t *testing.T) {
		t.Parallel()
		n := newNode(t, "prime-testnet", 1)
		require.Zero(t, n.effectiveGenesisCorroborationPeers())
		require.Empty(t, n.genesisControlGroups())
	})
	t.Run("multi operator networks keep the threshold", func(t *testing.T) {
		t.Parallel()
		for _, network := range []string{"mainnet", "preprod", "preview", "dijkstra"} {
			require.Equal(
				t,
				1,
				newNode(t, network, 1).effectiveGenesisCorroborationPeers(),
				network,
			)
		}
	})
	t.Run("explicit control groups keep the threshold", func(t *testing.T) {
		t.Parallel()
		n := newNode(t, "prime-testnet", 1)
		n.config.cfg = NewConfig().cfg
		WithGenesisControlGroups([]topology.PeerControlGroup{})(&n.config)
		require.Equal(t, 1, n.effectiveGenesisCorroborationPeers())
	})
	t.Run("private genesis keeps the threshold", func(t *testing.T) {
		t.Parallel()
		n := newNode(t, "prime-testnet", 1)
		n.config.cardanoNodeConfig.ShelleyGenesisHash = "private genesis"
		require.Equal(t, 1, n.effectiveGenesisCorroborationPeers())
	})
	t.Run("a reused magic keeps the threshold", func(t *testing.T) {
		t.Parallel()
		n := newNode(t, "prime-testnet", 1)
		n.config.networkMagic++
		require.Equal(t, 1, n.effectiveGenesisCorroborationPeers())
	})
	t.Run("configured zero stays zero", func(t *testing.T) {
		t.Parallel()
		require.Zero(t, newNode(t, "mainnet", 0).effectiveGenesisCorroborationPeers())
	})
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
