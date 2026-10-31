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
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/topology"
)

// canonicalGenesisNetwork pins a public network's identity and the relay
// operators that default to independent Genesis corroboration witnesses.
type canonicalGenesisNetwork struct {
	magic       uint32
	shelleyHash string
	suffix      string
	groups      []topology.PeerControlGroup
}

func blinkRelayGroup(suffix string) topology.PeerControlGroup {
	return topology.PeerControlGroup{
		Identity: "blinklabs",
		AccessPoints: []topology.TopologyConfigP2PAccessPoint{
			{Address: "relay1." + suffix + ".blinklabs.cloud", Port: 3001},
			{Address: "relay2." + suffix + ".blinklabs.cloud", Port: 3001},
		},
	}
}

func upstreamRelayGroup(
	identity, address string,
) topology.PeerControlGroup {
	return topology.PeerControlGroup{
		Identity: identity,
		AccessPoints: []topology.TopologyConfigP2PAccessPoint{
			{Address: address, Port: 3001},
		},
	}
}

func canonicalGenesisNetworks() []canonicalGenesisNetwork {
	return []canonicalGenesisNetwork{
		{
			magic:       6,
			shelleyHash: "b47eb627b52bf9cfe5ffd865658b47b5747a088d150a9c27216de374e836c385",
			suffix:      "dijkstra",
			groups: []topology.PeerControlGroup{
				upstreamRelayGroup("iog", "dijkstra-node.play.dev.cardano.org"),
				blinkRelayGroup("dijkstra"),
			},
		},
		{
			magic:       164,
			shelleyHash: "d1b19e006c1198fb7ac9d53073c1a9f77acb1a9474c92310006ae4418290cda3",
			suffix:      "musashi",
			groups: []topology.PeerControlGroup{
				upstreamRelayGroup("iog", "leios-node.play.dev.cardano.org"),
				blinkRelayGroup("musashi"),
			},
		},
		{
			magic:       764824073,
			shelleyHash: "1a3be38bcbb7911969283716ad7aa550250226b76a61fc51cc9a9a35d9276d81",
			suffix:      "mainnet",
			groups: []topology.PeerControlGroup{
				upstreamRelayGroup("iog", "backbone.cardano.iog.io"),
				upstreamRelayGroup(
					"cardano-foundation",
					"backbone.mainnet.cardanofoundation.org",
				),
				blinkRelayGroup("mainnet"),
			},
		},
		{
			magic:       1,
			shelleyHash: "162d29c4e1cf6b8a84f2d692e67a3ac6bc7851bc3e6e4afe64d15778bed8bd86",
			suffix:      "preprod",
			groups: []topology.PeerControlGroup{
				upstreamRelayGroup("iog", "preprod-node.play.dev.cardano.org"),
				blinkRelayGroup("preprod"),
			},
		},
		{
			magic:       2,
			shelleyHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
			suffix:      "preview",
			groups: []topology.PeerControlGroup{
				upstreamRelayGroup("iog", "preview-node.play.dev.cardano.org"),
				blinkRelayGroup("preview"),
			},
		},
	}
}

// genesisControlGroups keeps Dingo's discovery policy separate from the
// canonical cardano-node configuration. Pin defaults to the genesis identity
// so private networks reusing a magic do not inherit public relay trust. A
// node syncing from genesis has no ledger or peer snapshot identities and
// dials only bootstrap peers, so without these defaults Genesis corroboration
// fails closed and chain selection never starts.
func (n *Node) genesisControlGroups() []topology.PeerControlGroup {
	if n.config.cfg != nil {
		if groups := n.config.cfg.GenesisBootstrap.ControlGroups; groups != nil {
			return topology.ClonePeerControlGroups(groups)
		}
	}
	cfg := n.config.CardanoNodeConfig()
	if cfg == nil || !n.config.genesisSelectionMode() {
		return nil
	}
	for _, network := range canonicalGenesisNetworks() {
		if n.config.networkMagic != network.magic ||
			cfg.ShelleyGenesisHash != network.shelleyHash {
			continue
		}
		canonical, err := cardano.NewCardanoNodeConfigFromEmbedFS(
			cardano.EmbeddedConfigFS,
			cardano.EmbeddedConfigPath(network.suffix),
		)
		if err != nil || cfg.ByronGenesisHash != canonical.ByronGenesisHash ||
			cfg.AlonzoGenesisHash != canonical.AlonzoGenesisHash ||
			cfg.ConwayGenesisHash != canonical.ConwayGenesisHash ||
			cfg.DijkstraGenesisHash != canonical.DijkstraGenesisHash {
			return nil
		}
		return topology.ClonePeerControlGroups(network.groups)
	}
	return nil
}
