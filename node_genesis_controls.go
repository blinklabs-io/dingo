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

// genesisControlGroups keeps Dingo's discovery policy separate from the
// canonical cardano-node configuration. Pin defaults to the genesis identity
// so private networks reusing a magic do not inherit public relay trust.
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
	var upstream, suffix string
	switch {
	case n.config.networkMagic == 6 && cfg.ShelleyGenesisHash == "b47eb627b52bf9cfe5ffd865658b47b5747a088d150a9c27216de374e836c385":
		upstream, suffix = "dijkstra-node.play.dev.cardano.org", "dijkstra"
	case n.config.networkMagic == 164 && cfg.ShelleyGenesisHash == "d1b19e006c1198fb7ac9d53073c1a9f77acb1a9474c92310006ae4418290cda3":
		upstream, suffix = "leios-node.play.dev.cardano.org", "musashi"
	default:
		return nil
	}
	canonical, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS, cardano.EmbeddedConfigPath(suffix),
	)
	if err != nil || cfg.ByronGenesisHash != canonical.ByronGenesisHash ||
		cfg.AlonzoGenesisHash != canonical.AlonzoGenesisHash ||
		cfg.ConwayGenesisHash != canonical.ConwayGenesisHash ||
		cfg.DijkstraGenesisHash != canonical.DijkstraGenesisHash {
		return nil
	}
	return []topology.PeerControlGroup{
		{
			Identity: "iog",
			AccessPoints: []topology.TopologyConfigP2PAccessPoint{
				{Address: upstream, Port: 3001},
			},
		},
		{
			Identity: "blinklabs",
			AccessPoints: []topology.TopologyConfigP2PAccessPoint{
				{Address: "relay1." + suffix + ".blinklabs.cloud", Port: 3001},
				{Address: "relay2." + suffix + ".blinklabs.cloud", Port: 3001},
			},
		},
	}
}
