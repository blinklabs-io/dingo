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

package config

import (
	"fmt"

	"github.com/blinklabs-io/node/topology"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	BindAddr      string `split_words:"true"`
	CardanoConfig string `envconfig:"config"`
	DatabasePath  string `split_words:"true"`
	IntersectTip  bool   `split_words:"true"`
	Network       string
	MetricsPort   uint `split_words:"true"`
	Port          uint
	Topology      string
}

var globalConfig = &Config{
	BindAddr:      "0.0.0.0",
	CardanoConfig: "./configs/cardano/preview/config.json",
	DatabasePath:  ".node",
	IntersectTip:  false,
	Network:       "preview",
	MetricsPort:   12798,
	Port:          3001,
	Topology:      "",
}

func LoadConfig() (*Config, error) {
	err := envconfig.Process("cardano", globalConfig)
	if err != nil {
		return nil, fmt.Errorf("error processing environment: %+v", err)
	}
	_, err = LoadTopologyConfig()
	if err != nil {
		return nil, fmt.Errorf("error loading topology: %+v", err)
	}
	return globalConfig, nil
}

func GetConfig() *Config {
	return globalConfig
}

var globalTopologyConfig = &topology.TopologyConfig{}

func LoadTopologyConfig() (*topology.TopologyConfig, error) {
	if globalConfig.Topology == "" {
		// Use default bootstrap peers for specified network
		network, ok := ouroboros.NetworkByName(globalConfig.Network)
		if !ok {
			return nil, fmt.Errorf("unknown network: %s", globalConfig.Network)
		}
		if len(network.BootstrapPeers) == 0 {
			return nil, fmt.Errorf("no known bootstrap peers for network %s", globalConfig.Network)
		}
		for _, peer := range network.BootstrapPeers {
			globalTopologyConfig.BootstrapPeers = append(
				globalTopologyConfig.BootstrapPeers,
				topology.TopologyConfigP2PAccessPoint{
					Address: peer.Address,
					Port:    peer.Port,
				},
			)
		}
		return globalTopologyConfig, nil
	}
	tc, err := topology.NewTopologyConfigFromFile(globalConfig.Topology)
	if err != nil {
		return nil, fmt.Errorf("failed to load topology file: %+v", err)
	}
	// update globalTopologyConfig
	globalTopologyConfig = tc
	return globalTopologyConfig, nil
}

func GetTopologyConfig() *topology.TopologyConfig {
	return globalTopologyConfig
}
