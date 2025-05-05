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
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v3"
)

type ctxKey string

const configContextKey ctxKey = "dingo.config"

func WithContext(ctx context.Context, cfg *Config) context.Context {
	return context.WithValue(ctx, configContextKey, cfg)
}

func FromContext(ctx context.Context) *Config {
	cfg, ok := ctx.Value(configContextKey).(*Config)
	if !ok {
		return nil
	}
	return cfg
}

type Config struct {
	BindAddr        string `yaml:"bindAddr"        split_words:"true"`
	CardanoConfig   string `yaml:"cardanoConfig"                      envconfig:"config"`
	DatabasePath    string `yaml:"databasePath"    split_words:"true"`
	SocketPath      string `yaml:"socketPath"      split_words:"true"`
	Network         string `yaml:"network"`
	TlsCertFilePath string `yaml:"tlsCertFilePath"                    envconfig:"TLS_CERT_FILE_PATH"`
	TlsKeyFilePath  string `yaml:"tlsKeyFilePath"                     envconfig:"TLS_KEY_FILE_PATH"`
	Topology        string `yaml:"topology"`
	MetricsPort     uint   `yaml:"metricsPort"     split_words:"true"`
	PrivateBindAddr string `yaml:"privateBindAddr" split_words:"true"`
	PrivatePort     uint   `yaml:"privatePort"     split_words:"true"`
	RelayPort       uint   `yaml:"relayPort"                          envconfig:"port"`
	UtxorpcPort     uint   `yaml:"utxorpcPort"     split_words:"true"`
	IntersectTip    bool   `yaml:"intersectTip"    split_words:"true"`
}

var globalConfig = &Config{
	BindAddr:        "0.0.0.0",
	CardanoConfig:   "./config/cardano/preview/config.json",
	DatabasePath:    ".dingo",
	SocketPath:      "dingo.socket",
	IntersectTip:    false,
	Network:         "preview",
	MetricsPort:     12798,
	PrivateBindAddr: "127.0.0.1",
	PrivatePort:     3002,
	RelayPort:       3001,
	UtxorpcPort:     9090,
	Topology:        "",
	TlsCertFilePath: "",
	TlsKeyFilePath:  "",
}

func LoadConfig(configFile string) (*Config, error) {
	// Load config file as YAML if provided
	if configFile == "" {
		// Check for config file in this path: ~/.dingo/dingo.yaml
		if homeDir, err := os.UserHomeDir(); err == nil {
			userPath := filepath.Join(homeDir, ".dingo", "dingo.yaml")
			if _, err := os.Stat(userPath); err == nil {
				configFile = userPath
			}
		}

		// Try to check for /etc/dingo/dingo.yaml if still not found
		if configFile == "" {
			systemPath := "/etc/dingo/dingo.yaml"
			if _, err := os.Stat(systemPath); err == nil {
				configFile = systemPath
			}
		}
	}
	if configFile != "" {
		buf, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		err = yaml.Unmarshal(buf, globalConfig)
		if err != nil {
			return nil, fmt.Errorf("error parsing config file: %w", err)
		}
	}
	err := envconfig.Process("cardano", globalConfig)
	if err != nil {
		return nil, fmt.Errorf("error processing environment: %+w", err)
	}
	_, err = LoadTopologyConfig()
	if err != nil {
		return nil, fmt.Errorf("error loading topology: %+w", err)
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
			return nil, fmt.Errorf(
				"no known bootstrap peers for network %s",
				globalConfig.Network,
			)
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
		return nil, fmt.Errorf("failed to load topology file: %+w", err)
	}
	// update globalTopologyConfig
	globalTopologyConfig = tc
	return globalTopologyConfig, nil
}

func GetTopologyConfig() *topology.TopologyConfig {
	return globalTopologyConfig
}
