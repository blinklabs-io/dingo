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

package config

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v3"
)

// validNetworkName matches names consisting only of alphanumeric
// characters, hyphens, and underscores.
var validNetworkName = regexp.MustCompile(
	`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`,
)

type ctxKey string

const configContextKey ctxKey = "dingo.config"

const DefaultShutdownTimeout = "30s"

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

const (
	DefaultBlobPlugin         = "badger"
	DefaultMetadataPlugin     = "sqlite"
	DefaultEvictionWatermark  = 0.90
	DefaultRejectionWatermark = 0.95
)

// ErrPluginListRequested is returned when the user requests to list available plugins
// This is not an error condition but a successful operation that displays plugin information
var ErrPluginListRequested = errors.New("plugin list requested")

// RunMode represents the operational mode of the dingo node
type RunMode string

const (
	RunModeServe RunMode = "serve" // Full node with network connectivity (default)
	RunModeLoad  RunMode = "load"  // Batch import from ImmutableDB
	RunModeDev   RunMode = "dev"   // Development mode (isolated, no outbound)
)

// Valid returns true if the RunMode is a known valid mode
func (m RunMode) Valid() bool {
	switch m {
	case RunModeServe, RunModeLoad, RunModeDev, "":
		return true
	default:
		return false
	}
}

// IsDevMode returns true if the mode enables development behaviors
// (forge blocks, disable outbound, skip topology)
func (m RunMode) IsDevMode() bool {
	return m == RunModeDev
}

type tempConfig struct {
	Config   *Config                   `yaml:"config,omitempty"`
	Database *databaseConfig           `yaml:"database,omitempty"`
	Blob     map[string]map[string]any `yaml:"blob,omitempty"`
	Metadata map[string]map[string]any `yaml:"metadata,omitempty"`
}

type databaseConfig struct {
	Blob     map[string]any `yaml:"blob,omitempty"`
	Metadata map[string]any `yaml:"metadata,omitempty"`
}

// ChainsyncConfig holds configuration for the multi-client chainsync
// subsystem.
type ChainsyncConfig struct {
	// MaxClients is the maximum number of concurrent chainsync client
	// connections. Default: 3.
	MaxClients int `yaml:"maxClients"   envconfig:"DINGO_CHAINSYNC_MAX_CLIENTS"`
	// StallTimeout is the duration after which a client with no
	// activity is considered stalled. Default: "30s".
	StallTimeout string `yaml:"stallTimeout" envconfig:"DINGO_CHAINSYNC_STALL_TIMEOUT"`
}

// DefaultChainsyncConfig returns the default chainsync configuration.
// StallTimeout must match chainsync.DefaultStallTimeout and the
// fallback in internal/node/node.go.
func DefaultChainsyncConfig() ChainsyncConfig {
	return ChainsyncConfig{
		MaxClients:   3,
		StallTimeout: "30s",
	}
}

// CacheConfig holds configuration for the tiered CBOR cache system.
type CacheConfig struct {
	// HotUtxoEntries is the maximum number of UTxO CBOR entries in the hot cache.
	HotUtxoEntries int `yaml:"hotUtxoEntries"  envconfig:"DINGO_CACHE_HOT_UTXO_ENTRIES"`
	// HotTxEntries is the maximum number of transaction CBOR entries in the hot cache.
	HotTxEntries int `yaml:"hotTxEntries"    envconfig:"DINGO_CACHE_HOT_TX_ENTRIES"`
	// HotTxMaxBytes is the maximum memory in bytes for the hot transaction cache.
	HotTxMaxBytes int64 `yaml:"hotTxMaxBytes"   envconfig:"DINGO_CACHE_HOT_TX_MAX_BYTES"`
	// BlockLRUEntries is the maximum number of blocks in the LRU cache.
	BlockLRUEntries int `yaml:"blockLruEntries" envconfig:"DINGO_CACHE_BLOCK_LRU_ENTRIES"`
	// WarmupBlocks is the number of recent blocks to scan during cache warmup.
	WarmupBlocks int `yaml:"warmupBlocks"    envconfig:"DINGO_CACHE_WARMUP_BLOCKS"`
	// WarmupSync blocks startup until cache warmup is complete when true.
	WarmupSync bool `yaml:"warmupSync"      envconfig:"DINGO_CACHE_WARMUP_SYNC"`
}

// DefaultCacheConfig returns the default cache configuration values.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		HotUtxoEntries:  50000,
		HotTxEntries:    10000,
		HotTxMaxBytes:   268435456, // 256 MB
		BlockLRUEntries: 500,
		WarmupBlocks:    1000,
		WarmupSync:      true,
	}
}

type Config struct {
	MetadataPlugin     string  `yaml:"metadataPlugin"     envconfig:"DINGO_DATABASE_METADATA_PLUGIN"`
	TlsKeyFilePath     string  `yaml:"tlsKeyFilePath"     envconfig:"TLS_KEY_FILE_PATH"`
	Topology           string  `yaml:"topology"`
	CardanoConfig      string  `yaml:"cardanoConfig"      envconfig:"config"`
	DatabasePath       string  `yaml:"databasePath"                                                     split_words:"true"`
	SocketPath         string  `yaml:"socketPath"                                                       split_words:"true"`
	TlsCertFilePath    string  `yaml:"tlsCertFilePath"    envconfig:"TLS_CERT_FILE_PATH"`
	BindAddr           string  `yaml:"bindAddr"                                                         split_words:"true"`
	BlobPlugin         string  `yaml:"blobPlugin"         envconfig:"DINGO_DATABASE_BLOB_PLUGIN"`
	PrivateBindAddr    string  `yaml:"privateBindAddr"                                                  split_words:"true"`
	ShutdownTimeout    string  `yaml:"shutdownTimeout"                                                  split_words:"true"`
	Network            string  `yaml:"network"`
	MempoolCapacity    int64   `yaml:"mempoolCapacity"                                                  split_words:"true"`
	EvictionWatermark  float64 `yaml:"evictionWatermark"  envconfig:"DINGO_MEMPOOL_EVICTION_WATERMARK"`
	RejectionWatermark float64 `yaml:"rejectionWatermark" envconfig:"DINGO_MEMPOOL_REJECTION_WATERMARK"`
	PrivatePort        uint    `yaml:"privatePort"                                                      split_words:"true"`
	RelayPort          uint    `yaml:"relayPort"          envconfig:"port"`
	UtxorpcPort        uint    `yaml:"utxorpcPort"                                                      split_words:"true"`
	BarkBaseUrl        string  `yaml:"barkBaseUrl"        envconfig:"DINGO_BARK_BASE_URL"`
	BarkSecurityWindow uint64  `yaml:"barkSecurityWindow" envconfig:"DINGO_BARK_SECURITY_WINDOW"`
	BarkPort           uint    `yaml:"barkPort"           envconfig:"DINGO_BARK_PORT"`
	MetricsPort        uint    `yaml:"metricsPort"                                                      split_words:"true"`
	IntersectTip       bool    `yaml:"intersectTip"                                                     split_words:"true"`
	ValidateHistorical bool    `yaml:"validateHistorical"                                               split_words:"true"`
	RunMode            RunMode `yaml:"runMode"            envconfig:"DINGO_RUN_MODE"`
	ImmutableDbPath    string  `yaml:"immutableDbPath"    envconfig:"DINGO_IMMUTABLE_DB_PATH"`
	// Database worker pool tuning (worker count and task queue size)
	DatabaseWorkers   int `yaml:"databaseWorkers"    envconfig:"DINGO_DATABASE_WORKERS"`
	DatabaseQueueSize int `yaml:"databaseQueueSize"  envconfig:"DINGO_DATABASE_QUEUE_SIZE"`

	// Peer targets (0 = use default, -1 = unlimited)
	TargetNumberOfKnownPeers       int `yaml:"targetNumberOfKnownPeers"       envconfig:"DINGO_TARGET_KNOWN_PEERS"`
	TargetNumberOfEstablishedPeers int `yaml:"targetNumberOfEstablishedPeers" envconfig:"DINGO_TARGET_ESTABLISHED_PEERS"`
	TargetNumberOfActivePeers      int `yaml:"targetNumberOfActivePeers"      envconfig:"DINGO_TARGET_ACTIVE_PEERS"`

	// Per-source quotas for active peers (0 = use default, negative = disable)
	ActivePeersTopologyQuota int `yaml:"activePeersTopologyQuota" envconfig:"DINGO_ACTIVE_PEERS_TOPOLOGY_QUOTA"`
	ActivePeersGossipQuota   int `yaml:"activePeersGossipQuota"   envconfig:"DINGO_ACTIVE_PEERS_GOSSIP_QUOTA"`
	ActivePeersLedgerQuota   int `yaml:"activePeersLedgerQuota"   envconfig:"DINGO_ACTIVE_PEERS_LEDGER_QUOTA"`

	// Cache configuration for the tiered CBOR cache system
	Cache CacheConfig `yaml:"cache"`

	// Chainsync configuration for multi-client support
	Chainsync ChainsyncConfig `yaml:"chainsync"`

	// KES (Key Evolving Signature) configuration for block production
	// SlotsPerKESPeriod is the number of slots in a KES period.
	// After this many slots, the KES key must be evolved to the next period.
	// Default: 129600 (mainnet value = 1.5 days at 1 second per slot)
	SlotsPerKESPeriod uint64 `yaml:"slotsPerKESPeriod" envconfig:"DINGO_SLOTS_PER_KES_PERIOD"`
	// MaxKESEvolutions is the maximum number of times a KES key can evolve.
	// For Cardano's KES depth of 6, this is 2^6 - 2 = 62 evolutions.
	// After this many evolutions, a new operational certificate must be issued.
	// Default: 62
	MaxKESEvolutions uint64 `yaml:"maxKESEvolutions"  envconfig:"DINGO_MAX_KES_EVOLUTIONS"`

	// Block production configuration (SPO mode)
	// Environment variables match cardano-node naming convention for compatibility
	// Note: envconfig.Process("cardano", ...) adds "CARDANO_" prefix automatically
	BlockProducer                 bool   `yaml:"blockProducer"                 envconfig:"BLOCK_PRODUCER"`
	ShelleyVRFKey                 string `yaml:"shelleyVrfKey"                 envconfig:"SHELLEY_VRF_KEY"`
	ShelleyKESKey                 string `yaml:"shelleyKesKey"                 envconfig:"SHELLEY_KES_KEY"`
	ShelleyOperationalCertificate string `yaml:"shelleyOperationalCertificate" envconfig:"SHELLEY_OPERATIONAL_CERTIFICATE"`

	// Mesh (Coinbase Rosetta) API listen address (empty = disabled)
	MeshListenAddress string `yaml:"meshListenAddress" envconfig:"DINGO_MESH_LISTEN_ADDRESS"`
}

func (c *Config) ParseCmdlineArgs(programName string, args []string) error {
	fs := flag.NewFlagSet(programName, flag.ExitOnError)
	fs.StringVar(
		&c.BlobPlugin,
		"blob",
		DefaultBlobPlugin,
		"blob store plugin to use, 'list' to show available",
	)
	fs.StringVar(
		&c.MetadataPlugin,
		"metadata",
		DefaultMetadataPlugin,
		"metadata store plugin to use, 'list' to show available",
	)
	// Database worker pool flags
	fs.IntVar(
		&c.DatabaseWorkers,
		"db-workers",
		5,
		"database worker pool worker count",
	)
	fs.IntVar(
		&c.DatabaseQueueSize,
		"db-queue-size",
		50,
		"database worker pool task queue size",
	)
	// NOTE: Plugin flags are handled by Cobra in main.go
	// if err := plugin.PopulateCmdlineOptions(fs); err != nil {
	// 	return err
	// }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Handle plugin listing
	if c.BlobPlugin == "list" {
		fmt.Println("Available blob plugins:")
		blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
		for _, p := range blobPlugins {
			fmt.Printf("  %s: %s\n", p.Name, p.Description)
		}
		return ErrPluginListRequested
	}
	if c.MetadataPlugin == "list" {
		fmt.Println("Available metadata plugins:")
		metadataPlugins := plugin.GetPlugins(plugin.PluginTypeMetadata)
		for _, p := range metadataPlugins {
			fmt.Printf("  %s: %s\n", p.Name, p.Description)
		}
		return ErrPluginListRequested
	}

	return nil
}

var globalConfig = &Config{
	MempoolCapacity:    1048576,
	EvictionWatermark:  DefaultEvictionWatermark,
	RejectionWatermark: DefaultRejectionWatermark,
	BindAddr:           "0.0.0.0",
	CardanoConfig:      "", // Will be set dynamically based on network
	DatabasePath:       ".dingo",
	SocketPath:         "dingo.socket",
	IntersectTip:       false,
	ValidateHistorical: false,
	Network:            "preview",
	MetricsPort:        12798,
	PrivateBindAddr:    "127.0.0.1",
	PrivatePort:        3002,
	RelayPort:          3001,
	BarkBaseUrl:        "",
	BarkSecurityWindow: 10000,
	BarkPort:           9091,
	UtxorpcPort:        9090,
	Topology:           "",
	TlsCertFilePath:    "",
	TlsKeyFilePath:     "",
	BlobPlugin:         DefaultBlobPlugin,
	MetadataPlugin:     DefaultMetadataPlugin,
	RunMode:            RunModeServe,
	ImmutableDbPath:    "",
	ShutdownTimeout:    DefaultShutdownTimeout,
	// Defaults for database worker pool tuning
	DatabaseWorkers:   5,
	DatabaseQueueSize: 50,
	// Cache configuration defaults
	Cache: DefaultCacheConfig(),
	// Chainsync configuration defaults
	Chainsync: DefaultChainsyncConfig(),
	// KES configuration defaults (mainnet values)
	SlotsPerKESPeriod: 129600, // 1.5 days at 1 second per slot
	MaxKESEvolutions:  62,     // 2^6 - 2 for KES depth 6
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

		// First unmarshal into temp config to handle plugin sections
		var tempCfg tempConfig
		err = yaml.Unmarshal(buf, &tempCfg)
		if err != nil {
			return nil, fmt.Errorf("error parsing config file: %w", err)
		}

		// If config section exists, use it for main config
		if tempCfg.Config != nil {
			// Overlay config values onto existing defaults
			configBytes, err := yaml.Marshal(tempCfg.Config)
			if err != nil {
				return nil, fmt.Errorf("error re-marshalling config: %w", err)
			}
			err = yaml.Unmarshal(configBytes, globalConfig)
			if err != nil {
				return nil, fmt.Errorf("error parsing config section: %w", err)
			}
		} else {
			// Otherwise unmarshal the whole file as main config (backward compatibility)
			err = yaml.Unmarshal(buf, globalConfig)
			if err != nil {
				return nil, fmt.Errorf("error parsing config file: %w", err)
			}
		}

		// Process plugin configurations
		pluginConfig := make(map[string]map[string]map[string]any)
		if tempCfg.Blob != nil {
			pluginConfig["blob"] = tempCfg.Blob
		}
		if tempCfg.Metadata != nil {
			pluginConfig["metadata"] = tempCfg.Metadata
		}
		// Handle database section if present
		if tempCfg.Database != nil {
			if tempCfg.Database.Blob != nil {
				// Extract plugin name if specified
				if pluginVal, exists := tempCfg.Database.Blob["plugin"]; exists {
					if pluginName, ok := pluginVal.(string); ok {
						globalConfig.BlobPlugin = pluginName
						// Remove plugin from config map
						delete(tempCfg.Database.Blob, "plugin")
					}
				}
				// Build plugin config map
				blobConfig := make(map[string]map[string]any)
				for k, v := range tempCfg.Database.Blob {
					if val, ok := v.(map[string]any); ok {
						blobConfig[k] = val
					} else if val, ok := v.(map[any]any); ok {
						// Convert map[any]any to map[string]any
						stringAnyMap := make(map[string]any)
						for vk, vv := range val {
							if keyStr, ok := vk.(string); ok {
								stringAnyMap[keyStr] = vv
							}
						}
						blobConfig[k] = stringAnyMap
					} else {
						// Log skipped non-map config entries
						fmt.Fprintf(os.Stderr, "warning: skipping blob config entry %q: expected map, got %T\n", k, v)
					}
				}
				// Merge with existing blob config instead of overwriting
				if pluginConfig["blob"] == nil {
					pluginConfig["blob"] = blobConfig
				} else {
					maps.Copy(pluginConfig["blob"], blobConfig)
				}
			}
			if tempCfg.Database.Metadata != nil {
				// Extract plugin name if specified
				if pluginVal, exists := tempCfg.Database.Metadata["plugin"]; exists {
					if pluginName, ok := pluginVal.(string); ok {
						globalConfig.MetadataPlugin = pluginName
						// Remove plugin from config map
						delete(tempCfg.Database.Metadata, "plugin")
					}
				}
				// Build plugin config map
				metadataConfig := make(map[string]map[string]any)
				for k, v := range tempCfg.Database.Metadata {
					if val, ok := v.(map[string]any); ok {
						metadataConfig[k] = val
					} else if val, ok := v.(map[any]any); ok {
						// Convert map[any]any to map[string]any
						stringAnyMap := make(map[string]any)
						for vk, vv := range val {
							if keyStr, ok := vk.(string); ok {
								stringAnyMap[keyStr] = vv
							}
						}
						metadataConfig[k] = stringAnyMap
					} else {
						// Log skipped non-map config entries
						fmt.Fprintf(os.Stderr, "warning: skipping metadata config entry %q: expected map, got %T\n", k, v)
					}
				}
				// Merge with existing metadata config instead of overwriting
				if pluginConfig["metadata"] == nil {
					pluginConfig["metadata"] = metadataConfig
				} else {
					maps.Copy(pluginConfig["metadata"], metadataConfig)
				}
			}
		}
		if len(pluginConfig) > 0 {
			err = plugin.ProcessConfig(pluginConfig)
			if err != nil {
				return nil, fmt.Errorf(
					"error processing plugin config: %w",
					err,
				)
			}
		}
	}
	// Process environment variables
	err := envconfig.Process("cardano", globalConfig)
	if err != nil {
		return nil, fmt.Errorf("error processing environment: %+w", err)
	}

	// Process plugin environment variables
	err = plugin.ProcessEnvVars()
	if err != nil {
		return nil, fmt.Errorf(
			"error processing plugin environment variables: %w",
			err,
		)
	}

	// Validate and default RunMode
	if !globalConfig.RunMode.Valid() {
		return nil, fmt.Errorf(
			"invalid runMode: %q (must be 'serve', 'load', or 'dev')",
			globalConfig.RunMode,
		)
	}
	if globalConfig.RunMode == "" {
		globalConfig.RunMode = RunModeServe
	}

	// Validate block producer configuration
	if globalConfig.BlockProducer {
		var missing []string
		if globalConfig.ShelleyVRFKey == "" {
			missing = append(missing, "shelleyVrfKey")
		}
		if globalConfig.ShelleyKESKey == "" {
			missing = append(missing, "shelleyKesKey")
		}
		if globalConfig.ShelleyOperationalCertificate == "" {
			missing = append(missing, "shelleyOperationalCertificate")
		}
		if len(missing) > 0 {
			return nil, fmt.Errorf(
				"blockProducer enabled but missing required key paths: %v",
				missing,
			)
		}
	}

	// Default unset watermarks. In Go, unset float64 fields are 0,
	// which is indistinguishable from an explicit 0. We default 0 to
	// the standard value; the subsequent validation rejects any value
	// that ends up <= 0 after defaulting.
	if globalConfig.EvictionWatermark == 0 {
		globalConfig.EvictionWatermark = DefaultEvictionWatermark
	}
	if globalConfig.RejectionWatermark == 0 {
		globalConfig.RejectionWatermark = DefaultRejectionWatermark
	}
	if globalConfig.EvictionWatermark <= 0 ||
		globalConfig.EvictionWatermark >= 1.0 {
		return nil, fmt.Errorf(
			"invalid evictionWatermark: %f (must be in range (0, 1))",
			globalConfig.EvictionWatermark,
		)
	}
	if globalConfig.RejectionWatermark <= 0 ||
		globalConfig.RejectionWatermark > 1.0 {
		return nil, fmt.Errorf(
			"invalid rejectionWatermark: %f (must be in range (0, 1])",
			globalConfig.RejectionWatermark,
		)
	}
	if globalConfig.EvictionWatermark >= globalConfig.RejectionWatermark {
		return nil, fmt.Errorf(
			"evictionWatermark (%f) must be less than rejectionWatermark (%f)",
			globalConfig.EvictionWatermark,
			globalConfig.RejectionWatermark,
		)
	}

	// Validate network name to prevent path traversal (INT-03).
	// Only alphanumeric characters, hyphens, and underscores are
	// permitted. Values containing '/', '\', '..', or other path
	// separators are rejected outright.
	if !validNetworkName.MatchString(globalConfig.Network) {
		return nil, fmt.Errorf(
			"invalid network name %q: "+
				"must contain only alphanumeric characters, "+
				"hyphens, and underscores",
			globalConfig.Network,
		)
	}

	// Set default CardanoConfig path based on network if not provided by user
	if globalConfig.CardanoConfig == "" {
		if globalConfig.Network == "preview" {
			globalConfig.CardanoConfig = "preview/config.json"
		} else {
			globalConfig.CardanoConfig = "/opt/cardano/" + globalConfig.Network + "/config.json"
		}
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
	if globalConfig.RunMode.IsDevMode() {
		return globalTopologyConfig, nil
	}
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
