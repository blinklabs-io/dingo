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

package config

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
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

// ValidateNetworkName checks that a network name contains only
// permitted characters and returns an error if it does not.
func ValidateNetworkName(network string) error {
	if !validNetworkName.MatchString(network) {
		return fmt.Errorf(
			"invalid network name %q: "+
				"must contain only alphanumeric "+
				"characters, hyphens, and underscores",
			network,
		)
	}
	return nil
}

type ctxKey string

const configContextKey ctxKey = "dingo.config"

const DefaultShutdownTimeout = "30s"

// DefaultLedgerCatchupTimeout is the maximum time LoadWithDB will wait
// for the ledger to process all blocks before returning an error.
const DefaultLedgerCatchupTimeout = "30m"

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
	DefaultBlobPlugin                  = "badger"
	DefaultMetadataPlugin              = "sqlite"
	DefaultEvictionWatermark           = 0.90
	DefaultRejectionWatermark          = 0.95
	DefaultForgeSyncToleranceSlots     = 100
	DefaultForgeStaleGapThresholdSlots = 1000
	DefaultMempoolCapacityPraos        = 1048576  // 1 MiB
	DefaultMempoolCapacityLeios        = 26214400 // 25 MiB
)

// ErrPluginListRequested is returned when the user requests to list
// available plugins. This is not an error condition but a successful
// operation that displays plugin information.
var ErrPluginListRequested = errors.New("plugin list requested")

// RunMode represents the operational mode of the dingo node
type RunMode string

const (
	RunModeServe RunMode = "serve" // Full node with network connectivity (default)
	RunModeLoad  RunMode = "load"  // Batch import from ImmutableDB
	RunModeDev   RunMode = "dev"   // Development mode (isolated, no outbound)
	RunModeLeios RunMode = "leios" // Full node with experimental Leios capabilities
)

// StartEra controls experimental direct startup in a later ledger era.
type StartEra string

const (
	StartEraDefault  StartEra = ""
	StartEraDijkstra StartEra = "dijkstra"
)

// Valid returns true if the RunMode is a known valid mode
func (m RunMode) Valid() bool {
	switch m {
	case RunModeServe, RunModeLoad, RunModeDev, RunModeLeios, "":
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

func (e StartEra) Valid() bool {
	switch e {
	case StartEraDefault, StartEraDijkstra:
		return true
	default:
		return false
	}
}

func (e StartEra) IsDijkstra() bool {
	return e == StartEraDijkstra
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
	// activity is considered stalled. Default: "2m".
	StallTimeout string `yaml:"stallTimeout" envconfig:"DINGO_CHAINSYNC_STALL_TIMEOUT"`
}

// GenesisBootstrapConfig holds configuration for Genesis-mode chain
// selection and bootstrap-time peer promotion.
type GenesisBootstrapConfig struct {
	// Enabled controls whether Genesis bootstrap mode is used when the node
	// starts from origin.
	Enabled bool `yaml:"enabled" envconfig:"DINGO_GENESIS_BOOTSTRAP_ENABLED"`
	// WindowSlots overrides the Genesis density comparison window in slots.
	// A zero value derives the window from Shelley genesis parameters (3k/f).
	WindowSlots uint64 `yaml:"windowSlots" envconfig:"DINGO_GENESIS_BOOTSTRAP_WINDOW_SLOTS"`
	// PromotionMinDiversityGroups sets the minimum number of diversity groups
	// to prefer while promoting peers during bootstrap.
	PromotionMinDiversityGroups int `yaml:"promotionMinDiversityGroups" envconfig:"DINGO_GENESIS_BOOTSTRAP_PROMOTION_MIN_DIVERSITY_GROUPS"`
}

// HistoryExpiryConfig controls local expiry of immutable block history.
type HistoryExpiryConfig struct {
	// Enabled starts the background expiry worker when true.
	Enabled bool `yaml:"enabled" envconfig:"DINGO_HISTORY_EXPIRY_ENABLED"`
	// Frequency controls how often the worker scans for expired block CBOR.
	Frequency time.Duration `yaml:"frequency" envconfig:"DINGO_HISTORY_EXPIRY_FREQUENCY"`
}

// OffchainMetadataConfig holds API-mode off-chain metadata fetcher settings.
// Zero values fall back to the fetcher's internal defaults.
type OffchainMetadataConfig struct {
	// Interval controls how often the fetcher discovers and fetches due rows.
	Interval time.Duration `yaml:"interval" envconfig:"DINGO_OFFCHAIN_METADATA_INTERVAL"`
	// RequestTimeout limits each HTTP(S) metadata request.
	RequestTimeout time.Duration `yaml:"requestTimeout" envconfig:"DINGO_OFFCHAIN_METADATA_REQUEST_TIMEOUT"`
	// UserAgent is sent with outbound metadata requests.
	UserAgent string `yaml:"userAgent" envconfig:"DINGO_OFFCHAIN_METADATA_USER_AGENT"`
	// IPFSGatewayURL is the gateway prefix used for ipfs:// URLs.
	IPFSGatewayURL string `yaml:"ipfsGatewayUrl" envconfig:"DINGO_OFFCHAIN_METADATA_IPFS_GATEWAY_URL"`
	// BatchSize bounds the number of due rows claimed per fetcher pass.
	BatchSize int `yaml:"batchSize" envconfig:"DINGO_OFFCHAIN_METADATA_BATCH_SIZE"`
	// MaxBytes bounds the response body bytes read from each document.
	MaxBytes int64 `yaml:"maxBytes" envconfig:"DINGO_OFFCHAIN_METADATA_MAX_BYTES"`
	// AllowPrivateAddresses permits fetching private, loopback, and link-local
	// addresses. Leave false for the default SSRF guard.
	AllowPrivateAddresses bool `yaml:"allowPrivateAddresses" envconfig:"DINGO_OFFCHAIN_METADATA_ALLOW_PRIVATE_ADDRESSES"`
}

// DefaultChainsyncConfig returns the default chainsync configuration.
// StallTimeout must match chainsync.DefaultStallTimeout and the
// fallback in internal/node/node.go.
func DefaultChainsyncConfig() ChainsyncConfig {
	return ChainsyncConfig{
		MaxClients:   3,
		StallTimeout: "2m",
	}
}

// DefaultGenesisBootstrapConfig returns the default Genesis bootstrap
// configuration values.
func DefaultGenesisBootstrapConfig() GenesisBootstrapConfig {
	return GenesisBootstrapConfig{
		Enabled: true,
	}
}

// DefaultHistoryExpiryConfig returns the default history expiry settings.
func DefaultHistoryExpiryConfig() HistoryExpiryConfig {
	return HistoryExpiryConfig{
		Frequency: time.Hour,
	}
}

// CacheConfig holds configuration for the tiered CBOR cache system.
type CacheConfig struct {
	// HotUtxoEntries is the maximum number of UTxO CBOR entries in the hot
	// cache.
	HotUtxoEntries int `yaml:"hotUtxoEntries"  envconfig:"DINGO_CACHE_HOT_UTXO_ENTRIES"`
	// HotTxEntries is the maximum number of transaction CBOR entries in the hot
	// cache.
	HotTxEntries int `yaml:"hotTxEntries"    envconfig:"DINGO_CACHE_HOT_TX_ENTRIES"`
	// HotTxMaxBytes is the maximum memory in bytes for the hot transaction
	// cache.
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

// LoggingConfig holds log output configuration.
type LoggingConfig struct {
	// Format selects the log output handler: "text" (default) or "json".
	// JSON is intended for machine-parseable ingestion (ELK/Loki).
	Format string `yaml:"format" envconfig:"DINGO_LOGGING_FORMAT"`
	// Level is the minimum log level: "debug", "info" (default), "warn",
	// or "error". The --debug flag, when set, overrides this to "debug".
	Level string `yaml:"level"  envconfig:"DINGO_LOGGING_LEVEL"`
}

// DefaultLoggingConfig returns the default logging configuration.
func DefaultLoggingConfig() LoggingConfig {
	return LoggingConfig{
		Format: "text",
		Level:  "info",
	}
}

// MidnightConfig holds configuration for the Midnight indexer and its
// optional gRPC API surface. Indexing is only active when Dingo is running
// in API storage mode; Port 0 disables only the gRPC server.
type MidnightConfig struct {
	Port uint   `yaml:"port" envconfig:"DINGO_MIDNIGHT_PORT"`
	Host string `yaml:"host" envconfig:"DINGO_MIDNIGHT_HOST"`

	CNightPolicyID              string `yaml:"cnight_policy_id"`
	CNightAssetName             string `yaml:"cnight_asset_name"`
	MappingValidatorAddress     string `yaml:"mapping_validator_address"`
	AuthTokenAssetName          string `yaml:"auth_token_asset_name"`
	CommitteeCandidateAddress   string `yaml:"committee_candidate_address"`
	TechnicalCommitteeAddress   string `yaml:"technical_committee_address"`
	TechnicalCommitteePolicyID  string `yaml:"technical_committee_policy_id"`
	CouncilAddress              string `yaml:"council_address"`
	CouncilPolicyID             string `yaml:"council_policy_id"`
	PermissionedCandidatePolicy string `yaml:"permissioned_candidate_policy"`
}

// DefaultMidnightConfig returns the default Midnight indexer settings.
func DefaultMidnightConfig() MidnightConfig {
	return MidnightConfig{
		Port: 50051,
		Host: "0.0.0.0",
	}
}

type Config struct {
	MetadataPlugin       string   `yaml:"metadataPlugin"     envconfig:"DINGO_DATABASE_METADATA_PLUGIN"`
	TlsKeyFilePath       string   `yaml:"tlsKeyFilePath"     envconfig:"TLS_KEY_FILE_PATH"`
	Topology             string   `yaml:"topology"`
	CardanoConfig        string   `yaml:"cardanoConfig"      envconfig:"config"`
	DatabasePath         string   `yaml:"databasePath"                                                     split_words:"true"`
	SocketPath           string   `yaml:"socketPath"                                                       split_words:"true"`
	TlsCertFilePath      string   `yaml:"tlsCertFilePath"    envconfig:"TLS_CERT_FILE_PATH"`
	BindAddr             string   `yaml:"bindAddr"                                                         split_words:"true"`
	BlobPlugin           string   `yaml:"blobPlugin"         envconfig:"DINGO_DATABASE_BLOB_PLUGIN"`
	PrivateBindAddr      string   `yaml:"privateBindAddr"                                                  split_words:"true"`
	ShutdownTimeout      string   `yaml:"shutdownTimeout"                                                  split_words:"true"`
	LedgerCatchupTimeout string   `yaml:"ledgerCatchupTimeout"  envconfig:"DINGO_LEDGER_CATCHUP_TIMEOUT"`
	Network              string   `yaml:"network"`
	NetworkMagic         uint32   `yaml:"networkMagic"                                                     split_words:"true"`
	MempoolCapacity      int64    `yaml:"mempoolCapacity"                                                  split_words:"true"`
	EvictionWatermark    float64  `yaml:"evictionWatermark"  envconfig:"DINGO_MEMPOOL_EVICTION_WATERMARK"`
	RejectionWatermark   float64  `yaml:"rejectionWatermark" envconfig:"DINGO_MEMPOOL_REJECTION_WATERMARK"`
	PrivatePort          uint     `yaml:"privatePort"                                                      split_words:"true"`
	RelayPort            uint     `yaml:"relayPort"          envconfig:"port"`
	BarkBaseUrl          string   `yaml:"barkBaseUrl"        envconfig:"DINGO_BARK_BASE_URL"`
	BarkPort             uint     `yaml:"barkPort"           envconfig:"DINGO_BARK_PORT"`
	UtxorpcPort          uint     `yaml:"utxorpcPort"        envconfig:"DINGO_UTXORPC_PORT"`
	CORSAllowedOrigins   []string `yaml:"corsAllowedOrigins" envconfig:"DINGO_CORS_ALLOWED_ORIGINS"`
	MetricsPort          uint     `yaml:"metricsPort"                                                      split_words:"true"`
	DebugPort            uint     `yaml:"debugPort"          envconfig:"DINGO_DEBUG_PORT"`
	IntersectTip         bool     `yaml:"intersectTip"                                                     split_words:"true"`
	ValidateHistorical   bool     `yaml:"validateHistorical"                                               split_words:"true"`
	RunMode              RunMode  `yaml:"runMode"            envconfig:"DINGO_RUN_MODE"`
	StartEra             StartEra `yaml:"startEra"           envconfig:"DINGO_START_ERA"`
	ImmutableDbPath      string   `yaml:"immutableDbPath"    envconfig:"DINGO_IMMUTABLE_DB_PATH"`
	// Database worker pool tuning (worker count and task queue size)
	DatabaseWorkers   int `yaml:"databaseWorkers"    envconfig:"DINGO_DATABASE_WORKERS"`
	DatabaseQueueSize int `yaml:"databaseQueueSize"  envconfig:"DINGO_DATABASE_QUEUE_SIZE"`
	BackfillBatchSize int `yaml:"backfillBatchSize" envconfig:"DINGO_BACKFILL_BATCH_SIZE"`

	// Peer targets (0 = use default, -1 = unlimited)
	TargetNumberOfKnownPeers       int `yaml:"targetNumberOfKnownPeers"       envconfig:"DINGO_TARGET_KNOWN_PEERS"`
	TargetNumberOfEstablishedPeers int `yaml:"targetNumberOfEstablishedPeers" envconfig:"DINGO_TARGET_ESTABLISHED_PEERS"`
	TargetNumberOfActivePeers      int `yaml:"targetNumberOfActivePeers"      envconfig:"DINGO_TARGET_ACTIVE_PEERS"`

	// Per-source quotas for active peers (0 = use default, negative = disable)
	ActivePeersTopologyQuota int `yaml:"activePeersTopologyQuota" envconfig:"DINGO_ACTIVE_PEERS_TOPOLOGY_QUOTA"`
	ActivePeersGossipQuota   int `yaml:"activePeersGossipQuota"   envconfig:"DINGO_ACTIVE_PEERS_GOSSIP_QUOTA"`
	ActivePeersLedgerQuota   int `yaml:"activePeersLedgerQuota"   envconfig:"DINGO_ACTIVE_PEERS_LEDGER_QUOTA"`

	// Peer governor tuning (0 = use default)
	MinHotPeers              int           `yaml:"minHotPeers"         envconfig:"DINGO_MIN_HOT_PEERS"`
	ReconcileInterval        time.Duration `yaml:"reconcileInterval"   envconfig:"DINGO_RECONCILE_INTERVAL"`
	InactivityTimeout        time.Duration `yaml:"inactivityTimeout"   envconfig:"DINGO_INACTIVITY_TIMEOUT"`
	InboundWarmTarget        int           `yaml:"inboundWarmTarget"   envconfig:"DINGO_INBOUND_WARM_TARGET"`
	InboundHotQuota          int           `yaml:"inboundHotQuota"     envconfig:"DINGO_INBOUND_HOT_QUOTA"`
	InboundMinTenure         time.Duration `yaml:"inboundMinTenure"    envconfig:"DINGO_INBOUND_MIN_TENURE"`
	InboundHotScoreThreshold float64       `yaml:"inboundHotScoreThreshold" envconfig:"DINGO_INBOUND_HOT_SCORE_THRESHOLD"`
	InboundPruneAfter        time.Duration `yaml:"inboundPruneAfter"   envconfig:"DINGO_INBOUND_PRUNE_AFTER"`
	InboundDuplexOnlyForHot  bool          `yaml:"inboundDuplexOnlyForHot" envconfig:"DINGO_INBOUND_DUPLEX_ONLY_FOR_HOT"`
	InboundCooldown          time.Duration `yaml:"inboundCooldown"     envconfig:"DINGO_INBOUND_COOLDOWN"`
	MaxConnectionsPerIP      int           `yaml:"maxConnectionsPerIP" envconfig:"DINGO_MAX_CONNECTIONS_PER_IP"`
	MaxInboundConns          int           `yaml:"maxInboundConns"     envconfig:"DINGO_MAX_INBOUND_CONNS"`

	// Cache configuration for the tiered CBOR cache system
	Cache CacheConfig `yaml:"cache"`

	// Chainsync configuration for multi-client support
	Chainsync ChainsyncConfig `yaml:"chainsync"`

	// Genesis bootstrap configuration for from-origin chain selection.
	GenesisBootstrap GenesisBootstrapConfig `yaml:"genesisBootstrap"`

	// History expiry configuration for local immutable block CBOR expiry.
	HistoryExpiry HistoryExpiryConfig `yaml:"historyExpiry"`

	// Off-chain metadata fetcher configuration.
	OffchainMetadata OffchainMetadataConfig `yaml:"offchainMetadata"`

	// Logging configuration (output format and level)
	Logging LoggingConfig `yaml:"logging"`

	// Midnight indexer and gRPC API configuration.
	Midnight MidnightConfig `yaml:"midnight"`

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
	ForgeSyncToleranceSlots       uint64 `yaml:"forgeSyncToleranceSlots"       envconfig:"DINGO_FORGE_SYNC_TOLERANCE_SLOTS"`
	ForgeStaleGapThresholdSlots   uint64 `yaml:"forgeStaleGapThresholdSlots"   envconfig:"DINGO_FORGE_STALE_GAP_THRESHOLD_SLOTS"`

	// Leios voting configuration (experimental, leios runMode only).
	// LeiosVoteSigningKeyFile is the path to a hex-encoded BLS12-381
	// vote signing key. When set on a block producer whose pool is a
	// committee member, the node emits Leios votes for endorser blocks.
	LeiosVoteSigningKeyFile string `yaml:"leiosVoteSigningKeyFile" envconfig:"DINGO_LEIOS_VOTE_SIGNING_KEY_FILE"`
	// LeiosVoterPublicKeys maps hex pool key hashes to hex-encoded
	// BLS12-381 voter public keys for vote signature verification.
	// CIP-0164 key registration is not yet specified, so this static
	// registry stands in for it (devnet-style).
	LeiosVoterPublicKeys map[string]string `yaml:"leiosVoterPublicKeys" envconfig:"DINGO_LEIOS_VOTER_PUBLIC_KEYS"`

	// Blockfrost REST API port (0 = disabled)
	BlockfrostPort uint `yaml:"blockfrostPort" envconfig:"DINGO_BLOCKFROST_PORT"`
	// Mesh (Coinbase Rosetta) API port (0 = disabled)
	MeshPort uint `yaml:"meshPort" envconfig:"DINGO_MESH_PORT"`

	// PeerSharing enables the peer sharing protocol, allowing this node
	// to advertise known peers to other nodes on request. Pointer
	// distinguishes "operator did not set this" (nil) from "explicitly
	// false". On a block producer the resolved default is false unless
	// this field is explicitly set to true.
	PeerSharing *bool `yaml:"peerSharing" envconfig:"DINGO_PEER_SHARING"`

	// Storage mode: "core" (default) or "api".
	// "core" stores only consensus data
	// (UTxOs, certs, pools, pparams).
	// "api" additionally stores witnesses, scripts,
	// datums, redeemers, and tx metadata.
	// APIs (blockfrost, utxorpc, mesh) require
	// "api" mode.
	StorageMode string `yaml:"storageMode" envconfig:"DINGO_STORAGE_MODE"`

	// Mithril snapshot bootstrap configuration
	Mithril MithrilConfig `yaml:"mithril"`
}

// midnightNetworkDefaults holds per-network Midnight constants sourced from
// https://github.com/input-output-hk/acropolis/tree/master/processes/midnight_indexer
var midnightNetworkDefaults = map[string]MidnightConfig{
	"mainnet": {
		CNightPolicyID:              "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa",
		CNightAssetName:             "4e49474854",
		MappingValidatorAddress:     "addr_test1wplxjzranravtp574s2wz00md7vz9rzpucu252je68u9a8qzjheng",
		TechnicalCommitteeAddress:   "addr_test1wqx3yfmsp82nmtyjj4k86s3l04l6lvwaqh2vk2ygcge7kdsk4xc7j",
		TechnicalCommitteePolicyID:  "0d12277009d53dac92956c7d423f7d7fafb1dd05d4cb2888c233eb36",
		CouncilAddress:              "addr_test1wqqwkauz0ypglg5e4u780kcp8hzt75u72yg6z7td62gnk0qed0p06",
		CouncilPolicyID:             "00eb778279028fa299af3c77db013dc4bf539e5111a1796dd2913b3c",
		PermissionedCandidatePolicy: "f8625f11a58fa5ab5b85502a8fe5c843ece460c9c5f9273be17d3424",
	},
	"preview": {
		CNightPolicyID:              "d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1",
		MappingValidatorAddress:     "addr_test1wplxjzranravtp574s2wz00md7vz9rzpucu252je68u9a8qzjheng",
		CommitteeCandidateAddress:   "addr_test1wz5ax0hjvhx2uqef8sqrxnmfywd37hea4truhqxu4yxp9hsvggkfm",
		TechnicalCommitteeAddress:   "addr_test1wptcy7h9rmkhdnhn3jvm6tuhehcq2hhhzntvn00nq79ph8c44v43j",
		TechnicalCommitteePolicyID:  "57827ae51eed76cef38c99bd2f97cdf0055ef714d6c9bdf3078a1b9f",
		CouncilAddress:              "addr_test1wzy47zdsq22pg9l48c5v0f835ljdjzkz47sa5za9cehcejcw28k2d",
		CouncilPolicyID:             "895f09b002941417f53e28c7a4f1a7e4d90ac2afa1da0ba5c66f8ccb",
		PermissionedCandidatePolicy: "24dccfce2576ae6fa7149bc485850656ae6faf9f4158891316773a78",
	},
}

func applyMidnightNetworkDefaults(cfg *Config) {
	defaults, ok := midnightNetworkDefaults[cfg.Network]
	if !ok {
		return
	}
	if cfg.Midnight.CNightPolicyID == "" {
		cfg.Midnight.CNightPolicyID = defaults.CNightPolicyID
	}
	if cfg.Midnight.CNightAssetName == "" {
		cfg.Midnight.CNightAssetName = defaults.CNightAssetName
	}
	if cfg.Midnight.MappingValidatorAddress == "" {
		cfg.Midnight.MappingValidatorAddress = defaults.MappingValidatorAddress
	}
	if cfg.Midnight.AuthTokenAssetName == "" {
		cfg.Midnight.AuthTokenAssetName = defaults.AuthTokenAssetName
	}
	if cfg.Midnight.CommitteeCandidateAddress == "" {
		cfg.Midnight.CommitteeCandidateAddress = defaults.CommitteeCandidateAddress
	}
	if cfg.Midnight.TechnicalCommitteeAddress == "" {
		cfg.Midnight.TechnicalCommitteeAddress = defaults.TechnicalCommitteeAddress
	}
	if cfg.Midnight.TechnicalCommitteePolicyID == "" {
		cfg.Midnight.TechnicalCommitteePolicyID = defaults.TechnicalCommitteePolicyID
	}
	if cfg.Midnight.CouncilAddress == "" {
		cfg.Midnight.CouncilAddress = defaults.CouncilAddress
	}
	if cfg.Midnight.CouncilPolicyID == "" {
		cfg.Midnight.CouncilPolicyID = defaults.CouncilPolicyID
	}
	if cfg.Midnight.PermissionedCandidatePolicy == "" {
		cfg.Midnight.PermissionedCandidatePolicy = defaults.PermissionedCandidatePolicy
	}
}

// MithrilConfig holds configuration for Mithril snapshot bootstrapping.
type MithrilConfig struct {
	// Enabled controls whether Mithril integration is available.
	Enabled bool `yaml:"enabled"            envconfig:"DINGO_MITHRIL_ENABLED"`
	// AggregatorURL overrides the default aggregator URL for the network.
	// If empty, the URL is auto-detected from the configured network.
	AggregatorURL string `yaml:"aggregatorUrl"      envconfig:"DINGO_MITHRIL_AGGREGATOR_URL"`
	// Backend selects the Mithril artifact backend: "v2" (default) uses
	// incremental Cardano database artifacts; "v1" uses the legacy full
	// snapshot archives, which upstream Mithril is phasing out.
	Backend string `yaml:"backend"            envconfig:"DINGO_MITHRIL_BACKEND"`
	// DownloadDir is the directory where snapshot archives are downloaded.
	// If empty, a randomized temporary directory is created automatically.
	DownloadDir string `yaml:"downloadDir"        envconfig:"DINGO_MITHRIL_DOWNLOAD_DIR"`
	// DownloadIdleTimeout is the maximum idle time to wait for snapshot
	// response headers or body bytes before retrying. Empty uses the
	// downloader default; a negative duration disables idle detection.
	DownloadIdleTimeout string `yaml:"downloadIdleTimeout" envconfig:"DINGO_MITHRIL_DOWNLOAD_IDLE_TIMEOUT"`
	// DownloadMaxIdleRetries is the number of consecutive idle retries
	// allowed without additional bytes. Zero uses the downloader default.
	DownloadMaxIdleRetries int `yaml:"downloadMaxIdleRetries" envconfig:"DINGO_MITHRIL_DOWNLOAD_MAX_IDLE_RETRIES"`
	// CleanupAfterLoad controls whether temporary files are removed
	// after the ImmutableDB has been loaded.
	CleanupAfterLoad bool `yaml:"cleanupAfterLoad"   envconfig:"DINGO_MITHRIL_CLEANUP"`
	// VerifyCertificates enables certificate chain verification
	// during bootstrap. When true, the bootstrap process walks
	// the Mithril certificate chain from the snapshot back to the
	// genesis certificate to verify the chain is unbroken.
	VerifyCertificates bool `yaml:"verifyCertificates" envconfig:"DINGO_MITHRIL_VERIFY_CERTS"`
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
	// MempoolCapacity is left as the zero sentinel; LoadConfig fills
	// it in based on RunMode (Praos vs Leios) after CLI/env/YAML have
	// been merged.
	MempoolCapacity:      0,
	EvictionWatermark:    DefaultEvictionWatermark,
	RejectionWatermark:   DefaultRejectionWatermark,
	BindAddr:             "0.0.0.0",
	CardanoConfig:        "", // Will be set dynamically based on network
	DatabasePath:         ".dingo",
	SocketPath:           "dingo.socket",
	IntersectTip:         false,
	ValidateHistorical:   false,
	Network:              "preview",
	NetworkMagic:         0,
	MetricsPort:          12798,
	DebugPort:            0,
	PrivateBindAddr:      "127.0.0.1",
	PrivatePort:          3002,
	RelayPort:            3001,
	BarkBaseUrl:          "",
	BarkPort:             0,
	UtxorpcPort:          9090,
	CORSAllowedOrigins:   []string{"*"},
	BlockfrostPort:       3000,
	MeshPort:             8080,
	Topology:             "",
	TlsCertFilePath:      "",
	TlsKeyFilePath:       "",
	BlobPlugin:           DefaultBlobPlugin,
	MetadataPlugin:       DefaultMetadataPlugin,
	StorageMode:          "core",
	RunMode:              RunModeServe,
	StartEra:             StartEraDefault,
	ImmutableDbPath:      "",
	ShutdownTimeout:      DefaultShutdownTimeout,
	LedgerCatchupTimeout: DefaultLedgerCatchupTimeout,
	// Defaults for database worker pool and API backfill tuning
	DatabaseWorkers:   5,
	DatabaseQueueSize: 50,
	BackfillBatchSize: 100,
	// Cache configuration defaults
	Cache: DefaultCacheConfig(),
	// Chainsync configuration defaults
	Chainsync: DefaultChainsyncConfig(),
	// Genesis bootstrap defaults
	GenesisBootstrap: DefaultGenesisBootstrapConfig(),
	// History expiry defaults
	HistoryExpiry: DefaultHistoryExpiryConfig(),
	// Logging defaults (text output at info level)
	Logging: DefaultLoggingConfig(),
	// Midnight defaults
	Midnight: DefaultMidnightConfig(),
	// KES configuration defaults (mainnet values)
	SlotsPerKESPeriod: 129600, // 1.5 days at 1 second per slot
	MaxKESEvolutions:  62,     // 2^6 - 2 for KES depth 6
	// Mithril defaults
	Mithril: MithrilConfig{
		Enabled:            true,
		Backend:            "v2",
		CleanupAfterLoad:   true,
		VerifyCertificates: true,
	},
	// Forging defaults
	ForgeSyncToleranceSlots:     DefaultForgeSyncToleranceSlots,
	ForgeStaleGapThresholdSlots: DefaultForgeStaleGapThresholdSlots,
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
			// Otherwise unmarshal the whole file as main config (backward
			// compatibility)
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
			"invalid runMode: %q (must be 'serve', 'load', 'dev', or 'leios')",
			globalConfig.RunMode,
		)
	}
	if globalConfig.RunMode == "" {
		globalConfig.RunMode = RunModeServe
	}
	if !globalConfig.StartEra.Valid() {
		return nil, fmt.Errorf(
			"invalid startEra: %q (must be empty or 'dijkstra')",
			globalConfig.StartEra,
		)
	}

	// Default unset MempoolCapacity based on RunMode. CLI/env/YAML have
	// already been merged at this point; an explicit non-zero setting
	// from any of those layers wins per existing config priority.
	if globalConfig.MempoolCapacity == 0 {
		if globalConfig.RunMode == RunModeLeios {
			globalConfig.MempoolCapacity = DefaultMempoolCapacityLeios
		} else {
			globalConfig.MempoolCapacity = DefaultMempoolCapacityPraos
		}
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
	if globalConfig.ForgeSyncToleranceSlots == 0 {
		globalConfig.ForgeSyncToleranceSlots = DefaultForgeSyncToleranceSlots
	}
	if globalConfig.ForgeStaleGapThresholdSlots == 0 {
		globalConfig.ForgeStaleGapThresholdSlots = DefaultForgeStaleGapThresholdSlots
	}
	if globalConfig.HistoryExpiry.Frequency <= 0 {
		globalConfig.HistoryExpiry.Frequency = time.Hour
	}

	// Validate network name to prevent path traversal (INT-03).
	if err := ValidateNetworkName(globalConfig.Network); err != nil {
		return nil, err
	}
	applyMidnightNetworkDefaults(globalConfig)

	// NOTE: Do not set a default CardanoConfig here. The network flag
	// can be overridden after LoadConfig returns (see main.go
	// PersistentPreRunE). Each consumer resolves the cardano config
	// path using cfg.Network at call time instead.

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
	tc, err := LoadTopologyConfigFor(globalConfig)
	if err != nil {
		return nil, err
	}
	globalTopologyConfig = tc
	return globalTopologyConfig, nil
}

func LoadTopologyConfigFor(cfg *Config) (*topology.TopologyConfig, error) {
	if cfg == nil {
		return nil, errors.New("nil config")
	}
	if cfg.RunMode.IsDevMode() {
		return &topology.TopologyConfig{}, nil
	}
	if cfg.Topology == "" {
		embeddedTopologyPath := path.Join(cfg.Network, "topology.json")
		tc, err := topology.NewTopologyConfigFromFS(
			cardano.EmbeddedConfigFS,
			embeddedTopologyPath,
		)
		if err == nil {
			return tc, nil
		}
		if !errors.Is(err, fs.ErrNotExist) ||
			!embeddedTopologyFileMissing(embeddedTopologyPath) {
			return nil, fmt.Errorf(
				"failed to load embedded topology file: %w",
				err,
			)
		}
		network, ok := ouroboros.NetworkByName(cfg.Network)
		if !ok {
			return nil, fmt.Errorf("unknown network: %s", cfg.Network)
		}
		if len(network.BootstrapPeers) == 0 {
			return nil, fmt.Errorf(
				"no known bootstrap peers for network %s",
				cfg.Network,
			)
		}
		ret := &topology.TopologyConfig{}
		for _, peer := range network.BootstrapPeers {
			ret.BootstrapPeers = append(
				ret.BootstrapPeers,
				topology.TopologyConfigP2PAccessPoint{
					Address: peer.Address,
					Port:    peer.Port,
				},
			)
		}
		return ret, nil
	}
	tc, err := topology.NewTopologyConfigFromFile(cfg.Topology)
	if err != nil {
		return nil, fmt.Errorf("failed to load topology file: %+w", err)
	}
	return tc, nil
}

func embeddedTopologyFileMissing(file string) bool {
	topologyFile, err := cardano.EmbeddedConfigFS.Open(file)
	if err == nil {
		_ = topologyFile.Close()
		return false
	}
	return errors.Is(err, fs.ErrNotExist)
}

func GetTopologyConfig() *topology.TopologyConfig {
	return globalTopologyConfig
}
