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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
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
	DefaultEvictionWatermark           = 0.90
	DefaultRejectionWatermark          = 0.95
	DefaultForgeSyncToleranceSlots     = 100
	DefaultForgeStaleGapThresholdSlots = 1000
	DefaultMempoolCapacityPraos        = 1048576  // 1 MiB
	DefaultMempoolCapacityLeios        = 26214400 // 25 MiB
	DefaultMempoolImplementation       = "fifo"
)

// RunMode represents the operational mode of the dingo node
type RunMode string

const (
	RunModeServe RunMode = "serve" // Full node with network connectivity (default)
	RunModeLoad  RunMode = "load"  // Batch import from ImmutableDB
	RunModeDev   RunMode = "dev"   // Development mode (isolated, no outbound)
	RunModeLeios RunMode = "leios" // Full node with experimental Leios capabilities

	// RunModeSync and RunModeMithril are effective run modes used only for
	// validation, not configurable runMode values (RunMode.Valid rejects
	// them); cmd/dingo passes the one matching the invoked command to
	// Config.Validate. Neither starts the relay/private serving listeners
	// or the API listeners. They differ in their auxiliary-listener
	// surface: RunModeSync is the Mithril snapshot sync operation (via
	// `dingo sync --mithril` or `dingo mithril sync`), which starts a
	// Prometheus metrics listener and an optional pprof debug listener;
	// RunModeMithril is the read-only Mithril query subcommands (`list`,
	// `show`, and bare `mithril`), which start no listeners at all.
	// Keeping them distinct lets Validate check exactly the ports each
	// invocation binds.
	RunModeSync    RunMode = "sync"
	RunModeMithril RunMode = "mithril"
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
	case RunModeSync, RunModeMithril:
		// Effective-only modes used for validation; never configurable runModes.
		return false
	default:
		return false
	}
}

// IsDevMode returns true if the mode enables development behaviors
// (forge blocks, disable outbound, skip topology)
func (m RunMode) IsDevMode() bool {
	return m == RunModeDev
}

// RequiresListeners reports whether an (effective) run mode runs as a
// serving node, starting the relay and private (NtN/NtC) listeners. The
// serving modes (serve, dev, leios, and the empty default) do; the load
// and one-shot sync/mithril utilities do not. (The metrics and debug
// listeners are gated separately by Validate: serving modes and the
// Mithril sync operation start them, the read-only Mithril subcommands do
// not.)
func (m RunMode) RequiresListeners() bool {
	switch m {
	case RunModeServe, RunModeDev, RunModeLeios, "":
		return true
	case RunModeLoad, RunModeSync, RunModeMithril:
		return false
	default:
		return false
	}
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
	Config *Config `yaml:"config,omitempty"`
}

var midnightYAMLFields map[string]struct{}

func collectMidnightYAMLFields(buf []byte) map[string]struct{} {
	var doc yaml.Node
	if err := yaml.Unmarshal(buf, &doc); err != nil {
		return nil
	}
	if len(doc.Content) == 0 {
		return nil
	}
	root := doc.Content[0]
	midnight := mappingValue(root, "midnight")
	if configNode := mappingValue(root, "config"); configNode != nil {
		midnight = mappingValue(configNode, "midnight")
	}
	if midnight == nil || midnight.Kind != yaml.MappingNode {
		return nil
	}
	fields := map[string]struct{}{}
	for i := 0; i+1 < len(midnight.Content); i += 2 {
		fields[midnight.Content[i].Value] = struct{}{}
	}
	return fields
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func midnightYAMLFieldSet(field string) bool {
	_, ok := midnightYAMLFields[field]
	return ok
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
	// Strategy selects how headers from multiple eligible peers drive
	// ledger ingress: "primary", "parallel", or "round-robin".
	// Default: "primary".
	Strategy string `yaml:"strategy"     envconfig:"DINGO_CHAINSYNC_STRATEGY"`
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
	// CorroborationPeers sets the number of independent peers that must report
	// the same recent blocks before a fast (shallow) block source may drive
	// Genesis-mode chain selection. This is the Ouroboros Genesis trust control
	// for biased fast-sync sources: an uncorroborated or divergent fast source
	// is denied selection and stalls rather than steering the local chain. A
	// zero value disables corroboration (density-only Genesis selection).
	CorroborationPeers int `yaml:"corroborationPeers" envconfig:"DINGO_GENESIS_BOOTSTRAP_CORROBORATION_PEERS"`
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
		Strategy:     "primary",
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

	CNightPolicyID              string `yaml:"cnightPolicyId"`
	CNightAssetName             string `yaml:"cnightAssetName"`
	MappingValidatorAddress     string `yaml:"mappingValidatorAddress"`
	AuthTokenPolicyID           string `yaml:"authTokenPolicyId"`
	AuthTokenAssetName          string `yaml:"authTokenAssetName"`
	CommitteeCandidateAddress   string `yaml:"committeeCandidateAddress"`
	TechnicalCommitteeAddress   string `yaml:"technicalCommitteeAddress"`
	TechnicalCommitteePolicyID  string `yaml:"technicalCommitteePolicyId"`
	CouncilAddress              string `yaml:"councilAddress"`
	CouncilPolicyID             string `yaml:"councilPolicyId"`
	PermissionedCandidatePolicy string `yaml:"permissionedCandidatePolicy"`
}

// DefaultMidnightConfig returns the default Midnight indexer settings.
func DefaultMidnightConfig() MidnightConfig {
	return MidnightConfig{
		Port: 50051,
		Host: "0.0.0.0",
	}
}

type Config struct {
	Plugins                PluginsConfig `yaml:"plugins"`
	TlsKeyFilePath         string        `yaml:"tlsKeyFilePath"     envconfig:"TLS_KEY_FILE_PATH"`
	Topology               string        `yaml:"topology"`
	CardanoConfig          string        `yaml:"cardanoConfig"      envconfig:"config"`
	DatabasePath           string        `yaml:"databasePath"                                                     split_words:"true"`
	SocketPath             string        `yaml:"socketPath"                                                       split_words:"true"`
	TlsCertFilePath        string        `yaml:"tlsCertFilePath"    envconfig:"TLS_CERT_FILE_PATH"`
	BindAddr               string        `yaml:"bindAddr"                                                         split_words:"true"`
	PrivateBindAddr        string        `yaml:"privateBindAddr"                                                  split_words:"true"`
	ShutdownTimeout        string        `yaml:"shutdownTimeout"                                                  split_words:"true"`
	LedgerCatchupTimeout   string        `yaml:"ledgerCatchupTimeout"  envconfig:"DINGO_LEDGER_CATCHUP_TIMEOUT"`
	Network                string        `yaml:"network"`
	NetworkMagic           uint32        `yaml:"networkMagic"                                                     split_words:"true"`
	PrivatePort            uint          `yaml:"privatePort"                                                      split_words:"true"`
	RelayPort              uint          `yaml:"relayPort"          envconfig:"port"`
	BarkBaseUrl            string        `yaml:"barkBaseUrl"        envconfig:"DINGO_BARK_BASE_URL"`
	BarkBlockDownloadHosts []string      `yaml:"barkBlockDownloadHosts" envconfig:"DINGO_BARK_BLOCK_DOWNLOAD_HOSTS"`
	BarkPort               uint          `yaml:"barkPort"           envconfig:"DINGO_BARK_PORT"`
	CORSAllowedOrigins     []string      `yaml:"corsAllowedOrigins" envconfig:"DINGO_CORS_ALLOWED_ORIGINS"`
	MetricsPort            uint          `yaml:"metricsPort"                                                      split_words:"true"`
	DebugPort              uint          `yaml:"debugPort"          envconfig:"DINGO_DEBUG_PORT"`
	IntersectTip           bool          `yaml:"intersectTip"                                                     split_words:"true"`
	ValidateHistorical     bool          `yaml:"validateHistorical"                                               split_words:"true"`
	// StrictUtxoValidation errors out (instead of silently skipping) when a
	// consumed UTxO cannot be found or recovered for a block past the
	// recorded Mithril sync boundary. Leave disabled when bootstrapping from
	// a non-genesis chainsync intersect point without a Mithril snapshot.
	StrictUtxoValidation bool `yaml:"strictUtxoValidation" split_words:"true"`
	// Tracing enables OpenTelemetry tracing. Disabled by default: with no
	// collector listening, the OTLP exporter logs noisy connection errors.
	// Spans are sent via OTLP HTTP; configure the destination with the
	// standard OTEL_EXPORTER_OTLP_* env vars.
	Tracing bool `yaml:"tracing" envconfig:"DINGO_TRACING_ENABLED"`
	// TracingStdout redirects spans to stdout instead of OTLP. Requires
	// Tracing to also be enabled. Mostly useful for local debugging.
	TracingStdout   bool     `yaml:"tracingStdout" envconfig:"DINGO_TRACING_STDOUT"`
	RunMode         RunMode  `yaml:"runMode"            envconfig:"DINGO_RUN_MODE"`
	StartEra        StartEra `yaml:"startEra"           envconfig:"DINGO_START_ERA"`
	ImmutableDbPath string   `yaml:"immutableDbPath"    envconfig:"DINGO_IMMUTABLE_DB_PATH"`
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
	ValidateForgedBlock           bool   `yaml:"validateForgedBlock"           envconfig:"DINGO_VALIDATE_FORGED_BLOCK"`

	// MinPoolMargin is the CIP-23 minimum pool margin (minimum variable fee) in
	// basis points, [0, 10000] (150 = 1.5%); 0 disables it. Consensus-affecting
	// and off by default; effective only in Dijkstra and later. Enable a nonzero
	// value only where every node also enables the same value. See
	// ARCHITECTURE.md ("Reward Calculation And Precomputation").
	MinPoolMargin uint `yaml:"minPoolMargin" envconfig:"DINGO_MIN_POOL_MARGIN"`
	// CIP-50 pledge-leverage staking rewards. Consensus-affecting; defaults
	// off. PledgeLeverageEnabled turns on the L*pledge reward cap and
	// PledgeLeverage is L in [1, 10000]. Enable only on a network where every
	// node also enables it. See
	// docs/plans/2026-07-19-cip50-pledge-leverage-design.md.
	PledgeLeverageEnabled bool `yaml:"pledgeLeverageEnabled" envconfig:"DINGO_PLEDGE_LEVERAGE_ENABLED"`
	PledgeLeverage        uint `yaml:"pledgeLeverage"        envconfig:"DINGO_PLEDGE_LEVERAGE"`
	// CIP-0163 full-pot reward distribution. Consensus-affecting; defaults
	// off. When enabled the entire epoch reward pot is distributed to eligible
	// pools and delegators instead of returning the residual to reserves.
	// Enable only on a network where every node also enables it. See
	// docs/plans/2026-07-19-cip163-full-pot-distribution-design.md.
	FullPotRewardsEnabled bool `yaml:"fullPotRewardsEnabled" envconfig:"DINGO_FULL_POT_REWARDS_ENABLED"`
	// UnsafeFullPotRewardsOnStandardNetworks is an explicit unsafe override
	// for running CIP-0163 full-pot rewards on predefined public networks.
	// Leave false except for controlled off-consensus experiments.
	UnsafeFullPotRewardsOnStandardNetworks bool `yaml:"unsafeFullPotRewardsOnStandardNetworks" envconfig:"DINGO_UNSAFE_FULL_POT_REWARDS_ON_STANDARD_NETWORKS"`
	// CIP-0163 reward-account inactivity expiry. Consensus-affecting; defaults
	// off. See ARCHITECTURE.md ("Stake Snapshots", CIP-0163 reward-account
	// inactivity).
	DelegatorInactivityEnabled bool   `yaml:"delegatorInactivityEnabled" envconfig:"DINGO_DELEGATOR_INACTIVITY_ENABLED"`
	DelegatorInactivity        uint64 `yaml:"delegatorInactivity"        envconfig:"DINGO_DELEGATOR_INACTIVITY"`

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

// PluginsConfig is the canonical configuration tree for compiled-in plugin
// capabilities.
type PluginsConfig struct {
	Storage StoragePluginsConfig `yaml:"storage"`
	Mempool hostplugin.Selection `yaml:"mempool"`
	API     APIPluginsConfig     `yaml:"api"`
}

type StoragePluginsConfig struct {
	Blob     hostplugin.Selection `yaml:"blob"`
	Metadata hostplugin.Selection `yaml:"metadata"`
}

type APIPluginsConfig struct {
	Blockfrost hostplugin.Selection `yaml:"blockfrost"`
	Mesh       hostplugin.Selection `yaml:"mesh"`
	Utxorpc    hostplugin.Selection `yaml:"utxorpc"`
}

func defaultPluginsConfig() PluginsConfig {
	return PluginsConfig{
		Storage: StoragePluginsConfig{
			Blob:     hostplugin.Selection{Provider: "badger", Config: map[string]any{}},
			Metadata: hostplugin.Selection{Provider: "sqlite", Config: map[string]any{}},
		},
		Mempool: hostplugin.Selection{Provider: "default", Config: map[string]any{
			"evictionWatermark":  DefaultEvictionWatermark,
			"rejectionWatermark": DefaultRejectionWatermark,
		}},
		API: APIPluginsConfig{
			Blockfrost: hostplugin.Selection{Provider: "builtin", Config: map[string]any{"port": 3000}},
			Mesh:       hostplugin.Selection{Provider: "builtin", Config: map[string]any{"port": 8080}},
			Utxorpc:    hostplugin.Selection{Provider: "builtin", Config: map[string]any{"port": 9090}},
		},
	}
}

// midnightNetworkDefaults holds per-network Midnight constants sourced from
// Acropolis. At the time these defaults were added, Acropolis published
// constants for mainnet and preview only.
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

func clearMidnightNetworkDefaults(cfg *Config, network string) {
	defaults, ok := midnightNetworkDefaults[network]
	if !ok {
		return
	}
	if !midnightYAMLFieldSet("cnightPolicyId") &&
		cfg.Midnight.CNightPolicyID == defaults.CNightPolicyID {
		cfg.Midnight.CNightPolicyID = ""
	}
	if !midnightYAMLFieldSet("cnightAssetName") &&
		cfg.Midnight.CNightAssetName == defaults.CNightAssetName {
		cfg.Midnight.CNightAssetName = ""
	}
	if !midnightYAMLFieldSet("mappingValidatorAddress") &&
		cfg.Midnight.MappingValidatorAddress == defaults.MappingValidatorAddress {
		cfg.Midnight.MappingValidatorAddress = ""
	}
	if !midnightYAMLFieldSet("authTokenAssetName") &&
		cfg.Midnight.AuthTokenAssetName == defaults.AuthTokenAssetName {
		cfg.Midnight.AuthTokenAssetName = ""
	}
	if !midnightYAMLFieldSet("committeeCandidateAddress") &&
		cfg.Midnight.CommitteeCandidateAddress == defaults.CommitteeCandidateAddress {
		cfg.Midnight.CommitteeCandidateAddress = ""
	}
	if !midnightYAMLFieldSet("technicalCommitteeAddress") &&
		cfg.Midnight.TechnicalCommitteeAddress == defaults.TechnicalCommitteeAddress {
		cfg.Midnight.TechnicalCommitteeAddress = ""
	}
	if !midnightYAMLFieldSet("technicalCommitteePolicyId") &&
		cfg.Midnight.TechnicalCommitteePolicyID == defaults.TechnicalCommitteePolicyID {
		cfg.Midnight.TechnicalCommitteePolicyID = ""
	}
	if !midnightYAMLFieldSet("councilAddress") &&
		cfg.Midnight.CouncilAddress == defaults.CouncilAddress {
		cfg.Midnight.CouncilAddress = ""
	}
	if !midnightYAMLFieldSet("councilPolicyId") &&
		cfg.Midnight.CouncilPolicyID == defaults.CouncilPolicyID {
		cfg.Midnight.CouncilPolicyID = ""
	}
	if !midnightYAMLFieldSet("permissionedCandidatePolicy") &&
		cfg.Midnight.PermissionedCandidatePolicy == defaults.PermissionedCandidatePolicy {
		cfg.Midnight.PermissionedCandidatePolicy = ""
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

var globalConfig = &Config{
	Plugins:              defaultPluginsConfig(),
	BindAddr:             "0.0.0.0",
	CardanoConfig:        "", // Will be set dynamically based on network
	DatabasePath:         ".dingo",
	SocketPath:           "dingo.socket",
	IntersectTip:         false,
	ValidateHistorical:   false,
	StrictUtxoValidation: false,
	Tracing:              false,
	TracingStdout:        false,
	Network:              "preview",
	NetworkMagic:         0,
	MetricsPort:          12798,
	DebugPort:            0,
	PrivateBindAddr:      "127.0.0.1",
	PrivatePort:          3002,
	RelayPort:            3001,
	BarkBaseUrl:          "",
	BarkPort:             0,
	CORSAllowedOrigins:   []string{"*"},
	Topology:             "",
	TlsCertFilePath:      "",
	TlsKeyFilePath:       "",
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
	// CIP-50 default L (feature disabled by default via PledgeLeverageEnabled)
	PledgeLeverage: 100,
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
	midnightYAMLFields = nil

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
		midnightYAMLFields = collectMidnightYAMLFields(buf)

		var root map[string]yaml.Node
		if err := yaml.Unmarshal(buf, &root); err != nil {
			return nil, fmt.Errorf("error parsing config file: %w", err)
		}
		if _, wrapped := root["config"]; wrapped {
			tempCfg := tempConfig{Config: globalConfig}
			decoder := yaml.NewDecoder(bytes.NewReader(buf))
			decoder.KnownFields(true)
			if err := decoder.Decode(&tempCfg); err != nil && !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("error parsing config section: %w", err)
			}
			if tempCfg.Config == nil {
				return nil, errors.New("config section must be a mapping")
			}
		} else {
			decoder := yaml.NewDecoder(bytes.NewReader(buf))
			decoder.KnownFields(true)
			if err := decoder.Decode(globalConfig); err != nil && !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("error parsing config file: %w", err)
			}
		}
	}
	// Process environment variables
	err := envconfig.Process("cardano", globalConfig)
	if err != nil {
		return nil, fmt.Errorf("error processing environment: %+w", err)
	}
	pluginEnviron := os.Environ()
	if err := applyAPIPortCompatibilityEnvironment(
		globalConfig,
		pluginEnviron,
	); err != nil {
		return nil, fmt.Errorf("process API port compatibility environment: %w", err)
	}
	pluginSelections := []struct {
		capability hostplugin.Capability
		selection  *hostplugin.Selection
	}{
		{hostplugin.CapabilityStorageBlob, &globalConfig.Plugins.Storage.Blob},
		{hostplugin.CapabilityStorageMetadata, &globalConfig.Plugins.Storage.Metadata},
		{hostplugin.CapabilityMempool, &globalConfig.Plugins.Mempool},
		{hostplugin.CapabilityAPIBlockfrost, &globalConfig.Plugins.API.Blockfrost},
		{hostplugin.CapabilityAPIMesh, &globalConfig.Plugins.API.Mesh},
		{hostplugin.CapabilityAPIUtxorpc, &globalConfig.Plugins.API.Utxorpc},
	}
	for _, item := range pluginSelections {
		if err := hostplugin.ApplyEnvironment(item.capability, item.selection, pluginEnviron); err != nil {
			return nil, fmt.Errorf("process plugin environment: %w", err)
		}
	}

	// LoadConfig only parses and merges configuration sources; it makes
	// no semantic judgments about the merged values. CLI flags are a
	// higher-precedence source merged afterwards by ApplyFlags, so any
	// defaulting or validation here would act on values a flag may
	// still override — defaults derived from the final configuration
	// are applied by ApplyDefaults, and semantic checks run in
	// Validate, both called after ApplyFlags.
	//
	// The Midnight network defaults applied here are the exception:
	// they let a config loaded without CLI flags resolve its per-network
	// values, and ApplyFlags compensates for a network change by
	// clearing the previous network's defaults and reapplying.
	applyMidnightNetworkDefaults(globalConfig)

	// NOTE: Do not set a default CardanoConfig here. The network flag
	// can be overridden after LoadConfig returns (see main.go
	// PersistentPreRunE). Each consumer resolves the cardano config
	// path using cfg.Network at call time instead. Topology is likewise
	// not resolved here: it derives from Network and Topology, both of
	// which a CLI flag may still change, so cmd/dingo loads it once
	// after the merged configuration has been defaulted and validated.

	return globalConfig, nil
}

// applyAPIPortCompatibilityEnvironment maps the API port environment names
// used before API providers became plugins onto their canonical plugin config.
// The generic plugin environment is applied afterwards, so the canonical name
// wins when an operator sets both forms during a migration.
func applyAPIPortCompatibilityEnvironment(cfg *Config, environ []string) error {
	type apiPortCompatibility struct {
		legacyName    string
		canonicalName string
		selection     *hostplugin.Selection
	}
	ports := []apiPortCompatibility{
		{
			legacyName:    "DINGO_BLOCKFROST_PORT",
			canonicalName: "DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT",
			selection:     &cfg.Plugins.API.Blockfrost,
		},
		{
			legacyName:    "DINGO_MESH_PORT",
			canonicalName: "DINGO_PLUGINS_API_MESH_CONFIG_PORT",
			selection:     &cfg.Plugins.API.Mesh,
		},
		{
			legacyName:    "DINGO_UTXORPC_PORT",
			canonicalName: "DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT",
			selection:     &cfg.Plugins.API.Utxorpc,
		},
	}
	values := make(map[string]string, len(environ))
	for _, entry := range environ {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			values[name] = value
		}
	}
	for _, port := range ports {
		if _, ok := values[port.canonicalName]; ok {
			continue
		}
		value, ok := values[port.legacyName]
		if !ok {
			continue
		}
		scalar, err := strconv.ParseUint(value, 0, 64)
		if err != nil {
			return fmt.Errorf("parse %s: %w", port.legacyName, err)
		}
		if port.selection.Config == nil {
			port.selection.Config = make(map[string]any)
		}
		port.selection.Config["port"] = scalar
	}
	return nil
}

// ApplyDefaults fills in unset values whose defaults depend on other
// settings in the fully merged configuration — most notably
// MempoolCapacity, whose default is chosen by RunMode. It must run
// after every configuration source has been merged (defaults, YAML,
// environment, and CLI flags via ApplyFlags): defaulting earlier would
// derive values from settings a higher-precedence source is still
// allowed to change. Call it before Validate; Validate rejects any
// value that is still invalid after defaulting.
func (c *Config) ApplyDefaults() {
	// An empty runMode selects the standard serving mode
	if c.RunMode == "" {
		c.RunMode = RunModeServe
	}
	if c.Plugins.Mempool.Config == nil {
		c.Plugins.Mempool.Config = make(map[string]any)
	}
	// Unset mempool capacity defaults based on RunMode.
	if _, ok := c.Plugins.Mempool.Config["capacity"]; !ok {
		if c.RunMode == RunModeLeios {
			c.Plugins.Mempool.Config["capacity"] = int64(DefaultMempoolCapacityLeios)
		} else {
			c.Plugins.Mempool.Config["capacity"] = int64(DefaultMempoolCapacityPraos)
		}
	}
	// Unset float64 fields are 0, which is indistinguishable from an
	// explicit 0; both select the standard watermark
	if pluginFloat64(c.Plugins.Mempool.Config["evictionWatermark"]) == 0 {
		c.Plugins.Mempool.Config["evictionWatermark"] = DefaultEvictionWatermark
	}
	if pluginFloat64(c.Plugins.Mempool.Config["rejectionWatermark"]) == 0 {
		c.Plugins.Mempool.Config["rejectionWatermark"] = DefaultRejectionWatermark
	}
	if c.ForgeSyncToleranceSlots == 0 {
		c.ForgeSyncToleranceSlots = DefaultForgeSyncToleranceSlots
	}
	if c.ForgeStaleGapThresholdSlots == 0 {
		c.ForgeStaleGapThresholdSlots = DefaultForgeStaleGapThresholdSlots
	}
	// Only an unset (zero) frequency takes the default; an explicitly
	// negative value is preserved so Validate can reject it instead of
	// the node silently starting the expiry worker on the default cadence
	if c.HistoryExpiry.Frequency == 0 {
		c.HistoryExpiry.Frequency = time.Hour
	}
}

func pluginInt64(value any) int64 {
	switch value := value.(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case uint:
		// #nosec G115 -- semantic range validation runs immediately afterwards.
		return int64(value)
	case uint64:
		// #nosec G115 -- semantic range validation runs immediately afterwards.
		return int64(value)
	case float64:
		return int64(value)
	default:
		return 0
	}
}

func pluginUint(value any) uint {
	// #nosec G115 -- semantic port range validation runs afterwards.
	return uint(pluginInt64(value))
}

// MempoolSettings returns the canonical default-provider settings after
// ApplyDefaults has run.
func (c *Config) MempoolSettings() (int64, float64, float64) {
	return pluginInt64(c.Plugins.Mempool.Config["capacity"]),
		pluginFloat64(c.Plugins.Mempool.Config["evictionWatermark"]),
		pluginFloat64(c.Plugins.Mempool.Config["rejectionWatermark"])
}

// APIPluginPort returns the configured port for a built-in API selection.
func APIPluginPort(selection hostplugin.Selection) uint {
	return pluginUint(selection.Config["port"])
}

func pluginFloat64(value any) float64 {
	switch value := value.(type) {
	case float64:
		return value
	case int:
		return float64(value)
	case int64:
		return float64(value)
	default:
		return 0
	}
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
		if cfg.Network == "" {
			// A networkMagic-only configuration has no network name to
			// resolve an embedded topology or bootstrap peers from;
			// peers must come from an explicit topology file. Return an
			// empty topology (as dev mode does) rather than failing
			// config load.
			return &topology.TopologyConfig{}, nil
		}
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
