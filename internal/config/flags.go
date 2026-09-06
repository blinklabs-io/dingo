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
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	storageModeCore = "core"
	storageModeAPI  = "api"
)

// flagSpec declares a CLI flag bound to a Config field by dotted path.
// register installs the flag with its default read from globalConfig;
// apply writes the parsed flag value back to cfg when the user set it.
type flagSpec struct {
	field    string
	name     string
	register func(*pflag.FlagSet, *Config)
	apply    func(*pflag.FlagSet, *Config) error
}

// flagSpecs is the single source of truth for every Config CLI flag.
// Ordering controls --help output and error reporting precedence.
var flagSpecs = []flagSpec{
	// Core
	stringFlag(
		"Plugins.Storage.Blob.Provider",
		"blob",
		"b",
		"blob store provider",
	),
	stringFlag(
		"Plugins.Storage.Metadata.Provider",
		"metadata",
		"m",
		"metadata store provider",
	),
	stringFlag("Plugins.Mempool.Provider", "mempool", "", "mempool provider"),
	stringFlag(
		"DatabasePath",
		"data-dir",
		"",
		"data directory for all storage plugins (overrides CARDANO_DATABASE_PATH)",
	),
	stringFlag("BindAddr", "bind-addr", "", "public bind address"),
	stringFlag("SocketPath", "socket-path", "", "path to UNIX socket file"),
	transformStringFlag(
		"RunMode",
		"run-mode",
		"run mode: serve, load, dev, or leios",
		normalizeRunMode,
	),
	transformStringFlag(
		"StartEra",
		"start-era",
		"experimental start era: dijkstra",
		normalizeStartEra,
	),
	transformStringFlag(
		"StorageMode",
		"storage-mode",
		`storage mode: "core" (minimal) or "api" (full indexing)`,
		normalizeStorageMode,
	),
	stringFlag(
		"CardanoConfig",
		"cardano-config",
		"",
		"path to Cardano config file",
	),
	stringFlag("Topology", "topology", "", "path to topology file"),
	stringFlag(
		"ShutdownTimeout",
		"shutdown-timeout",
		"",
		"graceful shutdown timeout",
	),
	stringFlag(
		"LedgerCatchupTimeout",
		"ledger-catchup-timeout",
		"",
		"ledger catch-up timeout for load mode",
	),
	stringFlag(
		"TlsCertFilePath",
		"tls-cert-file-path",
		"",
		"path to TLS certificate file",
	),
	stringFlag(
		"TlsKeyFilePath",
		"tls-key-file-path",
		"",
		"path to TLS private key file",
	),
	stringFlag(
		"ImmutableDbPath",
		"immutable-db-path",
		"",
		"path to ImmutableDB for load mode",
	),
	boolFlag("IntersectTip", "intersect-tip", "start from current tip"),
	boolFlag(
		"ValidateHistorical",
		"validate-historical",
		"validate historical blocks",
	),
	boolFlag(
		"StrictUtxoValidation",
		"strict-utxo-validation",
		"error instead of skipping when a consumed UTxO past the Mithril sync boundary cannot be found or recovered",
	),
	boolFlag(
		"Tracing",
		"tracing",
		"enable OpenTelemetry tracing (configure destination with OTEL_EXPORTER_OTLP_* env vars)",
	),
	boolFlag(
		"TracingStdout",
		"tracing-stdout",
		"export traces to stdout instead of OTLP (requires --tracing; for debugging)",
	),

	// Networking
	// An explicitly empty --network is allowed: Validate() enforces
	// that network or networkMagic is set, so a magic-only invocation
	// can clear a configured network name.
	validatedStringFlag(
		"Network",
		"network",
		"n",
		"Cardano network name (e.g. preview, preprod, mainnet)",
		func(v string) error {
			if v == "" {
				return nil
			}
			return ValidateNetworkName(v)
		},
	),
	uint32Flag("NetworkMagic", "network-magic", "network magic override"),
	uintFlag("RelayPort", "port", "relay/NtN port"),
	stringFlag(
		"PrivateBindAddr",
		"private-bind-addr",
		"",
		"private bind address",
	),
	uintFlag("PrivatePort", "private-port", "private/NtC port"),
	uintFlag("MetricsPort", "metrics-port", "metrics port"),
	stringFlag(
		"DebugBindAddr",
		"debug-bind-addr",
		"",
		"pprof bind address (wildcard exposure requires an explicit override)",
	),
	uintFlag("DebugPort", "debug-port", "debug pprof port (0 = disabled)"),
	boolPtrFlag(
		"PeerSharing",
		"peer-sharing",
		"enable peer sharing protocol (default: cardano-node config.json fallback for non-block-producers; false for block producers)",
	),

	// APIs
	stringFlag(
		"Plugins.API.Utxorpc.Provider",
		"utxorpc-provider",
		"",
		"UTxO RPC API provider",
	),
	stringFlag(
		"Plugins.API.Blockfrost.Provider",
		"blockfrost-provider",
		"",
		"Blockfrost API provider",
	),
	stringFlag(
		"Plugins.API.Mesh.Provider",
		"mesh-provider",
		"",
		"Mesh API provider",
	),
	stringSliceFlag(
		"CORSAllowedOrigins",
		"cors-allowed-origins",
		"CORS allowed origins for API servers",
	),

	// API security (shared TLS/auth defaults for every selected
	// plugins.api.* provider; see internal/apiconfig and
	// ARCHITECTURE.md's "API security" section). Explicit
	// plugins.api.<name>.config.tls/auth fields override these per
	// provider.
	stringPtrFlag(
		"API.TLS.Mode",
		"api-tls-mode",
		`shared API TLS mode: "disabled" or "server" (unset: inherit provider setting, else disabled)`,
	),
	stringPtrFlag(
		"API.TLS.CertFilePath",
		"api-tls-cert-file-path",
		"shared API TLS certificate file path",
	),
	stringPtrFlag(
		"API.TLS.KeyFilePath",
		"api-tls-key-file-path",
		"shared API TLS private key file path",
	),
	stringPtrFlag(
		"API.Auth.Mode",
		"api-auth-mode",
		`shared API auth mode: "disabled" or "token" (unset: inherit provider setting, else disabled)`,
	),
	stringPtrFlag(
		"API.Auth.TokenFilePath",
		"api-auth-token-file-path",
		"shared API auth bearer token file path",
	),
	durationFlag(
		"OffchainMetadata.Interval",
		"offchain-metadata-interval",
		"off-chain metadata fetch interval (0 = default)",
	),
	durationFlag(
		"OffchainMetadata.RequestTimeout",
		"offchain-metadata-request-timeout",
		"off-chain metadata HTTP request timeout (0 = default)",
	),
	stringFlag(
		"OffchainMetadata.UserAgent",
		"offchain-metadata-user-agent",
		"",
		"off-chain metadata HTTP user agent (empty = default)",
	),
	stringFlag(
		"OffchainMetadata.IPFSGatewayURL",
		"offchain-metadata-ipfs-gateway-url",
		"",
		"IPFS gateway URL for off-chain metadata (empty = default)",
	),
	intFlag(
		"OffchainMetadata.BatchSize",
		"offchain-metadata-batch-size",
		"off-chain metadata rows to claim per pass (0 = default)",
	),
	int64Flag(
		"OffchainMetadata.MaxBytes",
		"offchain-metadata-max-bytes",
		"off-chain metadata max response bytes (0 = default)",
	),
	boolFlag(
		"OffchainMetadata.AllowPrivateAddresses",
		"offchain-metadata-allow-private-addresses",
		"allow off-chain metadata fetches to private, loopback, and link-local addresses",
	),
	boolFlag(
		"TokenRegistry.Enabled",
		"token-registry-enabled",
		`enable the CIP-26 token registry sync (requires storageMode "api")`,
	),
	stringFlag(
		"TokenRegistry.SourceURL",
		"token-registry-source-url",
		"",
		"CIP-26 token registry tarball URL (empty = select by network)",
	),
	durationFlag(
		"TokenRegistry.Interval",
		"token-registry-interval",
		"CIP-26 token registry sync interval (0 = default)",
	),
	durationFlag(
		"TokenRegistry.RequestTimeout",
		"token-registry-request-timeout",
		"CIP-26 token registry download timeout (0 = default)",
	),
	stringFlag(
		"TokenRegistry.UserAgent",
		"token-registry-user-agent",
		"",
		"CIP-26 token registry HTTP user agent (empty = default)",
	),
	int64Flag(
		"TokenRegistry.MaxBytes",
		"token-registry-max-bytes",
		"CIP-26 token registry max compressed download bytes (0 = default)",
	),
	int64Flag(
		"TokenRegistry.MaxEntryBytes",
		"token-registry-max-entry-bytes",
		"CIP-26 token registry max bytes per mapping (0 = default)",
	),
	boolFlag(
		"TokenRegistry.StoreLogos",
		"token-registry-store-logos",
		"persist CIP-26 token registry logos (roughly 90% of registry bytes)",
	),
	boolFlag(
		"TokenRegistry.AllowPrivateAddresses",
		"token-registry-allow-private-addresses",
		"allow token registry sync from private, loopback, and link-local addresses",
	),
	boolFlag(
		"Midnight.Enabled",
		"midnight-enabled",
		`enable the Midnight indexer (requires storageMode "api")`,
	),
	boolFlag(
		"Midnight.ServerEnabled",
		"midnight-server-enabled",
		`enable the Midnight gRPC server (requires storageMode "api")`,
	),
	boolFlag(
		"Midnight.ReflectionEnabled",
		"midnight-reflection-enabled",
		"enable Midnight gRPC reflection",
	),
	boolFlag(
		"Midnight.AllowInsecureRemote",
		"midnight-allow-insecure-remote",
		"allow plaintext Midnight gRPC on a non-loopback address",
	),
	uintFlag(
		"Midnight.Port",
		"midnight-port",
		"Midnight gRPC port (must be non-zero when the server is enabled)",
	),
	stringFlag(
		"Midnight.Host",
		"midnight-host",
		"",
		"Midnight gRPC listen address",
	),

	// Bark
	stringFlag("BarkBaseUrl", "bark-url", "", "Bark archive fallback base URL"),
	stringSliceFlag(
		"BarkBlockDownloadHosts",
		"bark-block-download-hosts",
		"allowed HTTPS hostnames for Bark block downloads",
	),
	uintFlag("BarkPort", "bark-port", "Bark RPC port"),
	stringFlag(
		"BarkHost",
		"bark-host",
		"",
		"Bark RPC listen address (defaults to loopback-only when the database lifecycle service is enabled, all interfaces otherwise)",
	),
	stringFlag(
		"BarkClientCAFilePath",
		"bark-client-ca-file-path",
		"",
		"path to a PEM CA bundle; client certs verified against it authenticate every Bark DatabaseService RPC (required whenever the database lifecycle service is enabled)",
	),
	stringSliceFlag(
		"BarkOperatorCertificateFingerprints",
		"bark-operator-certificate-fingerprints",
		"SHA-256 client certificate fingerprints authorized for destructive Bark DatabaseService RPCs",
	),

	// History expiry
	boolFlag(
		"HistoryExpiry.Enabled",
		"history-expiry-enabled",
		"enable local immutable block history expiry",
	),
	durationFlag(
		"HistoryExpiry.Frequency",
		"history-expiry-frequency",
		"history expiry scan frequency",
	),

	// Koios reward-parity observer (dingo #3098; one-off validation aid, not a
	// permanent subsystem)
	boolFlag(
		"KoiosParity.Enabled",
		"koios-parity-enabled",
		"validate closed-epoch reward state against Koios reference data as the node advances",
	),
	stringFlag(
		"KoiosParity.Network",
		"koios-parity-network",
		"",
		"Koios network to validate against: preview or preprod (default: node's own --network)",
	),
	stringFlag(
		"KoiosParity.CachePath",
		"koios-parity-cache-path",
		"",
		"Koios reference cache.db path (default: {data-dir}/.koios/cache.db)",
	),
	stringFlag(
		"KoiosParity.APIKey",
		"koios-parity-api-key",
		"",
		"Koios Bearer token for rate-limited access",
	),
	boolFlag(
		"KoiosParity.Strict",
		"koios-parity-strict",
		"stop/cancel the node on the first Koios/tool error or exact parity mismatch",
	),
	intFlag(
		"KoiosParity.GraceHours",
		"koios-parity-grace-hours",
		"hours after an epoch closes during which a missing Dingo-side row is treated as sync lag, not a failure",
	),
	boolFlag(
		"KoiosParity.Accounts",
		"koios-parity-accounts",
		"also validate #3097 per-account exact reward parity for every epoch (default: true)",
	),
	intFlag(
		"KoiosParity.AccountChunkSize",
		"koios-parity-account-chunk-size",
		"max stake addresses per /account_reward_history request (0 = package default, 100)",
	),
	intFlag(
		"KoiosParity.AccountChunkMaxBytes",
		"koios-parity-account-chunk-max-bytes",
		"max encoded body size per /account_reward_history request (0 = package default, 4KiB)",
	),

	// Peer governance
	intFlag(
		"TargetNumberOfKnownPeers",
		"target-known-peers",
		"target number of known peers",
	),
	intFlag(
		"TargetNumberOfEstablishedPeers",
		"target-established-peers",
		"target number of established peers",
	),
	intFlag(
		"TargetNumberOfActivePeers",
		"target-active-peers",
		"target number of active peers",
	),
	intFlag(
		"TargetNumberOfRootPeers",
		"target-root-peers",
		"target number of root peers",
	),
	intFlag(
		"ActivePeersTopologyQuota",
		"active-peers-topology-quota",
		"active peers topology source quota",
	),
	intFlag(
		"ActivePeersGossipQuota",
		"active-peers-gossip-quota",
		"active peers gossip source quota",
	),
	intFlag(
		"ActivePeersLedgerQuota",
		"active-peers-ledger-quota",
		"active peers ledger source quota",
	),
	intFlag("MinHotPeers", "min-hot-peers", "minimum hot peers"),
	durationFlag(
		"ReconcileInterval",
		"reconcile-interval",
		"peer governor reconcile interval",
	),
	durationFlag(
		"InactivityTimeout",
		"inactivity-timeout",
		"peer governor inactivity timeout",
	),
	intFlag(
		"InboundWarmTarget",
		"inbound-warm-target",
		"inbound warm peer target",
	),
	intFlag("InboundHotQuota", "inbound-hot-quota", "inbound hot peer quota"),
	durationFlag(
		"InboundMinTenure",
		"inbound-min-tenure",
		"minimum inbound tenure before hot promotion",
	),
	float64Flag(
		"InboundHotScoreThreshold",
		"inbound-hot-score-threshold",
		"minimum inbound score for hot promotion",
	),
	durationFlag(
		"InboundPruneAfter",
		"inbound-prune-after",
		"inbound prune grace duration",
	),
	boolFlag(
		"InboundDuplexOnlyForHot",
		"inbound-duplex-only-for-hot",
		"restrict duplex inbound handling to hot peers",
	),
	durationFlag(
		"InboundCooldown",
		"inbound-cooldown",
		"inbound governance cooldown duration",
	),
	intFlag(
		"MaxConnectionsPerIP",
		"max-connections-per-ip",
		"max simultaneous connections per IP",
	),
	intFlag("MaxInboundConns", "max-inbound-conns", "max inbound connections"),

	// Cache
	intFlag(
		"Cache.HotUtxoEntries",
		"cache-hot-utxo-entries",
		"hot UTxO cache entry limit",
	),
	intFlag(
		"Cache.HotTxEntries",
		"cache-hot-tx-entries",
		"hot TX cache entry limit",
	),
	int64Flag(
		"Cache.HotTxMaxBytes",
		"cache-hot-tx-max-bytes",
		"hot TX cache max bytes",
	),
	intFlag(
		"Cache.BlockLRUEntries",
		"cache-block-lru-entries",
		"block LRU cache entry limit",
	),
	intFlag(
		"Cache.WarmupBlocks",
		"cache-warmup-blocks",
		"cache warmup block count",
	),
	boolFlag(
		"Cache.WarmupSync",
		"cache-warmup-sync",
		"wait for cache warmup before serving",
	),

	// Chainsync
	intFlag(
		"Chainsync.MaxClients",
		"chainsync-max-clients",
		"max chainsync clients",
	),
	stringFlag(
		"Chainsync.StallTimeout",
		"chainsync-stall-timeout",
		"",
		"chainsync stall timeout",
	),
	stringFlag(
		"Chainsync.Strategy",
		"chainsync-strategy",
		"",
		"chainsync header sync strategy (primary|parallel|round-robin)",
	),

	// Genesis bootstrap
	boolFlag(
		"GenesisBootstrap.Enabled",
		"genesis-bootstrap-enabled",
		"enable Genesis bootstrap mode when starting from origin",
	),
	uint64Flag(
		"GenesisBootstrap.WindowSlots",
		"genesis-bootstrap-window-slots",
		"Genesis density comparison window in slots (0 derives from Shelley genesis 3k/f)",
	),
	intFlag(
		"GenesisBootstrap.PromotionMinDiversityGroups",
		"genesis-bootstrap-promotion-min-diversity-groups",
		"minimum diversity groups preferred during Genesis bootstrap peer promotion",
	),
	intFlag(
		"GenesisBootstrap.CorroborationPeers",
		"genesis-bootstrap-corroboration-peers",
		"independent peers that must corroborate a fast source before it drives Genesis selection (0 disables)",
	),

	// Logging
	transformStringFlag(
		"Logging.Format",
		"logging-format",
		"log output format: text (default) or json",
		normalizeLoggingValue,
	),
	transformStringFlag(
		"Logging.Level",
		"logging-level",
		"log level: debug, info (default), warn, or error",
		normalizeLoggingValue,
	),

	// Database workers and API backfill
	intFlag(
		"DatabaseWorkers",
		"db-workers",
		"database worker pool worker count",
	),
	intFlag(
		"DatabaseQueueSize",
		"db-queue-size",
		"database worker pool task queue size",
	),
	intFlag(
		"BackfillBatchSize",
		"backfill-batch-size",
		"API-mode metadata backfill block batch size",
	),
	boolFlag(
		"BlockPipelineEnabled",
		"block-pipeline-enabled",
		"decode blocks in the chainsync replay loop with a parallel worker pool instead of serially (not consensus-affecting; default off)",
	),
	boolFlag(
		"BlockPipelineValidateEnabled",
		"block-pipeline-validate-enabled",
		"also VRF/KES-validate blocks in the block-pipeline replay loop with a parallel worker pool (requires block-pipeline-enabled; default off)",
	),

	// Block production
	boolFlag("BlockProducer", "block-producer", "enable block production mode"),
	stringFlag(
		"ShelleyVRFKey",
		"shelley-vrf-key",
		"",
		"path to Shelley VRF signing key",
	),
	stringFlag(
		"ShelleyKESKey",
		"shelley-kes-key",
		"",
		"path to Shelley KES signing key",
	),
	stringFlag(
		"ShelleyOperationalCertificate",
		"shelley-opcert",
		"",
		"path to Shelley operational certificate",
	),
	uint64Flag(
		"SlotsPerKESPeriod",
		"slots-per-kes-period",
		"slots per KES period",
	),
	uint64Flag(
		"MaxKESEvolutions",
		"max-kes-evolutions",
		"maximum KES evolutions before certificate rotation",
	),
	uint64Flag(
		"ForgeSyncToleranceSlots",
		"forge-sync-tolerance-slots",
		"max slots behind tip before skipping block forging",
	),
	uint64Flag(
		"ForgeStaleGapThresholdSlots",
		"forge-stale-gap-threshold-slots",
		"slot gap threshold for stale slot clock alerts",
	),
	uint64Flag(
		"ForgeHeaderFrontierToleranceSlots",
		"forge-header-frontier-tolerance-slots",
		"max slots the ledger-applied tip may trail this node's own header frontier before skipping block forging",
	),
	boolFlag(
		"ValidateForgedBlock",
		"validate-forged-block",
		"validate forged blocks before adoption and diffusion (header crypto, body hash, per-tx ledger rules)",
	),

	// CIP-23 minimum pool margin / minimum variable fee (consensus-affecting; default 0 = off)
	uintFlag(
		"MinPoolMargin",
		"min-pool-margin",
		"CIP-23 minimum pool margin in basis points [0,10000] (150 = 1.5%); 0 disables (enable only where every node also enables it)",
	),
	// CIP-0163 full-pot reward distribution (consensus-affecting; default off)
	boolFlag(
		"FullPotRewardsEnabled",
		"full-pot-rewards-enabled",
		"enable CIP-0163 full-pot reward distribution (custom networks only unless explicitly unsafe)",
	),
	boolFlag(
		"UnsafeFullPotRewardsOnStandardNetworks",
		"unsafe-full-pot-rewards-on-standard-networks",
		"allow CIP-0163 full-pot rewards on predefined standard networks; consensus-breaking unless the network has adopted it",
	),
	// CIP-0163 reward-account inactivity expiry (consensus-affecting; default off)
	boolFlag(
		"DelegatorInactivityEnabled",
		"delegator-inactivity-enabled",
		"enable CIP-0163 reward-account inactivity expiry (only where every node also enables it)",
	),
	uint64Flag(
		"DelegatorInactivity",
		"delegator-inactivity",
		"CIP-0163 inactivity window in epochs, in [1,10000] (used when delegator-inactivity-enabled)",
	),

	// CIP-50 pledge-leverage staking rewards (consensus-affecting; default off)
	boolFlag(
		"PledgeLeverageEnabled",
		"pledge-leverage-enabled",
		"enable the CIP-50 pledge-leverage reward cap (only where every node also enables it)",
	),
	uintFlag(
		"PledgeLeverage",
		"pledge-leverage",
		"CIP-50 max pledge leverage L in [1,10000] (used when pledge-leverage-enabled)",
	),

	// Leios voting (experimental)
	stringFlag(
		"LeiosVoteSigningKeyFile",
		"leios-vote-signing-key-file",
		"",
		"path to Cardano text-envelope BLS12-381 Leios vote signing key or legacy raw hex scalar",
	),
	// Mithril
	boolFlag(
		"Mithril.Enabled",
		"mithril-enabled",
		"enable Mithril integration",
	),
	stringFlag(
		"Mithril.AggregatorURL",
		"mithril-aggregator-url",
		"",
		"Mithril aggregator URL override",
	),
	stringFlag(
		"Mithril.Backend",
		"mithril-backend",
		"",
		"Mithril artifact backend: v1 (legacy snapshots) or v2 (incremental database)",
	),
	stringFlag(
		"Mithril.DownloadDir",
		"mithril-download-dir",
		"",
		"Mithril snapshot download directory",
	),
	stringFlag(
		"Mithril.DownloadIdleTimeout",
		"mithril-download-idle-timeout",
		"",
		"Mithril snapshot download idle timeout",
	),
	intFlag(
		"Mithril.DownloadMaxIdleRetries",
		"mithril-download-max-idle-retries",
		"Mithril snapshot download idle retries without progress",
	),
	boolFlag(
		"Mithril.CleanupAfterLoad",
		"mithril-cleanup-after-load",
		"cleanup Mithril files after load",
	),
	boolFlag(
		"Mithril.VerifyCertificates",
		"mithril-verify-certs",
		"verify Mithril certificate chains",
	),
	boolFlag(
		"Mithril.AllowInsecureHTTP",
		"mithril-allow-insecure-http",
		"allow plain-HTTP Mithril aggregator/artifact URLs (local dev/test only)",
	),

	// Database lifecycle (snapshot/restore/truncate)
	boolFlag(
		"DatabaseLifecycle.SnapshotEnabled",
		"db-snapshot-enabled",
		"capture automatic database snapshots at epoch boundaries",
	),
	stringFlag(
		"DatabaseLifecycle.SnapshotDir",
		"db-snapshot-dir",
		"",
		"local filesystem directory for automatic database snapshots",
	),
	stringFlag(
		"DatabaseLifecycle.SnapshotCloudDestination",
		"db-snapshot-cloud-destination",
		"",
		"optional cloud destination to additionally mirror every snapshot to (s3://bucket/prefix or gcs://bucket/prefix); requires the dingo_extra_plugins build tag",
	),
	stringFlag(
		"DatabaseLifecycle.SnapshotCloudDestinationPrefix",
		"db-snapshot-cloud-destination-prefix",
		"",
		"additional path segment appended to the cloud destination before each snapshot's ID; set to a distinct value per node when multiple nodes share one cloud destination",
	),
	intFlag(
		"DatabaseLifecycle.SnapshotRetention",
		"db-snapshot-retention",
		"number of automatic snapshots to retain (0 = keep all)",
	),
	intFlag(
		"DatabaseLifecycle.SnapshotEveryNEpochs",
		"db-snapshot-every-n-epochs",
		"capture an automatic snapshot every N epoch boundaries",
	),
}

// RegisterFlags registers persistent CLI flags for every Config field.
func RegisterFlags(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()
	flags.SortFlags = false
	// One snapshot for the whole loop. GetConfig deep-copies, so calling it
	// per flag would deep-clone the config once per registered flag.
	defaults := GetConfig()
	for _, spec := range flagSpecs {
		spec.register(flags, defaults)
	}
}

// ApplyFlags writes explicitly set flags back to cfg. Flags the user did
// not pass are ignored so YAML and env-var values survive.
func ApplyFlags(cmd *cobra.Command, cfg *Config) error {
	flags := cmd.Root().PersistentFlags()
	previousNetwork := cfg.Network
	for _, spec := range flagSpecs {
		if err := spec.apply(flags, cfg); err != nil {
			return err
		}
		// Only gated fields, per provenance's documented contract: this loop
		// walks every registered flag, and recording the rest would fill the
		// map with entries nothing reads.
		if flags.Changed(spec.name) && isGatedField(spec.field) {
			cfg.recordProvenance(spec.field, SourceFlag)
		}
	}
	if cfg.Network != previousNetwork {
		clearMidnightNetworkDefaults(cfg, previousNetwork)
	}
	applyMidnightNetworkDefaults(cfg)
	configMu.Lock()
	globalConfig = cloneConfig(cfg)
	configMu.Unlock()
	// Topology is not resolved here: Network and Topology are final at
	// this point, but the merged configuration has not been validated
	// yet, so cmd/dingo loads topology once after ApplyDefaults and
	// Validate.
	return nil
}

// fieldByPath walks a dotted path (e.g. "Cache.HotUtxoEntries") on v.
func fieldByPath(v reflect.Value, path string) reflect.Value {
	for p := range strings.SplitSeq(path, ".") {
		v = v.FieldByName(p)
	}
	return v
}

// defaultValue reads a field's default from the snapshot the caller took, so
// flag registration does not clone the config once per flag.
func defaultValue(defaults *Config, field string) reflect.Value {
	return fieldByPath(reflect.ValueOf(defaults).Elem(), field)
}

func targetValue(cfg *Config, field string) reflect.Value {
	return fieldByPath(reflect.ValueOf(cfg).Elem(), field)
}

func stringFlag(field, name, shorthand, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			def := defaultValue(defaults, field).String()
			if shorthand != "" {
				f.StringP(name, shorthand, def, help)
				return
			}
			f.String(name, def, help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetString(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetString(v)
			return nil
		},
	}
}

func stringSliceFlag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			def := defaultValue(defaults, field).Interface().([]string)
			f.StringSlice(name, def, help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetStringSlice(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).Set(reflect.ValueOf(v))
			return nil
		},
	}
}

// validatedStringFlag rejects invalid values but stores them verbatim.
func validatedStringFlag(
	field, name, shorthand, help string,
	validate func(string) error,
) flagSpec {
	s := stringFlag(field, name, shorthand, help)
	s.apply = func(f *pflag.FlagSet, cfg *Config) error {
		if !f.Changed(name) {
			return nil
		}
		v, err := f.GetString(name)
		if err != nil {
			return err
		}
		if err := validate(v); err != nil {
			return err
		}
		targetValue(cfg, field).SetString(v)
		return nil
	}
	return s
}

// transformStringFlag normalizes the parsed value (e.g. lowercasing)
// and may reject it; the transformed value is stored.
func transformStringFlag(
	field, name, help string,
	transform func(string) (string, error),
) flagSpec {
	s := stringFlag(field, name, "", help)
	s.apply = func(f *pflag.FlagSet, cfg *Config) error {
		if !f.Changed(name) {
			return nil
		}
		v, err := f.GetString(name)
		if err != nil {
			return err
		}
		out, err := transform(v)
		if err != nil {
			return err
		}
		targetValue(cfg, field).SetString(out)
		return nil
	}
	return s
}

func boolFlag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Bool(name, defaultValue(defaults, field).Bool(), help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetBool(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetBool(v)
			return nil
		},
	}
}

// boolPtrFlag binds a CLI flag to a *bool field. The pointer distinguishes
// "operator did not set this" (nil) from "explicitly false", which the flag
// alone cannot express. The default the user sees in --help is false; we
// only write to the field when the flag was explicitly passed.
func boolPtrFlag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Bool(name, false, help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetBool(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).Set(reflect.ValueOf(&v))
			return nil
		},
	}
}

// stringPtrFlag binds a CLI flag to a *string field. The pointer
// distinguishes "operator did not set this" (nil, inherit from a broader
// scope or fall back to a disabled default) from an explicit value --
// needed for the api.tls/api.auth policy fields (internal/apiconfig),
// where an explicit "disabled" is meaningfully different from never
// setting a mode at all. We only write to the field when the flag was
// explicitly passed, matching boolPtrFlag's own contract.
func stringPtrFlag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.String(name, "", help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetString(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).Set(reflect.ValueOf(&v))
			return nil
		},
	}
}

func intFlag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Int(name, int(defaultValue(defaults, field).Int()), help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetInt(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetInt(int64(v))
			return nil
		},
	}
}

func int64Flag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Int64(name, defaultValue(defaults, field).Int(), help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetInt64(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetInt(v)
			return nil
		},
	}
}

func uintFlag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Uint(name, uint(defaultValue(defaults, field).Uint()), help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetUint(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetUint(uint64(v))
			return nil
		},
	}
}

// uint32Flag exposes a flag as pflag.Uint but writes into a uint32 field,
// rejecting values above math.MaxUint32 to avoid silent truncation.
func uint32Flag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Uint(name, uint(defaultValue(defaults, field).Uint()), help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetUint(name)
			if err != nil {
				return err
			}
			if uint64(v) > math.MaxUint32 {
				return fmt.Errorf(
					"--%s value %d exceeds maximum of %d",
					name, v, uint64(math.MaxUint32),
				)
			}
			targetValue(cfg, field).SetUint(uint64(v))
			return nil
		},
	}
}

func uint64Flag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Uint64(name, defaultValue(defaults, field).Uint(), help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetUint64(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetUint(v)
			return nil
		},
	}
}

func float64Flag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Float64(name, defaultValue(defaults, field).Float(), help)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetFloat64(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetFloat(v)
			return nil
		},
	}
}

func durationFlag(field, name, help string) flagSpec {
	return flagSpec{
		field: field,
		name:  name,
		register: func(f *pflag.FlagSet, defaults *Config) {
			f.Duration(
				name,
				time.Duration(defaultValue(defaults, field).Int()),
				help,
			)
		},
		apply: func(f *pflag.FlagSet, cfg *Config) error {
			if !f.Changed(name) {
				return nil
			}
			v, err := f.GetDuration(name)
			if err != nil {
				return err
			}
			targetValue(cfg, field).SetInt(int64(v))
			return nil
		},
	}
}

func normalizeRunMode(v string) (string, error) {
	mode := RunMode(strings.ToLower(v))
	if !mode.Valid() {
		return "", fmt.Errorf(
			"invalid run mode %q: must be 'serve', 'load', 'dev', or 'leios'",
			v,
		)
	}
	return string(mode), nil
}

func normalizeStartEra(v string) (string, error) {
	era := StartEra(strings.ToLower(v))
	if !era.Valid() {
		return "", fmt.Errorf(
			"invalid start era %q: must be empty or 'dijkstra'",
			v,
		)
	}
	return string(era), nil
}

func normalizeStorageMode(v string) (string, error) {
	m := strings.ToLower(v)
	switch m {
	case storageModeCore, storageModeAPI:
		return m, nil
	default:
		return "", fmt.Errorf(
			"invalid storage mode %q: must be %q or %q",
			v, storageModeCore, storageModeAPI,
		)
	}
}

// normalizeLoggingValue lower-cases a logging format/level flag so values are
// accepted case-insensitively (e.g. --logging-format=JSON). It does not
// validate: unknown values are handled by the logger's warn-and-fallback path,
// keeping flag, env, and YAML behavior identical.
func normalizeLoggingValue(v string) (string, error) {
	return strings.ToLower(strings.TrimSpace(v)), nil
}
