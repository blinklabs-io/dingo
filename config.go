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

package dingo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ListenerConfig = connmanager.ListenerConfig

// StorageMode controls how much data the metadata store persists.
type StorageMode string

const (
	// StorageModeCore stores only consensus and chain state data.
	// Witnesses, scripts, datums, redeemers, and tx metadata CBOR
	// are skipped. Suitable for block producers with no APIs.
	StorageModeCore StorageMode = "core"
	// StorageModeAPI stores everything needed for API queries
	// (blockfrost, utxorpc, mesh) in addition to core data.
	StorageModeAPI StorageMode = "api"
)

// Valid returns true if the storage mode is a recognized value.
func (m StorageMode) Valid() bool {
	switch m {
	case StorageModeCore, StorageModeAPI:
		return true
	default:
		return false
	}
}

// IsAPI returns true if the storage mode includes API data.
func (m StorageMode) IsAPI() bool {
	return m == StorageModeAPI
}

// HistoryExpiryConfig controls local expiry of immutable block history.
type HistoryExpiryConfig struct {
	Enabled   bool
	Frequency time.Duration
}

// OffchainMetadataConfig controls API-mode off-chain metadata fetching.
// Zero values use the internal fetcher defaults.
type OffchainMetadataConfig struct {
	HTTPClient            *http.Client
	Interval              time.Duration
	RequestTimeout        time.Duration
	UserAgent             string
	IPFSGatewayURL        string
	BatchSize             int
	MaxBytes              int64
	AllowPrivateAddresses bool
}

// MidnightConfig controls the Midnight indexer and optional gRPC listener.
// Indexing is only active in API storage mode. Port 0 disables the gRPC
// listener while leaving indexing eligible to run.
type MidnightConfig struct {
	Port uint
	Host string

	CNightPolicyID              string
	CNightAssetName             string
	MappingValidatorAddress     string
	AuthTokenPolicyID           string
	AuthTokenAssetName          string
	CommitteeCandidateAddress   string
	TechnicalCommitteeAddress   string
	TechnicalCommitteePolicyID  string
	CouncilAddress              string
	CouncilPolicyID             string
	PermissionedCandidatePolicy string
}

type Config struct {
	// cfg holds the internal configuration that was parsed from YAML/env/CLI.
	// This is the single source of truth for all configuration fields.
	cfg *internalconfig.Config

	// Runtime-only fields that are not loaded from YAML/env:
	promRegistry      prometheus.Registerer
	topologyConfig    *topology.TopologyConfig
	logger            *slog.Logger
	cardanoNodeConfig *cardano.CardanoNodeConfig
	intersectPoints   []ocommon.Point
	listeners         []ListenerConfig

	// Programmatic API-only fields (not in YAML/env):
	tracing             bool
	tracingStdout       bool
	ledgerPeerTarget    int
	leiosPipelineTiming *leios.PipelineTiming
	// Runtime-only, programmatic offchain metadata config (preserves HTTPClient)
	offchainMetadata OffchainMetadataConfig
	// Parsed duration for chainsync stall timeout (runtime convenience)
	chainsyncStallTimeout time.Duration
}

// configPopulateNetworkMagic uses the named network (if specified) to determine the network magic value (if not specified)
func (n *Node) configPopulateNetworkMagic() error {
	if n.config.cfg.NetworkMagic == 0 && n.config.cfg.Network != "" {
		tmpNetwork, ok := ouroboros.NetworkByName(n.config.cfg.Network)
		if !ok {
			return fmt.Errorf("unknown network name: %s", n.config.cfg.Network)
		}
		n.config.cfg.NetworkMagic = tmpNetwork.NetworkMagic
	}
	return nil
}

// configWrapPromRegistry wraps the prometheus registry with a "network" label
// so that all metrics registered through it carry the network name automatically.
func (n *Node) configWrapPromRegistry() {
	if n.config.promRegistry == nil {
		return
	}
	// Determine the network name: prefer the configured name, fall back to
	// reverse-lookup by network magic.
	networkName := n.config.cfg.Network
	if networkName == "" {
		if net, ok := ouroboros.NetworkByNetworkMagic(n.config.cfg.NetworkMagic); ok {
			networkName = net.String()
		} else {
			networkName = strconv.FormatUint(uint64(n.config.cfg.NetworkMagic), 10)
		}
	}
	if networkName == "" {
		return
	}
	n.config.promRegistry = prometheus.WrapRegistererWith(
		prometheus.Labels{"network": networkName},
		n.config.promRegistry,
	)
}

// registerBuildInfo registers a dingo_build_info gauge with version and
// commit labels. The gauge is always set to 1; Grafana reads the labels.
func (n *Node) registerBuildInfo() {
	if n.config.promRegistry == nil {
		return
	}
	promauto.With(n.config.promRegistry).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dingo_build_info",
			Help: "dingo build information",
		},
		[]string{"version", "commit", "goversion"},
	).WithLabelValues(
		version.GetVersionString(),
		version.CommitHash,
		runtime.Version(),
	).Set(1)
}

// rtsMetricsUpdateInterval is how often the background updater refreshes
// the RTS-style gauges from runtime.MemStats. Chosen to stay comfortably
// under the default Prometheus scrape interval (15s) so consecutive
// scrapes always see a fresh sample, without paying ReadMemStats
// stop-the-world cost more often than necessary.
const rtsMetricsUpdateInterval = 10 * time.Second

// rtsMetrics holds Haskell-cardano-node-style RTS memory gauges backed
// by Go runtime.MemStats. Matching the Haskell naming lets existing
// cardano-node dashboards and alert rules work against Dingo without
// rewriting queries. The gauges are approximate mappings from Go heap
// statistics to Haskell RTS GC concepts.
type rtsMetrics struct {
	gcLiveBytes prometheus.Gauge
	gcHeapBytes prometheus.Gauge
	gcMajorNum  prometheus.Gauge
	gcMinorNum  prometheus.Gauge
}

// registerRTSMetrics creates the four RTS gauges on the node's Prometheus
// registry. Safe to call when promRegistry is nil — in that case it
// returns early and leaves n.rtsMetrics nil, matching the registerBuildInfo
// nil-guard pattern.
func (n *Node) registerRTSMetrics() {
	if n.config.promRegistry == nil {
		return
	}
	factory := promauto.With(n.config.promRegistry)
	n.rtsMetrics = &rtsMetrics{
		gcLiveBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_RTS_gcLiveBytes_int",
			Help: "live heap bytes currently in use (Go runtime.MemStats.HeapAlloc)",
		}),
		gcHeapBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_RTS_gcHeapBytes_int",
			Help: "heap memory bytes obtained from the OS (Go runtime.MemStats.HeapSys)",
		}),
		gcMajorNum: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_RTS_gcMajorNum_int",
			Help: "count of forced GCs (Go runtime.MemStats.NumForcedGC)",
		}),
		gcMinorNum: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cardano_node_metrics_RTS_gcMinorNum_int",
			Help: "count of automatic GCs (Go runtime.MemStats.NumGC - NumForcedGC)",
		}),
	}
}

// updateRTSMetrics writes the four gauge values from a runtime.MemStats
// snapshot. Kept as a pure function (no ReadMemStats call, no timer) so
// unit tests can drive it with crafted MemStats values. The Go runtime
// guarantees NumForcedGC <= NumGC, so the subtraction never underflows.
func updateRTSMetrics(m *rtsMetrics, stats *runtime.MemStats) {
	m.gcLiveBytes.Set(float64(stats.HeapAlloc))
	m.gcHeapBytes.Set(float64(stats.HeapSys))
	m.gcMajorNum.Set(float64(stats.NumForcedGC))
	m.gcMinorNum.Set(float64(stats.NumGC - stats.NumForcedGC))
}

// runRTSMetricsUpdater samples runtime.MemStats on a ticker and writes
// the values into the RTS gauges. Collection happens on this goroutine
// rather than at Prometheus scrape time so a scrape never pays the
// ReadMemStats stop-the-world cost. Exits when ctx is cancelled.
//
// The interval parameter is accepted explicitly so tests can drive the
// updater with a much shorter tick without changing production cadence;
// production callers pass rtsMetricsUpdateInterval.
func (n *Node) runRTSMetricsUpdater(
	ctx context.Context,
	interval time.Duration,
) {
	if n.rtsMetrics == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Prime the gauges immediately so a scrape arriving before the
	// first tick returns real values instead of zero.
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	updateRTSMetrics(n.rtsMetrics, &stats)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runtime.ReadMemStats(&stats)
			updateRTSMetrics(n.rtsMetrics, &stats)
		}
	}
}

// isDevMode returns true if running in development mode
func (c *Config) isDevMode() bool {
	return c.cfg.RunMode == internalconfig.RunModeDev
}

// isMusashiNetwork reports whether the configured network is the experimental
// Musashi testnet (the IOG Leios prototype network), identified either by its
// network name or its network magic. The Musashi testnet hard-forks into the
// Dijkstra era, so a node syncing it must enable the Dijkstra era table
// regardless of the configured run mode. This lets `dingo -n musashi` follow
// the chain without also requiring `--run-mode leios`.
func (c *Config) isMusashiNetwork() bool {
	return c.cfg.Network == ouroboros.NetworkCardanoMusashi.Name ||
		c.cfg.NetworkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic
}

// experimentalLeiosNetworkingEnabled reports whether the Leios node-to-node
// mini-protocols (leios-fetch / leios-notify) should be offered on outbound
// and inbound connections.
//
// This is enabled on the Musashi testnet (so `dingo -n musashi` fetches
// endorser blocks and follows the Leios overlay) as well as via explicit
// opt-in (leios run mode or a Dijkstra start era). Earlier prototype relays
// reset any connection that opened the standalone leios-votes mini-protocol
// (protocol 20), which the prototype does not run; that protocol is now gated
// off for the prototype network (see EnableLeiosVotes wiring in node.go), and
// the leios-notify / leios-fetch codecs decode the prototype's wire dialect,
// so the Leios protocols can stay active on the Musashi testnet.
func (c *Config) experimentalLeiosNetworkingEnabled() bool {
	return c.isMusashiNetwork() ||
		c.cfg.RunMode == internalconfig.RunModeLeios ||
		c.cfg.StartEra.IsDijkstra()
}

// experimentalDijkstraEnabled reports whether the Dijkstra ledger era is
// active. Beyond the explicit opt-ins, the Musashi testnet hard-forks into
// Dijkstra, so syncing it requires the Dijkstra era table even over base
// protocols.
func (c *Config) experimentalDijkstraEnabled() bool {
	return c.experimentalLeiosNetworkingEnabled() || c.isMusashiNetwork()
}

func (n *Node) configValidate() error {
	// Default storageMode to "core" when unset, and validate.
	storageMode := StorageMode(n.config.cfg.StorageMode)
	if storageMode == "" {
		storageMode = StorageModeCore
		n.config.cfg.StorageMode = string(storageMode)
	}
	if !storageMode.Valid() {
		return fmt.Errorf(
			"invalid storage mode %q: must be %q or %q",
			storageMode,
			StorageModeCore,
			StorageModeAPI,
		)
	}
	if !n.config.cfg.StartEra.Valid() {
		return fmt.Errorf(
			"invalid start era %q: must be empty or %q",
			n.config.cfg.StartEra,
			internalconfig.StartEraDijkstra,
		)
	}
	// In core mode, ignore API ports — they are only used in API mode.
	// This lets defaults stay non-zero without requiring core-mode users
	// to explicitly disable each one.
	if n.config.cfg.NetworkMagic == 0 {
		return fmt.Errorf(
			"invalid network magic value: %d",
			n.config.cfg.NetworkMagic,
		)
	}
	if len(n.config.listeners) == 0 {
		return errors.New("no listeners defined")
	}
	for _, listener := range n.config.listeners {
		if listener.Listener != nil {
			continue
		}
		if listener.ListenNetwork != "" && listener.ListenAddress != "" {
			continue
		}
		return errors.New(
			"listener must provide net.Listener or listen network/address values",
		)
	}
	if n.config.CardanoNodeConfig() != nil {
		shelleyGenesis := n.config.CardanoNodeConfig().ShelleyGenesis()
		if shelleyGenesis == nil {
			return errors.New("unable to get Shelley genesis information")
		}
		if n.config.cfg.NetworkMagic != shelleyGenesis.NetworkMagic {
			return fmt.Errorf(
				"network magic (%d) doesn't match value from Shelley genesis (%d)",
				n.config.cfg.NetworkMagic,
				shelleyGenesis.NetworkMagic,
			)
		}
	}
	return nil
}

// ConfigOptionFunc is a type that represents functions that modify the Connection config
type ConfigOptionFunc func(*Config)

// NewConfig creates a new dingo config with the specified options.
// This is primarily for programmatic use (library API).
func NewConfig(opts ...ConfigOptionFunc) Config {
	// Start with a default internal config
	c := Config{
		cfg: &internalconfig.Config{
			EvictionWatermark:  internalconfig.DefaultEvictionWatermark,
			RejectionWatermark: internalconfig.DefaultRejectionWatermark,
			BindAddr:           "0.0.0.0",
			StorageMode:        string(StorageModeCore),
			RunMode:            internalconfig.RunModeServe,
			Cache:              internalconfig.DefaultCacheConfig(),
			Chainsync:          internalconfig.DefaultChainsyncConfig(),
			GenesisBootstrap:   internalconfig.DefaultGenesisBootstrapConfig(),
			HistoryExpiry:      internalconfig.DefaultHistoryExpiryConfig(),
			Logging:            internalconfig.DefaultLoggingConfig(),
			Midnight:           internalconfig.DefaultMidnightConfig(),
			CORSAllowedOrigins: []string{"*"},
		},
		// Default logger will throw away logs
		// We do this so we don't have to add guards around every log operation
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	// Apply options
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// NewConfigFromInternal creates a Config from an already-loaded internal
// configuration, along with runtime-only dependencies (logger, cardano
// config, topology). This is the main path for the dingo CLI and node
// startup, eliminating manual field-by-field mapping.
func NewConfigFromInternal(
	cfg *internalconfig.Config,
	logger *slog.Logger,
	cardanoCfg *cardano.CardanoNodeConfig,
	topoCfg *topology.TopologyConfig,
	promRegistry prometheus.Registerer,
) Config {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// If caller did not provide an internal config, seed defaults so accessors
	// and consumers never hit a nil pointer. This mirrors NewConfig's default
	// internal configuration.
	if cfg == nil {
		tmp := NewConfig()
		cfg = tmp.cfg
	}
	// Parse chainsync stall timeout for runtime use. Fall back to 2m on error.
	var chainsyncDur time.Duration
	if cfg.Chainsync.StallTimeout != "" {
		if d, err := time.ParseDuration(cfg.Chainsync.StallTimeout); err == nil {
			chainsyncDur = d
		} else {
			chainsyncDur = 2 * time.Minute
		}
	} else {
		chainsyncDur = 2 * time.Minute
	}
	return Config{
		cfg:                   cfg,
		logger:                logger,
		cardanoNodeConfig:     cardanoCfg,
		topologyConfig:        topoCfg,
		promRegistry:          promRegistry,
		chainsyncStallTimeout: chainsyncDur,
	}
}

// WithCardanoNodeConfig specifies the CardanoNodeConfig object to use. This is mostly used for loading genesis config files
// referenced by the dingo config
func WithCardanoNodeConfig(
	cardanoNodeConfig *cardano.CardanoNodeConfig,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cardanoNodeConfig = cardanoNodeConfig
	}
}

// WithBindAddr specifies the IP address used for API listeners
// (Blockfrost, Mesh, UTxO RPC). The default is "0.0.0.0" (all interfaces).
func WithBindAddr(addr string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.BindAddr = addr
	}
}

// WithDatabasePath specifies the persistent data directory to use. The default is to store everything in memory
func WithDatabasePath(dataDir string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.DatabasePath = dataDir
	}
}

// WithBlobPlugin specifies the blob storage plugin to use.
func WithBlobPlugin(plugin string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.BlobPlugin = plugin
	}
}

// WithMetadataPlugin specifies the metadata storage plugin to use.
func WithMetadataPlugin(plugin string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.MetadataPlugin = plugin
	}
}

// WithIntersectPoints specifies intersect point(s) for the initial chainsync. The default is to start at chain genesis
func WithIntersectPoints(points []ocommon.Point) ConfigOptionFunc {
	return func(c *Config) {
		c.intersectPoints = points
	}
}

// WithIntersectTip specifies whether to start the initial chainsync at the current tip. The default is to start at chain genesis
func WithIntersectTip(intersectTip bool) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.IntersectTip = intersectTip
	}
}

// WithLogger specifies the logger to use. This defaults to discarding log output
func WithLogger(logger *slog.Logger) ConfigOptionFunc {
	return func(c *Config) {
		c.logger = logger
	}
}

// WithListeners specifies the listener config(s) to use
func WithListeners(listeners ...ListenerConfig) ConfigOptionFunc {
	return func(c *Config) {
		c.listeners = append(c.listeners, listeners...)
	}
}

// WithNetwork specifies the named network to operate on. This will automatically set the appropriate network magic value
func WithNetwork(network string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.Network = network
	}
}

// WithNetworkMagic specifies the network magic value to use. This will override any named network specified
func WithNetworkMagic(networkMagic uint32) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.NetworkMagic = networkMagic
	}
}

// WithOutboundSourcePort specifies the source port to use for outbound connections. This defaults to dynamic source ports
func WithOutboundSourcePort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.RelayPort = port
	}
}

// WithUtxorpcTlsCertFilePath specifies the path to the TLS certificate for the gRPC API listener. This defaults to empty
func WithUtxorpcTlsCertFilePath(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.TlsCertFilePath = path
	}
}

// WithUtxorpcTlsKeyFilePath specifies the path to the TLS key for the gRPC API listener. This defaults to empty
func WithUtxorpcTlsKeyFilePath(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.TlsKeyFilePath = path
	}
}

// WithUtxorpcPort specifies the port to use for the gRPC API listener. 0 disables the server (default)
func WithUtxorpcPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.UtxorpcPort = port
	}
}

// WithPeerSharing specifies whether to enable peer sharing. This is disabled by default
func WithPeerSharing(peerSharing bool) ConfigOptionFunc {
	return func(c *Config) {
		ps := peerSharing
		c.cfg.PeerSharing = &ps
	}
}

// WithPrometheusRegistry specifies a prometheus.Registerer instance to add metrics to. In most cases, prometheus.DefaultRegistry would be
// a good choice to get metrics working
func WithPrometheusRegistry(registry prometheus.Registerer) ConfigOptionFunc {
	return func(c *Config) {
		c.promRegistry = registry
	}
}

// WithTopologyConfig specifies a topology.TopologyConfig to use for outbound peers
func WithTopologyConfig(
	topologyConfig *topology.TopologyConfig,
) ConfigOptionFunc {
	return func(c *Config) {
		c.topologyConfig = topologyConfig
	}
}

// WithTracing enables tracing. By default, spans are submitted to a HTTP(s) endpoint using OTLP. This can be configured
// using the OTEL_EXPORTER_OTLP_* env vars documented in the README for [go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp]
func WithTracing(tracing bool) ConfigOptionFunc {
	return func(c *Config) {
		c.tracing = tracing
	}
}

// WithTracingStdout enables tracing output to stdout. This also requires tracing to enabled separately. This is mostly useful for debugging
func WithTracingStdout(stdout bool) ConfigOptionFunc {
	return func(c *Config) {
		c.tracingStdout = stdout
	}
}

// WithShutdownTimeout specifies the timeout for graceful shutdown. The default is 30 seconds
func WithShutdownTimeout(timeout time.Duration) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ShutdownTimeout = timeout.String()
	}
}

// WithMempoolCapacity sets the mempool capacity (in bytes)
func WithMempoolCapacity(capacity int64) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.MempoolCapacity = capacity
	}
}

// WithEvictionWatermark sets the mempool eviction watermark
// as a fraction of capacity (0.0-1.0). When a new TX would
// push the mempool past this fraction, oldest TXs are evicted
// to make room. Default is 0.90 (90%).
func WithEvictionWatermark(
	watermark float64,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.EvictionWatermark = watermark
	}
}

// WithRejectionWatermark sets the mempool rejection watermark
// as a fraction of capacity (0.0-1.0). New TXs are rejected
// when the mempool would exceed this fraction even after
// eviction. Default is 0.95 (95%).
func WithRejectionWatermark(
	watermark float64,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.RejectionWatermark = watermark
	}
}

// WithRunMode sets the operational mode ("serve", "load", or "dev").
// "dev" mode enables development behaviors (forge blocks, disable outbound).
func WithRunMode(mode string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.RunMode = internalconfig.RunMode(mode)
	}
}

// WithStartEra sets the experimental direct startup era. Empty uses the
// genesis protocol version; "dijkstra" starts directly in the Dijkstra era.
func WithStartEra(startEra string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.StartEra = internalconfig.StartEra(startEra)
	}
}

// WithValidateHistorical specifies whether to validate all historical blocks during ledger processing
func WithValidateHistorical(validate bool) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ValidateHistorical = validate
	}
}

// WithStrictUtxoValidation specifies whether an unrecoverable consumed UTxO
// past the recorded Mithril sync boundary is a hard error rather than a
// silently skipped condition. See database.Config.StrictUtxoValidation.
func WithStrictUtxoValidation(strict bool) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.StrictUtxoValidation = strict
	}
}

// WithDatabaseWorkerPoolConfig specifies the database worker pool configuration
func WithDatabaseWorkerPoolConfig(
	cfg ledger.DatabaseWorkerPoolConfig,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.DatabaseWorkers = cfg.WorkerPoolSize
		c.cfg.DatabaseQueueSize = cfg.TaskQueueSize
	}
}

// WithPeerTargets specifies the target number of peers in each state.
// Use 0 to use the default target, or -1 for unlimited.
// Default targets: known=150, established=50, active=20
func WithPeerTargets(
	targetKnown, targetEstablished, targetActive int,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.TargetNumberOfKnownPeers = targetKnown
		c.cfg.TargetNumberOfEstablishedPeers = targetEstablished
		c.cfg.TargetNumberOfActivePeers = targetActive
	}
}

// WithActivePeersQuotas specifies the per-source quotas for active peers.
// Use 0 to use the default quota, or a negative value to disable enforcement.
// Default quotas: topology=20, gossip=20, ledger=20
func WithActivePeersQuotas(
	topologyQuota, gossipQuota, ledgerQuota int,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ActivePeersTopologyQuota = topologyQuota
		c.cfg.ActivePeersGossipQuota = gossipQuota
		c.cfg.ActivePeersLedgerQuota = ledgerQuota
	}
}

// WithLedgerPeerTarget specifies the target number of known ledger peers.
// Discovery will add peers only until this target is reached.
// Negative values disable ledger peer discovery, 0 uses
// defaultLedgerPeerTarget, and positive values use that target. Default: 20.
func WithLedgerPeerTarget(n int) ConfigOptionFunc {
	return func(c *Config) {
		c.ledgerPeerTarget = n
	}
}

// WithMinHotPeers specifies the minimum number of hot peers before aggressive
// promotion is triggered. Non-positive values are ignored. Default: 10.
func WithMinHotPeers(n int) ConfigOptionFunc {
	return func(c *Config) {
		if n > 0 {
			c.cfg.MinHotPeers = n
		}
	}
}

// WithBootstrapPromotionMinDiversityGroups sets the minimum number of
// bootstrap-time peer diversity groups to prefer before falling back to pure
// score ordering. Non-positive values use the peer-governor default.
func WithBootstrapPromotionMinDiversityGroups(n int) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.GenesisBootstrap.PromotionMinDiversityGroups = n
	}
}

// WithReconcileInterval specifies how often the peer governor runs its
// reconciliation loop. Non-positive values are ignored. Default: 5m.
func WithReconcileInterval(d time.Duration) ConfigOptionFunc {
	return func(c *Config) {
		if d > 0 {
			c.cfg.ReconcileInterval = d
		}
	}
}

// WithInactivityTimeout specifies how long a hot peer can be inactive
// before being demoted to warm. Non-positive values are ignored. Default: 10m.
func WithInactivityTimeout(d time.Duration) ConfigOptionFunc {
	return func(c *Config) {
		if d > 0 {
			c.cfg.InactivityTimeout = d
		}
	}
}

// WithInboundPeerGovernance specifies explicit inbound peer governance budget
// and phase-1 policy fields. Non-positive values use peer governor defaults.
func WithInboundPeerGovernance(
	warmTarget int,
	hotQuota int,
	minTenure time.Duration,
	hotScoreThreshold float64,
	pruneAfter time.Duration,
	duplexOnlyForHot bool,
	cooldown time.Duration,
) ConfigOptionFunc {
	return func(c *Config) {
		if warmTarget > 0 {
			c.cfg.InboundWarmTarget = warmTarget
		}
		if hotQuota > 0 {
			c.cfg.InboundHotQuota = hotQuota
		}
		if minTenure > 0 {
			c.cfg.InboundMinTenure = minTenure
		}
		if hotScoreThreshold > 0 {
			c.cfg.InboundHotScoreThreshold = hotScoreThreshold
		}
		if pruneAfter > 0 {
			c.cfg.InboundPruneAfter = pruneAfter
		}
		c.cfg.InboundDuplexOnlyForHot = duplexOnlyForHot
		if cooldown > 0 {
			c.cfg.InboundCooldown = cooldown
		}
	}
}

// WithMaxConnectionsPerIP specifies the maximum number of concurrent
// inbound connections from a single IP. Non-positive values are ignored. Default: 5.
func WithMaxConnectionsPerIP(n int) ConfigOptionFunc {
	return func(c *Config) {
		if n > 0 {
			c.cfg.MaxConnectionsPerIP = n
		}
	}
}

// WithMaxInboundConns specifies the maximum number of inbound connections.
// Non-positive values are ignored. Default: 100.
func WithMaxInboundConns(n int) ConfigOptionFunc {
	return func(c *Config) {
		if n > 0 {
			c.cfg.MaxInboundConns = n
		}
	}
}

// WithGenesisBootstrap enables Genesis-mode chain selection during from-origin
// bootstrap. Genesis mode automatically exits once the local tip is within the
// configured Genesis window of the best known peer tip.
func WithGenesisBootstrap(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.GenesisBootstrap.Enabled = enabled
	}
}

// WithGenesisWindowSlots overrides the Genesis density comparison window.
// A zero value lets the node derive the window from Shelley genesis parameters
// using 3k/f.
func WithGenesisWindowSlots(slots uint64) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.GenesisBootstrap.WindowSlots = slots
	}
}

// WithBlockProducer enables block production mode (CARDANO_BLOCK_PRODUCER).
// When enabled, the node will attempt to produce blocks using the configured credentials.
func WithBlockProducer(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.BlockProducer = enabled
	}
}

// WithShelleyVRFKey specifies the path to the VRF signing key file (CARDANO_SHELLEY_VRF_KEY).
// Required for block production.
func WithShelleyVRFKey(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ShelleyVRFKey = path
	}
}

// WithShelleyKESKey specifies the path to the KES signing key file (CARDANO_SHELLEY_KES_KEY).
// Required for block production.
func WithShelleyKESKey(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ShelleyKESKey = path
	}
}

// WithShelleyOperationalCertificate specifies the path to the operational certificate file
// (CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE). Required for block production.
func WithShelleyOperationalCertificate(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ShelleyOperationalCertificate = path
	}
}

// WithLeiosVoteSigningKeyFile specifies the path to a hex-encoded BLS12-381
// Leios vote signing key (DINGO_LEIOS_VOTE_SIGNING_KEY_FILE). When set on a
// block producer whose pool is a Leios committee member, the node emits
// votes for endorser blocks. Experimental, leios runMode only.
func WithLeiosVoteSigningKeyFile(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.LeiosVoteSigningKeyFile = path
	}
}

// WithLeiosVoterPublicKeys specifies the static Leios voter public key
// registry (DINGO_LEIOS_VOTER_PUBLIC_KEYS): hex pool key hash to
// hex-encoded BLS12-381 public key. Stands in for CIP-0164 key
// registration, which is not yet specified. Experimental, leios runMode
// only.
func WithLeiosVoterPublicKeys(keys map[string]string) ConfigOptionFunc {
	return func(c *Config) {
		// Copy so later caller mutations cannot change live config
		c.cfg.LeiosVoterPublicKeys = maps.Clone(keys)
	}
}

// WithLeiosPipelineTiming overrides the provisional Leios pipeline stage
// timing windows. CIP-0164 has not finalized these parameters, so they are
// kept off-chain and overridable here rather than as protocol parameters.
// When unset, leios.DefaultPipelineTiming applies. Experimental, leios
// runMode only.
func WithLeiosPipelineTiming(timing leios.PipelineTiming) ConfigOptionFunc {
	return func(c *Config) {
		t := timing
		c.leiosPipelineTiming = &t
	}
}

// WithForgeSyncToleranceSlots sets the slot gap tolerated before forging is skipped.
// Use 0 to fall back to the built-in default.
func WithForgeSyncToleranceSlots(slots uint64) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ForgeSyncToleranceSlots = slots
	}
}

// WithForgeStaleGapThresholdSlots sets the slot gap threshold for stale database warnings.
// Use 0 to fall back to the built-in default.
func WithForgeStaleGapThresholdSlots(slots uint64) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ForgeStaleGapThresholdSlots = slots
	}
}

// WithValidateForgedBlock enables self-validation of locally-forged blocks
// before they are adopted onto the chain and diffused to peers. When enabled,
// the forger runs VRF/KES header crypto, body-hash consistency, and per-tx
// ledger validation on each forged block. A failing block is dropped without
// being adopted or diffused. Disabled by default.
func WithValidateForgedBlock(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.ValidateForgedBlock = enabled
	}
}

// WithBlockfrostPort specifies the port for the
// Blockfrost-compatible REST API server. The server binds
// to the node's bindAddr on this port. 0 disables the
// server (default).
func WithBlockfrostPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.BlockfrostPort = port
	}
}

func WithBarkBaseUrl(baseUrl string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.BarkBaseUrl = baseUrl
	}
}

func WithBarkBlockDownloadHosts(hosts []string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.BarkBlockDownloadHosts = slices.Clone(hosts)
	}
}

func WithBarkPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.BarkPort = port
	}
}

// WithHistoryExpiry configures local immutable block history expiry.
func WithHistoryExpiry(cfg HistoryExpiryConfig) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.HistoryExpiry = internalconfig.HistoryExpiryConfig{
			Enabled:   cfg.Enabled,
			Frequency: cfg.Frequency,
		}
	}
}

// WithCORSAllowedOrigins configures browser CORS access for public API
// servers. Use []string{"*"} to allow any origin, or an empty list to
// disable CORS headers.
func WithCORSAllowedOrigins(origins []string) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.CORSAllowedOrigins = slices.Clone(origins)
	}
}

// WithOffchainMetadataConfig configures the API-mode off-chain metadata
// fetcher. Zero values use the fetcher's internal defaults.
func WithOffchainMetadataConfig(cfg OffchainMetadataConfig) ConfigOptionFunc {
	return func(c *Config) {
		// Preserve the programmatic runtime-only HTTPClient and full cfg
		c.offchainMetadata = cfg
		c.cfg.OffchainMetadata = internalconfig.OffchainMetadataConfig{
			Interval:              cfg.Interval,
			RequestTimeout:        cfg.RequestTimeout,
			UserAgent:             cfg.UserAgent,
			IPFSGatewayURL:        cfg.IPFSGatewayURL,
			BatchSize:             cfg.BatchSize,
			MaxBytes:              cfg.MaxBytes,
			AllowPrivateAddresses: cfg.AllowPrivateAddresses,
		}
	}
}

// WithMidnightConfig configures the Midnight indexer and optional gRPC API.
func WithMidnightConfig(cfg MidnightConfig) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.Midnight = internalconfig.MidnightConfig{
			Port:                        cfg.Port,
			Host:                        cfg.Host,
			CNightPolicyID:              cfg.CNightPolicyID,
			CNightAssetName:             cfg.CNightAssetName,
			MappingValidatorAddress:     cfg.MappingValidatorAddress,
			AuthTokenPolicyID:           cfg.AuthTokenPolicyID,
			AuthTokenAssetName:          cfg.AuthTokenAssetName,
			CommitteeCandidateAddress:   cfg.CommitteeCandidateAddress,
			TechnicalCommitteeAddress:   cfg.TechnicalCommitteeAddress,
			TechnicalCommitteePolicyID:  cfg.TechnicalCommitteePolicyID,
			CouncilAddress:              cfg.CouncilAddress,
			CouncilPolicyID:             cfg.CouncilPolicyID,
			PermissionedCandidatePolicy: cfg.PermissionedCandidatePolicy,
		}
	}
}

// WithChainsyncMaxClients specifies the maximum number of
// concurrent chainsync client connections. Default is 3.
func WithChainsyncMaxClients(
	maxClients int,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.Chainsync.MaxClients = maxClients
	}
}

// WithChainsyncStallTimeout specifies the duration after
// which a chainsync client with no activity is considered
// stalled. Default is 2 minutes.
func WithChainsyncStallTimeout(
	timeout time.Duration,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.Chainsync.StallTimeout = timeout.String()
		c.chainsyncStallTimeout = timeout
	}
}

// WithChainsyncHeaderStrategy selects how headers from multiple
// eligible chainsync peers drive ledger ingress. The default is
// chainsync.HeaderSyncStrategyPrimary (single active peer with
// failover).
func WithChainsyncHeaderStrategy(
	strategy chainsync.HeaderSyncStrategy,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.Chainsync.Strategy = strategy.String()
	}
}

// WithMeshPort specifies the port for the Mesh (Coinbase
// Rosetta) compatible REST API server. The server binds
// to the node's bindAddr on this port. 0 disables the
// server (default).
func WithMeshPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.MeshPort = port
	}
}

// WithStorageMode specifies the storage mode. StorageModeCore
// stores only consensus data; StorageModeAPI adds full
// transaction metadata for API queries.
func WithStorageMode(mode StorageMode) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.StorageMode = string(mode)
	}
}

// WithCacheConfig sets the CBOR cache sizes for block LRU,
// hot UTxO, and hot TX caches.
func WithCacheConfig(blockLRU, hotUtxo, hotTx int, hotTxMaxBytes int64) ConfigOptionFunc {
	return func(c *Config) {
		c.cfg.Cache.BlockLRUEntries = blockLRU
		c.cfg.Cache.HotUtxoEntries = hotUtxo
		c.cfg.Cache.HotTxEntries = hotTx
		c.cfg.Cache.HotTxMaxBytes = hotTxMaxBytes
	}
}

// Accessor methods for reading config values.
// These provide the single public interface for accessing configuration fields,
// eliminating direct access to the internal config struct.

// Network returns the configured network name.
func (c *Config) Network() string {
	return c.cfg.Network
}

// NetworkMagic returns the configured network magic value.
func (c *Config) NetworkMagic() uint32 {
	return c.cfg.NetworkMagic
}

// DatabasePath returns the persistent data directory path.
func (c *Config) DatabasePath() string {
	return c.cfg.DatabasePath
}

// SocketPath returns the path to the UNIX domain socket for local client API.
func (c *Config) SocketPath() string {
	return c.cfg.SocketPath
}

// CardanoConfig returns the path to the cardano-node config JSON file.
func (c *Config) CardanoConfig() string {
	return c.cfg.CardanoConfig
}

// Topology returns the topology file path.
func (c *Config) Topology() string {
	return c.cfg.Topology
}

// BlobPlugin returns the configured blob storage plugin name.
func (c *Config) BlobPlugin() string {
	return c.cfg.BlobPlugin
}

// MetadataPlugin returns the configured metadata storage plugin name.
func (c *Config) MetadataPlugin() string {
	return c.cfg.MetadataPlugin
}

// BindAddr returns the IP address for API listeners.
func (c *Config) BindAddr() string {
	return c.cfg.BindAddr
}

// PrivateBindAddr returns the IP address for the private NtC listener.
func (c *Config) PrivateBindAddr() string {
	return c.cfg.PrivateBindAddr
}

// RelayPort returns the source port for outbound connections.
func (c *Config) RelayPort() uint {
	return c.cfg.RelayPort
}

// PrivatePort returns the port for the private NtC listener.
func (c *Config) PrivatePort() uint {
	return c.cfg.PrivatePort
}

// MetricsPort returns the Prometheus metrics endpoint port.
func (c *Config) MetricsPort() uint {
	return c.cfg.MetricsPort
}

// DebugPort returns the pprof debug endpoint port. 0 disables the endpoint.
func (c *Config) DebugPort() uint {
	return c.cfg.DebugPort
}

// BlockfrostPort returns the Blockfrost API port. 0 disables the server.
func (c *Config) BlockfrostPort() uint {
	return c.cfg.BlockfrostPort
}

// UtxorpcPort returns the UTxO RPC gRPC API port. 0 disables the server.
func (c *Config) UtxorpcPort() uint {
	return c.cfg.UtxorpcPort
}

// MeshPort returns the Mesh (Rosetta) API port. 0 disables the server.
func (c *Config) MeshPort() uint {
	return c.cfg.MeshPort
}

// BarkPort returns the Bark API port. 0 disables the server.
func (c *Config) BarkPort() uint {
	return c.cfg.BarkPort
}

// BarkBaseUrl returns the base URL for the Bark service.
func (c *Config) BarkBaseUrl() string {
	return c.cfg.BarkBaseUrl
}

// BarkBlockDownloadHosts returns the list of allowed hosts for block downloads via Bark.
func (c *Config) BarkBlockDownloadHosts() []string {
	return c.cfg.BarkBlockDownloadHosts
}

// TlsCertFilePath returns the path to the TLS certificate for gRPC APIs.
func (c *Config) TlsCertFilePath() string {
	return c.cfg.TlsCertFilePath
}

// TlsKeyFilePath returns the path to the TLS key for gRPC APIs.
func (c *Config) TlsKeyFilePath() string {
	return c.cfg.TlsKeyFilePath
}

// IntersectTip returns whether to start chainsync at the current chain tip.
func (c *Config) IntersectTip() bool {
	return c.cfg.IntersectTip
}

// ValidateHistorical returns whether to validate all historical blocks.
func (c *Config) ValidateHistorical() bool {
	return c.cfg.ValidateHistorical
}

// StrictUtxoValidation returns whether to error out when a consumed UTxO cannot be found.
func (c *Config) StrictUtxoValidation() bool {
	return c.cfg.StrictUtxoValidation
}

// ShutdownTimeout returns the graceful shutdown timeout as a string duration.
func (c *Config) ShutdownTimeout() string {
	if c.cfg == nil {
		return ""
	}
	return c.cfg.ShutdownTimeout
}

// ShutdownTimeoutDuration parses the configured graceful shutdown timeout.
func (c *Config) ShutdownTimeoutDuration() (time.Duration, error) {
	if c.cfg == nil {
		return 0, nil
	}
	if c.cfg.ShutdownTimeout == "" {
		return 0, nil
	}
	return time.ParseDuration(c.cfg.ShutdownTimeout)
}

// LedgerCatchupTimeout returns the maximum wait time for ledger catchup.
func (c *Config) LedgerCatchupTimeout() string {
	return c.cfg.LedgerCatchupTimeout
}

// MempoolCapacity returns the mempool capacity in bytes.
func (c *Config) MempoolCapacity() int64 {
	return c.cfg.MempoolCapacity
}

// EvictionWatermark returns the mempool eviction watermark (0.0-1.0).
func (c *Config) EvictionWatermark() float64 {
	return c.cfg.EvictionWatermark
}

// RejectionWatermark returns the mempool rejection watermark (0.0-1.0).
func (c *Config) RejectionWatermark() float64 {
	return c.cfg.RejectionWatermark
}

// RunMode returns the operational mode (serve, load, dev, or leios).
func (c *Config) RunMode() internalconfig.RunMode {
	return c.cfg.RunMode
}

// StartEra returns the experimental direct startup era.
func (c *Config) StartEra() internalconfig.StartEra {
	return c.cfg.StartEra
}

// ImmutableDbPath returns the path to an external ImmutableDB for load mode.
func (c *Config) ImmutableDbPath() string {
	return c.cfg.ImmutableDbPath
}

// StorageMode returns the storage mode (core or api).
func (c *Config) StorageMode() string {
	return c.cfg.StorageMode
}

// StorageModeEnum returns the storage mode as the typed StorageMode.
func (c *Config) StorageModeEnum() StorageMode {
	return StorageMode(c.cfg.StorageMode)
}

// SetStorageMode updates the storage mode. This is used during validation
// when a mode-specific default must be applied.
func (c *Config) SetStorageMode(mode string) {
	c.cfg.StorageMode = mode
}

// DatabaseWorkers returns the number of database worker goroutines.
func (c *Config) DatabaseWorkers() int {
	return c.cfg.DatabaseWorkers
}

// DatabaseQueueSize returns the database task queue size.
func (c *Config) DatabaseQueueSize() int {
	return c.cfg.DatabaseQueueSize
}

// BackfillBatchSize returns the batch size for database backfill operations.
func (c *Config) BackfillBatchSize() int {
	return c.cfg.BackfillBatchSize
}

// TargetNumberOfKnownPeers returns the target number of known peers.
func (c *Config) TargetNumberOfKnownPeers() int {
	return c.cfg.TargetNumberOfKnownPeers
}

// SetTargetNumberOfKnownPeers updates the known peers target.
// This is used when applying cardano-node config fallbacks.
func (c *Config) SetTargetNumberOfKnownPeers(n int) {
	c.cfg.TargetNumberOfKnownPeers = n
}

// TargetNumberOfEstablishedPeers returns the target number of established peers.
func (c *Config) TargetNumberOfEstablishedPeers() int {
	return c.cfg.TargetNumberOfEstablishedPeers
}

// SetTargetNumberOfEstablishedPeers updates the established peers target.
// This is used when applying cardano-node config fallbacks.
func (c *Config) SetTargetNumberOfEstablishedPeers(n int) {
	c.cfg.TargetNumberOfEstablishedPeers = n
}

// TargetNumberOfActivePeers returns the target number of active peers.
func (c *Config) TargetNumberOfActivePeers() int {
	return c.cfg.TargetNumberOfActivePeers
}

// SetTargetNumberOfActivePeers updates the active peers target.
// This is used when applying cardano-node config fallbacks.
func (c *Config) SetTargetNumberOfActivePeers(n int) {
	c.cfg.TargetNumberOfActivePeers = n
}

// ActivePeersTopologyQuota returns the per-source quota for topology peers.
func (c *Config) ActivePeersTopologyQuota() int {
	return c.cfg.ActivePeersTopologyQuota
}

// ActivePeersGossipQuota returns the per-source quota for gossip peers.
func (c *Config) ActivePeersGossipQuota() int {
	return c.cfg.ActivePeersGossipQuota
}

// ActivePeersLedgerQuota returns the per-source quota for ledger peers.
func (c *Config) ActivePeersLedgerQuota() int {
	return c.cfg.ActivePeersLedgerQuota
}

// MinHotPeers returns the minimum hot peers before aggressive promotion.
func (c *Config) MinHotPeers() int {
	return c.cfg.MinHotPeers
}

// ReconcileInterval returns the peer governor reconciliation interval.
func (c *Config) ReconcileInterval() time.Duration {
	return c.cfg.ReconcileInterval
}

// InactivityTimeout returns the hot peer inactivity timeout.
func (c *Config) InactivityTimeout() time.Duration {
	return c.cfg.InactivityTimeout
}

// InboundWarmTarget returns the inbound warm peer target.
func (c *Config) InboundWarmTarget() int {
	return c.cfg.InboundWarmTarget
}

// InboundHotQuota returns the inbound hot peer quota.
func (c *Config) InboundHotQuota() int {
	return c.cfg.InboundHotQuota
}

// InboundMinTenure returns the minimum tenure before inbound promotion.
func (c *Config) InboundMinTenure() time.Duration {
	return c.cfg.InboundMinTenure
}

// InboundHotScoreThreshold returns the score threshold for inbound hot promotion.
func (c *Config) InboundHotScoreThreshold() float64 {
	return c.cfg.InboundHotScoreThreshold
}

// InboundPruneAfter returns the duration before pruning idle inbound connections.
func (c *Config) InboundPruneAfter() time.Duration {
	return c.cfg.InboundPruneAfter
}

// InboundDuplexOnlyForHot returns whether duplex is required for hot inbound.
func (c *Config) InboundDuplexOnlyForHot() bool {
	return c.cfg.InboundDuplexOnlyForHot
}

// InboundCooldown returns the cooldown period after inbound demotion.
func (c *Config) InboundCooldown() time.Duration {
	return c.cfg.InboundCooldown
}

// MaxConnectionsPerIP returns the max concurrent inbound connections per IP.
func (c *Config) MaxConnectionsPerIP() int {
	return c.cfg.MaxConnectionsPerIP
}

// MaxInboundConns returns the maximum total inbound connections.
func (c *Config) MaxInboundConns() int {
	return c.cfg.MaxInboundConns
}

// Cache returns the cache configuration.
func (c *Config) Cache() internalconfig.CacheConfig {
	return c.cfg.Cache
}

// Chainsync returns the chainsync configuration.
func (c *Config) Chainsync() internalconfig.ChainsyncConfig {
	return c.cfg.Chainsync
}

// ChainsyncStallTimeoutDuration returns the parsed chainsync stall timeout.
func (c *Config) ChainsyncStallTimeoutDuration() time.Duration {
	if c.chainsyncStallTimeout != 0 {
		return c.chainsyncStallTimeout
	}
	if c.cfg.Chainsync.StallTimeout != "" {
		if d, err := time.ParseDuration(c.cfg.Chainsync.StallTimeout); err == nil {
			return d
		}
	}
	return 2 * time.Minute
}

// GenesisBootstrap returns the Genesis bootstrap configuration.
func (c *Config) GenesisBootstrap() internalconfig.GenesisBootstrapConfig {
	return c.cfg.GenesisBootstrap
}

// HistoryExpiry returns the history expiry configuration.
func (c *Config) HistoryExpiry() internalconfig.HistoryExpiryConfig {
	return c.cfg.HistoryExpiry
}

// OffchainMetadata returns the off-chain metadata fetcher configuration.
func (c *Config) OffchainMetadata() internalconfig.OffchainMetadataConfig {
	return c.cfg.OffchainMetadata
}

// Logging returns the logging configuration.
func (c *Config) Logging() internalconfig.LoggingConfig {
	return c.cfg.Logging
}

// Midnight returns the Midnight indexer configuration.
func (c *Config) Midnight() internalconfig.MidnightConfig {
	return c.cfg.Midnight
}

// CORSAllowedOrigins returns the CORS allowed origins list.
func (c *Config) CORSAllowedOrigins() []string {
	return c.cfg.CORSAllowedOrigins
}

// SlotsPerKESPeriod returns the number of slots per KES period.
func (c *Config) SlotsPerKESPeriod() uint64 {
	return c.cfg.SlotsPerKESPeriod
}

// MaxKESEvolutions returns the maximum number of KES key evolutions.
func (c *Config) MaxKESEvolutions() uint64 {
	return c.cfg.MaxKESEvolutions
}

// BlockProducer returns whether block production mode is enabled.
func (c *Config) BlockProducer() bool {
	return c.cfg.BlockProducer
}

// ShelleyVRFKey returns the path to the VRF signing key file.
func (c *Config) ShelleyVRFKey() string {
	return c.cfg.ShelleyVRFKey
}

// ShelleyKESKey returns the path to the KES signing key file.
func (c *Config) ShelleyKESKey() string {
	return c.cfg.ShelleyKESKey
}

// ShelleyOperationalCertificate returns the path to the operational certificate.
func (c *Config) ShelleyOperationalCertificate() string {
	return c.cfg.ShelleyOperationalCertificate
}

// ForgeSyncToleranceSlots returns the sync tolerance for block forging.
func (c *Config) ForgeSyncToleranceSlots() uint64 {
	return c.cfg.ForgeSyncToleranceSlots
}

// ForgeStaleGapThresholdSlots returns the stale gap threshold for warnings.
func (c *Config) ForgeStaleGapThresholdSlots() uint64 {
	return c.cfg.ForgeStaleGapThresholdSlots
}

// ValidateForgedBlock returns whether to self-validate forged blocks.
func (c *Config) ValidateForgedBlock() bool {
	return c.cfg.ValidateForgedBlock
}

// LeiosVoteSigningKeyFile returns the path to the Leios vote signing key.
func (c *Config) LeiosVoteSigningKeyFile() string {
	return c.cfg.LeiosVoteSigningKeyFile
}

// LeiosVoterPublicKeys returns the Leios voter public key registry.
func (c *Config) LeiosVoterPublicKeys() map[string]string {
	return c.cfg.LeiosVoterPublicKeys
}

// PeerSharing returns the peer sharing configuration.
func (c *Config) PeerSharing() *bool {
	return c.cfg.PeerSharing
}

// Mithril returns the Mithril bootstrap configuration.
func (c *Config) Mithril() internalconfig.MithrilConfig {
	return c.cfg.Mithril
}

// Logger returns the configured logger instance.
func (c *Config) Logger() *slog.Logger {
	return c.logger
}

// CardanoNodeConfig returns the loaded Cardano node configuration.
func (c *Config) CardanoNodeConfig() *cardano.CardanoNodeConfig {
	return c.cardanoNodeConfig
}

// TopologyConfig returns the loaded topology configuration.
func (c *Config) TopologyConfig() *topology.TopologyConfig {
	return c.topologyConfig
}

// PrometheusRegistry returns the Prometheus registry for metrics.
func (c *Config) PrometheusRegistry() prometheus.Registerer {
	return c.promRegistry
}

// IntersectPoints returns the intersect points for chainsync.
func (c *Config) IntersectPoints() []ocommon.Point {
	return c.intersectPoints
}

// Listeners returns the configured listener configurations.
func (c *Config) Listeners() []ListenerConfig {
	return c.listeners
}

// Tracing returns whether OpenTelemetry tracing is enabled.
func (c *Config) Tracing() bool {
	return c.tracing
}

// TracingStdout returns whether tracing output goes to stdout.
func (c *Config) TracingStdout() bool {
	return c.tracingStdout
}

// LedgerPeerTarget returns the target number of ledger peers.
func (c *Config) LedgerPeerTarget() int {
	return c.ledgerPeerTarget
}

// LeiosPipelineTiming returns the Leios pipeline timing configuration.
func (c *Config) LeiosPipelineTiming() *leios.PipelineTiming {
	return c.leiosPipelineTiming
}
