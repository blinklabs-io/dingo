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
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ListenerConfig = connmanager.ListenerConfig

// runMode constants for operational mode configuration
const (
	runModeServe = "serve"
	runModeLoad  = "load"
	runModeDev   = "dev"
	runModeLeios = "leios"
)

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
	promRegistry             prometheus.Registerer
	topologyConfig           *topology.TopologyConfig
	logger                   *slog.Logger
	cardanoNodeConfig        *cardano.CardanoNodeConfig
	dataDir                  string
	bindAddr                 string
	blobPlugin               string
	metadataPlugin           string
	network                  string
	tlsCertFilePath          string
	tlsKeyFilePath           string
	intersectPoints          []ocommon.Point
	listeners                []ListenerConfig
	mempoolCapacity          int64
	mempoolImplementation    mempool.Implementation
	evictionWatermark        float64
	rejectionWatermark       float64
	outboundSourcePort       uint
	utxorpcPort              uint
	barkBaseUrl              string
	barkBlockDownloadHosts   []string
	barkPort                 uint
	historyExpiry            HistoryExpiryConfig
	corsAllowedOrigins       []string
	networkMagic             uint32
	intersectTip             bool
	peerSharing              bool
	validateHistorical       bool
	strictUtxoValidation     bool
	tracing                  bool
	tracingStdout            bool
	runMode                  string
	startEra                 internalconfig.StartEra
	shutdownTimeout          time.Duration
	DatabaseWorkerPoolConfig ledger.DatabaseWorkerPoolConfig
	// Peer targets (0 = use default, -1 = unlimited)
	targetNumberOfKnownPeers       int
	targetNumberOfEstablishedPeers int
	targetNumberOfActivePeers      int
	// Per-source quotas for active peers (0 = use default)
	activePeersTopologyQuota int
	activePeersGossipQuota   int
	activePeersLedgerQuota   int
	// Ledger peer discovery (negative = disabled, 0 = use
	// defaultLedgerPeerTarget, positive = target)
	ledgerPeerTarget int
	// Peer governor tuning (0 = use default)
	minHotPeers                          int
	reconcileInterval                    time.Duration
	inactivityTimeout                    time.Duration
	bootstrapPromotionMinDiversityGroups int
	inboundWarmTarget                    int
	inboundHotQuota                      int
	inboundMinTenure                     time.Duration
	inboundHotScoreThreshold             float64
	inboundPruneAfter                    time.Duration
	inboundDuplexOnlyForHot              bool
	inboundCooldown                      time.Duration
	maxConnectionsPerIP                  int
	maxInboundConns                      int
	genesisBootstrap                     bool
	genesisWindowSlots                   uint64
	genesisCorroborationPeers            int
	// Block production configuration (SPO mode)
	// Field names match cardano-node environment variable naming convention
	blockProducer                 bool
	shelleyVRFKey                 string
	shelleyKESKey                 string
	shelleyOperationalCertificate string
	// Forging tolerances (0 = use defaults)
	forgeSyncToleranceSlots     uint64
	forgeStaleGapThresholdSlots uint64
	// validateForgedBlock enables self-validation of locally-forged blocks
	// (VRF/KES header crypto, body-hash, per-tx ledger rules) before the
	// block is adopted onto the chain and diffused to peers.
	validateForgedBlock bool
	// CIP-23 minimum pool margin (minimum variable fee) in basis points,
	// [0, 10000]; consensus-affecting, 0 = off; effective only in Dijkstra+.
	minPoolMargin uint
	// CIP-50 pledge-leverage staking rewards (consensus-affecting; default
	// off). pledgeLeverage is L in [1, 10000], used only when enabled.
	pledgeLeverageEnabled bool
	pledgeLeverage        uint
	// CIP-0163 full-pot reward distribution (consensus-affecting; default
	// off). Distributes the entire epoch reward pot to eligible pools instead
	// of returning the residual to reserves.
	fullPotRewardsEnabled                  bool
	unsafeFullPotRewardsOnStandardNetworks bool
	// CIP-0163 reward-account inactivity expiry (consensus-affecting; default
	// off). delegatorInactivity is the inactivity window in epochs, used only
	// when enabled.
	delegatorInactivityEnabled bool
	delegatorInactivity        uint64
	// Leios voting configuration (experimental)
	leiosVoteSigningKeyFile string
	leiosVoterPublicKeys    map[string]string
	// Leios pipeline timing override (experimental, provisional). Nil
	// uses leios.DefaultPipelineTiming.
	leiosPipelineTiming *leios.PipelineTiming
	// Blockfrost API port (0 = disabled)
	blockfrostPort uint
	// Off-chain metadata fetcher configuration
	offchainMetadata OffchainMetadataConfig
	// Midnight indexer and gRPC API configuration
	midnight MidnightConfig
	// Chainsync multi-client configuration
	chainsyncMaxClients   int
	chainsyncStallTimeout time.Duration
	chainsyncStrategy     chainsync.HeaderSyncStrategy
	// Mesh API port (0 = disabled)
	meshPort uint
	// Storage mode: "core" or "api"
	storageMode StorageMode
	// CBOR cache configuration
	cacheBlockLRUEntries int
	cacheHotUtxoEntries  int
	cacheHotTxEntries    int
	cacheHotTxMaxBytes   int64
}

// configPopulateNetworkMagic uses the named network (if specified) to determine the network magic value (if not specified)
func (n *Node) configPopulateNetworkMagic() error {
	if n.config.networkMagic == 0 && n.config.network != "" {
		tmpCfg := n.config
		tmpNetwork, ok := ouroboros.NetworkByName(n.config.network)
		if !ok {
			return fmt.Errorf("unknown network name: %s", n.config.network)
		}
		tmpCfg.networkMagic = tmpNetwork.NetworkMagic
		n.config = tmpCfg
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
	networkName := n.config.network
	if networkName == "" {
		if net, ok := ouroboros.NetworkByNetworkMagic(n.config.networkMagic); ok {
			networkName = net.String()
		} else {
			networkName = strconv.FormatUint(uint64(n.config.networkMagic), 10)
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
	return c.runMode == runModeDev
}

// isMusashiNetwork reports whether the configured network is the experimental
// Musashi testnet (the IOG Leios prototype network), identified either by its
// network name or its network magic. The Musashi testnet hard-forks into the
// Dijkstra era, so a node syncing it must enable the Dijkstra era table
// regardless of the configured run mode. This lets `dingo -n musashi` follow
// the chain without also requiring `--run-mode leios`.
func (c *Config) isMusashiNetwork() bool {
	return c.network == ouroboros.NetworkCardanoMusashi.Name ||
		c.networkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic
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
		c.runMode == runModeLeios ||
		c.startEra.IsDijkstra()
}

// experimentalDijkstraEnabled reports whether the Dijkstra ledger era is
// active. Beyond the explicit opt-ins, the Musashi testnet hard-forks into
// Dijkstra, so syncing it requires the Dijkstra era table even over base
// protocols.
func (c *Config) experimentalDijkstraEnabled() bool {
	return c.experimentalLeiosNetworkingEnabled() || c.isMusashiNetwork()
}

func (n *Node) configValidate() error {
	if n.config.mempoolImplementation == "" {
		n.config.mempoolImplementation = mempool.ImplementationFIFO
	}
	if !n.config.mempoolImplementation.Valid() {
		return fmt.Errorf(
			"invalid mempool implementation %q: must be %q",
			n.config.mempoolImplementation,
			mempool.ImplementationFIFO,
		)
	}
	// Default storageMode to "core" when unset, and validate.
	if n.config.storageMode == "" {
		n.config.storageMode = StorageModeCore
	}
	if !n.config.storageMode.Valid() {
		return fmt.Errorf(
			"invalid storage mode %q: must be %q or %q",
			n.config.storageMode,
			StorageModeCore,
			StorageModeAPI,
		)
	}
	if !n.config.startEra.Valid() {
		return fmt.Errorf(
			"invalid start era %q: must be empty or %q",
			n.config.startEra,
			internalconfig.StartEraDijkstra,
		)
	}
	// CIP-23 minimum pool margin is expressed in basis points. Validate the
	// exported WithMinPoolMargin option at the root node boundary so an invalid
	// value cannot reach ledger reward or certificate validation.
	if n.config.minPoolMargin > 10_000 {
		return fmt.Errorf(
			"min pool margin (%d) must be in [0, 10000] basis points",
			n.config.minPoolMargin,
		)
	}
	if n.config.pledgeLeverageEnabled &&
		(n.config.pledgeLeverage < 1 || n.config.pledgeLeverage > 10_000) {
		return fmt.Errorf(
			"pledge leverage (%d) must be in [1, 10000] when enabled",
			n.config.pledgeLeverage,
		)
	}
	if n.config.fullPotRewardsEnabled &&
		!n.config.unsafeFullPotRewardsOnStandardNetworks {
		if network, ok := internalconfig.FullPotRewardsStandardNetwork(
			n.config.network,
			n.config.networkMagic,
		); ok {
			return fmt.Errorf(
				"full pot rewards are not permitted on standard network %q "+
					"without unsafe full-pot rewards opt-in",
				network,
			)
		}
	}
	// In core mode, ignore API ports — they are only used in API mode.
	// This lets defaults stay non-zero without requiring core-mode users
	// to explicitly disable each one.
	if n.config.networkMagic == 0 {
		return fmt.Errorf(
			"invalid network magic value: %d",
			n.config.networkMagic,
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
	if n.config.cardanoNodeConfig != nil {
		shelleyGenesis := n.config.cardanoNodeConfig.ShelleyGenesis()
		if shelleyGenesis == nil {
			return errors.New("unable to get Shelley genesis information")
		}
		if n.config.networkMagic != shelleyGenesis.NetworkMagic {
			return fmt.Errorf(
				"network magic (%d) doesn't match value from Shelley genesis (%d)",
				n.config.networkMagic,
				shelleyGenesis.NetworkMagic,
			)
		}
	}
	if n.config.delegatorInactivityEnabled &&
		(n.config.delegatorInactivity < 1 || n.config.delegatorInactivity > 10_000) {
		return fmt.Errorf(
			"delegator inactivity (%d) must be in [1, 10000] when enabled",
			n.config.delegatorInactivity,
		)
	}
	return nil
}

// ConfigOptionFunc is a type that represents functions that modify the Connection config
type ConfigOptionFunc func(*Config)

// NewConfig creates a new dingo config with the specified options
func NewConfig(opts ...ConfigOptionFunc) Config {
	c := Config{
		// Default logger will throw away logs
		// We do this so we don't have to add guards around every log operation
		logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		mempoolImplementation: mempool.ImplementationFIFO,
		genesisBootstrap:      true,
		historyExpiry: HistoryExpiryConfig{
			Frequency: time.Hour,
		},
		midnight: MidnightConfig{
			Port: 50051,
			Host: "0.0.0.0",
		},
		corsAllowedOrigins: []string{
			"*",
		},
	}
	// Apply options
	for _, opt := range opts {
		opt(&c)
	}
	return c
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
		c.bindAddr = addr
	}
}

// WithDatabasePath specifies the persistent data directory to use. The default is to store everything in memory
func WithDatabasePath(dataDir string) ConfigOptionFunc {
	return func(c *Config) {
		c.dataDir = dataDir
	}
}

// WithBlobPlugin specifies the blob storage plugin to use.
func WithBlobPlugin(plugin string) ConfigOptionFunc {
	return func(c *Config) {
		c.blobPlugin = plugin
	}
}

// WithMetadataPlugin specifies the metadata storage plugin to use.
func WithMetadataPlugin(plugin string) ConfigOptionFunc {
	return func(c *Config) {
		c.metadataPlugin = plugin
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
		c.intersectTip = intersectTip
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
		c.network = network
	}
}

// WithNetworkMagic specifies the network magic value to use. This will override any named network specified
func WithNetworkMagic(networkMagic uint32) ConfigOptionFunc {
	return func(c *Config) {
		c.networkMagic = networkMagic
	}
}

// WithOutboundSourcePort specifies the source port to use for outbound connections. This defaults to dynamic source ports
func WithOutboundSourcePort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.outboundSourcePort = port
	}
}

// WithUtxorpcTlsCertFilePath specifies the path to the TLS certificate for the gRPC API listener. This defaults to empty
func WithUtxorpcTlsCertFilePath(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.tlsCertFilePath = path
	}
}

// WithUtxorpcTlsKeyFilePath specifies the path to the TLS key for the gRPC API listener. This defaults to empty
func WithUtxorpcTlsKeyFilePath(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.tlsKeyFilePath = path
	}
}

// WithUtxorpcPort specifies the port to use for the gRPC API listener. 0 disables the server (default)
func WithUtxorpcPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.utxorpcPort = port
	}
}

// WithPeerSharing specifies whether to enable peer sharing. This is disabled by default
func WithPeerSharing(peerSharing bool) ConfigOptionFunc {
	return func(c *Config) {
		c.peerSharing = peerSharing
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
		c.shutdownTimeout = timeout
	}
}

// WithMempoolImplementation selects the mempool backend. FIFO is the default.
func WithMempoolImplementation(
	implementation mempool.Implementation,
) ConfigOptionFunc {
	return func(c *Config) {
		c.mempoolImplementation = implementation
	}
}

// WithMempoolCapacity sets the mempool capacity (in bytes)
func WithMempoolCapacity(capacity int64) ConfigOptionFunc {
	return func(c *Config) {
		c.mempoolCapacity = capacity
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
		c.evictionWatermark = watermark
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
		c.rejectionWatermark = watermark
	}
}

// WithRunMode sets the operational mode ("serve", "load", or "dev").
// "dev" mode enables development behaviors (forge blocks, disable outbound).
func WithRunMode(mode string) ConfigOptionFunc {
	return func(c *Config) {
		c.runMode = mode
	}
}

// WithStartEra sets the experimental direct startup era. Empty uses the
// genesis protocol version; "dijkstra" starts directly in the Dijkstra era.
func WithStartEra(startEra string) ConfigOptionFunc {
	return func(c *Config) {
		c.startEra = internalconfig.StartEra(startEra)
	}
}

// WithValidateHistorical specifies whether to validate all historical blocks during ledger processing
func WithValidateHistorical(validate bool) ConfigOptionFunc {
	return func(c *Config) {
		c.validateHistorical = validate
	}
}

// WithStrictUtxoValidation specifies whether an unrecoverable consumed UTxO
// past the recorded Mithril sync boundary is a hard error rather than a
// silently skipped condition. See database.Config.StrictUtxoValidation.
func WithStrictUtxoValidation(strict bool) ConfigOptionFunc {
	return func(c *Config) {
		c.strictUtxoValidation = strict
	}
}

// WithDatabaseWorkerPoolConfig specifies the database worker pool configuration
func WithDatabaseWorkerPoolConfig(
	cfg ledger.DatabaseWorkerPoolConfig,
) ConfigOptionFunc {
	return func(c *Config) {
		c.DatabaseWorkerPoolConfig = cfg
	}
}

// WithPeerTargets specifies the target number of peers in each state.
// Use 0 to use the default target, or -1 for unlimited.
// Default targets: known=150, established=50, active=20
func WithPeerTargets(
	targetKnown, targetEstablished, targetActive int,
) ConfigOptionFunc {
	return func(c *Config) {
		c.targetNumberOfKnownPeers = targetKnown
		c.targetNumberOfEstablishedPeers = targetEstablished
		c.targetNumberOfActivePeers = targetActive
	}
}

// WithActivePeersQuotas specifies the per-source quotas for active peers.
// Use 0 to use the default quota, or a negative value to disable enforcement.
// Default quotas: topology=20, gossip=20, ledger=20
func WithActivePeersQuotas(
	topologyQuota, gossipQuota, ledgerQuota int,
) ConfigOptionFunc {
	return func(c *Config) {
		c.activePeersTopologyQuota = topologyQuota
		c.activePeersGossipQuota = gossipQuota
		c.activePeersLedgerQuota = ledgerQuota
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
			c.minHotPeers = n
		}
	}
}

// WithBootstrapPromotionMinDiversityGroups sets the minimum number of
// bootstrap-time peer diversity groups to prefer before falling back to pure
// score ordering. Non-positive values use the peer-governor default.
func WithBootstrapPromotionMinDiversityGroups(n int) ConfigOptionFunc {
	return func(c *Config) {
		c.bootstrapPromotionMinDiversityGroups = n
	}
}

// WithReconcileInterval specifies how often the peer governor runs its
// reconciliation loop. Non-positive values are ignored. Default: 5m.
func WithReconcileInterval(d time.Duration) ConfigOptionFunc {
	return func(c *Config) {
		if d > 0 {
			c.reconcileInterval = d
		}
	}
}

// WithInactivityTimeout specifies how long a hot peer can be inactive
// before being demoted to warm. Non-positive values are ignored. Default: 10m.
func WithInactivityTimeout(d time.Duration) ConfigOptionFunc {
	return func(c *Config) {
		if d > 0 {
			c.inactivityTimeout = d
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
			c.inboundWarmTarget = warmTarget
		}
		if hotQuota > 0 {
			c.inboundHotQuota = hotQuota
		}
		if minTenure > 0 {
			c.inboundMinTenure = minTenure
		}
		if hotScoreThreshold > 0 {
			c.inboundHotScoreThreshold = hotScoreThreshold
		}
		if pruneAfter > 0 {
			c.inboundPruneAfter = pruneAfter
		}
		c.inboundDuplexOnlyForHot = duplexOnlyForHot
		if cooldown > 0 {
			c.inboundCooldown = cooldown
		}
	}
}

// WithMaxConnectionsPerIP specifies the maximum number of concurrent
// inbound connections from a single IP. Non-positive values are ignored. Default: 5.
func WithMaxConnectionsPerIP(n int) ConfigOptionFunc {
	return func(c *Config) {
		if n > 0 {
			c.maxConnectionsPerIP = n
		}
	}
}

// WithMaxInboundConns specifies the maximum number of inbound connections.
// Non-positive values are ignored. Default: 100.
func WithMaxInboundConns(n int) ConfigOptionFunc {
	return func(c *Config) {
		if n > 0 {
			c.maxInboundConns = n
		}
	}
}

// WithGenesisBootstrap enables Genesis-mode chain selection during from-origin
// bootstrap. Genesis mode automatically exits once the local tip is within the
// configured Genesis window of the best known peer tip.
func WithGenesisBootstrap(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.genesisBootstrap = enabled
	}
}

// WithGenesisWindowSlots overrides the Genesis density comparison window.
// A zero value lets the node derive the window from Shelley genesis parameters
// using 3k/f.
func WithGenesisWindowSlots(slots uint64) ConfigOptionFunc {
	return func(c *Config) {
		c.genesisWindowSlots = slots
	}
}

// WithGenesisCorroborationPeers sets the number of independent peers that must
// report the same recent blocks before a fast (shallow) block source may drive
// Genesis-mode chain selection. This is the Ouroboros Genesis trust control for
// biased fast-sync sources (e.g. the Genesis Sync Accelerator): an
// uncorroborated or divergent fast source is denied selection and stalls rather
// than steering the local chain.
//
// Only a zero value disables corroboration (density-only Genesis selection). A
// negative value is invalid and fails closed: the chain selector clamps it to 1
// (require one corroborator) rather than treating it as disabled, so a
// misconfiguration cannot silently switch off the security gate.
func WithGenesisCorroborationPeers(peers int) ConfigOptionFunc {
	return func(c *Config) {
		c.genesisCorroborationPeers = peers
	}
}

// WithBlockProducer enables block production mode (CARDANO_BLOCK_PRODUCER).
// When enabled, the node will attempt to produce blocks using the configured credentials.
func WithBlockProducer(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.blockProducer = enabled
	}
}

// WithMinPoolMargin configures the CIP-23 minimum pool margin (minimum variable
// fee) in basis points, [0, 10000] (150 = 1.5%). It is consensus-affecting and
// off by default (0), taking effect only in Dijkstra and later. Enable a nonzero
// value only on a network where every node also enables the same value.
func WithMinPoolMargin(basisPoints uint) ConfigOptionFunc {
	return func(c *Config) {
		c.minPoolMargin = basisPoints
	}
}

// WithPledgeLeverage configures the CIP-50 pledge-leverage staking reward cap.
// It is consensus-affecting and disabled by default; enable it only on a
// network where every node also enables it. leverage is L, the maximum ratio
// of total stake to pledge, and is used only when enabled.
func WithPledgeLeverage(enabled bool, leverage uint) ConfigOptionFunc {
	return func(c *Config) {
		c.pledgeLeverageEnabled = enabled
		c.pledgeLeverage = leverage
	}
}

// WithFullPotRewards configures CIP-0163 full-pot reward distribution. It is
// consensus-affecting and disabled by default; enable it only on a network
// where every node also enables it. When enabled, the entire epoch reward pot
// is distributed to eligible pools and delegators instead of returning the
// residual to reserves.
func WithFullPotRewards(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.fullPotRewardsEnabled = enabled
	}
}

// WithUnsafeFullPotRewardsOnStandardNetworks allows CIP-0163 full-pot reward
// distribution on predefined standard networks. This is consensus-breaking
// unless the network has explicitly adopted the rule; leave disabled for
// normal operation.
func WithUnsafeFullPotRewardsOnStandardNetworks(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.unsafeFullPotRewardsOnStandardNetworks = enabled
	}
}

// WithShelleyVRFKey specifies the path to the VRF signing key file (CARDANO_SHELLEY_VRF_KEY).
// Required for block production.
func WithShelleyVRFKey(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.shelleyVRFKey = path
	}
}

// WithShelleyKESKey specifies the path to the KES signing key file (CARDANO_SHELLEY_KES_KEY).
// Required for block production.
func WithShelleyKESKey(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.shelleyKESKey = path
	}
}

// WithShelleyOperationalCertificate specifies the path to the operational certificate file
// (CARDANO_SHELLEY_OPERATIONAL_CERTIFICATE). Required for block production.
func WithShelleyOperationalCertificate(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.shelleyOperationalCertificate = path
	}
}

// WithLeiosVoteSigningKeyFile specifies the path to a hex-encoded BLS12-381
// Leios vote signing key (DINGO_LEIOS_VOTE_SIGNING_KEY_FILE). When set on a
// block producer whose pool is a Leios committee member, the node emits
// votes for endorser blocks. Experimental, leios runMode only.
func WithLeiosVoteSigningKeyFile(path string) ConfigOptionFunc {
	return func(c *Config) {
		c.leiosVoteSigningKeyFile = path
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
		c.leiosVoterPublicKeys = maps.Clone(keys)
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
		c.forgeSyncToleranceSlots = slots
	}
}

// WithForgeStaleGapThresholdSlots sets the slot gap threshold for stale database warnings.
// Use 0 to fall back to the built-in default.
func WithForgeStaleGapThresholdSlots(slots uint64) ConfigOptionFunc {
	return func(c *Config) {
		c.forgeStaleGapThresholdSlots = slots
	}
}

// WithValidateForgedBlock enables self-validation of locally-forged blocks
// before they are adopted onto the chain and diffused to peers. When enabled,
// the forger runs VRF/KES header crypto, body-hash consistency, and per-tx
// ledger validation on each forged block. A failing block is dropped without
// being adopted or diffused. Disabled by default.
func WithValidateForgedBlock(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.validateForgedBlock = enabled
	}
}

// WithDelegatorInactivity configures the CIP-0163 reward-account inactivity
// expiry. It is consensus-affecting and disabled by default; enable it only on
// a network where every node also enables it. epochs is the inactivity window
// and is used only when enabled.
func WithDelegatorInactivity(enabled bool, epochs uint64) ConfigOptionFunc {
	return func(c *Config) {
		c.delegatorInactivityEnabled = enabled
		c.delegatorInactivity = epochs
	}
}

// WithBlockfrostPort specifies the port for the
// Blockfrost-compatible REST API server. The server binds
// to the node's bindAddr on this port. 0 disables the
// server (default).
func WithBlockfrostPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.blockfrostPort = port
	}
}

func WithBarkBaseUrl(baseUrl string) ConfigOptionFunc {
	return func(c *Config) {
		c.barkBaseUrl = baseUrl
	}
}

func WithBarkBlockDownloadHosts(hosts []string) ConfigOptionFunc {
	return func(c *Config) {
		c.barkBlockDownloadHosts = slices.Clone(hosts)
	}
}

func WithBarkPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.barkPort = port
	}
}

// WithHistoryExpiry configures local immutable block history expiry.
func WithHistoryExpiry(cfg HistoryExpiryConfig) ConfigOptionFunc {
	return func(c *Config) {
		c.historyExpiry = cfg
	}
}

// WithCORSAllowedOrigins configures browser CORS access for public API
// servers. Use []string{"*"} to allow any origin, or an empty list to
// disable CORS headers.
func WithCORSAllowedOrigins(origins []string) ConfigOptionFunc {
	return func(c *Config) {
		c.corsAllowedOrigins = slices.Clone(origins)
	}
}

// WithOffchainMetadataConfig configures the API-mode off-chain metadata
// fetcher. Zero values use the fetcher's internal defaults.
func WithOffchainMetadataConfig(cfg OffchainMetadataConfig) ConfigOptionFunc {
	return func(c *Config) {
		c.offchainMetadata = cfg
	}
}

// WithMidnightConfig configures the Midnight indexer and optional gRPC API.
func WithMidnightConfig(cfg MidnightConfig) ConfigOptionFunc {
	return func(c *Config) {
		c.midnight = cfg
	}
}

// WithChainsyncMaxClients specifies the maximum number of
// concurrent chainsync client connections. Default is 3.
func WithChainsyncMaxClients(
	maxClients int,
) ConfigOptionFunc {
	return func(c *Config) {
		c.chainsyncMaxClients = maxClients
	}
}

// WithChainsyncStallTimeout specifies the duration after
// which a chainsync client with no activity is considered
// stalled. Default is 2 minutes.
func WithChainsyncStallTimeout(
	timeout time.Duration,
) ConfigOptionFunc {
	return func(c *Config) {
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
		c.chainsyncStrategy = strategy
	}
}

// WithMeshPort specifies the port for the Mesh (Coinbase
// Rosetta) compatible REST API server. The server binds
// to the node's bindAddr on this port. 0 disables the
// server (default).
func WithMeshPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.meshPort = port
	}
}

// WithStorageMode specifies the storage mode. StorageModeCore
// stores only consensus data; StorageModeAPI adds full
// transaction metadata for API queries.
func WithStorageMode(mode StorageMode) ConfigOptionFunc {
	return func(c *Config) {
		c.storageMode = mode
	}
}

// WithCacheConfig sets the CBOR cache sizes for block LRU,
// hot UTxO, and hot TX caches.
func WithCacheConfig(
	blockLRU, hotUtxo, hotTx int,
	hotTxMaxBytes int64,
) ConfigOptionFunc {
	return func(c *Config) {
		c.cacheBlockLRUEntries = blockLRU
		c.cacheHotUtxoEntries = hotUtxo
		c.cacheHotTxEntries = hotTx
		c.cacheHotTxMaxBytes = hotTxMaxBytes
	}
}
