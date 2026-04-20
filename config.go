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
	"runtime"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledger"
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
	evictionWatermark        float64
	rejectionWatermark       float64
	outboundSourcePort       uint
	utxorpcPort              uint
	barkBaseUrl              string
	barkSecurityWindow       uint64
	barkPort                 uint
	networkMagic             uint32
	intersectTip             bool
	peerSharing              bool
	validateHistorical       bool
	tracing                  bool
	tracingStdout            bool
	runMode                  string
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
	maxConnectionsPerIP                  int
	maxInboundConns                      int
	genesisBootstrap                     bool
	genesisWindowSlots                   uint64
	// Block production configuration (SPO mode)
	// Field names match cardano-node environment variable naming convention
	blockProducer                 bool
	shelleyVRFKey                 string
	shelleyKESKey                 string
	shelleyOperationalCertificate string
	// Forging tolerances (0 = use defaults)
	forgeSyncToleranceSlots     uint64
	forgeStaleGapThresholdSlots uint64
	// Blockfrost API port (0 = disabled)
	blockfrostPort uint
	// Chainsync multi-client configuration
	chainsyncMaxClients   int
	chainsyncStallTimeout time.Duration
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
// rewriting queries. See docs/plans/RTS_METRICS.md for the semantic
// mapping between Go and Haskell GC concepts.
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

func (n *Node) configValidate() error {
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
	return nil
}

// ConfigOptionFunc is a type that represents functions that modify the Connection config
type ConfigOptionFunc func(*Config)

// NewConfig creates a new dingo config with the specified options
func NewConfig(opts ...ConfigOptionFunc) Config {
	c := Config{
		// Default logger will throw away logs
		// We do this so we don't have to add guards around every log operation
		logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		genesisBootstrap: true,
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

// WithValidateHistorical specifies whether to validate all historical blocks during ledger processing
func WithValidateHistorical(validate bool) ConfigOptionFunc {
	return func(c *Config) {
		c.validateHistorical = validate
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

// WithBlockProducer enables block production mode (CARDANO_BLOCK_PRODUCER).
// When enabled, the node will attempt to produce blocks using the configured credentials.
func WithBlockProducer(enabled bool) ConfigOptionFunc {
	return func(c *Config) {
		c.blockProducer = enabled
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

func WithBarkSecurityWindow(window uint64) ConfigOptionFunc {
	return func(c *Config) {
		c.barkSecurityWindow = window
	}
}

func WithBarkPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.barkPort = port
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
// stalled. Default is 30 seconds.
func WithChainsyncStallTimeout(
	timeout time.Duration,
) ConfigOptionFunc {
	return func(c *Config) {
		c.chainsyncStallTimeout = timeout
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
func WithCacheConfig(blockLRU, hotUtxo, hotTx int, hotTxMaxBytes int64) ConfigOptionFunc {
	return func(c *Config) {
		c.cacheBlockLRUEntries = blockLRU
		c.cacheHotUtxoEntries = hotUtxo
		c.cacheHotTxEntries = hotTx
		c.cacheHotTxMaxBytes = hotTxMaxBytes
	}
}
