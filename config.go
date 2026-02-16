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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

type ListenerConfig = connmanager.ListenerConfig

// runMode constants for operational mode configuration
const (
	runModeServe = "serve"
	runModeLoad  = "load"
	runModeDev   = "dev"
)

type Config struct {
	promRegistry             prometheus.Registerer
	topologyConfig           *topology.TopologyConfig
	logger                   *slog.Logger
	cardanoNodeConfig        *cardano.CardanoNodeConfig
	dataDir                  string
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
	// Block production configuration (SPO mode)
	// Field names match cardano-node environment variable naming convention
	blockProducer                 bool
	shelleyVRFKey                 string
	shelleyKESKey                 string
	shelleyOperationalCertificate string
	// Blockfrost API listen address (empty = disabled)
	blockfrostListenAddress string
	// Chainsync multi-client configuration
	chainsyncMaxClients   int
	chainsyncStallTimeout time.Duration
	// Mesh API listen address (empty = disabled)
	meshListenAddress string
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

// isDevMode returns true if running in development mode
func (c *Config) isDevMode() bool {
	return c.runMode == runModeDev
}

func (n *Node) configValidate() error {
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
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
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

// WithUtxorpcPort specifies the port to use for the gRPC API listener. This defaults to port 9090
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
// Default quotas: topology=3, gossip=12, ledger=5
func WithActivePeersQuotas(
	topologyQuota, gossipQuota, ledgerQuota int,
) ConfigOptionFunc {
	return func(c *Config) {
		c.activePeersTopologyQuota = topologyQuota
		c.activePeersGossipQuota = gossipQuota
		c.activePeersLedgerQuota = ledgerQuota
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

func WithBarkPort(port uint) ConfigOptionFunc {
	return func(c *Config) {
		c.barkPort = port
	}
}

// WithBlockfrostListenAddress specifies the listen
// address for the Blockfrost-compatible REST API server.
// An empty string disables the server. The default is
// empty (disabled).
func WithBlockfrostListenAddress(
	addr string,
) ConfigOptionFunc {
	return func(c *Config) {
		c.blockfrostListenAddress = addr
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

// WithMeshListenAddress specifies the listen address
// for the Mesh (Coinbase Rosetta) compatible REST API
// server. An empty string disables the server. The
// default is empty (disabled).
func WithMeshListenAddress(
	addr string,
) ConfigOptionFunc {
	return func(c *Config) {
		c.meshListenAddress = addr
	}
}
