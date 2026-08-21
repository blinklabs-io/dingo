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

package dingo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/api/blockfrost"
	"github.com/blinklabs-io/dingo/api/mesh"
	"github.com/blinklabs-io/dingo/api/utxorpc"
	"github.com/blinklabs-io/dingo/bark"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/chainsyncrecycler"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/historyexpiry"
	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/node/ledgerpeers"
	"github.com/blinklabs-io/dingo/internal/offchainmetadata"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/mempool"
	midnightindexer "github.com/blinklabs-io/dingo/midnight/indexer"
	midnightserver "github.com/blinklabs-io/dingo/midnight/server"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
)

type Node struct {
	connManager *connmanager.ConnectionManager
	peerGov     *peergov.PeerGovernor
	// poolRelayProvider backs peerGov's LedgerPeerProvider. Tracked here (not
	// a throwaway local) so quiesceForLiveLifecycleOp can Close it -- it has
	// no Stop of its own otherwise, so a live database restore/truncate,
	// which constructs a fresh one on every cycle, would leak its EventBus
	// subscription every time (node_lifecycle.go).
	poolRelayProvider       *ledger.PoolRelayProvider
	chainsyncState          *chainsync.State
	chainSelector           *chainselection.ChainSelector
	eventBus                *event.EventBus
	pluginHost              *plugin.Host
	destinationRegistry     *lifecycle.DestinationRegistry
	mempool                 mempool.Service
	chainManager            *chain.ChainManager
	db                      *database.Database
	ledgerState             *ledger.LedgerState
	snapshotMgr             *snapshot.Manager
	dbLifecycleMgr          *dblifecycle.Manager
	leiosVoteManager        *leios.VoteManager
	leiosPipelineManager    *leios.PipelineManager
	bark                    *bark.Bark
	historyExpiry           *historyexpiry.Pruner
	koiosParityObserver     *koiosparity.Observer
	midnightServer          *midnightserver.Server
	offchainMetadataFetcher *offchainmetadata.Fetcher
	tokenRegistrySync       *offchainmetadata.TokenRegistrySync
	midnightIndexer         *midnightindexer.Indexer
	// ouroborosRef holds the current Ouroboros. It is atomic because a live
	// snapshot/restore replaces the instance while EventBus handlers and
	// component callbacks -- which resolve it at call time, by design -- may
	// be reading it from other goroutines. Read it through the ouroboros()
	// accessor; the only writer is the replacement in
	// reinitializeNetworkingCore and the initial construction in Run.
	ouroborosRef atomic.Pointer[ouroborosPkg.Ouroboros]
	// ouroborosConfig retains the settings half of the config Run built, so a
	// live restore can reconstruct ouroboros against rebuilt dependencies
	// without recomputing them and drifting from Run.
	ouroborosConfig                  ouroborosPkg.OuroborosConfig
	blockForger                      *forging.BlockForger
	leaderElection                   *leader.Election
	rtsMetrics                       *rtsMetrics
	shutdownFuncs                    []func(context.Context) error
	deferredIndexMaintenanceDone     chan struct{}
	config                           Config
	ctx                              context.Context
	cancel                           context.CancelFunc
	shutdownOnce                     sync.Once
	shutdownErr                      error
	chainsyncStallRecycler           *chainsyncrecycler.Recycler
	chainsyncIngressEligibilityMu    sync.RWMutex
	chainsyncIngressEligibilityCache map[ouroboros.ConnectionId]bool

	// EventBus subscriber IDs for handlers bound to components that a live
	// database restore/truncate rebuilds from scratch (node_lifecycle.go).
	// Every other Run()-registered handler is either a closure over n itself
	// (self-healing — reads the current field value at call time) or bound
	// to a component that lifecycle rebuild leaves untouched, so it needs no
	// tracked ID. Captured here (rather than discarded, as Run() otherwise
	// would) purely so node_lifecycle.go can unsubscribe the stale handler
	// before rebuilding its component; Run()'s own behavior is unchanged.
	chainsyncClientRemoveSubId event.EventSubscriberId
	connManagerRecycleSubId    event.EventSubscriberId
	leiosVoteEmittedSubId      event.EventSubscriberId
	// koiosParitySubId is tracked for the same reason: observer.
	// HandleEpochTransitionEvent is bound to the *koiosparity.Observer
	// instance startKoiosParityObserver creates, which a live database
	// restore/truncate must tear down and rebuild (a stale observer would
	// otherwise keep running against the pre-rebuild n.db) -- see
	// node_lifecycle.go's quiesceForLiveLifecycleOp/
	// reinitializeBackgroundManagers handling of it.
	koiosParitySubId event.EventSubscriberId

	// liveLifecycleMu serializes live database Restore/Truncate calls
	// (node_lifecycle.go) so two can never quiesce/rebuild concurrently.
	// Deliberately NOT held by Snapshot (see snapshotMu): Snapshot never
	// nils/rebuilds n.ledgerState or n.chainsyncState the way Restore/
	// Truncate do, so a background reader like the chainsync recycler
	// tick only needs to know whether a REBUILD is in flight -- not
	// whether an unrelated, non-rebuilding Snapshot happens to be
	// running, which this mutex would otherwise make indistinguishable.
	liveLifecycleMu sync.Mutex

	// snapshotMu serializes Snapshot calls against each other and against
	// a concurrent Restore/Truncate (which closes n.db out from under an
	// in-progress Snapshot if not excluded), and is what enforces bark
	// DatabaseService's "one operation at a time" invariant for Snapshot
	// specifically. Restore/Truncate take both this and liveLifecycleMu;
	// Snapshot takes only this one -- so a long-running Snapshot (a full
	// local copy plus cloud upload) never blocks a background reader that
	// only cares about liveLifecycleMu, such as the chainsync recycler
	// tick's stall-detection/plateau-recovery check, matching Snapshot's
	// own documented "keeps syncing normally" behavior.
	snapshotMu sync.Mutex

	// rebuildableMetrics tracks every Prometheus collector registered by a
	// component a live database restore/truncate rebuilds, so
	// closeStorageForLiveLifecycleOp can unregister them before the
	// rebuild re-registers fresh ones under the same names. See
	// metrics_registerer.go.
	rebuildableMetrics *rebuildableRegisterer
}

func New(cfg Config) (*Node, error) {
	pluginHost, err := internalplugins.NewHost()
	if err != nil {
		return nil, fmt.Errorf("create plugin host: %w", err)
	}
	// Cloud destination schemes (s3, gcs) are registered explicitly here,
	// at composition time, rather than via a process-global registry each
	// scheme's own package would otherwise populate through an init() —
	// see database/lifecycle/destination.go's DestinationRegistry doc
	// comment.
	destinationRegistry := lifecycle.NewDestinationRegistry()
	lifecycle.RegisterBuiltinDestinations(destinationRegistry)
	n := &Node{
		config:              cfg,
		pluginHost:          pluginHost,
		destinationRegistry: destinationRegistry,
	}
	for capability, selection := range cfg.pluginSelections {
		// API capabilities are validated against their *merged* config
		// (shared api.tls/api.auth defaults folded in) so an invalid
		// effective TLS/auth policy -- e.g. a partial certificate/key
		// pair -- is rejected here, before any listener starts, using
		// the exact same merge apiPluginSelection applies at Start()/
		// reinitializeAPIServers time. See apiProviderConfig.
		if configPath, ok := apiProviderConfigPath[capability]; ok {
			var err error
			selection, err = cfg.apiProviderConfig(capability, selection)
			if err != nil {
				return nil, fmt.Errorf("invalid plugin selection: %w", err)
			}
			if err := validateAPIProviderSecurityPolicy(
				configPath, selection.Config,
			); err != nil {
				return nil, fmt.Errorf("invalid plugin selection: %w", err)
			}
		}
		if err := pluginHost.ValidateSelection(
			capability, selection.Provider, selection.Config,
		); err != nil {
			return nil, fmt.Errorf("invalid plugin selection: %w", err)
		}
	}
	if err := n.configPopulateNetworkMagic(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	// Wrap the prometheus registry with a "network" label so all metrics
	// registered by subsystems carry the network name automatically.
	// This must happen before any component registers metrics.
	n.configWrapPromRegistry()
	n.registerBuildInfo()
	n.registerRTSMetrics()
	if err := n.configValidate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	// NewEventBus starts background async-worker goroutines, so create the bus
	// only after configuration validates. If it were created earlier, a
	// validation failure would return a nil Node while leaving those goroutines
	// running, with no handle for the caller to Stop() them.
	n.eventBus = event.NewEventBus(n.config.promRegistry, n.config.logger)
	// Everything registered above (build info, RTS gauges, the EventBus)
	// lives for the node's entire lifetime and is never rebuilt, so it's
	// registered directly against the pre-wrap registerer. Everything
	// that reads n.config.promRegistry from here on — in Run() and in
	// every node_lifecycle.go reinitialize call — goes through this
	// wrapper instead, so a live restore/truncate can unregister and
	// re-register it without a duplicate-collector panic.
	if n.config.promRegistry != nil {
		n.rebuildableMetrics = newRebuildableRegisterer(n.config.promRegistry)
		n.config.promRegistry = n.rebuildableMetrics
	}
	return n, nil
}

// legacyUtxorpcTLSPolicy expresses the pre-#2996 root tlsCertFilePath/
// tlsKeyFilePath fields as an apiconfig.TLSPolicy, for UTxORPC only. It
// deliberately does not feed cfg.apiConfig.TLS (the shared api.tls default
// every provider inherits from): UTxORPC was the only provider these root
// fields ever configured TLS for, and promoting them to a shared default
// would silently switch Blockfrost/Mesh from plaintext to TLS on upgrade
// for any deployment that set them, breaking existing plaintext clients.
// See ARCHITECTURE.md's "API security" section for this compatibility
// decision. Returns the zero TLSPolicy (no effect on the merge) unless
// both root fields are set.
func legacyUtxorpcTLSPolicy(cfg *Config) apiconfig.TLSPolicy {
	if cfg.tlsCertFilePath == "" || cfg.tlsKeyFilePath == "" {
		return apiconfig.TLSPolicy{}
	}
	mode := string(apiconfig.TLSModeServer)
	return apiconfig.TLSPolicy{
		Mode:         &mode,
		CertFilePath: &cfg.tlsCertFilePath,
		KeyFilePath:  &cfg.tlsKeyFilePath,
	}
}

// apiProviderConfig merges the shared api.tls/api.auth policy (and, for
// UTxORPC only, the legacy root TLS compatibility fields) into selection's
// own "tls"/"auth" config sections, field by field, and returns the result.
// It is the single place this merge happens, called both by the early
// plugin-selection validation in New() and by apiPluginSelection, so a
// provider config validated at startup and the one actually resolved at
// Start()/reinitializeAPIServers time can never diverge.
func (c *Config) apiProviderConfig(
	capability plugin.Capability,
	selection plugin.Selection,
) (plugin.Selection, error) {
	var legacyTLS apiconfig.TLSPolicy
	if capability == plugin.CapabilityAPIUtxorpc {
		legacyTLS = legacyUtxorpcTLSPolicy(c)
	}
	merged, err := apiconfig.MergeProviderConfig(
		selection.Config,
		legacyTLS,
		c.apiConfig.TLS,
		c.apiConfig.Auth,
	)
	if err != nil {
		return selection, fmt.Errorf(
			"merge api security policy for capability %s: %w",
			capability, err,
		)
	}
	selection.Config = merged
	return selection, nil
}

// apiProviderConfigPath maps each API capability to the dotted config path
// its provider config lives at, for error messages -- see
// validateAPIProviderSecurityPolicy and each provider's own
// cfg.TLS.Resolve/cfg.Auth.Resolve call, which use the identical path.
var apiProviderConfigPath = map[plugin.Capability]string{
	plugin.CapabilityAPIBlockfrost: "plugins.api.blockfrost.config",
	plugin.CapabilityAPIMesh:       "plugins.api.mesh.config",
	plugin.CapabilityAPIUtxorpc:    "plugins.api.utxorpc.config",
}

// validateAPIProviderSecurityPolicy resolves and validates the merged
// tls/auth sections of an API provider's config (already merged with the
// shared api.tls/api.auth defaults by apiProviderConfig), surfacing a
// partial certificate/key pair or an invalid mode before any listener
// starts -- the same validation each provider's own RegisterProvider
// factory performs at Resolve()/Start() time, run here again so New()
// itself rejects it at construction, before Run() ever attempts to start
// a listener.
func validateAPIProviderSecurityPolicy(
	configPath string,
	rawConfig map[string]any,
) error {
	tlsPolicy, err := apiconfig.DecodeTLSPolicy(rawConfig)
	if err != nil {
		return fmt.Errorf("%s.tls: %w", configPath, err)
	}
	if _, err := tlsPolicy.Resolve(configPath + ".tls"); err != nil {
		return err
	}
	authPolicy, err := apiconfig.DecodeAuthPolicy(rawConfig)
	if err != nil {
		return fmt.Errorf("%s.auth: %w", configPath, err)
	}
	if _, err := authPolicy.Resolve(configPath + ".auth"); err != nil {
		return err
	}
	return nil
}

func (n *Node) apiPluginSelection(
	capability plugin.Capability,
) (plugin.Selection, uint, error) {
	selection, ok := n.config.pluginSelections[capability]
	if !ok {
		return selection, 0, fmt.Errorf(
			"plugin selection is missing for capability %s",
			capability,
		)
	}
	if selection.Provider == "" {
		return selection, 0, fmt.Errorf(
			"plugin provider is empty for capability %s",
			capability,
		)
	}
	if _, ok := apiProviderConfigPath[capability]; ok {
		var err error
		selection, err = n.config.apiProviderConfig(capability, selection)
		if err != nil {
			return selection, 0, err
		}
	}
	portValue, ok := selection.Config["port"]
	if !ok {
		defaultPorts := map[plugin.Capability]uint{
			plugin.CapabilityAPIBlockfrost: 3000,
			plugin.CapabilityAPIMesh:       8080,
			plugin.CapabilityAPIUtxorpc:    9090,
		}
		return selection, defaultPorts[capability], nil
	}
	var port uint64
	switch value := portValue.(type) {
	case int:
		if value < 0 {
			return selection, 0, fmt.Errorf("negative port for capability %s", capability)
		}
		port = uint64(value)
	case uint:
		port = uint64(value)
	case uint64:
		port = value
	case int64:
		if value < 0 {
			return selection, 0, fmt.Errorf("negative port for capability %s", capability)
		}
		port = uint64(value)
	case float64:
		if value < 0 || value != float64(uint64(value)) {
			return selection, 0, fmt.Errorf("invalid port for capability %s: %v", capability, value)
		}
		port = uint64(value)
	default:
		return selection, 0, fmt.Errorf("invalid port type for capability %s: %T", capability, portValue)
	}
	if port > 65535 {
		return selection, 0, fmt.Errorf(
			"port for capability %s exceeds 65535: %d",
			capability,
			port,
		)
	}
	return selection, uint(port), nil
}

// effectiveBarkHost decides the interface Bark actually binds to.
// configuredHost (from --bark-host/DINGO_BARK_HOST/config) always wins when
// set -- an explicit operator choice. Otherwise, when lifecycleEnabled (the
// database lifecycle service's destructive Restore/Truncate/CreateSnapshot/
// etc. RPCs will be mounted), this defaults to loopback-only rather than
// letting bark.go's own empty-Host default ("0.0.0.0") expose them on every
// interface; with no lifecycle service mounted, "" is returned unchanged so
// bark's own existing default behavior (all interfaces) is preserved for
// deployments only using it for the read-only Archive service. Bind address
// is a network control, independent of the mTLS client-certificate
// authentication check Bark.Start enforces whenever lifecycleEnabled (see
// BarkConfig.TlsClientCAFilePath) -- this default narrows exposure as
// defense in depth, it is not what makes those RPCs safe to reach.
func effectiveBarkHost(configuredHost string, lifecycleEnabled bool) string {
	if configuredHost != "" {
		return configuredHost
	}
	if lifecycleEnabled {
		return "127.0.0.1"
	}
	return ""
}

// ouroboros returns the current Ouroboros instance. Callers resolve it through
// here rather than caching it, so a live restore's replacement is picked up
// automatically and the read is synchronized against the replacement.
func (n *Node) ouroboros() *ouroborosPkg.Ouroboros {
	return n.ouroborosRef.Load()
}

//nolint:contextcheck // Run is the lifecycle boundary and derives n.ctx from the caller context.
func (n *Node) Run(ctx context.Context) error {
	// Configure tracing
	n.warnIfTracingMisconfigured()
	if n.config.tracing {
		if err := n.setupTracing(ctx); err != nil {
			return err
		}
	}
	n.ctx, n.cancel = context.WithCancel(ctx)

	// Start the RTS metrics updater goroutine. It samples runtime.MemStats
	// on a ticker and exits when n.ctx is cancelled by the existing
	// shutdown or startup-failure cleanup, so it does not need an entry in
	// the `started` cleanup stack.
	go n.runRTSMetricsUpdater(n.ctx, rtsMetricsUpdateInterval)

	// Track started components for cleanup on failure
	var started []func()
	stopPluginCapability := func(capability plugin.Capability) func() {
		return func() {
			if err := n.pluginHost.StopCapability(
				context.Background(),
				capability,
			); err != nil {
				n.config.logger.Error(
					"failed to stop plugin capability during cleanup",
					"capability",
					capability,
					"error",
					err,
				)
			}
		}
	}
	success := false
	defer func() {
		r := recover()
		if r != nil {
			if n.cancel != nil {
				n.cancel()
			}
			// Cleanup on panic, then re-panic
			for _, s := range slices.Backward(started) {
				s()
			}
			panic(r)
		} else if !success {
			if n.cancel != nil {
				n.cancel()
			}
			// Cleanup on failure (non-panic)
			for _, s := range slices.Backward(started) {
				s()
			}
		}
	}()

	// Register eventBus cleanup (created in New(), has background goroutines).
	// Close (not Stop): startup-failure cleanup is terminal, and Stop restarts
	// the async-worker pool, leaking those goroutines.
	started = append(started, func() { n.eventBus.Close() })
	started = append(started, func() {
		if err := n.pluginHost.Stop(context.Background()); err != nil {
			n.config.logger.Error(
				"failed to stop plugin host during cleanup",
				"error",
				err,
			)
		}
	})

	// Reconcile a live Restore's directory swap (node_lifecycle.go's
	// swapInRestoredDataDir) that was interrupted by a crash, process
	// kill, or power failure before it could be confirmed and cleaned up
	// -- must run before anything else below opens n.config.dataDir.
	if err := n.reconcileInterruptedLiveRestoreSwap(); err != nil {
		return fmt.Errorf(
			"reconcile interrupted live restore swap: %w", err,
		)
	}

	// Resolve provider-owned storage before constructing the database that uses
	// it. The startup cleanup stack stops any provider that started before a
	// later storage resolution failure.
	stores, err := internalplugins.ResolveStorage(
		n.ctx,
		n.pluginHost,
		internalplugins.StorageSelections{
			Blob:     n.config.pluginSelections[plugin.CapabilityStorageBlob],
			Metadata: n.config.pluginSelections[plugin.CapabilityStorageMetadata],
		},
		internalplugins.StorageDependencies{
			DataDir: n.config.dataDir, RunMode: n.config.runMode,
			StorageMode:    string(n.config.storageMode),
			MaxConnections: n.config.DatabaseWorkerPoolConfig.WorkerPoolSize,
			Logger:         n.config.logger, PromRegistry: n.config.promRegistry,
		},
	)
	if err != nil {
		return err
	}

	// Load database
	dbNeedsRecovery := false
	dbConfig := &database.Config{
		DataDir:              n.config.dataDir,
		Logger:               n.config.logger,
		PromRegistry:         n.config.promRegistry,
		StorageMode:          string(n.config.storageMode),
		Network:              n.config.network,
		NetworkMagic:         n.config.networkMagic,
		StartEra:             string(n.config.startEra),
		StrictUtxoValidation: n.config.strictUtxoValidation,
		BlobPlugin: n.config.pluginSelections[plugin.CapabilityStorageBlob].
			Provider,
		MetadataPlugin: n.config.pluginSelections[plugin.CapabilityStorageMetadata].
			Provider,
		CacheConfig: database.CborCacheConfig{
			BlockLRUEntries: n.config.cacheBlockLRUEntries,
			HotUtxoEntries:  n.config.cacheHotUtxoEntries,
			HotTxEntries:    n.config.cacheHotTxEntries,
			HotTxMaxBytes:   n.config.cacheHotTxMaxBytes,
		},
	}
	db, err := database.New(dbConfig, stores)
	if db == nil {
		if err != nil {
			n.config.logger.Error(
				"failed to create database",
				"error",
				err,
			)
			return err
		}
		n.config.logger.Error(
			"failed to create database",
			"error",
			"empty database returned",
		)
		return errors.New("empty database returned")
	}
	n.db = db
	started = append(started, func() { n.db.Close() })
	if err != nil {
		var dbErr database.CommitTimestampError
		if !errors.As(err, &dbErr) {
			return fmt.Errorf("failed to open database: %w", err)
		}
		n.config.logger.Warn(
			"database initialization error, needs recovery",
			"error",
			err,
		)
		dbNeedsRecovery = true
	}
	if dbNeedsRecovery {
		// A database awaiting recovery has a known-inconsistent commit
		// state. Enforcing gates against it here could report a spurious
		// mismatch that masks the recovery path that is about to repair
		// it, so both phases are deferred until
		// RecoverCommitTimestampConflict has run, below. database.New
		// never got to call phase 1 (CheckNodeSettings) on this path
		// either -- checkCommitTimestamp failed first and New returned
		// immediately -- so phase 1 is not just deferred here, it has not
		// run for this startup at all until the deferred call below.
		n.config.logger.Info(
			"node settings gate enforcement deferred until database recovery completes",
		)
	} else if err := n.db.EnforceNodeSettings(n.nodeSettingsGateValues()); err != nil {
		return fmt.Errorf("node settings: %w", err)
	}
	if pending, pendingErr := lifecycle.GetPendingTruncate(n.db); pendingErr != nil {
		return fmt.Errorf(
			"check for interrupted database truncate: %w",
			pendingErr,
		)
	} else if pending != nil {
		return fmt.Errorf(
			"database truncate was interrupted after it started (target slot %d, target id %d); rerun the truncate operation before starting the node",
			pending.TargetSlot,
			pending.TargetID,
		)
	}
	// Load chain manager
	cm, err := chain.NewManager(
		n.db,
		n.eventBus,
		n.config.promRegistry,
	)
	if err != nil {
		return fmt.Errorf("failed to load chain manager: %w", err)
	}
	n.chainManager = cm
	n.chainsyncIngressEligibilityCache = make(
		map[ouroboros.ConnectionId]bool,
	)
	// The Dijkstra ledger era and the Leios node-to-node mini-protocols are
	// both enabled on the Leios testnet (and via explicit opt-in). The Leios
	// protocols dingo offers beyond what the prototype relays serve — the
	// standalone leios-votes protocol and leios-fetch BlockTxsRequest — are
	// gated off for the prototype network below, since initiating them resets
	// the connection. See Config.experimentalDijkstraEnabled /
	// experimentalLeiosNetworkingEnabled.
	enableDijkstra := n.config.experimentalDijkstraEnabled()
	enableLeiosNetworking := n.config.experimentalLeiosNetworkingEnabled()
	// Initialize Ouroboros
	// The endorser-block tx fetch keeps re-requesting an EB's still-diffusing
	// tail for up to the Leios diffusion window before giving up (the relay
	// diffuses an EB's transactions over several seconds, so the last partial
	// window lags). Derived from the pipeline timing (DiffuseWindowSlots) and
	// the Shelley slot length; zero disables the retry (e.g. networking off or
	// unknown slot length).
	var leiosTxFetchTailBudget time.Duration
	if enableLeiosNetworking && n.config.cardanoNodeConfig != nil {
		if sg := n.config.cardanoNodeConfig.ShelleyGenesis(); sg != nil {
			if secs, _ := sg.SlotLength.Float64(); secs > 0 {
				leiosTxFetchTailBudget = time.Duration(
					float64(n.leiosPipelineTiming().DiffuseWindowSlots) *
						secs * float64(time.Second),
				)
			}
		}
	}
	// On Musashi wait up to the same keep-alive server timeout bound that the
	// ouroboros layer enforces; elsewhere leave it 0 so gouroboros keeps its
	// default.
	var keepAliveTimeout time.Duration
	if n.config.isMusashiNetwork() {
		keepAliveTimeout = okeepalive.ServerTimeout
	}
	// n.ouroboros is constructed further down, once every dependency it
	// requires exists. Nothing between here and there dereferences it: the
	// callbacks handed to ledger, connmanager and peergov below are closures
	// that resolve n.ouroboros when they fire, not method values bound now.
	// Load state
	state, err := ledger.NewLedgerState(
		ledger.LedgerStateConfig{
			ChainManager:       n.chainManager,
			Database:           n.db,
			EventBus:           n.eventBus,
			Logger:             n.config.logger,
			CardanoNodeConfig:  n.config.cardanoNodeConfig,
			PromRegistry:       n.config.promRegistry,
			ForgeBlocks:        n.config.isDevMode(),
			ValidateHistorical: n.config.validateHistorical,
			EnableDijkstra:     enableDijkstra,
			StartInDijkstra:    n.config.startEra.IsDijkstra(),
			// Parallel block-decode pipeline for the chainsync replay loop
			// (issue #1894 phase 1). Not consensus-affecting; off by default.
			BlockPipelineEnabled: n.config.blockPipelineEnabled,
			// Parallel VRF/KES validate stage for the same pipeline (issue
			// #1894 phase 3). Off by default; requires BlockPipelineEnabled.
			BlockPipelineValidateEnabled: n.config.blockPipelineValidateEnabled,
			// Supplies fetched Leios endorser-block transactions so the ledger
			// can apply them when their referencing Dijkstra ranking block is
			// processed (completing the UTxO set for endorser-resident outputs).
			// Closure, not a method value: n.ouroboros does not exist yet
			// here, so this resolves it when the callback fires. Same for
			// EndorserBlockFetcher and BlockfetchRequestRangeFunc below.
			EndorserBlockProvider: func(
				ebHash []byte,
			) (uint64, []cbor.RawMessage, bool) {
				return n.ouroboros().EndorserBlockTxsByHash(ebHash)
			},
			// Actively fetches a referenced endorser block by point and caches
			// it. Used during historical catch-up: the prototype relay serves
			// any endorser block by point on demand, so a from-scratch sync can
			// backfill older ranking blocks' endorser-resident outputs and build
			// a complete UTxO set instead of trusting the chain.
			EndorserBlockFetcher: func(ebSlot uint64, ebHash []byte) error {
				return n.ouroboros().FetchEndorserBlockByPoint(ebSlot, ebHash)
			},
			// Wait, at the tip, for a ranking block's referenced endorser block
			// to arrive before applying it. Sourced from the pipeline timing
			// (CIP-0164-tracking, override-able via WithLeiosPipelineTiming)
			// rather than a hardcoded duration. We use CertifyByDeadlineSlots,
			// not DiffuseWindowSlots: by the time a ranking block references an
			// EB, that EB has already been certified, and measurement shows the
			// prototype relay's tx-offer delay (~3s) plus the fetch (~3s, incl.
			// the still-diffusing tail) exceeds the diffusion window — so the
			// certify deadline is the bound that matches when the EB is actually
			// available to fetch.
			EndorserBlockWaitSlots: n.leiosPipelineTiming().CertifyByDeadlineSlots,
			// Two-path Leios ledger selection: the Musashi prototype
			// (prototype-2026w29) applies only the certified parent EB, without
			// validation or consumed-input recovery (Haskell-conformant), whereas
			// dingo's forward path applies the current announcement normally
			// (CIP-conformant).
			LeiosApplyEndorserBlockTxs: !n.config.isMusashiNetwork(),
			// dingo's leadership stake omits reward-account balances (staking
			// rewards are not yet computed), which spuriously rejects the
			// dominant pool's eligible blocks on Musashi's concentrated
			// topology and wedges the chain. Trust rather than reject there
			// until reward calculation lands; enforce on real networks where
			// the omission is negligible. TPraos bootstrap pool-threshold
			// checks are waived separately inside header validation after
			// genesis overlay slots are handled.
			SkipLeaderStakeThresholdCheck: n.config.prototypeTrustBypassesEnabled(),
			// On Musashi, certified endorser txs and Dijkstra ranking-block txs are
			// trusted by the prototype; skip dingo's per-tx validation to match it
			// and keep block application at the production rate.
			SkipDijkstraTxValidation: n.config.prototypeTrustBypassesEnabled(),
			// CIP-23 minimum pool margin (minimum variable fee). Operator-set,
			// off by default (0), effective only in Dijkstra and later.
			MinPoolMargin: n.config.minPoolMargin,
			// CIP-50 pledge-leverage reward cap. Operator-set (not derived from
			// the network) and off by default; enable only where every node
			// also enables it.
			PledgeLeverageEnabled: n.config.pledgeLeverageEnabled,
			PledgeLeverage:        n.config.pledgeLeverage,
			// CIP-0163 full-pot reward distribution. Operator-set (not derived
			// from the network) and off by default; enable only where every node
			// also enables it.
			FullPotRewardsEnabled: n.config.fullPotRewardsEnabled,
			// CIP-0163 reward-account inactivity expiry (operator-set)
			DelegatorInactivityEnabled: n.config.delegatorInactivityEnabled,
			DelegatorInactivity:        n.config.delegatorInactivity,
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				return n.ouroboros().
					BlockfetchClientRequestRange(connId, start, end)
			},
			PeersWithBlockFunc: func(
				origin ouroboros.ConnectionId,
				point ocommon.Point,
			) []ouroboros.ConnectionId {
				if n.chainsyncState == nil {
					return nil
				}
				return n.chainsyncState.PeersWithBlock(origin, point)
			},
			RecordBlockfetchLatencyFunc: func(
				connId ouroboros.ConnectionId,
				latency time.Duration,
			) {
				if n.chainsyncState != nil {
					n.chainsyncState.RecordBlockfetchLatency(
						connId,
						latency,
					)
				}
			},
			BlockfetchLatencyFunc: func(
				connId ouroboros.ConnectionId,
			) (time.Duration, bool) {
				if n.chainsyncState == nil {
					return 0, false
				}
				return n.chainsyncState.BlockfetchLatency(connId)
			},
			BlockfetchLatencyMedianFunc: func() (time.Duration, int) {
				if n.chainsyncState == nil {
					return 0, 0
				}
				return n.chainsyncState.BlockfetchLatencyMedian()
			},
			DatabaseWorkerPoolConfig: n.config.DatabaseWorkerPoolConfig,
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				// Return the current best peer for rollback filtering and
				// blockfetch fallback. Headers can arrive from any eligible
				// peer, but rollbacks and retry selection still need a
				// current best connection.
				if n.chainsyncState != nil {
					return n.chainsyncState.GetClientConnId()
				}
				return nil
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return n.connManager != nil &&
					n.connManager.GetConnectionById(connId) != nil
			},
			ConnectionSwitchFunc: func() {
				// Retain older seen-header history so a switched peer
				// can replay only the post-tip segment from the local
				// intersect point without re-delivering older headers.
				if n.chainsyncState != nil && n.ledgerState != nil {
					n.chainsyncState.ClearSeenHeadersFrom(
						n.ledgerState.Tip().Point.Slot,
					)
				}
			},
			ClearSeenHeadersFromFunc: func(fromSlot uint64) {
				if n.chainsyncState != nil {
					n.chainsyncState.ClearSeenHeadersFrom(fromSlot)
				}
			},
			PeerHeaderLookupFunc: func(
				connId ouroboros.ConnectionId,
				hash []byte,
			) (ledger.ChainsyncEvent, []byte, bool) {
				if n.chainsyncState == nil {
					return ledger.ChainsyncEvent{}, nil, false
				}
				h, prevHash, ok := n.chainsyncState.LookupObservedHeader(
					connId,
					hash,
				)
				if !ok {
					return ledger.ChainsyncEvent{}, nil, false
				}
				return ledger.ChainsyncEvent{
					ConnectionId: h.ConnectionId,
					BlockHeader:  h.BlockHeader,
					Point:        h.Point,
					Tip:          h.Tip,
					BlockNumber:  h.BlockNumber,
					Type:         h.Type,
					Rollback:     h.Rollback,
				}, prevHash, true
			},
			GenesisSelectionStateFunc: func() (bool, uint64) {
				if n.chainSelector == nil {
					return false, 0
				}
				return n.chainSelector.GenesisSelectionState()
			},
			FatalErrorFunc: func(err error) {
				n.config.logger.Error(
					"fatal ledger error, initiating shutdown",
					"error", err,
				)
				n.cancel()
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to load state database: %w", err)
	}
	n.ledgerState = state
	// n.ouroboros is constructed once every dependency exists; see the
	// NewOuroboros call below.
	if err := n.chainManager.SetLedger(n.ledgerState); err != nil {
		return fmt.Errorf(
			"failed to configure chain security parameter: %w",
			err,
		)
	}

	if n.config.barkBaseUrl != "" {
		barkBlobStore, err := bark.NewBarkBlobStore(bark.BlobStoreBarkConfig{
			BaseUrl:                   n.config.barkBaseUrl,
			BlockDownloadAllowedHosts: n.config.barkBlockDownloadHosts,
			HTTPClient: &http.Client{
				Timeout: 30 * time.Second,
			},
		}, n.db.Blob())
		if err != nil {
			return fmt.Errorf("failed to create bark blob store: %w", err)
		}
		n.db.SetBlobStore(barkBlobStore)
	}

	// Recovery changes both the ledger tip and blob contents. Complete it
	// before starting background maintenance that reads or prunes either store.
	if dbNeedsRecovery {
		if err := n.ledgerState.RecoverCommitTimestampConflict(); err != nil {
			return fmt.Errorf("failed to recover database: %w", err)
		}
		// The deferred phase 1 pass: database.New returned before ever
		// calling CheckNodeSettings on this path (checkCommitTimestamp
		// fails first, and New returns its error immediately rather than
		// continuing on to phase 1), so storage_mode, network,
		// network_magic, start_era, and the plugin selections have not
		// been validated or persisted for this startup at all. The
		// database is consistent now that recovery has completed, so it
		// is safe to run that check here, before the deferred phase 2
		// pass below.
		n.config.logger.Info("running deferred node settings phase 1 check")
		if err := n.db.CheckNodeSettings(); err != nil {
			return fmt.Errorf("node settings phase 1: %w", err)
		}
		// The deferred phase 2 pass from above: the database is
		// consistent now, so a gate mismatch can no longer be confused
		// with the repair that just ran. This still lands before history
		// expiry, the Midnight indexer, and every network listener below,
		// so it completes before anything can apply a block or act on a
		// ledger feature flag phase 2 would have rejected.
		n.config.logger.Info("running deferred node settings gate enforcement")
		if err := n.db.EnforceNodeSettings(n.nodeSettingsGateValues()); err != nil {
			return fmt.Errorf("node settings: %w", err)
		}
	}

	if n.config.historyExpiry.Enabled {
		prunerFreq := n.config.historyExpiry.Frequency
		if prunerFreq <= 0 {
			prunerFreq = time.Hour
		}
		n.historyExpiry = historyexpiry.NewPruner(historyexpiry.PrunerConfig{
			LedgerState: state,
			DB:          n.db,
			Logger:      n.config.logger,
			Frequency:   prunerFreq,
		})

		if err := n.historyExpiry.Start(n.ctx); err != nil {
			return fmt.Errorf("failed to start history expiry: %w", err)
		}

		started = append(started, func() {
			_ = n.historyExpiry.Stop(context.Background())
		})
	}

	if err := n.backfillRewardLiveStake(); err != nil {
		return err
	}

	// Create and start the Midnight indexer before LedgerState.Start so that
	// (a) the synchronous backfill runs while no new blocks can arrive, and
	// (b) the EventBus subscription exists before any BlockActionApply events
	// can be emitted, eliminating the startup gap identified in #2114. The
	// epoch cache is loaded first because Midnight backfill writes epoch-keyed
	// Ariadne/candidate rows. Both the explicit opt-in and API storage mode
	// are required: the indexer depends on the api-mode indexes to function,
	// and storage mode alone is no longer sufficient to start it (an api-mode
	// deployment may not want Midnight indexing at all).
	if n.config.midnight.Enabled && n.config.storageMode.IsAPI() {
		if err := n.ledgerState.PrepareEpochCacheForStartup(); err != nil {
			return fmt.Errorf(
				"load epoch cache before Midnight indexer start: %w",
				err,
			)
		}
		midnightIdx, err := midnightindexer.New(midnightindexer.Config{
			EventBus:                n.eventBus,
			Metadata:                n.db.Metadata(),
			SlotTimer:               n.ledgerState,
			Logger:                  n.config.logger,
			PromRegistry:            n.config.promRegistry,
			CNightPolicyID:          n.config.midnight.CNightPolicyID,
			CNightAssetName:         n.config.midnight.CNightAssetName,
			MappingValidatorAddress: n.config.midnight.MappingValidatorAddress,
			AuthTokenPolicyID:       n.config.midnight.AuthTokenPolicyID,
			AuthTokenAssetName:      n.config.midnight.AuthTokenAssetName,
			// Governance / Ariadne / candidate scanning
			TechnicalCommitteeAddress:   n.config.midnight.TechnicalCommitteeAddress,
			TechnicalCommitteePolicyID:  n.config.midnight.TechnicalCommitteePolicyID,
			CouncilAddress:              n.config.midnight.CouncilAddress,
			CouncilPolicyID:             n.config.midnight.CouncilPolicyID,
			PermissionedCandidatePolicy: n.config.midnight.PermissionedCandidatePolicy,
			CommitteeCandidateAddress:   n.config.midnight.CommitteeCandidateAddress,
			SlotToEpoch: func(slot uint64) (uint64, error) {
				epoch, err := n.ledgerState.SlotToEpoch(slot)
				if err != nil {
					return 0, err
				}
				return epoch.EpochId, nil
			},
			BlockIterator: func(startSlot, endSlot uint64, fn func(models.Block) error) error {
				return database.ForEachBlockInRangeDB(
					n.db,
					startSlot,
					endSlot,
					fn,
				)
			},
			FatalErrorFunc: func(err error) {
				n.config.logger.Error(
					"fatal midnight indexer error, initiating shutdown",
					"error", err,
				)
				n.cancel()
			},
		})
		if err != nil {
			return fmt.Errorf("creating midnight indexer: %w", err)
		}
		n.midnightIndexer = midnightIdx
		n.config.logger.Info(
			"midnight indexer created, running backfill and subscribing to live events",
		)
		if err := n.midnightIndexer.Start(); err != nil {
			return fmt.Errorf("starting midnight indexer: %w", err)
		}
	}

	// Initialize snapshot manager for stake snapshot capture and wire the
	// authoritative epoch-boundary capture hooks before n.ledgerState.Start
	// below, whose slot-clock/block-processing goroutines are what can
	// first fire an epoch rollover: an epoch boundary reached before these
	// hooks exist would fall back to the event-driven capture only, and — if
	// the Koios parity observer is also enabled — race the observer's own
	// event.EpochTransitionEvent subscription into validating an epoch whose
	// reward rows the snapshot manager never got a chance to commit via
	// these hooks. Configuring both before the observer is wired below (and
	// both before Start) keeps the dependency ordering unambiguous: hooks
	// configured → observer subscribed → ledger started.
	n.snapshotMgr = snapshot.NewManager(
		n.db,
		n.eventBus,
		n.config.logger,
	)
	// Mirror the CIP-0163 reward-account inactivity gate into snapshot capture
	// so it matches the ledger config that drives account expiry stamping.
	if err := n.snapshotMgr.SetDelegatorInactivity(
		n.config.delegatorInactivityEnabled,
		n.config.delegatorInactivity,
	); err != nil {
		return fmt.Errorf("configuring snapshot manager: %w", err)
	}
	n.snapshotMgr.SetPromRegistry(n.config.promRegistry)
	// Wire the authoritative epoch-boundary capture before block sync begins so
	// each epoch rollover stages its mark snapshot atomically at the SNAP point.
	// Set before CaptureGenesisSnapshot/sync; a nil hook (never set) would leave
	// only the event-driven fallback capture.
	// The stake read runs at the SNAP point (after MIR and before POOLREAP/
	// enactment) and
	// the row write at the end of the rollover, both inside the same
	// transaction.
	n.ledgerState.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return n.snapshotMgr.ComputeEpochBoundarySnapshot(n.ctx, txn, evt)
		},
	)
	n.ledgerState.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return n.snapshotMgr.CaptureEpochBoundarySnapshot(n.ctx, txn, evt)
		},
	)

	// Optional in-process Koios reward-parity observer (dingo #3098). Wired
	// (and, critically, subscribed to event.EpochTransitionEventType) before
	// n.ledgerState.Start below, whose slot-clock/block-processing
	// goroutines are what can first publish that event — see
	// startKoiosParityObserver's doc comment (node_koiosparity.go). Wired
	// after the snapshot-manager hooks immediately above, so an epoch
	// boundary the observer reacts to always has its reward rows committed
	// via those hooks first.
	if n.config.koiosParity.Enabled {
		if err := n.startKoiosParityObserver(); err != nil {
			return fmt.Errorf("starting koios parity observer: %w", err)
		}
		started = append(started, func() {
			stopCtx, cancel := context.WithTimeout(
				context.Background(), n.configuredShutdownTimeout(),
			)
			defer cancel()
			if err := n.koiosParityObserver.Stop(stopCtx); err != nil {
				n.config.logger.Error(
					"failed to stop koios parity observer during cleanup",
					"error", err,
				)
			}
		})
	}

	// Start ledger.
	if err := n.ledgerState.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to start ledger: %w", err)
	}
	started = append(started, func() { n.ledgerState.Close() })
	// Register midnight indexer cleanup after LedgerState so it is torn down
	// first (reverse order): midnight.Stop() → ledgerState.Close().
	if n.midnightIndexer != nil {
		started = append(started, func() { n.midnightIndexer.Stop() })
	}
	// Capture genesis stake snapshot (epoch 0) so leader election works at epoch 2
	if err := n.snapshotMgr.CaptureGenesisSnapshot(ctx); err != nil {
		if err := n.handleGenesisSnapshotError(err); err != nil {
			return err
		}
	}
	if err := n.snapshotMgr.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to start snapshot manager: %w", err)
	}
	started = append(started, func() { _ = n.snapshotMgr.Stop() })
	// Initialize and start automatic database-snapshot manager (distinct
	// from the stake/reward snapshot manager above — see
	// internal/dblifecycle.Manager doc comment).
	n.dbLifecycleMgr = dblifecycle.NewManager(
		n.db,
		n.eventBus,
		n.config.databaseLifecycle,
		n.config.pluginSelections[plugin.CapabilityStorageBlob].Provider,
		n.config.pluginSelections[plugin.CapabilityStorageMetadata].Provider,
		n.destinationRegistry,
		n.config.logger,
	)
	if err := n.dbLifecycleMgr.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf(
			"failed to start database lifecycle manager: %w",
			err,
		)
	}
	started = append(started, func() { _ = n.dbLifecycleMgr.Stop() })
	// Initialize Leios vote manager (experimental)
	if enableDijkstra {
		//nolint:contextcheck // n.ctx is the node's lifecycle context
		if err := n.initLeiosVoteManager(n.ctx); err != nil {
			return fmt.Errorf(
				"failed to initialize leios vote manager: %w",
				err,
			)
		}
		started = append(started, func() { _ = n.leiosVoteManager.Stop() })
		// Initialize the Leios pipeline manager after the vote manager so
		// it stops first (LIFO cleanup): it consumes the vote manager's
		// EbQuorumEvent.
		//nolint:contextcheck // n.ctx is the node's lifecycle context
		if err := n.initLeiosPipelineManager(n.ctx); err != nil {
			return fmt.Errorf(
				"failed to initialize leios pipeline manager: %w",
				err,
			)
		}
		started = append(started, func() { _ = n.leiosPipelineManager.Stop() })
	} else if n.config.leiosVoteSigningKeyFile != "" {
		n.config.logger.Warn(
			"leios vote signing key configured without leios mode; voting disabled",
			"component", "node",
		)
	}
	// Resolve mempool only after ledger dependencies are available.
	mempoolSelection := n.config.pluginSelections[plugin.CapabilityMempool]
	n.mempool, err = plugin.Resolve[mempool.Service](
		n.ctx,
		n.pluginHost,
		plugin.CapabilityMempool,
		mempoolSelection.Provider,
		mempoolSelection.Config,
		mempool.ProviderDependencies{
			PromRegistry:    n.config.promRegistry,
			Validator:       n.ledgerState,
			Logger:          n.config.logger,
			EventBus:        n.eventBus,
			CurrentSlotFunc: n.ledgerState.CurrentOrTipSlot,
		},
	)
	if err != nil {
		return fmt.Errorf("resolve mempool: %w", err)
	}
	started = append(
		started,
		stopPluginCapability(plugin.CapabilityMempool),
	)
	// Set mempool adapter in ledger state for block forging.
	n.ledgerState.SetMempool(&ledgerMempoolAdapter{source: n.mempool})
	// Initialize chainsync state with multi-client configuration
	chainsyncCfg := chainsync.DefaultConfig()
	if n.config.chainsyncMaxClients > 0 {
		chainsyncCfg.MaxClients = n.config.chainsyncMaxClients
	}
	if n.config.chainsyncStallTimeout > 0 {
		chainsyncCfg.StallTimeout = n.config.chainsyncStallTimeout
	}
	chainsyncCfg.HeaderSyncStrategy = n.config.chainsyncStrategy
	chainsyncCfg.PromRegistry = n.config.promRegistry
	chainsyncCfg.ObservedHeaderLimitFunc = func() int {
		if n.chainSelector == nil {
			return 0
		}
		active, window := n.chainSelector.GenesisSelectionState()
		if !active {
			return 0
		}
		if window > uint64(math.MaxInt) {
			return math.MaxInt
		}
		// The MaxInt check above makes this conversion safe on both 32- and
		// 64-bit platforms.
		return int(window) //nolint:gosec // G115: window is bounded by MaxInt
	}
	n.chainsyncState = chainsync.NewStateWithConfig(
		n.eventBus,
		n.ledgerState,
		chainsyncCfg,
	)
	n.eventBus.SubscribeFunc(
		peergov.PeerEligibilityChangedEventType,
		n.handlePeerEligibilityChangedEvent,
	)
	n.eventBus.SubscribeFunc(
		peergov.PeerEligibilityChangedEventType,
		func(evt event.Event) {
			n.ouroboros().HandlePeerEligibilityChangedEvent(evt)
		},
	)
	// Subscriber ID captured for the same reason as chainManager's above —
	// n.chainsyncState is rebuilt during a live database restore/truncate.
	n.chainsyncClientRemoveSubId = n.eventBus.SubscribeFunc(
		chainsync.ClientRemoveRequestedEventType,
		n.chainsyncState.HandleClientRemoveRequestedEvent,
	)
	// Initialize chain selector for multi-peer chain selection
	chainSelectorSecurityParam := uint64(0)
	if k := n.ledgerState.SecurityParam(); k > 0 {
		chainSelectorSecurityParam = uint64(k) //nolint:gosec
	}
	genesisWindowSlots := n.config.genesisWindowSlots
	if genesisWindowSlots == 0 {
		genesisWindowSlots = chainselection.GenesisWindowSlotsForParams(
			chainSelectorSecurityParam,
			n.ledgerState.ActiveSlotCoeff(),
		)
	}
	genesisSelectionMode := n.config.genesisBootstrap &&
		!n.config.intersectTip &&
		len(n.config.intersectPoints) == 0
	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			Logger:                n.config.logger,
			EventBus:              n.eventBus,
			SecurityParam:         chainSelectorSecurityParam,
			GenesisMode:           genesisSelectionMode,
			GenesisWindowSlots:    genesisWindowSlots,
			MinCorroboratingPeers: n.config.genesisCorroborationPeers,
			ConnectionLive: func(connId ouroboros.ConnectionId) bool {
				return n.connManager != nil &&
					n.connManager.GetConnectionById(connId) != nil
			},
			BlockfetchLatency: func(connId ouroboros.ConnectionId) (time.Duration, bool) {
				if n.chainsyncState == nil {
					return 0, false
				}
				return n.chainsyncState.BlockfetchLatency(connId)
			},
		},
	)
	if genesisSelectionMode {
		n.config.logger.Info(
			"Genesis chain selection enabled",
			"genesis_window_slots", genesisWindowSlots,
			"security_param", chainSelectorSecurityParam,
			"min_corroborating_peers", n.config.genesisCorroborationPeers,
		)
	}
	// Wire chain-selector event subscriptions.
	n.subscribeChainSelectorEvents()
	// Start the chain selector
	if err := n.chainSelector.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to start chain selector: %w", err)
	}
	started = append(started, func() { n.chainSelector.Stop() })
	// Configure connection manager. The listener configs and outbound
	// connection options come from ouroboros, which does not exist yet
	// because it needs this ConnectionManager — so they are supplied as
	// providers, resolved on first use at Start, by which point ouroboros is
	// built.
	n.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:   n.config.logger,
			EventBus: n.eventBus,
			ListenersProvider: func() []connmanager.ListenerConfig {
				return n.ouroboros().ConfigureListeners(n.config.listeners)
			},
			OutboundSourcePort: n.config.outboundSourcePort,
			OutboundConnOptsProvider: func() []ouroboros.ConnectionOptionFunc {
				return n.ouroboros().OutboundConnOpts()
			},
			PromRegistry:        n.config.promRegistry,
			MaxConnectionsPerIP: n.config.maxConnectionsPerIP,
			MaxInboundConns:     n.config.maxInboundConns,
		},
	)
	// Wire connection-manager and inbound/outbound connection events.
	n.subscribeConnectionEvents()
	// Configure peer governor before opening listeners so topology-driven
	// outbound connections start first and do not lose the race to inbounds.
	// Create ledger relay provider for discovering peers from stake pool relays.
	n.poolRelayProvider, err = ledger.NewPoolRelayProvider(
		n.ledgerState,
		n.db,
		n.eventBus,
	)
	if err != nil {
		return fmt.Errorf("failed to create ledger relay provider: %w", err)
	}
	ledgerPeerProvider := ledgerpeers.NewProvider(n.poolRelayProvider)

	// Get UseLedgerAfterSlot from topology config (defaults to -1 = disabled).
	var useLedgerAfterSlot int64 = -1
	if n.config.topologyConfig != nil {
		useLedgerAfterSlot = n.config.topologyConfig.UseLedgerAfterSlot
	}

	n.peerGov = peergov.NewPeerGovernor(
		peergov.PeerGovernorConfig{
			Logger:          n.config.logger,
			EventBus:        n.eventBus,
			ConnManager:     n.connManager,
			DisableOutbound: n.config.isDevMode(),
			PromRegistry:    n.config.promRegistry,
			PeerRequestFunc: func(peer *peergov.Peer) []string {
				return n.ouroboros().RequestPeersFromPeer(peer)
			},
			LedgerPeerProvider:                   ledgerPeerProvider,
			UseLedgerAfterSlot:                   useLedgerAfterSlot,
			LedgerPeerTarget:                     n.config.ledgerPeerTarget,
			TargetNumberOfKnownPeers:             n.config.targetNumberOfKnownPeers,
			TargetNumberOfEstablishedPeers:       n.config.targetNumberOfEstablishedPeers,
			TargetNumberOfActivePeers:            n.config.targetNumberOfActivePeers,
			ActivePeersTopologyQuota:             n.config.activePeersTopologyQuota,
			ActivePeersGossipQuota:               n.config.activePeersGossipQuota,
			ActivePeersLedgerQuota:               n.config.activePeersLedgerQuota,
			InboundWarmTarget:                    n.config.inboundWarmTarget,
			InboundHotQuota:                      n.config.inboundHotQuota,
			InboundMinTenure:                     n.config.inboundMinTenure,
			InboundHotScoreThreshold:             n.config.inboundHotScoreThreshold,
			InboundPruneAfter:                    n.config.inboundPruneAfter,
			InboundDuplexOnlyForHot:              n.config.inboundDuplexOnlyForHot,
			InboundCooldown:                      n.config.inboundCooldown,
			MinHotPeers:                          n.config.minHotPeers,
			ReconcileInterval:                    n.config.reconcileInterval,
			InactivityTimeout:                    n.config.inactivityTimeout,
			SyncProgressProvider:                 n.ledgerState,
			BootstrapPromotionMinDiversityGroups: n.config.bootstrapPromotionMinDiversityGroups,
		},
	)
	// Construct ouroboros now that every dependency exists. It takes them all
	// up front and validates them, so it can never be observed partially
	// wired. This is deliberately the last construction before the peer
	// governor and connection manager start below.
	n.ouroborosConfig = ouroborosPkg.OuroborosConfig{
		Logger:                n.config.logger,
		EventBus:              n.eventBus,
		ConnManager:           n.connManager,
		LedgerState:           n.ledgerState,
		Mempool:               n.mempool,
		ChainsyncState:        n.chainsyncState,
		PeerGov:               n.peerGov,
		NetworkMagic:          n.config.networkMagic,
		PeerSharing:           n.config.peerSharing,
		IntersectTip:          n.config.intersectTip,
		IntersectPoints:       n.config.intersectPoints,
		PromRegistry:          n.retainedComponentPromRegistry(),
		ChainsyncBlockTimeout: n.config.chainsyncStallTimeout,
		EnableLeios:           enableLeiosNetworking,
		// The standalone leios-votes mini-protocol (protocol 20) is a dingo
		// extension ahead of the IOG Leios prototype. The prototype relays do
		// not run a protocol-20 responder and reset the connection if we
		// initiate it, so disable it on the Leios prototype network; there
		// votes are diffused inline over leios-notify. Keep it available for
		// non-prototype Leios peers (e.g. dingo-to-dingo) that support it.
		EnableLeiosVotes: enableLeiosNetworking && !n.config.isMusashiNetwork(),
		// Request endorser-block transaction bodies over leios-fetch, driven by
		// the peer's transactions offer (MsgBlockTxsOffer) — the relay's signal
		// that the EB's transactions are ready. Fetching before that offer
		// (e.g. right after the manifest) makes the prototype relay reset the
		// connection, so the fetch is gated on the txs offer, not the block
		// offer. Best-effort: a fetch failure never tears down the shared
		// connection.
		EnableLeiosTxFetch:           enableLeiosNetworking,
		LeiosTxFetchTailBudget:       leiosTxFetchTailBudget,
		ChainsyncIngressEligible:     n.isChainsyncIngressEligible,
		ChainsyncApplyEligible:       n.chainsyncApplyEligible,
		ChainsyncObservePeerTip:      n.chainsyncObservePeerTip,
		ChainsyncObservePeerRollback: n.chainsyncObservePeerRollback,
		// On the Musashi prototype network every mini-protocol shares one muxer
		// to a single relay; block/EB traffic can delay the relay's keep-alive
		// pong past the tight 10s gouroboros default, making dingo drop the
		// only relay and pay a reconnect + fork rollback. Wait up to the
		// keep-alive server timeout there so a slow-but-alive relay is not
		// dropped. Unset on other networks (fast dead-peer eviction retained).
		KeepAliveTimeout: keepAliveTimeout,
	}
	ouro, err := ouroborosPkg.NewOuroboros(n.ouroborosConfig)
	if err != nil {
		return fmt.Errorf("failed to construct ouroboros: %w", err)
	}
	// The Leios managers were started earlier in Run, before this instance
	// existed, so their handlers are attached here rather than at their own
	// construction. reinitializeNetworkingCore does the same after its
	// rebuild.
	if err := n.attachLeiosHandlers(ouro); err != nil {
		return err
	}
	n.ouroborosRef.Store(ouro)
	// The asynchronous Leios endorser-block persistence writer, the EventBus
	// subscriptions ouroboros makes on its own behalf, and its Prometheus
	// collectors are all released by Close. Registering it on both the
	// unwind stack and a defer covers startup failure and graceful shutdown;
	// Close is idempotent.
	defer func() { _ = n.ouroboros().Close() }()
	started = append(started, func() { _ = n.ouroboros().Close() })
	// A closure, not a method value, even though n.ouroboros already exists
	// here: a live restore replaces the instance, and a method value would
	// pin this subscription to the replaced one forever, so outbound
	// connections would be handled by a closed Ouroboros and the node would
	// silently stop starting chainsync clients after any restore.
	n.eventBus.SubscribeFunc(
		peergov.OutboundConnectionEventType,
		func(evt event.Event) { n.ouroboros().HandleOutboundConnEvent(evt) },
	)
	// Subscribe ouroboros to chainsync resync events from the ledger, so all
	// stop/restart orchestration lives in the ouroboros/chainsync component.
	// This is a method call rather than a handler registration, so unlike the
	// subscriptions in subscribeConnectionEvents it has to run after the
	// constructor above.
	n.ouroboros().SubscribeChainsyncResync(n.ctx) //nolint:contextcheck
	if n.config.topologyConfig != nil {
		topologyConfig := n.config.topologyConfig
		usePeerSnapshot := genesisSelectionMode &&
			topologyConfig.PeerSnapshot != nil &&
			topologyConfig.PeerSnapshot.HasRelays()
		if usePeerSnapshot {
			topologyConfig = topologyConfig.WithoutBootstrapPeers()
		}
		n.peerGov.LoadTopologyConfig(topologyConfig)
		if usePeerSnapshot {
			added := n.peerGov.LoadPeerSnapshot(
				n.ctx,
				n.config.topologyConfig.PeerSnapshot,
			)
			if added > 0 {
				n.config.logger.Info(
					"using peer snapshot for Genesis bootstrap",
					"snapshot_slot",
					n.config.topologyConfig.PeerSnapshot.Point.BlockPointSlot,
					"snapshot_peers_added",
					added,
					"bootstrap_peers_omitted",
					len(n.config.topologyConfig.BootstrapPeers),
				)
			} else {
				n.config.logger.Warn(
					"peer snapshot produced no usable peers; falling back to topology bootstrap peers",
					"snapshot_slot",
					n.config.topologyConfig.PeerSnapshot.Point.BlockPointSlot,
				)
				n.peerGov.LoadTopologyConfig(n.config.topologyConfig)
			}
		}
	}
	if err := n.peerGov.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("peer governor start failed: %w", err)
	}
	// Bounded, not context.Background(): Stop waits for an in-flight
	// GetPoolRelays call, so an unbounded wait here would let a later
	// startup failure block Node.Run forever instead of returning its
	// error.
	started = append(started, func() {
		stopCtx, cancel := context.WithTimeout(
			context.Background(),
			n.configuredShutdownTimeout(),
		)
		defer cancel()
		if err := n.peerGov.Stop(stopCtx); err != nil {
			n.config.logger.Warn(
				"peer governor shutdown during startup rollback",
				"error", err,
			)
		}
	})
	// Start listeners
	if err := n.connManager.Start(n.ctx); err != nil { //nolint:contextcheck
		return err
	}
	// A caller-supplied net.Listener (e.g. a test harness binding an
	// OS-assigned port up front) is single-use: Stop always closes every
	// listener it owns, including one it didn't create, so the exact
	// object can never be reused after a live Restore/Truncate's
	// quiesce-then-reinit cycle. Recording the concrete address it
	// actually resolved to now lets reinitializeNetworkingCore rebind a
	// fresh listener at that same address later, instead of trying (and
	// silently failing) to reuse the original, by-then-permanently-closed
	// object. See ConnectionManager.ResolvedListeners's doc comment.
	n.config.listeners = n.connManager.ResolvedListeners()
	started = append(started, func() { //nolint:contextcheck
		if err := n.connManager.Stop(context.Background()); err != nil {
			n.config.logger.Error(
				"failed to stop connection manager during cleanup",
				"error",
				err,
			)
		}
	})
	// Detect stalled chainsync clients and recycle truly stuck connections.
	if err := n.startChainsyncStallRecycler(n.ctx, chainsyncCfg); err != nil { //nolint:contextcheck
		return fmt.Errorf("chainsync stall recycler start failed: %w", err)
	}
	// On startup failure or panic, stop the recycler and wait for it before
	// unwinding later components that the recycler can still touch.
	started = append(started, n.waitChainsyncStallRecycler)
	// Resolve UTxO RPC only in API mode with a non-zero configured port.
	utxorpcSelection, utxorpcPort, err := n.apiPluginSelection(
		plugin.CapabilityAPIUtxorpc,
	)
	if err != nil {
		return err
	}
	if n.config.storageMode.IsAPI() && utxorpcPort > 0 {
		err = plugin.ResolveProvider(
			n.ctx, n.pluginHost, plugin.CapabilityAPIUtxorpc,
			utxorpcSelection.Provider, utxorpcSelection.Config,
			utxorpc.ProviderDependencies{
				Logger: n.config.logger, EventBus: n.eventBus,
				LedgerState: n.ledgerState, Mempool: n.mempool,
				Host:               n.config.bindAddr,
				CORSAllowedOrigins: n.config.corsAllowedOrigins,
			},
		)
		if err != nil {
			return fmt.Errorf("resolve utxorpc API: %w", err)
		}
		started = append(
			started,
			stopPluginCapability(plugin.CapabilityAPIUtxorpc),
		)
	}

	if n.config.barkPort > 0 {
		lifecycleEnabled := n.config.databaseLifecycle.SnapshotDir != ""
		barkHost := effectiveBarkHost(n.config.barkHost, lifecycleEnabled)
		if barkHost != n.config.barkHost {
			n.config.logger.Warn(
				"bark database lifecycle service (Restore/Truncate and friends) defaults to a loopback-only bind since no --bark-host was set; its destructive RPCs also require a verified mTLS client certificate (--bark-client-ca-file-path) independent of bind address, but widen this bind only behind your own trusted network controls",
				"component",
				"bark",
			)
		}
		barkConfig := bark.BarkConfig{
			Logger:              n.config.logger,
			DB:                  db,
			TlsCertFilePath:     n.config.tlsCertFilePath,
			TlsKeyFilePath:      n.config.tlsKeyFilePath,
			TlsClientCAFilePath: n.config.barkClientCAFilePath,
			Host:                barkHost,
			Port:                n.config.barkPort,
			CORSAllowedOrigins:  n.config.corsAllowedOrigins,
			DestinationRegistry: n.destinationRegistry,
		}
		// Mount the DatabaseService only when a snapshot directory is
		// configured — bark.NewBark requires one alongside Lifecycle, and
		// an operator who enabled bark only for its Archive service
		// shouldn't get a DatabaseService that fails on first call.
		if lifecycleEnabled {
			// cfg is never read: SetLiveNode below makes every Service
			// method delegate straight to n's own Restore/Truncate/
			// Snapshot rather than the offline path that would use it.
			dbLifecycleService := dblifecycle.NewService(
				&internalconfig.Config{},
				n.destinationRegistry,
				n.config.logger,
			)
			dbLifecycleService.SetLiveNode(n)
			barkConfig.Lifecycle = dbLifecycleService
			barkConfig.SnapshotDir = n.config.databaseLifecycle.SnapshotDir
			barkConfig.SnapshotCloudDestination = n.config.databaseLifecycle.SnapshotCloudDestination
		}
		var err error
		n.bark, err = bark.NewBark(barkConfig)
		if err != nil {
			return fmt.Errorf("failed to create bark server: %w", err)
		}
		if err := n.bark.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("failed to start bark server: %w", err)
		}
		started = append(started, func() { //nolint:contextcheck
			if err := n.bark.Stop(context.Background()); err != nil {
				n.config.logger.Error(
					"failed to stop bark during cleanup",
					"error",
					err,
				)
			}
		})
	}

	// Configure the Midnight gRPC server (only in API mode with a non-zero
	// port). Port 0 disables the server while leaving the indexer eligible to
	// run.
	if n.config.storageMode.IsAPI() && n.config.midnight.Port > 0 {
		var err error
		n.midnightServer, err = midnightserver.New(
			midnightserver.Config{
				Logger:   n.config.logger,
				Metadata: n.db.Metadata(),
				BlockNumberByHash: func(hash []byte) (uint64, bool, error) {
					block, err := database.BlockByHash(n.db, hash)
					if err != nil {
						if errors.Is(err, models.ErrBlockNotFound) {
							return 0, false, nil
						}
						return 0, false, err
					}
					return block.Number, true, nil
				},
				Host:            n.config.midnight.Host,
				Port:            n.config.midnight.Port,
				TLSCertFilePath: n.config.tlsCertFilePath,
				TLSKeyFilePath:  n.config.tlsKeyFilePath,
				ShutdownTimeout: n.config.shutdownTimeout,
				Database:        midnightserver.NewDatabase(n.db),
				SlotTimer:       n.ledgerState,
				PromRegistry:    n.config.promRegistry,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create midnight gRPC server: %w", err)
		}
		if err := n.midnightServer.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("starting midnight gRPC server: %w", err)
		}
		started = append(started, func() { //nolint:contextcheck
			if err := n.midnightServer.Stop(context.Background()); err != nil {
				n.config.logger.Error(
					"failed to stop midnight gRPC server during cleanup",
					"error",
					err,
				)
			}
		})
	}

	// Resolve Blockfrost API only in API mode with a non-zero configured port.
	blockfrostSelection, blockfrostPort, err := n.apiPluginSelection(
		plugin.CapabilityAPIBlockfrost,
	)
	if err != nil {
		return err
	}
	if n.config.storageMode.IsAPI() && blockfrostPort > 0 {
		adapter, err := blockfrost.NewNodeAdapter(
			n.ledgerState,
			n.mempool,
		)
		if err != nil {
			return fmt.Errorf(
				"creating blockfrost node adapter: %w",
				err,
			)
		}
		err = plugin.ResolveProvider(
			n.ctx, n.pluginHost, plugin.CapabilityAPIBlockfrost,
			blockfrostSelection.Provider, blockfrostSelection.Config,
			blockfrost.ProviderDependencies{
				Node: adapter, Logger: n.config.logger, Host: n.config.bindAddr,
				CORSAllowedOrigins: n.config.corsAllowedOrigins,
			},
		)
		if err != nil {
			return fmt.Errorf("resolve blockfrost API: %w", err)
		}
		started = append(
			started,
			stopPluginCapability(plugin.CapabilityAPIBlockfrost),
		)
	}

	meshSelection, meshPort, err := n.apiPluginSelection(
		plugin.CapabilityAPIMesh,
	)
	if err != nil {
		return err
	}
	if n.config.storageMode.IsAPI() && meshPort > 0 {
		var genesisHash string
		var genesisStartTimeSec int64
		if nc := n.config.cardanoNodeConfig; nc != nil {
			genesisHash = nc.ByronGenesisHash
			if sg := nc.ShelleyGenesis(); sg != nil {
				genesisStartTimeSec = sg.SystemStart.Unix()
			}
		}
		if genesisHash == "" || genesisStartTimeSec == 0 {
			return errors.New(
				"mesh API requires Cardano node config " +
					"(Byron genesis hash and Shelley genesis)",
			)
		}
		err = plugin.ResolveProvider(
			n.ctx, n.pluginHost, plugin.CapabilityAPIMesh,
			meshSelection.Provider, meshSelection.Config,
			mesh.ProviderDependencies{
				Logger:              n.config.logger,
				LedgerState:         n.ledgerState,
				Database:            mesh.NewMeshDatabase(n.db),
				Chain:               n.ledgerState.Chain(),
				Mempool:             n.mempool,
				Host:                n.config.bindAddr,
				Network:             n.config.network,
				NetworkMagic:        n.config.networkMagic,
				GenesisHash:         genesisHash,
				GenesisStartTimeSec: genesisStartTimeSec,
				CORSAllowedOrigins:  n.config.corsAllowedOrigins,
			},
		)
		if err != nil {
			return fmt.Errorf(
				"resolve mesh API: %w",
				err,
			)
		}
		started = append(
			started,
			stopPluginCapability(plugin.CapabilityAPIMesh),
		)
	}

	if n.config.storageMode.IsAPI() {
		fetcher, err := offchainmetadata.New(
			offchainmetadata.Config{
				Logger:                n.config.logger,
				Store:                 n.db.Metadata(),
				HTTPClient:            n.config.offchainMetadata.HTTPClient,
				Interval:              n.config.offchainMetadata.Interval,
				RequestTimeout:        n.config.offchainMetadata.RequestTimeout,
				UserAgent:             n.config.offchainMetadata.UserAgent,
				IPFSGatewayURL:        n.config.offchainMetadata.IPFSGatewayURL,
				BatchSize:             n.config.offchainMetadata.BatchSize,
				MaxBytes:              n.config.offchainMetadata.MaxBytes,
				AllowPrivateAddresses: n.config.offchainMetadata.AllowPrivateAddresses,
			},
		)
		if err != nil {
			return fmt.Errorf("creating off-chain metadata fetcher: %w", err)
		}
		n.offchainMetadataFetcher = fetcher
		if err := n.offchainMetadataFetcher.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("starting off-chain metadata fetcher: %w", err)
		}
		started = append(started, func() { //nolint:contextcheck
			if err := n.offchainMetadataFetcher.Stop(context.Background()); err != nil {
				n.config.logger.Error(
					"failed to stop off-chain metadata fetcher during cleanup",
					"error",
					err,
				)
			}
		})
		if n.config.tokenRegistry.Enabled {
			sync, err := n.newTokenRegistrySync()
			if err != nil {
				return fmt.Errorf("creating token registry sync: %w", err)
			}
			n.tokenRegistrySync = sync
			if err := n.tokenRegistrySync.Start(n.ctx); err != nil { //nolint:contextcheck
				return fmt.Errorf("starting token registry sync: %w", err)
			}
			started = append(started, func() { //nolint:contextcheck
				if err := n.tokenRegistrySync.Stop(context.Background()); err != nil {
					n.config.logger.Error(
						"failed to stop token registry sync during cleanup",
						"error",
						err,
					)
				}
			})
		}
		started = append(started, n.startDeferredIndexMaintenance())
	}

	// Initialize block forger if production mode is enabled
	if n.config.blockProducer {
		creds, err := n.validateBlockProducerStartup()
		if err != nil {
			return fmt.Errorf(
				"block producer startup validation failed: %w",
				err,
			)
		}
		// Cross-check loaded credentials against ledger state. Mismatch
		// against on-chain pool registration is fatal; "not yet
		// registered" is a warning so operators can stage credentials
		// before submitting the registration cert.
		if err := n.validateBlockProducerLedger(creds); err != nil {
			return fmt.Errorf(
				"block producer credentials failed ledger check: %w",
				err,
			)
		}
		//nolint:contextcheck // n.ctx is the node's lifecycle context, correct parent for forger
		if err := n.initBlockForger(n.ctx, creds); err != nil {
			return fmt.Errorf("failed to initialize block forger: %w", err)
		}
		// Enable Leios vote emission when a vote signing key is
		// configured (experimental, leios mode only)
		if err := n.enableLeiosVoting(creds); err != nil {
			return fmt.Errorf("failed to enable leios voting: %w", err)
		}
		// Wire forger's slot tracker into ledger state for slot
		// battle detection. The forger is created after the ledger
		// state, so we use the late-binding setter.
		if n.blockForger != nil {
			n.ledgerState.SetForgedBlockChecker(
				n.blockForger.SlotTracker(),
			)
			n.ledgerState.SetForgingEnabled(true)
			n.ledgerState.SetSlotBattleRecorder(
				n.blockForger,
			)
		}
		started = append(started, func() {
			if n.blockForger != nil {
				n.blockForger.Stop()
			}
			if n.leaderElection != nil {
				logErrIfNotNil(
					n.config.logger,
					"failed to stop leader election during cleanup",
					n.leaderElection.Stop(),
				)
			}
		})
	}

	// All components started successfully
	success = true

	// Only now -- every component above has actually started against
	// n.config.dataDir -- is a pre-restore backup left over from an
	// interrupted live restore swap (reconcileInterruptedLiveRestoreSwap,
	// above) confirmed unneeded; see swapInRestoredDataDir's doc comment
	// on why it must not be removed any earlier than this.
	n.removeConfirmedRestoreBackup()

	// Wait for shutdown signal
	<-n.ctx.Done()
	return nil
}

// logErrIfNotNil logs err at Error level if non-nil, so a cleanup step run
// from the startup failure/shutdown unwind stack doesn't fail silently.
func logErrIfNotNil(logger *slog.Logger, msg string, err error) {
	if err != nil {
		logger.Error(msg, "error", err)
	}
}

// taintValue encodes a taint bit for EnforceNodeSettings. A taint records
// that the database was produced under relaxed conditions; tightening later
// cannot clear it.
func taintValue(relaxed bool) string {
	if relaxed {
		return nodesettings.LatchOn
	}
	return nodesettings.LatchOff
}

// subscribeConnectionEvents wires the connection-manager side of the EventBus:
// recycle requests, connection-closed and inbound-connection delivery to
// ouroboros, and the ledger<->connmanager event translation that keeps ledger/
// from importing connmanager/. Subscriptions are registered before listeners
// start so inbound connections from peers that connect immediately are not
// lost.
func (n *Node) subscribeConnectionEvents() {
	// Subscriber ID captured for the same reason as chainManager's above —
	// n.connManager is rebuilt during a live database restore/truncate.
	n.connManagerRecycleSubId = n.eventBus.SubscribeFunc(
		connmanager.ConnectionRecycleRequestedEventType,
		n.connManager.HandleConnectionRecycleRequestedEvent,
	)
	// Translate ledger-owned recycle events to connmanager recycle events so
	// ledger/ does not import connmanager/.
	n.eventBus.SubscribeFunc(
		ledger.ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
			if !ok {
				return
			}
			n.eventBus.Publish(
				connmanager.ConnectionRecycleRequestedEventType,
				event.NewEvent(
					connmanager.ConnectionRecycleRequestedEventType,
					connmanager.ConnectionRecycleRequestedEvent{
						ConnectionId: e.ConnectionId,
						Reason:       e.Reason,
						// ConnKey is intentionally omitted: HandleConnectionRecycleRequestedEvent
						// closes the connection by ConnectionId alone and never reads ConnKey.
					},
				),
			)
		},
	)
	// Subscribe to connection events BEFORE starting listeners so that
	// inbound connections from peers that connect immediately are not lost.
	//
	// These are closures rather than method values because this runs before
	// n.ouroboros is constructed; each resolves it when the event fires,
	// which cannot happen until the listeners below are open.
	n.eventBus.SubscribeFunc(
		connmanager.ConnectionClosedEventType,
		func(evt event.Event) { n.ouroboros().HandleConnClosedEvent(evt) },
	)
	// Translate connmanager connection-closed events to ledger-owned events so
	// ledger/ does not import connmanager/.
	n.eventBus.SubscribeFunc(
		connmanager.ConnectionClosedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(connmanager.ConnectionClosedEvent)
			if !ok {
				return
			}
			n.eventBus.Publish(
				ledger.ConnectionClosedEventType,
				event.NewEvent(
					ledger.ConnectionClosedEventType,
					ledger.ConnectionClosedEvent{
						ConnectionId: e.ConnectionId,
						Error:        e.Error,
					},
				),
			)
		},
	)
	n.eventBus.SubscribeFunc(
		connmanager.InboundConnectionEventType,
		func(evt event.Event) { n.ouroboros().HandleInboundConnEvent(evt) },
	)
}

// subscribeChainSelectorEvents wires the EventBus subscriptions that feed the
// chain selector: peer tip/activity observations, chain switch and
// selected-to-none transitions, fork monitoring, connection teardown, and the
// peer-governance eligibility/priority forwarding. The peergov forwarding lives
// here in the node composition layer so chainselection/ keeps no dependency on
// peergov/.
func (n *Node) subscribeChainSelectorEvents() {
	// Subscribe chain selector to peer tip update events
	n.eventBus.SubscribeFunc(
		chainselection.PeerTipUpdateEventType,
		n.chainSelector.HandlePeerTipUpdateEvent,
	)
	n.eventBus.SubscribeFunc(
		chainselection.PeerTipUpdateEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(chainselection.PeerTipUpdateEvent)
			if !ok || n.peerGov == nil {
				return
			}
			n.peerGov.TouchPeerByConnId(e.ConnectionId)
		},
	)
	n.eventBus.SubscribeFunc(
		chainselection.PeerActivityEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(chainselection.PeerActivityEvent)
			if !ok {
				return
			}
			n.chainSelector.TouchPeerActivity(e.ConnectionId)
			if n.peerGov != nil {
				n.peerGov.TouchPeerByConnId(e.ConnectionId)
			}
		},
	)
	// Subscribe to chain switch events to update active connection
	n.eventBus.SubscribeFunc(
		chainselection.ChainSwitchEventType,
		n.handleChainSwitchEvent,
	)
	// Subscribe to selected-to-none transitions (selection stalled, e.g. an
	// uncorroborated Genesis fast source). Enforcement that the stalled source
	// stops feeding the ledger is handled by the ChainsyncApplyEligible gate;
	// this handler surfaces the stall for observability.
	n.eventBus.SubscribeFunc(
		chainselection.ChainSelectedNoneEventType,
		n.handleChainSelectedNoneEvent,
	)
	// Subscribe to chain fork events for monitoring
	n.eventBus.SubscribeFunc(
		chain.ChainForkEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(chain.ChainForkEvent)
			if !ok {
				return
			}
			n.config.logger.Warn(
				"chain fork detected",
				"fork_point_slot", e.ForkPoint.Slot,
				"fork_depth", e.ForkDepth,
				"alternate_head_slot", e.AlternateHead.Slot,
				"canonical_head_slot", e.CanonicalHead.Slot,
			)
		},
	)
	// Subscribe to connection closed events to remove peers from chain selector
	n.eventBus.SubscribeFunc(
		connmanager.ConnectionClosedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(connmanager.ConnectionClosedEvent)
			if !ok {
				return
			}
			n.chainSelector.RemovePeer(e.ConnectionId)
			n.deleteChainsyncIngressEligibility(e.ConnectionId)
		},
	)
	// Forward peer-governance eligibility and priority updates to the chain
	// selector. Subscription is placed here (node composition layer) so that
	// chainselection/ has no dependency on peergov/.
	n.eventBus.SubscribeFunc(
		peergov.PeerEligibilityChangedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(peergov.PeerEligibilityChangedEvent)
			if !ok {
				return
			}
			n.chainSelector.SetConnectionEligible(e.ConnectionId, e.Eligible)
		},
	)
	n.eventBus.SubscribeFunc(
		peergov.PeerPriorityChangedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(peergov.PeerPriorityChangedEvent)
			if !ok {
				return
			}
			n.chainSelector.SetConnectionPriority(e.ConnectionId, e.Priority)
		},
	)
}

// nodeSettingsGateValues assembles the phase 2 gate values -- the era
// genesis hashes and the ledger-semantics gates -- from n.config, for
// EnforceNodeSettings. It is called from two sites in Run: once for the
// normal startup path, and once for the deferred pass that runs
// immediately after RecoverCommitTimestampConflict when recovery was
// needed. Factored out so both sites build the same map from a single
// definition rather than two copies that could drift.
func (n *Node) nodeSettingsGateValues() nodesettings.Values {
	gateValues := nodesettings.Values{
		// The two validation taints live here, not in phase 1. Only full
		// node startup knows these settings; a bool has no "unknown"
		// sentinel, and the partial database.Config callers would
		// otherwise compute a relaxed taint of "on" from a zero value and
		// fail against every normally-created database.
		"historical_validation_relaxed": taintValue(
			!n.config.validateHistorical,
		),
		"strict_utxo_validation_relaxed": taintValue(
			!n.config.strictUtxoValidation,
		),
		"history_expiry_active": nodesettings.EncodeLatchBool(
			n.config.historyExpiry.Enabled, "",
		),
		"pledge_leverage": nodesettings.EncodeLatchBool(
			n.config.pledgeLeverageEnabled,
			strconv.FormatUint(uint64(n.config.pledgeLeverage), 10),
		),
		"full_pot_rewards": nodesettings.EncodeLatchBool(
			n.config.fullPotRewardsEnabled, "",
		),
		"delegator_inactivity": nodesettings.EncodeLatchBool(
			n.config.delegatorInactivityEnabled,
			strconv.FormatUint(n.config.delegatorInactivity, 10),
		),
		"min_pool_margin": nodesettings.EncodeLatchBool(
			n.config.minPoolMargin != 0,
			strconv.FormatUint(uint64(n.config.minPoolMargin), 10),
		),
	}
	if nodeCfg := n.config.CardanoNodeConfig(); nodeCfg != nil {
		gateValues["byron_genesis_hash"] = nodeCfg.ByronGenesisHash
		gateValues["shelley_genesis_hash"] = nodeCfg.ShelleyGenesisHash
		gateValues["alonzo_genesis_hash"] = nodeCfg.AlonzoGenesisHash
		gateValues["conway_genesis_hash"] = nodeCfg.ConwayGenesisHash
		gateValues["dijkstra_genesis_hash"] = nodeCfg.DijkstraGenesisHash
	}
	return gateValues
}

// backfillRewardLiveStake repairs databases created before the live reward
// stake aggregate existed. It runs after commit-timestamp recovery and before
// ledger processing can advance the chain, so the next epoch-boundary snapshot
// cannot observe a partially populated aggregate.
func (n *Node) backfillRewardLiveStake() error {
	return n.db.MetadataTxn(true).Do(func(txn *database.Txn) error {
		needed, err := n.db.Metadata().RewardLiveStakeNeedsBackfill(
			txn.Metadata(),
		)
		if err != nil {
			return fmt.Errorf("check reward live stake backfill: %w", err)
		}
		staleSnapshots, err := n.db.Metadata().
			StaleConsensusStakeSnapshotsExist(
				txn.Metadata(),
			)
		if err != nil {
			return fmt.Errorf("check stake snapshot provenance: %w", err)
		}
		if needed {
			tip, err := n.db.GetTip(txn)
			if err != nil {
				return fmt.Errorf(
					"get tip for reward live stake backfill: %w",
					err,
				)
			}
			n.config.logger.Info(
				"rebuilding reward live stake aggregate",
				"slot", tip.Point.Slot,
			)
			if err := n.db.RebuildRewardLiveStake(tip.Point.Slot, txn); err != nil {
				return fmt.Errorf("backfill reward live stake: %w", err)
			}
		}
		if staleSnapshots {
			return errors.New(
				"consensus stake snapshots were produced by an older accounting " +
					"version and cannot be safely reconstructed from this database; " +
					"rebootstrap from immutable blocks or a trusted snapshot",
			)
		}
		return nil
	})
}

// startDeferredIndexMaintenance finishes lazy deferred-index rebuilds
// using the node's already-open database handle. The critical subset is
// rebuilt before API readiness, so this background repair can restore
// secondary query indexes and clear the pending marker without opening a
// second Badger handle during serve startup.
func (n *Node) startDeferredIndexMaintenance() func() {
	manager, ok := n.db.Metadata().(metadata.DeferredIndexManager)
	if !ok {
		return func() {}
	}
	pending, err := manager.HasDeferredIndexesPending()
	if err != nil {
		n.config.logger.Error(
			"checking deferred-index maintenance state failed",
			"error", err,
		)
		return func() {}
	}
	if !pending {
		n.config.logger.Info("deferred-index maintenance not needed")
		return func() {}
	}
	done := make(chan struct{})
	n.deferredIndexMaintenanceDone = done
	n.config.logger.Info("deferred-index maintenance starting")
	go func() {
		defer close(done)
		if err := manager.BuildDeferredIndexes(); err != nil {
			n.config.logger.Error(
				"deferred-index maintenance failed",
				"error", err,
			)
			return
		}
		n.config.logger.Info("deferred-index maintenance complete")
	}()
	return func() {
		timeout := n.configuredShutdownTimeout()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			n.config.logger.Warn(
				"timed out waiting for deferred-index maintenance; continuing cleanup",
				"timeout",
				timeout,
			)
		}
	}
}

// newTokenRegistrySync builds the API-mode CIP-26 token registry sync from
// node config. Both the startup path and the live storage-restart path use it
// so the two cannot drift in how they map configuration onto the syncer.
func (n *Node) newTokenRegistrySync() (
	*offchainmetadata.TokenRegistrySync,
	error,
) {
	return offchainmetadata.NewTokenRegistrySync(
		offchainmetadata.TokenRegistryConfig{
			Logger:                n.config.logger,
			Store:                 n.db.Metadata(),
			HTTPClient:            n.config.tokenRegistry.HTTPClient,
			SourceURL:             n.config.tokenRegistry.SourceURL,
			Network:               n.config.network,
			UserAgent:             n.config.tokenRegistry.UserAgent,
			Interval:              n.config.tokenRegistry.Interval,
			RequestTimeout:        n.config.tokenRegistry.RequestTimeout,
			MaxBytes:              n.config.tokenRegistry.MaxBytes,
			MaxEntryBytes:         n.config.tokenRegistry.MaxEntryBytes,
			StoreLogos:            n.config.tokenRegistry.StoreLogos,
			AllowPrivateAddresses: n.config.tokenRegistry.AllowPrivateAddresses,
		},
	)
}
