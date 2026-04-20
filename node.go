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
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/bark"
	"github.com/blinklabs-io/dingo/blockfrost"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/mesh"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/utxorpc"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

type Node struct {
	connManager                      *connmanager.ConnectionManager
	peerGov                          *peergov.PeerGovernor
	chainsyncState                   *chainsync.State
	chainSelector                    *chainselection.ChainSelector
	eventBus                         *event.EventBus
	mempool                          *mempool.Mempool
	chainManager                     *chain.ChainManager
	db                               *database.Database
	ledgerState                      *ledger.LedgerState
	snapshotMgr                      *snapshot.Manager
	utxorpc                          *utxorpc.Utxorpc
	bark                             *bark.Bark
	barkPruner                       *bark.Pruner
	blockfrostAPI                    *blockfrost.Blockfrost
	meshAPI                          *mesh.Server
	ouroboros                        *ouroborosPkg.Ouroboros
	blockForger                      *forging.BlockForger
	leaderElection                   *leader.Election
	rtsMetrics                       *rtsMetrics
	shutdownFuncs                    []func(context.Context) error
	config                           Config
	ctx                              context.Context
	cancel                           context.CancelFunc
	shutdownOnce                     sync.Once
	chainsyncIngressEligibilityMu    sync.RWMutex
	chainsyncIngressEligibilityCache map[ouroboros.ConnectionId]bool
}

func New(cfg Config) (*Node, error) {
	n := &Node{
		config: cfg,
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
	n.eventBus = event.NewEventBus(n.config.promRegistry, n.config.logger)
	if err := n.configValidate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return n, nil
}

//nolint:contextcheck // Run is the lifecycle boundary and derives n.ctx from the caller context.
func (n *Node) Run(ctx context.Context) error {
	// Configure tracing
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
	success := false
	defer func() {
		r := recover()
		if r != nil {
			if n.cancel != nil {
				n.cancel()
			}
			// Cleanup on panic, then re-panic
			for i := len(started) - 1; i >= 0; i-- {
				started[i]()
			}
			panic(r)
		} else if !success {
			if n.cancel != nil {
				n.cancel()
			}
			// Cleanup on failure (non-panic)
			for i := len(started) - 1; i >= 0; i-- {
				started[i]()
			}
		}
	}()

	// Register eventBus cleanup (created in New(), has background goroutines)
	started = append(started, func() { n.eventBus.Stop() })

	// Load database
	dbNeedsRecovery := false
	dbConfig := &database.Config{
		DataDir:        n.config.dataDir,
		Logger:         n.config.logger,
		PromRegistry:   n.config.promRegistry,
		BlobPlugin:     n.config.blobPlugin,
		RunMode:        n.config.runMode,
		MetadataPlugin: n.config.metadataPlugin,
		MaxConnections: n.config.DatabaseWorkerPoolConfig.WorkerPoolSize,
		StorageMode:    string(n.config.storageMode),
		Network:        n.config.network,
		CacheConfig: database.CborCacheConfig{
			BlockLRUEntries: n.config.cacheBlockLRUEntries,
			HotUtxoEntries:  n.config.cacheHotUtxoEntries,
			HotTxEntries:    n.config.cacheHotTxEntries,
			HotTxMaxBytes:   n.config.cacheHotTxMaxBytes,
		},
	}
	db, err := database.New(dbConfig)
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
	// Initialize Ouroboros
	n.ouroboros = ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger:                   n.config.logger,
		EventBus:                 n.eventBus,
		ConnManager:              n.connManager,
		NetworkMagic:             n.config.networkMagic,
		PeerSharing:              n.config.peerSharing,
		IntersectTip:             n.config.intersectTip,
		IntersectPoints:          n.config.intersectPoints,
		PromRegistry:             n.config.promRegistry,
		EnableLeios:              n.config.runMode == runModeLeios,
		ChainsyncIngressEligible: n.isChainsyncIngressEligible,
	})
	// Load state
	state, err := ledger.NewLedgerState(
		ledger.LedgerStateConfig{
			ChainManager:               n.chainManager,
			Database:                   n.db,
			EventBus:                   n.eventBus,
			Logger:                     n.config.logger,
			CardanoNodeConfig:          n.config.cardanoNodeConfig,
			PromRegistry:               n.config.promRegistry,
			ForgeBlocks:                n.config.isDevMode(),
			ValidateHistorical:         n.config.validateHistorical,
			BlockfetchRequestRangeFunc: n.ouroboros.BlockfetchClientRequestRange,
			DatabaseWorkerPoolConfig:   n.config.DatabaseWorkerPoolConfig,
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
				return n.chainsyncState.LookupObservedHeader(connId, hash)
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
	n.ouroboros.LedgerState = n.ledgerState
	if err := n.chainManager.SetLedger(n.ledgerState); err != nil {
		return fmt.Errorf("failed to configure chain security parameter: %w", err)
	}

	if n.config.barkBaseUrl != "" {
		n.db.SetBlobStore(bark.NewBarkBlobStore(bark.BlobStoreBarkConfig{
			BaseUrl:        n.config.barkBaseUrl,
			SecurityWindow: n.config.barkSecurityWindow,
			HTTPClient: &http.Client{
				Timeout: 30 * time.Second,
			},
			LedgerState: state,
			Logger:      n.config.logger,
		}, n.db.Blob()))

		n.barkPruner = bark.NewPruner(bark.PrunerConfig{
			LedgerState:    state,
			DB:             n.db,
			Logger:         n.config.logger,
			SecurityWindow: n.config.barkSecurityWindow,
			Frequency:      time.Hour,
		})

		if err := n.barkPruner.Start(n.ctx); err != nil {
			return fmt.Errorf("failed to start pruner: %w", err)
		}

		started = append(started, func() {
			_ = n.barkPruner.Stop(context.Background())
		})
	}

	// Run DB recovery if needed
	if dbNeedsRecovery {
		if err := n.ledgerState.RecoverCommitTimestampConflict(); err != nil {
			return fmt.Errorf("failed to recover database: %w", err)
		}
	}
	// Start ledger
	if err := n.ledgerState.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to start ledger: %w", err)
	}
	started = append(started, func() { n.ledgerState.Close() })
	// Initialize and start snapshot manager for stake snapshot capture
	n.snapshotMgr = snapshot.NewManager(
		n.db,
		n.eventBus,
		n.config.logger,
	)
	// Capture genesis stake snapshot (epoch 0) so leader election works at epoch 2
	if err := n.snapshotMgr.CaptureGenesisSnapshot(ctx); err != nil {
		n.config.logger.Warn(
			"failed to capture genesis snapshot",
			"error", err,
		)
	}
	if err := n.snapshotMgr.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to start snapshot manager: %w", err)
	}
	started = append(started, func() { _ = n.snapshotMgr.Stop() })
	// Initialize mempool
	n.mempool = mempool.NewMempool(mempool.MempoolConfig{
		MempoolCapacity:    n.config.mempoolCapacity,
		EvictionWatermark:  n.config.evictionWatermark,
		RejectionWatermark: n.config.rejectionWatermark,
		Logger:             n.config.logger,
		EventBus:           n.eventBus,
		PromRegistry:       n.config.promRegistry,
		Validator:          n.ledgerState,
		CurrentSlotFunc:    n.ledgerState.CurrentOrTipSlot,
	},
	)
	started = append(started, func() { //nolint:contextcheck
		if err := n.mempool.Stop(context.Background()); err != nil {
			n.config.logger.Error(
				"failed to stop mempool during cleanup",
				"error",
				err,
			)
		}
	})
	// Set mempool in ledger state for block forging
	n.ledgerState.SetMempool(n.mempool)
	n.ouroboros.Mempool = n.mempool
	// Initialize chainsync state with multi-client configuration
	chainsyncCfg := chainsync.DefaultConfig()
	if n.config.chainsyncMaxClients > 0 {
		chainsyncCfg.MaxClients = n.config.chainsyncMaxClients
	}
	if n.config.chainsyncStallTimeout > 0 {
		chainsyncCfg.StallTimeout = n.config.chainsyncStallTimeout
	}
	n.chainsyncState = chainsync.NewStateWithConfig(
		n.eventBus,
		n.ledgerState,
		chainsyncCfg,
	)
	n.ouroboros.ChainsyncState = n.chainsyncState
	n.eventBus.SubscribeFunc(
		peergov.PeerEligibilityChangedEventType,
		n.handlePeerEligibilityChangedEvent,
	)
	n.eventBus.SubscribeFunc(
		peergov.PeerEligibilityChangedEventType,
		n.ouroboros.HandlePeerEligibilityChangedEvent,
	)
	n.eventBus.SubscribeFunc(
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
			Logger:             n.config.logger,
			EventBus:           n.eventBus,
			SecurityParam:      chainSelectorSecurityParam,
			GenesisMode:        genesisSelectionMode,
			GenesisWindowSlots: genesisWindowSlots,
			ConnectionLive: func(connId ouroboros.ConnectionId) bool {
				return n.connManager != nil &&
					n.connManager.GetConnectionById(connId) != nil
			},
		},
	)
	if genesisSelectionMode {
		n.config.logger.Info(
			"Genesis chain selection enabled",
			"genesis_window_slots", genesisWindowSlots,
			"security_param", chainSelectorSecurityParam,
		)
	}
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
	// Start the chain selector
	if err := n.chainSelector.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("failed to start chain selector: %w", err)
	}
	started = append(started, func() { n.chainSelector.Stop() })
	// Configure connection manager
	tmpListeners := n.ouroboros.ConfigureListeners(n.config.listeners)
	n.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:              n.config.logger,
			EventBus:            n.eventBus,
			Listeners:           tmpListeners,
			OutboundSourcePort:  n.config.outboundSourcePort,
			OutboundConnOpts:    n.ouroboros.OutboundConnOpts(),
			PromRegistry:        n.config.promRegistry,
			MaxConnectionsPerIP: n.config.maxConnectionsPerIP,
			MaxInboundConns:     n.config.maxInboundConns,
		},
	)
	n.eventBus.SubscribeFunc(
		connmanager.ConnectionRecycleRequestedEventType,
		n.connManager.HandleConnectionRecycleRequestedEvent,
	)
	n.ouroboros.ConnManager = n.connManager
	// Subscribe ouroboros to chainsync resync events from the
	// ledger. This replaces the previous ChainsyncResyncFunc
	// closure so all stop/restart orchestration lives in the
	// ouroboros/chainsync component. Registered after ConnManager
	// is wired so the handler can look up connections.
	n.ouroboros.SubscribeChainsyncResync(n.ctx) //nolint:contextcheck
	// Subscribe to connection events BEFORE starting listeners so that
	// inbound connections from peers that connect immediately are not lost.
	n.eventBus.SubscribeFunc(
		connmanager.ConnectionClosedEventType,
		n.ouroboros.HandleConnClosedEvent,
	)
	n.eventBus.SubscribeFunc(
		connmanager.InboundConnectionEventType,
		n.ouroboros.HandleInboundConnEvent,
	)
	// Configure peer governor before opening listeners so topology-driven
	// outbound connections start first and do not lose the race to inbounds.
	// Create ledger peer provider for discovering peers from stake pool relays.
	ledgerPeerProvider, err := ledger.NewLedgerPeerProvider(
		n.ledgerState,
		n.db,
		n.eventBus,
	)
	if err != nil {
		return fmt.Errorf("failed to create ledger peer provider: %w", err)
	}

	// Get UseLedgerAfterSlot from topology config (defaults to -1 = disabled).
	var useLedgerAfterSlot int64 = -1
	if n.config.topologyConfig != nil {
		useLedgerAfterSlot = n.config.topologyConfig.UseLedgerAfterSlot
	}

	n.peerGov = peergov.NewPeerGovernor(
		peergov.PeerGovernorConfig{
			Logger:                               n.config.logger,
			EventBus:                             n.eventBus,
			ConnManager:                          n.connManager,
			DisableOutbound:                      n.config.isDevMode(),
			PromRegistry:                         n.config.promRegistry,
			PeerRequestFunc:                      n.ouroboros.RequestPeersFromPeer,
			LedgerPeerProvider:                   ledgerPeerProvider,
			UseLedgerAfterSlot:                   useLedgerAfterSlot,
			LedgerPeerTarget:                     n.config.ledgerPeerTarget,
			TargetNumberOfKnownPeers:             n.config.targetNumberOfKnownPeers,
			TargetNumberOfEstablishedPeers:       n.config.targetNumberOfEstablishedPeers,
			TargetNumberOfActivePeers:            n.config.targetNumberOfActivePeers,
			ActivePeersTopologyQuota:             n.config.activePeersTopologyQuota,
			ActivePeersGossipQuota:               n.config.activePeersGossipQuota,
			ActivePeersLedgerQuota:               n.config.activePeersLedgerQuota,
			MinHotPeers:                          n.config.minHotPeers,
			ReconcileInterval:                    n.config.reconcileInterval,
			InactivityTimeout:                    n.config.inactivityTimeout,
			SyncProgressProvider:                 n.ledgerState,
			BootstrapPromotionMinDiversityGroups: n.config.bootstrapPromotionMinDiversityGroups,
		},
	)
	n.ouroboros.PeerGov = n.peerGov
	n.eventBus.SubscribeFunc(
		peergov.OutboundConnectionEventType,
		n.ouroboros.HandleOutboundConnEvent,
	)
	if n.config.topologyConfig != nil {
		n.peerGov.LoadTopologyConfig(n.config.topologyConfig)
	}
	if err := n.peerGov.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("peer governor start failed: %w", err)
	}
	started = append(started, func() { n.peerGov.Stop() })
	// Start listeners
	if err := n.connManager.Start(n.ctx); err != nil { //nolint:contextcheck
		return err
	}
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
	// Use a grace period + cooldown to avoid flapping healthy but quiet peers.
	stallCheckInterval := min(
		max(chainsyncCfg.StallTimeout/2, 10*time.Second),
		30*time.Second,
	)
	stallRecoveryGrace := max(chainsyncCfg.StallTimeout, 30*time.Second)
	stallRecycleCooldown := max(2*chainsyncCfg.StallTimeout, 2*time.Minute)
	recyclerCtx, recyclerCancel := context.WithCancel(n.ctx) //nolint:gosec // G118: cancel func stored in started slice
	started = append(started, recyclerCancel)
	go func(interval, grace, cooldown time.Duration) {
		for {
			if recyclerCtx.Err() != nil {
				return
			}
			if !n.runStallCheckerLoop(func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				recycleAt := make(map[string]time.Time)
				lastRecycled := make(map[string]time.Time)
				lastProgressSlot := n.ledgerState.Tip().Point.Slot
				lastProgressAt := time.Now()
				plateauRecoveryThreshold := plateauThreshold(
					chainsyncCfg.StallTimeout,
				)
				for {
					select {
					case <-recyclerCtx.Done():
						return
					case <-ticker.C:
						n.runStallCheckerTick(func() {
							now := time.Now()
							localTip := n.ledgerState.Tip()
							localTipSlot := localTip.Point.Slot
							if n.chainSelector != nil {
								n.chainSelector.SetLocalTip(localTip)
								if k := n.ledgerState.SecurityParam(); k > 0 {
									n.chainSelector.SetSecurityParam(uint64(k)) //nolint:gosec
								}
							}
							n.processChainsyncRecyclerTick(
								now,
								localTipSlot,
								chainsyncCfg,
								recycleAt,
								lastRecycled,
								&lastProgressSlot,
								&lastProgressAt,
								plateauRecoveryThreshold,
								grace,
								cooldown,
							)
						})
					}
				}
			}) {
				return
			}
			select {
			case <-recyclerCtx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}(stallCheckInterval, stallRecoveryGrace, stallRecycleCooldown)
	// Configure UTxO RPC (only in API mode with a non-zero port)
	if n.config.storageMode.IsAPI() && n.config.utxorpcPort > 0 {
		n.utxorpc = utxorpc.NewUtxorpc(
			utxorpc.UtxorpcConfig{
				Logger:          n.config.logger,
				EventBus:        n.eventBus,
				LedgerState:     n.ledgerState,
				Mempool:         n.mempool,
				Host:            n.config.bindAddr,
				Port:            n.config.utxorpcPort,
				TlsCertFilePath: n.config.tlsCertFilePath,
				TlsKeyFilePath:  n.config.tlsKeyFilePath,
			},
		)
		if err := n.utxorpc.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("starting utxorpc: %w", err)
		}
		started = append(started, func() { //nolint:contextcheck
			if err := n.utxorpc.Stop(context.Background()); err != nil {
				n.config.logger.Error(
					"failed to stop utxorpc during cleanup",
					"error",
					err,
				)
			}
		})
	}

	if n.config.barkPort > 0 {
		n.bark = bark.NewBark(
			bark.BarkConfig{
				Logger:          n.config.logger,
				DB:              db,
				TlsCertFilePath: n.config.tlsCertFilePath,
				TlsKeyFilePath:  n.config.tlsKeyFilePath,
				Port:            n.config.barkPort,
			},
		)
		if err := n.bark.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("failed to start bark server: %w", err)
		}
		started = append(started, func() { //nolint:contextcheck
			if err := n.bark.Stop(context.Background()); err != nil {
				n.config.logger.Error("failed to stop bark during cleanup", "error", err)
			}
		})
	}

	// Configure Blockfrost API (only in API mode with a non-zero port)
	if n.config.storageMode.IsAPI() && n.config.blockfrostPort > 0 {
		listenAddr := net.JoinHostPort(
			n.config.bindAddr,
			strconv.FormatUint(uint64(n.config.blockfrostPort), 10),
		)
		adapter, err := blockfrost.NewNodeAdapter(
			n.ledgerState,
		)
		if err != nil {
			return fmt.Errorf(
				"creating blockfrost node adapter: %w",
				err,
			)
		}
		n.blockfrostAPI = blockfrost.New(
			blockfrost.BlockfrostConfig{
				ListenAddress: listenAddr,
			},
			adapter,
			n.config.logger,
		)
		if err := n.blockfrostAPI.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("starting blockfrost API: %w", err)
		}
		started = append(started, func() { //nolint:contextcheck
			if err := n.blockfrostAPI.Stop(context.Background()); err != nil {
				n.config.logger.Error(
					"failed to stop blockfrost API during cleanup",
					"error",
					err,
				)
			}
		})
	}

	// Configure Mesh API (only in API mode with a non-zero port)
	if n.config.storageMode.IsAPI() && n.config.meshPort > 0 {
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
		listenAddr := net.JoinHostPort(
			n.config.bindAddr,
			strconv.FormatUint(uint64(n.config.meshPort), 10),
		)
		var meshErr error
		n.meshAPI, meshErr = mesh.NewServer(
			mesh.ServerConfig{
				Logger:              n.config.logger,
				EventBus:            n.eventBus,
				LedgerState:         n.ledgerState,
				Database:            n.db,
				Chain:               n.ledgerState.Chain(),
				Mempool:             n.mempool,
				ListenAddress:       listenAddr,
				Network:             n.config.network,
				NetworkMagic:        n.config.networkMagic,
				GenesisHash:         genesisHash,
				GenesisStartTimeSec: genesisStartTimeSec,
			},
		)
		if meshErr != nil {
			return fmt.Errorf(
				"create mesh API server: %w",
				meshErr,
			)
		}
		if err := n.meshAPI.Start(n.ctx); err != nil { //nolint:contextcheck
			return fmt.Errorf("starting mesh API: %w", err)
		}
		started = append(started, func() { //nolint:contextcheck
			if err := n.meshAPI.Stop(context.Background()); err != nil {
				n.config.logger.Error(
					"failed to stop mesh API during cleanup",
					"error",
					err,
				)
			}
		})
	}

	// Initialize block forger if production mode is enabled
	if n.config.blockProducer {
		creds, err := n.validateBlockProducerStartup()
		if err != nil {
			return fmt.Errorf("block producer startup validation failed: %w", err)
		}
		//nolint:contextcheck // n.ctx is the node's lifecycle context, correct parent for forger
		if err := n.initBlockForger(n.ctx, creds); err != nil {
			return fmt.Errorf("failed to initialize block forger: %w", err)
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
				_ = n.leaderElection.Stop()
			}
		})
	}

	// All components started successfully
	success = true

	// Wait for shutdown signal
	<-n.ctx.Done()
	return nil
}
