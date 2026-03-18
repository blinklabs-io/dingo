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
	"net"
	"net/http"
	"runtime/debug"
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
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	blockfrostAPI                    *blockfrost.Blockfrost
	meshAPI                          *mesh.Server
	ouroboros                        *ouroborosPkg.Ouroboros
	blockForger                      *forging.BlockForger
	leaderElection                   *leader.Election
	shutdownFuncs                    []func(context.Context) error
	config                           Config
	ctx                              context.Context
	cancel                           context.CancelFunc
	shutdownOnce                     sync.Once
	chainsyncIngressEligibilityMu    sync.RWMutex
	chainsyncIngressEligibilityCache map[ouroboros.ConnectionId]bool
}

func plateauThreshold(stallTimeout time.Duration) time.Duration {
	return max(2*stallTimeout, 4*time.Minute)
}

func (n *Node) isChainsyncIngressEligible(
	connId ouroboros.ConnectionId,
) bool {
	n.chainsyncIngressEligibilityMu.RLock()
	defer n.chainsyncIngressEligibilityMu.RUnlock()
	if n.chainsyncIngressEligibilityCache == nil {
		return false
	}
	eligible, ok := n.chainsyncIngressEligibilityCache[connId]
	if !ok {
		return false
	}
	return eligible
}

func (n *Node) setChainsyncIngressEligibility(
	connId ouroboros.ConnectionId,
	eligible bool,
) {
	n.chainsyncIngressEligibilityMu.Lock()
	defer n.chainsyncIngressEligibilityMu.Unlock()
	if n.chainsyncIngressEligibilityCache == nil {
		n.chainsyncIngressEligibilityCache = make(
			map[ouroboros.ConnectionId]bool,
		)
	}
	n.chainsyncIngressEligibilityCache[connId] = eligible
}

func (n *Node) deleteChainsyncIngressEligibility(
	connId ouroboros.ConnectionId,
) {
	n.chainsyncIngressEligibilityMu.Lock()
	defer n.chainsyncIngressEligibilityMu.Unlock()
	if n.chainsyncIngressEligibilityCache == nil {
		return
	}
	delete(n.chainsyncIngressEligibilityCache, connId)
}

func (n *Node) handlePeerEligibilityChangedEvent(evt event.Event) {
	e, ok := evt.Data.(peergov.PeerEligibilityChangedEvent)
	if !ok {
		return
	}
	n.setChainsyncIngressEligibility(e.ConnectionId, e.Eligible)
}

func shouldRecycleLocalTipPlateau(
	now time.Time,
	lastProgressAt time.Time,
	localTipSlot uint64,
	bestPeerTipSlot uint64,
	lastRecycledAt *time.Time,
	cooldown time.Duration,
	threshold time.Duration,
) bool {
	if bestPeerTipSlot <= localTipSlot {
		return false
	}
	if now.Sub(lastProgressAt) <= threshold {
		return false
	}
	if lastRecycledAt != nil && now.Sub(*lastRecycledAt) < cooldown {
		return false
	}
	return true
}

func (n *Node) processChainsyncRecyclerTick(
	now time.Time,
	localTipSlot uint64,
	chainsyncCfg chainsync.Config,
	recycleAt map[string]time.Time,
	lastRecycled map[string]time.Time,
	lastProgressSlot *uint64,
	lastProgressAt *time.Time,
	plateauRecoveryThreshold time.Duration,
	grace time.Duration,
	cooldown time.Duration,
) {
	if localTipSlot > *lastProgressSlot {
		*lastProgressSlot = localTipSlot
		*lastProgressAt = now
	}
	n.chainsyncState.CheckStalledClients()
	trackedClients := n.chainsyncState.GetTrackedClients()
	trackedByID := make(
		map[string]chainsync.TrackedClient,
		len(trackedClients),
	)
	for _, conn := range trackedClients {
		connKey := conn.ConnId.String()
		trackedByID[connKey] = conn
		if conn.Status != chainsync.ClientStatusStalled {
			delete(recycleAt, connKey)
		}
	}
	// Prune expired cooldown entries so this map does
	// not grow without bound over long runtimes.
	for connKey, last := range lastRecycled {
		if now.Sub(last) >= cooldown {
			delete(lastRecycled, connKey)
		}
	}
	// Safety net: if local tip has not moved for a long time
	// while peers are ahead, recycle the selected chainsync
	// connection even if it is not marked stalled.
	if n.chainSelector != nil {
		if bestPeer := n.chainSelector.GetBestPeer(); bestPeer != nil {
			if bestPeerTip := n.chainSelector.GetPeerTip(*bestPeer); bestPeerTip != nil &&
				bestPeerTip.Tip.Point.Slot > localTipSlot {
				targetConn := n.chainsyncState.GetClientConnId()
				if targetConn == nil {
					targetCopy := *bestPeer
					targetConn = &targetCopy
				}
				connKey := targetConn.String()
				var lastRecycledAt *time.Time
				if last, ok := lastRecycled[connKey]; ok {
					lastCopy := last
					lastRecycledAt = &lastCopy
				}
				if shouldRecycleLocalTipPlateau(
					now,
					*lastProgressAt,
					localTipSlot,
					bestPeerTip.Tip.Point.Slot,
					lastRecycledAt,
					cooldown,
					plateauRecoveryThreshold,
				) {
					n.config.logger.Warn(
						"local tip plateau detected, recycling chainsync connection",
						"connection_id", connKey,
						"local_tip_slot", localTipSlot,
						"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
						"plateau_duration", now.Sub(*lastProgressAt),
					)
					n.eventBus.PublishAsync(
						connmanager.ConnectionRecycleRequestedEventType,
						event.NewEvent(
							connmanager.ConnectionRecycleRequestedEventType,
							connmanager.ConnectionRecycleRequestedEvent{
								ConnectionId: *targetConn,
								ConnKey:      connKey,
								Reason:       "local_tip_plateau",
							},
						),
					)
					delete(recycleAt, connKey)
					lastRecycled[connKey] = now
					*lastProgressAt = now
				}
			}
		}
	}
	for _, conn := range trackedClients {
		if conn.Status != chainsync.ClientStatusStalled {
			continue
		}
		connKey := conn.ConnId.String()
		if _, exists := recycleAt[connKey]; !exists {
			recycleAt[connKey] = now.Add(grace)
			n.config.logger.Info(
				"chainsync client stalled, scheduling guarded recycle",
				"connection_id", connKey,
				"stall_timeout", chainsyncCfg.StallTimeout,
				"grace_period", grace,
			)
		}
	}
	for connKey, dueAt := range recycleAt {
		if now.Before(dueAt) {
			continue
		}
		tracked, ok := trackedByID[connKey]
		if !ok || tracked.Status != chainsync.ClientStatusStalled {
			delete(recycleAt, connKey)
			continue
		}
		connId := tracked.ConnId
		if last, ok := lastRecycled[connKey]; ok &&
			now.Sub(last) < cooldown {
			recycleAt[connKey] = now.Add(cooldown - now.Sub(last))
			continue
		}
		active := n.chainsyncState.GetClientConnId()
		if active == nil {
			// If no active client is selected and this client
			// is overdue + stalled, recycle to force a fresh
			// connection attempt and avoid indefinite stalls.
			n.config.logger.Warn(
				"chainsync client stalled with no active selection, recycling connection",
				"connection_id", connKey,
				"stall_timeout", chainsyncCfg.StallTimeout,
				"grace_period", grace,
				"recycle_cooldown", cooldown,
			)
			n.eventBus.PublishAsync(
				connmanager.ConnectionRecycleRequestedEventType,
				event.NewEvent(
					connmanager.ConnectionRecycleRequestedEventType,
					connmanager.ConnectionRecycleRequestedEvent{
						ConnectionId: connId,
						ConnKey:      connKey,
						Reason:       "stalled_connection_no_active_selection",
					},
				),
			)
			delete(recycleAt, connKey)
			lastRecycled[connKey] = now
			continue
		}
		if active.String() != connKey {
			// Don't recycle non-primary stalled clients. Keep state clean.
			n.eventBus.PublishAsync(
				chainsync.ClientRemoveRequestedEventType,
				event.NewEvent(
					chainsync.ClientRemoveRequestedEventType,
					chainsync.ClientRemoveRequestedEvent{
						ConnId:  connId,
						ConnKey: connKey,
						Reason:  "stalled_non_primary_connection",
					},
				),
			)
			delete(recycleAt, connKey)
			continue
		}
		n.config.logger.Warn(
			"chainsync client stalled, recycling active connection",
			"connection_id", connKey,
			"stall_timeout", chainsyncCfg.StallTimeout,
			"grace_period", grace,
			"recycle_cooldown", cooldown,
		)
		n.eventBus.PublishAsync(
			connmanager.ConnectionRecycleRequestedEventType,
			event.NewEvent(
				connmanager.ConnectionRecycleRequestedEventType,
				connmanager.ConnectionRecycleRequestedEvent{
					ConnectionId: connId,
					ConnKey:      connKey,
					Reason:       "stalled_active_connection",
				},
			),
		)
		delete(recycleAt, connKey)
		lastRecycled[connKey] = now
	}
}

func (n *Node) handleChainSwitchEvent(evt event.Event) {
	e, ok := evt.Data.(chainselection.ChainSwitchEvent)
	if !ok {
		return
	}
	prevConn := "(none)"
	if e.PreviousConnectionId.LocalAddr != nil &&
		e.PreviousConnectionId.RemoteAddr != nil {
		prevConn = e.PreviousConnectionId.String()
	}
	n.config.logger.Info(
		"chain switch: updating active connection",
		"previous_connection", prevConn,
		"new_connection", e.NewConnectionId.String(),
		"new_tip_block", e.NewTip.BlockNumber,
		"new_tip_slot", e.NewTip.Point.Slot,
	)
	// Peer switches only change which already-running chainsync stream feeds
	// the ledger. Restarting chainsync here re-enters FindIntersect and can
	// race the protocol state machine under load.
	n.chainsyncState.SetClientConnId(e.NewConnectionId)
}

func New(cfg Config) (*Node, error) {
	eventBus := event.NewEventBus(cfg.promRegistry, cfg.logger)
	n := &Node{
		config:   cfg,
		eventBus: eventBus,
	}
	if err := n.configPopulateNetworkMagic(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
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

	// Track started components for cleanup on failure
	var started []func()
	success := false
	defer func() {
		r := recover()
		if r != nil {
			// Cleanup on panic, then re-panic
			for i := len(started) - 1; i >= 0; i-- {
				started[i]()
			}
			panic(r)
		} else if !success {
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
	n.chainManager.SetLedger(n.ledgerState)

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
	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			Logger:   n.config.logger,
			EventBus: n.eventBus,
		},
	)
	// Subscribe chain selector to peer tip update events
	n.eventBus.SubscribeFunc(
		chainselection.PeerTipUpdateEventType,
		n.chainSelector.HandlePeerTipUpdateEvent,
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
			Logger:                         n.config.logger,
			EventBus:                       n.eventBus,
			ConnManager:                    n.connManager,
			DisableOutbound:                n.config.isDevMode(),
			PromRegistry:                   n.config.promRegistry,
			PeerRequestFunc:                n.ouroboros.RequestPeersFromPeer,
			LedgerPeerProvider:             ledgerPeerProvider,
			UseLedgerAfterSlot:             useLedgerAfterSlot,
			TargetNumberOfKnownPeers:       n.config.targetNumberOfKnownPeers,
			TargetNumberOfEstablishedPeers: n.config.targetNumberOfEstablishedPeers,
			TargetNumberOfActivePeers:      n.config.targetNumberOfActivePeers,
			ActivePeersTopologyQuota:       n.config.activePeersTopologyQuota,
			ActivePeersGossipQuota:         n.config.activePeersGossipQuota,
			ActivePeersLedgerQuota:         n.config.activePeersLedgerQuota,
			MinHotPeers:                    n.config.minHotPeers,
			ReconcileInterval:              n.config.reconcileInterval,
			InactivityTimeout:              n.config.inactivityTimeout,
			SyncProgressProvider:           n.ledgerState,
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
	// Configure UTxO RPC (only when port is configured)
	if n.config.utxorpcPort > 0 {
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

	// Configure Blockfrost API (if port is set)
	if n.config.blockfrostPort > 0 {
		listenAddr := net.JoinHostPort(
			n.config.bindAddr,
			strconv.FormatUint(uint64(n.config.blockfrostPort), 10),
		)
		adapter := blockfrost.NewNodeAdapter(n.ledgerState)
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

	// Configure Mesh API (if port is set)
	if n.config.meshPort > 0 {
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

func (n *Node) runStallCheckerTick(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			n.config.logger.Error(
				"panic in stall checker tick, continuing",
				"panic", r,
				"stack", string(stack),
			)
		}
	}()
	fn()
}

func (n *Node) runStallCheckerLoop(fn func()) (recovered bool) {
	defer func() {
		if r := recover(); r != nil {
			recovered = true
			stack := debug.Stack()
			n.config.logger.Error(
				"panic in stall checker goroutine",
				"panic", r,
				"stack", string(stack),
			)
		}
	}()
	fn()
	return false
}

func (n *Node) Stop() error {
	var err error
	n.shutdownOnce.Do(func() {
		err = n.shutdown()
	})
	return err
}

func (n *Node) shutdown() error {
	shutdownStart := time.Now()
	// Create shutdown context with timeout (default 30s if not configured)
	shutdownTimeout := 30 * time.Second
	if n.config.shutdownTimeout > 0 {
		shutdownTimeout = n.config.shutdownTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if n.cancel != nil {
		n.cancel()
	}

	var err error

	n.config.logger.Info(
		"starting graceful shutdown",
		"timeout", shutdownTimeout,
	)

	// Phase 1: Stop accepting new work
	n.config.logger.Info("shutdown phase 1: stopping new work")

	// Stop block forger first to prevent new blocks
	if n.blockForger != nil {
		n.blockForger.Stop()
	}

	// Stop leader election to clean up resources
	if n.leaderElection != nil {
		if stopErr := n.leaderElection.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("leader election shutdown: %w", stopErr),
			)
		}
	}

	if n.chainSelector != nil {
		n.chainSelector.Stop()
	}

	if n.peerGov != nil {
		n.peerGov.Stop()
	}

	if n.snapshotMgr != nil {
		if stopErr := n.snapshotMgr.Stop(); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("snapshot manager shutdown: %w", stopErr),
			)
		}
	}

	if n.utxorpc != nil {
		if stopErr := n.utxorpc.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("utxorpc shutdown: %w", stopErr))
		}
	}

	if n.bark != nil {
		if stopErr := n.bark.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("bark shutdown: %w", stopErr))
		}
	}

	if n.blockfrostAPI != nil {
		if stopErr := n.blockfrostAPI.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("blockfrost API shutdown: %w", stopErr),
			)
		}
	}

	if n.meshAPI != nil {
		if stopErr := n.meshAPI.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("mesh API shutdown: %w", stopErr),
			)
		}
	}

	n.config.logger.Info(
		"shutdown phase 1 complete",
		"elapsed", time.Since(shutdownStart).Round(time.Millisecond),
	)

	// Phase 2: Drain and close connections
	n.config.logger.Info("shutdown phase 2: draining connections")
	phase2Start := time.Now()

	if n.mempool != nil {
		if stopErr := n.mempool.Stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("mempool shutdown: %w", stopErr))
		}
	}

	if n.connManager != nil {
		if stopErr := n.connManager.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("connection manager shutdown: %w", stopErr),
			)
		}
	}

	n.config.logger.Info(
		"shutdown phase 2 complete",
		"elapsed", time.Since(phase2Start).Round(time.Millisecond),
	)

	// Phase 3: Flush state and close database
	n.config.logger.Info("shutdown phase 3: flushing state")
	phase3Start := time.Now()

	if n.ledgerState != nil {
		n.config.logger.Info("closing ledger state")
		t := time.Now()
		if closeErr := n.ledgerState.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("ledger state close: %w", closeErr),
			)
		}
		n.config.logger.Info(
			"ledger state closed",
			"elapsed", time.Since(t).Round(time.Millisecond),
		)
	}

	if n.db != nil {
		n.config.logger.Info("closing database")
		t := time.Now()
		if closeErr := n.db.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("database close: %w", closeErr),
			)
		}
		n.config.logger.Info(
			"database closed",
			"elapsed", time.Since(t).Round(time.Millisecond),
		)
	}

	n.config.logger.Info(
		"shutdown phase 3 complete",
		"elapsed", time.Since(phase3Start).Round(time.Millisecond),
	)

	// Phase 4: Cleanup resources
	n.config.logger.Info("shutdown phase 4: cleanup resources")

	// Call registered shutdown functions
	for _, fn := range n.shutdownFuncs {
		if fnErr := fn(ctx); fnErr != nil {
			err = errors.Join(err, fmt.Errorf("shutdown function: %w", fnErr))
		}
	}
	n.shutdownFuncs = nil

	if n.eventBus != nil {
		n.eventBus.Stop()
	}

	n.config.logger.Info(
		"graceful shutdown complete",
		"total_elapsed", time.Since(shutdownStart).Round(time.Millisecond),
	)
	return err
}

func (n *Node) validateBlockProducerStartup() (*forging.PoolCredentials, error) {
	creds := forging.NewPoolCredentials()
	if err := creds.LoadFromFiles(
		n.config.shelleyVRFKey,
		n.config.shelleyKESKey,
		n.config.shelleyOperationalCertificate,
	); err != nil {
		return nil, fmt.Errorf("load pool credentials: %w", err)
	}
	if err := creds.ValidateOpCert(); err != nil {
		return nil, fmt.Errorf("validate operational certificate: %w", err)
	}
	n.config.logger.Info(
		"block producer credentials validated",
		"pool_id", creds.GetPoolID().String(),
		"opcert_expiry_period", creds.OpCertExpiryPeriod(),
	)
	return creds, nil
}

// initBlockForger initializes the block forger for production mode.
// This requires VRF, KES, and OpCert key files to be configured.
func (n *Node) initBlockForger(
	ctx context.Context,
	creds *forging.PoolCredentials,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	// Create mempool adapter for the forging package
	mempoolAdapter := &mempoolAdapter{mempool: n.mempool}

	// Create epoch nonce adapter for the builder
	epochNonceAdapter := &epochNonceAdapter{ledgerState: n.ledgerState}

	// Create block builder
	builder, err := forging.NewDefaultBlockBuilder(forging.BlockBuilderConfig{
		Logger:          n.config.logger,
		Mempool:         mempoolAdapter,
		PParamsProvider: n.ledgerState,
		ChainTip:        n.chainManager.PrimaryChain(),
		EpochNonce:      epochNonceAdapter,
		Credentials:     creds,
		TxValidator:     n.ledgerState,
	})
	if err != nil {
		return fmt.Errorf("failed to create block builder: %w", err)
	}

	// Create block broadcaster (uses the chain manager and event bus)
	broadcaster := &blockBroadcaster{
		chain:    n.chainManager.PrimaryChain(),
		eventBus: n.eventBus,
		logger:   n.config.logger,
	}

	// Create the leader election component
	// Convert pool ID from PoolId to PoolKeyHash (both are [28]byte)
	poolID := creds.GetPoolID()
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], poolID[:])

	// Create adapters for the providers that leader.Election needs
	stakeProvider := &stakeDistributionAdapter{ledgerState: n.ledgerState}
	epochProvider := &epochInfoAdapter{ledgerState: n.ledgerState}

	// Get VRF secret key from credentials
	vrfSKey := creds.GetVRFSKey()

	// Create leader election with real stake distribution
	election := leader.NewElection(
		poolKeyHash,
		vrfSKey,
		stakeProvider,
		epochProvider,
		n.eventBus,
		n.config.logger,
	)
	if n.db != nil {
		if scheduleStore := leader.NewSyncStateScheduleStore(
			n.db.Metadata(),
		); scheduleStore != nil {
			election.SetScheduleStore(scheduleStore)
		}
	}

	// Start leader election (subscribes to epoch transitions)
	if err := election.Start(ctx); err != nil {
		return fmt.Errorf("failed to start leader election: %w", err)
	}

	// Store election for cleanup during shutdown
	n.leaderElection = election

	// Create slot clock adapter for the forger
	slotClock := &slotClockAdapter{ledgerState: n.ledgerState}

	// Create the block forger with the real leader election
	forger, err := forging.NewBlockForger(forging.ForgerConfig{
		Mode:                        forging.ModeProduction,
		Logger:                      n.config.logger,
		Credentials:                 creds,
		LeaderChecker:               election,
		BlockBuilder:                builder,
		BlockBroadcaster:            broadcaster,
		SlotClock:                   slotClock,
		ForgeSyncToleranceSlots:     n.config.forgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots: n.config.forgeStaleGapThresholdSlots,
		PromRegistry:                n.config.promRegistry,
	})
	if err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return fmt.Errorf("failed to create block forger: %w", err)
	}

	// Start the forger with the passed context
	if err := forger.Start(ctx); err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return fmt.Errorf("failed to start block forger: %w", err)
	}

	n.blockForger = forger
	n.config.logger.Info(
		"block forger started in production mode with leader election",
		"pool_id", poolID.String(),
	)

	return nil
}

// mempoolAdapter adapts the mempool.Mempool to forging.MempoolProvider.
type mempoolAdapter struct {
	mempool *mempool.Mempool
}

func (a *mempoolAdapter) Transactions() []forging.MempoolTransaction {
	txs := a.mempool.Transactions()
	result := make([]forging.MempoolTransaction, len(txs))
	for i, tx := range txs {
		result[i] = forging.MempoolTransaction{
			Hash: tx.Hash,
			Cbor: tx.Cbor,
			Type: tx.Type,
		}
	}
	return result
}

// blockBroadcaster implements forging.BlockBroadcaster using the chain manager.
type blockBroadcaster struct {
	chain    *chain.Chain
	eventBus *event.EventBus
	logger   *slog.Logger
}

func (b *blockBroadcaster) AddBlock(
	block gledger.Block,
	_ []byte,
) error {
	// Add block to the chain (CBOR is stored internally by the block).
	// addBlockInternal notifies iterators after storing the block.
	if err := b.chain.AddBlock(block, nil); err != nil {
		return fmt.Errorf("failed to add block to chain: %w", err)
	}

	b.logger.Info(
		"block added to chain",
		"slot", block.SlotNumber(),
		"hash", block.Hash(),
		"block_number", block.BlockNumber(),
	)

	return nil
}

// stakeDistributionAdapter adapts ledger.LedgerState to leader.StakeDistributionProvider.
// It queries the metadata store directly with a nil transaction so the SQLite
// read pool is used, avoiding contention with block-processing write locks.
type stakeDistributionAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *stakeDistributionAdapter) GetPoolStake(
	epoch uint64,
	poolKeyHash []byte,
) (uint64, error) {
	snapshot, err := a.ledgerState.Database().Metadata().GetPoolStakeSnapshot(
		epoch, "mark", poolKeyHash, nil,
	)
	if err != nil {
		return 0, err
	}
	if snapshot == nil {
		return 0, nil
	}
	return uint64(snapshot.TotalStake), nil
}

func (a *stakeDistributionAdapter) GetTotalActiveStake(
	epoch uint64,
) (uint64, error) {
	return a.ledgerState.Database().Metadata().GetTotalActiveStake(
		epoch, "mark", nil,
	)
}

// epochInfoAdapter adapts ledger.LedgerState to leader.EpochInfoProvider.
type epochInfoAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *epochInfoAdapter) CurrentEpoch() uint64 {
	return a.ledgerState.CurrentEpoch()
}

func (a *epochInfoAdapter) EpochNonce(epoch uint64) []byte {
	return a.ledgerState.EpochNonce(epoch)
}

func (a *epochInfoAdapter) NextEpochNonceReadyEpoch() (uint64, bool) {
	return a.ledgerState.NextEpochNonceReadyEpoch()
}

func (a *epochInfoAdapter) SlotsPerEpoch() uint64 {
	return a.ledgerState.SlotsPerEpoch()
}

func (a *epochInfoAdapter) ActiveSlotCoeff() float64 {
	return a.ledgerState.ActiveSlotCoeff()
}

// slotClockAdapter adapts ledger.LedgerState to forging.SlotClockProvider.
type slotClockAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *slotClockAdapter) CurrentSlot() (uint64, error) {
	return a.ledgerState.CurrentSlot()
}

func (a *slotClockAdapter) SlotsPerKESPeriod() uint64 {
	return a.ledgerState.SlotsPerKESPeriod()
}

func (a *slotClockAdapter) ChainTipSlot() uint64 {
	return a.ledgerState.ChainTipSlot()
}

func (a *slotClockAdapter) NextSlotTime() (time.Time, error) {
	return a.ledgerState.NextSlotTime()
}

func (a *slotClockAdapter) UpstreamTipSlot() uint64 {
	return a.ledgerState.UpstreamTipSlot()
}

// epochNonceAdapter adapts ledger.LedgerState to forging.EpochNonceProvider.
type epochNonceAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *epochNonceAdapter) CurrentEpoch() uint64 {
	return a.ledgerState.CurrentEpoch()
}

func (a *epochNonceAdapter) EpochNonce(epoch uint64) []byte {
	return a.ledgerState.EpochNonce(epoch)
}
