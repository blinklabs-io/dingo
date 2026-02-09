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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/utxorpc"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type Node struct {
	connManager    *connmanager.ConnectionManager
	peerGov        *peergov.PeerGovernor
	chainsyncState *chainsync.State
	chainSelector  *chainselection.ChainSelector
	eventBus       *event.EventBus
	mempool        *mempool.Mempool
	chainManager   *chain.ChainManager
	db             *database.Database
	ledgerState    *ledger.LedgerState
	snapshotMgr    *snapshot.Manager
	utxorpc        *utxorpc.Utxorpc
	blockfrostAPI  *blockfrost.Blockfrost
	ouroboros      *ouroborosPkg.Ouroboros
	blockForger    *forging.BlockForger
	leaderElection *leader.Election
	shutdownFuncs  []func(context.Context) error
	config         Config
	ctx            context.Context
	cancel         context.CancelFunc
	shutdownOnce   sync.Once
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
		MetadataPlugin: n.config.metadataPlugin,
		MaxConnections: n.config.DatabaseWorkerPoolConfig.WorkerPoolSize,
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
	)
	if err != nil {
		return fmt.Errorf("failed to load chain manager: %w", err)
	}
	n.chainManager = cm
	// Initialize Ouroboros
	n.ouroboros = ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger:          n.config.logger,
		EventBus:        n.eventBus,
		ConnManager:     n.connManager,
		NetworkMagic:    n.config.networkMagic,
		PeerSharing:     n.config.peerSharing,
		IntersectTip:    n.config.intersectTip,
		IntersectPoints: n.config.intersectPoints,
		PromRegistry:    n.config.promRegistry,
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
				// Return the active chainsync client connection from chainsync state
				if n.chainsyncState != nil {
					return n.chainsyncState.GetClientConnId()
				}
				return nil
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
	if err := n.snapshotMgr.Start(); err != nil { //nolint:contextcheck
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
	// Initialize chainsync state
	n.chainsyncState = chainsync.NewState(
		n.eventBus,
		n.ledgerState,
	)
	n.ouroboros.ChainsyncState = n.chainsyncState
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
		func(evt event.Event) {
			e, ok := evt.Data.(chainselection.ChainSwitchEvent)
			if !ok {
				return
			}
			n.config.logger.Info(
				"chain switch: updating active connection",
				"previous_connection", e.PreviousConnectionId.String(),
				"new_connection", e.NewConnectionId.String(),
				"new_tip_block", e.NewTip.BlockNumber,
				"new_tip_slot", e.NewTip.Point.Slot,
			)
			n.chainsyncState.SetClientConnId(e.NewConnectionId)
		},
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
			Logger:             n.config.logger,
			EventBus:           n.eventBus,
			Listeners:          tmpListeners,
			OutboundSourcePort: n.config.outboundSourcePort,
			OutboundConnOpts:   n.ouroboros.OutboundConnOpts(),
			PromRegistry:       n.config.promRegistry,
		},
	)
	n.ouroboros.ConnManager = n.connManager
	// Subscribe to connection closed events
	n.eventBus.SubscribeFunc(
		connmanager.ConnectionClosedEventType,
		n.ouroboros.HandleConnClosedEvent,
	)
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
	// Configure peer governor
	// Create ledger peer provider for discovering peers from stake pool relays
	ledgerPeerProvider, err := ledger.NewLedgerPeerProvider(n.ledgerState, n.db)
	if err != nil {
		return fmt.Errorf("failed to create ledger peer provider: %w", err)
	}

	// Get UseLedgerAfterSlot from topology config (defaults to -1 = disabled)
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
		return err
	}
	started = append(started, func() { n.peerGov.Stop() })
	// Configure UTxO RPC
	n.utxorpc = utxorpc.NewUtxorpc(
		utxorpc.UtxorpcConfig{
			Logger:      n.config.logger,
			EventBus:    n.eventBus,
			LedgerState: n.ledgerState,
			Mempool:     n.mempool,
			Port:        n.config.utxorpcPort,
		},
	)
	if err := n.utxorpc.Start(n.ctx); err != nil { //nolint:contextcheck
		return err
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
	// Configure Blockfrost API (if listen address is set)
	if n.config.blockfrostListenAddress != "" {
		adapter := blockfrost.NewNodeAdapter(n.ledgerState)
		n.blockfrostAPI = blockfrost.New(
			blockfrost.BlockfrostConfig{
				ListenAddress: n.config.blockfrostListenAddress,
			},
			adapter,
			n.config.logger,
		)
		if err := n.blockfrostAPI.Start(n.ctx); err != nil { //nolint:contextcheck
			return err
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

	// Initialize block forger if production mode is enabled
	if n.config.blockProducer {
		//nolint:contextcheck // n.ctx is the node's lifecycle context, correct parent for forger
		if err := n.initBlockForger(n.ctx); err != nil {
			return fmt.Errorf("failed to initialize block forger: %w", err)
		}
		// Wire forger's slot tracker into ledger state for slot
		// battle detection. The forger is created after the ledger
		// state, so we use the late-binding setter.
		if n.blockForger != nil {
			n.ledgerState.SetForgedBlockChecker(
				n.blockForger.SlotTracker(),
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

func (n *Node) Stop() error {
	var err error
	n.shutdownOnce.Do(func() {
		err = n.shutdown()
	})
	return err
}

func (n *Node) shutdown() error {
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

	n.config.logger.Debug("starting graceful shutdown")

	// Phase 1: Stop accepting new work
	n.config.logger.Debug("shutdown phase 1: stopping new work")

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

	if n.blockfrostAPI != nil {
		if stopErr := n.blockfrostAPI.Stop(ctx); stopErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("blockfrost API shutdown: %w", stopErr),
			)
		}
	}

	// Phase 2: Drain and close connections
	n.config.logger.Debug("shutdown phase 2: draining connections")

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

	// Phase 3: Flush state and close database
	n.config.logger.Debug("shutdown phase 3: flushing state")

	if n.ledgerState != nil {
		if closeErr := n.ledgerState.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("ledger state close: %w", closeErr),
			)
		}
	}

	if n.db != nil {
		if closeErr := n.db.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("database close: %w", closeErr),
			)
		}
	}

	// Phase 4: Cleanup resources
	n.config.logger.Debug("shutdown phase 4: cleanup resources")

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

	n.config.logger.Debug("graceful shutdown complete")
	return err
}

// initBlockForger initializes the block forger for production mode.
// This requires VRF, KES, and OpCert key files to be configured.
func (n *Node) initBlockForger(ctx context.Context) error {
	// Load pool credentials from configured key files
	creds := forging.NewPoolCredentials()
	if err := creds.LoadFromFiles(
		n.config.shelleyVRFKey,
		n.config.shelleyKESKey,
		n.config.shelleyOperationalCertificate,
	); err != nil {
		return fmt.Errorf("failed to load pool credentials: %w", err)
	}

	// Validate the operational certificate matches the KES key
	if err := creds.ValidateOpCert(); err != nil {
		return fmt.Errorf("invalid operational certificate: %w", err)
	}

	n.config.logger.Info(
		"loaded pool credentials for block production",
		"pool_id", creds.GetPoolID().String(),
		"opcert_expiry_period", creds.OpCertExpiryPeriod(),
	)

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
		Mode:             forging.ModeProduction,
		Logger:           n.config.logger,
		Credentials:      creds,
		LeaderChecker:    election,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        slotClock,
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
	// Add block to the chain (CBOR is stored internally by the block)
	if err := b.chain.AddBlock(block, nil); err != nil {
		return fmt.Errorf("failed to add block to chain: %w", err)
	}

	b.logger.Info(
		"block added to chain",
		"slot", block.SlotNumber(),
		"hash", block.Hash(),
		"block_number", block.BlockNumber(),
	)

	// chain.AddBlock already publishes ChainUpdateEventType, so subscribers
	// (block propagation, ledger updates, etc.) are notified automatically.

	return nil
}

// stakeDistributionAdapter adapts ledger.LedgerState to leader.StakeDistributionProvider.
type stakeDistributionAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *stakeDistributionAdapter) GetPoolStake(
	epoch uint64,
	poolKeyHash []byte,
) (uint64, error) {
	txn := a.ledgerState.Database().Transaction(false)
	var stake uint64
	err := txn.Do(func(txn *database.Txn) error {
		view := a.ledgerState.NewView(txn)
		dist, err := view.GetStakeDistribution(epoch)
		if err != nil {
			return err
		}
		stake = dist.PoolStakes[hex.EncodeToString(poolKeyHash)]
		return nil
	})
	return stake, err
}

func (a *stakeDistributionAdapter) GetTotalActiveStake(epoch uint64) (uint64, error) {
	txn := a.ledgerState.Database().Transaction(false)
	var stake uint64
	err := txn.Do(func(txn *database.Txn) error {
		view := a.ledgerState.NewView(txn)
		dist, err := view.GetStakeDistribution(epoch)
		if err != nil {
			return err
		}
		stake = dist.TotalStake
		return nil
	})
	return stake, err
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
