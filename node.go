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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/utxorpc"
)

type Node struct {
	config         Config
	connManager    *connmanager.ConnectionManager
	peerGov        *peergov.PeerGovernor
	chainsyncState *chainsync.State
	eventBus       *event.EventBus
	mempool        *mempool.Mempool
	chainManager   *chain.ChainManager
	db             *database.Database
	ledgerState    *ledger.LedgerState
	utxorpc        *utxorpc.Utxorpc
	ouroboros      *ouroboros.Ouroboros
	shutdownFuncs  []func(context.Context) error
	ctx            context.Context
	cancel         context.CancelFunc
}

func New(cfg Config) (*Node, error) {
	eventBus := event.NewEventBus(cfg.promRegistry)
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
	var err error
	ouroborosInstance, err := ouroboros.NewOuroboros(ouroboros.OuroborosConfig{
		NetworkMagic: n.config.networkMagic,
		Node:         n,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ouroboros: %w", err)
	}
	n.ouroboros = ouroborosInstance
	return n, nil
}

func (n *Node) Run() error {
	n.ctx, n.cancel = context.WithCancel(context.Background())
	// Configure tracing
	if n.config.tracing {
		if err := n.setupTracing(); err != nil {
			return err
		}
	}
	// Load database
	dbNeedsRecovery := false
	dbConfig := &database.Config{
		BlobCacheSize: n.config.badgerCacheSize,
		DataDir:       n.config.dataDir,
		Logger:        n.config.logger,
		PromRegistry:  n.config.promRegistry,
	}
	db, err := database.New(dbConfig)
	if db == nil {
		n.config.logger.Error(
			"failed to create database",
			"error",
			"empty database returned",
		)
		return errors.New("empty database returned")
	}
	n.db = db
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
	// Load state
	state, err := ledger.NewLedgerState(
		ledger.LedgerStateConfig{
			ChainManager:               n.chainManager,
			Database:                   n.db,
			EventBus:                   n.eventBus,
			Logger:                     n.config.logger,
			CardanoNodeConfig:          n.config.cardanoNodeConfig,
			PromRegistry:               n.config.promRegistry,
			ForgeBlocks:                n.config.devMode,
			ValidateHistorical:         n.config.validateHistorical,
			BlockfetchRequestRangeFunc: n.ouroboros.BlockfetchClientRequestRange,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to load state database: %w", err)
	}
	// Add shutdown cleanup for ledger/database
	n.shutdownFuncs = append(
		n.shutdownFuncs,
		func(_ context.Context) error {
			return state.Close()
		},
	)
	n.ledgerState = state
	// Run DB recovery if needed
	if dbNeedsRecovery {
		if err := n.ledgerState.RecoverCommitTimestampConflict(); err != nil {
			return fmt.Errorf("failed to recover database: %w", err)
		}
	}
	// Start ledger
	if err := n.ledgerState.Start(n.ctx); err != nil {
		return fmt.Errorf("failed to start ledger: %w", err)
	}
	// Initialize mempool
	n.mempool = mempool.NewMempool(mempool.MempoolConfig{
		MempoolCapacity: n.config.mempoolCapacity,
		Logger:          n.config.logger,
		EventBus:        n.eventBus,
		PromRegistry:    n.config.promRegistry,
		Validator:       n.ledgerState,
	},
	)
	// Set mempool in ledger state for block forging
	n.ledgerState.SetMempool(n.mempool)
	// Initialize chainsync state
	n.chainsyncState = chainsync.NewState(
		n.eventBus,
		n.ledgerState,
	)
	// Configure connection manager
	if err := n.configureConnManager(); err != nil {
		return err
	}
	// Configure peer governor
	n.peerGov = peergov.NewPeerGovernor(
		peergov.PeerGovernorConfig{
			Logger:          n.config.logger,
			EventBus:        n.eventBus,
			ConnManager:     n.connManager,
			DisableOutbound: n.config.devMode,
		},
	)
	n.eventBus.SubscribeFunc(
		peergov.OutboundConnectionEventType,
		n.handleOutboundConnEvent,
	)
	if n.config.topologyConfig != nil {
		n.peerGov.LoadTopologyConfig(n.config.topologyConfig)
	}
	if err := n.peerGov.Start(); err != nil {
		return err
	}
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
	if err := n.utxorpc.Start(); err != nil {
		return err
	}

	// Wait for shutdown
	<-n.ctx.Done()
	return nil
}

func (n *Node) Stop(ctx context.Context) error {
	return n.shutdown(ctx)
}

func (n *Node) shutdown(ctx context.Context) error {
	if n.cancel != nil {
		n.cancel()
	}
	var err error
	// Call shutdown functions
	for _, fn := range n.shutdownFuncs {
		err = errors.Join(err, fn(ctx))
	}
	n.shutdownFuncs = nil
	return err
}

func (n *Node) configureConnManager() error {
	// Configure listeners
	nodeToClientOpts := ouroboros.NodeToClientServerOpts(
		n.config.networkMagic,
		n.ouroboros.ChainsyncServerConnOpts(),
		n.ouroboros.LocalStateQueryServerConnOpts(),
		n.ouroboros.LocalTxMonitorServerConnOpts(),
		n.ouroboros.LocalTxSubmissionServerConnOpts(),
	)
	nodeToNodeOpts := ouroboros.NodeToNodeServerOpts(
		n.config.peerSharing,
		n.config.networkMagic,
		n.ouroboros.PeersharingServerConnOpts(),
		n.ouroboros.TxsubmissionServerConnOpts(),
		n.ouroboros.ChainsyncServerConnOpts(),
		n.ouroboros.BlockfetchServerConnOpts(),
	)
	tmpListeners := ouroboros.ConfigureListeners(
		n.config.listeners,
		nodeToClientOpts,
		nodeToNodeOpts,
	)
	// Create connection manager
	n.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:             n.config.logger,
			EventBus:           n.eventBus,
			Listeners:          tmpListeners,
			OutboundSourcePort: n.config.outboundSourcePort,
			OutboundConnOpts: ouroboros.OutboundOpts(
				n.config.networkMagic,
				n.config.peerSharing,
				n.ouroboros.PeersharingClientConnOpts(),
				n.ouroboros.PeersharingServerConnOpts(),
				n.ouroboros.TxsubmissionClientConnOpts(),
				n.ouroboros.TxsubmissionServerConnOpts(),
				n.ouroboros.ChainsyncClientConnOpts(),
				n.ouroboros.ChainsyncServerConnOpts(),
				n.ouroboros.BlockfetchClientConnOpts(),
				n.ouroboros.BlockfetchServerConnOpts(),
			),
		},
	)
	// Subscribe to connection closed events
	n.eventBus.SubscribeFunc(
		connmanager.ConnectionClosedEventType,
		n.handleConnClosedEvent,
	)
	// Start listeners
	if err := n.connManager.Start(); err != nil {
		return err
	}
	return nil
}

func (n *Node) handleConnClosedEvent(evt event.Event) {
	e := evt.Data.(connmanager.ConnectionClosedEvent)
	connId := e.ConnectionId
	// Remove any chainsync client state
	n.chainsyncState.RemoveClient(connId)
	// Remove mempool consumer
	n.mempool.RemoveConsumer(connId)
	// Release chainsync client
	n.chainsyncState.RemoveClientConnId(connId)
}

func (n *Node) handleOutboundConnEvent(evt event.Event) {
	e := evt.Data.(peergov.OutboundConnectionEvent)
	connId := e.ConnectionId
	// TODO: replace this with handling for multiple chainsync clients (#385)
	// Start chainsync client if we don't have another
	shouldStartChainsync := n.chainsyncState.TryStartChainsync()
	if shouldStartChainsync {
		if err := n.ouroboros.ChainsyncClientStart(connId); err != nil {
			n.config.logger.Error(
				"failed to start chainsync client",
				"error",
				err,
			)
			n.chainsyncState.SetStartingFalse()
			return
		}
		n.chainsyncState.SetClientConnIdAndStartingFalse(connId)
	}
	// Start txsubmission client
	if err := n.ouroboros.TxsubmissionClientStart(connId); err != nil {
		n.config.logger.Error(
			"failed to start txsubmission client",
			"error",
			err,
		)
		return
	}
}

func (n *Node) ConnManager() *connmanager.ConnectionManager { return n.connManager }

func (n *Node) Config() ouroboros.ConfigInterface { return n.config }

func (n *Node) LedgerState() *ledger.LedgerState { return n.ledgerState }

func (n *Node) ChainsyncState() *chainsync.State { return n.chainsyncState }

func (n *Node) EventBus() *event.EventBus { return n.eventBus }

func (n *Node) Mempool() *mempool.Mempool { return n.mempool }

func (n *Node) PeerGov() *peergov.PeerGovernor { return n.peerGov }
