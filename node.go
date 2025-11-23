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
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/utxorpc"
)

type Node struct {
	connManager    *connmanager.ConnectionManager
	peerGov        *peergov.PeerGovernor
	chainsyncState *chainsync.State
	eventBus       *event.EventBus
	mempool        *mempool.Mempool
	chainManager   *chain.ChainManager
	db             *database.Database
	ledgerState    *ledger.LedgerState
	utxorpc        *utxorpc.Utxorpc
	ouroboros      *ouroborosPkg.Ouroboros
	shutdownFuncs  []func(context.Context) error
	config         Config
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
	return n, nil
}

func (n *Node) Run() error {
	// Configure tracing
	if n.config.tracing {
		if err := n.setupTracing(); err != nil {
			return err
		}
	}
	// Load database
	dbNeedsRecovery := false
	dbConfig := &database.Config{
		DataDir:      n.config.dataDir,
		Logger:       n.config.logger,
		PromRegistry: n.config.promRegistry,
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
	// Initialize Ouroboros
	n.ouroboros = ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger:          n.config.logger,
		EventBus:        n.eventBus,
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
	n.ouroboros.LedgerState = n.ledgerState
	// Run DB recovery if needed
	if dbNeedsRecovery {
		if err := n.ledgerState.RecoverCommitTimestampConflict(); err != nil {
			return fmt.Errorf("failed to recover database: %w", err)
		}
	}
	// Start ledger
	if err := n.ledgerState.Start(); err != nil {
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
	n.ouroboros.Mempool = n.mempool
	// Initialize chainsync state
	n.chainsyncState = chainsync.NewState(
		n.eventBus,
		n.ledgerState,
	)
	n.ouroboros.ChainsyncState = n.chainsyncState
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
	if err := n.connManager.Start(); err != nil {
		return err
	}
	// Configure peer governor
	n.peerGov = peergov.NewPeerGovernor(
		peergov.PeerGovernorConfig{
			Logger:          n.config.logger,
			EventBus:        n.eventBus,
			ConnManager:     n.connManager,
			DisableOutbound: n.config.devMode,
			PromRegistry:    n.config.promRegistry,
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

	// Wait forever
	select {}
}

func (n *Node) Stop() error {
	return n.shutdown()
}

func (n *Node) shutdown() error {
	ctx := context.TODO()
	var err error
	// Shutdown ledger
	err = errors.Join(err, n.ledgerState.Close())
	// Call shutdown functions
	for _, fn := range n.shutdownFuncs {
		err = errors.Join(err, fn(ctx))
	}
	n.shutdownFuncs = nil
	return err
}
