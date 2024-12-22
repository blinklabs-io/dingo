// Copyright 2024 Blink Labs Software
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
	"slices"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/state"

	ouroboros "github.com/blinklabs-io/gouroboros"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
)

type Node struct {
	config         Config
	connManager    *connmanager.ConnectionManager
	peerGov        *peergov.PeerGovernor
	chainsyncState *chainsync.State
	eventBus       *event.EventBus
	mempool        *mempool.Mempool
	ledgerState    *state.LedgerState
	shutdownFuncs  []func(context.Context) error
}

func New(cfg Config) (*Node, error) {
	eventBus := event.NewEventBus(cfg.promRegistry)
	n := &Node{
		config:   cfg,
		eventBus: eventBus,
		mempool: mempool.NewMempool(
			cfg.logger,
			eventBus,
			cfg.promRegistry,
		),
	}
	if err := n.configPopulateNetworkMagic(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %s", err)
	}
	if err := n.configValidate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %s", err)
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
	// Load state
	state, err := state.NewLedgerState(
		state.LedgerStateConfig{
			DataDir:                    n.config.dataDir,
			EventBus:                   n.eventBus,
			Logger:                     n.config.logger,
			CardanoNodeConfig:          n.config.cardanoNodeConfig,
			PromRegistry:               n.config.promRegistry,
			BlockfetchRequestRangeFunc: n.blockfetchClientRequestRange,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to load state database: %w", err)
	}
	n.ledgerState = state
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
			Logger:      n.config.logger,
			EventBus:    n.eventBus,
			ConnManager: n.connManager,
		},
	)
	n.eventBus.SubscribeFunc(peergov.OutboundConnectionEventType, n.handleOutboundConnEvent)
	if n.config.topologyConfig != nil {
		n.peerGov.LoadTopologyConfig(n.config.topologyConfig)
	}
	if err := n.peerGov.Start(); err != nil {
		return err
	}

	// Wait forever
	select {}
}

func (n *Node) Stop() error {
	// TODO: use a cancelable context and wait for it above to call shutdown
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

func (n *Node) configureConnManager() error {
	// Configure listeners
	tmpListeners := make([]ListenerConfig, len(n.config.listeners))
	for idx, l := range n.config.listeners {
		if l.UseNtC {
			// Node-to-client
			l.ConnectionOpts = append(
				l.ConnectionOpts,
				ouroboros.WithNetworkMagic(n.config.networkMagic),
				ouroboros.WithChainSyncConfig(
					ochainsync.NewConfig(
						n.chainsyncServerConnOpts()...,
					),
				),
				// TODO: add localtxsubmission
				// TODO: add localstatequery
				// TODO: add localtxmonitor
			)
		} else {
			// Node-to-node config
			l.ConnectionOpts = append(
				l.ConnectionOpts,
				ouroboros.WithPeerSharing(n.config.peerSharing),
				ouroboros.WithNetworkMagic(n.config.networkMagic),
				ouroboros.WithPeerSharingConfig(
					opeersharing.NewConfig(
						n.peersharingServerConnOpts()...,
					),
				),
				ouroboros.WithTxSubmissionConfig(
					otxsubmission.NewConfig(
						n.txsubmissionServerConnOpts()...,
					),
				),
				ouroboros.WithChainSyncConfig(
					ochainsync.NewConfig(
						n.chainsyncServerConnOpts()...,
					),
				),
				ouroboros.WithBlockFetchConfig(
					oblockfetch.NewConfig(
						n.blockfetchServerConnOpts()...,
					),
				),
			)
		}
		tmpListeners[idx] = l
	}
	// Create connection manager
	n.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:             n.config.logger,
			EventBus:           n.eventBus,
			Listeners:          tmpListeners,
			OutboundSourcePort: n.config.outboundSourcePort,
			OutboundConnOpts: []ouroboros.ConnectionOptionFunc{
				ouroboros.WithNetworkMagic(n.config.networkMagic),
				ouroboros.WithNodeToNode(true),
				ouroboros.WithKeepAlive(true),
				ouroboros.WithFullDuplex(true),
				ouroboros.WithPeerSharing(n.config.peerSharing),
				ouroboros.WithPeerSharingConfig(
					opeersharing.NewConfig(
						slices.Concat(
							n.peersharingClientConnOpts(),
							n.peersharingServerConnOpts(),
						)...,
					),
				),
				ouroboros.WithTxSubmissionConfig(
					otxsubmission.NewConfig(
						slices.Concat(
							n.txsubmissionClientConnOpts(),
							n.txsubmissionServerConnOpts(),
						)...,
					),
				),
				ouroboros.WithChainSyncConfig(
					ochainsync.NewConfig(
						slices.Concat(
							n.chainsyncClientConnOpts(),
							n.chainsyncServerConnOpts(),
						)...,
					),
				),
				ouroboros.WithBlockFetchConfig(
					oblockfetch.NewConfig(
						slices.Concat(
							n.blockfetchClientConnOpts(),
							n.blockfetchServerConnOpts(),
						)...,
					),
				),
			},
		},
	)
	// Subscribe to connection closed events
	n.eventBus.SubscribeFunc(connmanager.ConnectionClosedEventType, n.handleConnClosedEvent)
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
	// TODO: replace this with handling for multiple chainsync clients
	// Start chainsync client if we don't have another
	n.chainsyncState.Lock()
	defer n.chainsyncState.Unlock()
	chainsyncClientConnId := n.chainsyncState.GetClientConnId()
	if chainsyncClientConnId == nil {
		if err := n.chainsyncClientStart(connId); err != nil {
			n.config.logger.Error("failed to start chainsync client", "error", err)
			return
		}
		n.chainsyncState.SetClientConnId(connId)
	}
	// Start txsubmission client
	if err := n.txsubmissionClientStart(connId); err != nil {
		n.config.logger.Error("failed to start chainsync client", "error", err)
		return
	}
}
