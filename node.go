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
	"sync"

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
	config             Config
	connManager        *connmanager.ConnectionManager
	peerGov            *peergov.PeerGovernor
	chainsyncState     *chainsync.State
	eventBus           *event.EventBus
	outboundConns      map[ouroboros.ConnectionId]outboundPeer
	outboundConnsMutex sync.Mutex
	mempool            *mempool.Mempool
	ledgerState        *state.LedgerState
	shutdownFuncs      []func(context.Context) error
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
		outboundConns: make(map[ouroboros.ConnectionId]outboundPeer),
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
	if err := n.peerGov.Start(); err != nil {
		return err
	}
	// Start outbound connections
	n.startOutboundConnections()

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
			tmpListeners[idx] = l
		}
	}
	// Create connection manager
	n.connManager = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:         n.config.logger,
			EventBus:       n.eventBus,
			ConnClosedFunc: n.connectionManagerConnClosed,
			Listeners:      tmpListeners,
		},
	)
	// Start listeners
	if err := n.connManager.Start(); err != nil {
		return err
	}
	return nil
}

func (n *Node) connectionManagerConnClosed(
	connId ouroboros.ConnectionId,
	err error,
) {
	if err != nil {
		n.config.logger.Error(
			fmt.Sprintf(
				"unexpected connection failure: %s",
				err,
			),
			"component", "network",
			"connection_id", connId.String(),
		)
	} else {
		n.config.logger.Info("connection closed",
			"component", "network",
			"connection_id", connId.String(),
		)
	}
	conn := n.connManager.GetConnectionById(connId)
	if conn == nil {
		return
	}
	// Remove connection
	n.connManager.RemoveConnection(connId)
	// Remove any chainsync client state
	n.chainsyncState.RemoveClient(connId)
	// Remove mempool consumer
	n.mempool.RemoveConsumer(connId)
	// Outbound connections
	n.outboundConnsMutex.Lock()
	if peer, ok := n.outboundConns[connId]; ok {
		// Release chainsync client
		n.chainsyncState.RemoveClientConnId(connId)
		// Reconnect outbound connection
		go n.reconnectOutboundConnection(peer)
	}
	n.outboundConnsMutex.Unlock()
}
