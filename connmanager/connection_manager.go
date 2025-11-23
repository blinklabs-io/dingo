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

package connmanager

import (
	"io"
	"log/slog"
	"sync"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	oprotocol "github.com/blinklabs-io/gouroboros/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ConnectionManagerConnClosedFunc is a function that takes a connection ID and an optional error
type ConnectionManagerConnClosedFunc func(ouroboros.ConnectionId, error)

const (
	// metricNamePrefix is the common prefix for all connection manager metrics
	metricNamePrefix = "cardano_node_metrics_connectionManager_"
)

type connectionInfo struct {
	conn      *ouroboros.Connection
	isInbound bool
	peerAddr  string
}

type peerConnectionState struct {
	hasIncoming bool
	hasOutgoing bool
}

type ConnectionManager struct {
	connections      map[ouroboros.ConnectionId]*connectionInfo
	config           ConnectionManagerConfig
	connectionsMutex sync.Mutex
	metrics          *connectionManagerMetrics
}

type ConnectionManagerConfig struct {
	Logger             *slog.Logger
	EventBus           *event.EventBus
	ConnClosedFunc     ConnectionManagerConnClosedFunc
	Listeners          []ListenerConfig
	OutboundConnOpts   []ouroboros.ConnectionOptionFunc
	OutboundSourcePort uint
	PromRegistry       prometheus.Registerer
}

type connectionManagerMetrics struct {
	incomingConns       prometheus.Gauge
	outgoingConns       prometheus.Gauge
	unidirectionalConns prometheus.Gauge
	duplexConns         prometheus.Gauge
	fullDuplexConns     prometheus.Gauge
	prunableConns       prometheus.Gauge
}

func NewConnectionManager(cfg ConnectionManagerConfig) *ConnectionManager {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "connmanager")
	c := &ConnectionManager{
		config: cfg,
		connections: make(
			map[ouroboros.ConnectionId]*connectionInfo,
		),
	}
	if cfg.PromRegistry != nil {
		c.initMetrics()
	}
	return c
}

func (c *ConnectionManager) initMetrics() {
	promautoFactory := promauto.With(c.config.PromRegistry)
	c.metrics = &connectionManagerMetrics{}
	c.metrics.incomingConns = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: metricNamePrefix + "incomingConns",
		Help: "number of incoming connections",
	})
	c.metrics.outgoingConns = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: metricNamePrefix + "outgoingConns",
		Help: "number of outgoing connections",
	})
	c.metrics.unidirectionalConns = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: metricNamePrefix + "unidirectionalConns",
			Help: "number of peers with unidirectional connections",
		},
	)
	c.metrics.duplexConns = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: metricNamePrefix + "duplexConns",
		Help: "number of peers with duplex connections",
	})
	c.metrics.fullDuplexConns = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: metricNamePrefix + "fullDuplexConns",
		Help: "number of full-duplex connections",
	})
	c.metrics.prunableConns = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: metricNamePrefix + "prunableConns",
		Help: "number of prunable connections",
	})
}

func (c *ConnectionManager) updateConnectionMetrics() {
	if c.metrics == nil {
		return
	}
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()

	incomingCount := 0
	outgoingCount := 0
	fullDuplexCount := 0
	prunableCount := 0
	peerConnectivity := make(
		map[string]*peerConnectionState,
	) // peerAddr -> connection state

	// Count connections and track peer connectivity in a single pass
	for _, info := range c.connections {
		if info.isInbound {
			incomingCount++
		} else {
			outgoingCount++
		}

		if peerConnectivity[info.peerAddr] == nil {
			peerConnectivity[info.peerAddr] = &peerConnectionState{}
		}
		if info.isInbound {
			peerConnectivity[info.peerAddr].hasIncoming = true
		} else {
			peerConnectivity[info.peerAddr].hasOutgoing = true
		}
	}

	// Count full-duplex and prunable connections
	for _, info := range c.connections {
		// Full-duplex: inbound connections that support bidirectional protocols
		if info.isInbound && info.conn != nil {
			_, versionData := info.conn.ProtocolVersion()
			if versionData.DiffusionMode() == oprotocol.DiffusionModeInitiatorAndResponder {
				fullDuplexCount++
			}
		}

		// Prunable: inbound connections from peers we don't have outgoing connections to
		// These are lower priority and candidates for pruning under resource pressure
		if info.isInbound && peerConnectivity[info.peerAddr] != nil &&
			!peerConnectivity[info.peerAddr].hasOutgoing {
			prunableCount++
		}
	}

	// Count duplex and unidirectional connections
	duplexCount := 0
	unidirectionalCount := 0
	for _, state := range peerConnectivity {
		if state.hasIncoming && state.hasOutgoing {
			duplexCount++
		} else if state.hasIncoming || state.hasOutgoing {
			unidirectionalCount++
		}
	}

	c.metrics.incomingConns.Set(float64(incomingCount))
	c.metrics.outgoingConns.Set(float64(outgoingCount))
	c.metrics.unidirectionalConns.Set(float64(unidirectionalCount))
	c.metrics.duplexConns.Set(float64(duplexCount))
	c.metrics.fullDuplexConns.Set(float64(fullDuplexCount))
	c.metrics.prunableConns.Set(float64(prunableCount))
}

func (c *ConnectionManager) Start() error {
	if err := c.startListeners(); err != nil {
		return err
	}
	return nil
}

func (c *ConnectionManager) AddConnection(
	conn *ouroboros.Connection,
	isInbound bool,
	peerAddr string,
) {
	connId := conn.Id()
	c.connectionsMutex.Lock()
	c.connections[connId] = &connectionInfo{
		conn:      conn,
		isInbound: isInbound,
		peerAddr:  peerAddr,
	}
	c.connectionsMutex.Unlock()
	c.updateConnectionMetrics()
	go func() {
		err := <-conn.ErrorChan()
		// Remove connection
		c.RemoveConnection(connId)
		// Generate event
		if c.config.EventBus != nil {
			c.config.EventBus.Publish(
				ConnectionClosedEventType,
				event.NewEvent(
					ConnectionClosedEventType,
					ConnectionClosedEvent{
						ConnectionId: connId,
						Error:        err,
					},
				),
			)
		}
		// Call configured connection closed callback func
		if c.config.ConnClosedFunc != nil {
			c.config.ConnClosedFunc(connId, err)
		}
	}()
}

func (c *ConnectionManager) RemoveConnection(connId ouroboros.ConnectionId) {
	c.connectionsMutex.Lock()
	delete(c.connections, connId)
	c.connectionsMutex.Unlock()
	c.updateConnectionMetrics()
}

func (c *ConnectionManager) GetConnectionById(
	connId ouroboros.ConnectionId,
) *ouroboros.Connection {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	if info, exists := c.connections[connId]; exists {
		return info.conn
	}
	return nil // nil indicates connection not found
}
