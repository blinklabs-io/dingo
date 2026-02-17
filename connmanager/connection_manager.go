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
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
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

	// DefaultMaxInboundConnections is the default maximum number of
	// simultaneous inbound connections accepted by the connection manager.
	// This prevents resource exhaustion from malicious or accidental
	// connection floods.
	DefaultMaxInboundConnections = 100
)

type connectionInfo struct {
	conn      *ouroboros.Connection
	peerAddr  string
	isInbound bool
	ipKey     string // rate-limit key (IP or /64 prefix for IPv6)
}

type peerConnectionState struct {
	hasIncoming bool
	hasOutgoing bool
}

type ConnectionManager struct {
	connections      map[ouroboros.ConnectionId]*connectionInfo
	inboundReserved  int // slots reserved by tryReserveInboundSlot but not yet added
	metrics          *connectionManagerMetrics
	listeners        []net.Listener
	config           ConnectionManagerConfig
	connectionsMutex sync.Mutex
	listenersMutex   sync.Mutex
	closing          bool
	goroutineWg      sync.WaitGroup // tracks spawned goroutines for clean shutdown
	ipConns          map[string]int // IP key -> active connection count
	ipConnsMutex     sync.Mutex
}

// DefaultMaxConnectionsPerIP is the default maximum number of concurrent
// connections allowed from a single IP address (or /64 prefix for IPv6).
const DefaultMaxConnectionsPerIP = 5

type ConnectionManagerConfig struct {
	PromRegistry       prometheus.Registerer
	Logger             *slog.Logger
	EventBus           *event.EventBus
	ConnClosedFunc     ConnectionManagerConnClosedFunc
	Listeners          []ListenerConfig
	OutboundConnOpts   []ouroboros.ConnectionOptionFunc
	OutboundSourcePort uint
	MaxInboundConns    int // 0 means use DefaultMaxInboundConnections
	// MaxConnectionsPerIP limits the number of concurrent inbound
	// connections from the same IP address. IPv6 addresses are grouped
	// by /64 prefix. A value of 0 means use DefaultMaxConnectionsPerIP.
	MaxConnectionsPerIP int
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
	if cfg.MaxInboundConns <= 0 {
		cfg.MaxInboundConns = DefaultMaxInboundConnections
	}
	if cfg.MaxConnectionsPerIP <= 0 {
		cfg.MaxConnectionsPerIP = DefaultMaxConnectionsPerIP
	}
	c := &ConnectionManager{
		config: cfg,
		connections: make(
			map[ouroboros.ConnectionId]*connectionInfo,
		),
		ipConns: make(map[string]int),
	}
	if cfg.PromRegistry != nil {
		c.initMetrics()
	}
	return c
}

// inboundCount returns the current number of inbound connections.
// The caller must hold connectionsMutex.
func (c *ConnectionManager) inboundCountLocked() int {
	count := 0
	for _, info := range c.connections {
		if info.isInbound {
			count++
		}
	}
	return count
}

// InboundCount returns the current number of inbound connections.
func (c *ConnectionManager) InboundCount() int {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	return c.inboundCountLocked()
}

// tryReserveInboundSlot atomically checks whether an inbound connection
// can be accepted and, if so, reserves a slot. This prevents a TOCTOU race
// where multiple concurrent Accept calls could all pass the limit check
// before any of them calls AddConnection.
// Returns true if the slot was reserved, false if the limit has been reached.
// The caller must call releaseInboundSlot when the reservation is no longer
// needed (i.e. if connection setup fails before AddConnection is called).
func (c *ConnectionManager) tryReserveInboundSlot() bool {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	if c.inboundCountLocked()+c.inboundReserved >= c.config.MaxInboundConns {
		return false
	}
	c.inboundReserved++
	return true
}

// releaseInboundSlot releases a previously reserved inbound slot.
// This must be called if the connection setup fails after a successful
// tryReserveInboundSlot call and before AddConnection is called.
func (c *ConnectionManager) releaseInboundSlot() {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	c.inboundReserved--
}

// consumeInboundSlot converts a reserved inbound slot into an actual
// connection entry via AddConnection. The reservation is consumed
// (decremented) since the connection itself now occupies the slot.
func (c *ConnectionManager) consumeInboundSlot() {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	c.inboundReserved--
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
	if c == nil {
		return
	}
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

	if c.metrics.incomingConns != nil {
		c.metrics.incomingConns.Set(float64(incomingCount))
	}
	if c.metrics.outgoingConns != nil {
		c.metrics.outgoingConns.Set(float64(outgoingCount))
	}
	if c.metrics.unidirectionalConns != nil {
		c.metrics.unidirectionalConns.Set(float64(unidirectionalCount))
	}
	if c.metrics.duplexConns != nil {
		c.metrics.duplexConns.Set(float64(duplexCount))
	}
	if c.metrics.fullDuplexConns != nil {
		c.metrics.fullDuplexConns.Set(float64(fullDuplexCount))
	}
	if c.metrics.prunableConns != nil {
		c.metrics.prunableConns.Set(float64(prunableCount))
	}
}

func (c *ConnectionManager) Start(ctx context.Context) error {
	if err := c.startListeners(ctx); err != nil {
		return err
	}
	return nil
}

func (c *ConnectionManager) Stop(ctx context.Context) error {
	var err error

	c.config.Logger.Debug("stopping connection manager")

	// Mark closing to suppress accept-loop noise
	c.listenersMutex.Lock()
	c.closing = true
	c.listenersMutex.Unlock()

	// Stop accepting new connections (this causes listener goroutines to exit)
	c.stopListeners()

	// Close all existing connections gracefully
	// This triggers error watchers to receive from ErrorChan and exit
	c.connectionsMutex.Lock()
	conns := make([]*ouroboros.Connection, 0, len(c.connections))
	for _, info := range c.connections {
		conns = append(conns, info.conn)
	}
	c.connectionsMutex.Unlock()

	// Close connections with timeout awareness
	closeDone := make(chan error, 1)
	go func() {
		var closeErr error
		for _, conn := range conns {
			if conn != nil {
				if err := conn.Close(); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
			}
		}
		closeDone <- closeErr
	}()

	select {
	case closeErr := <-closeDone:
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		// All connections closed
	case <-ctx.Done():
		c.config.Logger.Warn(
			"shutdown timeout exceeded, some connections may not have closed cleanly",
		)
		err = errors.Join(err, ctx.Err())
		// Return early - don't wait for goroutines if we've already timed out
		c.config.Logger.Debug("connection manager stopped with timeout")
		return err
	}

	// Wait for all goroutines (listeners and error watchers) to exit
	goroutineDone := make(chan struct{})
	go func() {
		c.goroutineWg.Wait()
		close(goroutineDone)
	}()

	select {
	case <-goroutineDone:
		c.config.Logger.Debug("all goroutines stopped cleanly")
	case <-ctx.Done():
		c.config.Logger.Warn(
			"shutdown timeout while waiting for goroutines",
		)
		err = errors.Join(err, ctx.Err())
	}

	c.config.Logger.Debug("connection manager stopped")
	return err
}

func (c *ConnectionManager) stopListeners() {
	c.listenersMutex.Lock()
	listeners := make([]net.Listener, 0, len(c.listeners))
	for _, listener := range c.listeners {
		if listener != nil {
			listeners = append(listeners, listener)
		}
	}
	c.listeners = nil
	c.listenersMutex.Unlock()

	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			c.config.Logger.Warn(
				"error closing listener",
				"error", err,
			)
		}
	}
}

func (c *ConnectionManager) AddConnection(
	conn *ouroboros.Connection,
	isInbound bool,
	peerAddr string,
) {
	c.addConnectionWithIPKey(conn, isInbound, peerAddr, "")
}

func (c *ConnectionManager) addConnectionWithIPKey(
	conn *ouroboros.Connection,
	isInbound bool,
	peerAddr string,
	ipKey string,
) {
	// Check if shutting down before adding to WaitGroup to prevent panic
	// during Stop()'s Wait() call. Must hold the same lock used to set closing.
	c.listenersMutex.Lock()
	if c.closing {
		c.listenersMutex.Unlock()
		// Shutting down - release IP slot and close connection
		c.releaseIPSlot(ipKey)
		if conn != nil {
			conn.Close()
		}
		return
	}
	c.goroutineWg.Add(1)
	c.listenersMutex.Unlock()

	connId := conn.Id()
	c.connectionsMutex.Lock()
	c.connections[connId] = &connectionInfo{
		conn:      conn,
		isInbound: isInbound,
		peerAddr:  peerAddr,
		ipKey:     ipKey,
	}
	c.connectionsMutex.Unlock()
	c.updateConnectionMetrics()
	go func() {
		defer c.goroutineWg.Done()
		err := <-conn.ErrorChan()
		// Remove connection (also releases IP slot)
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
	info := c.connections[connId]
	delete(c.connections, connId)
	c.connectionsMutex.Unlock()
	// Decrement per-IP counter if the connection had a tracked IP key
	if info != nil && info.ipKey != "" {
		c.releaseIPSlot(info.ipKey)
	}
	c.updateConnectionMetrics()
}

// HasFullDuplexInbound returns true if there is already an inbound connection
// from peerAddr that negotiated InitiatorAndResponder mode. This indicates the
// connection carries both client and server mini-protocols, making a separate
// outbound connection unnecessary.
func (c *ConnectionManager) HasFullDuplexInbound(
	peerAddr string,
) bool {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	for _, info := range c.connections {
		if !info.isInbound || info.conn == nil {
			continue
		}
		if info.peerAddr != peerAddr {
			continue
		}
		_, versionData := info.conn.ProtocolVersion()
		if versionData.DiffusionMode() == oprotocol.DiffusionModeInitiatorAndResponder {
			return true
		}
	}
	return false
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
