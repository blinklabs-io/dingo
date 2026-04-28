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
	"strings"
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
	isNtC     bool   // true for node-to-client (local) connections
	ipKey     string // rate-limit key (IP or /64 prefix for IPv6)
}

type ConnectionManager struct {
	connections      map[ouroboros.ConnectionId]*connectionInfo
	inboundCount     int // active inbound N2N connections; maintained under connectionsMutex
	inboundReserved  int // slots reserved by tryReserveInboundSlot but not yet added
	inboundPeerAddrs map[string]int
	peerConnectivity map[string]peerConnectionSummary
	metrics          *connectionManagerMetrics
	listeners        []net.Listener
	config           ConnectionManagerConfig
	connectionsMutex sync.Mutex
	listenersMutex   sync.Mutex
	closing          bool
	goroutineWg      sync.WaitGroup // tracks spawned goroutines for clean shutdown
	ipConns          map[string]int // IP key -> active connection count
	ipConnsMutex     sync.Mutex
	outboundCount    int
	fullDuplexCount  int
	unidirectional   int
	duplexPeers      int
	prunableConns    int
	trackedConnCount int
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

type peerConnectionSummary struct {
	inboundCount int
	hasOutbound  bool
}

func (s peerConnectionSummary) hasInbound() bool {
	return s.inboundCount > 0
}

func (s peerConnectionSummary) withInbound() peerConnectionSummary {
	s.inboundCount++
	return s
}

func (s peerConnectionSummary) withOutbound() peerConnectionSummary {
	s.hasOutbound = true
	return s
}

func (s peerConnectionSummary) withoutInbound() peerConnectionSummary {
	if s.inboundCount > 0 {
		s.inboundCount--
	}
	return s
}

func (s peerConnectionSummary) withoutOutbound() peerConnectionSummary {
	s.hasOutbound = false
	return s
}

func (s peerConnectionSummary) empty() bool {
	return s.inboundCount == 0 && !s.hasOutbound
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
		inboundPeerAddrs: make(map[string]int),
		peerConnectivity: make(map[string]peerConnectionSummary),
		ipConns:          make(map[string]int),
	}
	if cfg.PromRegistry != nil {
		c.initMetrics()
	}
	return c
}

// inboundCountLocked returns the current number of inbound N2N connections.
// The caller must hold connectionsMutex.
func (c *ConnectionManager) inboundCountLocked() int {
	return c.inboundCount
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
	if c.trackedConnCount != len(c.connections) {
		c.rebuildConnectionMetricsLocked()
	}
	incomingCount := c.inboundCount
	outgoingCount := c.outboundCount
	fullDuplexCount := c.fullDuplexCount
	unidirectionalCount := c.unidirectional
	duplexCount := c.duplexPeers
	prunableCount := c.prunableConns
	c.connectionsMutex.Unlock()

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

func connectionSummaryTotals(summary peerConnectionSummary) (int, int, int) {
	duplexCount := 0
	unidirectionalCount := 0
	prunableCount := 0
	if summary.hasInbound() && summary.hasOutbound {
		duplexCount = 1
	} else if summary.hasInbound() || summary.hasOutbound {
		unidirectionalCount = 1
	}
	if !summary.hasOutbound {
		prunableCount = summary.inboundCount
	}
	return duplexCount, unidirectionalCount, prunableCount
}

func (c *ConnectionManager) hasFullDuplex(info *connectionInfo) bool {
	if info == nil || !info.isInbound || info.isNtC || info.conn == nil {
		return false
	}
	return connectionIsDuplex(info.conn)
}

// connectionIsDuplex reports whether a connection negotiated
// InitiatorAndResponder (full-duplex) mode. Returns false when the
// handshake has not completed yet or the connection is nil, so it is
// safe to call at any time after ouroboros.NewConnection returns.
func connectionIsDuplex(conn *ouroboros.Connection) bool {
	if conn == nil {
		return false
	}
	_, versionData := conn.ProtocolVersion()
	if versionData == nil {
		return false
	}
	return versionData.DiffusionMode() ==
		oprotocol.DiffusionModeInitiatorAndResponder
}

func (c *ConnectionManager) tracksInboundPeerAddresses() bool {
	return c != nil && c.config.OutboundSourcePort != 0
}

func (c *ConnectionManager) updatePeerConnectivityLocked(
	peerAddr string,
	isInbound bool,
	add bool,
) {
	if peerAddr == "" {
		return
	}
	peerKey := NormalizePeerAddr(peerAddr)
	before := c.peerConnectivity[peerKey]
	after := before
	if add {
		if isInbound {
			after = after.withInbound()
		} else {
			after = after.withOutbound()
		}
	} else if isInbound {
		after = after.withoutInbound()
	} else {
		after = after.withoutOutbound()
	}
	beforeDuplex, beforeUnidirectional, beforePrunable := connectionSummaryTotals(
		before,
	)
	afterDuplex, afterUnidirectional, afterPrunable := connectionSummaryTotals(
		after,
	)
	c.duplexPeers += afterDuplex - beforeDuplex
	c.unidirectional += afterUnidirectional - beforeUnidirectional
	c.prunableConns += afterPrunable - beforePrunable
	if after.empty() {
		delete(c.peerConnectivity, peerKey)
		return
	}
	c.peerConnectivity[peerKey] = after
}

func (c *ConnectionManager) updateConnectionMetricsLocked(
	info *connectionInfo,
	add bool,
) {
	if info == nil || info.isNtC {
		c.trackedConnCount = len(c.connections)
		return
	}
	if add {
		if info.isInbound {
			c.inboundCount++
			if c.hasFullDuplex(info) {
				c.fullDuplexCount++
			}
		} else {
			c.outboundCount++
		}
		c.updatePeerConnectivityLocked(info.peerAddr, info.isInbound, true)
	} else {
		if info.isInbound {
			c.inboundCount--
			if c.hasFullDuplex(info) {
				c.fullDuplexCount--
			}
		} else {
			c.outboundCount--
		}
		c.updatePeerConnectivityLocked(info.peerAddr, info.isInbound, false)
	}
	c.trackedConnCount = len(c.connections)
}

func (c *ConnectionManager) rebuildConnectionMetricsLocked() {
	c.inboundCount = 0
	c.outboundCount = 0
	c.fullDuplexCount = 0
	c.unidirectional = 0
	c.duplexPeers = 0
	c.prunableConns = 0
	clear(c.peerConnectivity)
	for _, info := range c.connections {
		if info == nil || info.isNtC {
			continue
		}
		if info.isInbound {
			c.inboundCount++
			if c.hasFullDuplex(info) {
				c.fullDuplexCount++
			}
		} else {
			c.outboundCount++
		}
		c.updatePeerConnectivityLocked(info.peerAddr, info.isInbound, true)
	}
	c.trackedConnCount = len(c.connections)
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
) bool {
	return c.addConnectionImpl(conn, isInbound, false, peerAddr, "")
}

func (c *ConnectionManager) addConnectionWithIPKey(
	conn *ouroboros.Connection,
	isInbound bool,
	peerAddr string,
	ipKey string,
) bool {
	return c.addConnectionImpl(conn, isInbound, false, peerAddr, ipKey)
}

func (c *ConnectionManager) addNtCConnectionWithIPKey(
	conn *ouroboros.Connection,
	isInbound bool,
	peerAddr string,
	ipKey string,
) bool {
	return c.addConnectionImpl(conn, isInbound, true, peerAddr, ipKey)
}

func (c *ConnectionManager) addConnectionImpl(
	conn *ouroboros.Connection,
	isInbound bool,
	isNtC bool,
	peerAddr string,
	ipKey string,
) bool {
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
		return false
	}
	c.goroutineWg.Add(1)
	c.listenersMutex.Unlock()

	connId := conn.Id()
	c.connectionsMutex.Lock()

	// Detect ConnectionId collision. When both sides use listen-port
	// reuse (OutboundSourcePort), the inbound and outbound connections
	// produce identical ConnectionIds. Without dedup the inbound
	// silently overwrites the outbound, corrupting IsInboundConnection
	// and causing chainsync client callbacks to be dropped.
	if existing, ok := c.connections[connId]; ok && existing.conn != nil {
		switch {
		case !existing.isInbound && isInbound:
			// Existing outbound + new inbound: keep outbound.
			// Outbound connections carry the chainsync client (chain
			// truth source). Matches cardano-node dedup behavior.
			c.connectionsMutex.Unlock()
			c.config.Logger.Warn(
				"closing inbound connection that collides with existing outbound",
				"peer_addr",
				peerAddr,
			)
			conn.Close()
			c.releaseIPSlot(ipKey)
			c.goroutineWg.Done()
			return false

		case existing.isInbound && !isInbound:
			// Existing inbound + new outbound: outbound wins.
			// Clean up inbound peer address tracking for the evicted
			// connection before replacing it.
			c.config.Logger.Info(
				"replacing inbound connection with outbound on ConnectionId collision",
				"peer_addr",
				peerAddr,
			)
			if existing.isInbound &&
				!existing.isNtC &&
				c.tracksInboundPeerAddresses() {
				if peerKey := NormalizePeerAddr(existing.peerAddr); peerKey != "" {
					c.inboundPeerAddrs[peerKey]--
					if c.inboundPeerAddrs[peerKey] <= 0 {
						delete(c.inboundPeerAddrs, peerKey)
					}
				}
			}
			c.updateConnectionMetricsLocked(existing, false)
			existingConn := existing.conn
			existingIPKey := existing.ipKey
			// Remove the old entry so the evicted connection's
			// error-watcher goroutine cannot double-decrement
			// metrics via RemoveConnection.
			delete(c.connections, connId)
			c.connectionsMutex.Unlock()
			existingConn.Close()
			if existingIPKey != "" {
				c.releaseIPSlot(existingIPKey)
			}
			c.connectionsMutex.Lock()

		default:
			// Same direction — allow overwrite (reconnect/replacement).
			// Clean up old connection to avoid leaking metrics, IP slots,
			// and inbound peer counters.
			c.config.Logger.Info(
				"replacing same-direction connection on ConnectionId collision",
				"peer_addr", peerAddr,
				"direction_inbound", isInbound,
			)
			if existing.isInbound &&
				!existing.isNtC &&
				c.tracksInboundPeerAddresses() {
				if peerKey := NormalizePeerAddr(existing.peerAddr); peerKey != "" {
					c.inboundPeerAddrs[peerKey]--
					if c.inboundPeerAddrs[peerKey] <= 0 {
						delete(c.inboundPeerAddrs, peerKey)
					}
				}
			}
			c.updateConnectionMetricsLocked(existing, false)
			existingConn := existing.conn
			existingIPKey := existing.ipKey
			delete(c.connections, connId)
			c.connectionsMutex.Unlock()
			existingConn.Close()
			if existingIPKey != "" {
				c.releaseIPSlot(existingIPKey)
			}
			c.connectionsMutex.Lock()
		}
	}

	c.connections[connId] = &connectionInfo{
		conn:      conn,
		isInbound: isInbound,
		isNtC:     isNtC,
		peerAddr:  peerAddr,
		ipKey:     ipKey,
	}
	info := c.connections[connId]
	if isInbound && !isNtC && c.tracksInboundPeerAddresses() {
		if peerKey := NormalizePeerAddr(peerAddr); peerKey != "" {
			c.inboundPeerAddrs[peerKey]++
		}
	}
	c.updateConnectionMetricsLocked(info, true)
	c.connectionsMutex.Unlock()
	c.updateConnectionMetrics()
	go func() {
		defer c.goroutineWg.Done()
		err := <-conn.ErrorChan()
		// Remove connection (also releases IP slot)
		if !c.RemoveConnection(connId, conn) {
			return
		}
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
	return true
}

func (c *ConnectionManager) RemoveConnection(
	connId ouroboros.ConnectionId,
	conn *ouroboros.Connection,
) bool {
	c.connectionsMutex.Lock()
	info := c.connections[connId]
	// Only remove if the map still holds this exact connection.
	// A replacement connection may have re-registered under the
	// same ID (OutboundSourcePort reuse).
	if info == nil || info.conn != conn {
		c.connectionsMutex.Unlock()
		return false
	}
	delete(c.connections, connId)
	if info != nil &&
		info.isInbound &&
		!info.isNtC &&
		c.tracksInboundPeerAddresses() {
		if peerKey := NormalizePeerAddr(info.peerAddr); peerKey != "" {
			c.inboundPeerAddrs[peerKey]--
			if c.inboundPeerAddrs[peerKey] <= 0 {
				delete(c.inboundPeerAddrs, peerKey)
			}
		}
	}
	c.updateConnectionMetricsLocked(info, false)
	c.connectionsMutex.Unlock()
	// Decrement per-IP counter if the connection had a tracked IP key
	if info != nil && info.ipKey != "" {
		c.releaseIPSlot(info.ipKey)
	}
	c.updateConnectionMetrics()
	return true
}

// HasInboundPeerAddress returns true if there is already an inbound connection
// from the same remote peer address as peerAddr.
//
// This intentionally matches on the full remote address, not just the host.
// TCP collisions require the same 4-tuple, so a connection from the same host
// but a different source port must not suppress a valid outbound dial.
//
// When OutboundSourcePort is set (listen-port reuse), an outbound connection
// to the same remote peer address may collide with an existing inbound
// connection if the peer also connected from its listening port. In that case
// the existing inbound connection should be treated as the reusable duplex
// connection, matching ouroboros-network's exact-address connection tracking.
func (c *ConnectionManager) HasInboundPeerAddress(
	peerAddr string,
) bool {
	// Only relevant when listen port reuse is enabled. With ephemeral
	// source ports, an outbound connection uses a different 4-tuple and
	// cannot collide with any inbound connection.
	if !c.tracksInboundPeerAddresses() {
		return false
	}
	targetAddr := NormalizePeerAddr(peerAddr)
	if targetAddr == "" {
		return false
	}
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	return c.inboundPeerAddrs[targetAddr] > 0
}

// NormalizePeerAddr canonicalizes a host:port string into the form used
// as the connmanager's transport-layer peer key: IPs are normalized to
// their canonical form, hostnames are lowercased, and DNS is never
// consulted. Exposed so that subscribers of InboundConnectionEvent can
// key on the same identity the connection manager does, and so the
// inbound-arrival path in peergov does not drift from
// inboundPeerAddrs / HasInboundPeerAddress.
func NormalizePeerAddr(peerAddr string) string {
	host, port, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return strings.ToLower(peerAddr)
	}
	if ip := net.ParseIP(host); ip != nil {
		return net.JoinHostPort(ip.String(), port)
	}
	return net.JoinHostPort(strings.ToLower(host), port)
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

// IsInboundConnection returns true if the given connection ID is an inbound
// connection (a remote peer connected to us). Inbound peers are clients
// pulling data from us and should not be treated as chain truth sources.
func (c *ConnectionManager) IsInboundConnection(
	connId ouroboros.ConnectionId,
) bool {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()
	if info, exists := c.connections[connId]; exists {
		return info.isInbound
	}
	return false
}

// HandleConnectionRecycleRequestedEvent closes a connection when
// a recycle request event is received.
func (c *ConnectionManager) HandleConnectionRecycleRequestedEvent(
	evt event.Event,
) {
	e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
	if !ok {
		return
	}
	conn := c.GetConnectionById(e.ConnectionId)
	if conn != nil {
		c.config.Logger.Info(
			"recycling connection on request",
			"connection_id", e.ConnectionId.String(),
			"reason", e.Reason,
		)
		if err := conn.Close(); err != nil {
			c.config.Logger.Debug(
				"failed to close recycled connection",
				"connection_id", e.ConnectionId.String(),
				"error", err,
			)
		}
	}
}
