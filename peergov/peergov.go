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

package peergov

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/topology"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultReconcileInterval         = 30 * time.Minute
	defaultMaxReconnectFailures      = 3
	defaultMinHotPeers               = 3
	defaultInactivityTimeout         = 10 * time.Minute
	defaultTestCooldown              = 5 * time.Minute
	defaultDenyDuration              = 30 * time.Minute
	defaultLedgerPeerRefreshInterval = 1 * time.Hour

	// Default peer limits (applied when config value is 0)
	defaultMaxColdPeers = 200
	defaultMaxWarmPeers = 50
	defaultMaxHotPeers  = 20
)

const (
	initialReconnectDelay  = 1 * time.Second
	maxReconnectDelay      = 128 * time.Second
	reconnectBackoffFactor = 2
)

type PeerGovernor struct {
	metrics               *peerGovernorMetrics
	reconcileTicker       *time.Ticker
	stopCh                chan struct{}
	denyList              map[string]time.Time // address -> expiry time
	peers                 []*Peer
	config                PeerGovernorConfig
	lastLedgerPeerRefresh atomic.Int64 // UnixNano timestamp of last ledger peer discovery
	mu                    sync.Mutex
}

type PeerGovernorConfig struct {
	PromRegistry         prometheus.Registerer
	Logger               *slog.Logger
	EventBus             *event.EventBus
	ConnManager          *connmanager.ConnectionManager
	PeerRequestFunc      func(peer *Peer) []string
	PeerTestFunc         func(address string) error // Custom peer test function
	ReconcileInterval    time.Duration
	MaxReconnectFailures int
	MinHotPeers          int
	InactivityTimeout    time.Duration
	TestCooldown         time.Duration // Min time between suitability tests
	DenyDuration         time.Duration // How long to deny failed peers
	DisableOutbound      bool

	// Ledger peer discovery configuration
	LedgerPeerProvider        LedgerPeerProvider // Provider for ledger peer information
	UseLedgerAfterSlot        int64              // Slot after which to enable ledger peers (-1 = disabled)
	LedgerPeerRefreshInterval time.Duration      // How often to refresh ledger peers

	// Peer limits (0 = use default, -1 = unlimited)
	MaxColdPeers int // Maximum cold peers to keep
	MaxWarmPeers int // Maximum warm peers to maintain
	MaxHotPeers  int // Maximum hot peers to promote
}

type peerGovernorMetrics struct {
	coldPeers        prometheus.Gauge
	warmPeers        prometheus.Gauge
	hotPeers         prometheus.Gauge
	activePeers      prometheus.Gauge
	establishedPeers prometheus.Gauge
	knownPeers       prometheus.Gauge
	// Churn counters
	coldPeersPromotions  prometheus.Counter
	warmPeersPromotions  prometheus.Counter
	warmPeersDemotions   prometheus.Counter
	increasedKnownPeers  prometheus.Counter
	decreasedKnownPeers  prometheus.Counter
	increasedActivePeers prometheus.Counter
	decreasedActivePeers prometheus.Counter
}

func NewPeerGovernor(cfg PeerGovernorConfig) *PeerGovernor {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = defaultReconcileInterval
	}
	if cfg.MaxReconnectFailures == 0 {
		cfg.MaxReconnectFailures = defaultMaxReconnectFailures
	}
	if cfg.MinHotPeers == 0 {
		cfg.MinHotPeers = defaultMinHotPeers
	}
	if cfg.InactivityTimeout == 0 {
		cfg.InactivityTimeout = defaultInactivityTimeout
	}
	if cfg.TestCooldown == 0 {
		cfg.TestCooldown = defaultTestCooldown
	}
	if cfg.DenyDuration == 0 {
		cfg.DenyDuration = defaultDenyDuration
	}
	if cfg.LedgerPeerRefreshInterval == 0 {
		cfg.LedgerPeerRefreshInterval = defaultLedgerPeerRefreshInterval
	}
	// Peer limits: 0 means use default, -1 means unlimited
	if cfg.MaxColdPeers == 0 {
		cfg.MaxColdPeers = defaultMaxColdPeers
	} else if cfg.MaxColdPeers < 0 {
		cfg.MaxColdPeers = 0 // 0 internally means unlimited
	}
	if cfg.MaxWarmPeers == 0 {
		cfg.MaxWarmPeers = defaultMaxWarmPeers
	} else if cfg.MaxWarmPeers < 0 {
		cfg.MaxWarmPeers = 0
	}
	if cfg.MaxHotPeers == 0 {
		cfg.MaxHotPeers = defaultMaxHotPeers
	} else if cfg.MaxHotPeers < 0 {
		cfg.MaxHotPeers = 0
	}
	cfg.Logger = cfg.Logger.With("component", "peergov")
	p := &PeerGovernor{
		config:   cfg,
		peers:    []*Peer{},
		denyList: make(map[string]time.Time),
	}
	if cfg.PromRegistry != nil {
		p.initMetrics()
	}
	return p
}

func (p *PeerGovernor) initMetrics() {
	promautoFactory := promauto.With(p.config.PromRegistry)
	p.metrics = &peerGovernorMetrics{}
	p.metrics.coldPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_cold",
		Help: "number of cold peers",
	})
	p.metrics.warmPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_warm",
		Help: "number of warm peers",
	})
	p.metrics.hotPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_hot",
		Help: "number of hot peers",
	})
	p.metrics.activePeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_ActivePeers",
		Help: "number of active peers",
	})
	p.metrics.establishedPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_EstablishedPeers",
		Help: "number of established peers",
	})
	p.metrics.knownPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_KnownPeers",
		Help: "number of known peers",
	})
	// Churn counters
	p.metrics.coldPeersPromotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_ColdPeersPromotions",
			Help: "number of cold peers promoted to warm",
		},
	)
	p.metrics.warmPeersPromotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmPeersPromotions",
			Help: "number of warm peers promoted to hot",
		},
	)
	p.metrics.warmPeersDemotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmPeersDemotions",
			Help: "number of hot peers demoted to warm",
		},
	)
	p.metrics.increasedKnownPeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_IncreasedKnownPeers",
			Help: "number of peers added to known set",
		},
	)
	p.metrics.decreasedKnownPeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_DecreasedKnownPeers",
			Help: "number of peers removed from known set",
		},
	)
	p.metrics.increasedActivePeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_IncreasedActivePeers",
			Help: "number of active peers increased",
		},
	)
	p.metrics.decreasedActivePeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_DecreasedActivePeers",
			Help: "number of active peers decreased",
		},
	)
}

// updatePeerMetrics updates the Prometheus metrics for peer counts.
// This function assumes p.mu is already held by the caller.
func (p *PeerGovernor) updatePeerMetrics() {
	if p.metrics == nil {
		return
	}
	// NOTE: Caller must hold p.mu

	coldCount := 0
	warmCount := 0
	hotCount := 0
	activeCount := 0
	establishedCount := 0
	knownCount := len(p.peers)

	for _, peer := range p.peers {
		switch peer.State {
		case PeerStateCold:
			coldCount++
		case PeerStateWarm:
			warmCount++
			// Warm peers have established connections
			if peer.Connection != nil {
				establishedCount++
			}
		case PeerStateHot:
			hotCount++
			// Hot peers have established connections and are active
			if peer.Connection != nil {
				establishedCount++
				activeCount++
			}
		}
	}

	p.metrics.coldPeers.Set(float64(coldCount))
	p.metrics.warmPeers.Set(float64(warmCount))
	p.metrics.hotPeers.Set(float64(hotCount))
	p.metrics.activePeers.Set(float64(activeCount))
	p.metrics.establishedPeers.Set(float64(establishedCount))
	p.metrics.knownPeers.Set(float64(knownCount))
}

func (p *PeerGovernor) Start(ctx context.Context) error {
	// Setup connmanager event listeners
	if p.config.EventBus != nil {
		p.config.EventBus.SubscribeFunc(
			connmanager.InboundConnectionEventType,
			p.handleInboundConnectionEvent,
		)
		p.config.EventBus.SubscribeFunc(
			connmanager.ConnectionClosedEventType,
			p.handleConnectionClosedEvent,
		)
	}
	// Start reconcile loop
	ticker := time.NewTicker(p.config.ReconcileInterval)
	stopCh := make(chan struct{})
	p.mu.Lock()
	p.reconcileTicker = ticker
	p.stopCh = stopCh
	p.mu.Unlock()
	go func(t *time.Ticker, stop <-chan struct{}) {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				p.reconcile()
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}(ticker, stopCh)
	// Start outbound connections
	p.startOutboundConnections()
	return nil
}

// Stop gracefully shuts down the peer governor
func (p *PeerGovernor) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.reconcileTicker != nil {
		p.reconcileTicker.Stop()
		if p.stopCh != nil {
			close(p.stopCh)
		}
		p.reconcileTicker = nil
		p.stopCh = nil
	}
}

func (p *PeerGovernor) LoadTopologyConfig(
	topologyConfig *topology.TopologyConfig,
) {
	if p.config.DisableOutbound {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Remove peers originally sourced from the topology
	tmpPeers := []*Peer{}
	for _, tmpPeer := range p.peers {
		if tmpPeer == nil {
			continue
		}
		if tmpPeer.Source == PeerSourceTopologyBootstrapPeer ||
			tmpPeer.Source == PeerSourceTopologyLocalRoot ||
			tmpPeer.Source == PeerSourceTopologyPublicRoot {
			continue
		}
		tmpPeers = append(tmpPeers, tmpPeer)
	}
	p.peers = tmpPeers
	// Add topology bootstrap peers
	for _, bootstrapPeer := range topologyConfig.BootstrapPeers {
		tmpAddress := net.JoinHostPort(
			bootstrapPeer.Address,
			strconv.FormatUint(uint64(bootstrapPeer.Port), 10),
		)
		p.peers = append(
			p.peers,
			&Peer{
				Address:           tmpAddress,
				NormalizedAddress: p.normalizeAddress(tmpAddress),
				Source:            PeerSourceTopologyBootstrapPeer,
				State:             PeerStateCold,
			},
		)
	}
	// Add topology local roots
	for _, localRoot := range topologyConfig.LocalRoots {
		for _, ap := range localRoot.AccessPoints {
			tmpAddress := net.JoinHostPort(
				ap.Address,
				strconv.FormatUint(uint64(ap.Port), 10),
			)
			tmpPeer := &Peer{
				Address:           tmpAddress,
				NormalizedAddress: p.normalizeAddress(tmpAddress),
				Source:            PeerSourceTopologyLocalRoot,
				State:             PeerStateCold,
				Sharable:          localRoot.Advertise,
			}
			// Check for existing peer with this address
			existingPeerIdx := -1
			for i, existingPeer := range p.peers {
				if existingPeer != nil && existingPeer.Address == tmpAddress {
					existingPeerIdx = i
					break
				}
			}
			if existingPeerIdx >= 0 {
				existingPeer := p.peers[existingPeerIdx]
				// Preserve active inbound connection state
				if existingPeer.Source == PeerSourceInboundConn &&
					existingPeer.Connection != nil {
					tmpPeer.Connection = existingPeer.Connection
					tmpPeer.State = existingPeer.State
					tmpPeer.ReconnectCount = existingPeer.ReconnectCount
					tmpPeer.ReconnectDelay = existingPeer.ReconnectDelay
					tmpPeer.Sharable = existingPeer.Sharable
				}
				// Remove the existing peer
				p.peers = slices.Delete(
					p.peers, existingPeerIdx,
					existingPeerIdx+1)
			}
			p.peers = append(p.peers, tmpPeer)
		}
	}
	// Add topology public roots
	for _, publicRoot := range topologyConfig.PublicRoots {
		for _, ap := range publicRoot.AccessPoints {
			tmpAddress := net.JoinHostPort(
				ap.Address,
				strconv.FormatUint(uint64(ap.Port), 10),
			)
			tmpPeer := &Peer{
				Address:           tmpAddress,
				NormalizedAddress: p.normalizeAddress(tmpAddress),
				Source:            PeerSourceTopologyPublicRoot,
				State:             PeerStateCold,
				Sharable:          publicRoot.Advertise,
			}
			// Check for existing peer with this address
			existingPeerIdx := -1
			for i, existingPeer := range p.peers {
				if existingPeer != nil && existingPeer.Address == tmpAddress {
					existingPeerIdx = i
					break
				}
			}
			if existingPeerIdx >= 0 {
				existingPeer := p.peers[existingPeerIdx]
				// Preserve active inbound connection state
				if existingPeer.Source == PeerSourceInboundConn &&
					existingPeer.Connection != nil {
					tmpPeer.Connection = existingPeer.Connection
					tmpPeer.State = existingPeer.State
					tmpPeer.ReconnectCount = existingPeer.ReconnectCount
					tmpPeer.ReconnectDelay = existingPeer.ReconnectDelay
					tmpPeer.Sharable = existingPeer.Sharable
				}
				// Remove the existing peer
				p.peers = slices.Delete(
					p.peers, existingPeerIdx,
					existingPeerIdx+1)
			}
			p.peers = append(p.peers, tmpPeer)
		}
	}
	p.updatePeerMetrics()
}

func (p *PeerGovernor) GetPeers() []Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret := make([]Peer, 0, len(p.peers))
	for _, peer := range p.peers {
		if peer != nil {
			ret = append(ret, *peer)
		}
	}
	return ret
}

func (p *PeerGovernor) AddPeer(address string, source PeerSource) {
	normalized := p.normalizeAddress(address)
	p.mu.Lock()
	defer p.mu.Unlock()
	// Check deny list before adding
	if p.isDeniedLocked(normalized) {
		p.config.Logger.Debug(
			"not adding denied peer",
			"address", address,
		)
		return
	}
	// Check if already exists
	for _, peer := range p.peers {
		if peer != nil && peer.Address == address {
			return
		}
	}
	newPeer := &Peer{
		Address:           address,
		NormalizedAddress: normalized,
		Source:            source,
		State:             PeerStateCold,
	}
	// Gossip-discovered peers are sharable since they were already shared
	if source == PeerSourceP2PGossip {
		newPeer.Sharable = true
	}
	p.peers = append(p.peers, newPeer)
	p.updatePeerMetrics()
	if source == PeerSourceP2PGossip && p.metrics != nil {
		p.metrics.increasedKnownPeers.Inc()
	}
	if p.config.EventBus != nil {
		reason := "manual"
		switch source {
		case PeerSourceP2PGossip:
			reason = "peer sharing"
		case PeerSourceTopologyBootstrapPeer,
			PeerSourceTopologyLocalRoot,
			PeerSourceTopologyPublicRoot:
			reason = "topology"
		case PeerSourceInboundConn:
			reason = "inbound connection"
		}
		p.config.EventBus.Publish(
			PeerAddedEventType,
			event.NewEvent(
				PeerAddedEventType,
				PeerStateChangeEvent{Address: address, Reason: reason},
			),
		)
	}
}

// discoverLedgerPeers discovers peers from on-chain stake pool relay registrations.
// This method is called during reconciliation if ledger peers are enabled.
func (p *PeerGovernor) discoverLedgerPeers() {
	// Check if ledger peer provider is configured
	if p.config.LedgerPeerProvider == nil {
		return
	}

	// Check UseLedgerAfterSlot threshold first (before claiming refresh)
	if p.config.UseLedgerAfterSlot < 0 {
		// Ledger peers are disabled
		return
	}
	if p.config.UseLedgerAfterSlot > 0 {
		currentSlot := p.config.LedgerPeerProvider.CurrentSlot()
		// Safe conversion: UseLedgerAfterSlot is already checked to be > 0
		useLedgerAfterSlot := uint64(p.config.UseLedgerAfterSlot) // #nosec G115
		if currentSlot < useLedgerAfterSlot {
			p.config.Logger.Debug(
				"ledger peers not yet enabled",
				"current_slot", currentSlot,
				"use_ledger_after_slot", p.config.UseLedgerAfterSlot,
			)
			return
		}
	}

	// Atomically check and claim the refresh to prevent concurrent discoveries.
	// Use CompareAndSwap to ensure only one goroutine proceeds.
	now := time.Now().UnixNano()
	lastRefresh := p.lastLedgerPeerRefresh.Load()
	if time.Duration(now-lastRefresh) < p.config.LedgerPeerRefreshInterval {
		return
	}
	if !p.lastLedgerPeerRefresh.CompareAndSwap(lastRefresh, now) {
		// Another goroutine claimed the refresh
		return
	}

	// Get pool relays from ledger
	relays, err := p.config.LedgerPeerProvider.GetPoolRelays()
	if err != nil {
		p.config.Logger.Error(
			"failed to get ledger peers",
			"error", err,
		)
		// Reset timestamp to allow retry on next reconciliation cycle
		// rather than waiting for the full refresh interval
		p.lastLedgerPeerRefresh.Store(lastRefresh)
		return
	}

	// Track how many peers we added
	addedCount := 0

	// Add each relay as a peer (with deduplication)
	for _, relay := range relays {
		addresses := relay.Addresses()
		for _, addr := range addresses {
			if p.addLedgerPeer(addr) {
				addedCount++
			}
		}
	}

	if addedCount > 0 {
		p.config.Logger.Info(
			"discovered ledger peers",
			"added", addedCount,
			"total_relays", len(relays),
		)
	} else {
		p.config.Logger.Debug(
			"ledger peer discovery complete",
			"total_relays", len(relays),
			"new_peers", 0,
		)
	}
}

// addLedgerPeer adds a peer from ledger discovery with deduplication.
// Returns true if the peer was added, false if it already exists or is denied.
func (p *PeerGovernor) addLedgerPeer(address string) bool {
	normalized := p.normalizeAddress(address)

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check deny list
	if p.isDeniedLocked(normalized) {
		return false
	}

	// Check for existing peer using cached NormalizedAddress
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.NormalizedAddress == normalized || peer.Address == address {
			return false // Already exists
		}
	}

	// Add as new peer
	newPeer := &Peer{
		Address:           address,
		NormalizedAddress: normalized,
		Source:            PeerSourceP2PLedger,
		State:             PeerStateCold,
		Sharable:          true, // Ledger peers are public relays
	}
	p.peers = append(p.peers, newPeer)
	p.updatePeerMetrics()

	if p.metrics != nil {
		p.metrics.increasedKnownPeers.Inc()
	}

	if p.config.EventBus != nil {
		p.config.EventBus.Publish(
			PeerAddedEventType,
			event.NewEvent(
				PeerAddedEventType,
				PeerStateChangeEvent{Address: address, Reason: "ledger"},
			),
		)
	}

	return true
}

// normalizeAddress returns a canonical form of an address for deduplication.
// This normalizes IP addresses to their canonical string form and lowercases hostnames.
func (p *PeerGovernor) normalizeAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	// Parse IP to normalize format (e.g., ::1 vs 0:0:0:0:0:0:0:1)
	if ip := net.ParseIP(host); ip != nil {
		return net.JoinHostPort(ip.String(), port)
	}
	// For hostnames, lowercase them
	return net.JoinHostPort(strings.ToLower(host), port)
}

func (p *PeerGovernor) peerIndexByAddress(address string) int {
	for idx, tmpPeer := range p.peers {
		if tmpPeer != nil && tmpPeer.Address == address {
			return idx
		}
	}
	return -1
}

func (p *PeerGovernor) peerIndexByConnId(connId ouroboros.ConnectionId) int {
	for idx, tmpPeer := range p.peers {
		if tmpPeer == nil || tmpPeer.Connection == nil {
			continue
		}
		if tmpPeer.Connection.Id == connId {
			return idx
		}
	}
	return -1
}

func (p *PeerGovernor) SetPeerHotByConnId(connId ouroboros.ConnectionId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peerIdx := p.peerIndexByConnId(connId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].State = PeerStateHot
		p.peers[peerIdx].LastActivity = time.Now()
		p.updatePeerMetrics()
	}
}

// UpdatePeerBlockFetchObservation updates block fetch metrics for the peer with
// the given connection ID. This method is thread-safe.
func (p *PeerGovernor) UpdatePeerBlockFetchObservation(
	connId ouroboros.ConnectionId,
	latencyMs float64,
	success bool,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := p.peerIndexByConnId(connId)
	if idx != -1 && p.peers[idx] != nil {
		p.peers[idx].UpdateBlockFetchObservation(latencyMs, success)
	}
}

// UpdatePeerConnectionStability updates connection stability for the peer with
// the given connection ID. This method is thread-safe.
func (p *PeerGovernor) UpdatePeerConnectionStability(
	connId ouroboros.ConnectionId,
	observed float64,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := p.peerIndexByConnId(connId)
	if idx != -1 && p.peers[idx] != nil {
		p.peers[idx].UpdateConnectionStability(observed)
	}
}

// DenyPeer adds a peer to the deny list for the specified duration.
// If duration is 0, the configured default duration is used.
// This method is thread-safe.
func (p *PeerGovernor) DenyPeer(address string, duration time.Duration) {
	if duration == 0 {
		duration = p.config.DenyDuration
	}
	normalized := p.normalizeAddress(address)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.denyList[normalized] = time.Now().Add(duration)
	p.config.Logger.Debug(
		"peer added to deny list",
		"address", address,
		"duration", duration,
	)
}

// IsDenied checks if a peer is currently on the deny list.
// Returns true if the peer is denied and the denial has not expired.
// This method is thread-safe.
func (p *PeerGovernor) IsDenied(address string) bool {
	normalized := p.normalizeAddress(address)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isDeniedLocked(normalized)
}

// isDeniedLocked checks if a peer is on the deny list.
// This method assumes the mutex is already held by the caller.
func (p *PeerGovernor) isDeniedLocked(address string) bool {
	expiry, exists := p.denyList[address]
	if !exists {
		return false
	}
	if time.Now().After(expiry) {
		// Expired, remove from deny list
		delete(p.denyList, address)
		return false
	}
	return true
}

// cleanupDenyList removes expired entries from the deny list.
// This method assumes the mutex is already held by the caller.
func (p *PeerGovernor) cleanupDenyList() {
	now := time.Now()
	for address, expiry := range p.denyList {
		if now.After(expiry) {
			delete(p.denyList, address)
		}
	}
}

// TestPeer tests a peer's suitability by attempting a connection and verifying
// the Ouroboros protocol handshake succeeds. Returns true if the peer is
// suitable, false otherwise. Results are cached to avoid excessive testing.
// This method is thread-safe.
func (p *PeerGovernor) TestPeer(address string) (bool, error) {
	normalized := p.normalizeAddress(address)
	p.mu.Lock()

	// Find or create peer entry
	var peer *Peer
	if idx := p.peerIndexByAddress(address); idx == -1 {
		// Peer not known yet, create temporary entry to track test result
		peer = &Peer{
			Address:           address,
			NormalizedAddress: normalized,
			Source:            PeerSourceUnknown,
			State:             PeerStateCold,
		}
		p.peers = append(p.peers, peer)
		p.updatePeerMetrics()
	} else {
		peer = p.peers[idx]
	}

	// Check if recently tested (within cooldown)
	if !peer.LastTestTime.IsZero() &&
		time.Since(peer.LastTestTime) < p.config.TestCooldown {
		result := peer.LastTestResult == TestResultPass
		p.mu.Unlock()
		if result {
			return true, nil
		}
		return false, errors.New("peer failed previous test")
	}

	p.mu.Unlock()

	// Perform the test (outside lock to avoid blocking)
	var testErr error
	if p.config.PeerTestFunc != nil {
		// Use custom test function if provided
		testErr = p.config.PeerTestFunc(address)
	} else if p.config.ConnManager != nil {
		// Default: attempt connection via ConnManager
		conn, err := p.config.ConnManager.CreateOutboundConn(address)
		if err != nil {
			testErr = err
		} else {
			// Connection succeeded, close it since this is just a test
			conn.Close()
		}
	} else {
		testErr = errors.New("no test function or connection manager configured")
	}

	// Update test result
	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-find peer in case slice changed
	idx := p.peerIndexByAddress(address)
	if idx == -1 {
		// Peer was removed during test, nothing to update
		if testErr != nil {
			return false, testErr
		}
		return true, nil
	}
	peer = p.peers[idx]

	peer.LastTestTime = time.Now()
	if testErr != nil {
		peer.LastTestResult = TestResultFail
		// Add to deny list (we already hold the lock)
		// Use normalized address for consistent deny list lookups
		p.denyList[p.normalizeAddress(address)] = time.Now().Add(p.config.DenyDuration)
		p.config.Logger.Debug(
			"peer suitability test failed, added to deny list",
			"address", address,
			"error", testErr,
			"deny_duration", p.config.DenyDuration,
		)
		return false, testErr
	}

	peer.LastTestResult = TestResultPass
	p.config.Logger.Debug(
		"peer suitability test passed",
		"address", address,
	)
	return true, nil
}

func (p *PeerGovernor) startOutboundConnections() {
	// Skip outbound connections if disabled
	if p.config.DisableOutbound {
		p.config.Logger.Info(
			"outbound connections disabled, skipping outbound connections",
			"role", "client",
		)
		return
	}

	p.config.Logger.Debug(
		"starting connections",
		"role", "client",
	)

	for _, tmpPeer := range p.peers {
		if tmpPeer != nil {
			go p.createOutboundConnection(tmpPeer)
		}
	}
}

func (p *PeerGovernor) createOutboundConnection(peer *Peer) {
	if peer == nil {
		return
	}
	for {
		conn, err := p.config.ConnManager.CreateOutboundConn(peer.Address)
		if err == nil {
			connId := conn.Id()
			p.mu.Lock()
			peer.ReconnectCount = 0
			peer.setConnection(conn, true)
			peer.State = PeerStateWarm
			p.updatePeerMetrics()
			p.mu.Unlock()
			// Generate event
			if p.config.EventBus != nil {
				p.config.EventBus.Publish(
					OutboundConnectionEventType,
					event.NewEvent(
						OutboundConnectionEventType,
						OutboundConnectionEvent{
							ConnectionId: connId,
						},
					),
				)
			}
			return
		}
		p.config.Logger.Error(
			fmt.Sprintf(
				"outbound: failed to establish connection to %s: %s",
				peer.Address,
				err,
			),
		)
		p.mu.Lock()
		if peer.ReconnectDelay == 0 {
			peer.ReconnectDelay = initialReconnectDelay
		} else if peer.ReconnectDelay < maxReconnectDelay {
			peer.ReconnectDelay = peer.ReconnectDelay * reconnectBackoffFactor
		}
		peer.ReconnectCount += 1
		p.mu.Unlock()
		p.config.Logger.Info(
			fmt.Sprintf(
				"outbound: delaying %s (retry %d) before reconnecting to %s",
				peer.ReconnectDelay,
				peer.ReconnectCount,
				peer.Address,
			),
		)
		time.Sleep(peer.ReconnectDelay)
	}
}

func (p *PeerGovernor) handleInboundConnectionEvent(evt event.Event) {
	e := evt.Data.(connmanager.InboundConnectionEvent)
	address := e.RemoteAddr.String()
	normalized := p.normalizeAddress(address)

	p.mu.Lock()
	defer p.mu.Unlock()

	var tmpPeer *Peer
	peerIdx := p.peerIndexByAddress(address)
	if peerIdx == -1 {
		tmpPeer = &Peer{
			Address:           address,
			NormalizedAddress: normalized,
			Source:            PeerSourceInboundConn,
			State:             PeerStateCold,
		}
		// Add inbound peer
		p.peers = append(
			p.peers,
			tmpPeer,
		)
	} else {
		tmpPeer = p.peers[peerIdx]
	}
	if tmpPeer == nil {
		return
	}
	if p.config.ConnManager != nil {
		conn := p.config.ConnManager.GetConnectionById(e.ConnectionId)
		if conn != nil {
			tmpPeer.setConnection(conn, false)
			if tmpPeer.Connection != nil {
				tmpPeer.Sharable = tmpPeer.Connection.VersionData.PeerSharing()
				tmpPeer.State = PeerStateWarm
			}
		}
	}
	p.updatePeerMetrics()
}

func (p *PeerGovernor) handleConnectionClosedEvent(evt event.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := evt.Data.(connmanager.ConnectionClosedEvent)
	if e.Error != nil {
		p.config.Logger.Error(
			fmt.Sprintf(
				"unexpected connection failure: %s",
				e.Error,
			),
			"connection_id", e.ConnectionId.String(),
		)
	} else {
		p.config.Logger.Info("connection closed",
			"connection_id", e.ConnectionId.String(),
		)
	}
	peerIdx := p.peerIndexByConnId(e.ConnectionId)
	if peerIdx != -1 && p.peers[peerIdx] != nil {
		p.peers[peerIdx].Connection = nil
		p.peers[peerIdx].State = PeerStateCold
		p.updatePeerMetrics()
		if p.peers[peerIdx].Source != PeerSourceInboundConn {
			go p.createOutboundConnection(p.peers[peerIdx])
		}
	}
}

func (p *PeerGovernor) reconcile() {
	p.mu.Lock()

	p.config.Logger.Debug("starting peer reconcile")

	// Cleanup expired deny list entries
	p.cleanupDenyList()

	// Track changes for metrics
	var coldPromotions, warmPromotions, warmDemotions, knownRemoved, activeIncreased, activeDecreased int

	// Demotion/Promotion Logic
	for i := len(p.peers) - 1; i >= 0; i-- {
		peer := p.peers[i]
		if peer == nil {
			continue
		}
		switch peer.State {
		case PeerStateHot:
			// Demote if inactive (no connection or last activity > timeout)
			if peer.Connection == nil ||
				time.Since(peer.LastActivity) > p.config.InactivityTimeout {
				p.peers[i].State = PeerStateWarm
				warmDemotions++
				activeDecreased++
				p.config.Logger.Info(
					"demoted peer to warm due to inactivity",
					"address",
					peer.Address,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerDemotedEventType,
						event.NewEvent(
							PeerDemotedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "inactive",
							},
						),
					)
				}
			}
		case PeerStateWarm:
			// Do not promote warm peers here; collect them and perform
			// score-based promotion later. This avoids unconditional
			// promotion and lets the scoring policy decide which warm
			// peers to promote when ensuring MinHotPeers.
			// Note: warm peers remain warm unless promoted in scoring block below.
		case PeerStateCold:
			// Promote to warm if connection exists
			if peer.Connection != nil {
				p.peers[i].State = PeerStateWarm
				coldPromotions++
				p.config.Logger.Info(
					"promoted peer to warm",
					"address",
					peer.Address,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerPromotedEventType,
						event.NewEvent(
							PeerPromotedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "connection established",
							},
						),
					)
				}
			} else if peer.ReconnectCount > p.config.MaxReconnectFailures {
				knownRemoved++
				p.config.Logger.Info(
					"removing failed peer",
					"address",
					peer.Address,
					"failures",
					peer.ReconnectCount,
				)
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerRemovedEventType,
						event.NewEvent(
							PeerRemovedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "excessive failures",
							},
						),
					)
				}
				// Remove from slice (safe while iterating backwards)
				p.peers = append(p.peers[:i], p.peers[i+1:]...)
			}
		}
	}

	// Ensure minimum hot peers (simple: promote more warm if needed)
	hotCount := 0
	for _, peer := range p.peers {
		if peer != nil && peer.State == PeerStateHot {
			hotCount++
		}
	}
	if hotCount < p.config.MinHotPeers {
		// Score-based selection: collect warm peers with connections, compute scores,
		// sort descending and promote top N required to reach MinHotPeers.
		candidates := []*Peer{}
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateWarm &&
				peer.Connection != nil {
				peer.UpdatePeerScore()
				candidates = append(candidates, peer)
			}
		}
		// Sort candidates by PerformanceScore descending using cmp.Compare
		slices.SortFunc(candidates, func(a, b *Peer) int {
			return cmp.Compare(b.PerformanceScore, a.PerformanceScore)
		})

		needed := p.config.MinHotPeers - hotCount
		for i := 0; i < len(candidates) && i < needed; i++ {
			peer := candidates[i]
			peer.State = PeerStateHot
			peer.LastActivity = time.Now()
			warmPromotions++
			activeIncreased++
			p.config.Logger.Info(
				"promoted peer to hot to meet minimum (score-based)",
				"address",
				peer.Address,
				"score",
				peer.PerformanceScore,
			)
			if p.config.EventBus != nil {
				p.config.EventBus.Publish(
					PeerPromotedEventType,
					event.NewEvent(
						PeerPromotedEventType,
						PeerStateChangeEvent{
							Address: peer.Address,
							Reason:  "minimum hot peers (score)",
						},
					),
				)
			}
			hotCount++
		}
	}

	// Enforce peer limits by removing excess peers
	// Priority order (highest to lowest): Topology > Gossip > Ledger > Inbound > Unknown
	p.enforcePeerLimits(&knownRemoved)

	// Collect eligible peers for peer sharing
	var eligiblePeers []*Peer
	if p.config.PeerRequestFunc != nil {
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateHot &&
				peer.Connection != nil && peer.Source != PeerSourceTopologyLocalRoot {
				eligiblePeers = append(eligiblePeers, peer)
			}
		}
	}

	// Update metrics
	p.updatePeerMetrics()
	if p.metrics != nil {
		p.metrics.coldPeersPromotions.Add(float64(coldPromotions))
		p.metrics.warmPeersPromotions.Add(float64(warmPromotions))
		p.metrics.warmPeersDemotions.Add(float64(warmDemotions))
		p.metrics.decreasedKnownPeers.Add(float64(knownRemoved))
		p.metrics.increasedActivePeers.Add(float64(activeIncreased))
		p.metrics.decreasedActivePeers.Add(float64(activeDecreased))
	}

	p.config.Logger.Debug(
		"peer reconcile completed",
		"changes",
		coldPromotions+warmPromotions+warmDemotions+knownRemoved,
	)

	// Peer Discovery via Peer Sharing (outside lock)
	p.mu.Unlock()
	for _, peer := range eligiblePeers {
		addrs := p.config.PeerRequestFunc(peer)
		for _, addr := range addrs {
			p.AddPeer(addr, PeerSourceP2PGossip)
		}
	}

	// Discover peers from ledger (stake pool relays)
	p.discoverLedgerPeers()
}

// enforcePeerLimits removes excess peers when limits are exceeded.
// It prioritizes keeping topology peers and removes lower-priority peers first.
// Must be called with p.mu held.
func (p *PeerGovernor) enforcePeerLimits(removedCount *int) {
	// Enforce hot peer limit
	if p.config.MaxHotPeers > 0 {
		p.enforceStateLimit(PeerStateHot, p.config.MaxHotPeers, removedCount)
	}

	// Enforce warm peer limit
	if p.config.MaxWarmPeers > 0 {
		p.enforceStateLimit(PeerStateWarm, p.config.MaxWarmPeers, removedCount)
	}

	// Enforce cold peer limit
	if p.config.MaxColdPeers > 0 {
		p.enforceStateLimit(PeerStateCold, p.config.MaxColdPeers, removedCount)
	}
}

// enforceStateLimit removes excess peers in a given state.
// Must be called with p.mu held.
func (p *PeerGovernor) enforceStateLimit(
	state PeerState,
	limit int,
	removedCount *int,
) {
	// Collect peers in this state
	var peersInState []*Peer
	for _, peer := range p.peers {
		if peer != nil && peer.State == state {
			peersInState = append(peersInState, peer)
		}
	}

	// Check if we're over the limit
	excess := len(peersInState) - limit
	if excess <= 0 {
		return
	}

	// Sort peers by removal priority (lowest priority first)
	// Priority order (highest to lowest):
	// - Topology peers (LocalRoot, PublicRoot, Bootstrap) - never remove
	// - Gossip peers
	// - Ledger peers
	// - Inbound peers
	// - Unknown peers
	slices.SortFunc(peersInState, func(a, b *Peer) int {
		// First compare by source priority (lower priority = remove first)
		aPriority := p.peerSourcePriority(a.Source)
		bPriority := p.peerSourcePriority(b.Source)
		if aPriority != bPriority {
			return cmp.Compare(aPriority, bPriority)
		}
		// Same priority: lower score = remove first
		return cmp.Compare(a.PerformanceScore, b.PerformanceScore)
	})

	// Remove excess peers (they're sorted lowest priority first)
	removed := 0
	for i := 0; i < len(peersInState) && removed < excess; i++ {
		peer := peersInState[i]

		// Never remove topology peers
		if p.isTopologyPeer(peer.Source) {
			continue
		}

		// Find and remove from main peer list
		for j := len(p.peers) - 1; j >= 0; j-- {
			if p.peers[j] == peer {
				p.config.Logger.Info(
					"removing peer due to limit exceeded",
					"address", peer.Address,
					"state", state,
					"source", peer.Source,
					"limit", limit,
				)
				// Close connection if peer has one (for warm/hot peers)
				if peer.Connection != nil && p.config.ConnManager != nil {
					conn := p.config.ConnManager.GetConnectionById(
						peer.Connection.Id,
					)
					if conn != nil {
						conn.Close()
					}
				}
				if p.config.EventBus != nil {
					p.config.EventBus.Publish(
						PeerRemovedEventType,
						event.NewEvent(
							PeerRemovedEventType,
							PeerStateChangeEvent{
								Address: peer.Address,
								Reason:  "limit exceeded",
							},
						),
					)
				}
				p.peers = append(p.peers[:j], p.peers[j+1:]...)
				removed++
				*removedCount++
				break
			}
		}
	}

	if removed > 0 {
		p.config.Logger.Debug(
			"enforced peer limit",
			"state", state,
			"limit", limit,
			"removed", removed,
		)
	}
}

// peerSourcePriority returns a priority value for a peer source.
// Higher values = higher priority (less likely to be removed).
func (p *PeerGovernor) peerSourcePriority(source PeerSource) int {
	switch source {
	case PeerSourceTopologyLocalRoot,
		PeerSourceTopologyPublicRoot,
		PeerSourceTopologyBootstrapPeer:
		return 100 // Topology peers - highest priority, never removed
	case PeerSourceP2PGossip:
		return 50 // Gossip peers - medium-high priority
	case PeerSourceP2PLedger:
		return 30 // Ledger peers - medium priority
	case PeerSourceInboundConn:
		return 20 // Inbound peers - lower priority
	default:
		return 10 // Unknown - lowest priority
	}
}

// isTopologyPeer returns true if the peer source is a topology source.
func (p *PeerGovernor) isTopologyPeer(source PeerSource) bool {
	switch source {
	case PeerSourceTopologyLocalRoot,
		PeerSourceTopologyPublicRoot,
		PeerSourceTopologyBootstrapPeer:
		return true
	default:
		return false
	}
}
