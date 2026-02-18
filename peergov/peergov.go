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
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultReconcileInterval         = 30 * time.Minute
	defaultMaxReconnectFailures      = 3
	defaultMinHotPeers               = 3
	defaultInactivityTimeout         = 10 * time.Minute
	defaultTestCooldown              = 5 * time.Minute
	defaultDenyDuration              = 30 * time.Minute
	defaultLedgerPeerRefreshInterval = 1 * time.Hour

	// Default peer targets (applied when config value is 0)
	defaultTargetNumberOfKnownPeers       = 150
	defaultTargetNumberOfEstablishedPeers = 50
	defaultTargetNumberOfActivePeers      = 20
	defaultTargetNumberOfRootPeers        = 60

	// Default per-source quotas for active peers
	defaultActivePeersTopologyQuota = 3
	defaultActivePeersGossipQuota   = 12
	defaultActivePeersLedgerQuota   = 5

	// Default churn configuration
	defaultGossipChurnInterval     = 5 * time.Minute
	defaultGossipChurnPercent      = 0.20 // 20%
	defaultPublicRootChurnInterval = 30 * time.Minute
	defaultPublicRootChurnPercent  = 0.20 // 20%
	defaultMinScoreThreshold       = 0.3

	// Default inbound peer validation configuration
	defaultInboundHotScoreThreshold = 0.6 // Higher threshold for inbound peers
	defaultInboundMinTenure         = 10 * time.Minute

	// Default bootstrap exit configuration
	defaultMinLedgerPeersForExit = 5
	defaultSyncProgressForExit   = 0.95 // 95%
)

const (
	initialReconnectDelay  = 1 * time.Second
	maxReconnectDelay      = 128 * time.Second
	reconnectBackoffFactor = 2
	inboundCheckDelay      = 30 * time.Second
)

// Peer source priority values for removal decisions.
// Higher values = higher priority (less likely to be removed).
const (
	peerPriorityTopology = 100 // Topology peers - highest priority, never removed
	peerPriorityGossip   = 50  // Gossip peers - medium-high priority
	peerPriorityLedger   = 30  // Ledger peers - medium priority
	peerPriorityInbound  = 20  // Inbound peers - lower priority
	peerPriorityUnknown  = 10  // Unknown - lowest priority
)

type PeerGovernor struct {
	metrics               *peerGovernorMetrics
	reconcileTicker       *time.Ticker
	gossipChurnTicker     *time.Ticker
	publicRootChurnTicker *time.Ticker
	stopCh                chan struct{}
	ctx                   context.Context      // Context for cancellation
	denyList              map[string]time.Time // address -> expiry time
	peers                 []*Peer
	config                PeerGovernorConfig
	lastLedgerPeerRefresh atomic.Int64 // UnixNano timestamp of last ledger peer discovery
	bootstrapExited       bool         // Whether bootstrap peers have been exited
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

	// Peer targets (0 = use default, -1 = unlimited)
	// These are goals the system works toward, not hard limits.
	TargetNumberOfKnownPeers       int // Target count of known (cold) peers
	TargetNumberOfEstablishedPeers int // Target count of established (warm) peers
	TargetNumberOfActivePeers      int // Target count of active (hot) peers
	TargetNumberOfRootPeers        int // Target count of root peers from topology

	// Per-source quotas within TargetNumberOfActivePeers
	// These specify how active peer slots are distributed by source.
	// Sum should not exceed TargetNumberOfActivePeers.
	ActivePeersTopologyQuota int // Active peer slots for topology sources (local + public roots)
	ActivePeersGossipQuota   int // Active peer slots for gossip sources
	ActivePeersLedgerQuota   int // Active peer slots for ledger sources

	// Scoring configuration
	EMAAlpha float64 // EMA smoothing factor for scoring metrics (0 = use default 0.2)

	// Churn configuration
	// Gossip churn is aggressive - demotes to cold (closes connection)
	GossipChurnInterval time.Duration // How often to churn gossip peers (0 = default 5min)
	GossipChurnPercent  float64       // Fraction of hot gossip peers to churn (0 = default 0.20)

	// Public root churn is conservative - demotes to warm (keeps connection)
	PublicRootChurnInterval time.Duration // How often to churn public roots (0 = default 30min)
	PublicRootChurnPercent  float64       // Fraction of hot public roots to churn (0 = default 0.20)

	// Score threshold below which peers are always churned
	MinScoreThreshold float64 // Minimum acceptable score (0 = default 0.3)

	// Inbound peer validation configuration
	// Inbound peers require higher scores and minimum tenure before hot promotion
	InboundHotScoreThreshold float64       // Min score for inbound hot (0 = default 0.6)
	InboundMinTenure         time.Duration // Min time as warm before hot (0 = default 10min)

	// Bootstrap peer exit configuration
	// Bootstrap peers are used during initial sync but should be exited once
	// the node has established sufficient ledger peers or reached sync progress.
	MinLedgerPeersForExit int                  // Min ledger peers to exit bootstrap (0 = default 5)
	SyncProgressForExit   float64              // Sync progress threshold to exit (0 = default 0.95)
	AutoBootstrapRecovery *bool                // Re-enable bootstrap if hot peers drop too low (nil = default true)
	SyncProgressProvider  SyncProgressProvider // Provider for current sync progress
}

// pendingEvent holds an event to publish after releasing locks.
type pendingEvent struct {
	eventType event.EventType
	data      any
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
	// Peer targets: 0 means use default, -1 means unlimited
	if cfg.TargetNumberOfKnownPeers == 0 {
		cfg.TargetNumberOfKnownPeers = defaultTargetNumberOfKnownPeers
	} else if cfg.TargetNumberOfKnownPeers < 0 {
		cfg.TargetNumberOfKnownPeers = 0 // 0 internally means unlimited
	}
	if cfg.TargetNumberOfEstablishedPeers == 0 {
		cfg.TargetNumberOfEstablishedPeers = defaultTargetNumberOfEstablishedPeers
	} else if cfg.TargetNumberOfEstablishedPeers < 0 {
		cfg.TargetNumberOfEstablishedPeers = 0
	}
	if cfg.TargetNumberOfActivePeers == 0 {
		cfg.TargetNumberOfActivePeers = defaultTargetNumberOfActivePeers
	} else if cfg.TargetNumberOfActivePeers < 0 {
		cfg.TargetNumberOfActivePeers = 0
	}
	if cfg.TargetNumberOfRootPeers == 0 {
		cfg.TargetNumberOfRootPeers = defaultTargetNumberOfRootPeers
	} else if cfg.TargetNumberOfRootPeers < 0 {
		cfg.TargetNumberOfRootPeers = 0
	}
	// Per-source quotas: 0 means use default
	if cfg.ActivePeersTopologyQuota == 0 {
		cfg.ActivePeersTopologyQuota = defaultActivePeersTopologyQuota
	}
	if cfg.ActivePeersGossipQuota == 0 {
		cfg.ActivePeersGossipQuota = defaultActivePeersGossipQuota
	}
	if cfg.ActivePeersLedgerQuota == 0 {
		cfg.ActivePeersLedgerQuota = defaultActivePeersLedgerQuota
	}
	// Churn configuration: 0 means use default
	if cfg.GossipChurnInterval == 0 {
		cfg.GossipChurnInterval = defaultGossipChurnInterval
	}
	if cfg.GossipChurnPercent == 0 {
		cfg.GossipChurnPercent = defaultGossipChurnPercent
	}
	if cfg.PublicRootChurnInterval == 0 {
		cfg.PublicRootChurnInterval = defaultPublicRootChurnInterval
	}
	if cfg.PublicRootChurnPercent == 0 {
		cfg.PublicRootChurnPercent = defaultPublicRootChurnPercent
	}
	if cfg.MinScoreThreshold == 0 {
		cfg.MinScoreThreshold = defaultMinScoreThreshold
	}
	// Inbound peer validation defaults
	if cfg.InboundHotScoreThreshold == 0 {
		cfg.InboundHotScoreThreshold = defaultInboundHotScoreThreshold
	}
	if cfg.InboundMinTenure == 0 {
		cfg.InboundMinTenure = defaultInboundMinTenure
	}
	// Bootstrap exit configuration defaults
	if cfg.MinLedgerPeersForExit == 0 {
		cfg.MinLedgerPeersForExit = defaultMinLedgerPeersForExit
	}
	if cfg.SyncProgressForExit == 0 {
		cfg.SyncProgressForExit = defaultSyncProgressForExit
	}
	// AutoBootstrapRecovery: nil means use default (true), explicit false disables
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

	// Start churn tickers
	gossipChurnTicker := time.NewTicker(p.config.GossipChurnInterval)
	publicRootChurnTicker := time.NewTicker(p.config.PublicRootChurnInterval)

	p.mu.Lock()
	p.ctx = ctx
	p.reconcileTicker = ticker
	p.gossipChurnTicker = gossipChurnTicker
	p.publicRootChurnTicker = publicRootChurnTicker
	p.stopCh = stopCh
	p.mu.Unlock()

	// Reconcile loop
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

	// Gossip churn loop
	go func(t *time.Ticker, stop <-chan struct{}) {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				p.gossipChurn()
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}(gossipChurnTicker, stopCh)

	// Public root churn loop
	go func(t *time.Ticker, stop <-chan struct{}) {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				p.publicRootChurn()
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}(publicRootChurnTicker, stopCh)

	// Start outbound connections
	p.startOutboundConnections()
	return nil
}

// Stop gracefully shuts down the peer governor
func (p *PeerGovernor) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Stop all tickers
	if p.reconcileTicker != nil {
		p.reconcileTicker.Stop()
		p.reconcileTicker = nil
	}
	if p.gossipChurnTicker != nil {
		p.gossipChurnTicker.Stop()
		p.gossipChurnTicker = nil
	}
	if p.publicRootChurnTicker != nil {
		p.publicRootChurnTicker.Stop()
		p.publicRootChurnTicker = nil
	}
	// Close stop channel to signal goroutines
	if p.stopCh != nil {
		close(p.stopCh)
		p.stopCh = nil
	}
}
