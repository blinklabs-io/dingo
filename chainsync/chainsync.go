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

package chainsync

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DefaultMaxClients is the default maximum number of concurrent
// chainsync clients.
const DefaultMaxClients = 3

// DefaultStallTimeout is the default duration after which a
// client with no activity is considered stalled. This value
// must stay in sync with config.DefaultChainsyncConfig() and
// the fallback in internal/node/node.go.
const DefaultStallTimeout = 2 * time.Minute

// defaultSeenHeadersRetention is the fallback retention window, in slots,
// for the header deduplication cache when no stability window is available
// (e.g. before ledger state is wired). It mirrors the ledger's default
// stability window (2500 * 20).
const defaultSeenHeadersRetention = 2500 * 20

// seenHeadersPruneInterval bounds how often the header deduplication cache
// is scanned for pruning. A prune runs at most once per this many slots of
// forward progress, keeping the per-header cost negligible while capping
// the retained cache at roughly retentionWindow + seenHeadersPruneInterval
// slots of observed headers.
const seenHeadersPruneInterval = 1000

// ClientStatus represents the sync status of a chainsync client.
type ClientStatus int

const (
	// ClientStatusSyncing indicates the client is actively
	// receiving headers.
	ClientStatusSyncing ClientStatus = iota
	// ClientStatusSynced indicates the client has reached the
	// upstream chain tip.
	ClientStatusSynced
	// ClientStatusStalled indicates the client has not received
	// activity within the stall timeout.
	ClientStatusStalled
	// ClientStatusFailed indicates the client encountered an
	// error.
	ClientStatusFailed
)

// String returns a human-readable name for the ClientStatus.
func (s ClientStatus) String() string {
	switch s {
	case ClientStatusSyncing:
		return "syncing"
	case ClientStatusSynced:
		return "synced"
	case ClientStatusStalled:
		return "stalled"
	case ClientStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ChainsyncClientState holds per-connection state for a
// chainsync server-side client (node-to-client connections).
type ChainsyncClientState struct {
	ChainIter            *chain.ChainIterator
	Cursor               ocommon.Point
	NeedsInitialRollback bool
}

// TrackedClient holds per-connection state for a tracked
// chainsync client (outbound node-to-node connections).
type TrackedClient struct {
	ConnId ouroboros.ConnectionId
	Cursor ocommon.Point
	Tip    ochainsync.Tip
	Status ClientStatus
	// ObservabilityOnly marks connections that should keep
	// tip/activity metrics but must not consume the eligible
	// client pool or become active for ledger ingress.
	ObservabilityOnly bool
	// StartedAsOutbound records whether the connection was
	// initiated by us (outbound). This is stamped at
	// registration time and can be refreshed only when the
	// same ConnectionId is explicitly re-registered after a
	// connmanager collision replacement.
	StartedAsOutbound bool
	LastActivity      time.Time
	HeadersRecv       uint64
	// TODO: BytesRecv needs to be wired to the underlying
	// connection's byte counter. Currently unused.
	BytesRecv uint64
	// BlockfetchLatencyEWMA is an exponential moving average
	// (alpha=0.2) of the time from RequestRange to first block
	// response. Zero means no samples recorded yet.
	BlockfetchLatencyEWMA time.Duration
	blockfetchSampleCount uint64
}

// Config holds configuration for the chainsync State.
type Config struct {
	MaxClients   int
	StallTimeout time.Duration
	// HeaderSyncStrategy selects how headers from multiple eligible peers
	// drive ledger ingress. The zero value is HeaderSyncStrategyPrimary,
	// which preserves single-active behavior.
	HeaderSyncStrategy HeaderSyncStrategy
	// SeenHeadersRetention overrides the retention window, in slots, for
	// the header deduplication cache. When zero, the retention window is
	// derived from the ledger's current stability window (falling back to
	// defaultSeenHeadersRetention when no ledger state is available).
	SeenHeadersRetention uint64
	// PromRegistry, when non-nil, is used to register chainsync metrics
	// such as the current header deduplication cache size.
	PromRegistry prometheus.Registerer
}

// DefaultConfig returns the default chainsync configuration.
func DefaultConfig() Config {
	return Config{
		MaxClients:   DefaultMaxClients,
		StallTimeout: DefaultStallTimeout,
	}
}

// ChainProvider is the minimal interface chainsync requires from the ledger
// layer: local chain iteration for N2C server clients and the stability-window
// value used to bound the seen-header deduplication cache.
type ChainProvider interface {
	GetChainFromPoint(point ocommon.Point, inclusive bool) (*chain.ChainIterator, error)
	StabilityWindow() uint64
}

// ObservedHeader is a chainsync-owned record of a header received from a peer
// before cross-peer deduplication may suppress it. It carries enough context
// for fork resolution to reconstruct a peer's candidate fragment later.
type ObservedHeader struct {
	ConnectionId ouroboros.ConnectionId
	BlockHeader  gledger.BlockHeader
	Point        ocommon.Point
	Tip          ochainsync.Tip
	BlockNumber  uint64
	Type         uint
	Rollback     bool
}

// State manages chainsync client connections and header
// tracking for both server-side (N2C) and outbound (N2N)
// connections.
type State struct {
	eventBus      *event.EventBus
	chainProvider ChainProvider
	config        Config

	// Server-side clients (node-to-client connections)
	clients map[ouroboros.ConnectionId]*ChainsyncClientState

	// Tracked outbound clients (node-to-node connections)
	trackedClients     map[ouroboros.ConnectionId]*TrackedClient
	activeClientConnId *ouroboros.ConnectionId
	clientConnIdMutex  sync.RWMutex
	// roundRobinIndex is the rotation cursor for the round-robin header-sync
	// strategy. Guarded by clientConnIdMutex.
	roundRobinIndex uint64

	// Header deduplication: maps slot -> list of distinct
	// block hashes seen at that slot
	seenHeaders      map[uint64][]headerRecord
	seenHeadersMutex sync.Mutex
	// seenHeadersMaxSlot is the highest slot recorded in seenHeaders. It
	// is the frontier against which the retention window is applied when
	// pruning old observations.
	seenHeadersMaxSlot uint64
	// seenHeadersLastPruneCheck records the frontier slot at the last
	// prune attempt, rate-limiting prune scans to once per
	// seenHeadersPruneInterval slots of progress.
	seenHeadersLastPruneCheck uint64
	// seenHeadersGauge tracks the current number of slots retained in the
	// deduplication cache. Never nil; unregistered when PromRegistry is nil.
	seenHeadersGauge prometheus.Gauge
	// blockfetchLatencyGauge exposes the per-connection blockfetch latency
	// EWMA (seconds), labelled by remote peer address and full connection
	// ID. Never nil; unregistered when PromRegistry is nil. Series are
	// deleted on disconnect in RemoveClientConnId so cardinality stays
	// bounded by live connections.
	blockfetchLatencyGauge *prometheus.GaugeVec

	observedHeaders      map[ouroboros.ConnectionId]*observedHeaderChain
	observedHeadersMutex sync.RWMutex

	sync.Mutex
}

// headerRecord tracks a block hash and the connection that
// first reported it for a given slot.
type headerRecord struct {
	hash   []byte
	connId ouroboros.ConnectionId
}

type observedHeaderRecord struct {
	header   ObservedHeader
	prevHash []byte
}

type observedHeaderChain struct {
	order  []string
	byHash map[string]observedHeaderRecord
}

const maxObservedHeadersPerConn = 256

// NewState creates a new chainsync State with the given
// event bus and chain provider using default configuration.
func NewState(
	eventBus *event.EventBus,
	chainProvider ChainProvider,
) *State {
	return NewStateWithConfig(eventBus, chainProvider, DefaultConfig())
}

// NewStateWithConfig creates a new chainsync State with the
// given event bus, chain provider, and configuration.
func NewStateWithConfig(
	eventBus *event.EventBus,
	chainProvider ChainProvider,
	cfg Config,
) *State {
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = DefaultMaxClients
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = DefaultStallTimeout
	}
	s := &State{
		eventBus:       eventBus,
		chainProvider:  chainProvider,
		config:         cfg,
		clients:        make(map[ouroboros.ConnectionId]*ChainsyncClientState),
		trackedClients: make(map[ouroboros.ConnectionId]*TrackedClient),
		seenHeaders:    make(map[uint64][]headerRecord),
		observedHeaders: make(
			map[ouroboros.ConnectionId]*observedHeaderChain,
		),
	}
	// promauto.With(nil) returns a factory that creates the gauge without
	// registering it, so this is safe when PromRegistry is unset.
	s.seenHeadersGauge = promauto.With(cfg.PromRegistry).NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_chainsync_seen_headers",
			Help: "current number of slots retained in the chainsync header deduplication cache",
		},
	)
	s.blockfetchLatencyGauge = promauto.With(cfg.PromRegistry).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dingo_chainsync_blockfetch_latency_seconds",
			Help: "exponential moving average (alpha=0.2) of blockfetch RequestRange-to-first-block latency in seconds, by peer connection",
		},
		[]string{"peer", "connection_id"},
	)
	return s
}

// AddClient registers a server-side (N2C) chainsync client.
func (s *State) AddClient(
	connId connection.ConnectionId,
	intersectPoint ocommon.Point,
) (*ChainsyncClientState, error) {
	s.Lock()
	defer s.Unlock()
	// Return existing client state if already registered
	if existing, ok := s.clients[connId]; ok {
		return existing, nil
	}
	// Create initial chainsync state for connection
	if s.chainProvider == nil {
		return nil, errors.New("no chain provider available")
	}
	chainIter, err := s.chainProvider.GetChainFromPoint(
		intersectPoint,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"GetChainFromPoint failed: %w", err,
		)
	}
	s.clients[connId] = &ChainsyncClientState{
		Cursor:               intersectPoint,
		ChainIter:            chainIter,
		NeedsInitialRollback: true,
	}
	return s.clients[connId], nil
}

// RemoveClient unregisters a server-side (N2C) chainsync
// client.
func (s *State) RemoveClient(connId connection.ConnectionId) {
	s.Lock()
	defer s.Unlock()
	// Cancel any pending iterator operations
	if clientState, exists := s.clients[connId]; exists &&
		clientState.ChainIter != nil {
		clientState.ChainIter.Cancel()
	}
	// Remove client state entry
	delete(s.clients, connId)
}

// GetClientConnId returns the active chainsync client
// connection ID. This is the connection that should be used
// for block fetching.
func (s *State) GetClientConnId() *ouroboros.ConnectionId {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	return s.activeClientConnId
}

// SetClientConnId sets the active chainsync client connection
// ID. This is used when chain selection determines a new best
// peer.
func (s *State) SetClientConnId(connId ouroboros.ConnectionId) {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	s.activeClientConnId = &connId
}

// RemoveClientConnId removes a connection from tracking. If
// this was the active client, promotes the client with the
// highest tip slot as the new primary.
func (s *State) RemoveClientConnId(
	connId ouroboros.ConnectionId,
) {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	tc, exists := s.trackedClients[connId]
	wasPrimary := s.activeClientConnId != nil &&
		*s.activeClientConnId == connId
	wasEligible := exists && tc != nil && !tc.ObservabilityOnly
	delete(s.trackedClients, connId)
	peer, connectionID := blockfetchLatencyMetricLabels(connId)
	s.blockfetchLatencyGauge.DeleteLabelValues(peer, connectionID)
	s.clearObservedHeaderHistory(connId)
	if wasPrimary {
		s.activeClientConnId = nil
		s.promoteBestClientLocked()
	}
	// Emit client removed event
	if wasEligible && s.eventBus != nil {
		s.eventBus.PublishAsync(
			ClientRemovedEventType,
			event.NewEvent(
				ClientRemovedEventType,
				ClientRemovedEvent{
					ConnId:       connId,
					TotalClients: s.eligibleClientCountLocked(),
					WasPrimary:   wasPrimary,
				},
			),
		)
	}
}

func pointAheadOf(a, b ocommon.Point) bool {
	if a.Slot != b.Slot {
		return a.Slot > b.Slot
	}
	return !bytes.Equal(a.Hash, b.Hash)
}

// HandleClientRemoveRequestedEvent removes a tracked client when
// a component publishes a client removal request event.
func (s *State) HandleClientRemoveRequestedEvent(evt event.Event) {
	e, ok := evt.Data.(ClientRemoveRequestedEvent)
	if !ok {
		return
	}
	s.RemoveClientConnId(e.ConnId)
}

// promoteBestClientLocked selects the tracked client with the
// highest tip slot as the new active client. Healthy (syncing
// or synced) clients are preferred. If no healthy client exists,
// the stalled client with the most recent activity is promoted
// as a fallback — receiving a header will transition it back to
// syncing, breaking the deadlock where nil active selection
// prevents any client from making progress.
// Caller must hold clientConnIdMutex.
func (s *State) promoteBestClientLocked() {
	var bestId *ouroboros.ConnectionId
	var bestSlot uint64
	var bestStalledId *ouroboros.ConnectionId
	var bestStalledActivity time.Time
	for id, tc := range s.trackedClients {
		if tc.ObservabilityOnly || tc.Status == ClientStatusFailed {
			continue
		}
		if tc.Status == ClientStatusStalled {
			if bestStalledId == nil || tc.LastActivity.After(bestStalledActivity) {
				idCopy := id
				bestStalledId = &idCopy
				bestStalledActivity = tc.LastActivity
			}
			continue
		}
		if bestId == nil || tc.Tip.Point.Slot > bestSlot {
			idCopy := id
			bestId = &idCopy
			bestSlot = tc.Tip.Point.Slot
		}
	}
	if bestId != nil {
		s.activeClientConnId = bestId
	} else if bestStalledId != nil {
		// All clients stalled — promote the most recently active
		// one to prevent permanent nil-selection deadlock.
		s.activeClientConnId = bestStalledId
	} else {
		s.activeClientConnId = nil
	}
}

// addTrackedClientLocked registers a new tracked client. It
// initialises the TrackedClient, sets it as active if none
// exists, and emits a ClientAddedEvent.
// Caller must hold clientConnIdMutex.
func (s *State) addTrackedClientLocked(
	connId ouroboros.ConnectionId,
	observabilityOnly bool,
	startedAsOutbound bool,
) {
	s.trackedClients[connId] = &TrackedClient{
		ConnId:            connId,
		Status:            ClientStatusSyncing,
		ObservabilityOnly: observabilityOnly,
		StartedAsOutbound: startedAsOutbound,
		LastActivity:      time.Now(),
	}
	// Set as active if there's no active client
	if !observabilityOnly && s.activeClientConnId == nil {
		s.activeClientConnId = &connId
	}
	// Emit client added event
	if !observabilityOnly && s.eventBus != nil {
		s.eventBus.PublishAsync(
			ClientAddedEventType,
			event.NewEvent(
				ClientAddedEventType,
				ClientAddedEvent{
					ConnId:       connId,
					TotalClients: s.eligibleClientCountLocked(),
				},
			),
		)
	}
}

// AddClientConnId adds a connection ID to the set of tracked
// chainsync clients, enforcing the configured MaxClients limit.
// Returns true if the client was added, false if rejected
// (already tracked or at capacity). If no active client exists,
// this connection is automatically set as the active client.
// The client is recorded as outbound (StartedAsOutbound=true).
func (s *State) AddClientConnId(
	connId ouroboros.ConnectionId,
) bool {
	return s.TryAddClientConnIdWithDirection(
		connId,
		s.config.MaxClients,
		true,
	)
}

// TryAddObservedClientConnId adds a connection to observability-only tracking.
// Observability-only clients do not consume the eligible client limit and are
// never promoted as the active chainsync source.
func (s *State) TryAddObservedClientConnId(
	connId ouroboros.ConnectionId,
) bool {
	return s.TryAddObservedClientConnIdWithDirection(connId, false)
}

// TryAddObservedClientConnIdWithDirection adds a connection to
// observability-only tracking and records whether it was started
// as outbound.
func (s *State) TryAddObservedClientConnIdWithDirection(
	connId ouroboros.ConnectionId,
	startedAsOutbound bool,
) bool {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	if _, exists := s.trackedClients[connId]; exists {
		return false
	}
	s.addTrackedClientLocked(connId, true, startedAsOutbound)
	return true
}

// HasClientConnId returns true if the connection ID is being
// tracked.
func (s *State) HasClientConnId(
	connId ouroboros.ConnectionId,
) bool {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	_, exists := s.trackedClients[connId]
	return exists
}

// GetClientConnIds returns all tracked chainsync client
// connection IDs.
func (s *State) GetClientConnIds() []ouroboros.ConnectionId {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	connIds := make(
		[]ouroboros.ConnectionId,
		0,
		len(s.trackedClients),
	)
	for connId := range s.trackedClients {
		connIds = append(connIds, connId)
	}
	return connIds
}

// ClientConnCount returns the number of tracked chainsync
// clients.
func (s *State) ClientConnCount() int {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	return s.eligibleClientCountLocked()
}

// TryAddClientConnId atomically checks if a connection can be
// added (not already tracked and under maxClients limit) and
// adds it if allowed. Returns true if the connection was added,
// false otherwise.
func (s *State) TryAddClientConnId(
	connId ouroboros.ConnectionId,
	maxClients int,
) bool {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	// Check if already tracked
	if _, exists := s.trackedClients[connId]; exists {
		return false
	}
	// Check client limit
	if s.eligibleClientCountLocked() >= maxClients {
		return false
	}
	s.addTrackedClientLocked(connId, false, false)
	return true
}

// TryAddClientConnIdWithDirection is like TryAddClientConnId but
// additionally records whether the connection was started as
// outbound. This is used to stamp each tracked client with its
// connection direction at registration time, providing a reliable
// flag that survives ConnectionId collisions.
func (s *State) TryAddClientConnIdWithDirection(
	connId ouroboros.ConnectionId,
	maxClients int,
	startedAsOutbound bool,
) bool {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	// Check if already tracked
	if _, exists := s.trackedClients[connId]; exists {
		return false
	}
	// Check client limit
	if s.eligibleClientCountLocked() >= maxClients {
		return false
	}
	s.addTrackedClientLocked(connId, false, startedAsOutbound)
	return true
}

// ClientObservabilityOnly reports whether a tracked client is currently
// observability-only. The second return value reports whether the client exists.
func (s *State) ClientObservabilityOnly(
	connId ouroboros.ConnectionId,
) (bool, bool) {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		return false, false
	}
	return tc.ObservabilityOnly, true
}

// ClientStartedAsOutbound reports whether a tracked client was
// registered as an outbound connection. The second return value
// reports whether the client exists.
func (s *State) ClientStartedAsOutbound(
	connId ouroboros.ConnectionId,
) (bool, bool) {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		return false, false
	}
	return tc.StartedAsOutbound, true
}

// SetClientStartedAsOutbound updates the recorded connection
// direction for an existing tracked client. This is used when a
// ConnectionId collision causes a new physical connection to replace
// an older tracked connection under the same ID.
func (s *State) SetClientStartedAsOutbound(
	connId ouroboros.ConnectionId,
	startedAsOutbound bool,
) bool {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		return false
	}
	tc.StartedAsOutbound = startedAsOutbound
	return true
}

// SetClientObservabilityOnly toggles whether a tracked client participates in
// the eligible chainsync pool. Promoting an observability-only client back into
// the eligible pool respects MaxClients; when the pool is full, the client
// remains observability-only and this method returns false.
func (s *State) SetClientObservabilityOnly(
	connId ouroboros.ConnectionId,
	observabilityOnly bool,
) bool {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()

	tc, exists := s.trackedClients[connId]
	if !exists {
		return false
	}
	if tc.ObservabilityOnly == observabilityOnly {
		return true
	}
	if !observabilityOnly &&
		s.config.MaxClients > 0 &&
		s.eligibleClientCountLocked() >= s.config.MaxClients {
		return false
	}

	wasPrimary := s.activeClientConnId != nil &&
		*s.activeClientConnId == connId
	wasEligible := !tc.ObservabilityOnly
	tc.ObservabilityOnly = observabilityOnly
	needPromote := false
	if wasPrimary && observabilityOnly {
		s.activeClientConnId = nil
		needPromote = true
	}
	if !observabilityOnly && s.activeClientConnId == nil {
		needPromote = true
	}
	if needPromote {
		s.promoteBestClientLocked()
	}

	if s.eventBus == nil {
		return true
	}
	if wasEligible && observabilityOnly {
		s.eventBus.PublishAsync(
			ClientRemovedEventType,
			event.NewEvent(
				ClientRemovedEventType,
				ClientRemovedEvent{
					ConnId:       connId,
					TotalClients: s.eligibleClientCountLocked(),
					WasPrimary:   wasPrimary,
				},
			),
		)
	}
	if !wasEligible && !observabilityOnly {
		s.eventBus.PublishAsync(
			ClientAddedEventType,
			event.NewEvent(
				ClientAddedEventType,
				ClientAddedEvent{
					ConnId:       connId,
					TotalClients: s.eligibleClientCountLocked(),
				},
			),
		)
	}
	return true
}

// UpdateClientTip updates the cursor, tip, and activity
// tracking for a tracked client, and performs header
// deduplication. Returns true if the header at this point is
// new (not a duplicate).
func (s *State) UpdateClientTip(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	tip ochainsync.Tip,
) bool {
	return s.updateClientTip(connId, point, tip, true)
}

// UpdateClientTipWithoutDedup updates the cursor, tip, and
// activity tracking for a tracked client without recording the
// header in the shared dedup cache. This is used for peers that
// should not drive ledger ingress, so they do not suppress
// later delivery of the same header from an eligible peer.
func (s *State) UpdateClientTipWithoutDedup(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	tip ochainsync.Tip,
) {
	s.updateClientTip(connId, point, tip, false)
}

// RewindTrackedClientsTo rewinds tracked client cursors that sit ahead of the
// provided local ledger point. This keeps chainsync client state aligned with
// local rollback/recovery so peers do not strand us in AwaitReply on a stale
// higher cursor.
func (s *State) RewindTrackedClientsTo(
	point ocommon.Point,
) []ouroboros.ConnectionId {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	var ret []ouroboros.ConnectionId
	for connId, tc := range s.trackedClients {
		if !pointAheadOf(tc.Cursor, point) {
			continue
		}
		tc.Cursor = ocommon.Point{
			Slot: point.Slot,
			Hash: cloneBytes(point.Hash),
		}
		tc.Status = ClientStatusSyncing
		tc.LastActivity = time.Now()
		ret = append(ret, connId)
	}
	return ret
}

// RecordObservedHeader stores the raw per-connection header ancestry before
// cross-peer dedup can suppress delivery into the ledger queue. This lets
// fork resolution reconstruct the selected peer's candidate fragment even
// when earlier headers were first seen from another peer.
func (s *State) RecordObservedHeader(h ObservedHeader) {
	if h.BlockHeader == nil || len(h.Point.Hash) == 0 {
		return
	}
	prevHash := h.BlockHeader.PrevHash().Bytes()
	if len(prevHash) == 0 {
		return
	}

	s.observedHeadersMutex.Lock()
	defer s.observedHeadersMutex.Unlock()

	chainHistory := s.observedHeaders[h.ConnectionId]
	if chainHistory == nil {
		chainHistory = &observedHeaderChain{
			order: make([]string, 0, maxObservedHeadersPerConn),
			byHash: make(map[string]observedHeaderRecord,
				maxObservedHeadersPerConn),
		}
		s.observedHeaders[h.ConnectionId] = chainHistory
	}

	hashKey := hex.EncodeToString(h.Point.Hash)
	if _, exists := chainHistory.byHash[hashKey]; exists {
		return
	}
	h.Point.Hash = cloneBytes(h.Point.Hash)
	h.Tip.Point.Hash = cloneBytes(h.Tip.Point.Hash)
	chainHistory.order = append(chainHistory.order, hashKey)
	chainHistory.byHash[hashKey] = observedHeaderRecord{
		header:   h,
		prevHash: append([]byte(nil), prevHash...),
	}
	if len(chainHistory.order) <= maxObservedHeadersPerConn {
		return
	}
	evictKey := chainHistory.order[0]
	chainHistory.order = chainHistory.order[1:]
	delete(chainHistory.byHash, evictKey)
}

// LookupObservedHeader returns a previously observed header for the given
// connection/hash pair, along with its prev-hash ancestry.
func (s *State) LookupObservedHeader(
	connId ouroboros.ConnectionId,
	hash []byte,
) (ObservedHeader, []byte, bool) {
	s.observedHeadersMutex.RLock()
	defer s.observedHeadersMutex.RUnlock()

	chainHistory := s.observedHeaders[connId]
	if chainHistory == nil {
		return ObservedHeader{}, nil, false
	}
	record, ok := chainHistory.byHash[hex.EncodeToString(hash)]
	if !ok {
		return ObservedHeader{}, nil, false
	}
	record.header.Point.Hash = cloneBytes(record.header.Point.Hash)
	record.header.Tip.Point.Hash = cloneBytes(record.header.Tip.Point.Hash)
	return record.header, append([]byte(nil), record.prevHash...), true
}

func (s *State) clearObservedHeaderHistory(
	connId ouroboros.ConnectionId,
) {
	s.observedHeadersMutex.Lock()
	defer s.observedHeadersMutex.Unlock()
	delete(s.observedHeaders, connId)
}

func (s *State) ClearObservedHeaderHistory(
	connId ouroboros.ConnectionId,
) {
	s.clearObservedHeaderHistory(connId)
}

func (s *State) updateClientTip(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	tip ochainsync.Tip,
	dedup bool,
) bool {
	s.clientConnIdMutex.Lock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		s.clientConnIdMutex.Unlock()
		return true
	}
	tc.Cursor = point
	tc.Tip = tip
	tc.LastActivity = time.Now()
	tc.HeadersRecv++
	if tc.Status == ClientStatusStalled {
		tc.Status = ClientStatusSyncing
	}
	s.clientConnIdMutex.Unlock()

	if !dedup {
		return true
	}
	// Header deduplication and fork detection
	isNew := s.processHeader(connId, point)
	// Prune stale observations on the normal progress path so the dedup
	// cache stays bounded over long-running nodes.
	s.maybePruneSeenHeaders()
	return isNew
}

// processHeader checks whether the header at the given point
// has already been seen. If another client reported a different
// hash at the same slot, a fork detection event is emitted.
// Returns true if this is a new (non-duplicate) header.
func (s *State) processHeader(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) bool {
	s.seenHeadersMutex.Lock()
	defer s.seenHeadersMutex.Unlock()
	records := s.seenHeaders[point.Slot]
	// Check if any existing record matches this hash
	for _, rec := range records {
		if bytes.Equal(rec.hash, point.Hash) {
			// Duplicate header, same hash
			return false
		}
	}
	// New hash at this slot; record it.
	// Clone the hash to avoid aliasing the caller's buffer.
	hashClone := cloneBytes(point.Hash)
	newRec := headerRecord{
		hash:   hashClone,
		connId: connId,
	}
	s.seenHeaders[point.Slot] = append(records, newRec)
	if point.Slot > s.seenHeadersMaxSlot {
		s.seenHeadersMaxSlot = point.Slot
	}
	s.updateSeenHeadersGaugeLocked()
	// If there was already a different hash at this slot,
	// emit a fork detection event against the first record.
	// Clone the Point.Hash to avoid aliasing the caller's
	// buffer in the event payload.
	if len(records) > 0 {
		if s.eventBus != nil {
			clonedPoint := ocommon.NewPoint(
				point.Slot,
				cloneBytes(point.Hash),
			)
			s.eventBus.PublishAsync(
				ForkDetectedEventType,
				event.NewEvent(
					ForkDetectedEventType,
					ForkDetectedEvent{
						Slot:    point.Slot,
						HashA:   records[0].hash,
						HashB:   hashClone,
						ConnIdA: records[0].connId,
						ConnIdB: connId,
						Point:   clonedPoint,
					},
				),
			)
		}
	}
	return true
}

// HeaderPreviouslySeenFromOtherConn reports whether the exact header point was
// already recorded by a different connection. This lets the selected ingress
// peer replay a header first observed elsewhere without also replaying
// same-connection duplicates back into the ledger queue.
func (s *State) HeaderPreviouslySeenFromOtherConn(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) bool {
	s.seenHeadersMutex.Lock()
	defer s.seenHeadersMutex.Unlock()
	for _, rec := range s.seenHeaders[point.Slot] {
		if !bytes.Equal(rec.hash, point.Hash) {
			continue
		}
		return !trackedConnIdsEqual(rec.connId, connId)
	}
	return false
}

func trackedConnIdsEqual(
	a,
	b ouroboros.ConnectionId,
) bool {
	// Compare each address individually rather than via ConnectionId.String():
	// that method dereferences LocalAddr/RemoteAddr unconditionally and panics
	// when either is nil (e.g. a partial-nil tracked connection id).
	return sameTrackedNetAddr(a.LocalAddr, b.LocalAddr) &&
		sameTrackedNetAddr(a.RemoteAddr, b.RemoteAddr)
}

// sameTrackedNetAddr compares two addresses by string form, treating nil as
// equal only to nil.
func sameTrackedNetAddr(a, b net.Addr) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.String() == b.String()
}

// MarkClientSynced marks a tracked client as synced (at chain
// tip).
func (s *State) MarkClientSynced(
	connId ouroboros.ConnectionId,
) {
	s.clientConnIdMutex.Lock()
	tc, exists := s.trackedClients[connId]
	var changed bool
	if exists && tc.Status != ClientStatusSynced {
		tc.Status = ClientStatusSynced
		tc.LastActivity = time.Now()
		changed = true
	}
	var slot uint64
	if exists {
		slot = tc.Tip.Point.Slot
	}
	s.clientConnIdMutex.Unlock()
	if changed && s.eventBus != nil {
		s.eventBus.PublishAsync(
			ClientSyncedEventType,
			event.NewEvent(
				ClientSyncedEventType,
				ClientSyncedEvent{
					ConnId: connId,
					Slot:   slot,
				},
			),
		)
	}
}

// CheckStalledClients scans all tracked clients and marks any
// that have exceeded the stall timeout. If the primary client
// is stalled, a failover to the next best client is triggered.
// Returns the list of connection IDs that were newly marked as
// stalled.
func (s *State) CheckStalledClients() []ouroboros.ConnectionId {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()

	now := time.Now()
	var stalled []ouroboros.ConnectionId
	for id, tc := range s.trackedClients {
		if tc.ObservabilityOnly ||
			tc.Status == ClientStatusStalled ||
			tc.Status == ClientStatusFailed {
			continue
		}
		if now.Sub(tc.LastActivity) > s.config.StallTimeout {
			tc.Status = ClientStatusStalled
			stalled = append(stalled, id)
			if s.eventBus != nil {
				s.eventBus.PublishAsync(
					ClientStalledEventType,
					event.NewEvent(
						ClientStalledEventType,
						ClientStalledEvent{
							ConnId: id,
							Slot:   tc.Tip.Point.Slot,
						},
					),
				)
			}
		}
	}

	// Check if the primary client was stalled
	if len(stalled) > 0 &&
		s.activeClientConnId != nil &&
		slices.Contains(stalled, *s.activeClientConnId) {
		s.activeClientConnId = nil
		s.promoteBestClientLocked()
	}

	return stalled
}

// GetTrackedClient returns a deep copy of the TrackedClient
// for the given connection ID, or nil if not found. Byte
// slices inside Point.Hash are cloned so the caller cannot
// race with concurrent UpdateClientTip calls.
func (s *State) GetTrackedClient(
	connId ouroboros.ConnectionId,
) *TrackedClient {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		return nil
	}
	// Return a deep copy to avoid races on byte slices
	result := *tc
	result.Cursor.Hash = cloneBytes(tc.Cursor.Hash)
	result.Tip.Point.Hash = cloneBytes(tc.Tip.Point.Hash)
	return &result
}

// GetTrackedClients returns deep copies of all tracked
// clients.
func (s *State) GetTrackedClients() []TrackedClient {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	result := make([]TrackedClient, 0, len(s.trackedClients))
	for _, tc := range s.trackedClients {
		cpy := *tc
		cpy.Cursor.Hash = cloneBytes(tc.Cursor.Hash)
		cpy.Tip.Point.Hash = cloneBytes(tc.Tip.Point.Hash)
		result = append(result, cpy)
	}
	return result
}

// MaxClients returns the configured maximum number of chainsync
// clients.
func (s *State) MaxClients() int {
	return s.config.MaxClients
}

// ClearSeenHeaders removes all entries from the header
// deduplication cache. This should be called on rollback to
// avoid stale entries.
func (s *State) ClearSeenHeaders() {
	s.seenHeadersMutex.Lock()
	defer s.seenHeadersMutex.Unlock()
	s.seenHeaders = make(map[uint64][]headerRecord)
	s.seenHeadersMaxSlot = 0
	s.seenHeadersLastPruneCheck = 0
	s.updateSeenHeadersGaugeLocked()
}

// ClearSeenHeadersFrom removes entries from the header deduplication cache
// above the specified slot. This allows a restarted chainsync client
// to replay headers beyond a known-good intersect point after an active-peer
// switch without discarding older fork-detection history.
func (s *State) ClearSeenHeadersFrom(fromSlot uint64) {
	s.seenHeadersMutex.Lock()
	defer s.seenHeadersMutex.Unlock()
	for slot := range s.seenHeaders {
		if slot > fromSlot {
			delete(s.seenHeaders, slot)
		}
	}
	// The frontier and prune-check marker must not sit above the highest
	// remaining slot, or the retention window would be measured from a
	// slot that no longer has an observation.
	if s.seenHeadersMaxSlot > fromSlot {
		s.seenHeadersMaxSlot = fromSlot
	}
	if s.seenHeadersLastPruneCheck > s.seenHeadersMaxSlot {
		s.seenHeadersLastPruneCheck = s.seenHeadersMaxSlot
	}
	s.updateSeenHeadersGaugeLocked()
}

// cloneBytes returns a copy of the byte slice, or nil if the
// input is nil.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// PruneSeenHeaders removes entries from the header
// deduplication cache for slots older than the given slot.
func (s *State) PruneSeenHeaders(beforeSlot uint64) {
	s.seenHeadersMutex.Lock()
	defer s.seenHeadersMutex.Unlock()
	for slot := range s.seenHeaders {
		if slot < beforeSlot {
			delete(s.seenHeaders, slot)
		}
	}
	s.updateSeenHeadersGaugeLocked()
}

// seenHeadersRetention returns the retention window, in slots, for the
// header deduplication cache. A configured override wins; otherwise the
// ledger's current stability window is used, falling back to
// defaultSeenHeadersRetention when no ledger state is available.
func (s *State) seenHeadersRetention() uint64 {
	if s.config.SeenHeadersRetention > 0 {
		return s.config.SeenHeadersRetention
	}
	if s.chainProvider != nil {
		if w := s.chainProvider.StabilityWindow(); w > 0 {
			return w
		}
	}
	return defaultSeenHeadersRetention
}

// maybePruneSeenHeaders drops header deduplication entries for slots that
// have fallen behind the retention window (the Ouroboros stability window
// by default) relative to the highest slot observed. It is invoked on the
// normal header-progress path so a long-running node does not accumulate
// seenHeaders entries without bound. Slots within the retention window are
// always kept so recent duplicate and fork detection are unaffected.
//
// Pruning is rate-limited by seenHeadersPruneInterval: the cache is only
// rescanned after the frontier advances by at least that many slots, which
// keeps the amortized per-header cost negligible and bounds the retained
// cache at roughly retention + seenHeadersPruneInterval slots.
func (s *State) maybePruneSeenHeaders() {
	s.seenHeadersMutex.Lock()
	frontier := s.seenHeadersMaxSlot
	// Rate-limit rescans. The first comparison also guards the unsigned
	// subtraction against a frontier that moved backwards after a clear.
	if frontier >= s.seenHeadersLastPruneCheck &&
		frontier-s.seenHeadersLastPruneCheck < seenHeadersPruneInterval {
		s.seenHeadersMutex.Unlock()
		return
	}
	s.seenHeadersLastPruneCheck = frontier
	s.seenHeadersMutex.Unlock()

	// Resolve the retention window outside seenHeadersMutex: it may take
	// the ledger read lock, and we must not nest locks in that order.
	retention := s.seenHeadersRetention()
	if retention == 0 || frontier <= retention {
		return
	}
	cutoff := frontier - retention

	s.seenHeadersMutex.Lock()
	defer s.seenHeadersMutex.Unlock()
	for slot := range s.seenHeaders {
		if slot < cutoff {
			delete(s.seenHeaders, slot)
		}
	}
	s.updateSeenHeadersGaugeLocked()
}

// SeenHeadersLen returns the number of slots currently retained in the
// header deduplication cache. Primarily useful for metrics and tests.
func (s *State) SeenHeadersLen() int {
	s.seenHeadersMutex.Lock()
	defer s.seenHeadersMutex.Unlock()
	return len(s.seenHeaders)
}

// updateSeenHeadersGaugeLocked refreshes the dedup cache size gauge.
// Caller must hold seenHeadersMutex.
func (s *State) updateSeenHeadersGaugeLocked() {
	s.seenHeadersGauge.Set(float64(len(s.seenHeaders)))
}

func (s *State) eligibleClientCountLocked() int {
	count := 0
	for _, tc := range s.trackedClients {
		if !tc.ObservabilityOnly {
			count++
		}
	}
	return count
}

// blockfetchLatencyAlpha is the smoothing factor for the blockfetch
// latency EWMA. A value of 0.2 weights recent samples moderately.
const blockfetchLatencyAlpha = 0.2

// PeersWithBlock returns all tracked connection IDs — excluding
// origin — that have a recorded observed header at the given point.
// Callers use this to identify shadow peers for parallel blockfetch.
func (s *State) PeersWithBlock(
	origin ouroboros.ConnectionId,
	point ocommon.Point,
) []ouroboros.ConnectionId {
	if len(point.Hash) == 0 {
		return nil
	}
	hashKey := hex.EncodeToString(point.Hash)
	s.observedHeadersMutex.RLock()
	defer s.observedHeadersMutex.RUnlock()
	var result []ouroboros.ConnectionId
	for connId, chain := range s.observedHeaders {
		if connId == origin {
			continue
		}
		if _, ok := chain.byHash[hashKey]; ok {
			result = append(result, connId)
		}
	}
	return result
}

// RecordBlockfetchLatency updates the EWMA blockfetch latency for
// the given connection. Called by the ledger when the first block
// body arrives after a RequestRange.
func (s *State) RecordBlockfetchLatency(
	connId ouroboros.ConnectionId,
	latency time.Duration,
) {
	if latency <= 0 {
		return
	}
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	tc, ok := s.trackedClients[connId]
	if !ok {
		return
	}
	tc.blockfetchSampleCount++
	if tc.blockfetchSampleCount == 1 {
		tc.BlockfetchLatencyEWMA = latency
	} else {
		tc.BlockfetchLatencyEWMA = time.Duration(
			float64(latency)*blockfetchLatencyAlpha +
				float64(tc.BlockfetchLatencyEWMA)*(1-blockfetchLatencyAlpha),
		)
	}
	peer, connectionID := blockfetchLatencyMetricLabels(connId)
	s.blockfetchLatencyGauge.WithLabelValues(peer, connectionID).
		Set(tc.BlockfetchLatencyEWMA.Seconds())
}

func blockfetchLatencyMetricLabels(
	connId ouroboros.ConnectionId,
) (string, string) {
	return netAddrLabel(connId.RemoteAddr),
		fmt.Sprintf(
			"%s<->%s",
			netAddrLabel(connId.LocalAddr),
			netAddrLabel(connId.RemoteAddr),
		)
}

func netAddrLabel(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}

// BlockfetchLatency returns the blockfetch EWMA for the given
// connection and whether any samples have been recorded.
func (s *State) BlockfetchLatency(
	connId ouroboros.ConnectionId,
) (time.Duration, bool) {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	tc, ok := s.trackedClients[connId]
	if !ok || tc.blockfetchSampleCount == 0 {
		return 0, false
	}
	return tc.BlockfetchLatencyEWMA, true
}

// BlockfetchLatencyMedian returns the median EWMA latency across all
// tracked peers that have at least one sample, plus the sample count.
// Used to adapt thresholds (e.g. shadow blockfetch gating) to the
// observed peer population rather than a fixed cutoff.
func (s *State) BlockfetchLatencyMedian() (time.Duration, int) {
	s.clientConnIdMutex.RLock()
	samples := make([]time.Duration, 0, len(s.trackedClients))
	for _, tc := range s.trackedClients {
		if tc.blockfetchSampleCount > 0 && tc.BlockfetchLatencyEWMA > 0 {
			samples = append(samples, tc.BlockfetchLatencyEWMA)
		}
	}
	s.clientConnIdMutex.RUnlock()
	if len(samples) == 0 {
		return 0, 0
	}
	slices.Sort(samples)
	n := len(samples)
	if n%2 == 1 {
		return samples[n/2], n
	}
	return (samples[n/2-1] + samples[n/2]) / 2, n
}
