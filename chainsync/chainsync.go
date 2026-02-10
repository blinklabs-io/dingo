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
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/connection"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DefaultMaxClients is the default maximum number of concurrent
// chainsync clients.
const DefaultMaxClients = 3

// DefaultStallTimeout is the default duration after which a
// client with no activity is considered stalled. This value
// must stay in sync with config.DefaultChainsyncConfig() and
// the fallback in internal/node/node.go.
const DefaultStallTimeout = 30 * time.Second

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
	ConnId       ouroboros.ConnectionId
	Cursor       ocommon.Point
	Tip          ochainsync.Tip
	Status       ClientStatus
	LastActivity time.Time
	HeadersRecv  uint64
	// TODO: BytesRecv needs to be wired to the underlying
	// connection's byte counter. Currently unused.
	BytesRecv uint64
}

// Config holds configuration for the chainsync State.
type Config struct {
	MaxClients   int
	StallTimeout time.Duration
}

// DefaultConfig returns the default chainsync configuration.
func DefaultConfig() Config {
	return Config{
		MaxClients:   DefaultMaxClients,
		StallTimeout: DefaultStallTimeout,
	}
}

// State manages chainsync client connections and header
// tracking for both server-side (N2C) and outbound (N2N)
// connections.
type State struct {
	eventBus    *event.EventBus
	ledgerState *ledger.LedgerState
	config      Config

	// Server-side clients (node-to-client connections)
	clients map[ouroboros.ConnectionId]*ChainsyncClientState

	// Tracked outbound clients (node-to-node connections)
	trackedClients     map[ouroboros.ConnectionId]*TrackedClient
	activeClientConnId *ouroboros.ConnectionId
	clientConnIdMutex  sync.RWMutex

	// Header deduplication: maps slot -> list of distinct
	// block hashes seen at that slot
	seenHeaders      map[uint64][]headerRecord
	seenHeadersMutex sync.Mutex

	sync.Mutex
}

// headerRecord tracks a block hash and the connection that
// first reported it for a given slot.
type headerRecord struct {
	hash   []byte
	connId ouroboros.ConnectionId
}

// NewState creates a new chainsync State with the given
// event bus and ledger state using default configuration.
func NewState(
	eventBus *event.EventBus,
	ledgerState *ledger.LedgerState,
) *State {
	return NewStateWithConfig(eventBus, ledgerState, DefaultConfig())
}

// NewStateWithConfig creates a new chainsync State with the
// given event bus, ledger state, and configuration.
func NewStateWithConfig(
	eventBus *event.EventBus,
	ledgerState *ledger.LedgerState,
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
		ledgerState:    ledgerState,
		config:         cfg,
		clients:        make(map[ouroboros.ConnectionId]*ChainsyncClientState),
		trackedClients: make(map[ouroboros.ConnectionId]*TrackedClient),
		seenHeaders:    make(map[uint64][]headerRecord),
	}
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
	chainIter, err := s.ledgerState.GetChainFromPoint(
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
	wasPrimary := s.activeClientConnId != nil &&
		*s.activeClientConnId == connId
	delete(s.trackedClients, connId)
	if wasPrimary {
		s.activeClientConnId = nil
		s.promoteBestClientLocked()
	}
	// Emit client removed event
	if s.eventBus != nil {
		s.eventBus.PublishAsync(
			ClientRemovedEventType,
			event.NewEvent(
				ClientRemovedEventType,
				ClientRemovedEvent{
					ConnId:       connId,
					TotalClients: len(s.trackedClients),
					WasPrimary:   wasPrimary,
				},
			),
		)
	}
}

// promoteBestClientLocked selects the tracked client with the
// highest tip slot as the new active client. Only healthy
// (syncing or synced) clients are considered. If no healthy
// client exists, activeClientConnId is set to nil so that
// callers do not route work to a known-bad peer.
// Caller must hold clientConnIdMutex.
func (s *State) promoteBestClientLocked() {
	var bestId *ouroboros.ConnectionId
	var bestSlot uint64
	for id, tc := range s.trackedClients {
		if tc.Status == ClientStatusFailed ||
			tc.Status == ClientStatusStalled {
			continue
		}
		if bestId == nil || tc.Tip.Point.Slot > bestSlot {
			idCopy := id
			bestId = &idCopy
			bestSlot = tc.Tip.Point.Slot
		}
	}
	s.activeClientConnId = bestId
}

// addTrackedClientLocked registers a new tracked client. It
// initialises the TrackedClient, sets it as active if none
// exists, and emits a ClientAddedEvent.
// Caller must hold clientConnIdMutex.
func (s *State) addTrackedClientLocked(
	connId ouroboros.ConnectionId,
) {
	s.trackedClients[connId] = &TrackedClient{
		ConnId:       connId,
		Status:       ClientStatusSyncing,
		LastActivity: time.Now(),
	}
	// Set as active if there's no active client
	if s.activeClientConnId == nil {
		s.activeClientConnId = &connId
	}
	// Emit client added event
	if s.eventBus != nil {
		s.eventBus.PublishAsync(
			ClientAddedEventType,
			event.NewEvent(
				ClientAddedEventType,
				ClientAddedEvent{
					ConnId:       connId,
					TotalClients: len(s.trackedClients),
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
func (s *State) AddClientConnId(
	connId ouroboros.ConnectionId,
) bool {
	return s.TryAddClientConnId(connId, s.config.MaxClients)
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
	return len(s.trackedClients)
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
	if len(s.trackedClients) >= maxClients {
		return false
	}
	s.addTrackedClientLocked(connId)
	return true
}

// UpdateClientTip updates the cursor, tip, and activity
// tracking for a tracked client. Returns true if the header at
// this point is new (not a duplicate).
func (s *State) UpdateClientTip(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	tip ochainsync.Tip,
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

	// Header deduplication and fork detection
	return s.processHeader(connId, point)
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
		if tc.Status == ClientStatusStalled ||
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
	if len(stalled) > 0 && s.activeClientConnId != nil {
		for _, id := range stalled {
			if *s.activeClientConnId == id {
				s.activeClientConnId = nil
				s.promoteBestClientLocked()
				break
			}
		}
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
}
