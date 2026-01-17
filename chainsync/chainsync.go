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
	"sync"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/connection"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type ChainsyncClientState struct {
	ChainIter            *chain.ChainIterator
	Cursor               ocommon.Point
	NeedsInitialRollback bool
}

type State struct {
	eventBus           *event.EventBus
	ledgerState        *ledger.LedgerState
	clients            map[ouroboros.ConnectionId]*ChainsyncClientState
	trackedClients     map[ouroboros.ConnectionId]struct{}
	activeClientConnId *ouroboros.ConnectionId
	clientConnIdMutex  sync.RWMutex
	sync.Mutex
}

func NewState(
	eventBus *event.EventBus,
	ledgerState *ledger.LedgerState,
) *State {
	s := &State{
		eventBus:       eventBus,
		ledgerState:    ledgerState,
		clients:        make(map[ouroboros.ConnectionId]*ChainsyncClientState),
		trackedClients: make(map[ouroboros.ConnectionId]struct{}),
	}
	return s
}

func (s *State) AddClient(
	connId connection.ConnectionId,
	intersectPoint ocommon.Point,
) (*ChainsyncClientState, error) {
	s.Lock()
	defer s.Unlock()
	// Create initial chainsync state for connection
	chainIter, err := s.ledgerState.GetChainFromPoint(intersectPoint, false)
	if err != nil {
		return nil, err
	}
	if _, ok := s.clients[connId]; !ok {
		s.clients[connId] = &ChainsyncClientState{
			Cursor:               intersectPoint,
			ChainIter:            chainIter,
			NeedsInitialRollback: true,
		}
	}
	return s.clients[connId], nil
}

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

// GetClientConnId returns the active chainsync client connection ID.
// This is the connection that should be used for block fetching.
func (s *State) GetClientConnId() *ouroboros.ConnectionId {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	return s.activeClientConnId
}

// SetClientConnId sets the active chainsync client connection ID.
// This is used when chain selection determines a new best peer.
func (s *State) SetClientConnId(connId ouroboros.ConnectionId) {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	s.activeClientConnId = &connId
}

// RemoveClientConnId removes a connection from tracking. If this was the
// active client, selects a fallback from remaining tracked clients.
func (s *State) RemoveClientConnId(connId ouroboros.ConnectionId) {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	delete(s.trackedClients, connId)
	if s.activeClientConnId != nil && *s.activeClientConnId == connId {
		s.activeClientConnId = nil
		// Select fallback from remaining tracked clients
		for fallbackId := range s.trackedClients {
			s.activeClientConnId = &fallbackId
			break
		}
	}
}

// AddClientConnId adds a connection ID to the set of tracked chainsync clients.
// If no active client exists, this connection is automatically set as the active
// client. This ensures there is always an active client when at least one is tracked.
func (s *State) AddClientConnId(connId ouroboros.ConnectionId) {
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	s.trackedClients[connId] = struct{}{}
	// Set as active if there's no active client
	if s.activeClientConnId == nil {
		s.activeClientConnId = &connId
	}
}

// HasClientConnId returns true if the connection ID is being tracked.
func (s *State) HasClientConnId(connId ouroboros.ConnectionId) bool {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	_, exists := s.trackedClients[connId]
	return exists
}

// GetClientConnIds returns all tracked chainsync client connection IDs.
func (s *State) GetClientConnIds() []ouroboros.ConnectionId {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	connIds := make([]ouroboros.ConnectionId, 0, len(s.trackedClients))
	for connId := range s.trackedClients {
		connIds = append(connIds, connId)
	}
	return connIds
}

// ClientConnCount returns the number of tracked chainsync clients.
func (s *State) ClientConnCount() int {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	return len(s.trackedClients)
}

// TryAddClientConnId atomically checks if a connection can be added (not
// already tracked and under maxClients limit) and adds it if allowed.
// Returns true if the connection was added, false otherwise.
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
	// Add the client
	s.trackedClients[connId] = struct{}{}
	// Set as active if there's no active client
	if s.activeClientConnId == nil {
		s.activeClientConnId = &connId
	}
	return true
}
