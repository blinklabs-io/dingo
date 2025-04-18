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
	Cursor               ocommon.Point
	ChainIter            *chain.ChainIterator
	NeedsInitialRollback bool
}

type State struct {
	sync.Mutex
	eventBus     *event.EventBus
	ledgerState  *ledger.LedgerState
	clients      map[ouroboros.ConnectionId]*ChainsyncClientState
	clientConnId *ouroboros.ConnectionId // TODO: replace with handling of multiple chainsync clients (#385)
}

func NewState(
	eventBus *event.EventBus,
	ledgerState *ledger.LedgerState,
) *State {
	s := &State{
		eventBus:    eventBus,
		ledgerState: ledgerState,
		clients:     make(map[ouroboros.ConnectionId]*ChainsyncClientState),
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
	// Remove client state entry
	delete(s.clients, connId)
}

// TODO: replace with handling of multiple chainsync clients (#385)
func (s *State) GetClientConnId() *ouroboros.ConnectionId {
	return s.clientConnId
}

// TODO: replace with handling of multiple chainsync clients (#385)
func (s *State) SetClientConnId(connId ouroboros.ConnectionId) {
	s.clientConnId = &connId
}

// TODO: replace with handling of multiple chainsync clients (#385)
func (s *State) RemoveClientConnId(connId ouroboros.ConnectionId) {
	if s.clientConnId != nil && *s.clientConnId == connId {
		s.clientConnId = nil
	}
}
