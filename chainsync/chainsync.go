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
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/blinklabs-io/node/event"
	"github.com/blinklabs-io/node/state"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/connection"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ChainsyncClientState struct {
	Cursor               ocommon.Point
	ChainIter            *state.ChainIterator
	NeedsInitialRollback bool
}

type State struct {
	sync.Mutex
	eventBus     *event.EventBus
	ledgerState  *state.LedgerState
	clients      map[ouroboros.ConnectionId]*ChainsyncClientState
	clientConnId *ouroboros.ConnectionId // TODO: replace with handling of multiple chainsync clients
	metrics      struct {
		blockNum prometheus.Gauge
		slotNum  prometheus.Gauge
	}
}

func NewState(eventBus *event.EventBus, ledgerState *state.LedgerState, promRegistry prometheus.Registerer) *State {
	s := &State{
		eventBus:    eventBus,
		ledgerState: ledgerState,
		clients:     make(map[ouroboros.ConnectionId]*ChainsyncClientState),
	}
	// Init metrics
	promautoFactory := promauto.With(promRegistry)
	s.metrics.blockNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockNum_int",
		Help: "current block number",
	})
	s.metrics.slotNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_slotNum_int",
		Help: "current slot number",
	})
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

// TODO: replace with handling of multiple chainsync clients
func (s *State) GetClientConnId() *ouroboros.ConnectionId {
	return s.clientConnId
}

// TODO: replace with handling of multiple chainsync clients
func (s *State) SetClientConnId(connId ouroboros.ConnectionId) {
	s.clientConnId = &connId
}

// TODO: replace with handling of multiple chainsync clients
func (s *State) RemoveClientConnId(connId ouroboros.ConnectionId) {
	if s.clientConnId != nil && *s.clientConnId == connId {
		s.clientConnId = nil
	}
}

func (s *State) AddBlock(block ledger.Block, blockType uint) error {
	s.Lock()
	defer s.Unlock()
	slotNumber := block.SlotNumber()
	// TODO: figure out something for Byron. this won't work, since the
	// block number isn't stored in the block itself
	blockNumber := block.BlockNumber()
	// Uodate metrics
	s.metrics.blockNum.Set(float64(blockNumber))
	s.metrics.slotNum.Set(float64(slotNumber))
	// Generate event
	blkHash, err := hex.DecodeString(block.Hash())
	if err != nil {
		return fmt.Errorf("decode block hash: %w", err)
	}
	s.eventBus.Publish(
		state.ChainsyncEventType,
		event.NewEvent(
			state.ChainsyncEventType,
			state.ChainsyncEvent{
				Point: ocommon.NewPoint(block.SlotNumber(), blkHash),
				Type:  blockType,
				Block: block,
			},
		),
	)
	return nil
}

func (s *State) Rollback(slot uint64, hash string) error {
	s.Lock()
	defer s.Unlock()
	// Generate event
	blkHash, err := hex.DecodeString(hash)
	if err != nil {
		return fmt.Errorf("decode block hash: %w", err)
	}
	s.eventBus.Publish(
		state.ChainsyncEventType,
		event.NewEvent(
			state.ChainsyncEventType,
			state.ChainsyncEvent{
				Rollback: true,
				Point:    ocommon.NewPoint(slot, blkHash),
			},
		),
	)
	return nil
}
