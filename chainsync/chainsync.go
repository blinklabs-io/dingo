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

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/connection"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	maxRecentBlocks = 20 // Number of recent blocks to cache
)

type ChainsyncPoint struct {
	SlotNumber  uint64
	BlockHash   string
	BlockNumber uint64
}

func (c ChainsyncPoint) String() string {
	return fmt.Sprintf(
		"< slot_number = %d, block_number = %d, block_hash = %s >",
		c.SlotNumber,
		c.BlockNumber,
		c.BlockHash,
	)
}

func (c ChainsyncPoint) ToTip() ochainsync.Tip {
	hashBytes, _ := hex.DecodeString(c.BlockHash)
	return ochainsync.Tip{
		BlockNumber: c.BlockNumber,
		Point: ocommon.Point{
			Slot: c.SlotNumber,
			Hash: hashBytes[:],
		},
	}
}

type ChainsyncBlock struct {
	Point    ChainsyncPoint
	Cbor     []byte
	Type     uint
	Rollback bool
}

func (c ChainsyncBlock) String() string {
	return fmt.Sprintf(
		"%s (%d bytes)",
		c.Point.String(),
		len(c.Cbor),
	)
}

type ChainsyncClientState struct {
	Cursor               ChainsyncPoint
	BlockChan            chan ChainsyncBlock
	NeedsInitialRollback bool
}

type State struct {
	sync.Mutex
	tip          ChainsyncPoint
	clients      map[ouroboros.ConnectionId]*ChainsyncClientState
	recentBlocks []ChainsyncBlock // TODO: replace with hook(s) for block storage/retrieval
	subs         map[ouroboros.ConnectionId]chan ChainsyncBlock
}

func NewState() *State {
	return &State{
		clients: make(map[ouroboros.ConnectionId]*ChainsyncClientState),
	}
}

func (s *State) Tip() ChainsyncPoint {
	return s.tip
}

func (s *State) RecentBlocks() []ChainsyncBlock {
	// TODO: replace with hook to get recent blocks
	return s.recentBlocks[:]
}

func (s *State) AddClient(connId connection.ConnectionId, cursor ChainsyncPoint) *ChainsyncClientState {
	s.Lock()
	defer s.Unlock()
	// Create initial chainsync state for connection
	if _, ok := s.clients[connId]; !ok {
		s.clients[connId] = &ChainsyncClientState{
			Cursor:               cursor,
			BlockChan:            s.sub(connId),
			NeedsInitialRollback: true,
		}
	}
	return s.clients[connId]
}

func (s *State) RemoveClient(connId connection.ConnectionId) {
	s.Lock()
	defer s.Unlock()
	if clientState, ok := s.clients[connId]; ok {
		// Unsub from chainsync updates
		if clientState.BlockChan != nil {
			s.unsub(connId)
		}
		// Remove client state entry
		delete(s.clients, connId)
	}
}

func (s *State) sub(key ouroboros.ConnectionId) chan ChainsyncBlock {
	s.Lock()
	defer s.Unlock()
	tmpChan := make(chan ChainsyncBlock, maxRecentBlocks)
	if s.subs == nil {
		s.subs = make(map[ouroboros.ConnectionId]chan ChainsyncBlock)
	}
	s.subs[key] = tmpChan
	// Send all current blocks
	for _, block := range s.recentBlocks {
		tmpChan <- block
	}
	return tmpChan
}

func (s *State) unsub(key ouroboros.ConnectionId) {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.subs[key]; ok {
		close(s.subs[key])
		delete(s.subs, key)
	}
}

func (s *State) AddBlock(block ChainsyncBlock) {
	s.Lock()
	defer s.Unlock()
	// TODO: add hooks for storing new blocks
	s.recentBlocks = append(
		s.recentBlocks,
		block,
	)
	// Prune older blocks
	if len(s.recentBlocks) > maxRecentBlocks {
		s.recentBlocks = s.recentBlocks[len(s.recentBlocks)-maxRecentBlocks:]
	}
	// Publish new block to subscribers
	for _, pubChan := range s.subs {
		pubChan <- block
	}
}

func (s *State) Rollback(slot uint64, hash string) {
	s.Lock()
	defer s.Unlock()
	// TODO: add hook for getting recent blocks
	// Remove recent blocks newer than the rollback block
	for idx, block := range s.recentBlocks {
		if block.Point.SlotNumber > slot {
			s.recentBlocks = s.recentBlocks[:idx]
			break
		}
	}
	// Publish rollback to subscribers
	for _, pubChan := range s.subs {
		pubChan <- ChainsyncBlock{
			Rollback: true,
			Point: ChainsyncPoint{
				SlotNumber: slot,
				BlockHash:  hash,
			},
		}
	}
}
