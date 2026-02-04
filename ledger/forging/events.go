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

// Package forging contains types and utilities for block production.
//
// # Block Propagation
//
// Block propagation to peers is handled automatically by the chain package.
// When a forged block is added via chain.AddBlock(), the method closes
// the chain's waitingChan (see chain/chain.go lines 174-180), which signals
// any blocking ChainIterators. The ouroboros chainsync server (see
// ouroboros/chainsync.go chainsyncServerRequestNext) waits on these iterators
// via ChainIterator.Next(true), which blocks on waitingChan when at chain tip.
// When the channel is closed, iterators wake up and deliver the new block
// to connected peers via RollForward messages.
//
// This means there is no need for explicit propagation logic when forging
// blocks - adding the block to the chain automatically triggers delivery
// to all subscribed chainsync clients.
package forging

import "github.com/blinklabs-io/dingo/event"

// SlotBattleEventType is the event type for slot battles (competing blocks)
const SlotBattleEventType = event.EventType("forging.slot-battle")

// SlotBattleEvent is emitted when the node detects competing blocks for the
// same slot, either from receiving an external block while preparing to forge
// or when detecting a fork at the same slot height.
type SlotBattleEvent struct {
	// Slot is the slot number where the battle occurred
	Slot uint64
	// LocalBlockHash is the hash of our locally forged block (if any)
	LocalBlockHash []byte
	// RemoteBlockHash is the hash of the competing block from peers
	RemoteBlockHash []byte
	// Won indicates whether our local block was selected for the chain
	Won bool
}
