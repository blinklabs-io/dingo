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

package event

import "time"

// BlockForgedEventType is the event type for locally forged blocks
const BlockForgedEventType = EventType("block.forged")

// BlockForgedEvent is emitted when the node successfully forges a new block.
// This event is published after the block has been added to the local chain
// via chain.AddBlock(), which triggers automatic propagation to connected peers.
type BlockForgedEvent struct {
	// Slot is the slot number where the block was forged
	Slot uint64
	// BlockNumber is the block height in the chain
	BlockNumber uint64
	// BlockHash is the hash of the forged block
	BlockHash []byte
	// TxCount is the number of transactions included in the block
	TxCount uint
	// BlockSize is the size of the block in bytes
	BlockSize uint
	// Timestamp is when the block was forged
	Timestamp time.Time
}
