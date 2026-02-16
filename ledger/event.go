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

package ledger

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	BlockfetchEventType        event.EventType = "blockfetch.event"
	BlockEventType             event.EventType = "ledger.block"
	ChainsyncEventType         event.EventType = "chainsync.event"
	LedgerErrorEventType       event.EventType = "ledger.error"
	PoolStateRestoredEventType event.EventType = "ledger.pool.restored"
	TransactionEventType       event.EventType = "ledger.tx"
)

// It represents the direction a block is applied to the ledger.
type BlockAction string

const (
	BlockActionApply BlockAction = "Apply"
	BlockActionUndo  BlockAction = "Undo"
)

// It represents a persisted block apply or rollback action.
type BlockEvent struct {
	Action BlockAction
	Block  models.Block
	Point  ocommon.Point
}

// BlockfetchEvent represents either a Block or BatchDone blockfetch event. We use
// a single event type for both to make synchronization easier.
type BlockfetchEvent struct {
	ConnectionId ouroboros.ConnectionId // Connection ID associated with event
	Block        ledger.Block
	Point        ocommon.Point // Chain point for block
	Type         uint          // Block type ID
	BatchDone    bool          // Set to true for a BatchDone event
}

// ChainsyncEvent represents either a RollForward or RollBackward chainsync event.
// We use a single event type for both to make synchronization easier.
type ChainsyncEvent struct {
	ConnectionId ouroboros.ConnectionId // Connection ID associated with event
	BlockHeader  ledger.BlockHeader
	Point        ocommon.Point  // Chain point for roll forward/backward
	Tip          ochainsync.Tip // Upstream chain tip
	BlockNumber  uint64
	Type         uint // Block or header type ID
	Rollback     bool // Set to true for a Rollback event
}

// LedgerErrorEvent represents an error that occurred during ledger processing.
type LedgerErrorEvent struct {
	Error     error         // The actual error that occurred
	Operation string        // The operation that failed (e.g., "block_header", "rollback")
	Point     ocommon.Point // Chain point where the error occurred, if applicable
}

// PoolStateRestoredEvent is emitted after pool state is restored during a rollback.
// Subscribers (like peer providers) can use this to invalidate cached pool data.
type PoolStateRestoredEvent struct {
	Slot uint64 // The slot to which pool state was restored
}

// TransactionEvent is emitted when a transaction is applied or rolled back.
// Check the Rollback field to determine direction.
type TransactionEvent struct {
	Transaction ledger.Transaction
	Point       ocommon.Point
	BlockNumber uint64
	TxIndex     uint32
	Rollback    bool
}
