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

package state

import (
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	ChainBlockEventType    = "ledger.chain-block"
	ChainRollbackEventType = "ledger.chain-rollback"
)

type ChainBlockEvent struct {
	Point ocommon.Point
	Block database.Block
}

type ChainRollbackEvent struct {
	Point ocommon.Point
}

const (
	BlockfetchEventType event.EventType = "blockfetch.event"
	ChainsyncEventType  event.EventType = "chainsync.event"
)

// BlockfetchEvent represents either a Block or BatchDone blockfetch event. We use
// a single event type for both to make synchronization easier.
type BlockfetchEvent struct {
	ConnectionId ouroboros.ConnectionId // Connection ID associated with event
	Point        ocommon.Point          // Chain point for block
	Block        ledger.Block
	Type         uint // Block type ID
	BatchDone    bool // Set to true for a BatchDone event
}

// ChainsyncEvent represents either a RollForward or RollBackward chainsync event.
// We use a single event type for both to make synchronization easier.
type ChainsyncEvent struct {
	ConnectionId ouroboros.ConnectionId // Connection ID associated with event
	Point        ocommon.Point          // Chain point for roll forward/backward
	Tip          ochainsync.Tip         // Upstream chain tip
	BlockNumber  uint64
	BlockHeader  ledger.BlockHeader
	Type         uint // Block or header type ID
	Rollback     bool
}
