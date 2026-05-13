// Copyright 2025 Blink Labs Software
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

package chain

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	ChainUpdateEventType   = "chain.update"
	ChainForkEventType     = "chain.fork_detected"
	BlockProposedEventType = "chain.block_proposed"
)

type ChainBlockEvent struct {
	Point ocommon.Point
	Block models.Block
}

// BlockProposedEvent carries a locally forged block to the chain component for
// adoption. Ack is optional; when set, the chain handler reports the AddBlock
// result without blocking if the sender has already moved on.
type BlockProposedEvent struct {
	Block ledger.Block
	Ack   chan<- error
}

func (e BlockProposedEvent) Respond(err error) {
	if e.Ack == nil {
		return
	}
	select {
	case e.Ack <- err:
	default:
	}
}

type ChainRollbackEvent struct {
	Point            ocommon.Point
	RolledBackBlocks []models.Block // Blocks that were rolled back, in reverse order (newest first)
}

// ChainForkEvent is emitted when a chain fork is detected.
// This allows subscribers to monitor fork activity for alerting and metrics.
type ChainForkEvent struct {
	// ForkPoint is the common ancestor where the chains diverge
	ForkPoint ocommon.Point
	// ForkDepth is the number of blocks rolled back from the canonical chain
	ForkDepth uint64
	// AlternateHead is the tip of the competing chain
	AlternateHead ocommon.Point
	// CanonicalHead is the tip of the current canonical chain
	CanonicalHead ocommon.Point
}
