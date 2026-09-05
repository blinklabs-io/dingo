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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	ChainUpdateEventType = "chain.update"
	ChainForkEventType   = "chain.fork_detected"
	// ChainHeaderAnnouncementEventType carries a Leios endorser-block
	// announcement read from a ranking-block header at chainsync
	// roll-forward, before the block body has been fetched and applied.
	ChainHeaderAnnouncementEventType = "chain.header_announcement"
)

type ChainBlockEvent struct {
	Point ocommon.Point
	Block models.Block
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

// ChainHeaderAnnouncementEvent is published when a ranking-block header that
// announces a Leios endorser block is admitted to the header queue. It is a
// header-arrival signal, not an apply signal: the announcing ranking block has
// not been fetched, validated or applied when this is published, and it may
// still be rolled back.
//
// It exists because the Leios vote window is measured from the announcing
// ranking block's slot, while applying an EB-announcing ranking block waits on
// fetching that same endorser block. Consumers that must act inside the vote
// window cannot wait for ChainUpdateEventType.
type ChainHeaderAnnouncementEvent struct {
	// Slot is the announcing ranking block's slot.
	Slot uint64
	// RbHash is the announcing ranking block's header hash.
	RbHash lcommon.Blake2b256
	// EbHash is the announced endorser block's hash.
	EbHash lcommon.Blake2b256
	// EbSize is the announced endorser block's declared size in bytes.
	EbSize uint64
}
