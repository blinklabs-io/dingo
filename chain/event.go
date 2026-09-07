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
	// ChainHeaderEventType is the ordered header-lifecycle stream. It
	// carries ChainHeaderAnnouncementEvent when a ranking-block header
	// bearing a Leios endorser-block announcement enters the header queue,
	// and ChainHeaderInvalidationEvent when queued headers leave it again.
	// Both are enqueued on the chain-level sequencer under c.mutex, so a
	// single subscriber observes them in true chain-mutation order.
	ChainHeaderEventType = "chain.header"
)

type ChainBlockEvent struct {
	Point ocommon.Point
	Block models.Block
}

type ChainRollbackEvent struct {
	Point            ocommon.Point
	RolledBackBlocks []models.Block // Blocks that were rolled back, in reverse order (newest first)
	// Seq is the chain-mutation sequence number of this rollback, shared
	// with the ChainHeaderInvalidationEvent describing the same mutation.
	// It lets a consumer of both this event type and ChainHeaderEventType
	// tell which mutation came first even though the two types are
	// delivered on independent channels. Zero means unsequenced (an event
	// synthesized outside chain.Chain).
	Seq uint64
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

// ChainHeaderAnnouncementEvent is published when a ranking block announcing a
// Leios endorser block enters the chain, by either of the two routes a ranking
// block can take. Applied says which, and the two carry different guarantees:
//
//   - Applied false, the ordinary case: a peer's header was admitted to the
//     header queue. This is a header-arrival signal, not an apply signal. The
//     announcing ranking block has not been fetched, validated or applied when
//     this is published, and it may still be rolled back.
//   - Applied true: a locally forged block was added to the chain. A local
//     block never passes through the header queue, so its announcement is
//     emitted from the add itself, by which point the block is validated and
//     on the chain. It can still be rolled back later, so the invalidation
//     counterpart still applies, but it is not provisional in the way an
//     unfetched peer header is.
//
// A consumer that must not treat an applied local block as an unfetched header
// arrival should branch on Applied.
//
// It exists because the Leios vote window is measured from the announcing
// ranking block's slot, while applying an EB-announcing ranking block waits on
// fetching that same endorser block. Consumers that must act inside the vote
// window cannot wait for ChainUpdateEventType.
//
// Its counterpart is ChainHeaderInvalidationEvent: because a ranking block can
// leave the chain either way, a consumer must process both, and they are
// delivered on one event type precisely so it can never see them out of order.
type ChainHeaderAnnouncementEvent struct {
	// Slot is the announcing ranking block's slot.
	Slot uint64
	// RbHash is the announcing ranking block's header hash.
	RbHash lcommon.Blake2b256
	// EbHash is the announced endorser block's hash.
	EbHash lcommon.Blake2b256
	// EbSize is the announced endorser block's declared size in bytes.
	EbSize uint64
	// Seq is this header admission's chain-mutation sequence number.
	Seq uint64
	// Applied reports that the announcing ranking block was already on the
	// chain when this was published, which is the case for a locally forged
	// block: it never passes through the header queue, so its announcement
	// is emitted from the block add rather than from a header arrival. False
	// for the ordinary peer-header case, where the block has not been
	// fetched, validated or applied. See the type comment.
	Applied bool
}

// ChainHeaderInvalidationEvent is published when queued headers leave the
// header queue without becoming blocks on our chain: a rollback, or the header
// queue being discarded wholesale (peer switch, header mismatch, failed
// blockfetch start). Every ChainHeaderAnnouncementEvent above Point describes a
// ranking block that is no longer on our chain and must not be acted on.
//
// Point is the highest point that remains valid: the rollback point, or the
// block tip when the queue is discarded (everything above the block tip was
// queued headers and is now gone).
type ChainHeaderInvalidationEvent struct {
	Point ocommon.Point
	// RbHashes names the discarded ranking-block headers explicitly. It
	// exists because Point alone only describes a chain that shrank: when
	// the chain *grows* past discarded headers -- a locally forged block
	// replacing queued peer headers -- some of those headers sit at or
	// below the new tip, so no point-based rule can name them. Consumers
	// must drop announcements matching either rule.
	RbHashes []lcommon.Blake2b256
	// Reason is a short, stable label for logs and metrics.
	Reason string
	// Seq is this invalidation's chain-mutation sequence number. For a
	// rollback it equals the Seq on the ChainRollbackEvent describing the
	// same mutation.
	Seq uint64
}

// Reasons carried by ChainHeaderInvalidationEvent.
const (
	// HeaderInvalidationRollback: the chain rolled back to Point.
	HeaderInvalidationRollback = "rollback"
	// HeaderInvalidationQueueCleared: the queued headers above Point were
	// discarded without being fetched.
	HeaderInvalidationQueueCleared = "queue_cleared"
	// HeaderInvalidationLocalBlock: a locally forged block was added on the
	// same parent as the queued peer headers, which discards them. The
	// chain grew rather than shrank, so RbHashes, not Point, names them.
	HeaderInvalidationLocalBlock = "local_block"
)
