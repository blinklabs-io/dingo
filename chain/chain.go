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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	initialBlockIndex uint64 = 1
	// Mainnet full blocks can make larger batches exceed practical Badger
	// transaction limits during import, so keep the runtime batch size
	// conservative even if smaller benchmark fixtures tolerate more.
	blockImportBatchSize = 50
	// Keep a dense window near the tip so short peer gaps intersect on a
	// recent block, then fall back to exponentially older points to avoid
	// origin intersects when the peer lags by more than a few dozen blocks.
	intersectDensePointCount = 32
)

type Chain struct {
	eventBus             *event.EventBus
	manager              *ChainManager
	waitingChan          chan struct{}
	headers              []queuedHeader
	blocks               []ocommon.Point
	iterators            []*ChainIterator
	currentTip           ochainsync.Tip
	tipBlockIndex        uint64
	mutationGeneration   uint64
	lastCommonBlockIndex uint64
	id                   ChainId
	mutex                sync.RWMutex
	waitingChanMutex     sync.Mutex
	persistent           bool

	// pendingUpdates is the chain-level sequencer for deferred chain.update
	// / chain.fork publication. The mutex-holding ledger paths (blockfetch
	// add under chainsyncBlockfetchMutex, rollback under chainsyncMutex) must
	// not publish inline -- a back-pressured Publish there deadlocks the node
	// (see ledger.pendingPublishes). They instead enqueue the event onto this
	// single shared queue *atomically with the chain mutation, under c.mutex*
	// (queueDeferredEventLocked), and drain it after releasing their outer
	// mutex (PublishPendingChainUpdates).
	//
	// Because enqueue happens under c.mutex -- the same lock that serializes
	// every chain mutation -- enqueue order equals mutation order, and a
	// single FIFO drain therefore publishes in mutation order across BOTH
	// handlers. A blockfetch add and a chainsync rollback can no longer invert
	// (add queued, rollback published first) the way two independent
	// per-handler queues could, which would have driven the chain.update
	// subscriber's block apply/undo notifications out of order.
	//
	// pendingUpdatesMutex guards the slice for the brief enqueue/dequeue; it
	// is never held across a Publish. publishMutex serializes drains so that,
	// when two goroutines flush concurrently, their Publish calls stay in
	// pop (FIFO) order rather than interleaving. Neither is a ledger mutex, so
	// draining -- which only ever runs after the caller has released its outer
	// ledger mutex -- cannot reintroduce the drain deadlock.
	pendingUpdates      []event.Event
	pendingUpdatesMutex sync.Mutex
	publishMutex        sync.Mutex

	// batchCommitMutex keeps a rollback from resolving block indices that a
	// chain-owned batch transaction has already applied to the in-memory
	// chain but has not yet committed.
	//
	// addRawBlocks and AddBlocks advance c.tipBlockIndex, c.currentTip,
	// c.headers and c.blocks inside their transaction's closure while holding
	// c.mutex, and txn.Do only commits after that closure has returned and
	// both chain locks have been released. Anything that takes c.mutex in the
	// window between the two observes a tip index whose block the store
	// cannot serve yet: ChainManager.removeBlockByIndex opens its own
	// transaction, and no transaction can see another's uncommitted writes.
	// rollbackLocked's removal loop starts at c.tipBlockIndex, so it failed
	// its very first iteration with models.ErrBlockNotFound at an index the
	// in-memory chain legitimately holds.
	//
	// A batch holds this for read from before it mutates memory until its
	// transaction has concluded; rollbackLocked holds it for write, so the
	// removal loop only ever runs against a store that has caught up with
	// memory. The batch's read hold is deferred until txn.Do returns, so a
	// transaction panic cannot leak the hold or strand a later rollback.
	// Go's RWMutex is writer-preferring, so a waiting rollback also blocks
	// further batches from starting rather than being starved by them.
	//
	// It covers only the transactions the chain itself owns. addBlockInternal
	// takes a caller-supplied transaction whose commit the chain neither
	// performs nor observes, so the same window remains open there.
	batchCommitMutex sync.RWMutex
}

type queuedHeader struct {
	header         ledger.BlockHeader
	point          ocommon.Point
	prevHash       []byte
	blockNumber    uint64
	cryptoVerified bool
}

func (c *Chain) Tip() ochainsync.Tip {
	if c == nil {
		return ochainsync.Tip{}
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.currentTip
}

func (c *Chain) HeaderTip() ochainsync.Tip {
	if c == nil {
		return ochainsync.Tip{}
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.headerTip()
}

// TipPredecessor reports the block context an alternative to the current chain
// tip must be built on: the tip itself (the block that would be competed with)
// and the point of the tip's immediate predecessor, which becomes the
// alternative block's parent.
//
// This is the data ouroboros-consensus' mkCurrentBlockContext returns for its
// EQ case, where a leader whose slot is already occupied forges an alternative
// with the same block number as the occupant and the occupant's predecessor as
// parent (Ouroboros/Consensus/NodeKernel/Forge.hs).
//
// ok is false whenever that context cannot be established: an empty chain, a
// tip that is the first block on the chain (its parent is genesis, which is not
// a rollback target here), or a predecessor the chain can no longer resolve.
// Callers must treat that as "no alternative can be built" and must not fall
// back to the live tip: binding the tip itself as parent produces a block whose
// parent slot equals its own, which envelope validation and every Praos peer
// reject.
func (c *Chain) TipPredecessor() (
	parent ocommon.Point,
	tip ochainsync.Tip,
	ok bool,
) {
	if c == nil {
		return ocommon.Point{}, ochainsync.Tip{}, false
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	// Both lookups below go through blockByIndexLocked, which resolves a
	// common-prefix index against the primary chain when this chain is an
	// in-memory fork. That read needs the primary-chain lock as well as the
	// manager lock, in the chain -> primary -> manager order this helper
	// establishes; taking manager.mutex directly would let a concurrent
	// primary-chain mutation move the common prefix between the two lookups
	// and yield a tip and a parent that never sat next to each other.
	unlockBlockIndexReadLocks := c.lockBlockIndexReadLocks()
	defer unlockBlockIndexReadLocks()
	// initialBlockIndex is the first block; it has no predecessor on this
	// chain, and neither does an empty chain.
	if c.tipBlockIndex <= initialBlockIndex {
		return ocommon.Point{}, ochainsync.Tip{}, false
	}
	tipBlock, err := c.blockByIndexLocked(c.tipBlockIndex)
	if err != nil {
		return ocommon.Point{}, ochainsync.Tip{}, false
	}
	parentBlock, err := c.blockByIndexLocked(c.tipBlockIndex - 1)
	if err != nil {
		return ocommon.Point{}, ochainsync.Tip{}, false
	}
	// Cross-check the two records against each other and against the tip this
	// chain advertises. The chain maintains all three invariants, but a caller
	// of this function is about to sign a block against the answer, so a
	// disagreement is reported as "no context" rather than resolved.
	//
	// The parent must also sit strictly below the tip's slot. That is the
	// ordering contract this function's doc comment states, and the caller
	// signs against the answer: a parent whose slot is at or above the
	// contested slot produces a block Praos and envelope validation reject.
	if !bytes.Equal(tipBlock.Hash, c.currentTip.Point.Hash) ||
		!bytes.Equal(tipBlock.PrevHash, parentBlock.Hash) ||
		parentBlock.Slot >= tipBlock.Slot {
		return ocommon.Point{}, ochainsync.Tip{}, false
	}
	return ocommon.NewPoint(parentBlock.Slot, parentBlock.Hash),
		c.currentTip,
		true
}

// blockNumberContiguous reports whether a block number legitimately follows its
// parent's. Shelley-era and later increment by exactly one per block. Byron
// epoch-boundary blocks reuse the parent's block number (they do not increment),
// so in the Byron era both parentNumber and parentNumber+1 are valid. Any other
// value (notably an inflated one) is non-contiguous and rejected.
func blockNumberContiguous(eraId uint8, blockNumber, parentNumber uint64) bool {
	if blockNumber == parentNumber+1 {
		return true
	}
	if eraId == byron.EraIdByron && blockNumber == parentNumber {
		return true
	}
	return false
}

func (c *Chain) headerTip() ochainsync.Tip {
	if len(c.headers) == 0 {
		return c.currentTip
	}
	lastHeader := c.headers[len(c.headers)-1]
	return ochainsync.Tip{
		Point:       lastHeader.point,
		BlockNumber: lastHeader.blockNumber,
	}
}

// MaxQueuedHeaders returns the maximum number of headers that may be
// queued. The limit is the larger of securityParam * 2 and
// DefaultMaxQueuedHeaders. Using the default as a floor ensures the
// queue is large enough for the chainsync/blockfetch pipeline: headers
// arrive much faster than blocks, so the queue must accommodate several
// blockfetch batches worth of headers beyond the accumulation threshold
// to avoid drops that break the header chain.
func (c *Chain) MaxQueuedHeaders() int {
	if c == nil || c.manager == nil {
		return DefaultMaxQueuedHeaders
	}
	// Before SetLedger succeeds, securityParam is zero and the default
	// floor applies (tests or early bootstrap only).
	if sp := c.manager.securityParam; sp > 0 {
		return max(sp*2, DefaultMaxQueuedHeaders)
	}
	return DefaultMaxQueuedHeaders
}

func (c *Chain) AddBlockHeader(header ledger.BlockHeader) error {
	return c.addBlockHeader(header, false)
}

func (c *Chain) AddVerifiedBlockHeader(header ledger.BlockHeader) error {
	return c.addBlockHeader(header, true)
}

func (c *Chain) addBlockHeader(
	header ledger.BlockHeader,
	cryptoVerified bool,
) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	headerHash := header.Hash()
	headerPrevHash := header.PrevHash()
	queued := queuedHeader{
		header: header,
		point: ocommon.Point{
			Slot: header.SlotNumber(),
			Hash: headerHash.Bytes(),
		},
		prevHash:       headerPrevHash.Bytes(),
		blockNumber:    header.BlockNumber(),
		cryptoVerified: cryptoVerified,
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Reject headers when the queue is at capacity to prevent
	// unbounded memory growth from a malicious peer.
	if len(c.headers) >= c.MaxQueuedHeaders() {
		return ErrHeaderQueueFull
	}
	// Make sure header fits on chain tip
	if c.tipBlockIndex >= initialBlockIndex ||
		len(c.headers) > 0 {
		headerTip := c.headerTip()
		if !bytes.Equal(queued.prevHash, headerTip.Point.Hash) {
			return NewBlockNotFitChainTipError(
				headerHash.String(),
				headerPrevHash.String(),
				hex.EncodeToString(headerTip.Point.Hash),
			)
		}
		// Bind the header's self-reported block number to the parent's. The
		// header chains onto the tip (prev hash matched above), so its block
		// number must be contiguous: exactly parent+1 for Shelley-era and
		// later. Byron epoch-boundary blocks reuse the parent's number, so in
		// the Byron era both parent and parent+1 are accepted. Rejecting a
		// non-contiguous number stops a forged (inflated) block number from
		// entering the chain and winning chain selection's longest-chain rule.
		if !blockNumberContiguous(
			header.Era().Id,
			queued.blockNumber,
			headerTip.BlockNumber,
		) {
			return NewBlockNumberNotContiguousError(
				headerHash.String(),
				queued.blockNumber,
				headerTip.BlockNumber,
			)
		}
	}
	// Add header
	c.headers = append(c.headers, queued)
	return nil
}

func (c *Chain) AddBlock(
	block ledger.Block,
	txn *database.Txn,
) error {
	evt, err := c.addBlockInternal(block, ocommon.Point{}, txn, true, false, nil)
	if err != nil {
		return err
	}
	// Publish event immediately for standalone (non-batched) callers.
	// forgeBlock reaches this on the scheduler goroutine, not under a ledger
	// mutex, so a backpressured Publish here cannot stall the chainsync
	// drain. The mutex-holding blockfetch drain uses AddBlockWithPointDeferred
	// instead, which returns the event for the ledger to publish after it
	// releases chainsyncBlockfetchMutex. See ledger.pendingPublishes and the
	// blinklabs-io/dingo drain deadlock.
	if c.eventBus != nil && evt.Type != "" {
		c.eventBus.Publish(ChainUpdateEventType, evt)
	}
	return nil
}

// AddLocalBlock adds a locally forged block without comparing it to queued
// peer headers. A successful local block invalidates those pending headers;
// the actual chain-tip and block-number checks remain mandatory.
func (c *Chain) AddLocalBlock(block ledger.Block) error {
	evt, err := c.addBlockInternal(
		block,
		ocommon.Point{},
		nil,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	if c.eventBus != nil && evt.Type != "" {
		c.eventBus.Publish(ChainUpdateEventType, evt)
	}
	return nil
}

// AddLocalBlockDeferred adds a locally forged block exactly like AddLocalBlock
// but, instead of publishing the resulting chain.update inline, enqueues it on
// the chain-level sequencer under c.mutex and returns it. This is the entry
// point for a forged block adopted from a path that holds a ledger mutex --
// specifically the equal-slot alternative, which rolls the chain back to the
// contested block's parent under chainsyncMutex and then adopts the local
// sibling. Publishing inline from there is the drain deadlock described on
// AddBlockWithPointDeferred; enqueuing under c.mutex also keeps this add
// ordered behind the rollback that preceded it. A returned event with an empty
// Type means there is nothing to publish.
func (c *Chain) AddLocalBlockDeferred(
	block ledger.Block,
) (event.Event, error) {
	return c.addBlockInternal(
		block,
		ocommon.Point{},
		nil,
		false,
		true,
		nil,
	)
}

// AddBlockWithPoint adds a block using a caller-supplied point. This avoids
// recomputing the block hash when the caller already has the canonical slot/hash
// pair from a validated upstream source such as blockfetch.
func (c *Chain) AddBlockWithPoint(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
) error {
	evt, err := c.addBlockInternal(block, point, txn, true, false, nil)
	if err != nil {
		return err
	}
	if c.eventBus != nil && evt.Type != "" {
		c.eventBus.Publish(ChainUpdateEventType, evt)
	}
	return nil
}

// AddBlockWithPointDeferred adds a block exactly like AddBlockWithPoint but,
// instead of publishing the resulting chain.update inline, enqueues it on the
// chain-level sequencer under c.mutex and returns it (the return value is
// retained for callers/tests that inspect it; publication happens only through
// the sequencer). The ledger's chainsync/blockfetch drain calls this while
// holding chainsyncBlockfetchMutex and then drains the sequencer after the
// mutex is released, so the (potentially backpressured) delivery never runs
// under the lock. Publishing inline under that mutex is what deadlocked the
// node: a terminal chain.update subscriber that stopped draining parked the
// publish with the mutex held, handleEventChainsync then blocked on the same
// mutex, and the ledger.chainsync buffer filled (blinklabs-io/dingo preview
// freeze). Enqueuing under c.mutex also keeps this add ordered against a
// concurrent chainsync rollback in true chain-mutation order; see the
// pendingUpdates field and PublishPendingChainUpdates. A returned event with an
// empty Type means there is nothing to publish.
func (c *Chain) AddBlockWithPointDeferred(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
) (event.Event, error) {
	return c.addBlockInternal(block, point, txn, true, true, nil)
}

// AddBlockWithPointDeferredIf is AddBlockWithPointDeferred with a caller
// supplied admission predicate, evaluated while the chain mutex is held and
// before any chain state is read or written. When admit reports false the add
// is abandoned and ErrBlockAddNotAdmitted is returned; nothing is mutated and
// no event is queued.
//
// It exists because a precondition checked by the caller before calling is
// not a precondition of the mutation. The blockfetch drain holds
// chainsyncBlockfetchMutex, not the chainsyncMutex the equal-slot alternative
// adoption holds, so a rollback can publish a new generation and truncate the
// chain in the window between the drain testing its batch and this add taking
// the chain mutex. Passing the test as a predicate closes that window: the
// generation comparison and the tip mutation it guards become one atomic step
// under the same lock every other chain mutation takes.
//
// admit must not acquire the chain or manager mutex, and must not block: it
// runs on the chain's own write path.
func (c *Chain) AddBlockWithPointDeferredIf(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
	admit func() bool,
) (event.Event, error) {
	return c.addBlockInternal(block, point, txn, true, true, admit)
}

// queueDeferredEventLocked appends evt to the chain-level sequencer. The caller
// must hold c.mutex, so the enqueue is atomic with the chain mutation that
// produced evt: no other mutation can interleave between updating the tip and
// recording its event, which is what makes enqueue order equal mutation order.
// See the pendingUpdates field.
func (c *Chain) queueDeferredEventLocked(evt event.Event) {
	if c == nil || c.eventBus == nil || evt.Type == "" {
		return
	}
	c.pendingUpdatesMutex.Lock()
	c.pendingUpdates = append(c.pendingUpdates, evt)
	c.pendingUpdatesMutex.Unlock()
}

// PublishPendingChainUpdates drains the chain-level sequencer, publishing every
// queued deferred event strictly FIFO -- i.e. in chain-mutation order. It is
// safe to call from any goroutine and must be called only after the caller has
// released its outer ledger mutex (chainsyncMutex / chainsyncBlockfetchMutex);
// a drain may publish an event another handler enqueued, so publishing under a
// ledger mutex would reintroduce the drain deadlock.
//
// publishMutex serializes concurrent drains so their Publish calls preserve pop
// order; pendingUpdatesMutex is dropped before each (potentially
// back-pressured) Publish so an enqueue never blocks behind delivery.
func (c *Chain) PublishPendingChainUpdates() {
	if c == nil || c.eventBus == nil {
		return
	}
	c.publishMutex.Lock()
	defer c.publishMutex.Unlock()
	for {
		c.pendingUpdatesMutex.Lock()
		if len(c.pendingUpdates) == 0 {
			c.pendingUpdates = nil
			c.pendingUpdatesMutex.Unlock()
			return
		}
		evt := c.pendingUpdates[0]
		c.pendingUpdates = c.pendingUpdates[1:]
		c.pendingUpdatesMutex.Unlock()
		c.eventBus.Publish(evt.Type, evt)
	}
}

// addBlockInternal performs all block-adding logic but returns the event
// instead of publishing it. This allows AddBlocks to defer event
// publication until the entire batch transaction has committed, preventing
// subscribers from observing data that may be rolled back.
func (c *Chain) addBlockInternal(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
	matchPendingHeader bool,
	deferred bool,
	admit func() bool,
) (event.Event, error) {
	if c == nil {
		return event.Event{}, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// We get a write lock on the manager to cover the integrity checks and adding the block below
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Ask the caller, under the same lock that serializes every chain
	// mutation, whether this add is still wanted. Evaluating it here is the
	// point: a caller that checks its own precondition before calling has
	// already released whatever it inspected, so a rollback landing in
	// between would leave that check stale. See AddBlockWithPointDeferredIf.
	if admit != nil && !admit() {
		return event.Event{}, ErrBlockAddNotAdmitted
	}
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return event.Event{}, fmt.Errorf("reconcile chain: %w", err)
	}
	evt, err := c.addBlockLocked(
		block,
		point,
		txn,
		true,
		matchPendingHeader,
	)
	if err != nil {
		return event.Event{}, err
	}
	// Deferred callers (the mutex-holding blockfetch drain) publish through the
	// chain-level sequencer rather than inline. Enqueue while c.mutex is still
	// held so this add is sequenced ahead of any mutation that acquires the
	// lock after it; the caller drains after releasing its outer ledger mutex.
	// See the pendingUpdates field and PublishPendingChainUpdates.
	if deferred {
		c.queueDeferredEventLocked(evt)
	}
	return evt, nil
}

func (c *Chain) addBlockLocked(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
	notifyWaiters bool,
	matchPendingHeader bool,
) (event.Event, error) {
	blockHashBytes := point.Hash
	if len(blockHashBytes) == 0 {
		blockHashBytes = block.Hash().Bytes()
		point = ocommon.NewPoint(block.SlotNumber(), blockHashBytes)
	}
	blockPrevHashBytes := []byte(nil)
	blockNumber := block.BlockNumber()
	// Check that the new block matches our first header, if any
	if matchPendingHeader && len(c.headers) > 0 {
		firstHeader := c.headers[0]
		if !bytes.Equal(blockHashBytes, firstHeader.point.Hash) {
			return event.Event{}, NewBlockNotMatchHeaderError(
				hex.EncodeToString(blockHashBytes),
				firstHeader.header.Hash().String(),
			)
		}
		blockPrevHashBytes = firstHeader.prevHash
		blockNumber = firstHeader.blockNumber
	}
	if len(blockPrevHashBytes) == 0 {
		blockPrevHashBytes = block.PrevHash().Bytes()
	}
	// Check that this block fits on the current chain tip
	if c.tipBlockIndex >= initialBlockIndex {
		if !bytes.Equal(blockPrevHashBytes, c.currentTip.Point.Hash) {
			return event.Event{}, NewBlockNotFitChainTipError(
				hex.EncodeToString(blockHashBytes),
				hex.EncodeToString(blockPrevHashBytes),
				hex.EncodeToString(c.currentTip.Point.Hash),
			)
		}
		// Bind the block number to the parent's (defense in depth alongside the
		// header-ingestion check): a block that fits on the tip must carry a
		// contiguous number so a forged number cannot enter the chain.
		if !blockNumberContiguous(
			block.Era().Id,
			blockNumber,
			c.currentTip.BlockNumber,
		) {
			return event.Event{}, NewBlockNumberNotContiguousError(
				hex.EncodeToString(blockHashBytes),
				blockNumber,
				c.currentTip.BlockNumber,
			)
		}
	}
	// Build new block record
	tmpPoint := point
	newBlockIndex := c.tipBlockIndex + 1
	tmpBlock := models.Block{
		ID:       newBlockIndex,
		Slot:     tmpPoint.Slot,
		Hash:     tmpPoint.Hash,
		Number:   blockNumber,
		Type:     uint(block.Type()), //nolint:gosec
		PrevHash: blockPrevHashBytes,
		Cbor:     block.Cbor(),
	}
	if err := c.manager.addBlock(tmpBlock, txn, c.persistent); err != nil {
		return event.Event{}, fmt.Errorf("store block: %w", err)
	}
	if !c.persistent {
		c.blocks = append(c.blocks, tmpPoint)
	}
	// Remove matching header entry, if any
	if matchPendingHeader && len(c.headers) > 0 {
		c.headers = slices.Delete(c.headers, 0, 1)
	} else if !matchPendingHeader {
		c.headers = c.headers[:0]
	}
	// Update tip
	c.currentTip = ochainsync.Tip{
		Point:       tmpPoint,
		BlockNumber: blockNumber,
	}
	c.tipBlockIndex = newBlockIndex
	c.mutationGeneration++
	if notifyWaiters {
		c.notifyWaitingIterators()
	}
	if c.eventBus == nil {
		return event.Event{}, nil
	}
	// Build event for caller to publish after transaction commit
	evt := event.NewEvent(
		ChainUpdateEventType,
		ChainBlockEvent{
			Point: tmpPoint,
			Block: tmpBlock,
		},
	)
	return evt, nil
}

func (c *Chain) AddBlocks(blocks []ledger.Block) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	batchOffset := 0
	batchSize := 0
	for {
		batchSize = min(
			blockImportBatchSize,
			len(blocks)-batchOffset,
		)
		if batchSize == 0 {
			break
		}
		// Collect events during the transaction so they can be
		// published only after the transaction commits successfully.
		// This prevents subscribers from observing rolled-back data
		// when a later block in the batch fails.
		pendingEvents := make([]event.Event, 0, batchSize)
		// Hold the batch-commit barrier from before this batch mutates the
		// in-memory chain until its transaction has concluded, so a
		// concurrent rollback cannot resolve an index the store has yet to
		// commit. See the batchCommitMutex field.
		err := func() error {
			c.batchCommitMutex.RLock()
			defer c.batchCommitMutex.RUnlock()
			txn := c.manager.db.BlobTxn(true)
			var (
				savedTip             ochainsync.Tip
				savedTipBlockIndex   uint64
				savedGeneration      uint64
				savedHeaders         []queuedHeader
				savedBlocks          []ocommon.Point
				batchApplied         bool
				appliedTip           ochainsync.Tip
				appliedTipBlockIndex uint64
				appliedGeneration    uint64
			)
			err := txn.Do(func(txn *database.Txn) error {
				c.mutex.Lock()
				defer c.mutex.Unlock()
				c.manager.mutex.Lock()
				defer c.manager.mutex.Unlock()
				if err := c.reconcile(); err != nil {
					return fmt.Errorf("reconcile chain: %w", err)
				}
				savedTip = c.currentTip
				savedTipBlockIndex = c.tipBlockIndex
				savedGeneration = c.mutationGeneration
				savedHeaders = slices.Clone(c.headers)
				if !c.persistent {
					savedBlocks = slices.Clone(c.blocks)
				}
				for _, tmpBlock := range blocks[batchOffset : batchOffset+batchSize] {
					evt, err := c.addBlockLocked(
						tmpBlock,
						ocommon.Point{},
						txn,
						false,
						true,
					)
					if err != nil {
						c.currentTip = savedTip
						c.tipBlockIndex = savedTipBlockIndex
						c.mutationGeneration = savedGeneration
						c.headers = savedHeaders
						if !c.persistent {
							c.blocks = savedBlocks
						}
						return err
					}
					if evt.Type != "" {
						pendingEvents = append(pendingEvents, evt)
					}
				}
				batchApplied = true
				appliedTip = c.currentTip
				appliedTipBlockIndex = c.tipBlockIndex
				appliedGeneration = c.mutationGeneration
				return nil
			})
			if err != nil && batchApplied {
				c.mutex.Lock()
				c.manager.mutex.Lock()
				if c.batchRestoreIsSafeLocked(
					appliedTip,
					appliedTipBlockIndex,
					appliedGeneration,
				) {
					c.currentTip = savedTip
					c.tipBlockIndex = savedTipBlockIndex
					c.mutationGeneration = savedGeneration
					c.headers = savedHeaders
					if !c.persistent {
						c.blocks = savedBlocks
					}
				} else {
					slog.Default().Error(
						"skipped in-memory restore after block batch commit failure: chain moved under the batch",
						"component", "chain",
						"chain_id", c.id,
						"applied_tip_block_index", appliedTipBlockIndex,
						"tip_block_index", c.tipBlockIndex,
						"applied_generation", appliedGeneration,
						"mutation_generation", c.mutationGeneration,
						"applied_tip_slot", appliedTip.Point.Slot,
						"applied_tip_hash", hex.EncodeToString(appliedTip.Point.Hash),
						"tip_slot", c.currentTip.Point.Slot,
						"tip_hash", hex.EncodeToString(c.currentTip.Point.Hash),
						"error", err,
					)
				}
				c.manager.mutex.Unlock()
				c.mutex.Unlock()
			}
			return err
		}()
		if err != nil {
			return err
		}
		c.notifyWaitingIterators()
		// Transaction committed successfully; publish all events. This
		// bulk-import path (internal/node/load) does not run under a ledger
		// mutex, so an inline Publish here cannot stall the chainsync drain.
		if c.eventBus != nil {
			for _, evt := range pendingEvents {
				c.eventBus.Publish(ChainUpdateEventType, evt)
			}
		}
		batchOffset += batchSize
	}
	return nil
}

// RawBlock contains pre-extracted block fields for direct storage
// without requiring a full ledger.Block decode.
type RawBlock struct {
	Slot        uint64
	Hash        []byte
	BlockNumber uint64
	Type        uint
	PrevHash    []byte
	Cbor        []byte
}

func (c *Chain) addRawBlockLocked(
	rb RawBlock,
	txn *database.Txn,
	callback func(RawBlock, *database.Txn) error,
) (event.Event, error) {
	// Validate hash fields before any comparisons
	if len(rb.Hash) == 0 {
		return event.Event{}, errors.New(
			"invalid raw block: empty Hash",
		)
	}
	// Validate PrevHash only when tipBlockIndex >= initialBlockIndex. When tipBlockIndex < initialBlockIndex
	// but headers are queued, we may be inserting the genesis/first block which legitimately has no PrevHash.
	// The narrower check ensures we only enforce PrevHash presence once the chain is beyond the initial block.
	if c.tipBlockIndex >= initialBlockIndex &&
		len(rb.PrevHash) == 0 {
		return event.Event{}, errors.New(
			"invalid raw block: empty PrevHash",
		)
	}
	// Check that the new block matches our first header, if any
	if len(c.headers) > 0 {
		firstHeader := c.headers[0]
		if !bytes.Equal(rb.Hash, firstHeader.point.Hash) {
			return event.Event{}, NewBlockNotMatchHeaderError(
				hex.EncodeToString(rb.Hash),
				firstHeader.header.Hash().String(),
			)
		}
	}
	// Check that this block fits on the current chain tip
	if c.tipBlockIndex >= initialBlockIndex {
		if !bytes.Equal(rb.PrevHash, c.currentTip.Point.Hash) {
			return event.Event{}, NewBlockNotFitChainTipError(
				hex.EncodeToString(rb.Hash),
				hex.EncodeToString(rb.PrevHash),
				hex.EncodeToString(c.currentTip.Point.Hash),
			)
		}
	}
	tmpPoint := ocommon.NewPoint(rb.Slot, rb.Hash)
	newBlockIndex := c.tipBlockIndex + 1
	tmpBlock := models.Block{
		ID:       newBlockIndex,
		Slot:     tmpPoint.Slot,
		Hash:     tmpPoint.Hash,
		Number:   rb.BlockNumber,
		Type:     rb.Type,
		PrevHash: rb.PrevHash,
		Cbor:     rb.Cbor,
	}
	if err := c.manager.addBlock(tmpBlock, txn, c.persistent); err != nil {
		return event.Event{}, fmt.Errorf("persisting block: %w", err)
	}
	if callback != nil {
		if err := callback(rb, txn); err != nil {
			return event.Event{}, err
		}
	}
	if !c.persistent {
		c.blocks = append(c.blocks, tmpPoint)
	}
	if len(c.headers) > 0 {
		c.headers = slices.Delete(c.headers, 0, 1)
	}
	c.currentTip = ochainsync.Tip{
		Point:       tmpPoint,
		BlockNumber: rb.BlockNumber,
	}
	c.tipBlockIndex = newBlockIndex
	c.mutationGeneration++
	// Build event for deferred publication (same pattern as
	// addBlockLocked — publish after the transaction commits).
	if c.eventBus != nil {
		return event.NewEvent(
			ChainUpdateEventType,
			ChainBlockEvent{
				Point: tmpPoint,
				Block: tmpBlock,
			},
		), nil
	}
	return event.Event{}, nil
}

// AddRawBlocks adds a batch of pre-extracted blocks to the chain.
func (c *Chain) AddRawBlocks(blocks []RawBlock) error {
	return c.addRawBlocks(blocks, nil)
}

// AddRawBlocksWithCallback adds a batch of pre-extracted blocks to the chain
// and runs the callback in the same transaction after each block is persisted.
// Callers can use this to atomically attach additional blob-side state, such as
// offset indexes, without reopening the immutable DB on resume.
//
// The callback executes with c.mutex and c.manager.mutex locked, inside the
// active blob transaction, and BEFORE c.currentTip / c.tipBlockIndex are
// updated for the just-persisted block. As a result:
//   - The callback must not call back into Chain or ChainManager methods that
//     acquire those same locks (e.g., c.Tip(), c.HeaderTip(),
//     c.BlockByPoint()) — doing so will deadlock.
//   - Tip-state observed via fields read under those locks reflects the
//     pre-update tip, not the block being added.
//
// Error semantics: a callback error aborts the entire current batch, not just
// the offending block. addRawBlocks drives the loop inside txn.Do, which rolls
// back every block persisted by that transaction when the callback returns
// non-nil. Callers should make per-batch decisions idempotent so a retry on a
// later batch does not duplicate effects from a partial earlier attempt.
func (c *Chain) AddRawBlocksWithCallback(
	blocks []RawBlock,
	callback func(RawBlock, *database.Txn) error,
) error {
	return c.addRawBlocks(blocks, callback)
}

// batchRestoreIsSafeLocked reports whether a failed add batch may write its
// pre-batch snapshot back over the current chain state.
//
// It may only do so while the chain still shows exactly what that batch left
// behind. addRawBlocks releases c.mutex and c.manager.mutex when its
// transaction closure returns, before txn.Do commits, so any other goroutine
// can move the chain before the Commit-failure path runs -- and rolling the
// primary chain back while blockfetch appends to it is what every ledger
// recovery rewind does. Writing the snapshot over a rollback's result raises
// tipBlockIndex back above blocks that rollback deleted, leaving the chain
// claiming a tip it does not store: the next rollback measures an inflated
// fork depth against it, and any lookup in the resurrected span reports the
// block as missing.
//
// Callers must hold c.mutex and c.manager.mutex.
func (c *Chain) batchRestoreIsSafeLocked(
	appliedTip ochainsync.Tip,
	appliedTipBlockIndex uint64,
	appliedGeneration uint64,
) bool {
	return c.tipBlockIndex == appliedTipBlockIndex &&
		c.mutationGeneration == appliedGeneration &&
		bytes.Equal(c.currentTip.Point.Hash, appliedTip.Point.Hash)
}

func (c *Chain) addRawBlocks(
	blocks []RawBlock,
	callback func(RawBlock, *database.Txn) error,
) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	batchOffset := 0
	for {
		batchSize := min(blockImportBatchSize, len(blocks)-batchOffset)
		if batchSize == 0 {
			break
		}
		// Collect events inside the transaction callback and
		// publish them only after the transaction commits
		// successfully.
		pendingEvents := make([]event.Event, 0, batchSize)
		// Hold the batch-commit barrier from before this batch mutates the
		// in-memory chain until its transaction has concluded, so a
		// concurrent rollback cannot resolve an index the store has yet to
		// commit. See the batchCommitMutex field.
		err := func() error {
			c.batchCommitMutex.RLock()
			defer c.batchCommitMutex.RUnlock()
			txn := c.manager.db.BlobTxn(true)
			// addRawBlockLocked mutates c.currentTip, c.tipBlockIndex,
			// c.headers, and c.blocks before the txn commits. If a
			// later block in the batch fails the closure restores the
			// pre-batch state, but txn.Do also runs Commit *after* the
			// closure returns and after the chain locks are released —
			// a Commit failure rolls back the persistent state while
			// leaving the in-memory chain advanced. Capture the
			// snapshot here so we can also restore on Commit failure.
			var (
				savedTip             ochainsync.Tip
				savedTipBlockIndex   uint64
				savedGeneration      uint64
				savedHeaders         []queuedHeader
				savedBlocks          []ocommon.Point
				batchApplied         bool
				appliedTip           ochainsync.Tip
				appliedTipBlockIndex uint64
				appliedGeneration    uint64
			)
			err := txn.Do(func(txn *database.Txn) error {
				batch := blocks[batchOffset : batchOffset+batchSize]
				c.mutex.Lock()
				defer c.mutex.Unlock()
				c.manager.mutex.Lock()
				defer c.manager.mutex.Unlock()
				if err := c.reconcile(); err != nil {
					return fmt.Errorf("reconcile: %w", err)
				}
				savedTip = c.currentTip
				savedTipBlockIndex = c.tipBlockIndex
				savedGeneration = c.mutationGeneration
				savedHeaders = slices.Clone(c.headers)
				if !c.persistent {
					savedBlocks = slices.Clone(c.blocks)
				}
				for _, rb := range batch {
					evt, err := c.addRawBlockLocked(
						rb,
						txn,
						callback,
					)
					if err != nil {
						c.currentTip = savedTip
						c.tipBlockIndex = savedTipBlockIndex
						c.mutationGeneration = savedGeneration
						c.headers = savedHeaders
						if !c.persistent {
							c.blocks = savedBlocks
						}
						return err
					}
					if evt.Type != "" {
						pendingEvents = append(
							pendingEvents, evt,
						)
					}
				}
				// Record what this batch leaves behind so the Commit-failure
				// path below can tell its own state from someone else's.
				batchApplied = true
				appliedTip = c.currentTip
				appliedTipBlockIndex = c.tipBlockIndex
				appliedGeneration = c.mutationGeneration
				return nil
			})
			if err != nil {
				// Cover the Commit-failure path: closure returned nil
				// but txn.Do's later Commit failed, so memory still
				// reflects the post-batch tip while the DB rolled
				// back. Re-acquire the locks and restore -- but only
				// while the chain still shows exactly what this batch
				// left behind.
				//
				// The batch-commit barrier remains held while this recovery runs, so
				// another chain mutation cannot move the chain between the failed
				// commit and the snapshot check. A closure-internal error already
				// restored under the chain locks, so there is nothing left to do for
				// it here either.
				if batchApplied {
					c.mutex.Lock()
					c.manager.mutex.Lock()
					if c.batchRestoreIsSafeLocked(
						appliedTip,
						appliedTipBlockIndex,
						appliedGeneration,
					) {
						c.currentTip = savedTip
						c.tipBlockIndex = savedTipBlockIndex
						c.mutationGeneration = savedGeneration
						c.headers = savedHeaders
						if !c.persistent {
							c.blocks = savedBlocks
						}
					} else {
						// Skipping the restore is the correct choice -- writing
						// the snapshot over whatever moved the chain is the
						// divergence this guard exists to prevent -- but it
						// leaves the in-memory chain holding a batch the commit
						// discarded. Record it, so the missing block or inflated
						// fork depth it later surfaces as is attributable to this
						// commit failure instead of appearing without a cause.
						slog.Default().Error(
							"skipped in-memory restore after batch commit failure: chain moved under the batch",
							"component", "chain",
							"chain_id", c.id,
							"applied_tip_block_index", appliedTipBlockIndex,
							"tip_block_index", c.tipBlockIndex,
							"applied_generation", appliedGeneration,
							"mutation_generation", c.mutationGeneration,
							"applied_tip_slot", appliedTip.Point.Slot,
							"applied_tip_hash", hex.EncodeToString(appliedTip.Point.Hash),
							"tip_slot", c.currentTip.Point.Slot,
							"tip_hash", hex.EncodeToString(c.currentTip.Point.Hash),
							"error", err,
						)
					}
					c.manager.mutex.Unlock()
					c.mutex.Unlock()
				}
				return fmt.Errorf("add raw block batch: %w", err)
			}
			return nil
		}()
		if err != nil {
			return err
		}
		c.notifyWaitingIterators()
		// Publish events (only when eventBus is set). Not reached under a
		// ledger mutex, so an inline Publish is safe here.
		if c.eventBus != nil {
			for _, evt := range pendingEvents {
				c.eventBus.Publish(
					ChainUpdateEventType, evt,
				)
			}
		}
		batchOffset += batchSize
	}
	return nil
}

func (c *Chain) notifyWaitingIterators() {
	c.waitingChanMutex.Lock()
	defer c.waitingChanMutex.Unlock()
	if c.waitingChan != nil {
		close(c.waitingChan)
		c.waitingChan = nil
	}
}

func (c *Chain) Rollback(point ocommon.Point) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	pendingEvents, err := c.rollbackLocked(point, false)
	if err != nil {
		return err
	}
	// Publish events after locks are released to prevent deadlocks
	// when subscribers call back into chain/manager state. The
	// mutex-holding ledger rollback path uses RollbackDeferred instead,
	// which returns these events for the ledger to publish after it
	// releases chainsyncMutex.
	if c.eventBus != nil {
		for _, evt := range pendingEvents {
			c.eventBus.Publish(evt.Type, evt)
		}
	}
	return nil
}

// RollbackDeferred rewinds the chain exactly like Rollback but, instead of
// publishing the resulting chain.update / chain.fork events inline, enqueues
// them on the chain-level sequencer under c.mutex and returns them (the return
// value is retained for callers/tests that inspect it; publication happens only
// through the sequencer). The ledger's rollbackChainAndStateDeferred calls this
// while holding chainsyncMutex and then drains the sequencer after the mutex is
// released, so delivery (which can backpressure on a full subscriber buffer)
// never runs under the lock. Publishing inline under chainsyncMutex risks the
// same drain deadlock described on AddBlockWithPointDeferred.
//
// Enqueuing under c.mutex is what keeps this rollback's chain.update correctly
// ordered against a concurrent blockfetch add: the two run under different
// ledger mutexes and once flushed independent per-handler queues, so a rollback
// that mutated the chain after an add could otherwise be published before it.
// The shared sequencer preserves true chain-mutation order across both. See the
// pendingUpdates field and PublishPendingChainUpdates.
func (c *Chain) RollbackDeferred(
	point ocommon.Point,
) ([]event.Event, error) {
	if c == nil {
		return nil, errors.New("chain is nil")
	}
	return c.rollbackLocked(point, true)
}

// rollbackForkDepth returns the number of blocks a rollback to
// rollbackBlockIndex removes from the chain. The rollback point is normally at
// or behind the tip, but it can sit ahead of the tip: rolled-back blocks stay
// resolvable through the manager's block cache with their original (higher)
// block index, and ephemeral fork chains index above the primary chain tip, so
// fork resolution can hand us a rollback point above the current tip. Nothing
// sits between the tip and a point ahead of it, so the fork depth is zero.
// Subtracting directly would wrap around uint64 and make any such rollback look
// deeper than the security parameter K, which rejected and denied every peer
// permanently (issue #3035).
//
// rollbackPointBlock now refuses a point above the tip before either rollback
// entry point reaches this function, so the saturating branch is not exercised
// from Rollback or ValidateRollback any more. It is kept, and unit-tested
// directly by TestRollbackForkDepthSaturates, so the underflow cannot creep
// back in through a future caller.
//
// Callers must hold c.mutex.
func (c *Chain) rollbackForkDepth(
	point ocommon.Point,
	rollbackBlockIndex uint64,
) uint64 {
	if rollbackBlockIndex <= c.tipBlockIndex {
		return c.tipBlockIndex - rollbackBlockIndex
	}
	slog.Default().Warn(
		"rollback point is ahead of chain tip, treating fork depth as zero",
		"rollback_slot", point.Slot,
		"rollback_block_index", rollbackBlockIndex,
		"tip_slot", c.currentTip.Point.Slot,
		"tip_block_index", c.tipBlockIndex,
	)
	return 0
}

// rollbackPointBlock resolves a rollback target to the block this chain must
// truncate to, rejecting any target the chain does not currently hold.
//
// The lookup itself goes through ChainManager.blockByPoint, which answers from
// the retained block cache before the database. That cache deliberately keeps
// blocks the primary chain rolled back so ephemeral fork chains can still
// reconcile against them (see removeBlockByIndex and Chain.reconcile), which
// means an abandoned block stays resolvable by point and still reports the
// block index it used to occupy. Another fork has usually taken that index over
// by the time a peer offers the abandoned point again.
//
// Truncating to that stale index leaves the chain claiming a tip it does not
// store: the block physically at tipBlockIndex belongs to the competing fork,
// while currentTip names the abandoned one. Every block appended afterwards is
// then spliced onto a parent that is absent from the chain, so a spender can
// reach the ledger whose producing block was never applied and cannot be found
// by UtxoByRef, by transaction metadata, or by the backward chain scan. That is
// the non-converging tip-band wedge in issue #3005.
//
// A target whose retained index sits ahead of the tip is refused here too. That
// is the issue #3035/#3040 shape: no chain block occupies the index, so obeying
// it raised tipBlockIndex above the last block the chain actually stores and
// left currentTip naming an absent block, punching a hole that chain iteration
// stops at. It must be refused as not-on-chain rather than as an over-K
// rollback: #3035 was a node permanently denying every peer because that case
// was misclassified as exceeding the security parameter, whereas a not-found
// rollback makes callers re-intersect and recover. rollbackForkDepth keeps its
// saturating arithmetic so no future caller can reintroduce the uint64
// underflow that caused the misclassification.
//
// Callers must hold c.mutex and c.manager.mutex.
// checkEphemeralBufferSpan verifies that a fork's in-memory buffer holds an
// entry for every block it claims above its fork point. rollbackLocked indexes
// that buffer per rolled-back block, so a short buffer would otherwise surface
// as an out-of-range offset part-way through the deletion loop, after blocks
// had already been removed. Checking once up front keeps the rollback
// all-or-nothing. blockByIndexLocked applies the same bounds per lookup.
//
// Reaching the error means this chain's tip index and buffer already disagree,
// which no public call path produces; it is a corruption report, not a
// rejection of the caller's rollback point.
func (c *Chain) checkEphemeralBufferSpan() error {
	if c.persistent || c.tipBlockIndex <= c.lastCommonBlockIndex {
		return nil
	}
	want := c.tipBlockIndex - c.lastCommonBlockIndex
	if want <= uint64(len(c.blocks)) { //nolint:gosec
		return nil
	}
	return fmt.Errorf(
		"%w: %d blocks above fork point %d, buffer holds %d",
		ErrRollbackBeyondEphemeralChain,
		want,
		c.lastCommonBlockIndex,
		len(c.blocks),
	)
}

func (c *Chain) rollbackPointBlock(
	point ocommon.Point,
) (models.Block, error) {
	tmpBlock, err := c.manager.blockByPoint(point, nil)
	if err != nil {
		return models.Block{}, fmt.Errorf("lookup rollback point: %w", err)
	}
	if c.holdsBlockAtIndexLocked(tmpBlock.ID, tmpBlock.Hash) {
		return tmpBlock, nil
	}
	occupantHash := []byte(nil)
	if occupant, occErr := c.blockByIndexLocked(tmpBlock.ID); occErr == nil {
		occupantHash = occupant.Hash
	}
	c.manager.recordRollbackPointNotOnChain()
	slog.Default().Error(
		"cross-fork splice prevented: rejecting rollback to a point this chain no longer holds",
		"component", "chain",
		"chain_id", c.id,
		"rollback_slot", point.Slot,
		"rollback_hash", hex.EncodeToString(point.Hash),
		"retained_block_index", tmpBlock.ID,
		"block_hash_at_index", hex.EncodeToString(occupantHash),
		"tip_block_index", c.tipBlockIndex,
		"tip_slot", c.currentTip.Point.Slot,
		"tip_hash", hex.EncodeToString(c.currentTip.Point.Hash),
	)
	return models.Block{}, fmt.Errorf(
		"%w: slot %d hash %s resolved to block index %d, which this chain no longer holds",
		ErrRollbackPointNotOnChain,
		point.Slot,
		hex.EncodeToString(point.Hash),
		tmpBlock.ID,
	)
}

// findQueuedHeader scans queued headers backward for the rollback point
// without mutating them, and returns one of three outcomes:
//   - (index, nil) if a queued header matches point exactly.
//   - (-1, nil) if every queued header is strictly ahead of point, or if
//     point's slot matches the oldest queued header's under a different
//     hash — either way point is not a queued header, so rollback falls
//     through to the block-committed chain.
//   - (-1, models.ErrBlockNotFound) if point falls strictly between two
//     queued headers, or beyond the newest one without matching it — a
//     target that is not a valid rollback point.
//
// Callers must hold c.mutex.
func (c *Chain) findQueuedHeader(point ocommon.Point) (int, error) {
	for i, header := range slices.Backward(c.headers) {
		if header.point.Slot > point.Slot {
			continue
		}
		if header.point.Slot == point.Slot &&
			bytes.Equal(header.point.Hash, point.Hash) {
			return i, nil
		}
		if header.point.Slot < point.Slot {
			return -1, models.ErrBlockNotFound
		}
	}
	return -1, nil
}

// ValidateRollback verifies that Rollback(point) would be accepted without
// mutating chain state. Callers can use this to avoid applying external
// side effects before the chain's rollback pre-checks have run.
func (c *Chain) ValidateRollback(point ocommon.Point) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return fmt.Errorf("reconcile chain: %w", err)
	}
	if c.persistent && c.manager.securityParam <= 0 {
		return ErrSecurityParamNotConfigured
	}
	// Check headers for rollback point without mutating them
	if len(c.headers) > 0 {
		idx, err := c.findQueuedHeader(point)
		if err != nil {
			return err
		}
		if idx >= 0 {
			return nil
		}
	}
	// Lookup block for rollback point
	var rollbackBlockIndex uint64
	if point.Slot > 0 {
		tmpBlock, err := c.rollbackPointBlock(point)
		if err != nil {
			return err
		}
		rollbackBlockIndex = tmpBlock.ID
	}
	// Calculate fork depth before deleting blocks
	forkDepth := c.rollbackForkDepth(point, rollbackBlockIndex)
	// Reject rollbacks that exceed the security parameter K on
	// the persistent chain. Ephemeral (fork-tracking) chains are
	// not subject to this limit. When the chain is shorter than K
	// blocks (initial sync), the entire chain can be safely
	// replaced during sync.
	securityParam := c.manager.securityParam
	if c.persistent &&
		c.tipBlockIndex >= uint64(securityParam) && //nolint:gosec
		forkDepth > uint64(securityParam) { //nolint:gosec
		slog.Default().Warn(
			"rejecting rollback that exceeds "+
				"security parameter K",
			"fork_depth", forkDepth,
			"security_param", securityParam,
			"rollback_slot", point.Slot,
		)
		return ErrRollbackExceedsSecurityParam
	}
	return nil
}

// rollbackLocked performs all rollback logic under locks and returns
// events to be published by the caller after locks are released.
func (c *Chain) rollbackLocked(
	point ocommon.Point,
	deferred bool,
) ([]event.Event, error) {
	// Wait for any chain-owned batch transaction that has already applied to
	// the in-memory chain to conclude, so the removal loop below cannot ask
	// the store for an index whose write has not committed yet. See the
	// batchCommitMutex field.
	c.batchCommitMutex.Lock()
	defer c.batchCommitMutex.Unlock()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// We get a write lock on the manager to cover the integrity checks and block deletions
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return nil, fmt.Errorf("reconcile chain: %w", err)
	}
	if c.persistent && c.manager.securityParam <= 0 {
		return nil, ErrSecurityParamNotConfigured
	}
	// Check headers for rollback point. The scan itself does not mutate
	// c.headers, so a not-found error leaves the queue untouched; headers
	// are only deleted once we know the rollback will actually apply.
	if len(c.headers) > 0 {
		idx, err := c.findQueuedHeader(point)
		if err != nil {
			return nil, err
		}
		if idx >= 0 {
			// Rollback point is a queued header. Drop only the headers
			// after it and leave the matched header itself queued.
			c.headers = slices.Delete(c.headers, idx+1, len(c.headers))
			return nil, nil
		}
	}
	// Lookup block for rollback point
	var rollbackBlockIndex uint64
	var tmpBlock models.Block
	if point.Slot > 0 {
		var err error
		tmpBlock, err = c.rollbackPointBlock(point)
		if err != nil {
			return nil, err
		}
		rollbackBlockIndex = tmpBlock.ID
	}
	// Calculate fork depth before deleting blocks
	forkDepth := c.rollbackForkDepth(point, rollbackBlockIndex)
	// Reject rollbacks that exceed the security parameter K on
	// the persistent chain. Ephemeral (fork-tracking) chains are
	// not subject to this limit. When the chain is shorter than K
	// blocks (initial sync), the entire chain can be safely
	// replaced during sync.
	securityParam := c.manager.securityParam
	if c.persistent &&
		c.tipBlockIndex >= uint64(securityParam) && //nolint:gosec
		forkDepth > uint64(securityParam) { //nolint:gosec
		slog.Default().Warn(
			"rejecting rollback that exceeds "+
				"security parameter K",
			"fork_depth", forkDepth,
			"security_param", securityParam,
			"rollback_slot", point.Slot,
		)
		return nil, ErrRollbackExceedsSecurityParam
	}
	// Validate the in-memory buffer before deleting anything, so a
	// corrupt span fails the rollback whole rather than part-way through.
	if err := c.checkEphemeralBufferSpan(); err != nil {
		return nil, err
	}
	// Capture old tip for fork event before we modify it
	oldTip := c.currentTip
	// Collect and delete rolled-back blocks in a single pass
	var rolledBackBlocks []models.Block
	for i := c.tipBlockIndex; i > rollbackBlockIndex; i-- {
		if c.persistent {
			// Remove block from persistent store, returns the removed block
			block, err := c.manager.removeBlockByIndex(i)
			if err != nil {
				return nil, fmt.Errorf(
					"remove block at index %d: %w", i, err,
				)
			}
			if c.eventBus != nil {
				rolledBackBlocks = append(rolledBackBlocks, block)
			}
		} else {
			// Collect block for event emission before deletion
			if c.eventBus != nil {
				block, err := c.blockByIndexLocked(i)
				if err != nil {
					slog.Default().Warn(
						"failed to get block for rollback event",
						"index", i,
						"error", err,
					)
				} else {
					rolledBackBlocks = append(rolledBackBlocks, block)
				}
			}
			// Blocks at or below the fork point belong to the
			// common prefix held by the primary chain, not to this
			// fork's in-memory buffer, so there is nothing to delete
			// here. blockByIndexLocked draws the same boundary with
			// <=; using < placed the fork point itself on the buffer
			// side, where its index computes to -1.
			if i <= c.lastCommonBlockIndex {
				continue
			}
			// Remove from memory buffer
			memBlockIndex := int(i - c.lastCommonBlockIndex - initialBlockIndex) //nolint:gosec
			c.blocks = slices.Delete(
				c.blocks,
				memBlockIndex,
				memBlockIndex+1,
			)
		}
	}
	// A rollback past the fork point shortens the prefix this chain
	// shares with the primary: every block it still holds is now common.
	// Re-anchor here, after the loop has finished reading the old value
	// for buffer offsets, so lastCommonBlockIndex never exceeds the tip.
	if !c.persistent && rollbackBlockIndex < c.lastCommonBlockIndex {
		c.lastCommonBlockIndex = rollbackBlockIndex
	}
	// Clear out any headers
	c.headers = slices.Delete(c.headers, 0, len(c.headers))
	// Update tip
	c.currentTip = ochainsync.Tip{
		Point:       point,
		BlockNumber: tmpBlock.Number,
	}
	c.tipBlockIndex = rollbackBlockIndex
	c.mutationGeneration++
	// Update iterators for rollback
	for _, iter := range c.iterators {
		// Reverse iterators never deliver rollback markers, but if a
		// rollback shortened the chain past nextBlockIndex the
		// iterator must be clamped to the new tip so subsequent Next
		// calls return the still-present predecessor blocks instead
		// of mistaking missing blocks for origin. Clamping also
		// applies to rollback-to-origin (rollbackBlockIndex == 0,
		// the pre-genesis index — initialBlockIndex is 1): without
		// it, a regrown chain reaching the iterator's stale index
		// would silently hand out unrelated blocks.
		if iter.reverse {
			if iter.nextBlockIndex > rollbackBlockIndex {
				iter.nextBlockIndex = rollbackBlockIndex
			}
			continue
		}
		// Use startPoint for iterators that haven't delivered any blocks
		// yet (lastPoint is zero-value). Without this, newly created
		// iterators miss rollback signals entirely.
		refSlot := iter.lastPoint.Slot
		if refSlot == 0 && len(iter.lastPoint.Hash) == 0 {
			refSlot = iter.startPoint.Slot
		}
		if refSlot > point.Slot {
			// Don't update rollback point if the iterator already has an older one pending
			if iter.needsRollback && point.Slot > iter.rollbackPoint.Slot {
				continue
			}
			iter.rollbackPoint = point
			iter.needsRollback = true
		}
	}
	// Wake any iterators that are blocked waiting for new blocks so
	// they can process the rollback signal promptly.
	c.notifyWaitingIterators()
	// Build events for caller to publish after locks are released
	var pendingEvents []event.Event
	if len(rolledBackBlocks) > 0 {
		// Rollback event - only emit when blocks were actually removed
		pendingEvents = append(
			pendingEvents,
			event.NewEvent(
				ChainUpdateEventType,
				ChainRollbackEvent{
					Point:            point,
					RolledBackBlocks: rolledBackBlocks,
				},
			),
		)
		// Fork event - only emit if we actually rolled back blocks
		if forkDepth > 0 {
			pendingEvents = append(
				pendingEvents,
				event.NewEvent(
					ChainForkEventType,
					ChainForkEvent{
						ForkPoint:     point,
						ForkDepth:     forkDepth,
						AlternateHead: oldTip.Point,
						CanonicalHead: point,
					},
				),
			)
		}
	}
	// Deferred callers (the mutex-holding chainsync rollback path) publish
	// through the chain-level sequencer rather than inline. Enqueue while
	// c.mutex is still held so this rollback is sequenced against concurrent
	// blockfetch adds in true mutation order -- the rollback's chain.update
	// then cannot be published ahead of an add that mutated the chain before
	// it, or behind one that mutated after. The caller drains after releasing
	// chainsyncMutex. See the pendingUpdates field and
	// PublishPendingChainUpdates.
	if deferred {
		for _, evt := range pendingEvents {
			c.queueDeferredEventLocked(evt)
		}
	}
	return pendingEvents, nil
}

// ClearHeaders removes all queued block headers. This is used when
// the active peer changes and stale headers from the previous peer's
// chainsync session no longer fit the current chain tip.
func (c *Chain) ClearHeaders() {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.headers = c.headers[:0]
}

// RecentPoints returns up to count recent chain points in descending
// order (most recent first) using the in-memory chain state. This
// includes the current tip and, for non-persistent chains, any blocks
// stored in the in-memory buffer. For persistent chains, it walks
// backwards through the database using block indices.
//
// This method is useful for building intersection point lists that
// remain accurate even when the blob store has not yet been fully
// flushed, since the chain's in-memory tip is always up-to-date.
func (c *Chain) RecentPoints(count int) []ocommon.Point {
	if c == nil || count <= 0 {
		return nil
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	unlockBlockIndexReadLocks := c.lockBlockIndexReadLocks()
	defer unlockBlockIndexReadLocks()
	// If the chain has no blocks yet, return nothing
	if c.tipBlockIndex < initialBlockIndex {
		return nil
	}
	var points []ocommon.Point
	// Always include the current tip
	tip := c.currentTip.Point
	if tip.Slot > 0 || len(tip.Hash) > 0 {
		points = append(points, tip)
	}
	if len(points) >= count {
		return points[:count]
	}
	// Walk backwards through block indices to gather more points
	for idx := c.tipBlockIndex - 1; idx >= initialBlockIndex && len(points) < count; idx-- {
		blk, err := c.blockByIndexLocked(idx)
		if err != nil {
			break
		}
		points = append(
			points,
			ocommon.NewPoint(blk.Slot, blk.Hash),
		)
	}
	return points
}

// PointAtDepth returns the point depth blocks behind the current tip. A depth
// of zero returns the tip. When depth reaches beyond the retained chain, the
// immutable point is origin and found is false.
//
// Unlike RecentPoints, this performs one indexed lookup regardless of depth,
// which is important for consensus reads at the security-parameter boundary.
func (c *Chain) PointAtDepth(
	depth uint64,
) (point ocommon.Point, found bool, err error) {
	if c == nil {
		return ocommon.Point{}, false, errors.New("chain is nil")
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.tipBlockIndex < initialBlockIndex || depth >= c.tipBlockIndex {
		return ocommon.Point{}, false, nil
	}
	if depth == 0 {
		return ocommon.NewPoint(
			c.currentTip.Point.Slot,
			c.currentTip.Point.Hash,
		), true, nil
	}
	unlocks := c.lockBlockIndexReadLocks()
	defer unlocks()
	block, err := c.blockByIndexLocked(c.tipBlockIndex - depth)
	if err != nil {
		return ocommon.Point{}, false, err
	}
	return ocommon.NewPoint(block.Slot, block.Hash), true, nil
}

// IntersectPoints returns up to count points in descending order for
// chainsync FindIntersect. It keeps a dense window near the tip and
// then samples exponentially older blocks so lagging peers can still
// find a recent common point without falling all the way back to origin.
func (c *Chain) IntersectPoints(count int) []ocommon.Point {
	if c == nil || count <= 0 {
		return nil
	}
	// Read K before taking any chain lock: SetLedger can run while the node
	// is serving chainsync, and the accessor takes the manager lock, which
	// must not be acquired underneath the block-index read locks below.
	securityRung := uint64(0)
	if k := c.manager.SecurityParam(); k > 0 {
		securityRung = uint64(k) //nolint:gosec // guarded positive
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	unlockBlockIndexReadLocks := c.lockBlockIndexReadLocks()
	defer unlockBlockIndexReadLocks()
	if c.tipBlockIndex < initialBlockIndex {
		return nil
	}
	points := make([]ocommon.Point, 0, count)
	seen := make(map[string]struct{}, count)
	appendPoint := func(point ocommon.Point) {
		if len(points) >= count {
			return
		}
		key := fmt.Sprintf("%d:%x", point.Slot, point.Hash)
		if _, ok := seen[key]; ok {
			return
		}
		points = append(points, point)
		seen[key] = struct{}{}
	}
	appendBlockPoint := func(blockIndex uint64) {
		if len(points) >= count {
			return
		}
		blk, err := c.blockByIndexLocked(blockIndex)
		if err != nil {
			return
		}
		appendPoint(ocommon.NewPoint(blk.Slot, blk.Hash))
	}
	denseStartIndex := c.tipBlockIndex
	tip := c.currentTip.Point
	if tip.Slot > 0 || len(tip.Hash) > 0 {
		appendPoint(tip)
		if denseStartIndex > initialBlockIndex {
			denseStartIndex--
		} else {
			denseStartIndex = 0
		}
	}
	denseCount := min(count, intersectDensePointCount)
	for idx := denseStartIndex; idx >= initialBlockIndex && len(points) < denseCount; idx-- {
		appendBlockPoint(idx)
		if idx == initialBlockIndex {
			break
		}
	}
	if len(points) >= count {
		return points
	}
	if c.tipBlockIndex <= initialBlockIndex {
		return points
	}
	// Beyond the dense band the ladder doubles, so a peer L blocks behind
	// resolves its FindIntersect to the next rung at or past L. Once that
	// rung passes the security parameter K, the rollback the peer asks for
	// is refused as an over-K fork even though the peer's chain is a strict
	// prefix of ours: at K=108 the first rung past K is 128, so a peer only
	// 65 blocks behind is already refused. Offering the rung at exactly K
	// closes that band — with a point at offset K, every peer within K of
	// our tip holds it, so the shallowest rung it can match is at most K
	// deep and stays crossable. It costs exactly one extra point, so the
	// FindIntersect message stays the same size in practice (the list is
	// capped by count).
	//
	for offset := uint64(denseCount); len(points) < count; offset *= 2 { //nolint:gosec // denseCount is bounded to non-negative values
		if offset == 0 || offset >= c.tipBlockIndex {
			break
		}
		// Keep the list descending: emit the K rung before the first
		// doubling rung that would overshoot it.
		if securityRung > 0 && securityRung < offset {
			if securityRung < c.tipBlockIndex {
				appendBlockPoint(c.tipBlockIndex - securityRung)
			}
			securityRung = 0
			if len(points) >= count {
				break
			}
		}
		appendBlockPoint(c.tipBlockIndex - offset)
	}
	// The doubling loop can stop before reaching the K rung when the chain
	// is shorter than the next rung. Every offset it emitted is then at or
	// below K (a smaller K would have been emitted inside the loop), so
	// appending it here keeps the list descending.
	if securityRung > 0 && securityRung < c.tipBlockIndex &&
		len(points) < count {
		appendBlockPoint(c.tipBlockIndex - securityRung)
	}
	if len(points) < count {
		appendBlockPoint(initialBlockIndex)
	}
	return points
}

func (c *Chain) HeaderCount() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.headers)
}

func (c *Chain) FirstHeaderMatchesPoint(point ocommon.Point) bool {
	return c.firstHeaderMatchesPoint(point, false)
}

func (c *Chain) FirstVerifiedHeaderMatchesPoint(point ocommon.Point) bool {
	return c.firstHeaderMatchesPoint(point, true)
}

func (c *Chain) firstHeaderMatchesPoint(
	point ocommon.Point,
	requireCryptoVerified bool,
) bool {
	if c == nil {
		return false
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if len(c.headers) == 0 {
		return false
	}
	header := c.headers[0]
	if header.point.Slot != point.Slot {
		return false
	}
	if !bytes.Equal(header.point.Hash, point.Hash) {
		return false
	}
	return !requireCryptoVerified || header.cryptoVerified
}

func (c *Chain) HeaderRange(count int) (ocommon.Point, ocommon.Point) {
	if c == nil || count <= 0 {
		return ocommon.Point{}, ocommon.Point{}
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	var startPoint, endPoint ocommon.Point
	if len(c.headers) > 0 {
		firstHeader := c.headers[0]
		startPoint = firstHeader.point
		lastHeaderIdx := min(count, len(c.headers)) - 1
		lastHeader := c.headers[lastHeaderIdx]
		endPoint = lastHeader.point
	}
	return startPoint, endPoint
}

// FromPoint returns a ChainIterator starting at the specified point. If inclusive is true, the iterator
// will start at the specified point. Otherwise it will start at the point following the specified point
func (c *Chain) FromPoint(
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	return c.FromPointContext(context.Background(), point, inclusive)
}

// FromPointContext returns a ChainIterator that inherits cancellation from ctx.
func (c *Chain) FromPointContext(
	ctx context.Context,
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	if c == nil {
		return nil, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	iter, err := newChainIteratorWithContext(
		ctx,
		c,
		point,
		inclusive,
		false,
	)
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators, iter)
	iter.startCancelWatcher()
	return iter, nil
}

// FromPointReverse returns a ChainIterator that walks backward from the
// specified point toward chain origin. If inclusive is true the iterator
// yields the start point first; otherwise it yields the block preceding it.
// Blocking Next calls on a reverse iterator do not wait for new blocks; once
// origin is reached, Next returns ErrIteratorChainOrigin.
func (c *Chain) FromPointReverse(
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	return c.FromPointReverseContext(context.Background(), point, inclusive)
}

// FromPointReverseContext returns a reverse ChainIterator that inherits cancellation from ctx.
func (c *Chain) FromPointReverseContext(
	ctx context.Context,
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	if c == nil {
		return nil, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	iter, err := newChainIteratorWithContext(
		ctx,
		c,
		point,
		inclusive,
		true,
	)
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators, iter)
	iter.startCancelWatcher()
	return iter, nil
}

// removeIterator removes an iterator from the chain's iterator list.
// This is called when an iterator is cancelled to prevent memory leaks.
func (c *Chain) removeIterator(iter *ChainIterator) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, it := range c.iterators {
		if it == iter {
			c.iterators = slices.Delete(c.iterators, i, i+1)
			return
		}
	}
}

func (c *Chain) BlockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (models.Block, error) {
	return c.manager.BlockByPoint(point, txn)
}

// BlockBeforeSlot returns the highest-slot block before slotNumber on this
// chain. It walks the chain index instead of scanning blob keys so retained
// fork or synthetic blobs cannot be returned as canonical blocks.
func (c *Chain) BlockBeforeSlot(slotNumber uint64) (models.Block, error) {
	if c == nil {
		return models.Block{}, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	unlockBlockIndexReadLocks := c.lockBlockIndexReadLocks()
	defer unlockBlockIndexReadLocks()
	if err := c.reconcile(); err != nil {
		return models.Block{}, err
	}
	if c.tipBlockIndex < initialBlockIndex {
		return models.Block{}, models.ErrBlockNotFound
	}
	// Block slots are strictly increasing with block index on the canonical
	// chain, so binary-search for the highest index whose slot is below
	// slotNumber rather than walking backward from the tip. The old linear walk
	// cost O(tip - boundary) block reads; during catch-up the header chain runs
	// far ahead of the ledger tip, so a boundary near the ledger tip made every
	// lookup scan the entire header-ahead gap (the epoch-lab-nonce heal ran this
	// per recent epoch, wedging large-DB startup for minutes — #2771). The
	// search still resolves each candidate via blockByIndex (the active chain),
	// so retained fork or synthetic blobs are never returned.
	lo, hi := initialBlockIndex, c.tipBlockIndex
	var (
		result models.Block
		found  bool
	)
	for lo <= hi {
		mid := lo + (hi-lo)/2
		block, err := c.blockByIndexLocked(mid)
		if err != nil {
			return models.Block{}, err
		}
		if block.Slot < slotNumber {
			result = block
			found = true
			lo = mid + 1
		} else {
			if mid == initialBlockIndex {
				break
			}
			hi = mid - 1
		}
	}
	if !found {
		return models.Block{}, models.ErrBlockNotFound
	}
	return result, nil
}

// holdsBlockAtIndex reports whether this chain currently has the block with
// the given hash at the given index. It distinguishes a point that is still
// part of the chain from one that merely remains resolvable through the
// manager's retained-block cache after a rollback, since blockByPoint answers
// from that cache. Callers must hold c.mutex.
func (c *Chain) holdsBlockAtIndex(blockIndex uint64, blockHash []byte) bool {
	if blockIndex < initialBlockIndex || blockIndex > c.tipBlockIndex {
		return false
	}
	if c.manager == nil {
		return false
	}
	unlockBlockIndexReadLocks := c.lockBlockIndexReadLocks()
	defer unlockBlockIndexReadLocks()
	return c.holdsBlockAtIndexLocked(blockIndex, blockHash)
}

// holdsBlockAtIndexLocked is the lock-preserving form used by rollback paths
// that already hold c.mutex and c.manager.mutex.
func (c *Chain) holdsBlockAtIndexLocked(
	blockIndex uint64,
	blockHash []byte,
) bool {
	if blockIndex < initialBlockIndex || blockIndex > c.tipBlockIndex {
		return false
	}
	lookupChain := c
	if !c.persistent && blockIndex <= c.lastCommonBlockIndex {
		// Ephemeral chains keep their common prefix on the primary chain.
		// In-memory managers do not have an index-backed manager lookup, but
		// the primary chain can resolve that prefix through its own in-memory
		// points and the manager's block cache. Use the primary chain here so
		// a valid common point is not mistaken for a rolled-back point.
		// The primary pointer is immutable after manager initialization and is
		// the chain whose active index owns the common prefix.
		lookupChain = c.manager.primary
		if lookupChain == nil {
			return false
		}
	}
	tmpBlock, err := lookupChain.blockByIndexLocked(blockIndex)
	if err != nil {
		return false
	}
	return bytes.Equal(tmpBlock.Hash, blockHash)
}

// lockBlockIndexReadLocks acquires the locks required by a chain-index read.
// The caller must already hold c.mutex and must not hold manager.mutex. The
// primary pointer is immutable after manager initialization, so it can be
// read before taking the primary lock. This establishes the chain -> primary
// -> manager order used by chain creation and avoids a lock cycle with a
// creator that already holds the primary-chain write lock.
func (c *Chain) lockBlockIndexReadLocks() func() {
	primaryChain := c.manager.primary
	primaryLocked := primaryChain != nil &&
		primaryChain != c &&
		!primaryChain.persistent
	if primaryLocked {
		primaryChain.mutex.RLock()
	}
	c.manager.mutex.RLock()
	return func() {
		c.manager.mutex.RUnlock()
		if primaryLocked {
			primaryChain.mutex.RUnlock()
		}
	}
}

// blockByIndexLocked resolves a block from this chain's active index. The
// caller must hold c.mutex and c.manager.mutex; in-memory common-prefix reads
// additionally hold the primary-chain read lock when c is a fork.
func (c *Chain) blockByIndexLocked(
	blockIndex uint64,
) (models.Block, error) {
	if c.persistent || blockIndex <= c.lastCommonBlockIndex {
		// Query via manager for common blocks
		tmpBlock, err := c.manager.blockByIndexLocked(blockIndex, nil)
		if err != nil {
			return models.Block{}, err
		}
		return tmpBlock, nil
	}
	// Get from memory buffer
	//nolint:gosec
	memBlockIndex := int(
		blockIndex - c.lastCommonBlockIndex - initialBlockIndex,
	)
	if memBlockIndex < 0 || len(c.blocks) < memBlockIndex+1 {
		return models.Block{}, models.ErrBlockNotFound
	}
	memBlockPoint := c.blocks[memBlockIndex]
	tmpBlock, err := c.manager.blockByPoint(memBlockPoint, nil)
	if err != nil {
		return models.Block{}, err
	}
	return tmpBlock, nil
}

func chainIteratorPreviousPoint(iter *ChainIterator) ocommon.Point {
	if iter.lastPoint.Slot > 0 || len(iter.lastPoint.Hash) > 0 {
		return iter.lastPoint
	}
	return iter.startPoint
}

func blockFollowsPoint(block models.Block, point ocommon.Point) bool {
	if point.Slot == 0 && len(point.Hash) == 0 {
		return len(block.PrevHash) == 0
	}
	return bytes.Equal(block.PrevHash, point.Hash)
}

func (c *Chain) nextPersistentBlockAfterSparseIndex(
	iter *ChainIterator,
) (models.Block, bool, error) {
	if !c.persistent || iter.reverse ||
		iter.nextBlockIndex > c.tipBlockIndex {
		return models.Block{}, false, nil
	}
	previousPoint := chainIteratorPreviousPoint(iter)
	block, err := c.manager.blockAtOrAfterIndex(
		iter.nextBlockIndex+1,
		nil,
	)
	if errors.Is(err, models.ErrBlockNotFound) {
		return models.Block{}, false, fmt.Errorf(
			"persistent chain tip index %d is ahead of missing iterator index %d, but no later indexed block was found",
			c.tipBlockIndex,
			iter.nextBlockIndex,
		)
	}
	if err != nil {
		return models.Block{}, false, err
	}
	if blockFollowsPoint(block, previousPoint) {
		return block, true, nil
	}
	return models.Block{}, false, fmt.Errorf(
		"persistent chain index gap after index %d: block %d/%s at index %d has prev hash %s, expected %s",
		iter.nextBlockIndex,
		block.Slot,
		hex.EncodeToString(block.Hash),
		block.ID,
		hex.EncodeToString(block.PrevHash),
		hex.EncodeToString(previousPoint.Hash),
	)
}

func (c *Chain) iterNext(
	iter *ChainIterator,
	blocking bool,
) (*ChainIteratorResult, error) {
	for {
		c.mutex.Lock()
		// We get a read lock on the manager for the integrity check and initial block lookup
		c.manager.mutex.RLock()
		// Verify chain integrity
		if err := c.reconcile(); err != nil {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, err
		}
		// Check for pending rollback (forward iterators only; reverse
		// iterators never have rollbacks queued).
		if iter.needsRollback {
			ret := &ChainIteratorResult{}
			ret.Point = iter.rollbackPoint
			ret.Rollback = true
			iter.lastPoint = iter.rollbackPoint
			iter.needsRollback = false
			if iter.rollbackPoint.Slot > 0 {
				// Lookup block index for rollback point
				tmpBlock, err := c.manager.blockByPoint(
					iter.rollbackPoint,
					nil,
				)
				if err != nil {
					c.mutex.Unlock()
					c.manager.mutex.RUnlock()
					return nil, err
				}
				iter.nextBlockIndex = tmpBlock.ID + 1
			} else {
				// Rolling back to origin: reset to the first
				// block index so the iterator delivers all
				// blocks from genesis.
				iter.nextBlockIndex = initialBlockIndex
			}
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return ret, nil
		}
		// Reverse iterators terminate when they walk past origin.
		// nextBlockIndex == 0 is the sentinel set by newChainIterator
		// for "no more blocks available behind this point".
		if iter.reverse && iter.nextBlockIndex < initialBlockIndex {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, ErrIteratorChainOrigin
		}
		ret := &ChainIteratorResult{}
		// Lookup next block in metadata DB
		tmpBlock, err := c.blockByIndexLocked(iter.nextBlockIndex)
		if errors.Is(err, models.ErrBlockNotFound) && !iter.reverse {
			recoveredBlock, recovered, recoverErr := c.nextPersistentBlockAfterSparseIndex(
				iter,
			)
			if recoverErr != nil {
				err = recoverErr
			} else if recovered {
				tmpBlock = recoveredBlock
				iter.nextBlockIndex = tmpBlock.ID
				err = nil
			}
		}
		// Return immedidately if a block is found
		if err == nil {
			ret.Point = ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash)
			ret.Block = tmpBlock
			if iter.reverse {
				if iter.nextBlockIndex == initialBlockIndex {
					// Just delivered the genesis block; mark
					// the iterator as past origin so the next
					// call returns ErrIteratorChainOrigin.
					iter.nextBlockIndex = 0
				} else {
					iter.nextBlockIndex--
				}
			} else {
				iter.nextBlockIndex++
			}
			iter.lastPoint = ret.Point
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return ret, nil
		}
		// Return any actual error
		if !errors.Is(err, models.ErrBlockNotFound) {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return ret, err
		}
		// Reverse iterators never wait — origin does not grow.
		if iter.reverse {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, ErrIteratorChainOrigin
		}
		// Return immediately if we're not blocking
		if !blocking {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, ErrIteratorChainTip
		}
		// Register the wait channel before releasing c.mutex. Otherwise
		// a concurrent AddBlock can commit and notify in the gap between
		// the at-tip check above and this iterator joining the waiter set,
		// stranding ChainSync peers until a later chain event.
		c.waitingChanMutex.Lock()
		if c.waitingChan == nil {
			c.waitingChan = make(chan struct{})
		}
		waitChan := c.waitingChan
		c.waitingChanMutex.Unlock()
		c.mutex.Unlock()
		c.manager.mutex.RUnlock()

		select {
		case <-waitChan:
			// Loop again now that we should have new data
			continue
		case <-iter.ctx.Done():
			// Iterator was cancelled
			return nil, iter.ctx.Err()
		}
	}
}

// NotifyIterators wakes all blocked iterators waiting for new blocks.
// Call this after a DB transaction that adds blocks has been committed
// to ensure iterators see the newly visible data.
func (c *Chain) NotifyIterators() {
	c.notifyWaitingIterators()
}

func (c *Chain) reconcile() error {
	// We reconcile against the primary/persistent chain, so no need to check if we are that chain
	if c.persistent {
		return nil
	}
	// Check with manager if there have been any primary chain rollback events that would trigger a reconcile
	if !c.manager.chainNeedsReconcile(c.id, c.lastCommonBlockIndex) {
		return nil
	}
	if c.manager.securityParam <= 0 {
		return ErrSecurityParamNotConfigured
	}
	securityParam := c.manager.securityParam
	// Check our blocks against primary chain until we find a match
	primaryChain := c.manager.primaryChainLocked()
	if primaryChain == nil {
		return models.ErrBlockNotFound
	}
	blockIndex := c.tipBlockIndex
	for i, v := range slices.Backward(c.blocks) {
		tmpBlock, err := primaryChain.blockByIndexLocked(blockIndex)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			return err
		}
		if err == nil &&
			v.Slot == tmpBlock.Slot &&
			bytes.Equal(v.Hash, tmpBlock.Hash) {
			// Adjust our chain-local blocks and offset point from primary chain
			c.blocks = slices.Delete(c.blocks, 0, i+1)
			c.lastCommonBlockIndex = tmpBlock.ID
			return nil
		}
		if blockIndex == 0 {
			break
		}
		blockIndex--
	}
	// Determine prev-hash from earliest known good block
	knownPoint := c.currentTip.Point
	// Iterate backward through chain based on prev-hash until we find a matching block on the primary chain
	// Accumulate blocks locally to avoid O(K²) prepending
	newBlocks := make([]ocommon.Point, 0, securityParam)
	if len(c.blocks) > 0 {
		knownPoint = c.blocks[0]
	} else {
		// No in-memory blocks: the chain's current tip is itself the
		// earliest known good block. Seed newBlocks so the tip is
		// preserved after we re-anchor lastCommonBlockIndex against
		// the primary; otherwise iteration past the new common point
		// would silently truncate at it.
		newBlocks = append(newBlocks, knownPoint)
	}
	knownBlock, err := c.manager.blockByPoint(knownPoint, nil)
	if err != nil {
		return err
	}
	decodedKnownBlock, err := knownBlock.Decode()
	if err != nil {
		return err
	}
	lastPrevHash := decodedKnownBlock.PrevHash().Bytes()
	iterationCount := 0
	for {
		if iterationCount >= securityParam {
			return models.ErrBlockNotFound
		}
		iterationCount++
		tmpBlock, err := c.manager.blockByHash(lastPrevHash)
		if err != nil {
			return err
		}
		// Lookup same block index on primary chain. When the primary
		// has rolled back past tmpBlock's old index the lookup misses;
		// treat tmpBlock as non-common and keep walking back via its
		// PrevHash rather than aborting reconcile.
		primaryBlock, err := primaryChain.blockByIndexLocked(tmpBlock.ID)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			return err
		}
		// Update last common block index and return when we find a matching block on the primary chain
		if err == nil &&
			tmpBlock.Slot == primaryBlock.Slot &&
			bytes.Equal(tmpBlock.Hash, primaryBlock.Hash) {
			c.lastCommonBlockIndex = tmpBlock.ID
			break
		}
		// Decode block and extract prev-hash
		decodedBlock, err := tmpBlock.Decode()
		if err != nil {
			return err
		}
		lastPrevHash = decodedBlock.PrevHash().Bytes()
		tmpPoint := ocommon.Point{
			Hash: tmpBlock.Hash,
			Slot: tmpBlock.Slot,
		}
		newBlocks = append(newBlocks, tmpPoint)
		c.lastCommonBlockIndex--
	}
	// Prepend accumulated blocks in a single operation (O(K) instead of O(K²))
	if len(newBlocks) > 0 {
		// Reverse newBlocks since they were collected in reverse order
		slices.Reverse(newBlocks)
		c.blocks = slices.Concat(newBlocks, c.blocks)
	}
	return nil
}
