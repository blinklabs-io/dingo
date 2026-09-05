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

package ledger

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/consensus/praos"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrNotChainTipSibling reports a block offered to AdoptLocalForgedSibling
// that does not in fact compete with the current chain tip: it does not share
// the tip's parent, does not carry the tip's block number, or the chain has no
// resolvable predecessor to fork from. Such a block is never adopted, because
// the rollback that adoption performs is only ever one block deep and only
// ever to the block the two candidates already agree on.
var ErrNotChainTipSibling = errors.New(
	"block does not compete with the current chain tip",
)

// praosPrefersCandidateSibling reports whether an equal-length candidate beats
// the incumbent tip under the standard Praos rule: higher block number, then
// the reference implementation's opcert-issue-number and VRF tiebreakers when
// they are armed.
//
// It deliberately has no notion of whose block is whose. ComparePraosTips
// answers ChainEqual whenever no reference implementation rule applies -- an
// unarmed tiebreaker, an unresolvable select view, identical VRF -- and
// ChainEqual keeps the incumbent. A locally forged block therefore has to win
// on the same evidence a peer's block would need, and a tie leaves the chain
// exactly where it was.
func praosPrefersCandidateSibling(
	candidateTip, incumbentTip ochainsync.Tip,
	candidateView, incumbentView praos.PraosTiebreakerView,
) bool {
	return praos.ComparePraosTips(
		candidateTip,
		incumbentTip,
		candidateView,
		incumbentView,
	) == praos.ChainABetter
}

// AdoptLocalForgedSibling offers a locally forged block that competes with the
// current chain tip, rather than extending it, to chain selection.
//
// This is the local counterpart of what a peer-delivered competing block does
// today. When a peer sends a header that does not fit our tip, chainsync's
// tryResolveFork compares it against the local tip with
// praos.ComparePraosTips, and if it wins, rolls the chain and ledger back to
// the common ancestor and adopts the peer's chain from there. A block this
// node forges itself at a slot a rival already occupies is the same situation
// with the same arbitration, but it arrives through the forger rather than
// through a connection, so it cannot enter that path: tryResolveFork is keyed
// on a ChainsyncEvent, a peer connection, that peer's retained header history
// and a blockfetch restart, none of which exist here.
//
// What is shared is the decision and the mutation:
//
//   - The decision is praos.ComparePraosTips on the two tips and their select
//     views, the same comparison tryResolveFork makes, with no preference for
//     the local block. A candidate that does not strictly win is dropped.
//   - The mutation is rollbackChainAndStateDeferred to the common parent
//     followed by the adoption of the winner, the same sequence tryResolveFork
//     performs. It runs under chainsyncMutex, which is the lock that path
//     holds, so a rollback driven from the forge loop is serialized against
//     concurrent chainsync header handling exactly as one driven from a peer
//     is.
//
// The rollback is always exactly one block deep, to the block both candidates
// name as parent, and it is only reached after the candidate has been checked
// to be a genuine sibling of the tip.
//
// It reports whether the block was adopted. Losing chain selection is a normal
// outcome and returns (false, nil): the rival keeps the slot and the forged
// block is discarded undiffused.
func (ls *LedgerState) AdoptLocalForgedSibling(
	block gledger.Block,
) (bool, error) {
	if ls == nil || ls.chain == nil {
		return false, errors.New("ledger state unavailable")
	}
	if block == nil {
		return false, errors.New("proposed block is nil")
	}
	// Registered before the mutex is taken so defer's LIFO order runs it
	// after the unlock: publishing under ls.chainsyncMutex deadlocks the
	// node. See pendingPublishes.
	var pending pendingPublishes
	defer pending.flush()
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()

	parent, incumbentTip, ok := ls.chain.TipPredecessor()
	if !ok {
		return false, fmt.Errorf(
			"%w: chain tip has no resolvable predecessor",
			ErrNotChainTipSibling,
		)
	}
	if err := validateLocalSibling(block, parent, incumbentTip); err != nil {
		return false, err
	}
	// Only compete for a tip the ledger has actually applied. While the
	// pipeline is still replaying, the primary chain runs ahead of committed
	// ledger state, and a rollback there would rewind a tip the ledger never
	// reached. The forger's own sync gates normally keep this unreachable.
	if appliedTip := ls.Tip(); !bytes.Equal(
		appliedTip.Point.Hash,
		incumbentTip.Point.Hash,
	) {
		return false, fmt.Errorf(
			"%w: ledger tip %x has not caught up to chain tip %x",
			ErrNotChainTipSibling,
			appliedTip.Point.Hash,
			incumbentTip.Point.Hash,
		)
	}

	blockHash := block.Hash().Bytes()
	candidateTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(block.SlotNumber(), blockHash),
		BlockNumber: block.BlockNumber(),
	}
	candidateView, _ := praos.GetPraosTiebreakerView(block.Header())
	incumbentView := ls.localTipPraosView(incumbentTip)
	if !praosPrefersCandidateSibling(
		candidateTip,
		incumbentTip,
		candidateView,
		incumbentView,
	) {
		ls.config.Logger.Info(
			"locally forged block lost chain selection to the block already at the tip",
			"component", "ledger",
			"slot", block.SlotNumber(),
			"forged_hash", hex.EncodeToString(blockHash),
			"tip_hash", hex.EncodeToString(incumbentTip.Point.Hash),
			"block_number", block.BlockNumber(),
		)
		return false, nil
	}

	ls.config.Logger.Info(
		"locally forged block won chain selection; replacing the block at the tip",
		"component", "ledger",
		"slot", block.SlotNumber(),
		"forged_hash", hex.EncodeToString(blockHash),
		"replaced_hash", hex.EncodeToString(incumbentTip.Point.Hash),
		"fork_point_slot", parent.Slot,
		"block_number", block.BlockNumber(),
	)
	if err := ls.rollbackChainAndStateDeferred(parent, &pending); err != nil {
		return false, fmt.Errorf(
			"roll back to fork point %x for locally forged sibling: %w",
			parent.Hash,
			err,
		)
	}
	if _, err := ls.chain.AddLocalBlockDeferred(block); err != nil {
		// The chain is now at the fork point with neither candidate on it.
		// That is recoverable -- chainsync re-offers the rival's header,
		// which no longer conflicts with our tip -- but it is not a state
		// to pass over quietly.
		ls.config.Logger.Error(
			"locally forged sibling could not be adopted after the fork-point rollback",
			"component", "ledger",
			"slot", block.SlotNumber(),
			"forged_hash", hex.EncodeToString(blockHash),
			"fork_point_slot", parent.Slot,
			"error", err,
		)
		return false, fmt.Errorf(
			"adopt locally forged sibling %x: %w",
			blockHash,
			err,
		)
	}
	pending.drainChain(ls.chain)
	return true, nil
}

// validateLocalSibling checks that block genuinely competes with the tip:
// the tip's slot, the tip's parent, the tip's block number, and a slot strictly
// above the parent's. Only a block that satisfies all four can be adopted by
// rolling back exactly one block, and only such a block is an equal-length
// candidate for the Praos comparison that follows.
//
// Slot equality is the admission boundary for this path, not merely the
// forger's precondition. This is the equal-slot (EQ) case of
// mkCurrentBlockContext and nothing else: a same-block-number competitor at a
// different slot is an ordinary fork, which belongs to chainsync's fork
// resolution with its ancestor search and rollback-depth accounting, not to a
// one-block rollback driven from the forge loop. Enforcing it here keeps the
// ledger gate closed even if a future caller reaches it without the forger's
// own currentSlot == tipSlot check.
func validateLocalSibling(
	block gledger.Block,
	parent ocommon.Point,
	incumbentTip ochainsync.Tip,
) error {
	blockHash := block.Hash().Bytes()
	if bytes.Equal(blockHash, incumbentTip.Point.Hash) {
		return fmt.Errorf(
			"%w: block %x is already the chain tip",
			ErrNotChainTipSibling,
			blockHash,
		)
	}
	if !bytes.Equal(block.PrevHash().Bytes(), parent.Hash) {
		return fmt.Errorf(
			"%w: block parent %x is not the chain tip's parent %x",
			ErrNotChainTipSibling,
			block.PrevHash().Bytes(),
			parent.Hash,
		)
	}
	if block.BlockNumber() != incumbentTip.BlockNumber {
		return fmt.Errorf(
			"%w: block number %d is not the chain tip's %d",
			ErrNotChainTipSibling,
			block.BlockNumber(),
			incumbentTip.BlockNumber,
		)
	}
	if block.SlotNumber() <= parent.Slot {
		return fmt.Errorf(
			"%w: block slot %d does not follow parent slot %d",
			ErrNotChainTipSibling,
			block.SlotNumber(),
			parent.Slot,
		)
	}
	if block.SlotNumber() != incumbentTip.Point.Slot {
		return fmt.Errorf(
			"%w: block slot %d is not the chain tip's slot %d",
			ErrNotChainTipSibling,
			block.SlotNumber(),
			incumbentTip.Point.Slot,
		)
	}
	return nil
}
