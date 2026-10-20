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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ValidateForgedBlock validates a locally-forged block before it is adopted
// onto the chain and diffused to peers. It runs three checks in order:
//
//  1. Header crypto — VRF proof and KES signature verification (skipped for
//     Byron-era blocks which use PBFT consensus and have no VRF/KES fields).
//  2. Body-hash consistency — verifies that the body hash in the header is
//     non-zero, catching any builder bug that would embed an all-zero hash.
//  3. Aggregate reference-script limits, then per-transaction ledger rules.
//     Each transaction in the block is
//     validated against the current UTxO state. An intra-block overlay
//     is maintained so that transactions spending outputs created earlier
//     in the same block are correctly resolved.
//
// A non-nil error means the block is invalid and must not be adopted or
// diffused. This function satisfies the forging.BlockValidator interface.
func (ls *LedgerState) ValidateForgedBlock(ctx context.Context,
	block ledger.Block,
	_ []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if block == nil {
		return errors.New("nil block")
	}

	// 1. Header crypto: VRF proof + KES signature.
	// Byron blocks use PBFT and have no VRF/KES fields; skip them.
	if block.Era().Id != byron.EraIdByron {
		if err := ls.verifyBlockHeaderCryptoWithEpochAdvance(ctx, block, true, false); err != nil {
			return fmt.Errorf("header crypto: %w", err)
		}
	}

	// 2. Body-hash consistency: a non-Byron block with an all-zero body
	// hash is always invalid — the builder must have committed a real hash.
	if err := ls.validateForgedBodyHash(block); err != nil {
		return err
	}

	// 3. Per-transaction ledger validation with intra-block UTxO overlay.
	if err := ls.validateForgedTxs(ctx, block); err != nil {
		return err
	}

	return nil
}

// validateForgedBodyHash checks that the body hash embedded in the header is
// non-zero for non-Byron blocks. gouroboros era parsers validate hash
// agreement during CBOR decode, but we re-check here as defence-in-depth.
func (ls *LedgerState) validateForgedBodyHash(block ledger.Block) error {
	if block.Era().Id == byron.EraIdByron {
		return nil
	}
	bodyHash := block.Header().BlockBodyHash()
	bh := bodyHash.Bytes()
	if len(bh) == 0 {
		return fmt.Errorf(
			"block at slot %d has empty body hash bytes",
			block.SlotNumber(),
		)
	}
	allZero := true
	for _, b := range bh {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return fmt.Errorf(
			"block at slot %d has all-zero body hash",
			block.SlotNumber(),
		)
	}
	return nil
}

const (
	forgedValidationWaitMsg = "forged block self-validation waiting for the ledger state to settle"
	// forgedValidationMaxAttempts bounds the sessions one forged block may
	// open while the ledger keeps changing under it. Every attempt after the
	// first waits for a publication or a finished transition, so the cap
	// only binds on a ledger that transitions without pause within the
	// forged slot, which bounds the wait on its own.
	forgedValidationMaxAttempts = 8
)

// errForgedValidationNotCurrent marks a validation session that stopped
// being current before it reached a verdict.
var errForgedValidationNotCurrent = errors.New(
	"forged transaction validation session is not current",
)

// errForgedParentNotApplied marks a validation session whose applied ledger
// tip trails the forged block's parent along the primary chain. The state it
// would judge against lacks the effects of every unapplied ancestor.
var errForgedParentNotApplied = errors.New(
	"applied ledger tip is not the forged block's parent",
)

// validateForgedTxs validates each transaction in the block against the
// current ledger state using an intra-block UTxO overlay. The overlay
// accumulates outputs produced by earlier transactions so that later
// transactions in the same block can spend them correctly.
//
// The overlay carries only this block's own effects, so a verdict is taken
// once the applied ledger tip has reached the block's parent: the forger admits
// a parent a few blocks ahead of the applied tip, and an unapplied ancestor's
// spent inputs and certificates are in neither the ledger state nor the
// overlay. Until the ledger reaches the parent, validation waits like it
// does for a transition in flight.
//
// A session that a block apply, rollover or rollback makes non-current has
// judged nothing, and on a producer one is often in flight -- typically the
// apply of the forged block's own parent. Validation therefore waits for
// that transition to publish and starts again, for as long as the forged
// slot lasts and the ledger tip stays the one validation started from, is
// the block's parent, or lies on the primary chain while that chain still
// ends at the parent. Any other tip is a chain the block does not extend.
// When no session can complete, the result wraps
// forging.ErrForgedBlockStateChanged: the block is dropped unjudged, never
// adopted.
func (ls *LedgerState) validateForgedTxs(
	ctx context.Context,
	block ledger.Block,
) error {
	txs := block.Transactions()
	if len(txs) == 0 {
		return nil
	}

	if err := ls.ValidateBlockReferenceScripts(ctx, block); err != nil {
		return err
	}

	parentHash := LeiosValidationParentHash(block.Header())
	_, startTip := ls.loadStateSnapshots()
	startHash := startTip.currentTip.Point.Hash
	slotEnd, slotEndErr := ls.SlotToTime(block.SlotNumber() + 1)
	for attempt := 1; ; {
		// Taken before the attempt, so a change that lands during it
		// closes this channel and the wait below cannot miss it.
		settled := ls.txValidationSettledChan()
		err := ls.validateForgedTxsOnce(ctx, block, txs, parentHash)
		switch {
		case errors.Is(err, errForgedParentNotApplied):
			// Each applied ancestor publishes once, so these waits are
			// bounded by the slot end rather than the attempt cap.
		case errors.Is(err, errForgedValidationNotCurrent):
			attempt++
		default:
			return err
		}
		if attempt > forgedValidationMaxAttempts {
			return fmt.Errorf(
				"%w: no current ledger state after %d attempts",
				forging.ErrForgedBlockStateChanged,
				attempt-1,
			)
		}
		// Waiting is bounded by the end of the forged slot. A slot whose
		// end cannot be placed gets no wait at all, so a transition that
		// never finishes cannot hold the forger.
		if slotEndErr != nil {
			return fmt.Errorf(
				"%w: end of slot %d unknown: %w",
				forging.ErrForgedBlockStateChanged,
				block.SlotNumber(),
				slotEndErr,
			)
		}
		slotEnded := fmt.Errorf(
			"%w: slot %d ended before the ledger state settled",
			forging.ErrForgedBlockStateChanged,
			block.SlotNumber(),
		)
		remaining := time.Until(slotEnd)
		if remaining <= 0 {
			return slotEnded
		}
		timer := time.NewTimer(remaining)
		ls.config.Logger.Debug(
			forgedValidationWaitMsg,
			"component", "ledger",
			"slot", block.SlotNumber(),
			"attempt", attempt-1,
		)
		var waitErr error
		select {
		case <-settled:
		case <-timer.C:
			waitErr = slotEnded
		case <-ctx.Done():
			waitErr = ctx.Err()
		}
		timer.Stop()
		if waitErr != nil {
			return waitErr
		}
		_, tip := ls.loadStateSnapshots()
		tipHash := tip.currentTip.Point.Hash
		if !bytes.Equal(tipHash, startHash) &&
			!bytes.Equal(tipHash, parentHash) &&
			//nolint:contextcheck // chain.TipRelation takes no context; it reads one locked chain snapshot
			!ls.forgedParentChainHolds(tip.currentTip.Point, parentHash) {
			return fmt.Errorf(
				"%w: ledger tip %x is not the forged block's parent %x",
				forging.ErrForgedBlockStateChanged,
				tipHash,
				parentHash,
			)
		}
	}
}

// forgedParentChainHolds reports whether tip lies on the primary chain while
// that chain still ends at the forged block's parent. The forger admits a
// slot while the ledger trails the primary chain by a few blocks, so the
// ledger can apply an unapplied block between the tip validation started
// from and the parent; that tip is on the block's own chain, not a rival.
func (ls *LedgerState) forgedParentChainHolds(
	tip ocommon.Point,
	parentHash []byte,
) bool {
	primaryTip, _, ancestor, err := ls.PrimaryChainTipRelation(tip)
	return err == nil && ancestor &&
		bytes.Equal(primaryTip.Point.Hash, parentHash)
}

// forgedTipBeforeParent classifies an applied ledger tip that is not the
// forged block's parent. A tip on the primary chain while that chain still
// ends at the parent is an ancestor the ledger has yet to apply forward from,
// so the session waits (errForgedParentNotApplied). Any other tip -- one the
// primary chain has passed the parent beyond, one past the parent, one off the
// primary chain, or one whose relation cannot be read -- is state the block
// does not extend, and the block is dropped unjudged
// (forging.ErrForgedBlockStateChanged). A ledger with no primary chain has no
// ancestry to place the parent in, so its applied state is judged as is;
// NewLedgerState always attaches one.
func (ls *LedgerState) forgedTipBeforeParent(
	tip ocommon.Point,
	parentHash []byte,
) error {
	ls.RLock()
	hasChain := ls.chain != nil
	ls.RUnlock()
	if !hasChain {
		return nil
	}
	if ls.forgedParentChainHolds(tip, parentHash) {
		return errForgedParentNotApplied
	}
	return fmt.Errorf(
		"%w: ledger tip %x is not on the primary chain ending at the forged block's parent %x",
		forging.ErrForgedBlockStateChanged,
		tip.Hash,
		parentHash,
	)
}

// validateForgedTxsOnce runs one validation session over txs. It returns
// errForgedValidationNotCurrent when the session stopped being current
// before every transaction was judged.
func (ls *LedgerState) validateForgedTxsOnce(
	ctx context.Context,
	block ledger.Block,
	txs []ledger.Transaction,
	parentHash []byte,
) error {
	// The tip relation takes the ledger read lock and the chain lock, so it
	// is read before the session opens: the session holds a database
	// transaction, and taking the ledger read lock under it can deadlock with
	// a publisher that holds the write lock and needs a database connection.
	// The session is then bound to that same tip, so the relation read here
	// describes the state the session judges.
	_, tipState := ls.loadStateSnapshots()
	tip := tipState.currentTip.Point
	if !bytes.Equal(tip.Hash, parentHash) {
		//nolint:contextcheck // chain.TipRelation takes no context; it reads one locked chain snapshot
		if err := ls.forgedTipBeforeParent(tip, parentHash); err != nil {
			return err
		}
	}
	// Non-nil even at origin, so the session always checks its tip.
	sessionTip := append(make([]byte, 0, len(tip.Hash)), tip.Hash...)
	// consumedUtxos: inputs spent within this block (prevents intra-block
	// double-spend).
	// createdUtxos: outputs produced within this block and not yet in the
	// persistent UTxO set.
	err := ls.withTxValidationSession(
		ctx,
		sessionTip,
		nil,
		false,
		false,
		func(
			validate func(
				ledger.Transaction,
				map[utxoref.Key]struct{},
				map[utxoref.Key]lcommon.Utxo,
				*utxoref.StateOverlay,
			) error,
			stillCurrent func() bool,
			_ func(func() error) (bool, error),
			_ txValidationApplyFunc,
		) error {
			if !stillCurrent() {
				return errForgedValidationNotCurrent
			}
			consumedUtxos := make(map[utxoref.Key]struct{}, len(txs)*2)
			createdUtxos := make(map[utxoref.Key]lcommon.Utxo, len(txs)*4)
			// pendingAccounts: reward-account effects of earlier transactions in
			// this block, so a later withdrawal sees the balance they leave.
			pendingAccounts := utxoref.NewStateOverlay()

			for _, tx := range txs {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !stillCurrent() {
					return errForgedValidationNotCurrent
				}
				if err := validate(
					tx,
					consumedUtxos,
					createdUtxos,
					pendingAccounts,
				); err != nil {
					// A rule failure judged against state that moved
					// during the call is not a verdict on the block.
					if !stillCurrent() {
						return errForgedValidationNotCurrent
					}
					return fmt.Errorf(
						"tx %s in forged block at slot %d: %w",
						tx.Hash(),
						block.SlotNumber(),
						err,
					)
				}

				// Advance the overlay with this transaction's effects.
				for _, utxo := range tx.Produced() {
					createdUtxos[utxoref.ForUtxo(utxo)] = utxo
				}
				// Use Consumed() instead of Inputs(): for a phase-2 failed Plutus tx
				// the regular inputs are NOT spent; only the collateral inputs are.
				// Inputs() would falsely mark regular inputs as consumed and reject
				// a later tx in the same block that spends those still-valid UTxOs.
				for _, input := range tx.Consumed() {
					consumedUtxos[utxoref.ForInput(input)] = struct{}{}
				}
				pendingAccounts.Apply(tx)
			}

			return nil
		},
	)
	// The ledger moved off the tip the relation was read for before the
	// session opened; that relation says nothing about the new tip.
	if errors.Is(err, ErrLeiosValidationParentSuperseded) {
		return errForgedValidationNotCurrent
	}
	return err
}

// ValidateBlockReferenceScripts checks a locally built block's aggregate
// reference-script budget before adoption. It does not run header crypto or
// transaction scripts, and is required even when full self-validation is off.
func (ls *LedgerState) ValidateBlockReferenceScripts(
	ctx context.Context,
	block ledger.Block,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if block == nil {
		return errors.New("nil block")
	}
	if block.Era().Id < conway.EraIdConway || len(block.Transactions()) == 0 {
		return nil
	}
	snapshot := ls.loadConsensusSnapshot()
	if ls.skipDijkstraTxValidation(snapshot.currentEra.Id) {
		return nil
	}

	pp := referenceScriptParams(
		block,
		snapshot.currentEra,
		ls.eraList(),
		snapshot.currentPParams,
		snapshot.prevEraPParams,
	)
	return ls.db.Transaction(ctx, false).Do(func(txn *database.Txn) error {
		lv := &LedgerView{txn: txn, ls: ls}
		err := validateBlockReferenceScripts(block, pp, lv)
		return storageFaultOrErr(lv, err)
	})
}
