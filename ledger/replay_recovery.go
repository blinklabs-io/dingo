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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var errRestartLedgerPipeline = errors.New(
	"restart ledger pipeline after local state recovery",
)

var errHaltLedgerPipeline = errors.New(
	"persistent tx validation failure after recovery attempts",
)

// errStaleChainIterator is returned by ledgerProcessBlock when a block's
// prev-hash doesn't match the current ledger tip. This signals that the
// chain iterator has been made stale by a concurrent rollback: the iterator's
// nextBlockIndex skipped ahead past the first new fork block and is now
// returning a block that extends a branch we are no longer on. The ledger
// pipeline must restart so its iterator rewinds to the current tip.
var errStaleChainIterator = errors.New("block does not fit chain tip: stale iterator after rollback")

type txValidationError struct {
	BlockPoint ocommon.Point
	TxHash     []byte
	Inputs     []lcommon.TransactionInput
	Cause      error
}

func (e *txValidationError) Error() string {
	return fmt.Sprintf(
		"tx %s validation failure at slot %d: %v",
		hex.EncodeToString(e.TxHash),
		e.BlockPoint.Slot,
		e.Cause,
	)
}

func (e *txValidationError) Unwrap() error {
	return e.Cause
}

// maxAtTipRecoveryAttempts caps the depth schedule for recovering from the
// same persistent at-tip validation failure. Each attempt rewinds the primary
// chain progressively deeper to give chainselection room to pick a different
// fork. After the cap, recovery keeps retrying at the deepest rewind depth and
// relies on ChainSync peer rotation to find a valid candidate chain.
const maxAtTipRecoveryAttempts = 3

type atTipRecoveryAttempt struct {
	BlockPoint ocommon.Point
	TxHash     []byte
	Attempts   int
}

func newAtTipRecoveryAttempt(
	validationErr *txValidationError,
) *atTipRecoveryAttempt {
	blockPoint := validationErr.BlockPoint
	blockPoint.Hash = append([]byte(nil), blockPoint.Hash...)
	return &atTipRecoveryAttempt{
		BlockPoint: blockPoint,
		TxHash:     append([]byte(nil), validationErr.TxHash...),
		Attempts:   1,
	}
}

func (a *atTipRecoveryAttempt) matches(
	validationErr *txValidationError,
) bool {
	return a != nil &&
		validationErr.BlockPoint.Slot == a.BlockPoint.Slot &&
		bytes.Equal(validationErr.BlockPoint.Hash, a.BlockPoint.Hash) &&
		bytes.Equal(validationErr.TxHash, a.TxHash)
}

type replayRecoveryCandidate struct {
	Input         lcommon.TransactionInput
	ProducerTx    *models.Transaction
	ProducerBlock models.Block
	RollbackPoint ocommon.Point
	Strategy      string
}

type replayRecoveryPendingInput struct {
	Input   lcommon.TransactionInput
	MaxSlot uint64
}

type replayRecoveryResolvedProducer struct {
	Input         lcommon.TransactionInput
	ProducerTx    *models.Transaction
	ProducerBlock models.Block
	Tx            lcommon.Transaction
	Strategy      string
}

type replayRecoveryChainIndex struct {
	Txs         map[string]replayRecoveryChainTx
	OldestBlock *models.Block
}

type replayRecoveryChainTx struct {
	Block models.Block
	Tx    lcommon.Transaction
}

func collectReferencedInputs(tx lcommon.Transaction) []lcommon.TransactionInput {
	var ret []lcommon.TransactionInput
	seen := make(map[string]struct{})
	appendInputs := func(inputs []lcommon.TransactionInput) {
		for _, input := range inputs {
			key := input.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			ret = append(ret, input)
		}
	}
	appendInputs(tx.Inputs())
	appendInputs(tx.Collateral())
	appendInputs(tx.ReferenceInputs())
	return ret
}

func (ls *LedgerState) tryRecoverFromTxValidationError(
	err error,
) (bool, error) {
	var validationErr *txValidationError
	if !errors.As(err, &validationErr) {
		return false, nil
	}
	if ls.IsAtTip() {
		return ls.recoverAtTipFromTxValidationError(validationErr)
	}
	candidate, err := ls.findReplayRecoveryCandidate(validationErr)
	if err != nil {
		return false, err
	}
	if candidate == nil {
		return false, nil
	}
	producerTxHash := candidate.Input.Id().String()
	if candidate.ProducerTx != nil {
		producerTxHash = hex.EncodeToString(candidate.ProducerTx.Hash)
	}
	ls.config.Logger.Warn(
		"detected inconsistent local ledger state during replay, rewinding metadata state",
		"component", "ledger",
		"recovery_strategy", candidate.Strategy,
		"tx_hash", hex.EncodeToString(validationErr.TxHash),
		"failing_block_slot", validationErr.BlockPoint.Slot,
		"missing_input", candidate.Input.String(),
		"producer_tx_hash", producerTxHash,
		"producer_block_slot", candidate.ProducerBlock.Slot,
		"rollback_slot", candidate.RollbackPoint.Slot,
		"rollback_hash", hex.EncodeToString(candidate.RollbackPoint.Hash),
	)
	if ls.replayRecoveryRollbackExceedsMithrilBoundary(candidate.RollbackPoint) {
		if err := ls.rejectReplayRecoveryAtMithrilBoundary(
			validationErr,
			candidate,
			producerTxHash,
		); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := ls.rollback(candidate.RollbackPoint); err != nil {
		return false, fmt.Errorf(
			"rollback ledger state for replay recovery: %w",
			err,
		)
	}
	return true, nil
}

func (ls *LedgerState) replayRecoveryRollbackExceedsMithrilBoundary(
	point ocommon.Point,
) bool {
	ls.RLock()
	defer ls.RUnlock()
	return ls.mithrilLedgerSlot > 0 && point.Slot < ls.mithrilLedgerSlot
}

func (ls *LedgerState) rejectReplayRecoveryAtMithrilBoundary(
	validationErr *txValidationError,
	candidate *replayRecoveryCandidate,
	producerTxHash string,
) error {
	ls.RLock()
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	rewindPoint := ls.currentTip.Point
	ls.RUnlock()
	if ls.config.ChainManager == nil {
		return fmt.Errorf(
			"replay recovery rollback exceeds Mithril trust boundary: %w",
			ErrRollbackExceedsMithrilBoundary,
		)
	}
	ls.config.Logger.Warn(
		"detected replay recovery below Mithril trust boundary, rejecting peer chain",
		"component", "ledger",
		"recovery_strategy", candidate.Strategy,
		"tx_hash", hex.EncodeToString(validationErr.TxHash),
		"failing_block_slot", validationErr.BlockPoint.Slot,
		"missing_input", candidate.Input.String(),
		"producer_tx_hash", producerTxHash,
		"producer_block_slot", candidate.ProducerBlock.Slot,
		"rollback_slot", candidate.RollbackPoint.Slot,
		"rollback_hash", hex.EncodeToString(candidate.RollbackPoint.Hash),
		"mithril_ledger_slot", mithrilLedgerSlot,
		"rewind_target_slot", rewindPoint.Slot,
		"rewind_target_hash", hex.EncodeToString(rewindPoint.Hash),
	)
	if err := ls.config.ChainManager.RewindPrimaryChainToPoint(rewindPoint); err != nil {
		return fmt.Errorf(
			"rewind primary chain to Mithril trust boundary: %w",
			err,
		)
	}
	ls.chainsyncMutex.Lock()
	ls.resetChainsyncResyncState()
	ls.setChainsyncState(SyncingChainsyncState)
	ls.chainsyncMutex.Unlock()
	if ls.config.EventBus != nil {
		var activeConnId ouroboros.ConnectionId
		if ls.config.GetActiveConnectionFunc != nil {
			if connId := ls.config.GetActiveConnectionFunc(); connId != nil {
				activeConnId = *connId
			}
		}
		ls.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					ConnectionId: activeConnId,
					Reason: event.
						ChainsyncResyncReasonRollbackExceedsMithril,
					Point: rewindPoint,
				},
			),
		)
	}
	return nil
}

func (ls *LedgerState) recoverAtTipFromTxValidationError(
	validationErr *txValidationError,
) (bool, error) {
	if ls.chain == nil || ls.config.ChainManager == nil {
		return false, nil
	}
	// Determine the rewind target. On the first attempt, rewind to the
	// authoritative ledger tip — the simplest case where chainselection
	// can re-pick from another peer with a compatible chain. On
	// subsequent attempts the same (block, tx) failure means the
	// rewind-to-tip didn't help: peers keep replaying the same losing
	// fork because chainselection's intersection point is still on it.
	// Rewind progressively deeper to expose a wider candidate set, up
	// to the era's stability window. Only halt if even the deepest
	// rewind has been tried.
	attempts := 1
	if ls.lastAtTipRecovery != nil &&
		ls.lastAtTipRecovery.matches(validationErr) {
		attempts = ls.lastAtTipRecovery.Attempts + 1
		if attempts > maxAtTipRecoveryAttempts {
			ls.config.Logger.Warn(
				"at-tip recovery exhausted scheduled rewind attempts, retrying with deepest rewind",
				"component", "ledger",
				"failing_slot", validationErr.BlockPoint.Slot,
				"failing_block_hash", hex.EncodeToString(
					validationErr.BlockPoint.Hash,
				),
				"tx_hash", hex.EncodeToString(validationErr.TxHash),
				"attempts", attempts,
			)
			attempts = maxAtTipRecoveryAttempts
		}
	}
	ls.lastAtTipRecovery = newAtTipRecoveryAttempt(validationErr)
	ls.lastAtTipRecovery.Attempts = attempts
	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()
	chainTip := ls.chain.Tip()
	// Compute the rewind target. Depth grows linearly with each retry,
	// capped at the era stability window so we never undo an immutable block.
	rewindPoint := ledgerTip.Point
	if attempts > 1 {
		stabilityWindow := ls.calculateStabilityWindow()
		// depth grows linearly so the final attempt reaches the full
		// stability window: (attempts-1)/(maxAttempts-1) of the window.
		depth := stabilityWindow * uint64(attempts-1) /
			uint64(maxAtTipRecoveryAttempts-1)
		if depth > 0 && depth < ledgerTip.Point.Slot {
			rewindSlot := ledgerTip.Point.Slot - depth
			deeperPoint, lookupErr := ls.findRewindPoint(rewindSlot)
			if lookupErr == nil {
				rewindPoint = deeperPoint
			} else {
				ls.config.Logger.Warn(
					"deep rewind lookup failed, using ledger tip",
					"component", "ledger",
					"target_slot", rewindSlot,
					"error", lookupErr.Error(),
				)
			}
		}
	}
	ls.config.Logger.Warn(
		"validation failure after reaching tip, rewinding primary chain",
		"component", "ledger",
		"tx_hash", hex.EncodeToString(validationErr.TxHash),
		"failing_block_slot", validationErr.BlockPoint.Slot,
		"ledger_tip_slot", ledgerTip.Point.Slot,
		"ledger_tip_hash", hex.EncodeToString(ledgerTip.Point.Hash),
		"primary_chain_tip_slot", chainTip.Point.Slot,
		"primary_chain_tip_hash", hex.EncodeToString(chainTip.Point.Hash),
		"rewind_target_slot", rewindPoint.Slot,
		"attempt", attempts,
	)
	if err := ls.config.ChainManager.RewindPrimaryChainToPoint(
		rewindPoint,
	); err != nil {
		return false, fmt.Errorf(
			"rewind primary chain after validation failure: %w",
			err,
		)
	}
	// Roll back the ledger metadata state to the rewind point. Without
	// this, the chain is pruned to rewindPoint but the UTxO database
	// still reflects the failing block's post-apply state — consumed
	// inputs stay consumed, created outputs stay created. When peers
	// re-deliver the block we just rewound past, ledger validation
	// looks up its inputs, finds them already marked consumed, and
	// returns "rule 22 bad input(s) ... rule 24 value not conserved
	// (consumed 0)" again, looping the recovery indefinitely until
	// process restart. RewindPrimaryChainToPoint by design only touches
	// the chain blob — the matching ledger rollback must be explicit.
	if err := ls.rollback(rewindPoint); err != nil {
		return false, fmt.Errorf(
			"rollback ledger state after validation failure: %w",
			err,
		)
	}
	if ls.config.EventBus != nil {
		ls.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					Reason: event.ChainsyncResyncReasonLiveTxValidationRecovery,
					Point:  rewindPoint,
				},
			),
		)
	}
	return true, nil
}

// findRewindPoint returns the highest committed chain point at or
// below targetSlot, used to compute deeper rewind anchors during
// at-tip validation recovery. Falls back to slot 0 if no earlier
// committed block can be located.
func (ls *LedgerState) findRewindPoint(targetSlot uint64) (ocommon.Point, error) {
	if ls.chain == nil {
		return ocommon.Point{Slot: targetSlot}, nil
	}
	block, err := database.BlockBeforeSlot(ls.db, targetSlot+1)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return ocommon.Point{}, nil
		}
		return ocommon.Point{}, err
	}
	return ocommon.Point{Slot: block.Slot, Hash: block.Hash}, nil
}

func (ls *LedgerState) findReplayRecoveryCandidate(
	validationErr *txValidationError,
) (*replayRecoveryCandidate, error) {
	chainIndex, err := ls.buildReplayRecoveryChainIndex(validationErr.BlockPoint)
	if err != nil {
		return nil, err
	}
	var candidate *replayRecoveryCandidate
	var unresolvedInputs []lcommon.TransactionInput
	pendingInputs := make([]replayRecoveryPendingInput, 0, len(validationErr.Inputs))
	for _, input := range validationErr.Inputs {
		pendingInputs = append(pendingInputs, replayRecoveryPendingInput{
			Input:   input,
			MaxSlot: validationErr.BlockPoint.Slot,
		})
	}
	seenInputs := make(map[string]struct{})
	expandedTxs := make(map[string]struct{})
	for len(pendingInputs) > 0 {
		pending := pendingInputs[0]
		pendingInputs = pendingInputs[1:]
		inputKey := pending.Input.String()
		if _, ok := seenInputs[inputKey]; ok {
			continue
		}
		seenInputs[inputKey] = struct{}{}
		resolved, err := ls.resolveReplayRecoveryProducer(
			pending,
			chainIndex,
		)
		if err != nil {
			return nil, err
		}
		if resolved == nil {
			unresolvedInputs = append(unresolvedInputs, pending.Input)
			continue
		}
		rollbackPoint, err := ls.replayRecoveryParentPoint(
			resolved.ProducerBlock,
		)
		if err != nil {
			return nil, err
		}
		if candidate == nil ||
			resolved.ProducerBlock.Slot < candidate.ProducerBlock.Slot {
			candidate = &replayRecoveryCandidate{
				Input:         resolved.Input,
				ProducerTx:    resolved.ProducerTx,
				ProducerBlock: resolved.ProducerBlock,
				RollbackPoint: rollbackPoint,
				Strategy:      resolved.Strategy,
			}
		}
		if resolved.Tx == nil {
			continue
		}
		txKey := string(resolved.Tx.Hash().Bytes())
		if _, ok := expandedTxs[txKey]; ok {
			continue
		}
		expandedTxs[txKey] = struct{}{}
		for _, depInput := range collectReferencedInputs(resolved.Tx) {
			pendingInputs = append(pendingInputs, replayRecoveryPendingInput{
				Input:   depInput,
				MaxSlot: resolved.ProducerBlock.Slot,
			})
		}
	}
	if len(unresolvedInputs) > 0 {
		fallbackCandidate, err := ls.replayRecoveryFallbackCandidate(
			validationErr.BlockPoint,
			unresolvedInputs,
		)
		if err != nil {
			return nil, err
		}
		if fallbackCandidate != nil && (candidate == nil ||
			fallbackCandidate.ProducerBlock.Slot < candidate.ProducerBlock.Slot) {
			candidate = fallbackCandidate
		}
	}
	return candidate, nil
}

func (ls *LedgerState) buildReplayRecoveryChainIndex(
	failingPoint ocommon.Point,
) (*replayRecoveryChainIndex, error) {
	failingBlock, err := database.BlockByPoint(ls.db, failingPoint)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return &replayRecoveryChainIndex{
				Txs: make(map[string]replayRecoveryChainTx),
			}, nil
		}
		return nil, fmt.Errorf(
			"lookup failing block %x at slot %d for replay recovery: %w",
			failingPoint.Hash,
			failingPoint.Slot,
			err,
		)
	}
	index := &replayRecoveryChainIndex{
		Txs: make(map[string]replayRecoveryChainTx),
	}
	if failingBlock.ID <= database.BlockInitialIndex {
		return index, nil
	}
	const maxReplayRecoveryScanBlocks = 4096
	scanned := 0
	for blockIndex := failingBlock.ID - 1; ; blockIndex-- {
		if scanned >= maxReplayRecoveryScanBlocks {
			break
		}
		block, err := ls.db.BlockByIndex(blockIndex, nil)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				if blockIndex == database.BlockInitialIndex {
					break
				}
				continue
			}
			return nil, fmt.Errorf(
				"lookup block %d during replay recovery scan: %w",
				blockIndex,
				err,
			)
		}
		if block.Slot >= failingPoint.Slot {
			if blockIndex == database.BlockInitialIndex {
				break
			}
			continue
		}
		index.OldestBlock = &block
		decodedBlock, err := block.Decode()
		if err != nil {
			ls.config.Logger.Debug(
				"skipping undecodable block during replay recovery scan",
				"component", "ledger",
				"block_slot", block.Slot,
				"block_hash", hex.EncodeToString(block.Hash),
				"error", err,
			)
			if blockIndex == database.BlockInitialIndex {
				break
			}
			scanned++
			continue
		}
		for _, tx := range decodedBlock.Transactions() {
			txKey := string(tx.Hash().Bytes())
			if _, ok := index.Txs[txKey]; ok {
				continue
			}
			index.Txs[txKey] = replayRecoveryChainTx{
				Block: block,
				Tx:    tx,
			}
		}
		scanned++
		if blockIndex == database.BlockInitialIndex {
			break
		}
	}
	return index, nil
}

func (ls *LedgerState) resolveReplayRecoveryProducer(
	pending replayRecoveryPendingInput,
	chainIndex *replayRecoveryChainIndex,
) (*replayRecoveryResolvedProducer, error) {
	utxo, err := ls.db.UtxoByRef(
		pending.Input.Id().Bytes(),
		pending.Input.Index(),
		nil,
	)
	if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
		return nil, fmt.Errorf(
			"lookup validation input %s: %w",
			pending.Input.String(),
			err,
		)
	}
	if utxo != nil {
		return nil, nil
	}
	producerTx, err := ls.db.GetTransactionByHash(
		pending.Input.Id().Bytes(),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup producer tx %s: %w",
			pending.Input.Id().String(),
			err,
		)
	}
	if producerTx != nil && len(producerTx.BlockHash) > 0 {
		producerBlock, err := database.BlockByHash(ls.db, producerTx.BlockHash)
		if err != nil {
			return nil, fmt.Errorf(
				"lookup producer block %x: %w",
				producerTx.BlockHash,
				err,
			)
		}
		if producerBlock.Slot >= pending.MaxSlot {
			return nil, nil
		}
		tx := ls.replayRecoveryResolveTxFromBlock(
			producerBlock,
			pending.Input.Id().Bytes(),
			chainIndex,
		)
		return &replayRecoveryResolvedProducer{
			Input:         pending.Input,
			ProducerTx:    producerTx,
			ProducerBlock: producerBlock,
			Tx:            tx,
			Strategy:      "metadata",
		}, nil
	}
	producerBlock, found, err := ls.replayRecoveryBlockFromTxBlob(
		pending.Input.Id().Bytes(),
	)
	if err != nil {
		return nil, err
	}
	if found {
		if producerBlock.Slot >= pending.MaxSlot {
			return nil, nil
		}
		tx := ls.replayRecoveryResolveTxFromBlock(
			producerBlock,
			pending.Input.Id().Bytes(),
			chainIndex,
		)
		return &replayRecoveryResolvedProducer{
			Input:         pending.Input,
			ProducerBlock: producerBlock,
			Tx:            tx,
			Strategy:      "tx-blob",
		}, nil
	}
	if chainIndex != nil {
		chainTx, ok := chainIndex.Txs[string(pending.Input.Id().Bytes())]
		if ok && chainTx.Block.Slot < pending.MaxSlot {
			return &replayRecoveryResolvedProducer{
				Input:         pending.Input,
				ProducerBlock: chainTx.Block,
				Tx:            chainTx.Tx,
				Strategy:      "chain-scan",
			}, nil
		}
	}
	return nil, nil
}

func (ls *LedgerState) replayRecoveryResolveTxFromBlock(
	block models.Block,
	txHash []byte,
	chainIndex *replayRecoveryChainIndex,
) lcommon.Transaction {
	if chainIndex != nil {
		chainTx, ok := chainIndex.Txs[string(txHash)]
		if ok && bytes.Equal(chainTx.Block.Hash, block.Hash) {
			return chainTx.Tx
		}
	}
	decodedBlock, err := block.Decode()
	if err != nil {
		ls.config.Logger.Debug(
			"skipping undecodable producer block during replay recovery",
			"component", "ledger",
			"block_slot", block.Slot,
			"block_hash", hex.EncodeToString(block.Hash),
			"error", err,
		)
		return nil
	}
	for _, tx := range decodedBlock.Transactions() {
		if bytes.Equal(tx.Hash().Bytes(), txHash) {
			return tx
		}
	}
	return nil
}

func (ls *LedgerState) replayRecoveryFallbackCandidate(
	failingPoint ocommon.Point,
	inputs []lcommon.TransactionInput,
) (*replayRecoveryCandidate, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	failingBlock, err := database.BlockByPoint(ls.db, failingPoint)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"lookup failing block %x at slot %d for replay fallback: %w",
			failingPoint.Hash,
			failingPoint.Slot,
			err,
		)
	}
	if failingBlock.ID <= database.BlockInitialIndex {
		return nil, nil
	}
	rewindBlocks := ls.SecurityParam()
	if rewindBlocks <= 0 {
		return nil, nil
	}
	targetIndex := database.BlockInitialIndex
	if failingBlock.ID > uint64(rewindBlocks) {
		targetIndex = failingBlock.ID - uint64(rewindBlocks)
	}
	anchorBlock, err := ls.db.BlockByIndex(targetIndex, nil)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup replay fallback block %d: %w",
			targetIndex,
			err,
		)
	}
	rollbackPoint, err := ls.replayRecoveryParentPoint(anchorBlock)
	if err != nil {
		return nil, err
	}
	return &replayRecoveryCandidate{
		Input:         inputs[0],
		ProducerBlock: anchorBlock,
		RollbackPoint: rollbackPoint,
		Strategy:      "security-param-fallback",
	}, nil
}

func (ls *LedgerState) replayRecoveryBlockFromTxBlob(
	txHash []byte,
) (models.Block, bool, error) {
	blob := ls.db.Blob()
	if blob == nil {
		return models.Block{}, false, nil
	}
	txn := ls.db.BlobTxn(false)
	if txn == nil || txn.Blob() == nil {
		return models.Block{}, false, nil
	}
	defer txn.Rollback() //nolint:errcheck

	txData, err := blob.GetTx(txn.Blob(), txHash)
	if err != nil {
		if errors.Is(err, dbtypes.ErrBlobKeyNotFound) {
			return models.Block{}, false, nil
		}
		return models.Block{}, false, fmt.Errorf(
			"lookup tx blob %s: %w",
			hex.EncodeToString(txHash),
			err,
		)
	}

	var point ocommon.Point
	switch {
	case database.IsTxOffsetStorage(txData):
		offset, err := database.DecodeTxOffset(txData)
		if err != nil {
			return models.Block{}, false, fmt.Errorf(
				"decode tx offset for %s: %w",
				hex.EncodeToString(txHash),
				err,
			)
		}
		point = ocommon.NewPoint(offset.BlockSlot, offset.BlockHash[:])
	case database.IsTxCborPartsStorage(txData):
		parts, err := database.DecodeTxCborParts(txData)
		if err != nil {
			return models.Block{}, false, fmt.Errorf(
				"decode tx parts for %s: %w",
				hex.EncodeToString(txHash),
				err,
			)
		}
		point = ocommon.NewPoint(parts.BlockSlot, parts.BlockHash[:])
	default:
		return models.Block{}, false, nil
	}

	block, err := database.BlockByPoint(ls.db, point)
	if err != nil {
		return models.Block{}, false, fmt.Errorf(
			"lookup producer block from tx blob %s: %w",
			hex.EncodeToString(txHash),
			err,
		)
	}
	return block, true, nil
}

func (ls *LedgerState) replayRecoveryParentPoint(
	block models.Block,
) (ocommon.Point, error) {
	if block.Slot == 0 || len(block.PrevHash) == 0 {
		return ocommon.Point{}, nil
	}
	parentBlock, err := database.BlockByHash(ls.db, block.PrevHash)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf(
			"lookup parent block for replay recovery at slot %d: %w",
			block.Slot,
			err,
		)
	}
	return ocommon.NewPoint(parentBlock.Slot, parentBlock.Hash), nil
}
