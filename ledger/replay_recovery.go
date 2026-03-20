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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var errRestartLedgerPipeline = errors.New(
	"restart ledger pipeline after local state recovery",
)

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

type replayRecoveryCandidate struct {
	Input         lcommon.TransactionInput
	ProducerTx    *models.Transaction
	ProducerBlock models.Block
	RollbackPoint ocommon.Point
	Strategy      string
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
	if err := ls.rollback(candidate.RollbackPoint); err != nil {
		return false, fmt.Errorf(
			"rollback ledger state for replay recovery: %w",
			err,
		)
	}
	return true, nil
}

func (ls *LedgerState) findReplayRecoveryCandidate(
	validationErr *txValidationError,
) (*replayRecoveryCandidate, error) {
	var candidate *replayRecoveryCandidate
	var unresolvedInputs []lcommon.TransactionInput
	for _, input := range validationErr.Inputs {
		utxo, err := ls.db.UtxoByRef(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
			return nil, fmt.Errorf(
				"lookup validation input %s: %w",
				input.String(),
				err,
			)
		}
		if utxo != nil {
			continue
		}
		producerTx, err := ls.db.GetTransactionByHash(
			input.Id().Bytes(),
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"lookup producer tx %s: %w",
				input.Id().String(),
				err,
			)
		}
		if producerTx == nil || len(producerTx.BlockHash) == 0 {
			producerBlock, found, err := ls.replayRecoveryBlockFromTxBlob(
				input.Id().Bytes(),
			)
			if err != nil {
				return nil, err
			}
			if !found {
				unresolvedInputs = append(unresolvedInputs, input)
				continue
			}
			if producerBlock.Slot >= validationErr.BlockPoint.Slot {
				continue
			}
			rollbackPoint, err := ls.replayRecoveryParentPoint(producerBlock)
			if err != nil {
				return nil, err
			}
			if candidate == nil ||
				producerBlock.Slot < candidate.ProducerBlock.Slot {
				candidate = &replayRecoveryCandidate{
					Input:         input,
					ProducerTx:    nil,
					ProducerBlock: producerBlock,
					RollbackPoint: rollbackPoint,
					Strategy:      "tx-blob",
				}
			}
			continue
		}
		producerBlock, err := database.BlockByHash(ls.db, producerTx.BlockHash)
		if err != nil {
			return nil, fmt.Errorf(
				"lookup producer block %x: %w",
				producerTx.BlockHash,
				err,
			)
		}
		// Intra-block dependencies should already be satisfied by the in-memory
		// overlay. A producer from the same or a later block indicates a normal
		// validation failure, not replayable local state corruption.
		if producerBlock.Slot >= validationErr.BlockPoint.Slot {
			continue
		}
		rollbackPoint, err := ls.replayRecoveryParentPoint(producerBlock)
		if err != nil {
			return nil, err
		}
		if candidate == nil ||
			producerBlock.Slot < candidate.ProducerBlock.Slot {
			candidate = &replayRecoveryCandidate{
				Input:         input,
				ProducerTx:    producerTx,
				ProducerBlock: producerBlock,
				RollbackPoint: rollbackPoint,
				Strategy:      "metadata",
			}
		}
	}
	if len(unresolvedInputs) > 0 {
		chainCandidate, err := ls.replayRecoveryCandidateFromChain(
			validationErr.BlockPoint,
			unresolvedInputs,
		)
		if err != nil {
			return nil, err
		}
		if chainCandidate != nil && (candidate == nil ||
			chainCandidate.ProducerBlock.Slot < candidate.ProducerBlock.Slot) {
			candidate = chainCandidate
		}
	}
	if candidate != nil {
		return candidate, nil
	}
	return ls.replayRecoveryFallbackCandidate(
		validationErr.BlockPoint,
		unresolvedInputs,
	)
}

func (ls *LedgerState) replayRecoveryCandidateFromChain(
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
			"lookup failing block %x at slot %d for replay recovery: %w",
			failingPoint.Hash,
			failingPoint.Slot,
			err,
		)
	}
	if failingBlock.ID <= database.BlockInitialIndex {
		return nil, nil
	}
	wantedInputs := make(map[string]lcommon.TransactionInput, len(inputs))
	for _, input := range inputs {
		wantedInputs[string(input.Id().Bytes())] = input
	}
	var candidate *replayRecoveryCandidate
	const maxReplayRecoveryScanBlocks = 4096
	scanned := 0
	for blockIndex := failingBlock.ID - 1; ; blockIndex-- {
		if scanned >= maxReplayRecoveryScanBlocks || len(wantedInputs) == 0 {
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
			input, ok := wantedInputs[string(tx.Hash().Bytes())]
			if !ok {
				continue
			}
			rollbackPoint, err := ls.replayRecoveryParentPoint(block)
			if err != nil {
				return nil, err
			}
			if candidate == nil || block.Slot < candidate.ProducerBlock.Slot {
				candidate = &replayRecoveryCandidate{
					Input:         input,
					ProducerBlock: block,
					RollbackPoint: rollbackPoint,
					Strategy:      "chain-scan",
				}
			}
			delete(wantedInputs, string(tx.Hash().Bytes()))
		}
		scanned++
		if blockIndex == database.BlockInitialIndex {
			break
		}
	}
	return candidate, nil
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
