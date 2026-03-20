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
	ls.config.Logger.Warn(
		"detected inconsistent local ledger state during replay, rewinding metadata state",
		"component", "ledger",
		"tx_hash", hex.EncodeToString(validationErr.TxHash),
		"failing_block_slot", validationErr.BlockPoint.Slot,
		"missing_input", candidate.Input.String(),
		"producer_tx_hash", hex.EncodeToString(candidate.ProducerTx.Hash),
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
			}
		}
	}
	return candidate, nil
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
