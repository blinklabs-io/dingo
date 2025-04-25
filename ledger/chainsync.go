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

package ledger

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// Max number of blocks to fetch in a single blockfetch call
	// This prevents us exceeding the configured recv queue size in the block-fetch protocol
	blockfetchBatchSize = 500

	// TODO: calculate from protocol params
	// Number of slots from upstream tip to stop doing blockfetch batches
	blockfetchBatchSlotThreshold = 2500 * 20

	// Timeout for updates on a blockfetch operation. This is based on a 2s BatchStart
	// and a 2s Block timeout for blockfetch
	blockfetchBusyTimeout = 5 * time.Second
)

func (ls *LedgerState) handleEventChainsync(evt event.Event) {
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	e := evt.Data.(ChainsyncEvent)
	if e.Rollback {
		if err := ls.handleEventChainsyncRollback(e); err != nil {
			// TODO: actually handle this error
			ls.config.Logger.Error(
				"failed to handle rollback",
				"component", "ledger",
				"error", err,
			)
			return
		}
	} else if e.BlockHeader != nil {
		if err := ls.handleEventChainsyncBlockHeader(e); err != nil {
			// TODO: actually handle this error
			ls.config.Logger.Error(
				fmt.Sprintf("ledger: failed to handle block header: %s", err),
			)
			return
		}
	}
}

func (ls *LedgerState) handleEventBlockfetch(evt event.Event) {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	e := evt.Data.(BlockfetchEvent)
	if e.BatchDone {
		if err := ls.handleEventBlockfetchBatchDone(e); err != nil {
			// TODO: actually handle this error
			ls.config.Logger.Error(
				fmt.Sprintf(
					"ledger: failed to handle blockfetch batch done: %s",
					err,
				),
			)
		}
	} else if e.Block != nil {
		if err := ls.handleEventBlockfetchBlock(e); err != nil {
			// TODO: actually handle this error
			ls.config.Logger.Error(
				fmt.Sprintf("ledger: failed to handle block: %s", err),
			)
		}
	}
}

func (ls *LedgerState) handleEventChainsyncRollback(e ChainsyncEvent) error {
	if err := ls.chain.Rollback(e.Point); err != nil {
		return err
	}
	return nil
}

func (ls *LedgerState) handleEventChainsyncBlockHeader(e ChainsyncEvent) error {
	// Allow us to build up a few blockfetch batches worth of headers
	allowedHeaderCount := blockfetchBatchSize * 4
	headerCount := ls.chain.HeaderCount()
	// Wait for current blockfetch batch to finish before we collect more block headers
	if headerCount >= allowedHeaderCount {
		// We assign the channel to a temp var to protect against trying to read from a nil channel
		// without a race condition
		tmpDoneChan := ls.chainsyncBlockfetchDoneChan
		if tmpDoneChan != nil {
			<-tmpDoneChan
		}
	}
	// Add header to chain
	if err := ls.chain.AddBlockHeader(e.BlockHeader); err != nil {
		if !errors.As(err, &chain.BlockNotFitChainTipError{}) {
			return err
		}
		ls.config.Logger.Warn(
			fmt.Sprintf(
				"ignoring chainsync block header: %s",
				err,
			),
		)
		return nil
	}
	// Wait for additional block headers before fetching block bodies if we're
	// far enough out from upstream tip
	if e.Point.Slot < e.Tip.Point.Slot &&
		(e.Tip.Point.Slot-e.Point.Slot > blockfetchBatchSlotThreshold) &&
		(headerCount+1) < allowedHeaderCount {
		return nil
	}
	// We use the blockfetch lock to ensure we aren't starting a batch at the same
	// time as blockfetch starts a new one to avoid deadlocks
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	// Don't start fetch if there's already one in progress
	if ls.chainsyncBlockfetchDoneChan != nil {
		// Clear blockfetch busy flag on timeout
		if time.Since(ls.chainsyncBlockfetchBusyTime) > blockfetchBusyTimeout {
			ls.blockfetchRequestRangeCleanup(true)
			ls.config.Logger.Warn(
				fmt.Sprintf(
					"blockfetch operation timed out after %s",
					blockfetchBusyTimeout,
				),
				"component",
				"ledger",
			)
			return nil
		}
		ls.chainsyncBlockfetchWaiting = true
		return nil
	}
	// Request next bulk range
	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	err := ls.blockfetchRequestRangeStart(
		e.ConnectionId,
		headerStart,
		headerEnd,
	)
	if err != nil {
		ls.blockfetchRequestRangeCleanup(true)
		return err
	}
	ls.chainsyncBlockfetchBusyTime = time.Now()
	return nil
}

//nolint:unparam
func (ls *LedgerState) handleEventBlockfetchBlock(e BlockfetchEvent) error {
	ls.chainsyncBlockEvents = append(
		ls.chainsyncBlockEvents,
		e,
	)
	// Update busy time in order to detect fetch timeout
	ls.chainsyncBlockfetchBusyTime = time.Now()
	return nil
}

func (ls *LedgerState) processBlockEvents() error {
	batchOffset := 0
	for {
		batchSize := min(
			10, // Chosen to stay well under badger transaction size limit
			len(ls.chainsyncBlockEvents)-batchOffset,
		)
		if batchSize <= 0 {
			break
		}
		ls.Lock()
		// Start a transaction
		txn := ls.db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			for _, evt := range ls.chainsyncBlockEvents[batchOffset : batchOffset+batchSize] {
				if err := ls.processBlockEvent(txn, evt); err != nil {
					return err
				}
			}
			return nil
		})
		ls.Unlock()
		if err != nil {
			return err
		}
		batchOffset += batchSize
	}
	ls.chainsyncBlockEvents = nil
	return nil
}

func (ls *LedgerState) createGenesisBlock() error {
	if ls.currentEpoch.SlotLength > 0 {
		return nil
	}
	txn := ls.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		// Record genesis UTxOs
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		genesisUtxos, err := byronGenesis.GenesisUtxos()
		if err != nil {
			return err
		}
		for _, utxo := range genesisUtxos {
			outAddr := utxo.Output.Address()
			outputCbor, err := cbor.Encode(utxo.Output)
			if err != nil {
				return err
			}
			err = ls.db.NewUtxo(
				utxo.Id.Id().Bytes(),
				utxo.Id.Index(),
				0,
				outAddr.PaymentKeyHash().Bytes(),
				outAddr.StakeKeyHash().Bytes(),
				outputCbor,
				txn,
			)
			if err != nil {
				return fmt.Errorf("add genesis UTxO: %w", err)
			}
		}
		return nil
	})
	return err
}

func (ls *LedgerState) calculateEpochNonce(
	txn *database.Txn,
	epochStartSlot uint64,
) ([]byte, error) {
	// No epoch nonce in Byron
	if ls.currentEra.Id == 0 {
		return nil, nil
	}
	// Use Shelley genesis hash for initial epoch nonce
	if len(ls.currentEpoch.Nonce) == 0 {
		genesisHashBytes, err := hex.DecodeString(
			ls.config.CardanoNodeConfig.ShelleyGenesisHash,
		)
		return genesisHashBytes, err
	}
	// Calculate stability window
	byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if byronGenesis == nil || shelleyGenesis == nil {
		return nil, errors.New("could not get genesis config")
	}
	stabilityWindow := new(big.Rat).Quo(
		big.NewRat(
			int64(3*byronGenesis.ProtocolConsts.K),
			1,
		),
		shelleyGenesis.ActiveSlotsCoeff.Rat,
	).Num().Uint64()
	stabilityWindowStartSlot := epochStartSlot - stabilityWindow
	// Get last block before stability window
	blockBeforeStabilityWindow, err := database.BlockBeforeSlotTxn(
		txn,
		stabilityWindowStartSlot,
	)
	if err != nil {
		return nil, err
	}
	// Get last block in previous epoch
	blockLastPrevEpoch, err := database.BlockBeforeSlotTxn(
		txn,
		ls.currentEpoch.StartSlot,
	)
	if err != nil {
		if errors.Is(err, database.ErrBlockNotFound) {
			return blockBeforeStabilityWindow.Nonce, nil
		}
		return nil, err
	}
	// Calculate nonce from inputs
	ret, err := lcommon.CalculateEpochNonce(
		blockBeforeStabilityWindow.Nonce,
		blockLastPrevEpoch.PrevHash,
		nil,
	)
	return ret.Bytes(), err
}

func (ls *LedgerState) processEpochRollover(
	txn *database.Txn,
	point ocommon.Point,
) error {
	// Create initial epoch
	if ls.currentEpoch.SlotLength == 0 {
		// Create initial epoch record
		epochSlotLength, epochLength, err := ls.currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return err
		}
		tmpNonce, err := ls.calculateEpochNonce(txn, 0)
		if err != nil {
			return err
		}
		err = ls.db.SetEpoch(
			0, // start slot
			0, // epoch
			tmpNonce,
			ls.currentEra.Id,
			epochSlotLength,
			epochLength,
			txn,
		)
		if err != nil {
			return err
		}
		newEpoch, err := ls.db.GetEpochLatest(txn)
		if err != nil {
			return err
		}
		ls.currentEpoch = newEpoch
		ls.metrics.epochNum.Set(float64(newEpoch.EpochId))
		ls.config.Logger.Debug(
			"added initial epoch to DB",
			"epoch", fmt.Sprintf("%+v", newEpoch),
			"component", "ledger",
		)
	}
	// Check for epoch rollover
	if point.Slot > ls.currentEpoch.StartSlot+uint64(
		ls.currentEpoch.LengthInSlots,
	) {
		// Apply pending pparam updates
		err := ls.db.ApplyPParamUpdates(
			point.Slot,
			ls.currentEpoch.EpochId,
			ls.currentEra.Id,
			&ls.currentPParams,
			ls.currentEra.DecodePParamsUpdateFunc,
			ls.currentEra.PParamsUpdateFunc,
			txn,
		)
		if err != nil {
			return err
		}
		// Create next epoch record
		epochSlotLength, epochLength, err := ls.currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return err
		}
		epochStartSlot := ls.currentEpoch.StartSlot + uint64(
			ls.currentEpoch.LengthInSlots,
		)
		tmpNonce, err := ls.calculateEpochNonce(txn, epochStartSlot)
		if err != nil {
			return err
		}
		err = ls.db.SetEpoch(
			epochStartSlot,
			ls.currentEpoch.EpochId+1,
			tmpNonce,
			ls.currentEra.Id,
			epochSlotLength,
			epochLength,
			txn,
		)
		if err != nil {
			return err
		}
		newEpoch, err := ls.db.GetEpochLatest(txn)
		if err != nil {
			return err
		}
		ls.currentEpoch = newEpoch
		ls.metrics.epochNum.Set(float64(newEpoch.EpochId))
		ls.config.Logger.Debug(
			"added next epoch to DB",
			"epoch", fmt.Sprintf("%+v", newEpoch),
			"component", "ledger",
		)
	}
	return nil
}

func (ls *LedgerState) processBlockEvent(
	txn *database.Txn,
	e BlockfetchEvent,
) error {
	// TODO: move this to ledger block processing
	// Calculate block rolling nonce
	var blockNonce []byte
	if ls.currentEra.CalculateEtaVFunc != nil {
		tmpEra := eras.Eras[e.Block.Era().Id]
		tmpNonce, err := tmpEra.CalculateEtaVFunc(
			ls.config.CardanoNodeConfig,
			ls.currentTipBlockNonce,
			e.Block,
		)
		if err != nil {
			return err
		}
		blockNonce = tmpNonce
	}
	// Add block to chain
	if err := ls.chain.AddBlock(e.Block, blockNonce, txn); err != nil {
		// Ignore and log errors about block not fitting on chain or matching first header
		if !errors.As(err, &chain.BlockNotFitChainTipError{}) &&
			!errors.As(err, &chain.BlockNotMatchHeaderError{}) {
			return err
		}
		ls.config.Logger.Warn(
			fmt.Sprintf(
				"ignoring blockfetch block: %s",
				err,
			),
		)
	}
	return nil
}

func (ls *LedgerState) processTransaction(
	txn *database.Txn,
	tx ledger.Transaction,
	point ocommon.Point,
) error {
	// Validate transaction
	if ls.currentEra.ValidateTxFunc != nil {
		lv := &LedgerView{
			txn: txn,
			ls:  ls,
		}
		err := ls.currentEra.ValidateTxFunc(
			tx,
			point.Slot,
			lv,
			ls.currentPParams,
		)
		if err != nil {
			ls.config.Logger.Warn(
				"TX " + tx.Hash().
					String() +
					" failed validation: " + err.Error(),
			)
			// return fmt.Errorf("TX validation failure: %w", err)
		}
	}
	// Process consumed UTxOs
	for _, consumed := range tx.Consumed() {
		if err := ls.consumeUtxo(txn, consumed, point.Slot); err != nil {
			return fmt.Errorf("remove consumed UTxO: %w", err)
		}
	}
	// Process produced UTxOs
	for _, produced := range tx.Produced() {
		outAddr := produced.Output.Address()
		err := ls.db.NewUtxo(
			produced.Id.Id().Bytes(),
			produced.Id.Index(),
			point.Slot,
			outAddr.PaymentKeyHash().Bytes(),
			outAddr.StakeKeyHash().Bytes(),
			produced.Output.Cbor(),
			txn,
		)
		if err != nil {
			return fmt.Errorf("add produced UTxO: %w", err)
		}
	}
	// XXX: generate event for each TX/UTxO?
	// Protocol parameter updates
	if updateEpoch, paramUpdates := tx.ProtocolParameterUpdates(); updateEpoch > 0 {
		for genesisHash, update := range paramUpdates {
			err := ls.db.SetPParamUpdate(
				genesisHash.Bytes(),
				update.Cbor(),
				point.Slot,
				updateEpoch,
				txn,
			)
			if err != nil {
				return err
			}
		}
	}
	// Certificates
	if err := ls.processTransactionCertificates(txn, point, tx); err != nil {
		return err
	}
	return nil
}

func (ls *LedgerState) blockfetchRequestRangeStart(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	err := ls.config.BlockfetchRequestRangeFunc(
		connId,
		start,
		end,
	)
	if err != nil {
		return err
	}
	// Create our blockfetch done signal channel
	ls.chainsyncBlockfetchDoneChan = make(chan struct{})
	return nil
}

func (ls *LedgerState) blockfetchRequestRangeCleanup(resetFlags bool) {
	// Reset buffer
	ls.chainsyncBlockEvents = slices.Delete(
		ls.chainsyncBlockEvents,
		0,
		len(ls.chainsyncBlockEvents),
	)
	// Close our blockfetch done signal channel
	if ls.chainsyncBlockfetchDoneChan != nil {
		close(ls.chainsyncBlockfetchDoneChan)
		ls.chainsyncBlockfetchDoneChan = nil
	}
	// Reset flags
	if resetFlags {
		ls.chainsyncBlockfetchWaiting = false
	}
}

func (ls *LedgerState) handleEventBlockfetchBatchDone(e BlockfetchEvent) error {
	// Process pending block events
	if err := ls.processBlockEvents(); err != nil {
		ls.blockfetchRequestRangeCleanup(true)
		return err
	}
	// Check for pending block range request
	if !ls.chainsyncBlockfetchWaiting ||
		ls.chain.HeaderCount() == 0 {
		// Allow collection of more block headers via chainsync
		ls.blockfetchRequestRangeCleanup(true)
		return nil
	}
	// Clean up from blockfetch batch
	ls.blockfetchRequestRangeCleanup(false)
	// Request next waiting bulk range
	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	err := ls.blockfetchRequestRangeStart(
		e.ConnectionId,
		headerStart,
		headerEnd,
	)
	if err != nil {
		ls.blockfetchRequestRangeCleanup(true)
		return err
	}
	ls.chainsyncBlockfetchBusyTime = time.Now()
	ls.chainsyncBlockfetchWaiting = false
	return nil
}
