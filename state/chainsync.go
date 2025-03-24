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

package state

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	blockfetchBatchSize          = 500
	blockfetchBatchSlotThreshold = 2500 * 20 // TODO: calculate from protocol params

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
	ls.Lock()
	defer ls.Unlock()
	return ls.rollback(e.Point)
}

func (ls *LedgerState) handleEventChainsyncBlockHeader(e ChainsyncEvent) error {
	// Check for out-of-order block headers
	// This is a stop-gap to handle disconnects during sync until we get chain selection working
	ls.chainsyncHeaderPointsMutex.Lock()
	if ls.chainsyncHeaderPoints != nil {
		if pointsLen := len(ls.chainsyncHeaderPoints); pointsLen > 0 &&
			e.Point.Slot < ls.chainsyncHeaderPoints[pointsLen-1].Slot {
			tmpHeaderPoints := make([]ocommon.Point, 0, pointsLen)
			for _, point := range ls.chainsyncHeaderPoints {
				if point.Slot >= e.Point.Slot {
					break
				}
				tmpHeaderPoints = append(tmpHeaderPoints, point)
			}
			ls.chainsyncHeaderPoints = tmpHeaderPoints
		}
	}
	ls.chainsyncHeaderPointsMutex.Unlock()
	// Wait for current blockfetch to finish if we've already got another batch worth queued up
	// This prevents us exceeding the configured recv queue size in the block-fetch protocol
	if len(ls.chainsyncHeaderPoints) >= blockfetchBatchSize {
		// Lock and immediately unlock to pause for current blockfetch process without blocking
		// anything else. This is clunky, but the alternative is a waiting on a channel, which
		// adds unnecessary complexity
		ls.chainsyncHeaderMutex.Lock()
		//nolint:staticcheck
		ls.chainsyncHeaderMutex.Unlock()
	}
	// Add to cached header points
	ls.chainsyncHeaderPointsMutex.Lock()
	defer ls.chainsyncHeaderPointsMutex.Unlock()
	ls.chainsyncHeaderPoints = append(
		ls.chainsyncHeaderPoints,
		e.Point,
	)
	// Wait for additional block headers before fetching block bodies if we're
	// far enough out from tip
	if e.Point.Slot < e.Tip.Point.Slot &&
		(e.Tip.Point.Slot-e.Point.Slot > blockfetchBatchSlotThreshold) &&
		len(ls.chainsyncHeaderPoints) < blockfetchBatchSize {
		return nil
	}
	// Don't start fetch if there's already one in progress
	if ls.chainsyncBlockfetchBusy {
		// Clear busy flag on timeout
		if time.Since(ls.chainsyncBlockfetchBusyTime) > blockfetchBusyTimeout {
			ls.chainsyncBlockfetchBusy = false
			ls.chainsyncBlockfetchWaiting = false
			ls.config.Logger.Warn(
				fmt.Sprintf(
					"blockfetch operation timed out after %s",
					blockfetchBusyTimeout,
				),
				"component",
				"ledger",
			)
			// Unblock new chainsync block headers
			ls.chainsyncHeaderMutex.Unlock()
			return nil
		}
		ls.chainsyncBlockfetchWaiting = true
		return nil
	}
	// Block new chainsync block headers until we fetch pending block bodies
	ls.chainsyncHeaderMutex.Lock()
	// Request current bulk range
	err := ls.config.BlockfetchRequestRangeFunc(
		e.ConnectionId,
		ls.chainsyncHeaderPoints[0],
		ls.chainsyncHeaderPoints[len(ls.chainsyncHeaderPoints)-1],
	)
	if err != nil {
		// Unblock chainsync block headers
		ls.chainsyncHeaderMutex.Unlock()
		return err
	}
	ls.chainsyncBlockfetchBusy = true
	ls.chainsyncBlockfetchBusyTime = time.Now()
	// Reset cached header points
	ls.chainsyncHeaderPoints = nil
	return nil
}

//nolint:unparam
func (ls *LedgerState) handleEventBlockfetchBlock(e BlockfetchEvent) error {
	// Check for out-of-order block events
	// This is a stop-gap to handle disconnects during sync until we get chain selection working
	if ls.chainsyncBlockEvents != nil {
		if eventsLen := len(ls.chainsyncBlockEvents); eventsLen > 0 &&
			e.Point.Slot < ls.chainsyncBlockEvents[eventsLen-1].Point.Slot {
			tmpBlockEvents := make([]BlockfetchEvent, 0, eventsLen)
			for _, tmpEvent := range ls.chainsyncBlockEvents {
				if tmpEvent.Point.Slot >= e.Point.Slot {
					break
				}
				tmpBlockEvents = append(tmpBlockEvents, tmpEvent)
			}
			ls.chainsyncBlockEvents = tmpBlockEvents
		}
	}
	ls.chainsyncBlockEvents = append(
		ls.chainsyncBlockEvents,
		e,
	)
	// Update busy time in order to detect fetch timeout
	ls.chainsyncBlockfetchBusyTime = time.Now()
	return nil
}

func (ls *LedgerState) processBlockEvents() error {
	// XXX: move this into the loop?
	ls.Lock()
	defer ls.Unlock()
	batchOffset := 0
	for {
		batchSize := min(
			10, // Chosen to stay well under badger transaction size limit
			len(ls.chainsyncBlockEvents)-batchOffset,
		)
		if batchSize <= 0 {
			break
		}
		// Start a transaction
		txn := ls.db.Transaction(true)
		err := txn.Do(func(txn *database.Txn) error {
			for _, evt := range ls.chainsyncBlockEvents[batchOffset : batchOffset+batchSize] {
				if err := ls.processBlockEvent(txn, evt); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		batchOffset += batchSize
	}
	ls.chainsyncBlockEvents = nil
	ls.config.Logger.Info(
		fmt.Sprintf(
			"chain extended, new tip: %x at slot %d",
			ls.currentTip.Point.Hash,
			ls.currentTip.Point.Slot,
		),
		"component",
		"ledger",
	)
	return nil
}

func (ls *LedgerState) vacuumMetadata() error {
	ls.config.Logger.Debug(
		"vacuuming metadata database",
		"epoch", fmt.Sprintf("%+v", ls.currentEpoch),
		"component", "ledger",
	)
	// Start a transaction
	txn := ls.db.Transaction(true)
	if result := txn.Metadata().Raw("VACUUM"); result.Error != nil {
		return result.Error
	}
	return nil
}

func (ls *LedgerState) validateBlock(e BlockfetchEvent) error {
	if ls.currentTip.BlockNumber > 0 {
		prevHashBytes, err := hex.DecodeString(e.Block.PrevHash())
		if err != nil {
			return err
		}
		if string(prevHashBytes) != string(ls.currentTip.Point.Hash) {
			return fmt.Errorf(
				"block %x (with prev hash %x) does not fit on current chain tip (%x)",
				e.Point.Hash,
				prevHashBytes,
				ls.currentTip.Point.Hash,
			)
		}
	}
	return nil
}

func (ls *LedgerState) processGenesisBlock(
	txn *database.Txn,
	point ocommon.Point,
	block ledger.Block,
) error {
	if ls.currentEpoch.ID == 0 {
		// Check for era change
		if uint(block.Era().Id) != ls.currentEra.Id {
			targetEraId := uint(block.Era().Id)
			// Transition through every era between the current and the target era
			for nextEraId := ls.currentEra.Id + 1; nextEraId <= targetEraId; nextEraId++ {
				if err := ls.transitionToEra(txn, nextEraId, ls.currentEpoch.EpochId, point.Slot); err != nil {
					return err
				}
			}
		}
		// Create initial epoch record
		epochSlotLength, epochLength, err := ls.currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return err
		}
		// Use Shelley genesis hash for initial epoch nonce for post-Byron eras
		var tmpNonce []byte
		if ls.currentEra.Id > 0 { // Byron
			genesisHashBytes, _ := hex.DecodeString(
				ls.config.CardanoNodeConfig.ShelleyGenesisHash,
			)
			tmpNonce = genesisHashBytes
		}
		err = txn.DB().Metadata().SetEpoch(
			0,
			0,
			tmpNonce,
			ls.currentEra.Id,
			epochSlotLength,
			epochLength,
			txn.Metadata(),
		)
		if err != nil {
			return err
		}
		newEpoch, err := txn.DB().GetEpochLatest(txn)
		if err != nil {
			return err
		}
		ls.currentEpoch = newEpoch
		ls.config.Logger.Debug(
			"added initial epoch to DB",
			"epoch", fmt.Sprintf("%+v", newEpoch),
			"component", "ledger",
		)
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
			tmpUtxo := models.Utxo{
				TxId:       utxo.Id.Id().Bytes(),
				OutputIdx:  utxo.Id.Index(),
				AddedSlot:  0,
				PaymentKey: outAddr.PaymentKeyHash().Bytes(),
				StakingKey: outAddr.StakeKeyHash().Bytes(),
				Cbor:       outputCbor,
			}
			if err := ls.addUtxo(txn, tmpUtxo); err != nil {
				return fmt.Errorf("add genesis UTxO: %w", err)
			}
		}
	}
	return nil
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
	block ledger.Block,
) error {
	// Check for epoch rollover
	if point.Slot > ls.currentEpoch.StartSlot+uint64(
		ls.currentEpoch.LengthInSlots,
	) {
		// Apply pending pparam updates
		if err := ls.applyPParamUpdates(txn, ls.currentEpoch.EpochId, point.Slot); err != nil {
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
		err = txn.DB().Metadata().SetEpoch(
			epochStartSlot,
			ls.currentEpoch.EpochId+1,
			tmpNonce,
			uint(block.Era().Id),
			epochSlotLength,
			epochLength,
			txn.Metadata(),
		)
		if err != nil {
			return err
		}
		newEpoch, err := txn.DB().GetEpochLatest(txn)
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
		if err := ls.vacuumMetadata(); err != nil {
			ls.config.Logger.Error(
				"failed to vacuum database",
				"epoch", fmt.Sprintf("%+v", newEpoch),
				"component", "ledger",
			)
		}
	}
	return nil
}

func (ls *LedgerState) processBlockEvent(
	txn *database.Txn,
	e BlockfetchEvent,
) error {
	// Check that the block fits on our current chain
	if err := ls.validateBlock(e); err != nil {
		return err
	}
	// Special handling for genesis block
	if err := ls.processGenesisBlock(txn, e.Point, e.Block); err != nil {
		return err
	}
	// Check for epoch rollover
	if err := ls.processEpochRollover(txn, e.Point, e.Block); err != nil {
		return err
	}
	// TODO: track this using protocol params and hard forks
	// Check for era change
	if uint(e.Block.Era().Id) != ls.currentEra.Id {
		targetEraId := uint(e.Block.Era().Id)
		// Transition through every era between the current and the target era
		for nextEraId := ls.currentEra.Id + 1; nextEraId <= targetEraId; nextEraId++ {
			if err := ls.transitionToEra(txn, nextEraId, ls.currentEpoch.EpochId, e.Point.Slot); err != nil {
				return err
			}
		}
	}
	// Calculate block rolling nonce
	var blockNonce []byte
	if ls.currentEra.CalculateEtaVFunc != nil {
		tmpNonce, err := ls.currentEra.CalculateEtaVFunc(
			ls.config.CardanoNodeConfig,
			ls.currentTipBlockNonce,
			e.Block,
		)
		if err != nil {
			return err
		}
		blockNonce = tmpNonce
	}
	// Add block to database
	prevHashBytes, err := hex.DecodeString(e.Block.PrevHash())
	if err != nil {
		return err
	}
	tmpBlock := database.Block{
		Slot: e.Point.Slot,
		Hash: e.Point.Hash,
		// TODO: figure out something for Byron. this won't work, since the
		// block number isn't stored in the block itself
		Number:   e.Block.BlockNumber(),
		Type:     e.Type,
		PrevHash: prevHashBytes,
		Nonce:    blockNonce,
		Cbor:     e.Block.Cbor(),
	}
	if err := ls.addBlock(txn, tmpBlock); err != nil {
		return fmt.Errorf("add block: %w", err)
	}
	// Process transactions
	for _, tx := range e.Block.Transactions() {
		if err := ls.processTransaction(txn, tx, e.Point); err != nil {
			return err
		}
	}
	// Generate event
	ls.config.EventBus.Publish(
		ChainBlockEventType,
		event.NewEvent(
			ChainBlockEventType,
			ChainBlockEvent{
				Point: e.Point,
				Block: tmpBlock,
			},
		),
	)
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
				"TX " + tx.Hash() + " failed validation: " + err.Error(),
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
		tmpUtxo := models.Utxo{
			TxId:       produced.Id.Id().Bytes(),
			OutputIdx:  produced.Id.Index(),
			AddedSlot:  point.Slot,
			PaymentKey: outAddr.PaymentKeyHash().Bytes(),
			StakingKey: outAddr.StakeKeyHash().Bytes(),
			Cbor:       produced.Output.Cbor(),
		}
		if err := ls.addUtxo(txn, tmpUtxo); err != nil {
			return fmt.Errorf("add produced UTxO: %w", err)
		}
	}
	// XXX: generate event for each TX/UTxO?
	// Protocol parameter updates
	if updateEpoch, paramUpdates := tx.ProtocolParameterUpdates(); updateEpoch > 0 {
		for genesisHash, update := range paramUpdates {
			err := txn.DB().Metadata().SetPParamUpdate(
				genesisHash.Bytes(),
				update.Cbor(),
				point.Slot,
				updateEpoch,
				txn.Metadata(),
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

func (ls *LedgerState) handleEventBlockfetchBatchDone(e BlockfetchEvent) error {
	// Process pending block events
	if err := ls.processBlockEvents(); err != nil {
		ls.chainsyncBlockfetchBusy = false
		ls.chainsyncBlockfetchWaiting = false
		ls.chainsyncHeaderMutex.Unlock()
		return err
	}
	ls.chainsyncHeaderPointsMutex.Lock()
	defer ls.chainsyncHeaderPointsMutex.Unlock()
	// Check for pending block range request
	if !ls.chainsyncBlockfetchWaiting ||
		len(ls.chainsyncHeaderPoints) == 0 {
		ls.chainsyncBlockfetchBusy = false
		ls.chainsyncBlockfetchWaiting = false
		// Allow collection of more block headers via chainsync
		ls.chainsyncHeaderMutex.Unlock()
		return nil
	}
	// Request waiting bulk range
	err := ls.config.BlockfetchRequestRangeFunc(
		e.ConnectionId,
		ls.chainsyncHeaderPoints[0],
		ls.chainsyncHeaderPoints[len(ls.chainsyncHeaderPoints)-1],
	)
	if err != nil {
		ls.chainsyncBlockfetchBusy = false
		ls.chainsyncBlockfetchWaiting = false
		ls.chainsyncHeaderMutex.Unlock()
		return err
	}
	ls.chainsyncBlockfetchBusyTime = time.Now()
	ls.chainsyncBlockfetchWaiting = false
	// Reset cached header points
	ls.chainsyncHeaderPoints = nil
	return nil
}
