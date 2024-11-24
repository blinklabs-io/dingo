// Copyright 2024 Blink Labs Software
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
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/state/models"

	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	blockfetchBatchSize          = 500
	blockfetchBatchSlotThreshold = 2500 * 20 // TODO: calculate from protocol params

	// Timeout for updates on a blockfetch operation. This is based on a 2s BatchStart
	// and a 2s Block timeout for blockfetch
	blockfetchBusyTimeout = 5 * time.Second
)

type chainsyncState struct {
	sync.Mutex
	headerPoints       []ocommon.Point
	blockEvents        []BlockfetchEvent
	blockfetchBusy     bool
	blockfetchBusyTime time.Time
	blockfetchWaiting  bool
	// Temporary holding space for block batch objects
	tmpBlocks        []models.Block
	tmpProducedUtxos []models.Utxo
	tmpConsumedUtxos []ledger.TransactionInput
	tmpBlobKeys      map[string][]byte
}

func (ls *LedgerState) handleEventChainsync(evt event.Event) {
	ls.chainsyncState.Lock()
	defer ls.chainsyncState.Unlock()
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
	ls.chainsyncState.Lock()
	defer ls.chainsyncState.Unlock()
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
	// Add to cached header points
	ls.chainsyncState.headerPoints = append(
		ls.chainsyncState.headerPoints,
		e.Point,
	)
	// Wait for additional block headers before fetching block bodies if we're
	// far enough out from tip
	if e.Point.Slot < e.Tip.Point.Slot &&
		(e.Tip.Point.Slot-e.Point.Slot > blockfetchBatchSlotThreshold) &&
		len(ls.chainsyncState.headerPoints) < blockfetchBatchSize {
		return nil
	}
	// Don't start fetch if there's already one in progress
	if ls.chainsyncState.blockfetchBusy {
		// Clear busy flag on timeout
		if time.Since(ls.chainsyncState.blockfetchBusyTime) > blockfetchBusyTimeout {
			ls.chainsyncState.blockfetchBusy = false
			ls.chainsyncState.blockfetchWaiting = false
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
		ls.chainsyncState.blockfetchWaiting = true
		return nil
	}
	// Request current bulk range
	err := ls.config.BlockfetchRequestRangeFunc(
		e.ConnectionId,
		ls.chainsyncState.headerPoints[0],
		ls.chainsyncState.headerPoints[len(ls.chainsyncState.headerPoints)-1],
	)
	if err != nil {
		return err
	}
	ls.chainsyncState.blockfetchBusy = true
	ls.chainsyncState.blockfetchBusyTime = time.Now()
	// Reset cached header points
	ls.chainsyncState.headerPoints = nil
	return nil
}

func (ls *LedgerState) handleEventBlockfetchBlock(e BlockfetchEvent) error {
	ls.chainsyncState.blockEvents = append(
		ls.chainsyncState.blockEvents,
		e,
	)
	// Update busy time in order to detect fetch timeout
	ls.chainsyncState.blockfetchBusyTime = time.Now()
	return nil
}

func (ls *LedgerState) processBlockEvents() error {
	// XXX: move this into the loop?
	ls.Lock()
	defer ls.Unlock()
	// TODO: remove batching here
	batchOffset := 0
	for {
		batchSize := min(
			10, // Chosen to stay well under badger transaction size limit
			len(ls.chainsyncState.blockEvents)-batchOffset,
		)
		if batchSize <= 0 {
			break
		}
		// Start a transaction
		txn := ls.db.Transaction(true)
		err := txn.Do(func(txn *database.Txn) error {
			for _, evt := range ls.chainsyncState.blockEvents[batchOffset : batchOffset+batchSize] {
				//tmpStart := time.Now()
				if err := ls.processBlockEvent(txn, evt); err != nil {
					return err
				}
				/*
					tmpDiff := time.Since(tmpStart)
					if tmpDiff >= 10*time.Millisecond {
						fmt.Printf("block %x took %s to process\n", evt.Point.Hash, tmpDiff)
					}
				*/
			}
			return nil
		})
		if err != nil {
			return err
		}
		batchOffset += batchSize
	}
	// Write batched data to DBs
	if err := ls.flushBlockData(); err != nil {
		return err
	}
	ls.chainsyncState.blockEvents = nil
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

func (ls *LedgerState) flushBlockData() error {
	lastBlock := ls.chainsyncState.tmpBlocks[len(ls.chainsyncState.tmpBlocks)-1]
	// Write blob keys
	// This has to be done separately from metadata entries due to transaction
	// size limits
	wb := ls.db.Blob().NewWriteBatch()
	for k, v := range ls.chainsyncState.tmpBlobKeys {
		if err := wb.Set([]byte(k), v); err != nil {
			return err
		}
	}
	if err := wb.Flush(); err != nil {
		return err
	}
	// Write blocks, produced UTxOs, and consumed inputs
	txn := ls.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		// Blocks
		if len(ls.chainsyncState.tmpBlocks) > 0 {
			if result := txn.Metadata().Create(&ls.chainsyncState.tmpBlocks); result.Error != nil {
				return result.Error
			}
		}
		// Produced UTxOs
		if len(ls.chainsyncState.tmpProducedUtxos) > 0 {
			if result := txn.Metadata().Create(&ls.chainsyncState.tmpProducedUtxos); result.Error != nil {
				return result.Error
			}
		}
		// Consumed UTxOs
		if len(ls.chainsyncState.tmpConsumedUtxos) > 0 {
			err := models.UtxosConsumeByRefTxn(
				txn,
				ls.chainsyncState.tmpConsumedUtxos,
				// Use slot from last block in batch
				ls.chainsyncState.tmpBlocks[len(ls.chainsyncState.tmpBlocks)-1].Slot,
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Update tip
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(lastBlock.Slot, lastBlock.Hash),
		BlockNumber: lastBlock.Number,
	}
	// Update metrics
	ls.metrics.blockNum.Set(float64(lastBlock.Number))
	ls.metrics.slotNum.Set(float64(lastBlock.Slot))
	ls.metrics.slotInEpoch.Set(float64(lastBlock.Slot - ls.currentEpoch.StartSlot))
	// Reset buffers
	ls.chainsyncState.tmpBlocks = nil
	ls.chainsyncState.tmpProducedUtxos = nil
	ls.chainsyncState.tmpConsumedUtxos = nil
	return nil
}

func (ls *LedgerState) processBlockEvent(
	txn *database.Txn,
	e BlockfetchEvent,
) error {
	tmpBlock := models.Block{
		Slot: e.Point.Slot,
		Hash: e.Point.Hash,
		// TODO: figure out something for Byron. this won't work, since the
		// block number isn't stored in the block itself
		Number: e.Block.BlockNumber(),
		Type:   e.Type,
		Cbor:   e.Block.Cbor(),
	}
	// Special handling for genesis block
	if ls.currentEpoch.ID == 0 {
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
		// Create initial epoch record
		epochSlotLength, epochLength, err := ls.currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return err
		}
		newEpoch := models.Epoch{
			EpochId:       0,
			EraId:         ls.currentEra.Id,
			StartSlot:     0,
			SlotLength:    epochSlotLength,
			LengthInSlots: epochLength,
		}
		if result := txn.Metadata().Create(&newEpoch); result.Error != nil {
			return result.Error
		}
		ls.currentEpoch = newEpoch
		ls.config.Logger.Debug(
			"added initial epoch to DB",
			"epoch", fmt.Sprintf("%+v", newEpoch),
			"component", "ledger",
		)
	}
	// Check for epoch rollover
	if e.Point.Slot > ls.currentEpoch.StartSlot+uint64(
		ls.currentEpoch.LengthInSlots,
	) {
		// Apply pending pparam updates
		if err := ls.applyPParamUpdates(txn, ls.currentEpoch.EpochId, e.Point.Slot); err != nil {
			return err
		}
		// Create next epoch record
		epochSlotLength, epochLength, err := ls.currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return err
		}
		newEpoch := models.Epoch{
			EpochId:       ls.currentEpoch.EpochId + 1,
			EraId:         uint(e.Block.Era().Id),
			SlotLength:    epochSlotLength,
			LengthInSlots: epochLength,
			StartSlot: ls.currentEpoch.StartSlot + uint64(
				ls.currentEpoch.LengthInSlots,
			),
		}
		if result := txn.Metadata().Create(&newEpoch); result.Error != nil {
			return result.Error
		}
		ls.currentEpoch = newEpoch
		ls.metrics.epochNum.Set(float64(newEpoch.EpochId))
		ls.config.Logger.Debug(
			"added next epoch to DB",
			"epoch", fmt.Sprintf("%+v", newEpoch),
			"component", "ledger",
		)
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
	// Add block to batch
	ls.chainsyncState.tmpBlocks = append(
		ls.chainsyncState.tmpBlocks,
		tmpBlock,
	)
	blobKeyBlock := models.BlockBlobKey(e.Point.Slot, e.Point.Hash)
	if ls.chainsyncState.tmpBlobKeys == nil {
		ls.chainsyncState.tmpBlobKeys = make(map[string][]byte)
	}
	ls.chainsyncState.tmpBlobKeys[string(blobKeyBlock)] = tmpBlock.Cbor
	/*
		if err := ls.addBlock(txn, tmpBlock); err != nil {
			return fmt.Errorf("add block: %w", err)
		}
	*/
	// Process transactions
	for _, tx := range e.Block.Transactions() {
		//tmpStart := time.Now()
		// Process consumed UTxOs
		ls.chainsyncState.tmpConsumedUtxos = append(
			ls.chainsyncState.tmpConsumedUtxos,
			tx.Consumed()...,
		)
		/*
			if err := models.UtxosConsumeByRefTxn(txn, tx.Consumed(), e.Point.Slot); err != nil {
				return err
			}
		*/
		/*
			for _, consumed := range tx.Consumed() {
				if err := ls.consumeUtxo(txn, consumed, e.Point.Slot); err != nil {
					return fmt.Errorf("remove consumed UTxO: %w", err)
				}
			}
		*/
		// Process produced UTxOs
		for _, produced := range tx.Produced() {
			outAddr := produced.Output.Address()
			tmpUtxo := models.Utxo{
				TxId:       produced.Id.Id().Bytes(),
				OutputIdx:  produced.Id.Index(),
				AddedSlot:  e.Point.Slot,
				PaymentKey: outAddr.PaymentKeyHash().Bytes(),
				StakingKey: outAddr.StakeKeyHash().Bytes(),
				Cbor:       produced.Output.Cbor(),
			}
			ls.chainsyncState.tmpProducedUtxos = append(
				ls.chainsyncState.tmpProducedUtxos,
				tmpUtxo,
			)
			blobKeyUtxo := models.UtxoBlobKey(produced.Id.Id().Bytes(), produced.Id.Index())
			ls.chainsyncState.tmpBlobKeys[string(blobKeyUtxo)] = tmpUtxo.Cbor
			/*
				if err := ls.addUtxo(txn, tmpUtxo); err != nil {
					return fmt.Errorf("add produced UTxO: %w", err)
				}
			*/
		}
		/*
			if err := models.UtxosCreate(txn, producedUtxos); err != nil {
				return err
			}
		*/
		// XXX: generate event for each TX/UTxO?
		// Protocol parameter updates
		if updateEpoch, paramUpdates := tx.ProtocolParameterUpdates(); updateEpoch > 0 {
			for genesisHash, update := range paramUpdates {
				tmpUpdate := models.PParamUpdate{
					AddedSlot:   e.Point.Slot,
					Epoch:       updateEpoch,
					GenesisHash: genesisHash.Bytes(),
					Cbor:        update.Cbor(),
				}
				if result := txn.Metadata().Create(&tmpUpdate); result.Error != nil {
					return result.Error
				}
			}
		}
		// Certificates
		if err := ls.processTransactionCertificates(txn, e.Point, tx); err != nil {
			return err
		}
		/*
			tmpDiff := time.Since(tmpStart)
			if tmpDiff > 1*time.Millisecond {
				fmt.Printf("TX %s took %s\n", tx.Hash(), tmpDiff)
			}
		*/
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

func (ls *LedgerState) handleEventBlockfetchBatchDone(e BlockfetchEvent) error {
	// Process pending block events
	if err := ls.processBlockEvents(); err != nil {
		return err
	}
	// Check for pending block range request
	if !ls.chainsyncState.blockfetchWaiting ||
		len(ls.chainsyncState.headerPoints) == 0 {
		ls.chainsyncState.blockfetchBusy = false
		ls.chainsyncState.blockfetchWaiting = false
		return nil
	}
	// Request waiting bulk range
	err := ls.config.BlockfetchRequestRangeFunc(
		e.ConnectionId,
		ls.chainsyncState.headerPoints[0],
		ls.chainsyncState.headerPoints[len(ls.chainsyncState.headerPoints)-1],
	)
	if err != nil {
		return err
	}
	ls.chainsyncState.blockfetchBusyTime = time.Now()
	ls.chainsyncState.blockfetchWaiting = false
	// Reset cached header points
	ls.chainsyncState.headerPoints = nil
	return nil
}
