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
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
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
	if ls.chainsyncState == SyncingChainsyncState {
		ls.config.Logger.Warn(
			fmt.Sprintf(
				"ledger: rolling back to %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
		ls.chainsyncState = RollbackChainsyncState
	}
	if err := ls.chain.Rollback(e.Point); err != nil {
		return fmt.Errorf("chain rollback failed: %w", err)
	}
	return nil
}

func (ls *LedgerState) handleEventChainsyncBlockHeader(e ChainsyncEvent) error {
	if ls.chainsyncState == RollbackChainsyncState {
		ls.config.Logger.Info(
			fmt.Sprintf(
				"ledger: switched to fork at %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
		ls.metrics.forks.Add(1)
	}
	ls.chainsyncState = SyncingChainsyncState
	// Allow us to build up a few blockfetch batches worth of headers
	allowedHeaderCount := blockfetchBatchSize * 4
	headerCount := ls.chain.HeaderCount()
	// Wait for current blockfetch batch to finish before we collect more block headers
	if headerCount >= allowedHeaderCount {
		// We assign the channel to a temp var to protect against trying to read from a nil channel
		// without a race condition
		tmpDoneChan := ls.chainsyncBlockfetchReadyChan
		if tmpDoneChan != nil {
			<-tmpDoneChan
		}
	}
	// Add header to chain
	if err := ls.chain.AddBlockHeader(e.BlockHeader); err != nil {
		return fmt.Errorf("failed adding chain block header: %w", err)
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
	if ls.chainsyncBlockfetchReadyChan != nil {
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
					return fmt.Errorf("failed processing block event: %w", err)
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
	if ls.currentTip.Point.Slot > 0 {
		return nil
	}
	txn := ls.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		// Record genesis UTxOs
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		byronGenesisUtxos, err := byronGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Byron genesis UTxOs: %w", err)
		}
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		shelleyGenesisUtxos, err := shelleyGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Shelley genesis UTxOs: %w", err)
		}
		for _, utxo := range slices.Concat(byronGenesisUtxos, shelleyGenesisUtxos) {
			outAddr := utxo.Output.Address()
			outputCbor, err := cbor.Encode(utxo.Output)
			if err != nil {
				return fmt.Errorf("encode UTxO: %w", err)
			}
			err = ls.db.NewUtxo(
				utxo.Id.Id().Bytes(),
				utxo.Id.Index(),
				0,
				outAddr.PaymentKeyHash().Bytes(),
				outAddr.StakeKeyHash().Bytes(),
				outputCbor,
				utxo.Output.Amount(),
				txo.Output.Assets(),
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
		if err != nil {
			return nil, fmt.Errorf("decode genesis hash: %w", err)
		}
		return genesisHashBytes, nil
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
		return nil, fmt.Errorf("lookup block before slot: %w", err)
	}
	blockBeforeStabilityWindowNonce, err := ls.db.GetBlockNonce(
		blockBeforeStabilityWindow.Hash,
		blockBeforeStabilityWindow.Slot,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("lookup block nonce: %w", err)
	}
	// Get last block in previous epoch
	blockLastPrevEpoch, err := database.BlockBeforeSlotTxn(
		txn,
		ls.currentEpoch.StartSlot,
	)
	if err != nil {
		if errors.Is(err, database.ErrBlockNotFound) {
			return blockBeforeStabilityWindowNonce, nil
		}
		return nil, fmt.Errorf("lookup block before slot: %w", err)
	}
	// Calculate nonce from inputs
	ret, err := lcommon.CalculateEpochNonce(
		blockBeforeStabilityWindowNonce,
		blockLastPrevEpoch.PrevHash,
		nil,
	)
	return ret.Bytes(), err
}

func (ls *LedgerState) processEpochRollover(
	txn *database.Txn,
) error {
	epochStartSlot := ls.currentEpoch.StartSlot + uint64(
		ls.currentEpoch.LengthInSlots,
	)
	// Create initial epoch
	if ls.currentEpoch.SlotLength == 0 {
		// Create initial epoch record
		epochSlotLength, epochLength, err := ls.currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return fmt.Errorf("calculate epoch length: %w", err)
		}
		tmpNonce, err := ls.calculateEpochNonce(txn, 0)
		if err != nil {
			return fmt.Errorf("calculate epoch nonce: %w", err)
		}
		err = ls.db.SetEpoch(
			epochStartSlot,
			0, // epoch
			tmpNonce,
			ls.currentEra.Id,
			epochSlotLength,
			epochLength,
			txn,
		)
		if err != nil {
			return fmt.Errorf("set epoch: %w", err)
		}
		// Reload epoch info
		if err := ls.loadEpochs(txn); err != nil {
			return fmt.Errorf("load epochs: %w", err)
		}
		ls.checkpointWrittenForEpoch = false
		ls.config.Logger.Debug(
			"added initial epoch to DB",
			"epoch", fmt.Sprintf("%+v", ls.currentEpoch),
			"component", "ledger",
		)
		return nil
	}
	// Apply pending pparam updates
	err := ls.db.ApplyPParamUpdates(
		epochStartSlot,
		ls.currentEpoch.EpochId,
		ls.currentEra.Id,
		&ls.currentPParams,
		ls.currentEra.DecodePParamsUpdateFunc,
		ls.currentEra.PParamsUpdateFunc,
		txn,
	)
	if err != nil {
		return fmt.Errorf("apply pparam updates: %w", err)
	}
	// Create next epoch record
	epochSlotLength, epochLength, err := ls.currentEra.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	if err != nil {
		return fmt.Errorf("calculate epoch length: %w", err)
	}
	tmpNonce, err := ls.calculateEpochNonce(txn, epochStartSlot)
	if err != nil {
		return fmt.Errorf("calculate epoch nonce: %w", err)
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
		return fmt.Errorf("set epoch: %w", err)
	}
	// Reload epoch info
	if err := ls.loadEpochs(txn); err != nil {
		return fmt.Errorf("load epochs: %w", err)
	}
	ls.checkpointWrittenForEpoch = false
	// Update the scheduler interval based on the new epoch's slot length
	if ls.Scheduler != nil {
		// nolint:gosec
		// The slot length will not exceed int64
		interval := time.Duration(ls.currentEpoch.SlotLength) * time.Millisecond
		ls.Scheduler.ChangeInterval(interval)
	}
	ls.config.Logger.Debug(
		"added next epoch to DB",
		"epoch", fmt.Sprintf("%+v", ls.currentEpoch),
		"component", "ledger",
	)
	// Start background cleanup of consumed UTxOs
	go ls.cleanupConsumedUtxos()

	// Clean up old block nonces and keep only last 3 epochs along with checkpoints
	var cutoffStart uint64
	if ls.currentEpoch.EpochId >= 4 {
		target := ls.currentEpoch.EpochId - 3
		for _, ep := range ls.epochCache {
			if ep.EpochId == target {
				cutoffStart = ep.StartSlot
				break
			}
		}
	}
	if cutoffStart > 0 {
		go ls.cleanupBlockNoncesBefore(cutoffStart)
	}
	return nil
}

func (ls *LedgerState) cleanupBlockNoncesBefore(startSlot uint64) {
	if startSlot == 0 {
		return
	}
	ls.config.Logger.Debug(
		fmt.Sprintf(
			"cleaning up non-checkpoint block nonces before slot %d",
			startSlot,
		),
		"component",
		"ledger",
	)
	ls.Lock()
	defer ls.Unlock()
	txn := ls.db.Transaction(true)
	if err := txn.Do(func(txn *database.Txn) error {
		return ls.db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(startSlot, txn)
	}); err != nil {
		ls.config.Logger.Error(
			fmt.Sprintf("failed to clean up old block nonces: %s", err),
			"component", "ledger",
		)
	}
}

func (ls *LedgerState) processBlockEvent(
	txn *database.Txn,
	e BlockfetchEvent,
) error {
	// Add block to chain
	if err := ls.chain.AddBlock(e.Block, txn); err != nil {
		// Ignore and log errors about block not fitting on chain or matching first header
		if !errors.As(err, &chain.BlockNotFitChainTipError{}) &&
			!errors.As(err, &chain.BlockNotMatchHeaderError{}) {
			return fmt.Errorf("add chain block: %w", err)
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
		return fmt.Errorf("request block range: %w", err)
	}
	// Reset blockfetch busy time
	ls.chainsyncBlockfetchBusyTime = time.Now()
	// Create our blockfetch done signal channels
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chainsyncBlockfetchBatchDoneChan = make(chan struct{})
	// Start goroutine to handle blockfetch timeout
	go func() {
		for {
			select {
			case <-ls.chainsyncBlockfetchBatchDoneChan:
				return
			case <-time.After(500 * time.Millisecond):
			}
			// Clear blockfetch busy flag on timeout
			if time.Since(
				ls.chainsyncBlockfetchBusyTime,
			) > blockfetchBusyTimeout {
				ls.blockfetchRequestRangeCleanup(true)
				ls.config.Logger.Warn(
					fmt.Sprintf(
						"blockfetch operation timed out after %s",
						blockfetchBusyTimeout,
					),
					"component",
					"ledger",
				)
				return
			}
		}
	}()
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
	if ls.chainsyncBlockfetchReadyChan != nil {
		close(ls.chainsyncBlockfetchReadyChan)
		ls.chainsyncBlockfetchReadyChan = nil
	}
	// Reset flags
	if resetFlags {
		ls.chainsyncBlockfetchWaiting = false
	}
}

func (ls *LedgerState) handleEventBlockfetchBatchDone(e BlockfetchEvent) error {
	// Cancel our blockfetch timeout watcher
	if ls.chainsyncBlockfetchBatchDoneChan != nil {
		close(ls.chainsyncBlockfetchBatchDoneChan)
	}
	// Process pending block events
	if err := ls.processBlockEvents(); err != nil {
		ls.blockfetchRequestRangeCleanup(true)
		return fmt.Errorf("process block events: %w", err)
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
	ls.chainsyncBlockfetchWaiting = false
	return nil
}
