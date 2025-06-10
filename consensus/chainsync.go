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

package consensus

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
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

func (c *Consensus) handleEventChainsync(evt event.Event) {
	c.chainsyncMutex.Lock()
	defer c.chainsyncMutex.Unlock()
	e := evt.Data.(ChainsyncEvent)
	if e.Rollback {
		if err := c.handleEventChainsyncRollback(e); err != nil {
			// TODO: actually handle this error
			c.logger.Error(
				"failed to handle rollback",
				"component", "ledger",
				"error", err,
			)
			return
		}
	} else if e.BlockHeader != nil {
		if err := c.handleEventChainsyncBlockHeader(e); err != nil {
			// TODO: actually handle this error
			c.logger.Error(
				fmt.Sprintf("ledger: failed to handle block header: %s", err),
			)
			return
		}
	}
}

func (c *Consensus) handleEventBlockfetch(evt event.Event) {
	c.chainsyncBlockfetchMutex.Lock()
	defer c.chainsyncBlockfetchMutex.Unlock()
	e := evt.Data.(BlockfetchEvent)
	if e.BatchDone {
		if err := c.handleEventBlockfetchBatchDone(e); err != nil {
			// TODO: actually handle this error
			c.logger.Error(
				fmt.Sprintf(
					"ledger: failed to handle blockfetch batch done: %s",
					err,
				),
			)
		}
	} else if e.Block != nil {
		if err := c.handleEventBlockfetchBlock(e); err != nil {
			// TODO: actually handle this error
			c.logger.Error(
				fmt.Sprintf("ledger: failed to handle block: %s", err),
			)
		}
	}
}

func (c *Consensus) handleEventChainsyncRollback(e ChainsyncEvent) error {
	if err := c.chain.Rollback(e.Point); err != nil {
		return fmt.Errorf("chain rollback failed: %w", err)
	}
	return nil
}

func (c *Consensus) handleEventChainsyncBlockHeader(e ChainsyncEvent) error {
	// Allow us to build up a few blockfetch batches worth of headers
	allowedHeaderCount := blockfetchBatchSize * 4
	headerCount := c.chain.HeaderCount()
	// Wait for current blockfetch batch to finish before we collect more block headers
	if headerCount >= allowedHeaderCount {
		// We assign the channel to a temp var to protect against trying to read from a nil channel
		// without a race condition
		tmpDoneChan := c.chainsyncBlockfetchReadyChan
		if tmpDoneChan != nil {
			<-tmpDoneChan
		}
	}
	// Add header to chain
	if err := c.chain.AddBlockHeader(e.BlockHeader); err != nil {
		if !errors.As(err, &chain.BlockNotFitChainTipError{}) {
			return fmt.Errorf("failed adding chain block header: %w", err)
		}
		c.logger.Warn(
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
	c.chainsyncBlockfetchMutex.Lock()
	defer c.chainsyncBlockfetchMutex.Unlock()
	// Don't start fetch if there's already one in progress
	if c.chainsyncBlockfetchReadyChan != nil {
		c.chainsyncBlockfetchWaiting = true
		return nil
	}
	// Request next bulk range
	headerStart, headerEnd := c.chain.HeaderRange(blockfetchBatchSize)
	err := c.blockfetchRequestRangeStart(
		e.ConnectionId,
		headerStart,
		headerEnd,
	)
	if err != nil {
		c.blockfetchRequestRangeCleanup(true)
		return err
	}
	return nil
}

//nolint:unparam
func (c *Consensus) handleEventBlockfetchBlock(e BlockfetchEvent) error {
	c.chainsyncBlockEvents = append(
		c.chainsyncBlockEvents,
		e,
	)
	// Update busy time in order to detect fetch timeout
	c.chainsyncBlockfetchBusyTime = time.Now()
	return nil
}

func (c *Consensus) processBlockEvents() error {
	batchOffset := 0
	for {
		batchSize := min(
			10, // Chosen to stay well under badger transaction size limit
			len(c.chainsyncBlockEvents)-batchOffset,
		)
		if batchSize <= 0 {
			break
		}
		c.Lock()
		// Start a transaction
		txn := c.db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			for _, evt := range c.chainsyncBlockEvents[batchOffset : batchOffset+batchSize] {
				if err := c.processBlockEvent(txn, evt); err != nil {
					return fmt.Errorf("failed processing block event: %w", err)
				}
			}
			return nil
		})
		c.Unlock()
		if err != nil {
			return err
		}
		batchOffset += batchSize
	}
	c.chainsyncBlockEvents = nil
	return nil
}

func (c *Consensus) processBlockEvent(
	txn *database.Txn,
	e BlockfetchEvent,
) error {
	// Add block to chain
	if err := c.chain.AddBlock(e.Block, txn); err != nil {
		// Ignore and log errors about block not fitting on chain or matching first header
		if !errors.As(err, &chain.BlockNotFitChainTipError{}) &&
			!errors.As(err, &chain.BlockNotMatchHeaderError{}) {
			return fmt.Errorf("add chain block: %w", err)
		}
		c.logger.Warn(
			fmt.Sprintf(
				"ignoring blockfetch block: %s",
				err,
			),
		)
	}
	return nil
}

func (c *Consensus) blockfetchRequestRangeStart(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	err := c.config.BlockfetchRequestRangeFunc(
		connId,
		start,
		end,
	)
	if err != nil {
		return fmt.Errorf("request block range: %w", err)
	}
	// Reset blockfetch busy time
	c.chainsyncBlockfetchBusyTime = time.Now()
	// Create our blockfetch done signal channels
	c.chainsyncBlockfetchReadyChan = make(chan struct{})
	c.chainsyncBlockfetchBatchDoneChan = make(chan struct{})
	// Start goroutine to handle blockfetch timeout
	go func() {
		for {
			select {
			case <-c.chainsyncBlockfetchBatchDoneChan:
				return
			case <-time.After(500 * time.Millisecond):
			}
			// Clear blockfetch busy flag on timeout
			if time.Since(
				c.chainsyncBlockfetchBusyTime,
			) > blockfetchBusyTimeout {
				c.blockfetchRequestRangeCleanup(true)
				c.logger.Warn(
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

func (c *Consensus) blockfetchRequestRangeCleanup(resetFlags bool) {
	// Reset buffer
	c.chainsyncBlockEvents = slices.Delete(
		c.chainsyncBlockEvents,
		0,
		len(c.chainsyncBlockEvents),
	)
	// Close our blockfetch done signal channel
	if c.chainsyncBlockfetchReadyChan != nil {
		close(c.chainsyncBlockfetchReadyChan)
		c.chainsyncBlockfetchReadyChan = nil
	}
	// Reset flags
	if resetFlags {
		c.chainsyncBlockfetchWaiting = false
	}
}

func (c *Consensus) handleEventBlockfetchBatchDone(e BlockfetchEvent) error {
	// Cancel our blockfetch timeout watcher
	if c.chainsyncBlockfetchBatchDoneChan != nil {
		close(c.chainsyncBlockfetchBatchDoneChan)
	}
	// Process pending block events
	if err := c.processBlockEvents(); err != nil {
		c.blockfetchRequestRangeCleanup(true)
		return fmt.Errorf("process block events: %w", err)
	}
	// Check for pending block range request
	if !c.chainsyncBlockfetchWaiting ||
		c.chain.HeaderCount() == 0 {
		// Allow collection of more block headers via chainsync
		c.blockfetchRequestRangeCleanup(true)
		return nil
	}
	// Clean up from blockfetch batch
	c.blockfetchRequestRangeCleanup(false)
	// Request next waiting bulk range
	headerStart, headerEnd := c.chain.HeaderRange(blockfetchBatchSize)
	err := c.blockfetchRequestRangeStart(
		e.ConnectionId,
		headerStart,
		headerEnd,
	)
	if err != nil {
		c.blockfetchRequestRangeCleanup(true)
		return err
	}
	c.chainsyncBlockfetchWaiting = false
	return nil
}
