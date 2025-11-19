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

package node

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

const (
	// LedgerCatchupPollInterval is how often to check if ledger has caught up
	LedgerCatchupPollInterval = config.DefaultLedgerCatchupPollInterval
	// LedgerCatchupStallTimeout is the maximum time to wait without progress before failing
	LedgerCatchupStallTimeout = config.DefaultLedgerCatchupStallTimeout
)

func Load(cfg *config.Config, logger *slog.Logger, immutableDir string) error {
	var nodeCfg *cardano.CardanoNodeConfig
	if cfg.CardanoConfig != "" {
		var err error
		nodeCfg, err = cardano.LoadCardanoNodeConfigWithFallback(
			cfg.CardanoConfig,
			cfg.Network,
			cardano.EmbeddedConfigPreviewNetworkFS,
		)
		if err != nil {
			return err
		}
		logger.Debug(
			fmt.Sprintf(
				"cardano network config: %+v",
				nodeCfg,
			),
			"component", "node",
		)
	}
	// Load database
	dbConfig := &database.Config{
		BlobCacheSize: cfg.BadgerCacheSize,
		DataDir:       cfg.DatabasePath,
		Logger:        logger,
		PromRegistry:  nil,
	}
	db, err := database.New(dbConfig)
	if err != nil {
		return err
	}
	// Load chain
	eventBus := event.NewEventBus(nil)
	cm, err := chain.NewManager(
		db,
		eventBus,
	)
	if err != nil {
		return fmt.Errorf("failed to load chain manager: %w", err)
	}
	c := cm.PrimaryChain()
	if c == nil {
		return errors.New("primary chain not available")
	}
	// Load state
	ls, err := ledger.NewLedgerState(
		ledger.LedgerStateConfig{
			Database:           db,
			ChainManager:       cm,
			Logger:             logger,
			CardanoNodeConfig:  nodeCfg,
			EventBus:           eventBus,
			ValidateHistorical: cfg.ValidateHistorical,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}
	// Start ledger asynchronously
	if err := ls.Start(); err != nil {
		return fmt.Errorf("failed to start ledger state: %w", err)
	}
	// Open immutable DB
	immutable, err := immutable.New(immutableDir)
	if err != nil {
		return fmt.Errorf("failed to read immutable DB: %w", err)
	}
	// Record immutable DB tip
	immutableTip, err := immutable.GetTip()
	if err != nil {
		return fmt.Errorf("failed to read immutable DB tip: %w", err)
	}
	if immutableTip == nil {
		return errors.New("immutable DB tip is nil")
	}
	// Copy all blocks
	logger.Info("copying blocks from immutable DB")
	chainTip := c.Tip()
	iter, err := immutable.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return fmt.Errorf("failed to get immutable DB iterator: %w", err)
	}
	var blocksCopied int
	blockBatch := make([]gledger.Block, 0, 500)
	for {
		for {
			next, err := iter.Next()
			if err != nil {
				return err
			}
			// No more blocks
			if next == nil {
				break
			}
			tmpBlock, err := gledger.NewBlockFromCbor(next.Type, next.Cbor)
			if err != nil {
				return err
			}
			// Skip first block when continuing a load operation
			if blocksCopied == 0 &&
				tmpBlock.SlotNumber() == chainTip.Point.Slot {
				continue
			}
			blockBatch = append(blockBatch, tmpBlock)
			if len(blockBatch) == cap(blockBatch) {
				break
			}
		}
		if len(blockBatch) == 0 {
			break
		}
		// Add block batch to chain
		if err := c.AddBlocks(blockBatch); err != nil {
			logger.Error(
				fmt.Sprintf(
					"failed to import block: %s",
					err,
				),
			)
			return nil
		}
		blocksCopied += len(blockBatch)
		blockBatch = slices.Delete(blockBatch, 0, len(blockBatch))
		if blocksCopied > 0 && blocksCopied%10000 == 0 {
			logger.Info(
				fmt.Sprintf(
					"copying blocks from immutable DB (%d blocks copied)",
					blocksCopied,
				),
			)
		}
	}
	logger.Info(
		fmt.Sprintf(
			"finished copying %d blocks from immutable DB",
			blocksCopied,
		),
	)
	// Wait for ledger to catch up with progress-based timeout
	pollInterval := cfg.LedgerCatchupPollInterval
	if pollInterval == 0 {
		pollInterval = LedgerCatchupPollInterval // fallback to default
	}
	stallTimeout := cfg.LedgerCatchupStallTimeout
	if stallTimeout == 0 {
		stallTimeout = LedgerCatchupStallTimeout // fallback to default
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastSlot uint64
	lastProgressTime := time.Now()
	var zeroSlotSeenAt time.Time

	for range ticker.C {
		tip := ls.Tip()
		if tip.Point.Slot == 0 {
			// Record when we first saw slot 0
			if zeroSlotSeenAt.IsZero() {
				zeroSlotSeenAt = time.Now()
			}
			// Check if we've been stuck at slot 0 for too long
			if time.Since(zeroSlotSeenAt) > stallTimeout {
				return fmt.Errorf(
					"ledger processing appears stalled: stuck at slot 0 for %v",
					stallTimeout,
				)
			}
			logger.Debug(
				"ledger tip slot is 0, waiting for ledger to initialize",
			)
			continue
		}

		// Slot has advanced from 0, reset zero slot tracking
		if !zeroSlotSeenAt.IsZero() {
			zeroSlotSeenAt = time.Time{}
		}

		currentSlot := tip.Point.Slot
		currentTime := time.Now()

		// Check if we've reached the target
		if currentSlot >= immutableTip.Slot {
			logger.Info(
				fmt.Sprintf(
					"finished processing %d blocks from immutable DB",
					blocksCopied,
				),
			)
			return nil
		}

		// Check for progress
		if currentSlot > lastSlot {
			// Progress made, reset the stall timer
			lastSlot = currentSlot
			lastProgressTime = currentTime
			logger.Debug(
				fmt.Sprintf(
					"ledger progress: slot %d/%d",
					currentSlot,
					immutableTip.Slot,
				),
			)
		} else if currentTime.Sub(lastProgressTime) > stallTimeout {
			// No progress for too long, fail
			return fmt.Errorf(
				"ledger processing appears stalled: no progress for %v (last slot: %d, target: %d)",
				stallTimeout,
				lastSlot,
				immutableTip.Slot,
			)
		}
	}
	panic("unreachable")
}
