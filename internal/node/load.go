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
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/state"
	"github.com/blinklabs-io/gouroboros/ledger"
)

func Load(logger *slog.Logger, immutableDir string) error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return err
	}
	var nodeCfg *cardano.CardanoNodeConfig
	if cfg.CardanoConfig != "" {
		tmpCfg, err := cardano.NewCardanoNodeConfigFromFile(cfg.CardanoConfig)
		if err != nil {
			return err
		}
		nodeCfg = tmpCfg
		logger.Debug(
			fmt.Sprintf(
				"cardano network config: %+v",
				nodeCfg,
			),
			"component", "node",
		)
	}
	// Load state
	eventBus := event.NewEventBus(nil)
	ls, err := state.NewLedgerState(
		state.LedgerStateConfig{
			DataDir:           cfg.DatabasePath,
			Logger:            logger,
			CardanoNodeConfig: nodeCfg,
			EventBus:          eventBus,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to load state database: %w", err)
	}
	c := ls.Chain()
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
	// Copy all blocks
	logger.Info("copying blocks from immutable DB")
	chainTip := c.Tip()
	iter, err := immutable.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return fmt.Errorf("failed to get immutable DB iterator: %w", err)
	}
	var blocksCopied int
	blockBatch := make([]ledger.Block, 0, 500)
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
			tmpBlock, err := ledger.NewBlockFromCbor(next.Type, next.Cbor)
			if err != nil {
				return err
			}
			// Skip first block when continuing a load operation
			if blocksCopied == 0 && tmpBlock.SlotNumber() == chainTip.Point.Slot {
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
				fmt.Sprintf("copying blocks from immutable DB (%d blocks copied)", blocksCopied),
			)
		}
	}
	logger.Info(
		fmt.Sprintf(
			"finished copying %d blocks from immutable DB",
			blocksCopied,
		),
	)
	// Wait for ledger to catch up
	for {
		time.Sleep(5 * time.Second)
		tip := ls.Tip()
		if tip.Point.Slot >= immutableTip.Slot {
			break
		}
	}
	logger.Info(
		fmt.Sprintf(
			"finished processing %d blocks from immutable DB",
			blocksCopied,
		),
	)
	return nil
}
