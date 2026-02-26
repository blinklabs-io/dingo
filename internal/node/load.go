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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

func Load(ctx context.Context, cfg *config.Config, logger *slog.Logger, immutableDir string) error {
	return LoadWithDB(ctx, cfg, logger, immutableDir, nil)
}

// ensureDB returns the provided database or opens a new one from cfg.
// The returned cleanup function must be deferred by the caller; it closes
// the database only when a new one was created.
func ensureDB(
	cfg *config.Config,
	logger *slog.Logger,
	db *database.Database,
) (*database.Database, func(), error) {
	if db != nil {
		return db, func() {}, nil
	}
	dbConfig := &database.Config{
		DataDir:        cfg.DatabasePath,
		Logger:         logger,
		PromRegistry:   nil,
		BlobPlugin:     cfg.BlobPlugin,
		MetadataPlugin: cfg.MetadataPlugin,
		MaxConnections: cfg.DatabaseWorkers,
	}
	newDB, err := database.New(dbConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("creating database: %w", err)
	}
	return newDB, func() { newDB.Close() }, nil
}

// WithBulkLoadPragmas enables bulk-load optimizations on the metadata
// store if it implements BulkLoadOptimizer. The returned cleanup
// function restores normal pragmas and must be deferred by the caller.
func WithBulkLoadPragmas(
	db *database.Database,
	logger *slog.Logger,
) func() {
	optimizer, ok := db.Metadata().(metadata.BulkLoadOptimizer)
	if !ok {
		return func() {}
	}
	if err := optimizer.SetBulkLoadPragmas(); err != nil {
		logger.Warn(
			"failed to set bulk-load optimizations",
			"error", err,
		)
		return func() {}
	}
	return func() {
		if err := optimizer.RestoreNormalPragmas(); err != nil {
			logger.Error(
				"failed to restore normal settings",
				"error", err,
			)
		}
	}
}

// LoadWithDB loads immutable DB blocks into the chain. If db is nil,
// a new database connection is opened (and closed on return).
func LoadWithDB(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	immutableDir string,
	db *database.Database,
) error {
	// Derive default config path from cfg.Network when cfg.CardanoConfig is empty
	cardanoConfigPath := cfg.CardanoConfig
	if cardanoConfigPath == "" {
		network := cfg.Network
		if network == "" {
			network = "preview"
		}
		cardanoConfigPath = network + "/config.json"
	}

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardanoConfigPath,
		cfg.Network,
		cardano.EmbeddedConfigPreviewNetworkFS,
	)
	if err != nil {
		return fmt.Errorf(
			"loading cardano node config: %w", err,
		)
	}
	logger.Debug(
		"cardano network config",
		"component", "node",
		"config", nodeCfg,
	)
	// Load database (open new one if not provided)
	db, closeDB, err := ensureDB(cfg, logger, db)
	if err != nil {
		return err
	}
	defer closeDB()
	// Enable bulk-load optimizations if the metadata store supports them
	defer WithBulkLoadPragmas(db, logger)()
	// Load chain
	eventBus := event.NewEventBus(nil, logger)
	defer eventBus.Stop()
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
			DatabaseWorkerPoolConfig: ledger.DatabaseWorkerPoolConfig{
				WorkerPoolSize: cfg.DatabaseWorkers,
				TaskQueueSize:  cfg.DatabaseQueueSize,
				Disabled:       false,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}
	if err := ls.Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}
	defer ls.Close()

	blocksCopied, immutableTipSlot, err := copyBlocks(
		ctx, logger, immutableDir, c,
	)
	if err != nil {
		return fmt.Errorf("loading blocks: %w", err)
	}

	// Wait for ledger to catch up with tight polling
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	catchupTimeout := 30 * time.Minute
	if cfg.LedgerCatchupTimeout != "" {
		if parsed, pErr := time.ParseDuration(
			cfg.LedgerCatchupTimeout,
		); pErr == nil {
			catchupTimeout = parsed
		} else {
			logger.Warn(
				"invalid ledgerCatchupTimeout, using default",
				"value", cfg.LedgerCatchupTimeout,
				"error", pErr,
			)
		}
	}
	timeout := time.After(catchupTimeout)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"cancelled waiting for ledger to catch up"+
					" (tip slot %d, target slot %d): %w",
				ls.Tip().Point.Slot,
				immutableTipSlot,
				ctx.Err(),
			)
		case <-ticker.C:
			tip := ls.Tip()
			if tip.Point.Slot >= immutableTipSlot {
				logger.Info(
					"finished processing blocks from immutable DB",
					"blocks_copied", blocksCopied,
				)
				return nil
			}
		case <-timeout:
			return fmt.Errorf(
				"timed out waiting for ledger to catch up"+
					" (tip slot %d, target slot %d)",
				ls.Tip().Point.Slot,
				immutableTipSlot,
			)
		}
	}
}

// LoadBlobsResult contains the result of a blob-only ImmutableDB load.
type LoadBlobsResult struct {
	BlocksCopied     int
	ImmutableTipSlot uint64
}

// LoadBlobsWithDB copies blocks from an ImmutableDB directory into the blob
// store without starting the ledger processing pipeline. This is used after
// a Mithril snapshot import where the ledger state has already been loaded
// from the snapshot. Returns the number of blocks copied and the immutable
// tip slot so the caller can update the metadata tip to match.
func LoadBlobsWithDB(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	immutableDir string,
	db *database.Database,
) (*LoadBlobsResult, error) {
	// Load database (open new one if not provided)
	callerProvidedDB := db != nil
	db, closeDB, err := ensureDB(cfg, logger, db)
	if err != nil {
		return nil, err
	}
	defer closeDB()
	// Enable bulk-load optimizations if available. When the
	// caller provides a db, they are responsible for pragma
	// management (avoids concurrent pragma modification if
	// multiple goroutines share the same database).
	if !callerProvidedDB {
		defer WithBulkLoadPragmas(db, logger)()
	}
	// Load chain without event bus (no ledger processing)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load chain manager: %w", err)
	}
	c := cm.PrimaryChain()
	if c == nil {
		return nil, errors.New("primary chain not available")
	}

	blocksCopied, immutableTipSlot, err := copyBlocksRaw(
		ctx, logger, immutableDir, c,
	)
	if err != nil {
		return nil, fmt.Errorf("loading blocks: %w", err)
	}

	return &LoadBlobsResult{
		BlocksCopied:     blocksCopied,
		ImmutableTipSlot: immutableTipSlot,
	}, nil
}

// copyBlocks reads blocks from an ImmutableDB directory and writes them to
// the chain's blob store. Returns the number of blocks copied and the
// immutable tip slot.
func copyBlocks(
	ctx context.Context,
	logger *slog.Logger,
	immutableDir string,
	c *chain.Chain,
) (int, uint64, error) {
	// Open immutable DB
	immutable, err := immutable.New(immutableDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB: %w", err)
	}
	// Record immutable DB tip
	immutableTip, err := immutable.GetTip()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB tip: %w", err)
	}
	if immutableTip == nil {
		return 0, 0, errors.New("immutable DB tip is nil")
	}
	// Copy all blocks
	logger.Info("copying blocks from immutable DB")
	chainTip := c.Tip()
	iter, err := immutable.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to get immutable DB iterator: %w",
			err,
		)
	}
	defer iter.Close()
	var blocksCopied int
	blockBatch := make([]gledger.Block, 0, 50)
	for {
		for {
			next, err := iter.Next()
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"reading next block: %w", err,
				)
			}
			// No more blocks
			if next == nil {
				break
			}
			tmpBlock, err := gledger.NewBlockFromCbor(next.Type, next.Cbor)
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"decoding block CBOR: %w", err,
				)
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
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"failed to import block: %w",
				err,
			)
		}
		blocksCopied += len(blockBatch)
		blockBatch = slices.Delete(blockBatch, 0, len(blockBatch))
		if blocksCopied > 0 && blocksCopied%10000 == 0 {
			logger.Info(
				"copying blocks from immutable DB",
				"blocks_copied", blocksCopied,
			)
		}
		// Check for cancellation after each batch
		if err := ctx.Err(); err != nil {
			return blocksCopied, immutableTip.Slot,
				fmt.Errorf("loading blocks: %w", err)
		}
	}
	logger.Info(
		"finished copying blocks from immutable DB",
		"blocks_copied", blocksCopied,
	)
	return blocksCopied, immutableTip.Slot, nil
}

// copyBlocksRaw is a lightweight variant of copyBlocks that decodes only
// block headers instead of full blocks. This is significantly faster for
// bulk loading since it skips decoding transaction bodies and witnesses
// (which can be 50-90KB per block) while extracting just the ~200-500
// byte header needed for chain indexing.
func copyBlocksRaw(
	ctx context.Context,
	logger *slog.Logger,
	immutableDir string,
	c *chain.Chain,
) (int, uint64, error) {
	imm, err := immutable.New(immutableDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB: %w", err)
	}
	immutableTip, err := imm.GetTip()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB tip: %w", err)
	}
	if immutableTip == nil {
		return 0, 0, errors.New("immutable DB tip is nil")
	}
	logger.Info("copying blocks from immutable DB (header-only decode)")
	chainTip := c.Tip()
	iter, err := imm.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to get immutable DB iterator: %w",
			err,
		)
	}
	defer iter.Close()
	var blocksCopied int
	blockBatch := make([]chain.RawBlock, 0, 50)
	for {
		for {
			next, err := iter.Next()
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"reading next block: %w", err,
				)
			}
			if next == nil {
				break
			}
			// Skip EBBs — Byron Epoch Boundary Blocks have a
			// different header layout that
			// NewBlockHeaderFromCbor cannot decode. This is
			// safe for chain continuity because EBBs are not
			// part of the PrevHash chain: the next regular
			// block's PrevHash points to the block before
			// the EBB, not the EBB itself.
			if next.IsEbb {
				continue
			}
			// Skip first block when continuing a load operation
			if blocksCopied == 0 &&
				next.Slot == chainTip.Point.Slot {
				continue
			}
			// Extract header CBOR from the block's outer array
			// (first element for all eras), then decode just the
			// header — skipping transaction bodies and witnesses.
			headerCbor, err := extractHeaderCbor(next.Cbor)
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"extracting block header CBOR: %w", err,
				)
			}
			header, err := gledger.NewBlockHeaderFromCbor(
				next.Type,
				headerCbor,
			)
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"decoding block header: %w", err,
				)
			}
			blockBatch = append(blockBatch, chain.RawBlock{
				Slot:        header.SlotNumber(),
				Hash:        header.Hash().Bytes(),
				BlockNumber: header.BlockNumber(),
				Type:        next.Type,
				PrevHash:    header.PrevHash().Bytes(),
				Cbor:        next.Cbor,
			})
			if len(blockBatch) == cap(blockBatch) {
				break
			}
		}
		if len(blockBatch) == 0 {
			break
		}
		if err := c.AddRawBlocks(blockBatch); err != nil {
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"failed to import block: %w",
				err,
			)
		}
		blocksCopied += len(blockBatch)
		blockBatch = slices.Delete(blockBatch, 0, len(blockBatch))
		if blocksCopied > 0 && blocksCopied%10000 == 0 {
			logger.Info(
				"copying blocks from immutable DB",
				"blocks_copied", blocksCopied,
			)
		}
		// Check for cancellation after each batch
		if err := ctx.Err(); err != nil {
			return blocksCopied, immutableTip.Slot,
				fmt.Errorf("loading blocks: %w", err)
		}
	}
	logger.Info(
		"finished copying blocks from immutable DB",
		"blocks_copied", blocksCopied,
	)
	return blocksCopied, immutableTip.Slot, nil
}

// extractHeaderCbor extracts the header CBOR from a full block's CBOR.
// All Cardano block eras encode as a CBOR array where the first element
// is the block header.
func extractHeaderCbor(blockCbor []byte) ([]byte, error) {
	var parts []gcbor.RawMessage
	if _, err := gcbor.Decode(blockCbor, &parts); err != nil {
		return nil, fmt.Errorf("decoding block outer array: %w", err)
	}
	if len(parts) == 0 {
		return nil, errors.New("empty block array")
	}
	return []byte(parts[0]), nil
}
