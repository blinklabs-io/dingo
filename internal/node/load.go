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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	fxcbor "github.com/fxamacker/cbor/v2"
)

// Mainnet full blocks can overflow practical Badger transaction limits with
// larger import batches, so keep the runtime load batch size aligned with the
// chain import batch cap.
const (
	loadBlockBatchSize  = 50
	progressLogInterval = 10 * time.Second
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
		RunMode:        string(cfg.RunMode),
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
	network := cfg.Network
	if cardanoConfigPath == "" {
		if network == "" {
			network = "preview"
		}
		cardanoConfigPath = network + "/config.json"
	}

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardanoConfigPath,
		network,
		cardano.EmbeddedConfigFS,
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
	// Immutable load replays trusted block batches directly into the ledger, so
	// it does not need the event-driven reread path here.
	cm, err := chain.NewManager(
		db,
		nil,
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
			Database:              db,
			ChainManager:          cm,
			Logger:                logger,
			CardanoNodeConfig:     nodeCfg,
			ValidateHistorical:    cfg.ValidateHistorical,
			TrustedReplay:         true,
			ManualBlockProcessing: true,
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

	replayCtx, cancelReplay := context.WithCancel(ctx)
	defer cancelReplay()
	replayBatches := make(chan []gledger.Block, 1)
	replayErrCh := make(chan error, 1)
	go func() {
		err := ls.ProcessTrustedBlockBatches(
			replayCtx,
			replayBatches,
		)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Cancel immediately so copyBlocksDirect and the
			// forwarding goroutine inside ProcessTrustedBlockBatches
			// unblock via ctx.Done() instead of deadlocking.
			cancelReplay()
		}
		replayErrCh <- err
	}()

	blocksCopied, immutableTipSlot, err := copyBlocksDirect(
		replayCtx, logger, immutableDir, c, replayBatches,
	)
	close(replayBatches)
	if err != nil {
		cancelReplay()
		<-replayErrCh
		return fmt.Errorf("loading blocks: %w", err)
	}
	if err := <-replayErrCh; err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("processing trusted block batches: %w", err)
	}
	logger.Info(
		"finished processing blocks from immutable DB",
		"blocks_copied", blocksCopied,
		"tip_slot", immutableTipSlot,
	)
	return nil
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

	var utxoOffsetsStored int
	blocksCopied, immutableTipSlot, err := copyBlocksRawWithCallback(
		ctx, logger, immutableDir, db, c,
		func(rb chain.RawBlock, txn *database.Txn) error {
			stored, err := storeRawBlockUtxoOffsets(txn, rb)
			if err != nil {
				return err
			}
			utxoOffsetsStored += stored
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("loading blocks: %w", err)
	}
	logger.Info(
		"finished storing immutable UTxO offsets during copy",
		"blocks_copied", blocksCopied,
		"utxo_offsets_stored", utxoOffsetsStored,
	)

	return &LoadBlobsResult{
		BlocksCopied:     blocksCopied,
		ImmutableTipSlot: immutableTipSlot,
	}, nil
}

// copyBlocksDirect reads immutable blocks once, persists them to the chain,
// and streams the decoded batches to the trusted ledger replay path.
func copyBlocksDirect(
	ctx context.Context,
	logger *slog.Logger,
	immutableDir string,
	c *chain.Chain,
	replayBatches chan<- []gledger.Block,
) (int, uint64, error) {
	immutable, err := immutable.New(immutableDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB: %w", err)
	}
	immutableTip, err := immutable.GetTip()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read immutable DB tip: %w", err)
	}
	if immutableTip == nil {
		return 0, 0, errors.New("immutable DB tip is nil")
	}
	logger.Info("copying blocks from immutable DB")
	chainTip := c.Tip()
	if chainTip.Point.Slot > immutableTip.Slot {
		logger.Info(
			"chain tip already beyond immutable DB tip; skipping immutable copy",
			"chain_tip_slot", chainTip.Point.Slot,
			"immutable_tip_slot", immutableTip.Slot,
		)
		return 0, immutableTip.Slot, nil
	}
	iter, err := immutable.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to get immutable DB iterator: %w",
			err,
		)
	}
	defer iter.Close()
	var blocksCopied int
	startTime := time.Now()
	lastProgressLog := time.Time{}
	var lastProgressSlot uint64
	blockBatch := make([]gledger.Block, 0, loadBlockBatchSize)
	verifyCfg := lcommon.VerifyConfig{
		SkipBodyHashValidation: true,
	}
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
			tmpBlock, err := gledger.NewBlockFromCbor(
				next.Type,
				next.Cbor,
				verifyCfg,
			)
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"decoding block CBOR: %w", err,
				)
			}
			if blocksCopied == 0 &&
				next.Slot == chainTip.Point.Slot &&
				bytes.Equal(next.Hash, chainTip.Point.Hash) {
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
		if err := c.AddBlocks(blockBatch); err != nil {
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"failed to import block: %w",
				err,
			)
		}
		replayBatch := append(
			make([]gledger.Block, 0, len(blockBatch)),
			blockBatch...,
		)
		select {
		case replayBatches <- replayBatch:
		case <-ctx.Done():
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"loading blocks: %w",
				ctx.Err(),
			)
		}
		blocksCopied += len(blockBatch)
		lastProgressSlot = replayBatch[len(replayBatch)-1].SlotNumber()
		blockBatch = blockBatch[:0]
		maybeLogBlockCopyProgress(
			logger,
			"copying blocks from immutable DB",
			blocksCopied,
			lastProgressSlot,
			immutableTip.Slot,
			startTime,
			&lastProgressLog,
		)
		if err := ctx.Err(); err != nil {
			return blocksCopied, immutableTip.Slot,
				fmt.Errorf("loading blocks: %w", err)
		}
	}
	return blocksCopied, immutableTip.Slot, nil
}

// copyBlocksRawWithCallback is a lightweight variant of copyBlocks that
// decodes only block headers instead of full blocks. This is significantly
// faster for bulk loading since it skips decoding transaction bodies and
// witnesses (which can be 50-90KB per block) while extracting just the
// ~200-500 byte header needed for chain indexing. The optional callback
// runs in the same transaction after each block is persisted, giving
// callers a hook to attach derived blob-side state such as UTxO offsets.
func copyBlocksRawWithCallback(
	ctx context.Context,
	logger *slog.Logger,
	immutableDir string,
	db *database.Database,
	c *chain.Chain,
	callback func(chain.RawBlock, *database.Txn) error,
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
	if chainTip.Point.Slot > immutableTip.Slot {
		logger.Info(
			"chain tip already beyond immutable DB tip; skipping immutable copy",
			"chain_tip_slot", chainTip.Point.Slot,
			"immutable_tip_slot", immutableTip.Slot,
		)
		if callback != nil && db != nil {
			blocksBackfilled, err := backfillRawBlockCallbacks(
				ctx,
				imm,
				db,
				callback,
			)
			if err != nil {
				return 0, immutableTip.Slot, fmt.Errorf(
					"backfill immutable raw block callback state: %w",
					err,
				)
			}
			logger.Info(
				"backfilled immutable raw block callback state",
				"blocks_backfilled", blocksBackfilled,
			)
		}
		return 0, immutableTip.Slot, nil
	}
	iter, err := imm.BlocksFromPoint(chainTip.Point)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to get immutable DB iterator: %w",
			err,
		)
	}
	defer iter.Close()
	var blocksCopied int
	startTime := time.Now()
	lastProgressLog := time.Time{}
	lastProgressSlot := chainTip.Point.Slot
	blockBatch := make([]chain.RawBlock, 0, loadBlockBatchSize)
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
			// EBBs (Byron Epoch Boundary Blocks) must be
			// imported: the next regular block's PrevHash
			// references the EBB, not the block before it.
			// Skip first block when continuing a load operation
			if blocksCopied == 0 &&
				next.Slot == chainTip.Point.Slot &&
				bytes.Equal(next.Hash, chainTip.Point.Hash) {
				continue
			}
			rawBlock, err := rawBlockFromImmutableBlock(next)
			if err != nil {
				return blocksCopied, immutableTip.Slot, fmt.Errorf(
					"building raw block: %w",
					err,
				)
			}
			blockBatch = append(blockBatch, rawBlock)
			if len(blockBatch) == cap(blockBatch) {
				break
			}
		}
		if len(blockBatch) == 0 {
			break
		}
		if err := c.AddRawBlocksWithCallback(blockBatch, callback); err != nil {
			return blocksCopied, immutableTip.Slot, fmt.Errorf(
				"failed to import block: %w",
				err,
			)
		}
		blocksCopied += len(blockBatch)
		if tmpLen := len(blockBatch); tmpLen > 0 {
			lastProgressSlot = blockBatch[tmpLen-1].Slot
		}
		blockBatch = blockBatch[:0]
		maybeLogBlockCopyProgress(
			logger,
			"copying blocks from immutable DB",
			blocksCopied,
			lastProgressSlot,
			immutableTip.Slot,
			startTime,
			&lastProgressLog,
		)
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

func backfillRawBlockCallbacks(
	ctx context.Context,
	imm *immutable.ImmutableDb,
	db *database.Database,
	callback func(chain.RawBlock, *database.Txn) error,
) (int, error) {
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	if err != nil {
		return 0, fmt.Errorf("failed to get immutable DB iterator: %w", err)
	}
	defer iter.Close()

	var blocksBackfilled int
	blockBatch := make([]chain.RawBlock, 0, loadBlockBatchSize)
	flush := func() error {
		if len(blockBatch) == 0 {
			return nil
		}
		txn := db.BlobTxn(true)
		defer txn.Release()
		if err := txn.Do(func(txn *database.Txn) error {
			for _, rb := range blockBatch {
				if err := callback(rb, txn); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		blocksBackfilled += len(blockBatch)
		blockBatch = blockBatch[:0]
		return nil
	}

	for {
		next, err := iter.Next()
		if err != nil {
			return blocksBackfilled, fmt.Errorf("reading next block: %w", err)
		}
		if next == nil {
			break
		}
		rawBlock, err := rawBlockFromImmutableBlock(next)
		if err != nil {
			return blocksBackfilled, fmt.Errorf("building raw block: %w", err)
		}
		blockBatch = append(blockBatch, rawBlock)
		if len(blockBatch) == cap(blockBatch) {
			if err := flush(); err != nil {
				return blocksBackfilled, err
			}
			if err := ctx.Err(); err != nil {
				return blocksBackfilled, fmt.Errorf("loading blocks: %w", err)
			}
		}
	}
	if err := flush(); err != nil {
		return blocksBackfilled, err
	}
	if err := ctx.Err(); err != nil {
		return blocksBackfilled, fmt.Errorf("loading blocks: %w", err)
	}
	return blocksBackfilled, nil
}

func rawBlockFromImmutableBlock(block *immutable.Block) (chain.RawBlock, error) {
	// Extract header CBOR from the block's outer array (first element for all
	// eras), then decode just the header without decoding transaction bodies.
	headerCbor, err := extractHeaderCbor(block.Cbor)
	if err != nil {
		return chain.RawBlock{}, fmt.Errorf(
			"extracting block header CBOR: %w",
			err,
		)
	}
	header, err := gledger.NewBlockHeaderFromCbor(block.Type, headerCbor)
	if err != nil {
		return chain.RawBlock{}, fmt.Errorf("decoding block header: %w", err)
	}
	return chain.RawBlock{
		Slot:        header.SlotNumber(),
		Hash:        header.Hash().Bytes(),
		BlockNumber: header.BlockNumber(),
		Type:        block.Type,
		PrevHash:    header.PrevHash().Bytes(),
		Cbor:        block.Cbor,
	}, nil
}

func storeRawBlockUtxoOffsets(
	txn *database.Txn,
	block chain.RawBlock,
) (int, error) {
	if txn == nil || txn.Blob() == nil {
		return 0, errors.New("blob transaction not available")
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return 0, errors.New("blob store not available")
	}
	var blockHash [32]byte
	copy(blockHash[:], block.Hash)
	totalUtxos := 0

	offsets, extractErr := lcommon.ExtractTransactionOffsets(block.Cbor)
	if extractErr != nil {
		return 0, fmt.Errorf(
			"block at slot %d: extract transaction offsets: %w",
			block.Slot,
			extractErr,
		)
	}
	if offsets == nil || len(offsets.Transactions) == 0 {
		return 0, nil
	}
	invalidTxs, err := extractInvalidTxIndices(block.Cbor)
	if err != nil {
		return 0, fmt.Errorf(
			"block at slot %d: decode invalid tx indices: %w",
			block.Slot,
			err,
		)
	}
	for txIdx, txLoc := range offsets.Transactions {
		bodyEnd := txLoc.Body.Offset + txLoc.Body.Length
		if int(bodyEnd) > len(block.Cbor) {
			return 0, fmt.Errorf(
				"block at slot %d: body range [%d:%d] exceeds block size %d",
				block.Slot,
				txLoc.Body.Offset,
				bodyEnd,
				len(block.Cbor),
			)
		}
		bodyBytes := block.Cbor[txLoc.Body.Offset:bodyEnd]
		txHash := lcommon.Blake2b256Hash(bodyBytes)
		_, txIsInvalid := invalidTxs[txIdx]
		if !txIsInvalid {
			for i, outLoc := range txLoc.Outputs {
				if outLoc.Length == 0 {
					continue
				}
				offset := database.CborOffset{
					BlockSlot:  block.Slot,
					BlockHash:  blockHash,
					ByteOffset: outLoc.Offset,
					ByteLength: outLoc.Length,
				}
				if err := blob.SetUtxo(
					txn.Blob(),
					txHash[:],
					uint32(i), // #nosec G115
					database.EncodeUtxoOffset(&offset),
				); err != nil {
					return 0, fmt.Errorf("storing UTxO offset: %w", err)
				}
				totalUtxos++
			}
			continue
		}
		collReturnOffset, collReturnLen, found, err := txBodyMapValueRange(
			block.Cbor[txLoc.Body.Offset:bodyEnd],
			txLoc.Body.Offset,
			database.TxBodyKeyCollateralReturn,
		)
		if err != nil {
			return 0, fmt.Errorf(
				"block at slot %d: transaction %x collateral return offset: %w",
				block.Slot,
				txHash[:8],
				err,
			)
		}
		if !found {
			continue
		}
		offset := database.CborOffset{
			BlockSlot:  block.Slot,
			BlockHash:  blockHash,
			ByteOffset: collReturnOffset,
			ByteLength: collReturnLen,
		}
		outputIdx := uint32(len(txLoc.Outputs)) // #nosec G115
		if err := blob.SetUtxo(
			txn.Blob(),
			txHash[:],
			outputIdx,
			database.EncodeUtxoOffset(&offset),
		); err != nil {
			return 0, fmt.Errorf(
				"storing collateral return UTxO offset: %w",
				err,
			)
		}
		totalUtxos++
	}
	return totalUtxos, nil
}

func extractInvalidTxIndices(blockCbor []byte) (map[int]struct{}, error) {
	var blockArray []gcbor.RawMessage
	if _, err := gcbor.Decode(blockCbor, &blockArray); err != nil {
		return nil, err
	}
	if len(blockArray) < 5 {
		return nil, nil
	}
	var invalidTxs []uint
	if _, err := gcbor.Decode([]byte(blockArray[4]), &invalidTxs); err != nil {
		return nil, err
	}
	if len(invalidTxs) == 0 {
		return nil, nil
	}
	set := make(map[int]struct{}, len(invalidTxs))
	for _, idx := range invalidTxs {
		set[int(idx)] = struct{}{} // #nosec G115
	}
	return set, nil
}

func txBodyMapValueRange(
	bodyCbor []byte,
	bodyOffset uint32,
	key uint64,
) (uint32, uint32, bool, error) {
	decoder, err := gcbor.NewStreamDecoder(bodyCbor)
	if err != nil {
		return 0, 0, false, err
	}
	count, _, _, err := decoder.DecodeMapHeader()
	if err != nil {
		return 0, 0, false, err
	}
	if count < 0 {
		return 0, 0, false, errors.New("indefinite tx body map")
	}
	for range count {
		var currentKey uint64
		if _, _, err := decoder.Decode(&currentKey); err != nil {
			return 0, 0, false, err
		}
		valueOffset, valueLen, err := decoder.Skip()
		if err != nil {
			return 0, 0, false, err
		}
		if currentKey != key {
			continue
		}
		valueOffset32, err := checkedUint32(valueOffset)
		if err != nil {
			return 0, 0, false, err
		}
		valueLen32, err := checkedUint32(valueLen)
		if err != nil {
			return 0, 0, false, err
		}
		if valueOffset32 > ^uint32(0)-bodyOffset {
			return 0, 0, false, fmt.Errorf(
				"value offset %d overflows body offset %d",
				valueOffset32,
				bodyOffset,
			)
		}
		return bodyOffset + valueOffset32, valueLen32, true, nil
	}
	return 0, 0, false, nil
}

func checkedUint32(v int) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("negative value %d", v)
	}
	if uint64(v) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("value %d overflows uint32", v)
	}
	return uint32(v), nil // #nosec G115
}

func maybeLogBlockCopyProgress(
	logger *slog.Logger,
	msg string,
	blocksCopied int,
	currentSlot uint64,
	tipSlot uint64,
	startTime time.Time,
	lastLogTime *time.Time,
) {
	now := time.Now()
	if !lastLogTime.IsZero() && now.Sub(*lastLogTime) < progressLogInterval {
		return
	}
	*lastLogTime = now

	elapsed := now.Sub(startTime)
	attrs := []any{
		"blocks_copied", blocksCopied,
		"slot", currentSlot,
	}
	if elapsed > 0 {
		attrs = append(
			attrs,
			"blocks_per_sec",
			fmt.Sprintf("%.0f", float64(blocksCopied)/elapsed.Seconds()),
		)
	}
	if tipSlot > 0 {
		attrs = append(
			attrs,
			"progress",
			fmt.Sprintf("%.1f%%", float64(currentSlot)/float64(tipSlot)*100),
		)
	}
	logger.Info(msg, attrs...)
}

// extractHeaderCbor extracts the header CBOR from a full block's CBOR.
// All Cardano block eras encode as a CBOR array where the first element
// is the block header.
func extractHeaderCbor(blockCbor []byte) ([]byte, error) {
	headerLen, err := cborArrayHeaderLen(blockCbor)
	if err != nil {
		return nil, err
	}
	var headerCbor gcbor.RawMessage
	if _, err := fxcbor.UnmarshalFirst(blockCbor[headerLen:], &headerCbor); err != nil {
		return nil, fmt.Errorf("decoding block header CBOR: %w", err)
	}
	if len(headerCbor) == 0 {
		return nil, errors.New("empty block header")
	}
	return []byte(headerCbor), nil
}

func cborArrayHeaderLen(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, errors.New("empty CBOR data")
	}
	majorType := data[0] & gcbor.CborTypeMask
	if majorType != gcbor.CborTypeArray {
		return 0, errors.New("block CBOR is not an array")
	}
	additional := data[0] &^ gcbor.CborTypeMask
	switch {
	case additional <= gcbor.CborMaxUintSimple:
		return 1, nil
	case additional == 24:
		if len(data) < 2 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 2, nil
	case additional == 25:
		if len(data) < 3 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 3, nil
	case additional == 26:
		if len(data) < 5 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 5, nil
	case additional == 27:
		if len(data) < 9 {
			return 0, errors.New("truncated CBOR array header")
		}
		return 9, nil
	case additional == 31:
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported CBOR array additional info: %d", additional)
	}
}
