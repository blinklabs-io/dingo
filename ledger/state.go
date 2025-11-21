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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	cleanupConsumedUtxosInterval   = 5 * time.Minute
	cleanupConsumedUtxosSlotWindow = 50000 // TODO: calculate this from params (#395)
)

type ChainsyncState string

const (
	InitChainsyncState     ChainsyncState = "init"
	RollbackChainsyncState ChainsyncState = "rollback"
	SyncingChainsyncState  ChainsyncState = "syncing"
)

type LedgerStateConfig struct {
	PromRegistry               prometheus.Registerer
	Logger                     *slog.Logger
	Database                   *database.Database
	ChainManager               *chain.ChainManager
	EventBus                   *event.EventBus
	CardanoNodeConfig          *cardano.CardanoNodeConfig
	BlockfetchRequestRangeFunc BlockfetchRequestRangeFunc
	ValidateHistorical         bool
	ForgeBlocks                bool
}

// BlockfetchRequestRangeFunc describes a callback function used to start a blockfetch request for
// a range of blocks
type BlockfetchRequestRangeFunc func(ouroboros.ConnectionId, ocommon.Point, ocommon.Point) error

// In ledger/state.go or a shared package
type MempoolProvider interface {
	Transactions() []mempool.MempoolTransaction
}
type LedgerState struct {
	metrics                          stateMetrics
	currentEra                       eras.EraDesc
	config                           LedgerStateConfig
	chainsyncBlockfetchBusyTime      time.Time
	currentPParams                   lcommon.ProtocolParameters
	mempool                          MempoolProvider
	chainsyncBlockfetchBatchDoneChan chan struct{}
	timerCleanupConsumedUtxos        *time.Timer
	Scheduler                        *Scheduler
	chain                            *chain.Chain
	chainsyncBlockfetchReadyChan     chan struct{}
	db                               *database.Database
	chainsyncState                   ChainsyncState
	currentTipBlockNonce             []byte
	chainsyncBlockEvents             []BlockfetchEvent
	epochCache                       []models.Epoch
	currentTip                       ochainsync.Tip
	currentEpoch                     models.Epoch
	sync.RWMutex
	chainsyncMutex             sync.Mutex
	chainsyncBlockfetchMutex   sync.Mutex
	chainsyncBlockfetchWaiting bool
	checkpointWrittenForEpoch  bool
	// Goroutine lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewLedgerState(cfg LedgerStateConfig) (*LedgerState, error) {
	if cfg.ChainManager == nil {
		return nil, errors.New("a ChainManager is required")
	}
	if cfg.Database == nil {
		return nil, errors.New("a Database is required")
	}
	ls := &LedgerState{
		config:         cfg,
		chainsyncState: InitChainsyncState,
		db:             cfg.Database,
		chain:          cfg.ChainManager.PrimaryChain(),
	}
	if cfg.Logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Initialize context for goroutine lifecycle management
	ls.ctx, ls.cancel = context.WithCancel(context.Background())
	return ls, nil
}

func (ls *LedgerState) Start() error {
	// Init metrics
	ls.metrics.init(ls.config.PromRegistry)
	// Setup event handlers
	if ls.config.EventBus != nil {
		ls.config.EventBus.SubscribeFunc(
			ChainsyncEventType,
			ls.handleEventChainsync,
		)
		ls.config.EventBus.SubscribeFunc(
			BlockfetchEventType,
			ls.handleEventBlockfetch,
		)
	}
	// Schedule periodic process to purge consumed UTxOs outside of the rollback window
	ls.scheduleCleanupConsumedUtxos()
	// Load epoch info from DB
	txn := ls.db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		return ls.loadEpochs(txn)
	})
	if err != nil {
		return fmt.Errorf("failed to load epoch info: %w", err)
	}
	ls.checkpointWrittenForEpoch = false
	// Load current protocol parameters from DB
	if err := ls.loadPParams(); err != nil {
		return fmt.Errorf("failed to load pparams: %w", err)
	}
	// Load current tip
	if err := ls.loadTip(); err != nil {
		return fmt.Errorf("failed to load tip: %w", err)
	}
	// Create genesis block
	if err := ls.createGenesisBlock(); err != nil {
		return fmt.Errorf("failed to create genesis block: %w", err)
	}
	// Initialize scheduler
	if err := ls.initScheduler(); err != nil {
		return fmt.Errorf("initialize scheduler: %w", err)
	}
	// Schedule block forging
	ls.initForge()
	// Start goroutine to process new blocks
	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
		ls.ledgerProcessBlocks()
	}()
	return nil
}

func (ls *LedgerState) RecoverCommitTimestampConflict() error {
	// Load current ledger tip
	tmpTip, err := ls.db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("failed to get tip: %w", err)
	}
	// Check if we can lookup tip block in chain
	_, err = ls.chain.BlockByPoint(tmpTip.Point, nil)
	if err != nil {
		// Rollback to raw chain tip on error
		chainTip := ls.chain.Tip()
		if err = ls.rollback(chainTip.Point); err != nil {
			return fmt.Errorf(
				"failed to rollback ledger: %w",
				err,
			)
		}
	}
	return nil
}

func (ls *LedgerState) Chain() *chain.Chain {
	return ls.chain
}

func (ls *LedgerState) Close() error {
	// Cancel context to signal goroutines to stop
	if ls.cancel != nil {
		ls.cancel()
	}
	// Wait for all goroutines to finish
	ls.wg.Wait()
	// Close database
	return ls.db.Close()
}

func (ls *LedgerState) initScheduler() error {
	// Initialize timer with current slot length
	slotLength := ls.currentEpoch.SlotLength
	if slotLength == 0 {
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		if shelleyGenesis == nil {
			return errors.New("could not get genesis config")
		}
		slotLength = uint(
			new(big.Int).Div(
				new(big.Int).Mul(
					big.NewInt(1000),
					shelleyGenesis.SlotLength.Num(),
				),
				shelleyGenesis.SlotLength.Denom(),
			).Uint64(),
		)
	}
	// nolint:gosec
	// Slot length is small enough to not overflow int64
	interval := time.Duration(slotLength) * time.Millisecond
	ls.Scheduler = NewScheduler(interval)
	ls.Scheduler.Start()
	return nil
}

func (ls *LedgerState) initForge() {
	// Schedule block forging if dev mode is enabled
	if ls.config.ForgeBlocks {
		// Calculate block interval from ActiveSlotsCoeff
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		if shelleyGenesis != nil {
			// Calculate block interval (1 / ActiveSlotsCoeff)
			activeSlotsCoeff := shelleyGenesis.ActiveSlotsCoeff
			if activeSlotsCoeff.Rat != nil &&
				activeSlotsCoeff.Rat.Num().Int64() > 0 {
				blockInterval := int(
					(1 * activeSlotsCoeff.Rat.Denom().Int64()) / activeSlotsCoeff.Rat.Num().
						Int64(),
				)
				// Scheduled forgeBlock to run at the calculated block interval
				// TODO: add callback to capture task run failure and increment "missed slot leader check" metric
				ls.Scheduler.Register(blockInterval, ls.forgeBlock, nil)

				ls.config.Logger.Info(
					"dev mode block forging enabled",
					"component", "ledger",
					"block_interval", blockInterval,
					"active_slots_coeff", activeSlotsCoeff.String(),
				)
			}
		}
	}
}

func (ls *LedgerState) scheduleCleanupConsumedUtxos() {
	ls.Lock()
	defer ls.Unlock()
	if ls.timerCleanupConsumedUtxos != nil {
		ls.timerCleanupConsumedUtxos.Stop()
	}
	ls.timerCleanupConsumedUtxos = time.AfterFunc(
		cleanupConsumedUtxosInterval,
		func() {
			ls.cleanupConsumedUtxos()
			// Schedule the next run
			ls.scheduleCleanupConsumedUtxos()
		},
	)
}

func (ls *LedgerState) cleanupConsumedUtxos() {
	// Get the current tip, since we're querying by slot
	tip := ls.Tip()
	// Delete UTxOs that are marked as deleted and older than our slot window
	ls.config.Logger.Debug(
		"cleaning up consumed UTxOs",
		"component", "ledger",
	)
	if tip.Point.Slot > cleanupConsumedUtxosSlotWindow {
		for {
			ls.Lock()
			count, err := ls.db.UtxosDeleteConsumed(
				tip.Point.Slot-cleanupConsumedUtxosSlotWindow,
				10000,
				nil,
			)
			ls.Unlock()
			if count == 0 {
				break
			}
			if err != nil {
				ls.config.Logger.Error(
					"failed to cleanup consumed UTxOs",
					"component", "ledger",
					"error", err,
				)
				break
			}
		}
	}
}

func (ls *LedgerState) rollback(point ocommon.Point) error {
	// Start a transaction
	txn := ls.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		// Delete rolled-back UTxOs
		err := ls.db.UtxosDeleteRolledback(point.Slot, txn)
		if err != nil {
			return fmt.Errorf("remove rolled-back UTxOs: %w", err)
		}
		// Restore spent UTxOs
		err = ls.db.UtxosUnspend(point.Slot, txn)
		if err != nil {
			return fmt.Errorf(
				"restore spent UTxOs after rollback: %w",
				err,
			)
		}
		// Update tip
		ls.currentTip = ochainsync.Tip{
			Point: point,
		}
		if point.Slot > 0 {
			rollbackBlock, err := ls.chain.BlockByPoint(point, txn)
			if err != nil {
				return fmt.Errorf("failed to get rollback block: %w", err)
			}
			ls.currentTip.BlockNumber = rollbackBlock.Number
		}
		if err = ls.db.SetTip(ls.currentTip, txn); err != nil {
			return fmt.Errorf("failed to set tip: %w", err)
		}
		ls.updateTipMetrics()
		return nil
	})
	if err != nil {
		return err
	}
	// Reload tip
	if err := ls.loadTip(); err != nil {
		return fmt.Errorf("failed to load tip: %w", err)
	}
	var hash string
	if point.Slot == 0 {
		hash = "<genesis>"
	} else {
		hash = hex.EncodeToString(point.Hash)
	}
	ls.config.Logger.Info(
		fmt.Sprintf(
			"chain rolled back, new tip: %s at slot %d",
			hash,
			point.Slot,
		),
		"component",
		"ledger",
	)
	return nil
}

func (ls *LedgerState) transitionToEra(
	txn *database.Txn,
	nextEraId uint,
	startEpoch uint64,
	addedSlot uint64,
) error {
	nextEra := eras.Eras[nextEraId]
	if nextEra.HardForkFunc != nil {
		// Perform hard fork
		// This generally means upgrading pparams from previous era
		newPParams, err := nextEra.HardForkFunc(
			ls.config.CardanoNodeConfig,
			ls.currentPParams,
		)
		if err != nil {
			return fmt.Errorf("hard fork failed: %w", err)
		}
		ls.currentPParams = newPParams
		ls.config.Logger.Debug(
			"updated protocol params",
			"pparams",
			fmt.Sprintf("%#v", ls.currentPParams),
		)
		// Write pparams update to DB
		pparamsCbor, err := cbor.Encode(&ls.currentPParams)
		if err != nil {
			return fmt.Errorf("failed to encode pparams: %w", err)
		}
		err = ls.db.SetPParams(
			pparamsCbor,
			addedSlot,
			startEpoch,
			nextEraId,
			txn,
		)
		if err != nil {
			return fmt.Errorf("failed to set pparams: %w", err)
		}
	}
	ls.currentEra = nextEra
	return nil
}

// consumeUtxo marks a UTxO as "deleted" without actually deleting it. This allows for a UTxO
// to be easily on rollback
func (ls *LedgerState) consumeUtxo(
	txn *database.Txn,
	utxoId ledger.TransactionInput,
	slot uint64,
) error {
	return ls.db.UtxoConsume(
		utxoId,
		slot,
		txn,
	)
}

// calculateStabilityWindow returns the stability window based on the current era.
// For Byron era, returns 2k. For Shelley+ eras, returns 3k/f.
// Returns the default threshold if genesis data is unavailable or invalid.
func (ls *LedgerState) calculateStabilityWindow() uint64 {
	if ls.config.CardanoNodeConfig == nil {
		ls.config.Logger.Warn(
			"cardano node config is nil, using default stability window",
		)
		return blockfetchBatchSlotThresholdDefault
	}

	// Byron era only needs Byron genesis
	if ls.currentEra.Id == 0 {
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		if byronGenesis == nil {
			return blockfetchBatchSlotThresholdDefault
		}
		k := byronGenesis.ProtocolConsts.K
		if k < 0 {
			ls.config.Logger.Warn("invalid negative security parameter", "k", k)
			return blockfetchBatchSlotThresholdDefault
		}
		if k == 0 {
			ls.config.Logger.Warn("security parameter is zero", "k", k)
			return blockfetchBatchSlotThresholdDefault
		}
		// Byron stability window is 2k slots
		return uint64(k) * 2 // #nosec G115
	}

	// Shelley+ eras only need Shelley genesis
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return blockfetchBatchSlotThresholdDefault
	}
	k := shelleyGenesis.SecurityParam
	if k < 0 {
		ls.config.Logger.Warn("invalid negative security parameter", "k", k)
		return blockfetchBatchSlotThresholdDefault
	}
	if k == 0 {
		ls.config.Logger.Warn("security parameter is zero", "k", k)
		return blockfetchBatchSlotThresholdDefault
	}
	securityParam := uint64(k)

	// Calculate 3k/f
	activeSlotsCoeff := shelleyGenesis.ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil {
		ls.config.Logger.Warn("ActiveSlotsCoeff.Rat is nil")
		return blockfetchBatchSlotThresholdDefault
	}

	if activeSlotsCoeff.Num().Sign() <= 0 {
		ls.config.Logger.Warn(
			"ActiveSlotsCoeff must be positive",
			"active_slots_coeff",
			activeSlotsCoeff.String(),
		)
		return blockfetchBatchSlotThresholdDefault
	}

	numerator := new(big.Int).SetUint64(securityParam)
	numerator.Mul(numerator, big.NewInt(3))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	denominator := new(big.Int).Set(activeSlotsCoeff.Num())
	window, remainder := new(
		big.Int,
	).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() != 0 {
		window.Add(window, big.NewInt(1))
	}
	if window.Sign() <= 0 {
		ls.config.Logger.Warn(
			"stability window calculation produced non-positive result",
			"security_param",
			securityParam,
			"active_slots_coeff",
			activeSlotsCoeff.String(),
		)
		return blockfetchBatchSlotThresholdDefault
	}
	if !window.IsUint64() {
		ls.config.Logger.Warn(
			"stability window calculation overflowed uint64",
			"security_param",
			securityParam,
			"active_slots_coeff",
			activeSlotsCoeff.String(),
			"window_num",
			window.String(),
		)
		return blockfetchBatchSlotThresholdDefault
	}
	return window.Uint64()
}

type readChainResult struct {
	rollbackPoint ocommon.Point
	blocks        []ledger.Block
	rollback      bool
}

func (ls *LedgerState) ledgerReadChain(resultCh chan readChainResult) {
	defer close(resultCh)
	// Create chain iterator
	iter, err := ls.chain.FromPoint(ls.currentTip.Point, false)
	if err != nil {
		ls.config.Logger.Error(
			"failed to create chain iterator: " + err.Error(),
		)
		return
	}
	// Read blocks from chain iterator and decode
	var next, cachedNext *chain.ChainIteratorResult
	var tmpBlock ledger.Block
	var needsRollback, shouldBlock bool
	var rollbackPoint ocommon.Point
	var result readChainResult
	for {
		select {
		case <-ls.ctx.Done():
			return
		default:
		}
		// We chose 500 as an arbitrary max batch size. A "chain extended" message will be logged after each batch
		nextBatch := make([]ledger.Block, 0, 500)
		// Gather up next batch of blocks
		for {
			if cachedNext != nil {
				next = cachedNext
				cachedNext = nil
			} else {
				next, err = iter.Next(shouldBlock)
				shouldBlock = false
				if err != nil {
					if !errors.Is(err, chain.ErrIteratorChainTip) {
						ls.config.Logger.Error(
							"failed to get next block from chain iterator: " + err.Error(),
						)
						return
					}
					shouldBlock = true
					// Break out of inner loop to flush DB transaction and log
					break
				}
			}
			if next == nil {
				ls.config.Logger.Error("next block from chain iterator is nil")
				return
			}
			if next.Rollback {
				// End existing batch and cache rollback if we have any blocks in the batch
				// We need special processing for rollbacks below
				if len(nextBatch) > 0 {
					cachedNext = next
					break
				}
				needsRollback = true
				rollbackPoint = next.Point
				break
			}
			// Decode block
			tmpBlock, err = next.Block.Decode()
			if err != nil {
				ls.config.Logger.Error(
					"failed to decode block: " + err.Error(),
				)
				return
			}
			// Add to batch
			nextBatch = append(
				nextBatch,
				tmpBlock,
			)
			// Don't exceed our pre-allocated capacity
			if len(nextBatch) == cap(nextBatch) {
				break
			}
		}
		if needsRollback {
			needsRollback = false
			result = readChainResult{
				rollback:      true,
				rollbackPoint: rollbackPoint,
			}
		} else {
			result = readChainResult{
				blocks: nextBatch,
			}
		}
		resultCh <- result
	}
}

func (ls *LedgerState) ledgerProcessBlocks() {
	// Start chain reader goroutine
	readChainResultCh := make(
		chan readChainResult,
		1,
	) // Add buffer to prevent blocking
	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
		ls.ledgerReadChain(readChainResultCh)
	}()
	// Process blocks
	var nextEpochEraId uint
	var needsEpochRollover bool
	var end, i int
	var txn *database.Txn
	var err error
	var nextBatch, cachedNextBatch []ledger.Block
	var delta *LedgerDelta
	var deltaBatch LedgerDeltaBatch
	shouldValidate := ls.config.ValidateHistorical
	for {
		select {
		case <-ls.ctx.Done():
			// Context cancelled, shutdown gracefully
			return
		default:
		}
		if needsEpochRollover {
			ls.Lock()
			needsEpochRollover = false
			txn := ls.db.Transaction(true)
			err = txn.Do(func(txn *database.Txn) error {
				// Check for era change
				if nextEpochEraId != ls.currentEra.Id {
					// Transition through every era between the current and the target era
					for nextEraId := ls.currentEra.Id + 1; nextEraId <= nextEpochEraId; nextEraId++ {
						if err := ls.transitionToEra(txn, nextEraId, ls.currentEpoch.EpochId, ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots)); err != nil {
							return err
						}
					}
				}
				// Process epoch rollover
				if err := ls.processEpochRollover(txn); err != nil {
					return err
				}
				return nil
			})
			ls.Unlock()
			if err != nil {
				ls.config.Logger.Error(
					"failed to process epoch rollover: " + err.Error(),
				)
				ls.cancel() // Cancel context on fatal error to stop other goroutines
				return
			}
		}
		if cachedNextBatch != nil {
			// Use cached block batch
			nextBatch = cachedNextBatch
			cachedNextBatch = nil
		} else {
			// Read next result from readChain channel
			select {
			case result, ok := <-readChainResultCh:
				if !ok {
					return
				}
				nextBatch = result.blocks
				// Process rollback
				if result.rollback {
					ls.Lock()
					if err = ls.rollback(result.rollbackPoint); err != nil {
						ls.Unlock()
						ls.config.Logger.Error(
							"failed to process rollback: " + err.Error(),
						)
						ls.cancel() // Cancel context on fatal error to stop other goroutines
						return
					}
					ls.Unlock()
					continue
				}
			case <-ls.ctx.Done():
				return
			}
		}
		// Process batch in groups of 50 to stay under DB txn limits
		for i = 0; i < len(nextBatch); i += 50 {
			ls.Lock()
			end = min(
				len(nextBatch),
				i+50,
			)
			txn = ls.db.Transaction(true)
			err = txn.Do(func(txn *database.Txn) error {
				deltaBatch = LedgerDeltaBatch{}
				for offset, next := range nextBatch[i:end] {
					tmpPoint := ocommon.Point{
						Slot: next.SlotNumber(),
						Hash: next.Hash().Bytes(),
					}
					// End processing of batch and cache remainder if we get a block from after the current epoch end, or if we need the initial epoch
					if tmpPoint.Slot >= (ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots)) ||
						ls.currentEpoch.SlotLength == 0 {
						needsEpochRollover = true
						nextEpochEraId = uint(next.Era().Id)
						// Cache rest of the batch for next loop
						cachedNextBatch = nextBatch[i+offset:]
						nextBatch = nil
						break
					}
					// Enable validation using the k-slot window from ShelleyGenesis.
					// Only recalculate if validation is not already enabled
					if !shouldValidate && i == 0 {
						var cutoffSlot uint64
						stabilityWindow := ls.calculateStabilityWindow()
						currentTipSlot := ls.currentTip.Point.Slot
						blockSlot := next.SlotNumber()
						if currentTipSlot >= stabilityWindow {
							cutoffSlot = currentTipSlot - stabilityWindow
						} else {
							cutoffSlot = 0
						}

						// Validate blocks within k-slot window, or historical blocks if ValidateHistorical enabled
						shouldValidate = blockSlot >= cutoffSlot ||
							ls.config.ValidateHistorical
					}
					// Process block
					delta, err = ls.ledgerProcessBlock(
						txn,
						tmpPoint,
						next,
						shouldValidate,
					)
					if err != nil {
						return err
					}
					if delta != nil {
						deltaBatch.addDelta(delta)
					}
					// Update tip
					ls.currentTip = ochainsync.Tip{
						Point:       tmpPoint,
						BlockNumber: next.BlockNumber(),
					}
					// Calculate block rolling nonce
					var blockNonce []byte
					if ls.currentEra.CalculateEtaVFunc != nil {
						tmpEra := eras.Eras[next.Era().Id]
						if tmpEra.CalculateEtaVFunc != nil {
							tmpNonce, err := tmpEra.CalculateEtaVFunc(
								ls.config.CardanoNodeConfig,
								ls.currentTipBlockNonce,
								next,
							)
							if err != nil {
								return fmt.Errorf("calculate etaV: %w", err)
							}
							blockNonce = tmpNonce
						}
					}
					// TODO: batch this
					// Determine epoch for this slot
					tmpEpoch, err := ls.SlotToEpoch(tmpPoint.Slot)
					if err != nil {
						return fmt.Errorf("slot->epoch: %w", err)
					}

					// First block we persist in the current epoch becomes the checkpoint
					isCheckpoint := false
					if tmpEpoch.EpochId == ls.currentEpoch.EpochId &&
						!ls.checkpointWrittenForEpoch {
						isCheckpoint = true
						ls.checkpointWrittenForEpoch = true
					}
					// Store block nonce in the DB
					if len(blockNonce) > 0 {
						err = ls.db.SetBlockNonce(
							tmpPoint.Hash,
							tmpPoint.Slot,
							blockNonce,
							isCheckpoint,
							txn,
						)
						if err != nil {
							return err
						}
						// Update tip block nonce
						ls.currentTipBlockNonce = blockNonce
					}
				}
				// Apply delta batch
				if err := deltaBatch.apply(ls, txn); err != nil {
					return err
				}
				// Update tip in database
				if err := ls.db.SetTip(ls.currentTip, txn); err != nil {
					return fmt.Errorf("failed to set tip: %w", err)
				}
				ls.updateTipMetrics()
				return nil
			})
			if err != nil {
				ls.Unlock()
				ls.config.Logger.Error(
					"failed to process block: " + err.Error(),
				)
				ls.cancel() // Cancel context on fatal error to stop other goroutines
				return
			}
			ls.Unlock()
			if needsEpochRollover {
				break
			}
		}
		if len(nextBatch) > 0 {
			ls.config.Logger.Info(
				fmt.Sprintf(
					"chain extended, new tip: %x at slot %d",
					ls.currentTip.Point.Hash,
					ls.currentTip.Point.Slot,
				),
				"component",
				"ledger",
			)
		}
	}
}

func (ls *LedgerState) ledgerProcessBlock(
	txn *database.Txn,
	point ocommon.Point,
	block ledger.Block,
	shouldValidate bool,
) (*LedgerDelta, error) {
	// Check that we're processing things in order
	if len(ls.currentTip.Point.Hash) > 0 {
		if string(
			block.PrevHash().Bytes(),
		) != string(
			ls.currentTip.Point.Hash,
		) {
			return nil, fmt.Errorf(
				"block %s (with prev hash %s) does not fit on current chain tip (%x)",
				block.Hash().String(),
				block.PrevHash().String(),
				ls.currentTip.Point.Hash,
			)
		}
	}
	// Process transactions
	var delta *LedgerDelta
	for i, tx := range block.Transactions() {
		if delta == nil {
			delta = &LedgerDelta{Point: point, BlockEraId: uint(block.Era().Id)}
		}

		// Calculate certificate deposits before validation if the transaction has certificates
		var deposits map[int]uint64
		if len(tx.Certificates()) > 0 {
			var err error
			deposits, err = ls.CalculateCertificateDeposits(
				tx.Certificates(),
			)
			if err != nil {
				return nil, fmt.Errorf(
					"calculate certificate deposits: %w",
					err,
				)
			}
		}

		// Validate transaction
		if shouldValidate {
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
					return nil, fmt.Errorf("TX validation failure: %w", err)
				}
			}
		}

		// Populate ledger delta from transaction with pre-calculated deposits
		delta.addTransactionWithDeposits(tx, i, deposits)

		// Apply delta immediately if we may need the data to validate the next TX
		if shouldValidate {
			if err := delta.apply(ls, txn); err != nil {
				return nil, err
			}
			delta = nil // reset
		}
	}
	return delta, nil
}

func (ls *LedgerState) updateTipMetrics() {
	// Update metrics
	ls.metrics.blockNum.Set(float64(ls.currentTip.BlockNumber))
	ls.metrics.slotNum.Set(float64(ls.currentTip.Point.Slot))
	ls.metrics.slotInEpoch.Set(
		float64(ls.currentTip.Point.Slot - ls.currentEpoch.StartSlot),
	)
}

func (ls *LedgerState) loadPParams() error {
	pparams, err := ls.db.GetPParams(
		ls.currentEpoch.EpochId,
		ls.currentEra.DecodePParamsFunc,
		nil,
	)
	if err != nil {
		return err
	}
	if pparams == nil {
		pparams, err = ls.loadGenesisProtocolParameters()
		if err != nil {
			return fmt.Errorf("bootstrap genesis protocol parameters: %w", err)
		}
	}
	ls.currentPParams = pparams
	return nil
}

func (ls *LedgerState) loadGenesisProtocolParameters() (lcommon.ProtocolParameters, error) {
	// Start with Shelley parameters as the base for all eras (Byron also uses Shelley as base)
	pparams, err := eras.HardForkShelley(ls.config.CardanoNodeConfig, nil)
	if err != nil {
		return nil, err
	}

	// If target era is Byron or Shelley, return the Shelley parameters
	if ls.currentEra.Id <= eras.ShelleyEraDesc.Id {
		return pparams, nil
	}

	// Chain through each era up to the target era
	for eraId := eras.AllegraEraDesc.Id; eraId <= ls.currentEra.Id; eraId++ {
		era := eras.GetEraById(eraId)
		if era == nil {
			return nil, fmt.Errorf("unknown era ID %d", eraId)
		}

		pparams, err = era.HardForkFunc(ls.config.CardanoNodeConfig, pparams)
		if err != nil {
			return nil, fmt.Errorf("era %s transition: %w", era.Name, err)
		}
	}

	return pparams, nil
}

func (ls *LedgerState) loadEpochs(txn *database.Txn) error {
	// Load and cache all epochs
	epochs, err := ls.db.GetEpochs(txn)
	if err != nil {
		return err
	}
	ls.epochCache = epochs
	if len(epochs) > 0 {
		// Set current epoch and era
		ls.currentEpoch = epochs[len(epochs)-1]
		eraDesc := eras.GetEraById(ls.currentEpoch.EraId)
		if eraDesc == nil {
			return fmt.Errorf("unknown era ID %d", ls.currentEpoch.EraId)
		}
		ls.currentEra = *eraDesc
		// Update metrics
		ls.metrics.epochNum.Set(float64(ls.currentEpoch.EpochId))
		return nil
	}
	// Populate initial epoch
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return errors.New("failed to load Shelley genesis")
	}
	startProtoVersion := shelleyGenesis.ProtocolParameters.ProtocolVersion.Major
	startEra := eras.ProtocolMajorVersionToEra[startProtoVersion]
	// Initialize current era to Byron when starting from genesis
	ls.currentEra = eras.Eras[0] // Byron era
	// Transition through every era between the current and the target era
	for nextEraId := ls.currentEra.Id + 1; nextEraId <= startEra.Id; nextEraId++ {
		if err := ls.transitionToEra(txn, nextEraId, ls.currentEpoch.EpochId, ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots)); err != nil {
			return err
		}
	}
	// Generate initial epoch
	if err := ls.processEpochRollover(txn); err != nil {
		return err
	}
	return nil
}

func (ls *LedgerState) loadTip() error {
	tmpTip, err := ls.db.GetTip(nil)
	if err != nil {
		return err
	}
	ls.currentTip = tmpTip
	// Load tip block and set cached block nonce
	if ls.currentTip.Point.Slot > 0 {
		tipNonce, err := ls.db.GetBlockNonce(
			tmpTip.Point,
			nil,
		)
		if err != nil {
			return err
		}
		ls.currentTipBlockNonce = tipNonce
	}
	ls.updateTipMetrics()
	return nil
}

func (ls *LedgerState) GetBlock(point ocommon.Point) (models.Block, error) {
	ret, err := ls.chain.BlockByPoint(point, nil)
	if err != nil {
		return models.Block{}, err
	}
	return ret, nil
}

// RecentChainPoints returns the requested count of recent chain points in descending order. This is used mostly
// for building a set of intersect points when acting as a chainsync client
func (ls *LedgerState) RecentChainPoints(count int) ([]ocommon.Point, error) {
	tmpBlocks, err := database.BlocksRecent(ls.db, count)
	if err != nil {
		return nil, err
	}
	ret := make([]ocommon.Point, 0, len(tmpBlocks))
	var tmpBlock models.Block
	for _, tmpBlock = range tmpBlocks {
		ret = append(
			ret,
			ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash),
		)
	}
	return ret, nil
}

// GetIntersectPoint returns the intersect between the specified points and the current chain
func (ls *LedgerState) GetIntersectPoint(
	points []ocommon.Point,
) (*ocommon.Point, error) {
	tip := ls.Tip()
	var ret ocommon.Point
	var tmpBlock models.Block
	var err error
	foundOrigin := false
	txn := ls.db.Transaction(false)
	err = txn.Do(func(txn *database.Txn) error {
		for _, point := range points {
			// Ignore points with a slot later than our current tip
			if point.Slot > tip.Point.Slot {
				continue
			}
			// Ignore points with a slot earlier than an existing match
			if point.Slot < ret.Slot {
				continue
			}
			// Check for special origin point
			if point.Slot == 0 && len(point.Hash) == 0 {
				foundOrigin = true
				continue
			}
			// Lookup block in metadata DB
			tmpBlock, err = ls.chain.BlockByPoint(point, txn)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf("failed to get block: %w", err)
			}
			// Update return value
			ret.Slot = tmpBlock.Slot
			ret.Hash = tmpBlock.Hash
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ret.Slot > 0 || foundOrigin {
		return &ret, nil
	}
	return nil, nil
}

// GetChainFromPoint returns a ChainIterator starting at the specified point. If inclusive is true, the iterator
// will start at the requested point, otherwise it will start at the next block.
func (ls *LedgerState) GetChainFromPoint(
	point ocommon.Point,
	inclusive bool,
) (*chain.ChainIterator, error) {
	return ls.chain.FromPoint(point, inclusive)
}

// Tip returns the current chain tip
func (ls *LedgerState) Tip() ochainsync.Tip {
	return ls.currentTip
}

// GetCurrentPParams returns the currentPParams value
func (ls *LedgerState) GetCurrentPParams() lcommon.ProtocolParameters {
	return ls.currentPParams
}

// UtxoByRef returns a single UTxO by reference
func (ls *LedgerState) UtxoByRef(
	txId []byte,
	outputIdx uint32,
) (*models.Utxo, error) {
	return ls.db.UtxoByRef(txId, outputIdx, nil)
}

// UtxosByAddress returns all UTxOs that belong to the specified address
func (ls *LedgerState) UtxosByAddress(
	addr ledger.Address,
) ([]models.Utxo, error) {
	ret := []models.Utxo{}
	utxos, err := ls.db.UtxosByAddress(addr, nil)
	if err != nil {
		return ret, err
	}
	ret = append(ret, utxos...)
	return ret, nil
}

// ValidateTx runs ledger validation on the provided transaction
func (ls *LedgerState) ValidateTx(
	tx lcommon.Transaction,
) error {
	if ls.currentEra.ValidateTxFunc != nil {
		txn := ls.db.Transaction(false)
		err := txn.Do(func(txn *database.Txn) error {
			lv := &LedgerView{
				txn: txn,
				ls:  ls,
			}
			err := ls.currentEra.ValidateTxFunc(
				tx,
				ls.currentTip.Point.Slot,
				lv,
				ls.currentPParams,
			)
			return err
		})
		if err != nil {
			return fmt.Errorf("TX %s failed validation: %w", tx.Hash(), err)
		}
	}
	return nil
}

// EvaluateTx evaluates the scripts in the provided transaction and returns the calculated
// fee, per-redeemer ExUnits, and total ExUnits
func (ls *LedgerState) EvaluateTx(
	tx lcommon.Transaction,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	var fee uint64
	var totalExUnits lcommon.ExUnits
	var redeemerExUnits map[lcommon.RedeemerKey]lcommon.ExUnits
	if ls.currentEra.EvaluateTxFunc != nil {
		txn := ls.db.Transaction(false)
		err := txn.Do(func(txn *database.Txn) error {
			lv := &LedgerView{
				txn: txn,
				ls:  ls,
			}
			var err error
			fee, totalExUnits, redeemerExUnits, err = ls.currentEra.EvaluateTxFunc(
				tx,
				lv,
				ls.currentPParams,
			)
			return err
		})
		if err != nil {
			return 0, lcommon.ExUnits{}, nil, fmt.Errorf(
				"TX %s failed evaluation: %w",
				tx.Hash(),
				err,
			)
		}
	}
	return fee, totalExUnits, redeemerExUnits, nil
}

// Sets the mempool for accessing transactions
func (ls *LedgerState) SetMempool(mempool MempoolProvider) {
	ls.mempool = mempool
}

// forgeBlock creates a conway block with transactions from mempool
// Also adds it to the primary chain
func (ls *LedgerState) forgeBlock() {
	// Get current chain tip
	currentTip := ls.chain.Tip()

	// Set Hash if empty
	if len(currentTip.Point.Hash) == 0 {
		currentTip.Point.Hash = make([]byte, 28)
	}

	// Calculate next slot and block number
	nextSlot, err := ls.TimeToSlot(time.Now())
	if err != nil {
		ls.config.Logger.Error(
			"failed to calculate slot from current time",
			"component", "ledger",
			"error", err,
		)
		return
	}
	nextBlockNumber := currentTip.BlockNumber + 1

	// Get current protocol parameters for limits
	pparams := ls.GetCurrentPParams()
	if pparams == nil {
		ls.config.Logger.Error(
			"failed to get protocol parameters",
			"component", "ledger",
		)
		return
	}

	// Safely cast protocol parameters to Conway type
	conwayPParams, ok := pparams.(*conway.ConwayProtocolParameters)
	if !ok {
		ls.config.Logger.Error(
			"protocol parameters are not Conway type",
			"component", "ledger",
		)
		return
	}

	var (
		transactionBodies      []conway.ConwayTransactionBody
		transactionWitnessSets []conway.ConwayTransactionWitnessSet
		blockSize              uint64
		totalExUnits           lcommon.ExUnits
		maxTxSize              = uint64(conwayPParams.MaxTxSize)
		maxBlockSize           = uint64(conwayPParams.MaxBlockBodySize)
		maxExUnits             = conwayPParams.MaxBlockExUnits
	)

	ls.config.Logger.Debug(
		"protocol parameter limits",
		"component", "ledger",
		"max_tx_size", maxTxSize,
		"max_block_size", maxBlockSize,
		"max_ex_units", maxExUnits,
	)

	var mempoolTxs []mempool.MempoolTransaction
	if ls.mempool != nil {
		mempoolTxs = ls.mempool.Transactions()
		ls.config.Logger.Debug(
			"found transactions in mempool",
			"component", "ledger",
			"tx_count", len(mempoolTxs),
		)

		// Iterate through transactions and add them until we hit limits
		for _, mempoolTx := range mempoolTxs {
			// Use raw CBOR from the mempool transaction
			txCbor := mempoolTx.Cbor
			txSize := uint64(len(txCbor))

			// Check MaxTxSize limit
			if txSize > maxTxSize {
				ls.config.Logger.Debug(
					"skipping transaction - exceeds MaxTxSize",
					"component", "ledger",
					"tx_size", txSize,
					"max_tx_size", maxTxSize,
				)
				continue
			}

			// Check MaxBlockSize limit
			if blockSize+txSize > maxBlockSize {
				ls.config.Logger.Debug(
					"block size limit reached",
					"component", "ledger",
					"current_size", blockSize,
					"tx_size", txSize,
					"max_block_size", maxBlockSize,
				)
				break
			}

			// Decode the transaction CBOR into full Conway transaction
			fullTx, err := conway.NewConwayTransactionFromCbor(txCbor)
			if err != nil {
				ls.config.Logger.Debug(
					"failed to decode full transaction, skipping",
					"component", "ledger",
					"error", err,
				)
				continue
			}

			// Pull ExUnits from redeemers in the witness set
			var txMemory, txSteps int64
			for _, redeemer := range fullTx.WitnessSet.Redeemers().Iter() {
				txMemory += redeemer.ExUnits.Memory
				txSteps += redeemer.ExUnits.Steps
			}
			estimatedTxExUnits := lcommon.ExUnits{
				Memory: txMemory,
				Steps:  txSteps,
			}

			// Check MaxExUnits limit
			if totalExUnits.Memory+estimatedTxExUnits.Memory > maxExUnits.Memory ||
				totalExUnits.Steps+estimatedTxExUnits.Steps > maxExUnits.Steps {
				ls.config.Logger.Debug(
					"ex units limit reached",
					"component", "ledger",
					"current_memory", totalExUnits.Memory,
					"current_steps", totalExUnits.Steps,
					"tx_memory", estimatedTxExUnits.Memory,
					"tx_steps", estimatedTxExUnits.Steps,
					"max_memory", maxExUnits.Memory,
					"max_steps", maxExUnits.Steps,
				)
				break
			}

			// Add transaction to our lists for later block creation
			transactionBodies = append(transactionBodies, fullTx.Body)
			transactionWitnessSets = append(
				transactionWitnessSets,
				fullTx.WitnessSet,
			)
			blockSize += txSize
			totalExUnits.Memory += estimatedTxExUnits.Memory
			totalExUnits.Steps += estimatedTxExUnits.Steps

			ls.config.Logger.Debug(
				"added transaction to block candidate lists",
				"component", "ledger",
				"tx_size", txSize,
				"block_size", blockSize,
				"tx_count", len(transactionBodies),
				"total_memory", totalExUnits.Memory,
				"total_steps", totalExUnits.Steps,
			)
		}
	}

	// Create Babbage block header body
	headerBody := babbage.BabbageBlockHeaderBody{
		BlockNumber: nextBlockNumber,
		Slot:        nextSlot,
		PrevHash:    lcommon.NewBlake2b256(currentTip.Point.Hash),
		IssuerVkey:  lcommon.IssuerVkey{},
		VrfKey:      []byte{},
		VrfResult: lcommon.VrfResult{
			Output: lcommon.Blake2b256{}.Bytes(),
		},
		BlockBodySize: blockSize,
		BlockBodyHash: lcommon.Blake2b256{},
		OpCert:        babbage.BabbageOpCert{},
		ProtoVersion: babbage.BabbageProtoVersion{
			Major: 8,
			Minor: 0,
		},
	}

	// Create Conway block header
	conwayHeader := &conway.ConwayBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body:      headerBody,
			Signature: []byte{},
		},
	}

	// Create a conway block with transactions
	conwayBlock := &conway.ConwayBlock{
		BlockHeader:            conwayHeader,
		TransactionBodies:      transactionBodies,
		TransactionWitnessSets: transactionWitnessSets,
		TransactionMetadataSet: map[uint]*cbor.LazyValue{},
		InvalidTransactions:    []uint{},
	}

	// Marshal the conway block to CBOR
	blockCbor, err := cbor.Encode(conwayBlock)
	if err != nil {
		ls.config.Logger.Error(
			"failed to marshal forged conway block to CBOR",
			"component", "ledger",
			"error", err,
		)
		return
	}

	// Re-decode block from CBOR
	// This is a bit of a hack, because things like Hash() rely on having the original CBOR available
	ledgerBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
	if err != nil {
		ls.config.Logger.Error(
			"failed to unmarshal forced Conway block from generated CBOR",
			"error", err,
		)
		return
	}

	// Add the block to the primary chain
	err = ls.chain.AddBlock(ledgerBlock, nil)
	if err != nil {
		ls.config.Logger.Error(
			"failed to add forged block to primary chain",
			"component", "ledger",
			"error", err,
		)
		return
	}

	// Log the successful block creation
	ls.config.Logger.Debug(
		"successfully forged and added conway block to primary chain",
		"component", "ledger",
		"slot", ledgerBlock.SlotNumber(),
		"hash", ledgerBlock.Hash(),
		"block_number", ledgerBlock.BlockNumber(),
		"prev_hash", ledgerBlock.PrevHash(),
		"block_size", blockSize,
		"tx_count", len(transactionBodies),
		"total_memory", totalExUnits.Memory,
		"total_steps", totalExUnits.Steps,
		"block_cbor", hex.EncodeToString(blockCbor),
	)
}
