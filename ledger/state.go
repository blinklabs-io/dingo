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
	"io"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
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

	validateHistoricalThreshold = 14 * (24 * time.Hour) // 2 weeks
)

type ChainsyncState string

const (
	InitChainsyncState     ChainsyncState = "init"
	RollbackChainsyncState ChainsyncState = "rollback"
	SyncingChainsyncState  ChainsyncState = "syncing"
)

type LedgerStateConfig struct {
	Logger             *slog.Logger
	Database           *database.Database
	ChainManager       *chain.ChainManager
	EventBus           *event.EventBus
	CardanoNodeConfig  *cardano.CardanoNodeConfig
	PromRegistry       prometheus.Registerer
	ValidateHistorical bool
	ForgeBlocks        bool
	// Callback(s)
	BlockfetchRequestRangeFunc BlockfetchRequestRangeFunc
}

// BlockfetchRequestRangeFunc describes a callback function used to start a blockfetch request for
// a range of blocks
type BlockfetchRequestRangeFunc func(ouroboros.ConnectionId, ocommon.Point, ocommon.Point) error

// In ledger/state.go or a shared package
type MempoolProvider interface {
	Transactions() []mempool.MempoolTransaction
}
type LedgerState struct {
	sync.RWMutex
	chainsyncMutex                   sync.Mutex
	chainsyncState                   ChainsyncState
	config                           LedgerStateConfig
	db                               *database.Database
	timerCleanupConsumedUtxos        *time.Timer
	Scheduler                        *Scheduler
	currentPParams                   lcommon.ProtocolParameters
	currentEpoch                     database.Epoch
	epochCache                       []database.Epoch
	currentEra                       eras.EraDesc
	currentTip                       ochainsync.Tip
	currentTipBlockNonce             []byte
	metrics                          stateMetrics
	chainsyncBlockEvents             []BlockfetchEvent
	chainsyncBlockfetchBusyTime      time.Time
	chainsyncBlockfetchBatchDoneChan chan struct{}
	chainsyncBlockfetchReadyChan     chan struct{}
	chainsyncBlockfetchMutex         sync.Mutex
	chainsyncBlockfetchWaiting       bool
	chain                            *chain.Chain
	mempool                          MempoolProvider
}

func NewLedgerState(cfg LedgerStateConfig) (*LedgerState, error) {
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
	return ls, nil
}

func (ls *LedgerState) Start() error {
	// Init metrics
	ls.metrics.init(ls.config.PromRegistry)
	// Setup event handlers
	ls.config.EventBus.SubscribeFunc(
		ChainsyncEventType,
		ls.handleEventChainsync,
	)
	ls.config.EventBus.SubscribeFunc(
		BlockfetchEventType,
		ls.handleEventBlockfetch,
	)
	// Schedule periodic process to purge consumed UTxOs outside of the rollback window
	ls.scheduleCleanupConsumedUtxos()
	// Load epoch info from DB
	if err := ls.loadEpochs(nil); err != nil {
		return fmt.Errorf("failed to load epoch info: %w", err)
	}
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
	go ls.ledgerProcessBlocks()
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
			if activeSlotsCoeff.Rat != nil && activeSlotsCoeff.Rat.Num().Int64() > 0 {
				blockInterval := int((1 * activeSlotsCoeff.Rat.Denom().Int64()) / activeSlotsCoeff.Rat.Num().Int64())
				// Scheduled forgeBlock to run at the calculated block interval
				ls.Scheduler.Register(blockInterval, ls.forgeBlock)

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

type readChainResult struct {
	blocks        []ledger.Block
	rollback      bool
	rollbackPoint ocommon.Point
}

func (ls *LedgerState) ledgerReadChain(resultCh chan readChainResult) {
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
	readChainResultCh := make(chan readChainResult)
	go ls.ledgerReadChain(readChainResultCh)
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
		if needsEpochRollover {
			ls.Lock()
			needsEpochRollover = false
			txn := ls.db.Transaction(true)
			err := txn.Do(func(txn *database.Txn) error {
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
				return
			}
		}
		if cachedNextBatch != nil {
			// Use cached block batch
			nextBatch = cachedNextBatch
			cachedNextBatch = nil
		} else {
			// Read next result from readChain channel
			result, ok := <-readChainResultCh
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
					return
				}
				ls.Unlock()
				continue
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
						break
					}
					// Enable validation if we're getting near current tip
					if !shouldValidate && i == 0 {
						// Determine wall time for next block slot
						slotTime, err := ls.SlotToTime(tmpPoint.Slot)
						if err != nil {
							return fmt.Errorf(
								"convert slot to time: %w",
								err,
							)
						}
						// Check difference from current time
						timeDiff := time.Since(slotTime)
						if timeDiff < validateHistoricalThreshold {
							shouldValidate = true
							ls.config.Logger.Debug(
								"enabling validation as we approach tip",
							)
						}
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
					// TODO: batch this
					// Store block nonce in the DB
					err := ls.db.SetBlockNonce(
						tmpPoint.Hash,
						tmpPoint.Slot,
						blockNonce,
						false,
						txn,
					)
					if err != nil {
						return err
					}
					// Update tip block nonce
					ls.currentTipBlockNonce = blockNonce
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
				return
			}
			ls.Unlock()
			if needsEpochRollover {
				break
			}
		}
		if len(nextBatch) > 0 {
			var hash string
			if ls.currentTip.Point.Slot == 0 {
				hash = "<genesis>"
			} else {
				hash = hex.EncodeToString(ls.currentTip.Point.Hash)
			}
			ls.config.Logger.Info(
				fmt.Sprintf(
					"chain extended, new tip: %s at slot %d",
					hash,
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
	for _, tx := range block.Transactions() {
		if delta == nil {
			delta = &LedgerDelta{
				Point: point,
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
					ls.config.Logger.Warn(
						"TX " + tx.Hash().
							String() +
							" failed validation: " + err.Error(),
					)
					// return fmt.Errorf("TX validation failure: %w", err)
				}
			}
		}
		// Populate ledger delta from transaction
		if err := delta.processTransaction(tx); err != nil {
			return nil, fmt.Errorf("process transaction: %w", err)
		}
		// Apply delta immediately if we may need the data to validate the next TX
		if shouldValidate {
			if err := delta.apply(ls, txn); err != nil {
				return nil, err
			}
			delta = nil
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
	ls.currentPParams = pparams
	return nil
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
		ls.currentEra = eras.Eras[ls.currentEpoch.EraId]
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
			tmpTip.Point.Hash,
			tmpTip.Point.Slot,
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

func (ls *LedgerState) GetBlock(point ocommon.Point) (*database.Block, error) {
	ret, err := ls.chain.BlockByPoint(point, nil)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// RecentChainPoints returns the requested count of recent chain points in descending order. This is used mostly
// for building a set of intersect points when acting as a chainsync client
func (ls *LedgerState) RecentChainPoints(count int) ([]ocommon.Point, error) {
	tmpBlocks, err := database.BlocksRecent(ls.db, count)
	if err != nil {
		return nil, err
	}
	ret := []ocommon.Point{}
	var tmpBlock database.Block
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
	var tmpBlock database.Block
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
				if errors.Is(err, chain.ErrBlockNotFound) {
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
) (database.Utxo, error) {
	return ls.db.UtxoByRef(txId, outputIdx, nil)
}

// UtxosByAddress returns all UTxOs that belong to the specified address
func (ls *LedgerState) UtxosByAddress(
	addr ledger.Address,
) ([]database.Utxo, error) {
	ret := []database.Utxo{}
	utxos, err := ls.db.UtxosByAddress(addr, nil)
	if err != nil {
		return ret, err
	}
	var tmpUtxo database.Utxo
	for _, utxo := range utxos {
		tmpUtxo = database.Utxo(utxo)
		ret = append(ret, tmpUtxo)
	}
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

// Sets the mempool for accessing transactions
func (ls *LedgerState) SetMempool(mempool MempoolProvider) {
	ls.mempool = mempool
}

// forgeBlock creates a conway block with transactions from mempool
// Also adds it to the primary chain
func (ls *LedgerState) forgeBlock() {
	// Get current chain tip
	currentTip := ls.chain.Tip()

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

	var (
		transactionBodies      []conway.ConwayTransactionBody
		transactionWitnessSets []conway.ConwayTransactionWitnessSet
		blockSize              uint64
		totalExUnits           lcommon.ExUnits
		maxTxSize              = uint64(pparams.(*conway.ConwayProtocolParameters).MaxTxSize)
		maxBlockSize           = uint64(pparams.(*conway.ConwayProtocolParameters).MaxBlockBodySize)
		maxExUnits             = pparams.(*conway.ConwayProtocolParameters).MaxBlockExUnits
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
			var txMemory, txSteps uint64
			fullTx.WitnessSet.Redeemers().Iter()(
				func(_ lcommon.RedeemerKey, redeemer lcommon.RedeemerValue) bool {
					txMemory += redeemer.ExUnits.Memory
					txSteps += redeemer.ExUnits.Steps
					return true
				},
			)
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
			transactionWitnessSets = append(transactionWitnessSets, fullTx.WitnessSet)
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
		BlockNumber:   nextBlockNumber,
		Slot:          nextSlot,
		PrevHash:      lcommon.NewBlake2b256(currentTip.Point.Hash),
		IssuerVkey:    lcommon.IssuerVkey{},
		VrfKey:        []byte{},
		VrfResult:     lcommon.VrfResult{},
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

	// Log the generated block CBOR (DEBUG level)
	ls.config.Logger.Debug(
		"forged empty conway block",
		"component", "ledger",
		"slot", nextSlot,
		"block_number", nextBlockNumber,
		"prev_hash", hex.EncodeToString(currentTip.Point.Hash),
		"block_cbor", hex.EncodeToString(blockCbor),
	)

	// Create a proper ledger.Block from the Conway block
	ledgerBlock := ledger.Block(conwayBlock)

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

	// Log the successful block creation (Info)
	ls.config.Logger.Info(
		"successfully forged and added conway block to primary chain",
		"component", "ledger",
		"slot", nextSlot,
		"block_number", nextBlockNumber,
		"prev_hash", hex.EncodeToString(currentTip.Point.Hash),
		"block_size", blockSize,
		"tx_count", len(transactionBodies),
		"total_memory", totalExUnits.Memory,
		"total_steps", totalExUnits.Steps,
	)
}
