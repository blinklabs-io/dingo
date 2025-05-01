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
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	cleanupConsumedUtxosInterval   = 5 * time.Minute
	cleanupConsumedUtxosSlotWindow = 50000 // TODO: calculate this from params (#395)
)

type LedgerStateConfig struct {
	Logger            *slog.Logger
	DataDir           string
	EventBus          *event.EventBus
	CardanoNodeConfig *cardano.CardanoNodeConfig
	PromRegistry      prometheus.Registerer
	// Callback(s)
	BlockfetchRequestRangeFunc BlockfetchRequestRangeFunc
}

// BlockfetchRequestRangeFunc describes a callback function used to start a blockfetch request for
// a range of blocks
type BlockfetchRequestRangeFunc func(ouroboros.ConnectionId, ocommon.Point, ocommon.Point) error

type LedgerState struct {
	sync.RWMutex
	chainsyncMutex              sync.Mutex
	config                      LedgerStateConfig
	db                          *database.Database
	timerCleanupConsumedUtxos   *time.Timer
	currentPParams              lcommon.ProtocolParameters
	currentEpoch                database.Epoch
	epochCache                  []database.Epoch
	currentEra                  eras.EraDesc
	currentTip                  ochainsync.Tip
	currentTipBlockNonce        []byte
	metrics                     stateMetrics
	chainsyncBlockEvents        []BlockfetchEvent
	chainsyncBlockfetchBusyTime time.Time
	chainsyncBlockfetchDoneChan chan struct{}
	chainsyncBlockfetchMutex    sync.Mutex
	chainsyncBlockfetchWaiting  bool
	chain                       *chain.Chain
}

func NewLedgerState(cfg LedgerStateConfig) (*LedgerState, error) {
	ls := &LedgerState{
		config: cfg,
	}
	if cfg.Logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Init metrics
	ls.metrics.init(ls.config.PromRegistry)
	// Load database
	needsRecovery := false
	db, err := database.New(cfg.Logger, cfg.DataDir)
	if db == nil {
		ls.config.Logger.Error(
			"failed to create database",
			"error",
			"empty database returned",
			"component",
			"ledger",
		)
		return nil, errors.New("empty database returned")
	}
	ls.db = db
	if err != nil {
		var dbErr database.CommitTimestampError
		if !errors.As(err, &dbErr) {
			return nil, fmt.Errorf("failed to open database: %w", err)
		}
		ls.config.Logger.Warn(
			"database initialization error, needs recovery",
			"error",
			err,
			"component",
			"ledger",
		)
		needsRecovery = true
	}
	// Load chain
	chain, err := chain.NewChain(
		ls.db,
		ls.config.EventBus,
		true, // persistent
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load chain: %w", err)
	}
	ls.chain = chain
	// Run recovery if needed
	if needsRecovery {
		if err := ls.recoverCommitTimestampConflict(); err != nil {
			return nil, fmt.Errorf("failed to recover database: %w", err)
		}
	}
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
		return nil, fmt.Errorf("failed to load epoch info: %w", err)
	}
	// Load current protocol parameters from DB
	if err := ls.loadPParams(); err != nil {
		return nil, fmt.Errorf("failed to load pparams: %w", err)
	}
	// Load current tip
	if err := ls.loadTip(); err != nil {
		return nil, fmt.Errorf("failed to load tip: %w", err)
	}
	// Create genesis block
	if err := ls.createGenesisBlock(); err != nil {
		return nil, fmt.Errorf("failed to create genesis block: %w", err)
	}
	// Start goroutine to process new blocks
	go ls.ledgerProcessBlocks()
	return ls, nil
}

func (ls *LedgerState) recoverCommitTimestampConflict() error {
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

func (ls *LedgerState) scheduleCleanupConsumedUtxos() {
	ls.Lock()
	defer ls.Unlock()
	if ls.timerCleanupConsumedUtxos != nil {
		ls.timerCleanupConsumedUtxos.Stop()
	}
	ls.timerCleanupConsumedUtxos = time.AfterFunc(
		cleanupConsumedUtxosInterval,
		func() {
			defer func() {
				// Schedule the next run
				ls.scheduleCleanupConsumedUtxos()
			}()
			// Get the current tip, since we're querying by slot
			tip := ls.Tip()
			// Delete UTxOs that are marked as deleted and older than our slot window
			ls.config.Logger.Debug(
				"cleaning up consumed UTxOs",
				"component", "ledger",
			)
			ls.Lock()
			err := ls.db.UtxosDeleteConsumed(
				tip.Point.Slot-cleanupConsumedUtxosSlotWindow,
				nil,
			)
			ls.Unlock()
			if err != nil {
				ls.config.Logger.Error(
					"failed to cleanup consumed UTxOs",
					"component", "ledger",
					"error", err,
				)
				return
			}
		},
	)
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

func (ls *LedgerState) ledgerProcessBlocks() {
	iter, err := ls.chain.FromPoint(ls.currentTip.Point, false)
	if err != nil {
		ls.config.Logger.Error(
			"failed to create chain iterator: " + err.Error(),
		)
		return
	}
	shouldBlock := false
	// We chose 500 as an arbitrary max batch size. A "chain extended" message will be logged after each batch
	nextBatch := make([]*chain.ChainIteratorResult, 0, 500)
	var next, nextRollback, cachedNext *chain.ChainIteratorResult
	var tmpBlock ledger.Block
	var needsRollback, needsEpochRollover bool
	var nextEpochEraId uint
	var end, i int
	var txn *database.Txn
	for {
		if needsEpochRollover {
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
			if err != nil {
				ls.config.Logger.Error(
					"failed to process epoch rollover: " + err.Error(),
				)
				return
			}
		}
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
			// End batch and cache next if we get a block from after the current epoch end, or if we need the initial epoch
			if next.Point.Slot >= (ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots)) || ls.currentEpoch.SlotLength == 0 {
				cachedNext = next
				needsEpochRollover = true
				// Decode next block to get era ID
				tmpBlock, err = next.Block.Decode()
				if err != nil {
					ls.config.Logger.Error(
						"failed to decode block: " + err.Error(),
					)
					return
				}
				nextEpochEraId = uint(tmpBlock.Era().Id)
				break
			}
			if next.Rollback {
				// End existing batch and cache rollback if we have any blocks in the batch
				// We need special processing for rollbacks below
				if len(nextBatch) > 0 {
					cachedNext = next
					break
				}
				needsRollback = true
			}
			// Add to batch
			nextBatch = append(nextBatch, next)
			// Don't exceed our pre-allocated capacity
			if len(nextBatch) == cap(nextBatch) {
				break
			}
		}
		// Process rollback
		if needsRollback {
			needsRollback = false
			// The rollback should be alone in the batch
			nextRollback = nextBatch[0]
			ls.Lock()
			if err = ls.rollback(nextRollback.Point); err != nil {
				ls.Unlock()
				ls.config.Logger.Error(
					"failed to process rollback: " + err.Error(),
				)
				return
			}
			ls.Unlock()
			// Clear out batch buffer
			nextBatch = slices.Delete(nextBatch, 0, len(nextBatch))
			continue
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
				for _, next := range nextBatch[i:end] {
					// Process block
					tmpBlock, err = next.Block.Decode()
					if err != nil {
						return fmt.Errorf("block decode failed: %w", err)
					}
					if err = ls.ledgerProcessBlock(txn, next.Point, tmpBlock); err != nil {
						return err
					}
					// Update tip
					ls.currentTip = ochainsync.Tip{
						Point:       next.Point,
						BlockNumber: next.Block.Number,
					}
					// Update tip block nonce
					ls.currentTipBlockNonce = next.Block.Nonce
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
		}
		if len(nextBatch) > 0 {
			// Clear out batch buffer
			nextBatch = slices.Delete(nextBatch, 0, len(nextBatch))
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
) error {
	// Check that we're processing things in order
	if len(ls.currentTip.Point.Hash) > 0 {
		if string(
			block.PrevHash().Bytes(),
		) != string(
			ls.currentTip.Point.Hash,
		) {
			return fmt.Errorf(
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
					"TX " + tx.Hash().
						String() +
						" failed validation: " + err.Error(),
				)
				// return fmt.Errorf("TX validation failure: %w", err)
			}
		}
		// Populate ledger delta from transaction and apply
		delta = &LedgerDelta{
			Point: point,
		}
		if err := delta.processTransaction(tx); err != nil {
			return fmt.Errorf("process transaction: %w", err)
		}
		if err := delta.apply(ls, txn); err != nil {
			return fmt.Errorf("apply ledger delta: %w", err)
		}
	}
	return nil
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
	// Set current epoch
	if len(epochs) > 0 {
		ls.currentEpoch = epochs[len(epochs)-1]
		ls.currentEra = eras.Eras[ls.currentEpoch.EraId]
	}
	// Update metrics
	ls.metrics.epochNum.Set(float64(ls.currentEpoch.EpochId))
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
		tipBlock, err := ls.chain.BlockByPoint(ls.currentTip.Point, nil)
		if err != nil {
			return fmt.Errorf("failed to get tip block: %w", err)
		}
		ls.currentTipBlockNonce = tipBlock.Nonce
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
