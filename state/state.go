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

package state

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/state/eras"
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
	currentEra                  eras.EraDesc
	currentTip                  ochainsync.Tip
	currentTipBlockNonce        []byte
	metrics                     stateMetrics
	chainsyncHeaderPoints       []ocommon.Point
	chainsyncHeaderPointsMutex  sync.Mutex
	chainsyncBlockEvents        []BlockfetchEvent
	chainsyncBlockfetchBusy     bool
	chainsyncBlockfetchBusyTime time.Time
	chainsyncBlockfetchMutex    sync.Mutex
	chainsyncBlockfetchWaiting  bool
	chainsyncHeaderMutex        sync.Mutex
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
			return nil, err
		}
		ls.config.Logger.Warn(
			"database initialization error",
			"error",
			err,
			"component",
			"ledger",
		)
		// Run recovery
		if err := ls.recoverCommitTimestampConflict(); err != nil {
			return nil, err
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
	// Load current epoch from DB
	if err := ls.loadEpoch(); err != nil {
		return nil, err
	}
	// Load current protocol parameters from DB
	if err := ls.loadPParams(); err != nil {
		return nil, err
	}
	// Load current tip
	if err := ls.loadTip(); err != nil {
		return nil, err
	}
	// Start goroutine to process new blocks
	go ls.ledgerProcessBlocks()
	return ls, nil
}

func (ls *LedgerState) recoverCommitTimestampConflict() error {
	// Load current tip
	tmpTip, err := ls.db.GetTip(nil)
	if err != nil {
		return err
	}
	// Try to load last n blocks and rollback to the last one we can load
	recentBlocks, err := database.BlocksRecent(ls.db, 100)
	if err != nil {
		return err
	}
	for _, tmpBlock := range recentBlocks {
		blockPoint := ocommon.NewPoint(
			tmpBlock.Slot,
			tmpBlock.Hash,
		)
		// Nothing to do if current tip is earlier than our latest block
		if tmpTip.Point.Slot <= tmpBlock.Slot {
			break
		}
		// Rollback block if current tip is ahead
		if tmpTip.Point.Slot > tmpBlock.Slot {
			if err2 := ls.rollback(blockPoint); err2 != nil {
				return fmt.Errorf(
					"failed to rollback: %w",
					err2,
				)
			}
			return nil
		}
	}
	return errors.New("failed to recover database")
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
		// Remove rolled-back blocks in reverse order
		tmpBlocks, err := database.BlocksAfterSlotTxn(txn, point.Slot)
		if err != nil {
			return fmt.Errorf("query blocks: %w", err)
		}
		for _, tmpBlock := range tmpBlocks {
			if err := ls.removeBlock(txn, tmpBlock); err != nil {
				return fmt.Errorf("remove block: %w", err)
			}
		}
		// Delete rolled-back UTxOs
		err = ls.db.UtxosDeleteRolledback(point.Slot, txn)
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
		recentBlocks, err := database.BlocksRecentTxn(txn, 1)
		if err != nil {
			return err
		}
		if len(recentBlocks) > 0 {
			ls.currentTip = ochainsync.Tip{
				Point: ocommon.NewPoint(
					recentBlocks[0].Slot,
					recentBlocks[0].Hash,
				),
				BlockNumber: recentBlocks[0].Number,
			}
			if err := ls.db.SetTip(ls.currentTip, txn); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Reload tip
	if err := ls.loadTip(); err != nil {
		return err
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
			return err
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
			return err
		}
		err = ls.db.SetPParams(
			pparamsCbor,
			addedSlot,
			startEpoch,
			nextEraId,
			txn,
		)
		if err != nil {
			return err
		}
	}
	ls.currentEra = nextEra
	return nil
}

func (ls *LedgerState) applyPParamUpdates(
	txn *database.Txn,
	currentEpoch uint64,
	addedSlot uint64,
) error {
	// Check for pparam updates that apply at the end of the epoch
	pparamUpdates, err := ls.db.
		Metadata().
		GetPParamUpdates(currentEpoch, txn.Metadata())
	if err != nil {
		return err
	}
	if len(pparamUpdates) > 0 {
		// We only want the latest for the epoch
		pparamUpdate := pparamUpdates[0]
		if ls.currentEra.DecodePParamsUpdateFunc != nil {
			tmpPParamUpdate, err := ls.currentEra.DecodePParamsUpdateFunc(
				pparamUpdate.Cbor,
			)
			if err != nil {
				return err
			}
			if ls.currentEra.PParamsUpdateFunc != nil {
				// Update current pparams
				newPParams, err := ls.currentEra.PParamsUpdateFunc(
					ls.currentPParams,
					tmpPParamUpdate,
				)
				if err != nil {
					return err
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
					return err
				}
				err = ls.db.SetPParams(
					pparamsCbor,
					addedSlot,
					uint64(currentEpoch+1),
					ls.currentEra.Id,
					txn,
				)
				if err != nil {
					return err
				}
			}
		}
	}
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
	iter, err := newChainIterator(ls, ls.currentTip.Point, false)
	if err != nil {
		ls.config.Logger.Error(
			"failed to create chain iterator: " + err.Error(),
		)
		return
	}
	shouldBlock := false
	// We chose 500 as an arbitrary max batch size. A "chain extended" message will be logged after each batch
	nextBatch := make([]*ChainIteratorResult, 0, 500)
	for {
		// Gather up next batch of blocks
		for {
			next, err := iter.Next(shouldBlock)
			shouldBlock = false
			if err != nil {
				if !errors.Is(err, ErrIteratorChainTip) {
					ls.config.Logger.Error(
						"failed to get next block from chain iterator: " + err.Error(),
					)
					return
				}
				shouldBlock = true
				// Break out of inner loop to flush DB transaction and log
				break
			}
			if next == nil {
				ls.config.Logger.Error("next block from chain iterator is nil")
				return
			}
			nextBatch = append(nextBatch, next)
			// End batch if there's a rollback, since we need special processing
			if next.Rollback {
				break
			}
			// Don't exceed our pre-allocated capacity
			if len(nextBatch) == cap(nextBatch) {
				break
			}
		}
		// Process batch in groups of 50 to stay under DB txn limits
		needsRollback := false
		for i := 0; i < len(nextBatch); i += 50 {
			ls.Lock()
			end := min(
				len(nextBatch),
				i+50,
			)
			txn := ls.db.Transaction(true)
			err := txn.Do(func(txn *database.Txn) error {
				for _, next := range nextBatch[i:end] {
					// Rollbacks need to be handled outside of the batch DB transaction
					// A rollback should only occur at the end of a batch
					if next.Rollback {
						needsRollback = true
						return nil
					}
					// Process block
					tmpBlock, err := next.Block.Decode()
					if err != nil {
						return err
					}
					if err := ls.ledgerProcessBlock(txn, next.Point, tmpBlock, next.Block.Nonce); err != nil {
						return err
					}
				}
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
		// Process rollback from end of batch
		if needsRollback {
			needsRollback = false
			// The rollback should be at the end of the batch
			nextRollback := nextBatch[len(nextBatch)-1]
			ls.Lock()
			if err := ls.rollback(nextRollback.Point); err != nil {
				ls.Unlock()
				ls.config.Logger.Error(
					"failed to process rollback: " + err.Error(),
				)
				return
			}
			ls.Unlock()
			// Skip "chain extended" logging below if batch only contains a rollback
			if len(nextBatch) == 1 {
				// Clear out batch buffer
				nextBatch = slices.Delete(nextBatch, 0, len(nextBatch))
				continue
			}
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

func (ls *LedgerState) ledgerProcessBlock(txn *database.Txn, point ocommon.Point, block ledger.Block, nonce []byte) error {
	// Check that we're processing things in order
	nextBlockNumber := ls.currentTip.BlockNumber + 1
	if point.Slot == 0 {
		nextBlockNumber = 0
	}
	if block.BlockNumber() > 0 && block.BlockNumber() != nextBlockNumber {
		return fmt.Errorf(
			"out of order block detected: got %d, expected %d",
			block.BlockNumber(),
			nextBlockNumber,
		)
	}
	// Special handling for genesis block
	if err := ls.processGenesisBlock(txn, point, block); err != nil {
		return err
	}
	// Check for epoch rollover
	if err := ls.processEpochRollover(txn, point, block); err != nil {
		return err
	}
	// TODO: track this using protocol params and hard forks
	// Check for era change
	if uint(block.Era().Id) != ls.currentEra.Id {
		targetEraId := uint(block.Era().Id)
		// Transition through every era between the current and the target era
		for nextEraId := ls.currentEra.Id + 1; nextEraId <= targetEraId; nextEraId++ {
			if err := ls.transitionToEra(txn, nextEraId, ls.currentEpoch.EpochId, point.Slot); err != nil {
				return err
			}
		}
	}
	// Process transactions
	for _, tx := range block.Transactions() {
		if err := ls.processTransaction(txn, tx, point); err != nil {
			return err
		}
	}
	// Update tip
	ls.currentTip = ochainsync.Tip{
		Point:       point,
		BlockNumber: nextBlockNumber,
	}
	if err := ls.db.SetTip(ls.currentTip, txn); err != nil {
		return err
	}
	// Update tip block nonce
	ls.currentTipBlockNonce = nonce
	// Update metrics
	ls.metrics.blockNum.Set(float64(ls.currentTip.BlockNumber))
	ls.metrics.slotNum.Set(float64(point.Slot))
	ls.metrics.slotInEpoch.Set(float64(point.Slot - ls.currentEpoch.StartSlot))
	return nil
}

func (ls *LedgerState) removeBlock(
	txn *database.Txn,
	block database.Block,
) error {
	if err := database.BlockDeleteTxn(txn, block); err != nil {
		return err
	}
	return nil
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

func (ls *LedgerState) loadEpoch() error {
	tmpEpoch, err := ls.db.GetEpochLatest(nil)
	if err != nil {
		return err
	}
	ls.currentEpoch = tmpEpoch
	ls.currentEra = eras.Eras[tmpEpoch.EraId]
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
		tipBlock, err := database.BlockByPoint(ls.db, ls.currentTip.Point)
		if err != nil {
			return err
		}
		ls.currentTipBlockNonce = tipBlock.Nonce
	}
	// Update metrics
	ls.metrics.blockNum.Set(float64(ls.currentTip.BlockNumber))
	ls.metrics.slotNum.Set(float64(ls.currentTip.Point.Slot))
	ls.metrics.slotInEpoch.Set(
		float64(ls.currentTip.Point.Slot - ls.currentEpoch.StartSlot),
	)
	return nil
}

func (ls *LedgerState) GetBlock(point ocommon.Point) (*database.Block, error) {
	ret, err := database.BlockByPoint(ls.db, point)
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
	for _, tmpBlock := range tmpBlocks {
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
	foundOrigin := false
	txn := ls.db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
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
			tmpBlock, err := database.BlockByPoint(ls.db, point)
			if err != nil {
				if errors.Is(err, database.ErrBlockNotFound) {
					continue
				}
				return err
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
) (*ChainIterator, error) {
	return newChainIterator(ls, point, inclusive)
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
	for _, utxo := range utxos {
		tmpUtxo := database.Utxo(utxo)
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

// chainTip returns the raw chain tip by fetching the latest block
func (ls *LedgerState) chainTip(txn *database.Txn) (ochainsync.Tip, error) {
	var tmpBlocks []database.Block
	var err error
	if txn == nil {
		tmpBlocks, err = database.BlocksRecent(ls.db, 1)
	} else {
		tmpBlocks, err = database.BlocksRecentTxn(txn, 1)
	}
	if err != nil {
		return ochainsync.Tip{}, err
	}
	var tmpBlock database.Block
	if len(tmpBlocks) > 0 {
		tmpBlock = tmpBlocks[0]
	}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash),
		BlockNumber: tmpBlock.Number,
	}
	return tip, nil
}
