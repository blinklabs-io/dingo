// Copyright 2024 Blink Labs Software
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/blinklabs-io/node/config/cardano"
	"github.com/blinklabs-io/node/database"
	"github.com/blinklabs-io/node/event"
	"github.com/blinklabs-io/node/state/eras"
	"github.com/blinklabs-io/node/state/models"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

const (
	cleanupConsumedUtxosInterval   = 15 * time.Minute
	cleanupConsumedUtxosSlotWindow = 50000 // TODO: calculate this from params
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
	db                          database.Database
	timerCleanupConsumedUtxos   *time.Timer
	currentPParams              any
	currentEpoch                models.Epoch
	currentEra                  eras.EraDesc
	currentTip                  ochainsync.Tip
	metrics                     stateMetrics
	chainsyncHeaderPoints       []ocommon.Point
	chainsyncBlockEvents        []BlockfetchEvent
	chainsyncBlockfetchBusy     bool
	chainsyncBlockfetchBusyTime time.Time
	chainsyncBlockfetchWaiting  bool
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
	if cfg.DataDir == "" {
		db, err := database.NewInMemory(ls.config.Logger)
		if db == nil {
			ls.config.Logger.Error(
				"failed to create database",
				"error",
				"empty database returned",
				"component",
				"ledger",
			)
			return nil, fmt.Errorf("empty database returned")
		}
		ls.db = db
		if err != nil {
			if _, ok := err.(database.CommitTimestampError); !ok {
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
	} else {
		db, err := database.NewPersistent(cfg.DataDir, cfg.Logger)
		if db == nil {
			ls.config.Logger.Error(
				"failed to create database",
				"error",
				"empty database returned",
				"component",
				"ledger",
			)
			return nil, fmt.Errorf("empty database returned")
		}
		ls.db = db
		if err != nil {
			if _, ok := err.(database.CommitTimestampError); !ok {
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
	}
	// Create the table schemas
	for _, model := range models.MigrateModels {
		if err := ls.db.Metadata().AutoMigrate(model); err != nil {
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
	// Init metrics
	ls.metrics.init(ls.config.PromRegistry)
	// Schedule periodic process to purge consumed UTxOs outside of the rollback window
	ls.scheduleCleanupConsumedUtxos()
	// TODO: schedule process to scan/clean blob DB for keys that don't have a corresponding metadata DB entry
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
	return ls, nil
}

func (ls *LedgerState) recoverCommitTimestampConflict() error {
	// Try to load last n blocks and rollback to the last one we can load
	var tmpBlocks []models.Block
	result := ls.db.Metadata().Order("id DESC").Limit(100).Find(&tmpBlocks)
	if result.Error != nil {
		return result.Error
	}
	for _, tmpBlock := range tmpBlocks {
		blockPoint := ocommon.NewPoint(
			tmpBlock.Slot,
			tmpBlock.Hash,
		)
		// Load individual block to also (attempt to) load CBOR
		if _, err := models.BlockByPoint(ls.db, blockPoint); err == nil {
			if err2 := ls.rollback(blockPoint); err2 != nil {
				return fmt.Errorf(
					"failed to rollback: %s",
					err2,
				)
			}
			return nil
		}
	}
	return fmt.Errorf("failed to recover database")
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
			ls.Lock()
			defer func() {
				// Unlock ledger state
				ls.Unlock()
				// Schedule the next run
				ls.scheduleCleanupConsumedUtxos()
			}()
			// Get the current tip, since we're querying by slot
			tip := ls.Tip()
			for {
				// Perform updates in a transaction
				batchDone := false
				txn := ls.db.Transaction(true)
				err := txn.Do(func(txn *database.Txn) error {
					// Get UTxOs that are marked as deleted and older than our slot window
					var tmpUtxos []models.Utxo
					result := txn.Metadata().
						Where("deleted_slot <= ?", tip.Point.Slot-cleanupConsumedUtxosSlotWindow).
						Order("id DESC").Limit(1000).
						Find(&tmpUtxos)
					if result.Error != nil {
						return fmt.Errorf(
							"failed to query consumed UTxOs: %w",
							result.Error,
						)
					}
					if len(tmpUtxos) == 0 {
						batchDone = true
						return nil
					}
					// Delete the UTxOs
					if err := models.UtxosDeleteTxn(txn, tmpUtxos); err != nil {
						return fmt.Errorf(
							"failed to remove consumed UTxO: %w",
							err,
						)
					}
					return nil
				})
				if err != nil {
					ls.config.Logger.Error(
						"failed to update utxos",
						"component", "ledger",
						"error", err,
					)
					return
				}
				if batchDone {
					break
				}
			}
		},
	)
}

func (ls *LedgerState) rollback(point ocommon.Point) error {
	// Start a transaction
	txn := ls.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		// Remove rolled-back blocks in reverse order
		var tmpBlocks []models.Block
		result := txn.Metadata().
			Where("slot > ?", point.Slot).
			Order("slot DESC").
			Find(&tmpBlocks)
		if result.Error != nil {
			return fmt.Errorf("query blocks: %w", result.Error)
		}
		for _, tmpBlock := range tmpBlocks {
			if err := ls.removeBlock(txn, tmpBlock); err != nil {
				return fmt.Errorf("remove block: %w", err)
			}
		}
		// Delete rolled-back UTxOs
		var tmpUtxos []models.Utxo
		result = txn.Metadata().
			Where("added_slot > ?", point.Slot).
			Order("id DESC").
			Find(&tmpUtxos)
		if result.Error != nil {
			return fmt.Errorf("remove rolled-back UTxOs: %w", result.Error)
		}
		if len(tmpUtxos) > 0 {
			if err := models.UtxosDeleteTxn(txn, tmpUtxos); err != nil {
				return fmt.Errorf("remove rolled-back UTxOs: %w", err)
			}
		}
		// Restore spent UTxOs
		result = txn.Metadata().
			Model(models.Utxo{}).
			Where("deleted_slot > ?", point.Slot).
			Update("deleted_slot", 0)
		if result.Error != nil {
			return fmt.Errorf(
				"restore spent UTxOs after rollback: %w",
				result.Error,
			)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Generate event
	ls.config.EventBus.Publish(
		ChainRollbackEventType,
		event.NewEvent(
			ChainRollbackEventType,
			ChainRollbackEvent{
				Point: point,
			},
		),
	)
	ls.config.Logger.Info(
		fmt.Sprintf(
			"chain rolled back, new tip: %x at slot %d",
			point.Hash,
			point.Slot,
		),
		"component",
		"ledger",
	)
	return nil
}

func (ls *LedgerState) transitionToEra(txn *database.Txn, nextEraId uint, startEpoch uint, addedSlot uint64) error {
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
		tmpPParams := models.PParams{
			AddedSlot: addedSlot,
			Epoch:     startEpoch,
			EraId:     nextEraId,
			Cbor:      pparamsCbor,
		}
		if result := txn.Metadata().Create(&tmpPParams); result.Error != nil {
			return result.Error
		}
	}
	ls.currentEra = nextEra
	return nil
}

func (ls *LedgerState) applyPParamUpdates(txn *database.Txn, currentEpoch uint, addedSlot uint64) error {
	// Check for pparam updates that apply at the end of the epoch
	var pparamUpdates []models.PParamUpdate
	result := txn.Metadata().
		Where("epoch = ?", currentEpoch).
		Order("id DESC").
		Find(&pparamUpdates)
	if result.Error != nil {
		return result.Error
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
				tmpPParams := models.PParams{
					AddedSlot: addedSlot,
					Epoch:     currentEpoch + 1,
					EraId:     ls.currentEra.Id,
					Cbor:      pparamsCbor,
				}
				if result := txn.Metadata().Create(&tmpPParams); result.Error != nil {
					return result.Error
				}
			}
		}
	}
	return nil
}

func (ls *LedgerState) addUtxo(txn *database.Txn, utxo models.Utxo) error {
	// Add UTxO to blob DB
	key := models.UtxoBlobKey(utxo.TxId, utxo.OutputIdx)
	err := txn.Blob().Set(key, utxo.Cbor)
	if err != nil {
		return err
	}
	// Add to metadata DB
	if result := txn.Metadata().Create(&utxo); result.Error != nil {
		return result.Error
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
	// Find UTxO
	utxo, err := models.UtxoByRefTxn(txn, utxoId.Id().Bytes(), utxoId.Index())
	if err != nil {
		// TODO: make this configurable?
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		return err
	}
	// Mark as deleted in specified slot
	utxo.DeletedSlot = slot
	if result := txn.Metadata().Save(&utxo); result.Error != nil {
		return result.Error
	}
	return nil
}

func (ls *LedgerState) addBlock(txn *database.Txn, block models.Block) error {
	// Add block to blob DB
	key := models.BlockBlobKey(block.Slot, block.Hash)
	err := txn.Blob().Set(key, block.Cbor)
	if err != nil {
		return err
	}
	// Add to metadata DB
	if result := txn.Metadata().Create(&block); result.Error != nil {
		return result.Error
	}
	// Update tip
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(block.Slot, block.Hash),
		BlockNumber: block.Number,
	}
	// Update metrics
	ls.metrics.blockNum.Set(float64(block.Number))
	ls.metrics.slotNum.Set(float64(block.Slot))
	ls.metrics.slotInEpoch.Set(float64(block.Slot - ls.currentEpoch.StartSlot))
	return nil
}

func (ls *LedgerState) removeBlock(
	txn *database.Txn,
	block models.Block,
) error {
	// Remove from metadata DB
	if result := txn.Metadata().Delete(&block); result.Error != nil {
		return result.Error
	}
	// Remove from blob DB
	key := models.BlockBlobKey(block.Slot, block.Hash)
	err := txn.Blob().Delete(key)
	if err != nil {
		return err
	}
	// Update tip
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(block.Slot, block.Hash),
		BlockNumber: block.Number,
	}
	return nil
}

func (ls *LedgerState) loadPParams() error {
	var tmpPParams models.PParams
	result := ls.db.Metadata().Order("id DESC").First(&tmpPParams)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil
		}
		return result.Error
	}
	currentPParams, err := ls.currentEra.DecodePParamsFunc(
		tmpPParams.Cbor,
	)
	if err != nil {
		return err
	}
	ls.currentPParams = currentPParams
	return nil
}

func (ls *LedgerState) loadEpoch() error {
	tmpEpoch, err := models.EpochLatest(ls.db)
	if err != nil {
		if err != gorm.ErrRecordNotFound {
			return err
		}
	}
	ls.currentEpoch = tmpEpoch
	ls.currentEra = eras.Eras[tmpEpoch.EraId]
	// Update metrics
	ls.metrics.epochNum.Set(float64(ls.currentEpoch.EpochId))
	return nil
}

func (ls *LedgerState) loadTip() error {
	var tmpBlock models.Block
	result := ls.db.Metadata().Order("number DESC").First(&tmpBlock)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			// Return origin when we have no blocks
			ls.currentTip = ochainsync.Tip{
				Point: ocommon.NewPointOrigin(),
			}
			return nil
		}
		return result.Error
	}
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash),
		BlockNumber: tmpBlock.Number,
	}
	// Update metrics
	ls.metrics.blockNum.Set(float64(ls.currentTip.BlockNumber))
	ls.metrics.slotNum.Set(float64(ls.currentTip.Point.Slot))
	ls.metrics.slotInEpoch.Set(float64(ls.currentTip.Point.Slot - ls.currentEpoch.StartSlot))
	return nil
}

func (ls *LedgerState) GetBlock(point ocommon.Point) (*models.Block, error) {
	ret, err := models.BlockByPoint(ls.db, point)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

// RecentChainPoints returns the requested count of recent chain points in descending order. This is used mostly
// for building a set of intersect points when acting as a chainsync client
func (ls *LedgerState) RecentChainPoints(count int) ([]ocommon.Point, error) {
	var tmpBlocks []models.Block
	result := ls.db.Metadata().
		Order("number DESC").
		Limit(count).
		Find(&tmpBlocks)
	if result.Error != nil {
		return nil, result.Error
	}
	var ret []ocommon.Point
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
			// Lookup block in metadata DB
			tmpBlock, err := models.BlockByPoint(ls.db, point)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
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
	if ret.Slot > 0 {
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

// UtxoByRef returns a single UTxO by reference
func (ls *LedgerState) UtxoByRef(
	txId []byte,
	outputIdx uint32,
) (models.Utxo, error) {
	return models.UtxoByRef(ls.db, txId, outputIdx)
}

// UtxosByAddress returns all UTxOs that belong to the specified address
func (ls *LedgerState) UtxosByAddress(
	addr ledger.Address,
) ([]models.Utxo, error) {
	return models.UtxosByAddress(ls.db, addr)
}
