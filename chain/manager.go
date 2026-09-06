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

package chain

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ChainId uint64

const (
	primaryChainId ChainId = 1
)

type ChainManager struct {
	db            *database.Database
	eventBus      *event.EventBus
	securityParam int
	chains        map[ChainId]*Chain
	// primary is immutable after loadPrimaryChain. Keeping the pointer
	// separately lets callers establish the chain -> primary -> manager lock
	// order without first reading cm.chains while a chain-creation writer may
	// already hold the primary-chain lock.
	primary             *Chain
	chainRollbackEvents map[ChainId][]uint64
	blockCache          *blockCache
	// rollbackPointNotOnChain counts rollback targets rejected because the
	// chain no longer holds the resolved block at its retained index. A
	// non-zero value means a peer (or local recovery) tried to splice a
	// continuation onto an abandoned fork; see Chain.rollbackPointBlock.
	rollbackPointNotOnChain prometheus.Counter
	mutex                   sync.RWMutex
}

func NewManager(
	db *database.Database,
	eventBus *event.EventBus,
	promRegistry ...prometheus.Registerer,
) (*ChainManager, error) {
	var registry prometheus.Registerer
	if len(promRegistry) > 0 {
		registry = promRegistry[0]
	}
	cm := &ChainManager{
		db:       db,
		eventBus: eventBus,
		chains:   make(map[ChainId]*Chain),
		chainRollbackEvents: make(
			map[ChainId][]uint64,
		),
		blockCache: newBlockCache(
			DefaultBlockCacheCapacity,
			registry,
		),
	}
	if registry != nil {
		cm.rollbackPointNotOnChain = promauto.With(registry).NewCounter(
			prometheus.CounterOpts{
				Name: "dingo_chain_rollback_point_not_on_chain_total",
				Help: "rollback targets rejected because the chain no longer holds the resolved block at its retained index",
			},
		)
	}
	if err := cm.loadPrimaryChain(); err != nil {
		return nil, err
	}
	return cm, nil
}

// recordRollbackPointNotOnChain increments the rejected-rollback-target
// counter when metrics are registered.
func (cm *ChainManager) recordRollbackPointNotOnChain() {
	if cm == nil || cm.rollbackPointNotOnChain == nil {
		return
	}
	cm.rollbackPointNotOnChain.Inc()
}

// SetLedger configures the Ouroboros security parameter K from the ledger.
// K must be positive; otherwise SetLedger returns ErrInvalidSecurityParam and
// leaves the previous configuration unchanged.
func (cm *ChainManager) SetLedger(
	ledgerState interface{ SecurityParam() int },
) error {
	k := ledgerState.SecurityParam()
	if k <= 0 {
		return fmt.Errorf(
			"%w: got %d",
			ErrInvalidSecurityParam,
			k,
		)
	}
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.securityParam = k
	return nil
}

// SecurityParam returns the configured Ouroboros security parameter K, or zero
// before SetLedger has run. SetLedger is not confined to startup — the state
// database can be reloaded while the node is serving chainsync — so readers
// outside the manager lock must go through here.
func (cm *ChainManager) SecurityParam() int {
	if cm == nil {
		return 0
	}
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.securityParam
}

func (cm *ChainManager) PrimaryChain() *Chain {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	chain := cm.primaryChainLocked()
	if chain == nil {
		panic("chain manager primary chain is not initialized")
	}
	return chain
}

// primaryChainLocked returns the primary chain without acquiring the mutex.
// The caller must already hold cm.mutex (read or write).
func (cm *ChainManager) primaryChainLocked() *Chain {
	if cm.chains == nil {
		return nil
	}
	return cm.chains[primaryChainId]
}

func (cm *ChainManager) primaryChain() (*Chain, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	chain := cm.primaryChainLocked()
	if chain == nil {
		return nil, errors.New("primary chain not available")
	}
	return chain, nil
}

func (cm *ChainManager) Chain(id ChainId) *Chain {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.chains[id]
}

// NewChain creates a new Chain that forks from the primary chain at the specified point. This is useful for managing outbound ChainSync clients
func (cm *ChainManager) NewChain(point ocommon.Point) (*Chain, error) {
	primaryChain, err := cm.primaryChain()
	if err != nil {
		return nil, err
	}
	primaryChain.mutex.Lock()
	defer primaryChain.mutex.Unlock()
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	if cm.primaryChainLocked() != primaryChain {
		return nil, errors.New("primary chain changed during fork creation")
	}
	intersectBlock, err := cm.blockByPoint(point, nil)
	if err != nil {
		return nil, err
	}
	// Increment current largest chain ID for new ID
	chainIds := slices.Sorted(maps.Keys(cm.chains))
	chainId := chainIds[len(chainIds)-1] + 1
	c := &Chain{
		id:                   chainId,
		manager:              cm,
		eventBus:             cm.eventBus,
		persistent:           false,
		lastCommonBlockIndex: intersectBlock.ID,
		tipBlockIndex:        intersectBlock.ID,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: intersectBlock.Slot,
				Hash: intersectBlock.Hash,
			},
			BlockNumber: intersectBlock.Number,
		},
	}
	cm.chains[chainId] = c
	return c, nil
}

// NewChainFromIntersect creates a new Chain that forks the primary chain at the latest common point.
func (cm *ChainManager) NewChainFromIntersect(
	points []ocommon.Point,
) (*Chain, error) {
	primaryChain, err := cm.primaryChain()
	if err != nil {
		return nil, err
	}
	primaryChain.mutex.Lock()
	defer primaryChain.mutex.Unlock()
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	if cm.primaryChainLocked() != primaryChain {
		return nil, errors.New("primary chain changed during fork creation")
	}
	tip := primaryChain.currentTip
	var intersectPoint ocommon.Point
	var intersectBlock models.Block
	foundOrigin := false
	txn := cm.db.BlobTxn(false)
	err = txn.Do(func(txn *database.Txn) error {
		for _, point := range points {
			// Ignore points with a slot later than our current tip
			if point.Slot > tip.Point.Slot {
				continue
			}
			// Ignore points with a slot earlier than an existing match
			if point.Slot < intersectPoint.Slot {
				continue
			}
			// Check for special origin point
			if point.Slot == 0 && len(point.Hash) == 0 {
				foundOrigin = true
				continue
			}
			// Lookup block in database
			intersectBlock, err = cm.blockByPoint(point, txn)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf("failed to get block: %w", err)
			}
			// Update return value
			intersectPoint.Slot = intersectBlock.Slot
			intersectPoint.Hash = intersectBlock.Hash
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if intersectPoint.Slot == 0 && !foundOrigin {
		return nil, ErrIntersectNotFound
	}
	// Increment current largest chain ID for new ID
	chainIds := slices.Sorted(maps.Keys(cm.chains))
	chainId := chainIds[len(chainIds)-1] + 1
	c := &Chain{
		id:                   chainId,
		manager:              cm,
		eventBus:             cm.eventBus,
		persistent:           false,
		lastCommonBlockIndex: intersectBlock.ID,
		tipBlockIndex:        intersectBlock.ID,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: intersectBlock.Slot,
				Hash: intersectBlock.Hash,
			},
			BlockNumber: intersectBlock.Number,
		},
	}
	cm.chains[chainId] = c
	return c, nil
}

func (cm *ChainManager) BlockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (models.Block, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.blockByPoint(point, txn)
}

func (cm *ChainManager) blockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (models.Block, error) {
	// Check in-memory cache
	if blk, ok := cm.blockCache.Get(point.Hash); ok {
		if blk.Slot == point.Slot {
			return blk, nil
		}
	}
	// Query database
	if cm.db != nil {
		var tmpBlock models.Block
		var err error
		if txn == nil {
			tmpBlock, err = database.BlockByPoint(cm.db, point)
		} else {
			tmpBlock, err = database.BlockByPointTxn(txn, point)
		}
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return models.Block{}, models.ErrBlockNotFound
			}
			return models.Block{}, err
		}
		return tmpBlock, nil
	}
	return models.Block{}, models.ErrBlockNotFound
}

func (cm *ChainManager) blockByHash(
	blockHash []byte,
) (models.Block, error) {
	// Check in-memory cache (block cache has its own locking).
	if blk, ok := cm.blockCache.Get(blockHash); ok {
		return blk, nil
	}
	// Fall through to database. Reconcile of a non-primary chain
	// walks back via prev-hash, and the ancestor it lands on may
	// still be present on the primary chain (so not in the cache,
	// which only holds rolled-back primary blocks and ephemeral
	// non-primary blocks).
	if cm.db != nil {
		blk, err := database.BlockByHash(cm.db, blockHash)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return models.Block{}, models.ErrBlockNotFound
			}
			return models.Block{}, err
		}
		return blk, nil
	}
	return models.Block{}, models.ErrBlockNotFound
}

// blockByIndexLocked resolves a block by index while preserving the manager's
// lock contract. The caller must hold cm.mutex. In-memory primary-chain
// storage is protected by the manager lock here, and callers that need a
// chain-level read lock acquire it before entering this helper.
func (cm *ChainManager) blockByIndexLocked(
	blockIndex uint64,
	txn *database.Txn,
) (models.Block, error) {
	// Query database when available.
	if cm.db != nil {
		tmpBlock, err := cm.db.BlockByIndex(blockIndex, txn)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return models.Block{}, models.ErrBlockNotFound
			}
			return models.Block{}, err
		}
		return tmpBlock, nil
	}
	// An in-memory manager has no index-backed store. Common blocks of an
	// ephemeral chain are still held by the primary chain, whose point buffer
	// resolves them through the manager's block cache. Callers of this helper
	// already hold the manager read/write lock when chain state must be
	// consistent, so use the lock-free primary lookup here just as the other
	// internal chain reconciliation paths do.
	if primaryChain := cm.primaryChainLocked(); primaryChain != nil &&
		!primaryChain.persistent {
		return primaryChain.blockByIndexLocked(blockIndex)
	}
	return models.Block{}, models.ErrBlockNotFound
}

func (cm *ChainManager) blockAtOrAfterIndex(
	blockIndex uint64,
	txn *database.Txn,
) (models.Block, error) {
	if cm.db == nil {
		return models.Block{}, models.ErrBlockNotFound
	}
	block, err := cm.db.BlockAtOrAfterIndex(blockIndex, txn)
	if errors.Is(err, models.ErrBlockNotFound) {
		return models.Block{}, models.ErrBlockNotFound
	}
	return block, err
}

func (cm *ChainManager) loadPrimaryChain() error {
	persistent := (cm.db != nil)
	chain := &Chain{
		id:         primaryChainId,
		manager:    cm,
		eventBus:   cm.eventBus,
		persistent: persistent,
	}
	if persistent {
		recentBlocks, err := database.BlocksRecent(cm.db, 1)
		if err != nil {
			return err
		}
		if len(recentBlocks) > 0 {
			chain.currentTip = ochainsync.Tip{
				Point: ocommon.Point{
					Slot: recentBlocks[0].Slot,
					Hash: recentBlocks[0].Hash,
				},
				BlockNumber: recentBlocks[0].Number,
			}
			chain.tipBlockIndex = recentBlocks[0].ID
		}
	}
	cm.chains[primaryChainId] = chain
	cm.primary = chain
	return nil
}

// RewindPrimaryChainToPoint prunes the persistent primary chain back to the
// specified point. It is used by the live primary-chain/ledger divergence
// reconciler, once ChainManager.SetLedger has configured the security
// parameter K.
//
// It shares its bound and side effects with a live Chain.Rollback rather
// than pruning blocks directly: the rewind is rejected outright, without
// touching any state, when it would exceed K, and a successful rewind
// publishes ChainRollbackEvent (and a ChainForkEvent when it actually
// removed blocks) and wakes/marks any chain iterators exactly once, the
// same signal NtC clients rely on for a live rollback. Previously this
// deleted blocks directly with no depth bound and no rollback/iterator
// signal, silently truncating the chain out from under downstream
// consumers (issue #3516).
//
// Do not call this before SetLedger: it returns ErrSecurityParamNotConfigured
// rather than silently pruning without a bound. RewindPrimaryChainAtStartup
// is for that case.
func (cm *ChainManager) RewindPrimaryChainToPoint(
	point ocommon.Point,
) error {
	primaryChain, err := cm.persistentPrimaryChain()
	if err != nil {
		return err
	}
	return primaryChain.Rollback(point)
}

// RewindPrimaryChainAtStartup prunes the persistent primary chain back to
// the specified point without requiring the security parameter K to be
// configured, for reconciling the primary chain against the ledger's own
// applied tip during startup -- before SetLedger has run (issue #3516
// review). It still publishes ChainRollbackEvent/ChainForkEvent and
// wakes/marks chain iterators exactly once, the same as
// RewindPrimaryChainToPoint; it only skips the K bound, since a startup
// gap between two already-durable local stores is not the untrusted-peer
// scenario that bound protects against. Never call this for a rollback an
// untrusted peer requested -- use RewindPrimaryChainToPoint (or
// SecurityParamConfigured to check readiness first) for anything reachable
// from chainsync.
func (cm *ChainManager) RewindPrimaryChainAtStartup(
	point ocommon.Point,
) error {
	primaryChain, err := cm.persistentPrimaryChain()
	if err != nil {
		return err
	}
	return primaryChain.RollbackUnbounded(point)
}

// SecurityParamConfigured reports whether SetLedger has configured the
// security parameter K yet. A caller reachable both before and after
// startup (e.g. the primary-chain/ledger divergence reconciler) uses this
// to choose between RewindPrimaryChainAtStartup and
// RewindPrimaryChainToPoint.
//
// This reads securityParam without cm.mutex, matching SetLedger's own
// unguarded write: both rely on SetLedger completing, during single-
// threaded startup composition, before any goroutine that could reach
// either side of this field exists (node.go constructs the ouroboros
// layer -- and with it every chainsync-reachable goroutine -- only after
// SetLedger returns).
func (cm *ChainManager) SecurityParamConfigured() bool {
	return cm.securityParam > 0
}

func (cm *ChainManager) persistentPrimaryChain() (*Chain, error) {
	primaryChain, err := cm.primaryChain()
	if err != nil {
		return nil, err
	}
	if !primaryChain.persistent {
		return nil, errors.New("primary chain is not persistent")
	}
	return primaryChain, nil
}

func (cm *ChainManager) addBlock(
	block models.Block,
	txn *database.Txn,
	persistent bool,
) error {
	if persistent {
		// Add block to database
		if err := cm.db.BlockCreate(block, txn); err != nil {
			return err
		}
	} else {
		// Add block to LRU cache (evicts oldest if at capacity)
		cm.blockCache.Put(block)
	}
	return nil
}

func (cm *ChainManager) removeBlockByIndex(
	blockIndex uint64,
) (models.Block, error) {
	// Record removed block event for each non-primary chain
	for chainId := range cm.chains {
		if chainId == primaryChainId {
			continue
		}
		cm.chainRollbackEvents[chainId] = append(
			cm.chainRollbackEvents[chainId],
			blockIndex,
		)
	}
	// Remove from database
	var removedBlock models.Block
	txn := cm.db.BlobTxn(true)
	err := txn.Do(func(txn *database.Txn) error {
		tmpBlock, err := cm.db.BlockByIndex(blockIndex, txn)
		if err != nil {
			return err
		}
		removedBlock = tmpBlock
		// Add block to LRU cache in case other chains are using it
		cm.blockCache.Put(tmpBlock)
		if err := database.BlockDeleteTxn(txn, tmpBlock); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return models.Block{}, err
	}
	return removedBlock, nil
}

func (cm *ChainManager) chainNeedsReconcile(
	chainId ChainId,
	lastCommonBlockIndex uint64,
) bool {
	events, ok := cm.chainRollbackEvents[chainId]
	if !ok {
		return false
	}
	ret := false
	for _, evtIndex := range events {
		if evtIndex <= lastCommonBlockIndex {
			ret = true
			break
		}
	}
	// Clear out events
	delete(cm.chainRollbackEvents, chainId)
	return ret
}
