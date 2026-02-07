// Copyright 2026 Blink Labs Software
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

package snapshot

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Calculator calculates stake distribution from the current ledger state.
type Calculator struct {
	db     *database.Database
	logger *slog.Logger
}

// NewCalculator creates a new stake calculator.
func NewCalculator(db *database.Database) *Calculator {
	logger := db.Logger()
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Calculator{db: db, logger: logger}
}

// StakeDistribution represents the stake distribution at a point in time.
// Uses ledger types for interoperability between database and ledger layers.
type StakeDistribution struct {
	Slot           uint64                         // Slot at which distribution was captured
	PoolStakes     map[lcommon.PoolKeyHash]uint64 // pool key hash -> total stake
	DelegatorCount map[lcommon.PoolKeyHash]uint64 // pool key hash -> delegator count
	TotalStake     uint64                         // Sum of all pool stakes
	TotalPools     uint64                         // Number of active pools
}

// CalculateStakeDistribution calculates the stake distribution at a given slot.
// This aggregates all delegated stake by pool from the account and UTxO tables.
//
// Pool selection is slot-aware: only pools registered at or before the
// requested slot are included. Stake aggregation currently uses the current
// ledger state (delegator counts from the accounts table; stake values are
// zero placeholders). A future slot-aware GetStakeByPoolsAtSlot method is
// needed for full consensus-grade accuracy; see getBatchPoolsDelegatedStake.
func (c *Calculator) CalculateStakeDistribution(
	ctx context.Context,
	slot uint64,
) (*StakeDistribution, error) {
	dist := &StakeDistribution{
		Slot:           slot,
		PoolStakes:     make(map[lcommon.PoolKeyHash]uint64),
		DelegatorCount: make(map[lcommon.PoolKeyHash]uint64),
	}

	// Read-only transaction so the entire calculation observes a
	// consistent database snapshot.
	txn := c.db.Transaction(false)
	defer func() { _ = txn.Commit() }()

	err := c.calculateFromAccounts(ctx, txn, slot, dist)
	if err != nil {
		return nil, fmt.Errorf("calculate from accounts: %w", err)
	}

	// Count total pools
	dist.TotalPools = uint64(len(dist.PoolStakes))

	return dist, nil
}

// calculateFromAccounts aggregates stake by querying accounts and their UTxOs.
// Uses batched queries to avoid N+1 database patterns.
func (c *Calculator) calculateFromAccounts(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	dist *StakeDistribution,
) error {
	meta := c.db.Metadata()
	metaTxn := (*txn).Metadata()

	// Get all active pools at the given slot.
	// Returns types.ErrNoEpochData (wrapped) if epoch data is not yet synced.
	pools, err := c.getActivePoolsAtSlot(ctx, meta, metaTxn, slot)
	if err != nil {
		return fmt.Errorf("get active pools: %w", err)
	}

	// If no pools found, return empty distribution (not an error)
	if len(pools) == 0 {
		return nil
	}

	// Batch fetch delegated stake for all pools in a single query
	stakeMap, delegatorMap, err := c.getBatchPoolsDelegatedStake(
		ctx,
		meta,
		metaTxn,
		pools,
	)
	if err != nil {
		return fmt.Errorf("get batch pools delegated stake: %w", err)
	}

	// Populate distribution from the batched results
	for _, poolHash := range pools {
		delegators := delegatorMap[poolHash]
		if delegators > 0 {
			// Record pool with delegators
			stake := stakeMap[poolHash]
			dist.PoolStakes[poolHash] = stake
			dist.DelegatorCount[poolHash] = delegators
			dist.TotalStake += stake
		}
	}

	return nil
}

// getActivePoolsAtSlot returns all pool key hashes that were active at the slot.
// A pool is active if it has a registration with added_slot <= slot and either
// no retirement or retirement.epoch > epoch at slot.
func (c *Calculator) getActivePoolsAtSlot(
	_ context.Context,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	slot uint64,
) ([]lcommon.PoolKeyHash, error) {
	// Query active pool key hashes at the given slot from the metadata store
	poolKeyHashBytes, err := meta.GetActivePoolKeyHashesAtSlot(slot, metaTxn)
	if err != nil {
		return nil, fmt.Errorf("get active pool key hashes at slot: %w", err)
	}

	// Convert [][]byte to []lcommon.PoolKeyHash
	pools := make([]lcommon.PoolKeyHash, 0, len(poolKeyHashBytes))
	for _, hashBytes := range poolKeyHashBytes {
		if len(hashBytes) != 28 {
			// Skip invalid pool key hashes (must be 28 bytes)
			continue
		}
		var poolHash lcommon.PoolKeyHash
		copy(poolHash[:], hashBytes)
		pools = append(pools, poolHash)
	}

	return pools, nil
}

// getBatchPoolsDelegatedStake returns stake for all pools in a single batch
// query. Returns maps of pool hash -> total stake and pool hash -> delegator
// count. Stake values are currently zero (placeholder) until UTxO aggregation
// is implemented; delegator counts reflect the current ledger state.
//
// TODO: Replace GetStakeByPools with a slot-aware GetStakeByPoolsAtSlot that:
//  1. Filters accounts active at the given slot (added_slot <= slot)
//  2. Considers delegation certificates at or before the slot
//  3. Aggregates UTxO values at the slot (created_slot <= slot AND
//     (spent_slot IS NULL OR spent_slot > slot))
func (c *Calculator) getBatchPoolsDelegatedStake(
	_ context.Context,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	pools []lcommon.PoolKeyHash,
) (map[lcommon.PoolKeyHash]uint64, map[lcommon.PoolKeyHash]uint64, error) {
	// Initialize result maps
	stakeMap := make(map[lcommon.PoolKeyHash]uint64, len(pools))
	delegatorMap := make(map[lcommon.PoolKeyHash]uint64, len(pools))

	if len(pools) == 0 {
		return stakeMap, delegatorMap, nil
	}

	// Convert pool key hashes to [][]byte for the metadata store query
	poolKeyHashBytes := make([][]byte, len(pools))
	for i, poolHash := range pools {
		hashCopy := make([]byte, 28)
		copy(hashCopy, poolHash[:])
		poolKeyHashBytes[i] = hashCopy
	}

	// Batch query delegator counts for all pools
	stakes, delegators, err := meta.GetStakeByPools(poolKeyHashBytes, metaTxn)
	if err != nil {
		return nil, nil, fmt.Errorf("get stake by pools: %w", err)
	}

	// Convert back to lcommon.PoolKeyHash keys
	for _, poolHash := range pools {
		stakeMap[poolHash] = stakes[string(poolHash[:])]
		delegatorMap[poolHash] = delegators[string(poolHash[:])]
	}

	return stakeMap, delegatorMap, nil
}
