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
	"errors"
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
	StakeInputs    []StakeInput
	Slot           uint64                         // Slot at which distribution was captured
	PoolStakes     map[lcommon.PoolKeyHash]uint64 // pool key hash -> total stake
	DelegatorCount map[lcommon.PoolKeyHash]uint64 // pool key hash -> delegator count
	TotalStake     uint64                         // Sum of all pool stakes
	TotalPools     uint64                         // Number of active pools
}

// StakeInput is a per-stake-credential snapshot input owned by the snapshot
// package. Persistence code converts it to database reward-state rows.
type StakeInput struct {
	PoolKeyHash   []byte
	CredentialTag uint8
	StakingKey    []byte
	Stake         uint64
	Registered    bool
}

// CalculateStakeDistribution calculates the stake distribution at a given slot.
// Pool selection and stake totals are both slot-aware. Reward input rows are
// only available from the live epoch-boundary path, so this public historical
// query returns pool totals and delegator counts without per-credential inputs.
func (c *Calculator) CalculateStakeDistribution(
	ctx context.Context,
	slot uint64,
) (*StakeDistribution, error) {
	// Read-only transaction so the entire calculation observes a
	// consistent database snapshot.
	txn := c.db.Transaction(false)
	defer func() { _ = txn.Commit() }()

	// Public historical query path: the CIP-0163 inactivity gate is a
	// consensus concern applied only by the snapshot manager, which supplies a
	// nonzero expiryEpoch. This query keeps expiryEpoch == 0 (gate off).
	return c.calculateHistoricalStakeDistributionInTxn(ctx, txn, slot, 0, 0)
}

// calculateStakeDistributionInTxn computes the leader-election and reward-basis
// stake for an epoch-boundary snapshot. expiryEpoch drives the CIP-0163
// reward-account inactivity gate: 0 disables it (byte-identical to pre-CIP), a
// nonzero value (the epoch the snapshot is computed for) excludes credentials
// whose account expired before that epoch. inactivityPeriod reconstructs each
// credential's expiration from witness history at the requested slot.
func (c *Calculator) calculateStakeDistributionInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (*StakeDistribution, error) {
	dist, err := c.calculateHistoricalStakeDistributionInTxn(
		ctx, txn, slot, expiryEpoch, inactivityPeriod,
	)
	if err != nil {
		return nil, err
	}

	stakeInputs, err := c.rewardStakeInputsInTxn(
		ctx, txn, slot, expiryEpoch, inactivityPeriod,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate reward stake inputs: %w", err)
	}
	dist.StakeInputs = stakeInputs
	if _, err := rewardStakeDistribution(dist); err != nil {
		return nil, fmt.Errorf("validate reward stake inputs: %w", err)
	}

	return dist, nil
}

func (c *Calculator) calculateHistoricalStakeDistributionInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (*StakeDistribution, error) {
	dist := &StakeDistribution{
		Slot:           slot,
		PoolStakes:     make(map[lcommon.PoolKeyHash]uint64),
		DelegatorCount: make(map[lcommon.PoolKeyHash]uint64),
	}

	err := c.calculateFromHistoricalStake(
		ctx, txn, slot, expiryEpoch, inactivityPeriod, dist,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate from historical stake: %w", err)
	}

	// Count total pools
	dist.TotalPools = uint64(len(dist.PoolStakes))

	return dist, nil
}

// rewardStakeInputsInTxn returns per-credential reward inputs. With the
// CIP-0163 gate off (expiryEpoch == 0) these come from the live reward
// aggregate; with the gate on they are reconstructed at slot from the same
// historical CTE as the leader-election pool totals in
// calculateHistoricalStakeDistributionInTxn, so both halves agree by
// construction.
func (c *Calculator) rewardStakeInputsInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) ([]StakeInput, error) {
	meta := c.db.Metadata()
	metaTxn := (*txn).Metadata()

	// Get all active pools at the given slot.
	// Returns types.ErrNoEpochData (wrapped) if epoch data is not yet synced.
	pools, err := c.getActivePoolsAtSlot(ctx, meta, metaTxn, slot)
	if err != nil {
		return nil, fmt.Errorf("get active pools: %w", err)
	}

	// If no pools found, return empty distribution (not an error)
	if len(pools) == 0 {
		return nil, nil
	}

	// Batch fetch reward credential inputs for all pools in a single query.
	stakeMap, err := c.getBatchPoolsDelegatedStake(
		ctx,
		meta,
		metaTxn,
		pools,
		slot,
		expiryEpoch,
		inactivityPeriod,
	)
	if err != nil {
		return nil, fmt.Errorf("get batch reward stake inputs: %w", err)
	}
	return stakeMap.inputs, nil
}

// calculateFromHistoricalStake computes slot-accurate pool totals without
// reading the live reward aggregate.
func (c *Calculator) calculateFromHistoricalStake(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	dist *StakeDistribution,
) error {
	meta := c.db.Metadata()
	metaTxn := (*txn).Metadata()

	pools, err := c.getActivePoolsAtSlot(ctx, meta, metaTxn, slot)
	if err != nil {
		return fmt.Errorf("get active pools: %w", err)
	}
	if len(pools) == 0 {
		return nil
	}

	stakeMap, delegatorMap, err := c.getBatchPoolsHistoricalStake(
		ctx,
		meta,
		metaTxn,
		pools,
		slot,
		expiryEpoch,
		inactivityPeriod,
	)
	if err != nil {
		return fmt.Errorf("get batch pools historical stake: %w", err)
	}

	for _, poolHash := range pools {
		delegators := delegatorMap[poolHash]
		if delegators > 0 {
			stake := stakeMap[poolHash]
			if dist.TotalStake > ^uint64(0)-stake {
				return errors.New("total active stake overflow")
			}
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

// getBatchPoolsDelegatedStake returns per-credential reward stake for all
// pools. With the CIP-0163 gate off it reads the live reward aggregate, which
// metadata block application keeps aligned with UTxO, account, delegation, and
// reward-balance changes so the epoch-boundary snapshot avoids scanning the
// UTxO set. With the gate on it reconstructs the inputs at slot from the same
// historical CTE as the leader-election pool totals, trading that optimization
// for reward inputs that agree with leader stake by construction.
func (c *Calculator) getBatchPoolsDelegatedStake(
	_ context.Context,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	pools []lcommon.PoolKeyHash,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (*rewardStakeAggregation, error) {
	// Initialize result maps
	stakeMap := &rewardStakeAggregation{
		values: make(map[lcommon.PoolKeyHash]uint64, len(pools)),
	}

	if len(pools) == 0 {
		return stakeMap, nil
	}

	// Convert pool key hashes to [][]byte for the metadata store query
	poolKeyHashBytes := make([][]byte, len(pools))
	for i, poolHash := range pools {
		hashCopy := make([]byte, 28)
		copy(hashCopy, poolHash[:])
		poolKeyHashBytes[i] = hashCopy
	}

	inputs, err := meta.GetRewardStakeInputsForPools(
		poolKeyHashBytes,
		slot,
		expiryEpoch,
		inactivityPeriod,
		metaTxn,
	)
	if err != nil {
		return nil, fmt.Errorf("get reward stake inputs: %w", err)
	}
	for _, input := range inputs {
		if input == nil {
			return nil, errors.New("nil reward stake input")
		}
		if len(input.PoolKeyHash) != len(lcommon.PoolKeyHash{}) {
			return nil, fmt.Errorf(
				"invalid reward stake input pool key length %d",
				len(input.PoolKeyHash),
			)
		}
		if len(input.StakingKey) != len(lcommon.PoolKeyHash{}) {
			return nil, fmt.Errorf(
				"invalid reward stake input credential length %d",
				len(input.StakingKey),
			)
		}
		if input.CredentialTag > 1 {
			return nil, fmt.Errorf(
				"invalid reward stake input credential tag %d",
				input.CredentialTag,
			)
		}
		var poolHash lcommon.PoolKeyHash
		copy(poolHash[:], input.PoolKeyHash)
		stake := uint64(input.Stake)
		if stake == 0 {
			continue
		}
		if stakeMap.values[poolHash] > ^uint64(0)-stake {
			return nil, fmt.Errorf(
				"delegated stake overflow for pool %x",
				poolHash[:],
			)
		}
		stakeMap.inputs = append(stakeMap.inputs, StakeInput{
			PoolKeyHash:   append([]byte(nil), input.PoolKeyHash...),
			CredentialTag: input.CredentialTag,
			StakingKey:    append([]byte(nil), input.StakingKey...),
			Stake:         stake,
			Registered:    input.Registered,
		})
		stakeMap.values[poolHash] += stake
	}

	return stakeMap, nil
}

func (c *Calculator) getBatchPoolsHistoricalStake(
	_ context.Context,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	pools []lcommon.PoolKeyHash,
	slot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (map[lcommon.PoolKeyHash]uint64, map[lcommon.PoolKeyHash]uint64, error) {
	stakeMap := make(map[lcommon.PoolKeyHash]uint64, len(pools))
	delegatorMap := make(map[lcommon.PoolKeyHash]uint64, len(pools))
	if len(pools) == 0 {
		return stakeMap, delegatorMap, nil
	}

	poolKeyHashBytes := make([][]byte, len(pools))
	for i, poolHash := range pools {
		hashCopy := make([]byte, len(poolHash))
		copy(hashCopy, poolHash[:])
		poolKeyHashBytes[i] = hashCopy
	}

	stakes, delegators, err := meta.GetStakeByPoolsAtSlot(
		poolKeyHashBytes,
		slot,
		expiryEpoch,
		inactivityPeriod,
		metaTxn,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("get stake by pools at slot: %w", err)
	}

	for _, poolHash := range pools {
		stakeMap[poolHash] = stakes[string(poolHash[:])]
		delegatorMap[poolHash] = delegators[string(poolHash[:])]
	}

	return stakeMap, delegatorMap, nil
}

type rewardStakeAggregation struct {
	inputs []StakeInput
	values map[lcommon.PoolKeyHash]uint64
}
