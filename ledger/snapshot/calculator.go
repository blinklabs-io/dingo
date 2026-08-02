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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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
	// nonzero expiryEpoch. This query keeps expiryEpoch == 0 (gate off), and
	// boundarySlot == 0 so it stays a plain "stake at slot" reconstruction with
	// no epoch-boundary reward semantics.
	return c.calculateHistoricalStakeDistributionInTxn(ctx, txn, slot, 0, 0, 0)
}

// boundaryRewardSlot validates the epoch-boundary reward cut before it reaches
// SQL. SNAP reward semantics are defined only for the mark snapshot's own
// boundary, whose snapshot slot is exactly one before the boundary slot (see
// captureEpochBoundarySnapshot in ledger/chainsync.go). Anything else — genesis
// and post-Mithril seeding, which reconstruct at an epoch start slot rather than
// one before it — gets 0, i.e. the plain "stake at slot" reconstruction.
func boundaryRewardSlot(slot, boundarySlot uint64) uint64 {
	if boundarySlot > 0 && boundarySlot == slot+1 {
		return boundarySlot
	}
	return 0
}

// calculateStakeDistributionInTxn computes an epoch-boundary snapshot from the
// transactionally maintained live reward-stake aggregate. The authoritative
// rollover hook calls this at the SNAP point, before any new-epoch block is
// applied, so the live rows are already the exact slot state and avoid a
// genesis-to-slot certificate and UTxO reconstruction.
func (c *Calculator) calculateStakeDistributionInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	expiryEpoch uint64,
) (*StakeDistribution, error) {
	dist, err := c.calculateLiveStakeDistributionInTxn(
		ctx, txn, slot, expiryEpoch,
	)
	if err != nil {
		return nil, err
	}
	if _, err := rewardStakeDistribution(dist); err != nil {
		return nil, fmt.Errorf("validate reward stake inputs: %w", err)
	}

	return dist, nil
}

// calculateBoundaryStakeDistributionInTxn uses the fast live aggregate while
// the transaction tip is at or before slot. A delayed fallback whose tip has
// already passed the boundary retains the historical reconstruction needed for
// slot accuracy.
//
// boundarySlot is the mark snapshot's boundary (slot+1); pass 0 for a capture
// that is not reconstructing an epoch boundary. See boundaryRewardSlot.
//
// Both halves — the leader-election pool totals and the per-credential reward
// basis — come from the same historical reconstruction, for the same
// (slot, boundarySlot), and are cross-checked against each other.
func (c *Calculator) calculateBoundaryStakeDistributionInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (*StakeDistribution, error) {
	tip, err := c.db.GetTip(txn)
	if err != nil {
		return nil, fmt.Errorf("get snapshot transaction tip: %w", err)
	}
	// An all-zero tip is "no persisted tip", not proof that the live aggregate
	// represents slot. Fall back to history in that ambiguous bootstrap/test
	// state.
	hasTip := tip.BlockNumber > 0 || len(tip.Point.Hash) > 0
	if hasTip && tip.Point.Slot <= slot {
		return c.calculateStakeDistributionInTxn(
			ctx, txn, slot, expiryEpoch,
		)
	}

	rewardSlot := boundaryRewardSlot(slot, boundarySlot)
	dist, err := c.calculateHistoricalStakeDistributionInTxn(
		ctx, txn, slot, rewardSlot, expiryEpoch, inactivityPeriod,
	)
	if err != nil {
		return nil, err
	}
	stakeInputs, err := c.rewardStakeInputsInTxn(
		ctx, txn, slot, rewardSlot, expiryEpoch, inactivityPeriod,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate reward stake inputs: %w", err)
	}
	dist.StakeInputs = stakeInputs
	// Cross-check the two halves against each other. Both were built from one
	// reconstruction, so a mismatch means the pool totals and the reward basis
	// genuinely disagree — the class of corruption that later surfaces as a
	// "reward stake input total mismatch" crash during reward application.
	if err := validateRewardStakeInputTotals(dist); err != nil {
		return nil, fmt.Errorf(
			"epoch-boundary fallback stake inputs disagree with pool totals: %w",
			err,
		)
	}
	if _, err := rewardStakeDistribution(dist); err != nil {
		return nil, fmt.Errorf("validate reward stake inputs: %w", err)
	}
	return dist, nil
}

func (c *Calculator) calculateLiveStakeDistributionInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	expiryEpoch uint64,
) (*StakeDistribution, error) {
	dist := &StakeDistribution{
		Slot:           slot,
		PoolStakes:     make(map[lcommon.PoolKeyHash]uint64),
		DelegatorCount: make(map[lcommon.PoolKeyHash]uint64),
	}
	meta := c.db.Metadata()
	metaTxn := (*txn).Metadata()
	pools, err := c.getActivePoolsAtSlot(ctx, meta, metaTxn, slot)
	if err != nil {
		return nil, fmt.Errorf("get active pools: %w", err)
	}
	if len(pools) == 0 {
		return dist, nil
	}

	poolKeyHashBytes := make([][]byte, len(pools))
	for i, poolHash := range pools {
		poolKeyHashBytes[i] = append([]byte(nil), poolHash[:]...)
	}
	rawInputs, err := meta.GetLiveStakeInputsForPools(
		poolKeyHashBytes, expiryEpoch, metaTxn,
	)
	if err != nil {
		return nil, fmt.Errorf("get live stake inputs: %w", err)
	}
	inputs, err := rewardStakeInputsFromRows(rawInputs)
	if err != nil {
		return nil, err
	}
	dist.StakeInputs = make([]StakeInput, 0, len(inputs))
	for _, input := range inputs {
		var poolHash lcommon.PoolKeyHash
		copy(poolHash[:], input.PoolKeyHash)
		dist.DelegatorCount[poolHash]++
		stake := uint64(input.Stake)
		if dist.PoolStakes[poolHash] > ^uint64(0)-stake {
			return nil, fmt.Errorf(
				"delegated stake overflow for pool %x", poolHash[:],
			)
		}
		dist.PoolStakes[poolHash] += stake
		if dist.TotalStake > ^uint64(0)-stake {
			return nil, errors.New("total active stake overflow")
		}
		dist.TotalStake += stake
		if stake > 0 {
			dist.StakeInputs = append(dist.StakeInputs, StakeInput{
				PoolKeyHash:   input.PoolKeyHash,
				CredentialTag: input.CredentialTag,
				StakingKey:    input.StakingKey,
				Stake:         stake,
				Registered:    input.Registered,
			})
		}
	}
	dist.TotalPools = uint64(len(dist.PoolStakes))
	return dist, nil
}

func (c *Calculator) calculateHistoricalStakeDistributionInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
	inactivityPeriod uint64,
) (*StakeDistribution, error) {
	dist := &StakeDistribution{
		Slot:           slot,
		PoolStakes:     make(map[lcommon.PoolKeyHash]uint64),
		DelegatorCount: make(map[lcommon.PoolKeyHash]uint64),
	}

	err := c.calculateFromHistoricalStake(
		ctx, txn, slot, boundarySlot, expiryEpoch, inactivityPeriod, dist,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate from historical stake: %w", err)
	}

	// Count total pools
	dist.TotalPools = uint64(len(dist.PoolStakes))

	return dist, nil
}

// rewardStakeInputsInTxn returns per-credential reward inputs reconstructed at
// slot from the same historical CTE as the leader-election pool totals in
// calculateHistoricalStakeDistributionInTxn — with or without the CIP-0163 gate
// — so both halves agree by construction instead of mixing a historical total
// with the live reward aggregate.
func (c *Calculator) rewardStakeInputsInTxn(
	ctx context.Context,
	txn *database.Txn,
	slot uint64,
	boundarySlot uint64,
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
		boundarySlot,
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
	boundarySlot uint64,
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
		boundarySlot,
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

// getBatchPoolsDelegatedStake returns historical per-credential reward stake
// for all pools. The fallback path reconstructs both these inputs and the
// leader-election totals at slot so they agree even when live account state has
// advanced beyond the boundary.
func (c *Calculator) getBatchPoolsDelegatedStake(
	_ context.Context,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	pools []lcommon.PoolKeyHash,
	slot uint64,
	boundarySlot uint64,
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

	// The reward basis is reconstructed from the same CTE as the leader-election
	// pool totals, for the same (slot, boundarySlot), with or without the
	// CIP-0163 gate, so the two halves of the fallback snapshot agree by
	// construction.
	//
	// The gate-off path used to read the live reward aggregate instead, which has
	// no slot predicate at all, so a fallback capture paired slot-accurate pool
	// totals with post-boundary live per-credential stake and nothing compared
	// the two.
	rawInputs, err := meta.GetEpochBoundaryRewardStakeInputsForPools(
		poolKeyHashBytes,
		slot,
		boundarySlot,
		expiryEpoch,
		inactivityPeriod,
		metaTxn,
	)
	if err != nil {
		return nil, fmt.Errorf("get reward stake inputs: %w", err)
	}
	inputs, err := rewardStakeInputsFromRows(rawInputs)
	if err != nil {
		return nil, err
	}
	for _, input := range inputs {
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
	boundarySlot uint64,
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

	stakes, delegators, err := meta.GetEpochBoundaryStakeByPools(
		poolKeyHashBytes,
		slot,
		boundarySlot,
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

// rewardStakeInputsFromRows converts and canonically deduplicates reward rows
// before they are aggregated. The canonical ordering makes the selected row
// independent of SQL backend order and pool-query chunking.
//
// reward_live_stake is unique on (credential_tag, staking_key), so a credential
// cannot legitimately contribute stake under two pools. Duplicate rows do occur
// though — most concretely, a duplicate reward_live_stake credential row seeded
// before idx_reward_live_stake_cred was unique made the per-credential stake
// inputs disagree with the per-pool aggregate and crashed reward application at
// an epoch rollover.
//
// rewardStakeDistribution already collapsed duplicates, but only into a
// throwaway copy used for validation, so nothing downstream was protected: the
// duplicate still reached PoolStakes, DelegatorCount, TotalStake, and from there
// PoolStakeSnapshot and EpochSummary. Deduping here, ahead of aggregation, makes
// the protection real and keeps the two dedupe rules identical (same key, same
// last-wins tie-break) so rewardStakeDistribution stays a no-op validator.
func rewardStakeInputsFromRows(
	rows []*models.RewardStakeInput,
) ([]StakeInput, error) {
	inputs := make([]StakeInput, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			return nil, errors.New("nil reward stake input")
		}
		if len(row.PoolKeyHash) != len(lcommon.PoolKeyHash{}) {
			return nil, fmt.Errorf("invalid reward stake input pool key length %d", len(row.PoolKeyHash))
		}
		if len(row.StakingKey) != len(lcommon.PoolKeyHash{}) {
			return nil, fmt.Errorf("invalid reward stake input credential length %d", len(row.StakingKey))
		}
		if row.CredentialTag > 1 {
			return nil, fmt.Errorf("invalid reward stake input credential tag %d", row.CredentialTag)
		}
		inputs = append(inputs, StakeInput{
			PoolKeyHash:   append([]byte(nil), row.PoolKeyHash...),
			CredentialTag: row.CredentialTag,
			StakingKey:    append([]byte(nil), row.StakingKey...),
			Stake:         uint64(row.Stake), Registered: row.Registered,
		})
	}
	return dedupeStakeInputs(inputs), nil
}

// dedupeStakeInputs selects the lexicographically greatest complete row for a
// duplicate credential after sorting by credential, pool, stake, and
// registration state. This explicit tie-break prevents backend/chunk order
// from changing consensus-critical leader stake snapshots.
func dedupeStakeInputs(inputs []StakeInput) []StakeInput {
	canonical := append([]StakeInput(nil), inputs...)
	sort.Slice(canonical, func(i, j int) bool {
		a, b := canonical[i], canonical[j]
		if a.CredentialTag != b.CredentialTag {
			return a.CredentialTag < b.CredentialTag
		}
		if c := bytes.Compare(a.StakingKey, b.StakingKey); c != 0 {
			return c < 0
		}
		if c := bytes.Compare(a.PoolKeyHash, b.PoolKeyHash); c != 0 {
			return c < 0
		}
		if a.Stake != b.Stake {
			return a.Stake < b.Stake
		}
		return !a.Registered && b.Registered
	})
	seen := make(map[string]int, len(canonical))
	result := make([]StakeInput, 0, len(canonical))
	for _, input := range canonical {
		key := string([]byte{input.CredentialTag}) + string(input.StakingKey)
		if idx, ok := seen[key]; ok {
			result[idx] = input
			continue
		}
		seen[key] = len(result)
		result = append(result, input)
	}
	return result
}
