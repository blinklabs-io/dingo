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
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var errRewardInputEpochUnavailable = errors.New(
	"reward input epoch metadata unavailable",
)

// saveSnapshot saves a stake distribution as a snapshot of the given type.
//
// resolveAutoVote controls whether the CIP-1694 reward-account
// auto-vote is computed against the live Pool/Account tables and
// frozen onto the snapshot rows. It MUST be true only when the
// caller knows that live state matches the target epoch boundary
// (e.g. the normal epoch-transition call at the boundary moment, or
// a genesis bootstrap). For historical seed writes — most notably
// the post-Mithril N-1 / N-2 seeding loop — pass false: those rows
// represent older boundaries than the live state and resolving them
// would silently freeze today's delegation map onto a historical
// snapshot. They are stored with RewardAccountAutoVoteResolved=false
// so the tally treats them as PoolRewardAccountAutoVoteNone.
func (m *Manager) saveSnapshot(
	ctx context.Context,
	epoch uint64,
	snapshotType string,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	resolveAutoVote bool,
) error {
	_ = ctx
	txn := m.db.Transaction(true) // read-write transaction
	defer func() { _ = txn.Rollback() }()

	meta := m.db.Metadata()
	metaTxn := txn.Metadata()

	// Save pool stake snapshots
	snapshots := make(
		[]*models.PoolStakeSnapshot,
		0,
		len(distribution.PoolStakes),
	)
	for poolKeyHash, stake := range distribution.PoolStakes {
		delegators := distribution.DelegatorCount[poolKeyHash]
		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:          epoch,
			SnapshotType:   snapshotType,
			PoolKeyHash:    poolKeyHash[:], // Convert [28]byte to []byte
			TotalStake:     types.Uint64(stake),
			DelegatorCount: delegators,
			CapturedSlot:   distribution.Slot,
		})
	}

	// Freeze the CIP-1694 SPO reward-account auto-vote per pool at
	// the snapshot boundary so governance ratification at epoch N
	// reads snapshot-era delegation rather than the live, possibly
	// re-delegated state. Skipped (rows left Resolved=false) when
	// the caller is seeding historical epochs where live state does
	// not match the target boundary.
	if resolveAutoVote {
		if err := m.db.ResolvePoolRewardAccountAutoVotes(
			snapshots, txn,
		); err != nil {
			return fmt.Errorf("resolve reward-account auto-votes: %w", err)
		}
	}

	if err := meta.DeletePoolStakeSnapshotsForEpoch(
		epoch, snapshotType, metaTxn,
	); err != nil {
		return fmt.Errorf("replace pool snapshots: delete prior set: %w", err)
	}
	if err := meta.SavePoolStakeSnapshots(snapshots, metaTxn); err != nil {
		return fmt.Errorf("save pool snapshots: %w", err)
	}

	// Save epoch summary
	summary := &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(distribution.TotalStake),
		TotalPoolCount:   distribution.TotalPools,
		TotalDelegators:  sumDelegators(distribution.DelegatorCount),
		EpochNonce:       evt.EpochNonce,
		BoundarySlot:     evt.BoundarySlot,
		SnapshotReady:    true,
	}

	if err := meta.SaveEpochSummary(summary, metaTxn); err != nil {
		return fmt.Errorf("save epoch summary: %w", err)
	}

	if err := m.saveRewardStateInputs(
		epoch,
		snapshotType,
		distribution,
		evt,
		meta,
		metaTxn,
	); err != nil {
		return fmt.Errorf("save reward state inputs: %w", err)
	}

	return txn.Commit()
}

func (m *Manager) saveRewardStateInputs(
	epoch uint64,
	snapshotType string,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) error {
	inputs, err := m.rewardPoolInputs(
		epoch,
		distribution,
		evt,
		meta,
		metaTxn,
	)
	if err != nil {
		if errors.Is(err, errRewardInputEpochUnavailable) {
			m.logger.Warn(
				"skipping reward inputs without ended-epoch metadata",
				"component", "snapshot",
				"epoch", epoch,
			)
			return nil
		}
		return err
	}
	snapshot := &models.RewardSnapshot{
		Epoch:            epoch,
		SnapshotType:     snapshotType,
		TotalActiveStake: types.Uint64(distribution.TotalStake),
		TotalPoolCount:   distribution.TotalPools,
		TotalDelegators:  sumDelegators(distribution.DelegatorCount),
		CapturedSlot:     distribution.Slot,
		BoundarySlot:     evt.BoundarySlot,
		EpochNonce:       evt.EpochNonce,
		ProtocolVersion:  evt.ProtocolVersion,
	}
	if err := meta.SaveRewardSnapshot(snapshot, metaTxn); err != nil {
		return fmt.Errorf("save reward snapshot: %w", err)
	}
	if err := meta.SaveRewardPoolInputs(inputs, metaTxn); err != nil {
		return fmt.Errorf("save reward pool inputs: %w", err)
	}
	return nil
}

func (m *Manager) rewardPoolInputs(
	epoch uint64,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) ([]*models.RewardPoolInput, error) {
	if len(distribution.PoolStakes) == 0 {
		return nil, nil
	}

	poolKeys := make([]lcommon.PoolKeyHash, 0, len(distribution.PoolStakes))
	for poolKey := range distribution.PoolStakes {
		poolKeys = append(poolKeys, poolKey)
	}
	blockCounts, totalBlocks, err := rewardPoolBlockCounts(
		meta,
		metaTxn,
		poolKeys,
		evt,
	)
	if err != nil {
		return nil, err
	}
	endedEpoch := uint64(0)
	if epoch > 0 {
		endedEpoch = epoch - 1
	}
	epochInfo, err := meta.GetEpoch(endedEpoch, metaTxn)
	if err != nil {
		return nil, fmt.Errorf(
			"get ended epoch %d for reward pool parameters: %w",
			endedEpoch,
			err,
		)
	}
	if epochInfo == nil {
		return nil, fmt.Errorf(
			"%w: ended epoch %d not found for reward pool parameters",
			errRewardInputEpochUnavailable,
			endedEpoch,
		)
	}
	registrations, err := meta.GetPoolRegistrationsEffectiveForEpoch(
		poolKeys,
		epochInfo.StartSlot,
		endedEpoch,
		distribution.Slot,
		metaTxn,
	)
	if err != nil {
		return nil, fmt.Errorf("get reward input pool registrations: %w", err)
	}
	registrationByHash := make(
		map[string]models.PoolRegistration,
		len(registrations),
	)
	ownerKeys := make([][]byte, 0)
	for _, registration := range registrations {
		registrationByHash[string(registration.PoolKeyHash)] = registration
		for _, owner := range registration.Owners {
			ownerKeys = append(ownerKeys, owner.KeyHash)
		}
	}
	ownerStakes, err := meta.GetPoolOwnerStakeAtSlot(
		ownerKeys,
		distribution.Slot,
		metaTxn,
	)
	if err != nil {
		return nil, fmt.Errorf("get historical pool-owner stake: %w", err)
	}

	inputs := make([]*models.RewardPoolInput, 0, len(distribution.PoolStakes))
	for poolKey, stake := range distribution.PoolStakes {
		poolKeyHash := make([]byte, len(poolKey))
		copy(poolKeyHash, poolKey[:])
		input := &models.RewardPoolInput{
			Epoch:          epoch,
			PoolKeyHash:    poolKeyHash,
			DelegatedStake: types.Uint64(stake),
			DelegatorCount: distribution.DelegatorCount[poolKey],
			CapturedSlot:   distribution.Slot,
			BoundarySlot:   evt.BoundarySlot,
		}
		if totalBlocks != nil {
			input.TotalBlocksInEpoch = new(*totalBlocks)
		}
		if blockCounts != nil {
			input.BlocksProduced = new(blockCounts[string(poolKey[:])])
		}
		if registration, ok := registrationByHash[string(poolKey[:])]; ok {
			input.Pledge = registration.Pledge
			input.Cost = registration.Cost
			input.Margin = cloneRat(registration.Margin)
			input.RewardAccount = append(
				[]byte(nil), registration.RewardAccount...,
			)
			input.RewardAccountCredentialTag = registration.RewardAccountCredentialTag
			for _, owner := range registration.Owners {
				input.OwnerStake += types.Uint64(
					ownerStakes[types.PoolCredentialStakeKey(
						poolKey[:], 0, owner.KeyHash,
					)],
				)
			}
		} else {
			m.logger.Warn(
				"missing pool registration while saving reward inputs",
				"component", "snapshot",
				"epoch", epoch,
				"snapshot_slot", distribution.Slot,
				"pool_key_hash", hex.EncodeToString(poolKey[:]),
			)
		}
		inputs = append(inputs, input)
	}
	return inputs, nil
}

func rewardPoolBlockCounts(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	poolKeys []lcommon.PoolKeyHash,
	evt event.EpochTransitionEvent,
) (map[string]uint64, *uint64, error) {
	if evt.BoundarySlot == 0 {
		return nil, nil, nil
	}
	epoch, err := meta.GetEpoch(evt.PreviousEpoch, metaTxn)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"get previous epoch for reward performance: %w",
			err,
		)
	}
	if epoch == nil || epoch.LengthInSlots == 0 {
		return nil, nil, nil
	}

	startSlot := epoch.StartSlot
	endSlot := startSlot + uint64(epoch.LengthInSlots) - 1
	boundaryEndSlot := evt.BoundarySlot - 1
	if boundaryEndSlot < endSlot {
		endSlot = boundaryEndSlot
	}
	if endSlot < startSlot {
		return nil, nil, nil
	}

	counts, total, err := meta.CountPoolBlocksInSlotRange(
		poolKeys,
		startSlot,
		endSlot,
		metaTxn,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"count reward pool blocks in epoch %d: %w",
			evt.PreviousEpoch,
			err,
		)
	}
	return counts, &total, nil
}

func cloneRat(r *types.Rat) *types.Rat {
	if r == nil || r.Rat == nil {
		return nil
	}
	return &types.Rat{Rat: new(big.Rat).Set(r.Rat)}
}

// rotateSnapshots promotes Mark→Set→Go for the new epoch.
func (m *Manager) rotateSnapshots(ctx context.Context, newEpoch uint64) {
	_ = ctx
	// In practice, we store snapshots with their epoch and type.
	// The active stake distribution for epoch N is the Mark snapshot
	// captured at the epoch boundary. Historical Set/Go-equivalent data is
	// addressed by older epoch numbers rather than by copying rows between
	// snapshot types.

	goEpoch := uint64(0)
	setEpoch := uint64(0)
	if newEpoch > 1 {
		goEpoch = newEpoch - 2
	}
	if newEpoch > 0 {
		setEpoch = newEpoch - 1
	}

	m.logger.Debug(
		"snapshot rotation conceptual",
		"component", "snapshot",
		"epoch", newEpoch,
		"go_epoch", goEpoch,
		"set_epoch", setEpoch,
	)
}

// cleanupOldSnapshots removes snapshots older than needed for the rotation model.
// We keep 3 epochs of snapshots (current, current-1, current-2 for Go).
func (m *Manager) cleanupOldSnapshots(
	ctx context.Context,
	currentEpoch uint64,
) error {
	_ = ctx
	// Keep snapshots for epochs: currentEpoch, currentEpoch-1, currentEpoch-2
	// Delete anything older than currentEpoch-2 (i.e., epoch < currentEpoch-2)
	if currentEpoch < 2 {
		return nil // Not enough history to clean
	}

	deleteBeforeEpoch := currentEpoch - 2

	txn := m.db.Transaction(true) // read-write transaction
	defer func() { _ = txn.Rollback() }()

	meta := m.db.Metadata()
	metaTxn := txn.Metadata()

	// Delete old pool stake snapshots
	// For cleaning up old data, we want to delete epochs < deleteBeforeEpoch
	if err := meta.DeletePoolStakeSnapshotsBeforeEpoch(
		deleteBeforeEpoch,
		metaTxn,
	); err != nil {
		return fmt.Errorf("cleanup pool snapshots: %w", err)
	}

	// Delete old epoch summaries
	if err := meta.DeleteEpochSummariesBeforeEpoch(
		deleteBeforeEpoch,
		metaTxn,
	); err != nil {
		return fmt.Errorf("cleanup epoch summaries: %w", err)
	}

	if err := meta.DeleteRewardStateBeforeEpoch(
		deleteBeforeEpoch,
		metaTxn,
	); err != nil {
		return fmt.Errorf("cleanup reward state: %w", err)
	}

	m.logger.Debug(
		"cleaned up old snapshots",
		"component", "snapshot",
		"before_epoch", deleteBeforeEpoch,
	)

	return txn.Commit()
}

// sumDelegators totals all delegator counts.
func sumDelegators(counts map[lcommon.PoolKeyHash]uint64) uint64 {
	var total uint64
	for _, count := range counts {
		total += count
	}
	return total
}
