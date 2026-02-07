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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// saveSnapshot saves a stake distribution as a snapshot of the given type.
func (m *Manager) saveSnapshot(
	ctx context.Context,
	epoch uint64,
	snapshotType string,
	distribution *StakeDistribution,
	evt event.EpochTransitionEvent,
) error {
	_ = ctx
	txn := m.db.Transaction(true) // read-write transaction
	defer func() { _ = txn.Rollback() }()

	meta := m.db.Metadata()
	metaTxn := txn.Metadata()

	// Save pool stake snapshots
	snapshots := make([]*models.PoolStakeSnapshot, 0, len(distribution.PoolStakes))
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

	return txn.Commit()
}

// rotateSnapshots promotes Mark→Set→Go for the new epoch.
func (m *Manager) rotateSnapshots(ctx context.Context, newEpoch uint64) {
	_ = ctx
	// Snapshot rotation in Ouroboros Praos:
	// - The Mark snapshot from epoch N-1 becomes Set for epoch N
	// - The Set snapshot from epoch N-1 becomes Go for epoch N
	// - We only need to track the type metadata, not copy data

	// In practice, we store snapshots with their epoch and type.
	// The "Go" snapshot used for leader election in epoch N is the
	// "Mark" snapshot captured at the end of epoch N-2.

	// For simplicity, we don't physically rotate - we query by epoch offset:
	// - Go snapshot for epoch N = data from epoch N-2
	// - Set snapshot for epoch N = data from epoch N-1
	// - Mark snapshot for epoch N = data captured at epoch N boundary

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
