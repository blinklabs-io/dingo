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

package indexer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
)

// DeferredGovernanceSnapshotsKey is the IndexerDB sync_state key listing the
// governance boundary snapshots whose rows the ledger had not written when
// their slot was copied, and that have not been copied since.
const DeferredGovernanceSnapshotsKey = "dingo:indexer:governance-snapshots-deferred"

// governanceSnapshotTable is the fact table the ledger fills after a deferred
// boundary commits.
const governanceSnapshotTableName = "governance_boundary_snapshot"

// pendingRatification mirrors the ledger's record of a boundary whose RATIFY
// decision, and so whose governance snapshot row, is not written yet.
type pendingRatification struct {
	Epoch        uint64 `json:"epoch"`
	BoundarySlot uint64 `json:"boundary_slot"`
}

// DeferredGovernanceSnapshotsFromPending converts the ledger's pending
// ratification record into the deferred list a copy that missed the snapshot
// row leaves for the writer. It returns the empty string for no record.
func DeferredGovernanceSnapshotsFromPending(raw string) (string, error) {
	entries, err := decodePendingRatification(raw)
	if err != nil || len(entries) == 0 {
		return "", err
	}
	out, err := json.Marshal(pendingStakeInputs{Entries: entries})
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func decodePendingRatification(raw string) ([]pendingStakeInput, error) {
	if raw == "" {
		return nil, nil
	}
	var rec pendingRatification
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, fmt.Errorf("indexer: decode pending ratification: %w", err)
	}
	return []pendingStakeInput{
		{Epoch: rec.Epoch, BoundarySlot: rec.BoundarySlot},
	}, nil
}

// ledgerPendingGovernanceSnapshots returns the boundary the ledger has not
// finished writing a governance snapshot for.
func (f *MetadataFacts) ledgerPendingGovernanceSnapshots() ([]pendingStakeInput, error) {
	raw, err := f.src.Metadata().GetSyncState(
		models.RatificationPendingKey, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("indexer: read pending ratification: %w", err)
	}
	return decodePendingRatification(raw)
}

func (f *MetadataFacts) pendingGovernanceSnapshotEpochs() (map[uint64]struct{}, error) {
	entries, err := f.ledgerPendingGovernanceSnapshots()
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]struct{}, len(entries))
	for _, e := range entries {
		out[e.Epoch] = struct{}{}
	}
	return out, nil
}

// copyDeferredBoundaryRows copies the rows of each deferred stake-input and
// governance snapshot the ledger has since finished writing.
func (f *MetadataFacts) copyDeferredBoundaryRows(
	ctx context.Context,
	dst *database.Txn,
) error {
	if err := f.copyDeferredStakeInputs(ctx, dst); err != nil {
		return err
	}
	return f.copyDeferredGovernanceSnapshots(ctx, dst)
}

func (f *MetadataFacts) copyDeferredGovernanceSnapshots(
	ctx context.Context,
	dst *database.Txn,
) error {
	entries, err := f.loadDeferred(dst, DeferredGovernanceSnapshotsKey)
	if err != nil || len(entries) == 0 {
		return err
	}
	stillPending, err := f.pendingGovernanceSnapshotEpochs()
	if err != nil {
		return err
	}
	table, err := factTableNamed(governanceSnapshotTableName)
	if err != nil {
		return err
	}
	var kept []pendingStakeInput
	for _, e := range entries {
		if _, pending := stillPending[e.Epoch]; pending {
			kept = append(kept, e)
			continue
		}
		if err := f.copyEpochRows(
			ctx, dst, table,
			e.BoundarySlot, e.BoundarySlot, &e.Epoch,
		); err != nil {
			return err
		}
	}
	if len(kept) == len(entries) {
		return nil
	}
	return f.saveDeferred(dst, DeferredGovernanceSnapshotsKey, kept)
}
