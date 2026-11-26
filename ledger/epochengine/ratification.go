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

package epochengine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
)

// pendingRatificationSyncKey names the record of a boundary whose RATIFY
// decision or mark snapshot has not been written yet. The boundary
// transaction writes it; whichever of the background job or the next boundary
// writes them deletes it in the same transaction.
const pendingRatificationSyncKey = database.PendingRatificationSyncKey

// PendingRatificationRecord identifies a boundary whose deferred RATIFY
// decision or mark snapshot is still to be written.
type PendingRatificationRecord struct {
	Epoch        uint64 `json:"epoch"`
	BoundarySlot uint64 `json:"boundary_slot"`
	ID           uint64 `json:"id"`
	// Snapshot is set when the boundary left mark[Epoch] to the job.
	Snapshot bool `json:"snapshot,omitempty"`
}

// DeferredBoundarySnapshot is a boundary's mark snapshot prepared on a read
// transaction pinned at the boundary's commit, written through another.
type DeferredBoundarySnapshot interface {
	SPOStakeRows() []*models.PoolStakeSnapshot
	Write(txn ledgerdb.Txn) error
}

// LoadPendingRatification returns the pending ratification record in txn, or
// nil when there is none.
func LoadPendingRatification(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) (*PendingRatificationRecord, error) {
	raw, err := ldb.GetSyncState(pendingRatificationSyncKey, txn)
	if err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var rec PendingRatificationRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, fmt.Errorf("decode pending ratification: %w", err)
	}
	return &rec, nil
}

// SavePendingRatification records rec as the boundary whose deferred work is
// pending.
func (e *Engine) SavePendingRatification(
	txn ledgerdb.Txn,
	rec PendingRatificationRecord,
) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode pending ratification: %w", err)
	}
	return e.ledgerDB().SetSyncState(
		pendingRatificationSyncKey, string(raw), txn,
	)
}

// SPOVotingState builds the SPO voting state of a mark snapshot's stake rows.
func SPOVotingState(
	rows []*models.PoolStakeSnapshot,
) *governance.SPOVotingState {
	var total uint64
	for _, r := range rows {
		total += uint64(r.TotalStake)
	}
	return &governance.SPOVotingState{Dist: rows, TotalStake: total}
}

// DecideRatification decides plan's RATIFY on snapshot, a read transaction
// pinned at the boundary's commit. A prepared mark snapshot supplies the SPO
// stake the decision tallies against.
func (e *Engine) DecideRatification(
	ctx context.Context,
	snapshot ledgerdb.Txn,
	plan *governance.RatificationPlan,
	prepared DeferredBoundarySnapshot,
) (*governance.RatificationDecision, error) {
	if plan == nil {
		return nil, nil
	}
	if prepared != nil {
		plan.SetBoundarySPOState(SPOVotingState(prepared.SPOStakeRows()))
	}
	return plan.Decide(ctx, snapshot)
}

// DecidedRatification is a boundary's decided deferred work.
type DecidedRatification struct {
	Record   PendingRatificationRecord
	Plan     *governance.RatificationPlan
	Decision *governance.RatificationDecision
	Snapshot DeferredBoundarySnapshot
}

// ApplyDecidedRatification writes job's decided marks and deletes its pending
// record in txn. It does nothing, and reports false, when the record in txn is
// not job's. takeStakeInputs, when non-nil, stages the reward stake inputs the
// boundary capture held back and runs only when a mark snapshot is written.
func (e *Engine) ApplyDecidedRatification(
	ctx context.Context,
	txn ledgerdb.Txn,
	job DecidedRatification,
	takeStakeInputs func(ledgerdb.Txn) error,
) (bool, error) {
	rec, err := LoadPendingRatification(e.ledgerDB(), txn)
	if err != nil {
		return false, err
	}
	if rec == nil || *rec != job.Record {
		return false, nil
	}
	if job.Snapshot != nil {
		if err := job.Snapshot.Write(txn); err != nil {
			return false, fmt.Errorf(
				"write mark snapshot for epoch %d: %w", rec.Epoch, err,
			)
		}
		if takeStakeInputs != nil {
			if err := takeStakeInputs(txn); err != nil {
				return false, fmt.Errorf(
					"stage reward stake inputs for epoch %d: %w",
					rec.Epoch, err,
				)
			}
		}
	}
	if job.Plan != nil {
		if _, err := job.Plan.Apply(ctx, job.Decision, txn); err != nil {
			return false, fmt.Errorf(
				"apply ratification for epoch %d: %w", rec.Epoch, err,
			)
		}
	}
	if err := e.ledgerDB().DeleteSyncState(pendingRatificationSyncKey, txn); err != nil {
		return false, err
	}
	return true, nil
}
