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

package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/governance"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// pendingRatificationSyncKey records a boundary whose RATIFY decision or mark
// snapshot has not been written yet. The boundary transaction writes it;
// whichever of the background job or the next boundary writes them deletes it
// in the same transaction.
const pendingRatificationSyncKey = "dingo:governance:ratify-pending"

type pendingRatificationRecord struct {
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
	Write(txn *database.Txn) error
}

// ratificationJob builds one boundary's deferred mark snapshot and decides its
// RATIFY on a read transaction pinned at that boundary's commit.
type ratificationJob struct {
	record      pendingRatificationRecord
	plan        *governance.RatificationPlan
	snapshotEvt *event.EpochTransitionEvent
	decided     chan struct{}
	decision    *governance.RatificationDecision
	snapshot    DeferredBoundarySnapshot
	err         error
	// settled closes once the decision is durable or the pending boundary was
	// rolled back.
	settled     chan struct{}
	settledOnce sync.Once
	applying    bool
}

func (j *ratificationJob) settle() {
	j.settledOnce.Do(func() { close(j.settled) })
}

func loadPendingRatification(
	db *database.Database,
	txn *database.Txn,
) (*pendingRatificationRecord, error) {
	raw, err := db.GetSyncState(pendingRatificationSyncKey, txn)
	if err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var rec pendingRatificationRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, fmt.Errorf("decode pending ratification: %w", err)
	}
	return &rec, nil
}

// deferBoundaryJob records the boundary's pending work in its transaction
// and, once it commits, pins a read transaction and runs the work on it in the
// background: mark[epoch] when snapshotEvt is set, then plan's RATIFY.
func (ls *LedgerState) deferBoundaryJob(
	txn *database.Txn,
	epoch uint64,
	boundarySlot uint64,
	plan *governance.RatificationPlan,
	snapshotEvt *event.EpochTransitionEvent,
) error {
	rec := pendingRatificationRecord{
		Epoch:        epoch,
		BoundarySlot: boundarySlot,
		ID:           ls.ratificationSeq.Add(1),
		Snapshot:     snapshotEvt != nil,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode pending ratification: %w", err)
	}
	if err := ls.db.SetSyncState(
		pendingRatificationSyncKey, string(raw), txn,
	); err != nil {
		return err
	}
	if snapshotEvt != nil {
		h := ls.deferredBoundarySnapshotHook.Load()
		if h == nil {
			return errors.New("deferred boundary snapshot hook unset")
		}
		h.announce(epoch)
	}
	job := &ratificationJob{
		record:      rec,
		plan:        plan,
		snapshotEvt: snapshotEvt,
		decided:     make(chan struct{}),
		settled:     make(chan struct{}),
	}
	// The callback runs in the committing goroutine before it returns, so no
	// later block has committed yet; the first read below fixes the snapshot.
	txn.AfterCommit(func() {
		snapshot := ls.db.Transaction(false)
		pinned, err := loadPendingRatification(ls.db, snapshot)
		if err == nil && (pinned == nil || *pinned != rec) {
			err = errors.New("pending ratification missing from its snapshot")
		}
		ls.ratificationMu.Lock()
		if previous := ls.ratificationJob; previous != nil {
			previous.settle()
		}
		ls.ratificationJob = job
		closed := ls.closed.Load()
		if !closed {
			ls.ratificationWG.Add(1)
		}
		ls.ratificationMu.Unlock()
		if closed {
			snapshot.Release()
			return
		}
		go func() {
			defer ls.ratificationWG.Done()
			ls.runRatificationJob(job, snapshot, err)
		}()
	})
	return nil
}

func (ls *LedgerState) runRatificationJob(
	job *ratificationJob,
	snapshot *database.Txn,
	pinErr error,
) {
	var (
		decision *governance.RatificationDecision
		prepared DeferredBoundarySnapshot
	)
	err := pinErr
	if err == nil && job.snapshotEvt != nil {
		prepared, err = ls.prepareDeferredBoundarySnapshot(
			snapshot, *job.snapshotEvt,
		)
		if err == nil && job.plan != nil {
			job.plan.SetBoundarySPOState(spoVotingState(prepared.SPOStakeRows()))
		}
	}
	if err == nil && job.plan != nil {
		decision, err = job.plan.Decide(snapshot)
	}
	snapshot.Release()
	ls.ratificationMu.Lock()
	job.decision = decision
	job.snapshot = prepared
	job.err = err
	close(job.decided)
	ls.ratificationMu.Unlock()
	if err != nil {
		ls.config.Logger.Error(
			"governance ratification failed; the next boundary will fail "+
				"until a restart rewinds below this one",
			"component", "ledger",
			"epoch", job.record.Epoch,
			"error", err,
		)
		return
	}
	if ls.ratificationApplyHook != nil {
		ls.ratificationApplyHook(job.record.Epoch)
	}
	ls.applyRatificationJob(job)
}

// applyRatificationJob writes a decided job's marks in its own transaction
// unless the next boundary or a rollback already consumed the record.
func (ls *LedgerState) applyRatificationJob(job *ratificationJob) {
	ls.ratificationMu.Lock()
	if job.applying || ls.ratificationJob != job || ls.closed.Load() {
		ls.ratificationMu.Unlock()
		return
	}
	job.applying = true
	ls.ratificationMu.Unlock()
	defer func() {
		ls.ratificationMu.Lock()
		job.applying = false
		ls.ratificationMu.Unlock()
	}()
	ls.rewardPrecomputeWriteMu.Lock()
	defer ls.rewardPrecomputeWriteMu.Unlock()
	// A rollback in flight may delete the record; the rollback retries this
	// once it finishes.
	if ls.rewardInputRollbackActive.Load() != 0 {
		return
	}
	txn := ls.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		return ls.writeRatificationDecision(txn, job)
	})
	if err != nil {
		ls.config.Logger.Warn(
			"failed to write governance ratification; the next boundary "+
				"writes it",
			"component", "ledger",
			"epoch", job.record.Epoch,
			"error", err,
		)
	}
}

// writeRatificationDecision writes job's decided marks and deletes its record
// in txn, doing nothing when the record in txn is not job's.
func (ls *LedgerState) writeRatificationDecision(
	txn *database.Txn,
	job *ratificationJob,
) error {
	rec, err := loadPendingRatification(ls.db, txn)
	if err != nil {
		return err
	}
	if rec == nil || *rec != job.record {
		return nil
	}
	if job.snapshot != nil {
		if err := job.snapshot.Write(txn); err != nil {
			return fmt.Errorf(
				"write mark snapshot for epoch %d: %w", rec.Epoch, err,
			)
		}
		if err := ls.takeDeferredRewardStakeInputs(txn); err != nil {
			return fmt.Errorf(
				"stage reward stake inputs for epoch %d: %w", rec.Epoch, err,
			)
		}
	}
	if job.plan != nil {
		if _, err := job.plan.Apply(job.decision, txn); err != nil {
			return fmt.Errorf(
				"apply ratification for epoch %d: %w", rec.Epoch, err,
			)
		}
	}
	if err := ls.db.DeleteSyncState(pendingRatificationSyncKey, txn); err != nil {
		return err
	}
	txn.AfterCommit(job.settle)
	return nil
}

// consumePendingRatification writes the previous boundary's RATIFY decision
// in this boundary's transaction if the background job has not, waiting for
// the job to decide. It runs before anything else at the boundary, so ENACT
// and DROP read the same marks the boundary before would have written.
func (ls *LedgerState) consumePendingRatification(txn *database.Txn) error {
	rec, err := loadPendingRatification(ls.db, txn)
	if err != nil || rec == nil {
		return err
	}
	ls.ratificationMu.Lock()
	job := ls.ratificationJob
	ls.ratificationMu.Unlock()
	if job == nil || job.record != *rec {
		return fmt.Errorf(
			"pending ratification for epoch %d at slot %d has no job; "+
				"restart to rewind below that boundary",
			rec.Epoch, rec.BoundarySlot,
		)
	}
	select {
	case <-job.decided:
	case <-ls.closeCh():
		return errors.New("ledger closing while waiting for ratification")
	}
	if job.err != nil {
		return fmt.Errorf(
			"ratification for epoch %d failed: %w", rec.Epoch, job.err,
		)
	}
	return ls.writeRatificationDecision(txn, job)
}

func (ls *LedgerState) closeCh() <-chan struct{} {
	if ls.publishCtx == nil {
		return nil
	}
	return ls.publishCtx.Done()
}

func (ls *LedgerState) closeCtx() context.Context {
	if ls.publishCtx == nil {
		return context.Background()
	}
	return ls.publishCtx
}

// WaitEpochBoundaryJob blocks until the most recent boundary's deferred work
// is durable: its RATIFY decision and, when the boundary left it, its mark
// snapshot. A reader of ratified or expired marks, or of that epoch's mark
// snapshot, sees what the boundary decided.
func (ls *LedgerState) WaitEpochBoundaryJob(ctx context.Context) error {
	ls.ratificationMu.Lock()
	job := ls.ratificationJob
	ls.ratificationMu.Unlock()
	if job == nil {
		return nil
	}
	select {
	case <-job.settled:
		return nil
	case <-job.decided:
		if job.err != nil {
			return fmt.Errorf(
				"ratification for epoch %d failed: %w",
				job.record.Epoch, job.err,
			)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-job.settled:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// discardPendingRatificationAfterSlot deletes, in the rollback transaction, a
// pending record whose boundary the rollback removes.
func (ls *LedgerState) discardPendingRatificationAfterSlot(
	txn *database.Txn,
	slot uint64,
) error {
	rec, err := loadPendingRatification(ls.db, txn)
	if err != nil || rec == nil || rec.BoundarySlot <= slot {
		return err
	}
	if err := ls.db.DeleteSyncState(pendingRatificationSyncKey, txn); err != nil {
		return err
	}
	txn.AfterCommit(func() {
		ls.ratificationMu.Lock()
		job := ls.ratificationJob
		if job != nil && job.record == *rec {
			ls.ratificationJob = nil
			job.settle()
		}
		ls.ratificationMu.Unlock()
	})
	return nil
}

// retryRatificationApply writes a decided job a rollback kept from writing.
func (ls *LedgerState) retryRatificationApply() {
	ls.ratificationMu.Lock()
	job := ls.ratificationJob
	ls.ratificationMu.Unlock()
	if job == nil {
		return
	}
	select {
	case <-job.decided:
	default:
		return
	}
	if job.err != nil {
		return
	}
	go ls.applyRatificationJob(job)
}

// resumePendingRatification rewinds below a boundary whose RATIFY decision a
// previous run never wrote. The snapshot it was deciding on is gone, and
// deciding on later state would count votes and stake from after the
// boundary. It records the rewind as a rollback intent, which start-up then
// recovers; the intent's block and byte limits bound how far back it can
// reach, and beyond them start-up fails rather than decide on the wrong state.
func (ls *LedgerState) resumePendingRatification() error {
	if err := ls.resumePendingRatificationIntent(); err != nil {
		return err
	}
	return ls.recoverRollbackIntent()
}

func (ls *LedgerState) resumePendingRatificationIntent() error {
	rec, err := loadPendingRatification(ls.db, nil)
	if err != nil || rec == nil {
		return err
	}
	block, err := database.BlockBeforeSlot(ls.db, rec.BoundarySlot)
	if err != nil {
		return fmt.Errorf(
			"find block before pending ratification boundary at slot %d: %w",
			rec.BoundarySlot, err,
		)
	}
	point := ocommon.NewPoint(block.Slot, block.Hash)
	var blocks []models.Block
	txn := ls.db.Transaction(false)
	if err := txn.Do(func(txn *database.Txn) error {
		var err error
		blocks, err = database.BlocksAfterSlotTxn(txn, point.Slot)
		return err
	}); err != nil {
		return fmt.Errorf("read blocks above rewind point: %w", err)
	}
	slices.Reverse(blocks)
	ls.config.Logger.Warn(
		"rewinding below a boundary whose ratification never completed",
		"component", "ledger",
		"epoch", rec.Epoch,
		"boundary_slot", rec.BoundarySlot,
		"rewind_slot", point.Slot,
		"blocks", len(blocks),
	)
	if err := persistRollbackIntent(ls.db, point, blocks); err != nil {
		return fmt.Errorf(
			"rewind below pending ratification for epoch %d: %w",
			rec.Epoch, err,
		)
	}
	return nil
}

type deferredBoundarySnapshotHookHolder struct {
	announce func(epoch uint64)
	prepare  func(*database.Txn, event.EpochTransitionEvent) (
		DeferredBoundarySnapshot, error,
	)
}

// SetDeferredEpochBoundarySnapshotHooks installs what a boundary needs to
// leave mark[NewEpoch] to its background job: announce runs in the boundary
// transaction before it commits, and prepare builds the snapshot reading only
// the given transaction. Without them every boundary captures its snapshot
// itself.
func (ls *LedgerState) SetDeferredEpochBoundarySnapshotHooks(
	announce func(epoch uint64),
	prepare func(*database.Txn, event.EpochTransitionEvent) (
		DeferredBoundarySnapshot, error,
	),
) {
	if announce == nil || prepare == nil {
		ls.deferredBoundarySnapshotHook.Store(nil)
		return
	}
	ls.deferredBoundarySnapshotHook.Store(
		&deferredBoundarySnapshotHookHolder{
			announce: announce, prepare: prepare,
		},
	)
}

func (ls *LedgerState) prepareDeferredBoundarySnapshot(
	txn *database.Txn,
	evt event.EpochTransitionEvent,
) (DeferredBoundarySnapshot, error) {
	h := ls.deferredBoundarySnapshotHook.Load()
	if h == nil {
		return nil, errors.New("deferred boundary snapshot hook unset")
	}
	prepared, err := h.prepare(txn, evt)
	if err != nil {
		return nil, fmt.Errorf("prepare mark snapshot: %w", err)
	}
	return prepared, nil
}

func spoVotingState(
	rows []*models.PoolStakeSnapshot,
) *governance.SPOVotingState {
	var total uint64
	for _, r := range rows {
		total += uint64(r.TotalStake)
	}
	return &governance.SPOVotingState{Dist: rows, TotalStake: total}
}
