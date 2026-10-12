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
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
)

// ErrQueryViewClosed is returned by QueryView.Query once the view has been
// closed, whether by its owner or by a forced expiry.
var ErrQueryViewClosed = errors.New("ledger query view is closed")

// QueryView is a read-only snapshot of the ledger that a LocalStateQuery
// session holds between Acquire and Release. Every query answered through it
// reads one database read transaction opened at acquire time, so a block
// applied or a rollback committed afterwards is not visible to the session.
//
// A view pins a database read transaction for its whole lifetime, which holds
// back WAL checkpoints and a read connection; owners must Close it promptly
// and bound how long they let it live.
type QueryView struct {
	ls  *LedgerState
	at  QueryPoint
	txn *database.Txn
	// transitionInfo freezes the in-memory forecast that accompanied the
	// database snapshot. Era-history queries must not observe a later forecast
	// while every persisted input remains pinned at Acquire.
	transitionInfo hardfork.TransitionInfo
	// cancel ends the context txn's metadata transaction is bound to. The
	// database cancels a transaction whose context ends, so the view owns
	// that context rather than inheriting the caller's.
	cancel context.CancelFunc

	mu       sync.Mutex
	inFlight int
	closed   bool
}

// AcquireQueryView opens a snapshot of the ledger and validates at against it.
// An unpinned at (the zero QueryPoint) snapshots the live tip; a pinned at
// must pass VerifyPointQueryable, and its error is returned unchanged so the
// caller can map the ErrPointNotOnChain and ErrHistoricalStateUnavailable
// sentinels to protocol-level Acquire failures.
//
// The snapshot is a database.NewReadSnapshotContext: its blob and metadata
// views are opened at one commit boundary, and it counts against the read
// snapshot admission cap, which keeps one metadata read connection free for
// the rest of the node however many views are held. When the cap is reached
// AcquireQueryView waits for a view to close, and returns ctx's error if ctx
// ends first. ctx bounds only opening the view, not the view's lifetime.
func (ls *LedgerState) AcquireQueryView(
	ctx context.Context,
	at QueryPoint,
) (*QueryView, error) {
	for {
		// The latest boundary's mark snapshot and RATIFY marks may still be
		// written by its background job; open the snapshot only after it has
		// decided, so the view never freezes a half-written boundary. The
		// wait sits inside ctx so a slow job cannot outlast the caller's
		// deadline.
		if err := ls.WaitEpochBoundaryJob(ctx); err != nil {
			return nil, err
		}
		txn, cancelView, err := ls.openQueryViewSnapshot(ctx)
		if err != nil {
			return nil, err
		}
		// A boundary can commit before its AfterCommit callback publishes
		// the job, so the wait above may have seen no job. The pending
		// record in the snapshot is the authority: when one is still there
		// the snapshot froze a half-written boundary, so drop it and wait
		// for the job to settle.
		pending, err := loadPendingRatification(ls.db, txn)
		if err != nil {
			txn.Release()
			cancelView()
			return nil, err
		}
		if pending != nil {
			txn.Release()
			cancelView()
			if err := ls.waitPendingBoundaryJob(ctx, *pending); err != nil {
				return nil, err
			}
			continue
		}
		if err := ls.VerifyPointQueryable(ctx, txn, at); err != nil {
			txn.Release()
			cancelView()
			return nil, err
		}
		transitionInfo := ls.loadConsensusSnapshot().transitionInfo
		return &QueryView{
			ls:             ls,
			at:             at,
			txn:            txn,
			cancel:         cancelView,
			transitionInfo: transitionInfo,
		}, nil
	}
}

// waitPendingBoundaryJob waits until the job for pending is published and
// settled, or the record is gone. A boundary's commit precedes its job's
// publication, so the job may not exist yet.
func (ls *LedgerState) waitPendingBoundaryJob(
	ctx context.Context,
	pending pendingRatificationRecord,
) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		ls.ratificationMu.Lock()
		job := ls.ratificationJob
		ls.ratificationMu.Unlock()
		if job != nil && job.record == pending {
			return ls.WaitEpochBoundaryJob(ctx)
		}
		select {
		case <-ticker.C:
			// The record may have been consumed by the next boundary.
			current, err := loadPendingRatification(ls.db, nil)
			if err != nil {
				return err
			}
			if current == nil || *current != pending {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("wait for epoch boundary job: %w", ctx.Err())
		}
	}
}

// openQueryViewSnapshot opens the read snapshot a view holds. ctx bounds
// only the open.
func (ls *LedgerState) openQueryViewSnapshot(
	ctx context.Context,
) (*database.Txn, context.CancelFunc, error) {
	viewCtx, cancelView := context.WithCancel(context.WithoutCancel(ctx))
	stopWatch := context.AfterFunc(ctx, cancelView)
	txn, _, err := database.NewReadSnapshotContext(viewCtx, ls.db)
	if !stopWatch() {
		// ctx ended while the snapshot was opening and cancelled viewCtx,
		// which also ends a transaction that did open.
		if err == nil {
			txn.Release()
		}
		cancelView()
		return nil, nil, fmt.Errorf("open ledger snapshot: %w", ctx.Err())
	}
	if err != nil {
		cancelView()
		return nil, nil, err
	}
	return txn, cancelView, nil
}

// Query answers a decoded LocalStateQuery message from the view's snapshot.
// It returns ErrQueryViewClosed once the view is closed.
func (v *QueryView) Query(
	ctx context.Context,
	query any,
	protocolVersion uint16,
) (result any, err error) {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil, ErrQueryViewClosed
	}
	v.inFlight++
	v.mu.Unlock()
	defer v.finishQuery()
	// Follow Txn.Do's panic contract: a panic in a store helper fails this
	// query rather than reaching the connection's handler goroutine.
	defer func() {
		if r := recover(); r != nil {
			if logger := v.ls.config.Logger; logger != nil {
				logger.Error(
					"panic in ledger query view",
					"component", "ledger",
					"panic", fmt.Sprintf("%v", r),
					"stack", string(debug.Stack()),
				)
			}
			result, err = nil, database.NewTxnPanicError("query view", r)
		}
	}()
	return v.ls.queryInTxnWithTransition(
		ctx, query, v.at, protocolVersion, v.txn, &v.transitionInfo,
	)
}

func (v *QueryView) finishQuery() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.inFlight--
	if v.closed && v.inFlight == 0 {
		v.release()
	}
}

// Close releases the snapshot. It does not wait for a query in flight: that
// query completes against the snapshot and the last one out releases it, so a
// forced expiry never blocks on a slow query. Close is idempotent.
func (v *QueryView) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return
	}
	v.closed = true
	if v.inFlight == 0 {
		v.release()
	}
}

func (v *QueryView) release() {
	v.txn.Release()
	v.cancel()
}
