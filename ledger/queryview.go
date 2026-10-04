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
	"errors"
	"sync"

	"github.com/blinklabs-io/dingo/database"
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

	mu       sync.Mutex
	inFlight int
	closed   bool
}

// AcquireQueryView opens a snapshot of the ledger and validates at against it.
// An unpinned at (the zero QueryPoint) snapshots the live tip; a pinned at
// must pass VerifyPointQueryable, and its error is returned unchanged so the
// caller can map the ErrPointNotOnChain and ErrHistoricalStateUnavailable
// sentinels to protocol-level Acquire failures.
func (ls *LedgerState) AcquireQueryView(at QueryPoint) (*QueryView, error) {
	// The latest boundary's mark snapshot and RATIFY marks may still be
	// written by its background job; open the snapshot only after it has
	// decided, so the view never freezes a half-written boundary.
	if err := ls.WaitEpochBoundaryJob(ls.closeCtx()); err != nil {
		return nil, err
	}
	txn := ls.db.Transaction(false)
	// SQLite starts a deferred read transaction's snapshot at its first
	// read, not at BEGIN, so read once now to freeze the state this Acquire
	// observed.
	if _, err := ls.db.GetTip(txn); err != nil {
		txn.Release()
		return nil, err
	}
	if err := ls.VerifyPointQueryable(txn, at); err != nil {
		txn.Release()
		return nil, err
	}
	return &QueryView{ls: ls, at: at, txn: txn}, nil
}

// Query answers a decoded LocalStateQuery message from the view's snapshot.
// It returns ErrQueryViewClosed once the view is closed.
func (v *QueryView) Query(
	query any,
	protocolVersion uint16,
) (any, error) {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil, ErrQueryViewClosed
	}
	v.inFlight++
	v.mu.Unlock()
	defer v.finishQuery()
	return v.ls.queryInTxn(query, v.at, protocolVersion, v.txn)
}

func (v *QueryView) finishQuery() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.inFlight--
	if v.closed && v.inFlight == 0 {
		v.txn.Release()
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
		v.txn.Release()
	}
}
