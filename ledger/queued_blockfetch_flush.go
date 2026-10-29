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
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// queuedBlockfetchMaxWait bounds how long an admitted queue below the batch
// threshold may accumulate. queuedBlockfetchIdleWait ends accumulation earlier
// once no header has joined the queue for that long: a queue that has stopped
// growing gains nothing from the rest of the maximum, while a steady stream
// keeps re-proving it is still growing and accumulates up to the maximum. It
// sits above the chainsync pipeline's round trip so that ordinary inter-header
// gaps in a flowing stream do not cut a batch short.
const (
	queuedBlockfetchMaxWait  = time.Second
	queuedBlockfetchIdleWait = 250 * time.Millisecond
)

type queuedBlockfetchFlush struct {
	head ocommon.Point
	// deadline is the first arming's maximum; lastHeader is when a header
	// last joined the queue. Both are guarded by queuedBlockfetchFlushMu.
	deadline   time.Time
	lastHeader time.Time
	cancel     func()
}

func (ls *LedgerState) queuedBlockfetchClock() time.Time {
	if ls.queuedBlockfetchNow != nil {
		return ls.queuedBlockfetchNow()
	}
	return time.Now()
}

// armQueuedBlockfetchFlush bounds accumulation of an already admitted queue.
// Further headers keep the first deadline: resetting it on every arrival would
// let a slow stream postpone body fetching indefinitely. They only postpone
// the idle cutoff, which never moves past that deadline.
func (ls *LedgerState) armQueuedBlockfetchFlush() {
	if ls.closed.Load() || ls.ctx == nil || ls.ctx.Err() != nil {
		return
	}
	head, _, count := ls.chain.HeaderRangeAfter(0, 1)
	if count == 0 {
		return
	}
	now := ls.queuedBlockfetchClock()
	ls.queuedBlockfetchFlushMu.Lock()
	defer ls.queuedBlockfetchFlushMu.Unlock()
	if ls.closed.Load() {
		return
	}
	if current := ls.queuedBlockfetchFlush; current != nil {
		if pointMatches(current.head, head) {
			current.lastHeader = now
			return
		}
		current.cancel()
	}
	flush := &queuedBlockfetchFlush{
		head:       head,
		deadline:   now.Add(queuedBlockfetchMaxWait),
		lastHeader: now,
	}
	ls.queuedBlockfetchFlush = flush
	ls.scheduleQueuedBlockfetchFlushLocked(flush, queuedBlockfetchIdleWait)
}

func (ls *LedgerState) scheduleQueuedBlockfetchFlushLocked(
	flush *queuedBlockfetchFlush,
	delay time.Duration,
) {
	schedule := ls.queuedBlockfetchSchedule
	if schedule == nil {
		schedule = func(delay time.Duration, callback func()) func() {
			timer := time.AfterFunc(delay, callback)
			return func() { timer.Stop() }
		}
	}
	flush.cancel = schedule(delay, func() {
		ls.queuedBlockfetchFlushDue(flush)
	})
}

// queuedBlockfetchFlushDue flushes once the queue has been idle for
// queuedBlockfetchIdleWait or the first deadline has passed, and otherwise
// re-arms for whichever of the two comes first.
func (ls *LedgerState) queuedBlockfetchFlushDue(flush *queuedBlockfetchFlush) {
	now := ls.queuedBlockfetchClock()
	ls.queuedBlockfetchFlushMu.Lock()
	if ls.queuedBlockfetchFlush != flush || ls.closed.Load() {
		ls.queuedBlockfetchFlushMu.Unlock()
		return
	}
	due := flush.lastHeader.Add(queuedBlockfetchIdleWait)
	if flush.deadline.Before(due) {
		due = flush.deadline
	}
	if now.Before(due) {
		ls.scheduleQueuedBlockfetchFlushLocked(flush, due.Sub(now))
		ls.queuedBlockfetchFlushMu.Unlock()
		return
	}
	ls.queuedBlockfetchFlushMu.Unlock()
	ls.flushQueuedBlockfetchAfterWait(flush)
}

func (ls *LedgerState) stopQueuedBlockfetchFlush() {
	ls.queuedBlockfetchFlushMu.Lock()
	defer ls.queuedBlockfetchFlushMu.Unlock()
	if flush := ls.queuedBlockfetchFlush; flush != nil {
		flush.cancel()
		ls.queuedBlockfetchFlush = nil
	}
}

func (ls *LedgerState) flushQueuedBlockfetchAfterWait(
	flush *queuedBlockfetchFlush,
) {
	// Match normal header admission's lock order. The callback itself does
	// no network work; Close already drains the continuation worker below.
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	ls.queuedBlockfetchFlushMu.Lock()
	current := ls.queuedBlockfetchFlush == flush
	if current {
		ls.queuedBlockfetchFlush = nil
	}
	ls.queuedBlockfetchFlushMu.Unlock()
	if !current || ls.closed.Load() || ls.ctx == nil || ls.ctx.Err() != nil ||
		ls.chainsyncBlockfetchReadyChan != nil ||
		ls.blockfetchContinuationPending {
		return
	}
	head, _, count := ls.chain.HeaderRangeAfter(0, 1)
	if count == 0 || !pointMatches(head, flush.head) {
		return
	}
	conn := ls.headerPipelineConnId
	if ls.config.GetActiveConnectionFunc != nil {
		active := ls.config.GetActiveConnectionFunc()
		if active == nil {
			return
		}
		conn = *active
	}
	if connIdKey(conn) == "" || !ls.isConnectionLive(conn) {
		return
	}
	ls.startQueuedBlockfetchFromEventLocked(
		conn, conn, "queued header batch flush failed",
	)
}
