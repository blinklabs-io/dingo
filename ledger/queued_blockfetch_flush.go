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

const queuedBlockfetchMaxWait = time.Second

type queuedBlockfetchFlush struct {
	head   ocommon.Point
	cancel func()
}

// armQueuedBlockfetchFlush bounds accumulation of an already admitted queue.
// Further headers keep the first deadline: resetting it on every arrival would
// let a slow stream postpone body fetching indefinitely.
func (ls *LedgerState) armQueuedBlockfetchFlush() {
	if ls.closed.Load() || ls.ctx == nil || ls.ctx.Err() != nil {
		return
	}
	head, _, count := ls.chain.HeaderRangeAfter(0, 1)
	if count == 0 {
		return
	}
	ls.queuedBlockfetchFlushMu.Lock()
	defer ls.queuedBlockfetchFlushMu.Unlock()
	if ls.closed.Load() {
		return
	}
	if current := ls.queuedBlockfetchFlush; current != nil {
		if pointMatches(current.head, head) {
			return
		}
		current.cancel()
	}
	flush := &queuedBlockfetchFlush{head: head}
	ls.queuedBlockfetchFlush = flush
	schedule := ls.queuedBlockfetchSchedule
	if schedule == nil {
		schedule = func(delay time.Duration, callback func()) func() {
			timer := time.AfterFunc(delay, callback)
			return func() { timer.Stop() }
		}
	}
	flush.cancel = schedule(queuedBlockfetchMaxWait, func() {
		ls.flushQueuedBlockfetchAfterWait(flush)
	})
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
