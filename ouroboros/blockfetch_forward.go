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

package ouroboros

import (
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// blockfetchQueuedEvent is one blockfetch event (a decoded Block or a
// terminal BatchDone) received from a peer but not yet handed to the ledger.
type blockfetchQueuedEvent struct {
	evt   event.Event
	bytes int
}

// blockfetchForwardState is one connection's queue of blockfetch events
// received but not yet published to the ledger, plus whether a forwarder
// goroutine is currently draining it. Guarded by
// Ouroboros.blockfetchForwardMu.
type blockfetchForwardState struct {
	queue   []blockfetchQueuedEvent
	bytes   int
	running bool
}

// enqueueBlockfetchEvent hands a blockfetch event (Block or BatchDone) to
// connId's forward queue instead of publishing it to the ledger inline. This
// is the fix for blinklabs-io/dingo#4782.
//
// blockfetchClientBlock and blockfetchClientRangeDone run on the gouroboros
// blockfetch client's receive goroutine, which the shared muxer's read loop
// depends on to keep draining incoming socket data -- including unrelated
// keep-alive pongs interleaved on the same connection. The ledger subscribes
// to ledger.blockfetch with SubscriberBackpressureBlock (lossless, buffer
// blockfetchCommitBatchSize), so publishing inline there parks the calling
// goroutine for as long as the ledger takes to drain its buffer: observed in
// production to run 2-5s per event during heavy epochs, comfortably past the
// 10s keep-alive timeout deliberately kept tight elsewhere (fast dead-peer
// eviction), tearing down a merely-busy upstream as if it were dead.
//
// enqueueBlockfetchEvent never blocks this caller: it is an append under a
// mutex, drained by a dedicated per-connection goroutine
// (runBlockfetchForwarder) that performs the potentially slow publish off
// this goroutine entirely. In-flight bytes are bounded not by this queue
// ever rejecting or blocking an enqueue, but by the blockfetch pipeline depth
// already enforced in chainsync_blockfetch_pipeline.go: at most one request
// is dispatched ahead of the active batch per connection
// (startQueuedBlockfetchPrefetchLocked refuses a second), and that pre-queued
// request is only promoted once the active batch has applied at least one
// block (tryPromoteQueuedBlockfetchLocked). So at most two ranges' worth of
// blocks can ever be outstanding on one connection, regardless of how far
// behind the ledger's consumption falls -- this fix does not change that
// bound, it only moves where the resulting wait happens.
func (o *Ouroboros) enqueueBlockfetchEvent(
	connId ouroboros.ConnectionId,
	evt event.Event,
	size int,
) {
	if o.eventBus == nil {
		return
	}
	enqueueStart := time.Now()
	o.blockfetchForwardMu.Lock()
	if o.blockfetchForward == nil {
		o.blockfetchForward = make(
			map[ouroboros.ConnectionId]*blockfetchForwardState,
		)
	}
	st, ok := o.blockfetchForward[connId]
	if !ok {
		st = &blockfetchForwardState{}
		o.blockfetchForward[connId] = st
	}
	st.queue = append(st.queue, blockfetchQueuedEvent{evt: evt, bytes: size})
	st.bytes += size
	startWorker := !st.running
	st.running = true
	o.blockfetchForwardMu.Unlock()

	o.addBlockfetchInFlight(size, 1)
	if o.blockfetchMetrics != nil {
		o.blockfetchMetrics.stageEnqueue.Observe(
			time.Since(enqueueStart).Seconds(),
		)
	}
	if startWorker {
		go o.runBlockfetchForwarder(connId)
	}
}

// runBlockfetchForwarder drains connId's queued blockfetch events in FIFO
// order, one at a time, publishing each to the EventBus from this dedicated
// goroutine instead of whatever goroutine called enqueueBlockfetchEvent. It
// returns once the queue empties and is restarted by enqueueBlockfetchEvent
// when more work arrives, so an idle connection holds no goroutine.
//
// At most one instance of this ever runs for a given connId at a time: the
// running flag transition from false to true happens under
// blockfetchForwardMu, in enqueueBlockfetchEvent, exactly once per
// idle-to-active transition; this function clears it back to false, also
// under the mutex, only once its own queue read observes nothing left to
// drain, which is the same point it returns. FIFO order within a connection
// is therefore preserved: no second goroutine can start draining the same
// connId's queue while this one is still running, and this one processes
// strictly in append order.
func (o *Ouroboros) runBlockfetchForwarder(connId ouroboros.ConnectionId) {
	for {
		o.blockfetchForwardMu.Lock()
		st := o.blockfetchForward[connId]
		if st == nil || len(st.queue) == 0 {
			if st != nil {
				st.running = false
				if st.bytes == 0 {
					delete(o.blockfetchForward, connId)
				}
			}
			o.blockfetchForwardMu.Unlock()
			return
		}
		qe := st.queue[0]
		st.queue[0] = blockfetchQueuedEvent{}
		st.queue = st.queue[1:]
		o.blockfetchForwardMu.Unlock()

		publishStart := time.Now()
		o.eventBus.Publish(ledger.BlockfetchEventType, qe.evt)
		if o.blockfetchMetrics != nil {
			o.blockfetchMetrics.stageLedgerPublish.Observe(
				time.Since(publishStart).Seconds(),
			)
		}

		o.blockfetchForwardMu.Lock()
		st.bytes -= qe.bytes
		o.blockfetchForwardMu.Unlock()
		o.addBlockfetchInFlight(-qe.bytes, -1)
	}
}

// addBlockfetchInFlight adjusts the aggregate in-flight blockfetch gauges.
// Safe to call when metrics are not initialized.
func (o *Ouroboros) addBlockfetchInFlight(bytesDelta int, blocksDelta int) {
	if o.blockfetchMetrics == nil {
		return
	}
	o.blockfetchMetrics.inFlightBytes.Add(float64(bytesDelta))
	o.blockfetchMetrics.inFlightBlocks.Add(float64(blocksDelta))
}
