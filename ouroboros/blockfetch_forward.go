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
// connId's forward queue, to be published to the ledger by that connection's
// forwarder goroutine (runBlockfetchForwarder).
//
// Its callers run on the gouroboros blockfetch receive goroutine, which the
// shared muxer's read loop depends on to keep draining the socket, including
// keep-alive pongs for the same connection. The ledger subscribes to
// ledger.blockfetch with lossless SubscriberBackpressureBlock, so a Publish
// there waits for the ledger to drain its buffer; making that wait here would
// stall the muxer and let the keep-alive timeout close a busy but live peer.
// This function therefore never blocks on the ledger: it appends under a
// mutex and returns. Events for one connection reach the ledger in the order
// they were enqueued.
//
// The queue itself is unbounded; its length is whatever has been received on
// the connection and not yet consumed by the ledger. The gouroboros in-flight
// byte budget (blockfetchMaxInFlightBytes) does not bound it, because that
// budget is released when gouroboros finishes receiving a range, which does
// not wait for the ledger. The ledger tracks at most an active and a pre-queued range
// per connection, but blockfetchRequestRangeCleanup releases tracked requests
// on timeout, rollback and fork restart while they may still be streaming,
// and a retry can redispatch on the same connection, so the backlog is not
// limited to a fixed number of ranges. dingo_blockfetch_inflight_bytes and
// dingo_blockfetch_inflight_blocks report it.
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
		if o.blockfetchForwardSpawn != nil {
			o.blockfetchForwardSpawn(connId, o.runBlockfetchForwarder)
		} else {
			go o.runBlockfetchForwarder(connId)
		}
	}
}

// runBlockfetchForwarder publishes connId's queued blockfetch events in FIFO
// order, one at a time, and returns once the queue is empty, so an idle
// connection holds no goroutine.
//
// At most one forwarder runs per connection, which is what preserves FIFO
// order: running is set only by the enqueue that finds it clear, and cleared
// only here, under blockfetchForwardMu, at the same point this goroutine
// observes an empty queue and returns.
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

		if o.blockfetchForwardBeforePublish != nil {
			o.blockfetchForwardBeforePublish(connId, qe.evt)
		}
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
