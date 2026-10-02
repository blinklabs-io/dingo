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

// blockfetchForwardMaxBytes bounds the block bytes one connection's forward
// queue may hold. The ledger requests at most an active and a pre-queued range
// per connection and the gouroboros in-flight byte budget admits
// blockfetchMaxInFlightBytes of outstanding ranges, so twice that budget is
// headroom a peer reaches only when requests released by the ledger keep
// streaming while the ledger is stalled.
const blockfetchForwardMaxBytes = 2 * blockfetchMaxInFlightBytes

// blockfetchForwardMaxEvents bounds the event count of one connection's
// forward queue, which the byte bound alone does not limit for ranges of
// small blocks: twice the blocks and BatchDone markers of the ranges the
// in-flight byte budget admits.
const blockfetchForwardMaxEvents = 2 *
	(blockfetchMaxInFlightBytes / ledger.BlockfetchMaxRangeBytes) *
	(ledger.BlockfetchBatchSize + 1)

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
	// overflowed is set when an enqueue would exceed the queue bounds. The
	// connection is then being closed, and every later event for it is
	// dropped until HandleConnClosedEvent removes this state.
	overflowed bool
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
// The gouroboros in-flight byte budget does not bound the queue, because that
// budget is released when gouroboros finishes receiving a range, which does
// not wait for the ledger. Nor does the ledger's limit of an active and a
// pre-queued range per connection: blockfetchRequestRangeCleanup releases
// tracked requests on timeout, rollback and fork restart while they may still
// be streaming, and a retry can redispatch on the same connection. The queue
// is therefore bounded here, by blockfetchForwardMaxBytes and
// blockfetchForwardMaxEvents. Waiting for space would reintroduce the stall
// described above, so an event that would exceed either bound instead
// terminates the connection: the event and every queued event not yet taken
// by the forwarder are discarded, the connection is closed off the receive
// goroutine, and later events for it are dropped. The ledger then handles the
// close as it does any other, releasing the connection's requests so they are
// fetched again elsewhere.
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
	if st.overflowed {
		o.blockfetchForwardMu.Unlock()
		return
	}
	maxBytes, maxEvents := o.blockfetchForwardLimits()
	if st.bytes+size > maxBytes || len(st.queue)+1 > maxEvents {
		queuedBytes, queuedEvents := st.bytes, len(st.queue)
		var discardedBytes int
		for _, qe := range st.queue {
			discardedBytes += qe.bytes
		}
		discardedEvents := len(st.queue)
		st.queue = nil
		st.bytes -= discardedBytes
		st.overflowed = true
		o.blockfetchForwardMu.Unlock()
		o.addBlockfetchInFlight(-discardedBytes, -discardedEvents)
		if o.blockfetchMetrics != nil {
			o.blockfetchMetrics.forwardOverflows.Inc()
		}
		o.config.Logger.Warn(
			"blockfetch: forward queue limit reached, closing connection",
			"connection_id", connId.String(),
			"queued_bytes", queuedBytes,
			"queued_events", queuedEvents,
			"max_bytes", maxBytes,
			"max_events", maxEvents,
		)
		if o.blockfetchForwardClose != nil {
			go o.blockfetchForwardClose(connId)
		} else {
			go o.blockfetchForwardCloseLive(connId)
		}
		return
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
		run := func(connId ouroboros.ConnectionId) {
			o.runBlockfetchForwarder(connId, st)
		}
		if o.blockfetchForwardSpawn != nil {
			o.blockfetchForwardSpawn(connId, run)
		} else {
			go run(connId)
		}
	}
}

// runBlockfetchForwarder publishes the queued blockfetch events of st, the
// forward state of connId, in FIFO order, one at a time, and returns once the
// queue is empty, so an idle connection holds no goroutine.
//
// At most one forwarder runs per state, which is what preserves FIFO order:
// running is set only by the enqueue that finds it clear, and cleared only
// here, under blockfetchForwardMu, at the same point this goroutine observes
// an empty queue and returns. The forwarder drains the state it was started
// for rather than re-reading the map, so a state replaced after a close
// cannot gain a second forwarder.
func (o *Ouroboros) runBlockfetchForwarder(
	connId ouroboros.ConnectionId,
	st *blockfetchForwardState,
) {
	for {
		o.blockfetchForwardMu.Lock()
		if len(st.queue) == 0 {
			st.running = false
			if st.bytes == 0 && !st.overflowed &&
				o.blockfetchForward[connId] == st {
				delete(o.blockfetchForward, connId)
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

// blockfetchForwardLimits returns the per-connection forward queue bounds in
// bytes and events.
func (o *Ouroboros) blockfetchForwardLimits() (int, int) {
	maxBytes := blockfetchForwardMaxBytes
	if o.blockfetchForwardMaxBytes > 0 {
		maxBytes = o.blockfetchForwardMaxBytes
	}
	maxEvents := blockfetchForwardMaxEvents
	if o.blockfetchForwardMaxEvents > 0 {
		maxEvents = o.blockfetchForwardMaxEvents
	}
	return maxBytes, maxEvents
}

// releaseBlockfetchForwardOverflow removes connId's forward state once the
// connection has closed, if that state overflowed, so a later connection with
// the same ConnectionId is not left dropping its events. A state that did not
// overflow is left to its forwarder, which removes it once drained.
func (o *Ouroboros) releaseBlockfetchForwardOverflow(
	connId ouroboros.ConnectionId,
) {
	o.blockfetchForwardMu.Lock()
	defer o.blockfetchForwardMu.Unlock()
	if st, ok := o.blockfetchForward[connId]; ok && st.overflowed {
		delete(o.blockfetchForward, connId)
	}
}

// blockfetchForwardCloseLive closes connId through connManager. This is the
// production value of the Ouroboros.blockfetchForwardClose seam.
func (o *Ouroboros) blockfetchForwardCloseLive(connId ouroboros.ConnectionId) {
	if o.connManager == nil {
		return
	}
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil {
		return
	}
	o.closeBlockfetchConnection(
		conn,
		connId.String(),
		"blockfetch forward queue limit reached",
	)
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
