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
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// networkHoldState records outstanding HoldNetworkIngest calls.
type networkHoldState struct {
	// held is read on every chainsync and blockfetch event, so it is an
	// atomic rather than a field under mu.
	held atomic.Bool
	// everHeld keeps a node that was never held from asking the tracker
	// about every event.
	everHeld atomic.Bool
	// mu guards holds and orders the first hold's discard against the last
	// release's resync, so a hold racing a release cannot interleave them.
	mu    sync.Mutex
	holds int
}

// HoldNetworkIngest stops the ledger storing anything Ouroboros delivers until
// release is called. Peers stay connected and the inbound chainsync and
// blockfetch servers keep serving committed history, but client chainsync
// headers and rollbacks are dropped without being queued and no blockfetch is
// issued. The headers already queued and any blockfetch batch in flight are
// discarded before it returns, so on return the network adds nothing to the
// chain and an external writer, such as ApplyTrustedBatch, meets no queued
// header.
//
// Holds nest: ingest resumes when every release has been called, and calling
// one release more than once has no further effect. The last release resets
// the header and blockfetch pipeline and publishes a ChainsyncResyncEvent
// with reason ChainsyncResyncReasonNetworkIngestReleased at the current chain
// tip, which closes every chainsync client so each reconnects and
// re-intersects from that tip. With a NetworkIngestHoldTracker configured, a
// connection that existed during the hold stays dropped after the release
// until it closes.
func (ls *LedgerState) HoldNetworkIngest() (release func()) {
	h := &ls.networkHold
	h.mu.Lock()
	defer h.mu.Unlock()
	h.holds++
	if h.holds == 1 {
		// Mark the connections before the flag: none can deliver unheld
		// once the flag is visible.
		if tracker := ls.config.NetworkIngestHoldTracker; tracker != nil {
			tracker.SetNetworkIngestHeld(true)
		}
		h.everHeld.Store(true)
		// Set before the discard takes the handler mutexes: a handler that
		// already holds them finishes first, and every later one sees the
		// hold.
		h.held.Store(true)
		ls.resetNetworkIngestPipeline(nil)
	}
	return sync.OnceFunc(ls.releaseNetworkIngest)
}

// NetworkIngestHeld reports whether a HoldNetworkIngest is outstanding.
func (ls *LedgerState) NetworkIngestHeld() bool {
	return ls.networkHold.held.Load()
}

// connectionIngestHeld reports whether connId's messages are dropped: ingest
// is held, or the connection existed during a hold and has not closed.
func (ls *LedgerState) connectionIngestHeld(
	connId ouroboros.ConnectionId,
) bool {
	if ls.NetworkIngestHeld() {
		return true
	}
	if !ls.networkHold.everHeld.Load() {
		return false
	}
	tracker := ls.config.NetworkIngestHoldTracker
	return tracker != nil && tracker.ConnectionIngestHeld(connId)
}

func (ls *LedgerState) releaseNetworkIngest() {
	h := &ls.networkHold
	h.mu.Lock()
	defer h.mu.Unlock()
	h.holds--
	if h.holds > 0 {
		return
	}
	// Stop marking new connections only after the flag clears, so no
	// connection is ever unmarked while its messages are being dropped.
	if tracker := ls.config.NetworkIngestHoldTracker; tracker != nil {
		defer tracker.SetNetworkIngestHeld(false)
	}
	if ls.chain == nil {
		h.held.Store(false)
		return
	}
	ls.resetNetworkIngestPipeline(func(pending *pendingPublishes) {
		h.held.Store(false)
		ls.setChainsyncState(SyncingChainsyncState)
		// Every client's cursor walked past the chain while held, and the
		// headers it delivered were dropped, so each must re-intersect.
		pending.add(
			ls.config.EventBus,
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					Reason: event.ChainsyncResyncReasonNetworkIngestReleased,
					Point:  ls.chain.Tip().Point,
				},
			),
		)
	})
}

// resetNetworkIngestPipeline discards the queued headers, buffered peer
// headers and blockfetch batch under the chainsync handler mutexes, then runs
// then, if set, still under chainsyncMutex. Events it queues are published
// after the mutexes are released.
func (ls *LedgerState) resetNetworkIngestPipeline(
	then func(*pendingPublishes),
) {
	if ls.chain == nil {
		return
	}
	var pending pendingPublishes
	defer pending.flush()
	pending.drainChain(ls.chain)
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	ls.resetChainsyncResyncState(&pending)
	if then != nil {
		then(&pending)
	}
}
