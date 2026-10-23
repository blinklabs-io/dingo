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
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// HeldPeerTipRefreshInterval is how often a held chainsync client's tip is
// refreshed while network ingest is held, so chainsync.State.PeerTipSlot lags
// a peer's tip by at most this long. It must stay well below the ChainSync
// idle timeout (ochainsync.IdleTimeout, 3673s), since a held client sends
// nothing else.
const HeldPeerTipRefreshInterval = 30 * time.Second

// heldTipClient is the part of a chainsync client the refresher uses.
// GetCurrentTip sends MsgFindIntersect with no points; the peer answers
// IntersectNotFound carrying its tip, and the client's read pointer and the
// protocol's next state (Idle) are unchanged. gouroboros queues it behind
// any RequestNext replies still outstanding, so it is safe on a client whose
// sync loop has just been stopped.
type heldTipClient interface {
	GetCurrentTip() (*ochainsync.Tip, error)
}

// heldTipRefresher runs while network ingest is held. A held message stops
// its client's sync loop (ochainsync.ErrStopSyncProcess), so a held client
// issues no RequestNext after the replies already in flight; the refresher
// keeps each one's tip current instead.
type heldTipRefresher struct {
	mu       sync.Mutex
	running  bool
	closed   bool
	stop     chan struct{}
	wg       sync.WaitGroup
	inFlight map[ouroboros.ConnectionId]struct{}
	// interval and clientFor are test seams; zero values select
	// HeldPeerTipRefreshInterval and the connection manager lookup.
	interval  time.Duration
	clientFor func(ouroboros.ConnectionId) heldTipClient
}

// ensureHeldTipRefresher starts the refresher unless it is running. It exits
// by itself once the hold is released.
func (o *Ouroboros) ensureHeldTipRefresher() {
	r := &o.heldTips
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running || r.closed {
		return
	}
	r.running = true
	if r.stop == nil {
		r.stop = make(chan struct{})
	}
	if r.inFlight == nil {
		r.inFlight = make(map[ouroboros.ConnectionId]struct{})
	}
	interval := r.interval
	if interval <= 0 {
		interval = HeldPeerTipRefreshInterval
	}
	stop := r.stop
	r.wg.Go(func() { o.runHeldTipRefresher(interval, stop) })
}

func (o *Ouroboros) runHeldTipRefresher(
	interval time.Duration,
	stop <-chan struct{},
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		r := &o.heldTips
		r.mu.Lock()
		if !o.networkIngestHeld() {
			// Decided under mu, so a held message racing this exit either
			// sees running still set and is covered by the next tick, or
			// starts a new refresher after this one has gone.
			r.running = false
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		if o.chainsyncState == nil {
			continue
		}
		for _, tc := range o.chainsyncState.GetTrackedClients() {
			// PeerTipSlot ignores observers, so their tips are not
			// refreshed.
			if !tc.ObservabilityOnly {
				o.refreshHeldTip(tc.ConnId, stop)
			}
		}
	}
}

// refreshHeldTip asks one client for its peer's tip unless a request for it
// is still outstanding: one queued behind in-flight replies can wait a block
// interval or more, and must not pile up.
func (o *Ouroboros) refreshHeldTip(
	connId ouroboros.ConnectionId,
	stop <-chan struct{},
) {
	r := &o.heldTips
	client := o.heldTipClientFor(connId)
	if client == nil {
		return
	}
	r.mu.Lock()
	if _, busy := r.inFlight[connId]; busy || r.closed {
		r.mu.Unlock()
		return
	}
	r.inFlight[connId] = struct{}{}
	r.mu.Unlock()
	// Not in wg: a request queued behind in-flight replies returns only when
	// they arrive or the connection closes, and Close must not wait for that.
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.inFlight, connId)
			r.mu.Unlock()
		}()
		tip, err := client.GetCurrentTip()
		select {
		case <-stop:
			return
		default:
		}
		if err != nil || tip == nil || o.chainsyncState == nil {
			return
		}
		o.chainsyncState.RecordHeldClientTip(connId, *tip)
	}()
}

func (o *Ouroboros) heldTipClientFor(
	connId ouroboros.ConnectionId,
) heldTipClient {
	if o.heldTips.clientFor != nil {
		return o.heldTips.clientFor(connId)
	}
	if o.connManager == nil {
		return nil
	}
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil || conn.ChainSync() == nil ||
		conn.ChainSync().Client == nil {
		return nil
	}
	return conn.ChainSync().Client
}

// stopHeldTipRefresher stops the refresher for good and waits for its loop.
// A request still blocked in GetCurrentTip records nothing once it returns.
func (o *Ouroboros) stopHeldTipRefresher() {
	r := &o.heldTips
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	if r.stop != nil {
		close(r.stop)
	}
	r.mu.Unlock()
	r.wg.Wait()
}
