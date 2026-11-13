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
	"context"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// outboundChainsyncLocks serializes the start, stop and ChainSync Jumping
// jumps of one connection's client, so a jump never reaches a client that a
// restart is replacing. An entry lives exactly as long as some caller holds or
// waits for it: dropping it while in use would hand a reconnect reusing the
// ConnectionId a second mutex, and keeping it forever would grow the map by
// one entry per connection ever made.
type outboundChainsyncLocks struct {
	mu    sync.Mutex
	locks map[ouroboros.ConnectionId]*outboundChainsyncLock
}

type outboundChainsyncLock struct {
	sync.Mutex
	users int
}

// acquire locks connId's client and returns the function that unlocks it.
func (l *outboundChainsyncLocks) acquire(
	connId ouroboros.ConnectionId,
) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = make(map[ouroboros.ConnectionId]*outboundChainsyncLock)
	}
	entry := l.locks[connId]
	if entry == nil {
		entry = &outboundChainsyncLock{}
		l.locks[connId] = entry
	}
	entry.users++
	l.mu.Unlock()
	entry.Lock()
	return func() {
		entry.Unlock()
		l.mu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(l.locks, connId)
		}
		l.mu.Unlock()
	}
}

// outboundChainsyncLifecycle fences the ChainSync Jumping driver's
// goroutines against Close. They read the ledger to build intersect points
// and can wait out a client drain, and the dependencies they touch are
// closed right after Close on a live restore, so Close cancels every
// in-flight one, waits for it, and admits no new one.
type outboundChainsyncLifecycle struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
	wg     sync.WaitGroup
}

func (l *outboundChainsyncLifecycle) initLocked() {
	if l.ctx == nil {
		l.ctx, l.cancel = context.WithCancel(context.Background())
	}
}

// begin admits one goroutine and returns the context that Close cancels.
// The caller must call l.wg.Done when admitted.
func (l *outboundChainsyncLifecycle) begin() (context.Context, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, false
	}
	l.initLocked()
	l.wg.Add(1)
	return l.ctx, true
}

func (l *outboundChainsyncLifecycle) close() {
	l.mu.Lock()
	l.closed = true
	l.initLocked()
	cancel := l.cancel
	l.mu.Unlock()
	cancel()
	l.wg.Wait()
}

// outboundChainsyncWanted reports whether connId should still run a
// ChainSync client: while its connection is open, an outbound client runs.
func (o *Ouroboros) outboundChainsyncWanted(
	connId ouroboros.ConnectionId,
) bool {
	return o.connManager != nil &&
		o.connManager.GetConnectionById(connId) != nil
}

// startOutboundChainsyncLocked is startOutboundChainsync under the
// connection's outboundChainsyncLocks lock, for a caller that does not hold
// it: ChainSync Jumping restarts a client under that lock, and a start that
// ran beside one could leave the role table and the running client
// disagreeing.
func (o *Ouroboros) startOutboundChainsyncLocked(
	connId ouroboros.ConnectionId,
	conn *ouroboros.Connection,
) bool {
	defer o.outboundChainsyncLocks.acquire(connId)()
	return o.startOutboundChainsync(connId, conn)
}

// startOutboundChainsync registers and starts the client unless it is already
// tracked, or starts it as a ChainSync Jumping jumper. It returns false only
// when the start failed and the connection was closed. conn is the
// connection the start runs on: the failure path must close that connection,
// not whatever holds the id by the time the start returns, because a
// reconnect can reuse the same local/remote address pair and ConnectionId is
// exactly that pair. The caller holds the connection's outboundChainsyncLocks
// lock.
func (o *Ouroboros) startOutboundChainsync(
	connId ouroboros.ConnectionId,
	conn *ouroboros.Connection,
) bool {
	// A tracked client is running. Starting it again would send
	// FindIntersect into a streaming session, which blocks until the
	// protocol times out and then closes the connection.
	if o.chainsyncState.HasClientConnId(connId) {
		return true
	}
	// Registration runs before the tracked client exists, so the
	// direction-aware fallback in shouldPublishChainsyncToLedger cannot see us
	// yet. Outbound keeps its legacy default of eligible when no
	// ChainsyncIngressEligible policy is wired.
	ingressEligible := true
	if o.config.ChainsyncIngressEligible != nil {
		ingressEligible = o.config.ChainsyncIngressEligible(connId)
	}
	role := o.csjAdmit(connId, ingressEligible)
	if role == csjRoleJumper {
		return o.startOutboundJumper(connId, conn)
	}
	if !o.registerTrackedChainsyncClient(
		connId,
		ingressEligible,
		true, // startedAsOutbound
	) {
		if !o.chainsyncState.HasClientConnId(connId) {
			// Not already tracked and TryAdd failed means limit reached
			o.config.Logger.Debug(
				"chainsync client limit reached, skipping eligible admission",
				"connection_id", connId.String(),
			)
			o.csjForget(connId)
		}
		return true
	}
	// A client stopped by a ChainSync Jumping restart is started again here;
	// Start is a no-op on a running one.
	if cs := conn.ChainSync(); cs != nil && cs.Client != nil {
		cs.Client.Start()
	}
	if err := o.chainsyncClientStart(connId); err != nil {
		o.chainsyncState.RemoveClientConnId(connId)
		o.csjForget(connId)
		o.config.Logger.Error(
			"failed to start chainsync client, closing outbound connection",
			"component", "network",
			"connection_id", connId.String(),
			"error", err,
		)
		// Close the connection so peer governance observes the failure and
		// applies its reconnect backoff. Returning with it open strands the
		// peer half-connected: TCP is up and the governor still counts it as
		// connected, but no chainsync client is tracked and nothing retries.
		// A transient fault such as a storage error during the intersect
		// lookup would silently cost the peer for the lifetime of the
		// connection.
		o.closeOutboundConnAfterChainsyncFailure(connId, conn)
		return false
	}
	o.csjStarted(connId, role)
	o.config.Logger.Debug(
		"started chainsync client",
		"connection_id", connId.String(),
		"total_clients", o.chainsyncState.ClientConnCount(),
	)
	return true
}

// startOutboundJumper starts a ChainSync Jumping jumper: the client is
// started but not synced, so it sends nothing until its first jump, and it is
// tracked as an observer because it needs no ingress slot.
func (o *Ouroboros) startOutboundJumper(
	connId ouroboros.ConnectionId,
	conn *ouroboros.Connection,
) bool {
	if !o.chainsyncState.TryAddObservedClientConnIdWithDirection(
		connId,
		true, // startedAsOutbound
	) {
		o.csjForget(connId)
		return true
	}
	if cs := conn.ChainSync(); cs != nil && cs.Client != nil {
		cs.Client.Start()
	}
	if o.peerGov != nil {
		o.peerGov.SetPeerHotByConnId(connId)
	}
	o.csjJumpNew(connId)
	o.config.Logger.Debug(
		"started chainsync client as jumper",
		"connection_id", connId.String(),
	)
	return true
}

// stopOutboundChainsync stops connId's client so that it can be started again
// in another ChainSync Jumping role, and drops its chainsync state, leaving
// the connection open. StopAfterDrain sends MsgDone once the peer has
// returned agency and leaves the client restartable on the same connection.
func (o *Ouroboros) stopOutboundChainsync(
	ctx context.Context,
	connId ouroboros.ConnectionId,
	conn *ouroboros.Connection,
) {
	if !o.chainsyncState.HasClientConnId(connId) {
		return
	}
	if cs := conn.ChainSync(); cs != nil && cs.Client != nil {
		drainCtx, cancel := context.WithTimeout(
			ctx,
			o.chainsyncDemotionTimeout(),
		)
		err := cs.Client.StopAfterDrain(drainCtx)
		cancel()
		if err != nil {
			// The peer still owes replies, so the protocol cannot be
			// returned to idle and restarted on this connection. Close it,
			// as the reference node does when a hot protocol does not stop
			// in time; the governor redials the peer.
			o.config.Logger.Warn(
				"chainsync client did not drain, closing connection",
				"component", "network",
				"connection_id", connId.String(),
				"error", err,
			)
			o.chainsyncState.RemoveClientConnId(connId)
			o.releaseChainsyncPeerState(connId)
			o.csjForget(connId) //nolint:contextcheck // A dynamo rotation outlives this stop.
			if hook := o.chainsyncClientStoppedHook; hook != nil {
				hook(connId)
			}
			_ = conn.Close()
			return
		}
	}
	o.chainsyncState.RemoveClientConnId(connId)
	o.releaseChainsyncPeerState(connId)
	o.csjForget(connId) //nolint:contextcheck // A dynamo rotation outlives this stop.
	if hook := o.chainsyncClientStoppedHook; hook != nil {
		hook(connId)
	}
	o.config.Logger.Debug(
		"stopped chainsync client",
		"component", "network",
		"connection_id", connId.String(),
	)
}

// chainsyncDemotionTimeout bounds how long a peer may take to hand ChainSync
// agency back when its client is stopped. It stays under the ChainSync
// MustReply/CanAwait timers, which a quiet tip can legitimately approach.
func (o *Ouroboros) chainsyncDemotionTimeout() time.Duration {
	if o.demotionTimeout > 0 {
		return o.demotionTimeout
	}
	return defaultChainsyncDemotionTimeout
}

const defaultChainsyncDemotionTimeout = 75 * time.Second

// releaseChainsyncPeerState drops the per-connection ChainSync bookkeeping
// other than the tracked client itself.
func (o *Ouroboros) releaseChainsyncPeerState(connId ouroboros.ConnectionId) {
	o.cancelFutureHeaderResync(connId)
	o.clearDeferredHeaderRecovery(connId)
	o.forgetAcceptedHeaderTip(connId)
	o.chainsyncMutex.Lock()
	delete(o.chainsyncStats, connId)
	o.chainsyncMutex.Unlock()
}
