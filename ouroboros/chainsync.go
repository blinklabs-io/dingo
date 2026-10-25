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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// chainsyncBehindTipRecycleCooldown spaces behind-local-tip recycles
	// across all connections. With no best peer every lagging honest peer
	// matches the stale-cursor shape at once; recycling them together leaves
	// only a peer whose cursor sits at the local tip, which may be a frozen
	// dead-fork peer. Peers still behind retry on their next header.
	chainsyncBehindTipRecycleCooldown = 30 * time.Second

	chainsyncIntersectPointCount = 100

	// chainsyncMaxFindIntersectPoints bounds how many points a peer may
	// send in a single FindIntersect request. Honest clients send a small,
	// bounded bisection list (our own client sends at most
	// chainsyncIntersectPointCount). This generous cap protects against a
	// malicious or buggy peer forcing avoidable intersection lookups against
	// the ledger until the protocol timeout fires, while leaving ample
	// headroom so legitimate sync is never rejected.
	chainsyncMaxFindIntersectPoints = 1000

	// chainsyncFindIntersectBudgetRate bounds the sustained rate (points per
	// second) of database lookup work a single ChainSync peer connection may
	// trigger via repeated FindIntersect requests. Cost is charged per point
	// actually looked up, after deduplication, so this bounds cumulative
	// work across many requests, not just the size of one request. Honest
	// clients issue FindIntersect rarely — on connect, and on resync after a
	// rollback we cannot follow — so this is far above legitimate use.
	chainsyncFindIntersectBudgetRate = 200

	// chainsyncFindIntersectBudgetBurst allows a peer to spend its entire
	// FindIntersect work budget on one immediate request up to
	// chainsyncMaxFindIntersectPoints, matching the point-count cap above so
	// a single in-bounds request is never rejected by the budget alone.
	chainsyncFindIntersectBudgetBurst = float64(chainsyncMaxFindIntersectPoints)

	// chainsyncDivergentPeerCooldown slows peers that repeatedly offer a
	// rollback we cannot safely follow. This prevents full-duplex reconnects
	// from immediately re-entering the same rollback loop.
	chainsyncDivergentPeerCooldown = 2 * time.Minute
)

type chainsyncHeaderAdmissionFunc func(
	context.Context,
	ledger.ChainsyncEvent,
) (bool, error)

type chainsyncScheduleAtFunc func(time.Time, func()) func()

type scheduledChainsyncResync struct {
	onset  time.Time
	reason string
	cancel func()
	fired  bool
}

type deferredHeaderRecoveryState struct {
	baseline ochainsync.Tip
	// point and tip are the newest withheld header and the tip the peer sent
	// with it, which is where the peer's protocol cursor sits. delivered
	// records whether RecordClientDelivery already counted that header.
	point     ocommon.Point
	tip       ochainsync.Tip
	delivered bool
	// resolvable is cleared when the peer rolls back: its protocol cursor no
	// longer sits at point, so applying point must not become its cursor.
	resolvable bool
}

// chainsyncAdmissionContext ends when the peer's chainsync client is asked to
// stop or its connection begins shutdown. It must not use the client's
// DoneChan: that closes only after the protocol loop running this callback
// returns, so a wait in the callback could never observe it.
func chainsyncAdmissionContext(
	cb ochainsync.CallbackContext,
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	var stop <-chan struct{}
	if cb.Client != nil {
		if proto := cb.Client.ProtocolInstance(); proto != nil {
			stop = proto.StopChan()
		}
	}
	connDone := cb.ConnectionDoneChan
	if stop == nil && connDone == nil {
		return ctx, cancel
	}
	go func() {
		select {
		case <-stop:
		case <-connDone:
		case <-ctx.Done():
		}
		cancel()
	}()
	return ctx, cancel
}

func defaultChainsyncScheduleAt(onset time.Time, fn func()) func() {
	timer := time.AfterFunc(time.Until(onset), fn)
	return func() {
		timer.Stop()
	}
}

// scheduleFutureHeaderResync keeps at most one recovery timer per connection.
// The earliest resolvable onset wins: every later dropped header is already
// covered by re-intersecting from the last ledger-accepted point at that time.
func (o *Ouroboros) scheduleFutureHeaderResync(
	connId ouroboros.ConnectionId,
	onset time.Time,
) {
	o.scheduleHeaderResync(
		connId,
		onset,
		event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery,
	)
}

// scheduleHeaderResync is scheduleFutureHeaderResync with the reason the
// recovery event carries. The winning schedule's reason is the one published.
// It reports whether this call armed a timer.
func (o *Ouroboros) scheduleHeaderResync(
	connId ouroboros.ConnectionId,
	onset time.Time,
	reason string,
) bool {
	if o.chainsyncScheduleAt == nil || o.eventBus == nil || onset.IsZero() {
		return false
	}
	if o.connManager != nil && o.connManager.GetConnectionById(connId) == nil {
		return false
	}
	o.futureHeaderResyncMu.Lock()
	if o.futureHeaderResyncClosed {
		o.futureHeaderResyncMu.Unlock()
		return false
	}
	if current := o.futureHeaderResyncs[connId]; current != nil {
		if current.fired {
			o.futureHeaderResyncMu.Unlock()
			return false
		}
		if !onset.Before(current.onset) {
			o.futureHeaderResyncMu.Unlock()
			return false
		}
		current.cancel()
	}
	scheduled := &scheduledChainsyncResync{onset: onset, reason: reason}
	// A timer whose onset is already due may invoke its callback before the
	// scheduler returns. Arm publication only after the marker and cancel
	// function are installed, or that callback can observe no current timer and
	// strand the connection in the withheld state without a recovery event.
	armed := make(chan struct{})
	scheduled.cancel = o.chainsyncScheduleAt(onset, func() {
		go func() {
			<-armed
			o.futureHeaderResyncMu.Lock()
			if o.futureHeaderResyncs[connId] != scheduled || scheduled.fired {
				o.futureHeaderResyncMu.Unlock()
				return
			}
			// Retain the marker until the resync handler stops the old
			// protocol. Headers received between timer onset and Stop must
			// stay withheld or they can advance the remote cursor across the
			// gap being recovered.
			scheduled.fired = true
			o.futureHeaderResyncMu.Unlock()

			ctx := o.futureHeaderResyncCtx
			if ctx == nil {
				return
			}
			o.eventBus.PublishOrderedContext(
				ctx,
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: connId,
						Reason:       scheduled.reason,
					},
				),
			)
		}()
	})
	o.futureHeaderResyncs[connId] = scheduled
	// Connection removal publishes its close event only after removing the
	// connection from connManager. Re-check that authoritative state while the
	// timer marker is installed and still protected: the close handler may have
	// run between the optimistic lookup above and this insertion, when there was
	// not yet a marker for it to cancel.
	if o.connManager != nil && o.connManager.GetConnectionById(connId) == nil {
		scheduled.cancel()
		delete(o.futureHeaderResyncs, connId)
		close(armed)
		o.futureHeaderResyncMu.Unlock()
		return false
	}
	close(armed)
	o.futureHeaderResyncMu.Unlock()
	return true
}

func (o *Ouroboros) futureHeaderResyncPending(
	connId ouroboros.ConnectionId,
) bool {
	o.futureHeaderResyncMu.Lock()
	defer o.futureHeaderResyncMu.Unlock()
	return o.futureHeaderResyncs[connId] != nil
}

// headerResyncDueOrFired reports whether connId has a re-intersection that
// has fired or whose onset is not after now. A timer with a later onset is not
// reported: scheduling an earlier onset replaces it.
func (o *Ouroboros) headerResyncDueOrFired(
	connId ouroboros.ConnectionId,
	now time.Time,
) bool {
	o.futureHeaderResyncMu.Lock()
	defer o.futureHeaderResyncMu.Unlock()
	scheduled := o.futureHeaderResyncs[connId]
	return scheduled != nil &&
		(scheduled.fired || !now.Before(scheduled.onset))
}

func (o *Ouroboros) cancelFutureHeaderResync(
	connId ouroboros.ConnectionId,
) {
	o.futureHeaderResyncMu.Lock()
	if scheduled := o.futureHeaderResyncs[connId]; scheduled != nil {
		scheduled.cancel()
		delete(o.futureHeaderResyncs, connId)
	}
	o.futureHeaderResyncMu.Unlock()
}

func (o *Ouroboros) markDeferredHeaderRecovery(
	connId ouroboros.ConnectionId,
	baseline ochainsync.Tip,
	point ocommon.Point,
	tip ochainsync.Tip,
	delivered bool,
) {
	if o.ledgerState == nil {
		return
	}
	baseline.Point.Hash = append([]byte(nil), baseline.Point.Hash...)
	point.Hash = append([]byte(nil), point.Hash...)
	tip.Point.Hash = append([]byte(nil), tip.Point.Hash...)
	// Recovery normally waits for the next applied block. With no queued
	// header there is none to apply, so that wait never ends (for example
	// once the queue owner disconnects and its headers are cleared). Re-intersect
	// at once instead, but only once per connection and applied tip, so a
	// peer serving a competing fork cannot turn this into a reconnect loop.
	idle := false
	if o.chainsyncLocalChainTips != nil {
		tipHash, headerTipHash := o.chainsyncLocalChainTips()
		// The chain already stores this header's block, delivered by a faster
		// peer, while the ledger is still applying it; that transition is what
		// deferred verification. Its chain update was published when the block
		// was stored, before this header arrived, so waiting for one cannot
		// resolve it, and re-intersecting would replay a block the node holds.
		// The ledger validates the block on the body path and re-syncs every
		// peer itself if that fails, so resolve it now as an applied deferral.
		// Only an undelivered header is resolved here: it is withheld as the
		// peer rolls forward to it, so the peer's cursor is at point. A late
		// ledger rejection of a delivered header can arrive after the peer
		// rolled back or withheld a newer or same-slot fork header, so it
		// keeps the ordinary recovery below.
		if !delivered && len(tipHash) > 0 &&
			bytes.Equal(tipHash, point.Hash) {
			o.resolveDeferredHeaderRecovery(connId, point, tip, delivered)
			return
		}
		idle = len(tipHash) > 0 && bytes.Equal(tipHash, headerTipHash)
	}
	o.deferredHeaderRecoveryMu.Lock()
	if o.deferredHeaderRecovery == nil {
		o.deferredHeaderRecovery = make(
			map[ouroboros.ConnectionId]deferredHeaderRecoveryState,
		)
	}
	recovery := deferredHeaderRecoveryState{
		baseline:   baseline,
		point:      point,
		tip:        tip,
		delivered:  delivered,
		resolvable: true,
	}
	if existing, alreadyDeferred := o.deferredHeaderRecovery[connId]; !alreadyDeferred {
		o.deferredHeaderRecovery[connId] = recovery
	} else {
		// Keep the first baseline, which still triggers recovery, but follow
		// the peer's cursor to its newest withheld header: applying it covers
		// every earlier one. A ledger rejection callback can report an older
		// header after a newer one was withheld, so never move point back
		// unless a rollback already moved the peer's cursor.
		baseline = existing.baseline
		if !existing.resolvable || point.Slot > existing.point.Slot {
			recovery.baseline = existing.baseline
			o.deferredHeaderRecovery[connId] = recovery
		}
	}
	o.deferredHeaderRecoveryMu.Unlock()
	current := o.ledgerState.Tip().Point
	if current.Slot != baseline.Point.Slot ||
		!bytes.Equal(current.Hash, baseline.Point.Hash) {
		// The ledger may have advanced during ancestry lookup, before this
		// connection was registered for the corresponding ChainUpdate event.
		o.retryDeferredHeadersAfterLedgerAdvance(event.NewEvent(
			chain.ChainUpdateEventType,
			chain.ChainBlockEvent{Point: current},
		))
		return
	}
	retryNow := false
	if idle {
		o.deferredHeaderRecoveryMu.Lock()
		retried, seen := o.deferredHeaderIdleRetry[connId]
		if !seen || !bytes.Equal(retried, baseline.Point.Hash) {
			if o.deferredHeaderIdleRetry == nil {
				o.deferredHeaderIdleRetry = make(
					map[ouroboros.ConnectionId][]byte,
				)
			}
			o.deferredHeaderIdleRetry[connId] = baseline.Point.Hash
			retryNow = true
		}
		o.deferredHeaderRecoveryMu.Unlock()
	}
	if retryNow {
		o.scheduleHeaderResync(
			connId,
			time.Now(),
			event.ChainsyncResyncReasonHeaderValidationRecovery,
		)
	}
}

// invalidateDeferredHeaderResolution keeps a connection's deferred-header
// recovery but stops a later application of its withheld header from becoming
// the connection's cursor, since a rollback moved the peer's protocol cursor
// off it. Ledger advance then re-intersects the peer as before.
// resolveDeferredHeaderRecovery keeps a peer whose withheld header the chain
// already holds and aligns the tracked cursor with the peer's, as when the
// deferred header is applied. The peer has just rolled forward to the held
// header, so any recovery pending for headers it delivered earlier no longer
// describes its chain and is dropped.
func (o *Ouroboros) resolveDeferredHeaderRecovery(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	tip ochainsync.Tip,
	delivered bool,
) {
	o.deferredHeaderRecoveryMu.Lock()
	delete(o.deferredHeaderRecovery, connId)
	delete(o.deferredHeaderIdleRetry, connId)
	o.deferredHeaderRecoveryMu.Unlock()
	if o.chainsyncState != nil {
		o.chainsyncState.AdmitDeferredClientTip(connId, point, tip, !delivered)
	}
}

func (o *Ouroboros) invalidateDeferredHeaderResolution(
	connId ouroboros.ConnectionId,
) {
	o.deferredHeaderRecoveryMu.Lock()
	if state, ok := o.deferredHeaderRecovery[connId]; ok {
		state.resolvable = false
		o.deferredHeaderRecovery[connId] = state
	}
	o.deferredHeaderRecoveryMu.Unlock()
}

func (o *Ouroboros) clearDeferredHeaderRecovery(
	connId ouroboros.ConnectionId,
) {
	o.deferredHeaderRecoveryMu.Lock()
	delete(o.deferredHeaderRecovery, connId)
	delete(o.deferredHeaderIdleRetry, connId)
	o.deferredHeaderRecoveryMu.Unlock()
}

func (o *Ouroboros) recordAcceptedHeaderTip(
	connId ouroboros.ConnectionId,
	hash []byte,
) {
	o.acceptedHeaderMu.Lock()
	if o.acceptedHeaderTips == nil {
		o.acceptedHeaderTips = make(map[ouroboros.ConnectionId][]byte)
	}
	o.acceptedHeaderTips[connId] = hash
	o.acceptedHeaderMu.Unlock()
}

func (o *Ouroboros) forgetAcceptedHeaderTip(connId ouroboros.ConnectionId) {
	o.acceptedHeaderMu.Lock()
	delete(o.acceptedHeaderTips, connId)
	o.acceptedHeaderMu.Unlock()
}

// deferredHeaderExtendsLocalChain reports whether a header whose producer
// authorization is deferred builds directly on the local chain: on the block
// tip, the header-queue tip, or this connection's accepted header tip, the
// last header that itself extended the local chain and that chain selection
// accepted. Such a header cannot be a competing fork, so it may reach the
// block-fetch queue, where the body path verifies it before application.
// Withholding it instead deadlocks whenever the deferral clears only by
// applying the header's own block, as at the Byron->Shelley boundary, where
// the first Shelley epoch's state does not exist until a Shelley block is
// applied. The ledger remains the authority: a header that no longer fits
// its queue there cannot clear the queue or roll back canonical state.
func (o *Ouroboros) deferredHeaderExtendsLocalChain(
	connId ouroboros.ConnectionId,
	header gledger.BlockHeader,
) bool {
	prevHash := header.PrevHash().Bytes()
	if o.chainsyncLocalChainTips != nil {
		tipHash, headerTipHash := o.chainsyncLocalChainTips()
		if (len(tipHash) > 0 && bytes.Equal(prevHash, tipHash)) ||
			(len(headerTipHash) > 0 && bytes.Equal(prevHash, headerTipHash)) {
			return true
		}
	}
	o.acceptedHeaderMu.Lock()
	accepted := o.acceptedHeaderTips[connId]
	o.acceptedHeaderMu.Unlock()
	return len(accepted) > 0 && bytes.Equal(prevHash, accepted)
}

func (o *Ouroboros) subscribeDeferredHeaderRecovery() {
	if o.eventBus == nil || o.ledgerState == nil {
		return
	}
	o.subscribeTracked(
		chain.ChainUpdateEventType,
		o.retryDeferredHeadersAfterLedgerAdvance,
	)
}

// retryDeferredHeadersAfterLedgerAdvance re-intersects peers whose headers
// could not be verified against the previous applied tip. The peer protocol
// replays the retained chain suffix in parent-first order; each redelivered
// header must pass the ordinary verifier before it reaches the selector or
// ledger. No deferred header itself is admitted to the ledger to make this
// progress.
func (o *Ouroboros) retryDeferredHeadersAfterLedgerAdvance(evt event.Event) {
	applied, ok := evt.Data.(chain.ChainBlockEvent)
	if !ok {
		return
	}
	o.deferredHeaderRecoveryMu.Lock()
	type recovery struct {
		conn  ouroboros.ConnectionId
		state deferredHeaderRecoveryState
	}
	pending := make([]recovery, 0, len(o.deferredHeaderRecovery))
	resolved := make([]recovery, 0, len(o.deferredHeaderRecovery))
	for conn, state := range o.deferredHeaderRecovery {
		if applied.Point.Slot == state.baseline.Point.Slot &&
			bytes.Equal(applied.Point.Hash, state.baseline.Point.Hash) {
			continue
		}
		candidate := recovery{conn: conn, state: state}
		if state.resolvable &&
			applied.Point.Slot == state.point.Slot &&
			bytes.Equal(applied.Point.Hash, state.point.Hash) {
			resolved = append(resolved, candidate)
			delete(o.deferredHeaderRecovery, conn)
			delete(o.deferredHeaderIdleRetry, conn)
			continue
		}
		pending = append(pending, candidate)
		delete(o.deferredHeaderRecovery, conn)
	}
	o.deferredHeaderRecoveryMu.Unlock()

	// Another peer can deliver and apply the same header while this peer's
	// producer authorization is waiting on ledger state. Application is the
	// authoritative validation result, so align the local cursor with the
	// peer's already-advanced protocol cursor instead of reconnecting it to
	// replay a block the node has just accepted. A header withheld before
	// verification never reached delivery accounting, so count it now.
	for _, candidate := range resolved {
		if o.chainsyncState != nil {
			o.chainsyncState.AdmitDeferredClientTip(
				candidate.conn,
				candidate.state.point,
				candidate.state.tip,
				!candidate.state.delivered,
			)
		}
	}

	for _, candidate := range pending {
		if o.scheduleHeaderResync(
			candidate.conn,
			time.Now(),
			event.ChainsyncResyncReasonHeaderValidationRecovery,
		) {
			continue
		}
		if o.futureHeaderResyncPending(candidate.conn) {
			continue
		}
		o.deferredHeaderRecoveryMu.Lock()
		connectionClosed := o.connManager != nil &&
			o.connManager.GetConnectionById(candidate.conn) == nil
		if !connectionClosed {
			if _, alreadyDeferred := o.deferredHeaderRecovery[candidate.conn]; !alreadyDeferred {
				o.deferredHeaderRecovery[candidate.conn] = candidate.state
			}
		}
		o.deferredHeaderRecoveryMu.Unlock()
	}
}

func (o *Ouroboros) stopFutureHeaderResyncs() {
	if o.futureHeaderResyncCancel != nil {
		o.futureHeaderResyncCancel()
	}
	o.futureHeaderResyncMu.Lock()
	o.futureHeaderResyncClosed = true
	for connId, scheduled := range o.futureHeaderResyncs {
		scheduled.cancel()
		delete(o.futureHeaderResyncs, connId)
	}
	o.futureHeaderResyncMu.Unlock()
}

func effectiveChainsyncBlockTimeout(timeout time.Duration) time.Duration {
	if timeout < ochainsync.MustReplyTimeoutMax {
		return ochainsync.MustReplyTimeoutMax
	}
	return timeout
}

// includeClient is true for node-to-node connections, which run a chainsync
// client as well as the server; node-to-client connections run only the
// server and get its local-client behaviour (see chainsyncServerConnOpts).
func (o *Ouroboros) chainsyncConnectionConfigOption(
	ctx context.Context,
	includeClient bool,
	trackActivity ...bool,
) ouroboros.ConnectionOptionFunc {
	return func(c *ouroboros.Connection) {
		limiter := newChainsyncFindIntersectRateLimiter(
			chainsyncFindIntersectBudgetRate,
			chainsyncFindIntersectBudgetBurst,
		)
		opts := o.chainsyncServerConnOpts(
			ctx,
			limiter,
			!includeClient,
			activityTrackingEnabled(trackActivity),
		)
		if includeClient {
			opts = slices.Concat(o.chainsyncClientConnOpts(), opts)
		}
		ouroboros.WithChainSyncConfig(
			ochainsync.NewConfig(opts...),
		)(c)
	}
}

// waitForLedger is set for node-to-client connections: see
// chainsyncServerRequestNext.
func (o *Ouroboros) chainsyncServerConnOpts(
	ctx context.Context,
	limiter *chainsyncFindIntersectRateLimiter,
	waitForLedger bool,
	trackActivity ...bool,
) []ochainsync.ChainSyncOptionFunc {
	activityEnabled := activityTrackingEnabled(trackActivity)
	requestNext := o.chainsyncServerRequestNextPeer
	if waitForLedger {
		requestNext = o.chainsyncServerRequestNextLocal
	}
	if activityEnabled {
		requestNext = o.chainsyncServerRequestNextTrustedLocal
	}
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithFindIntersectFunc(
			o.instrumentChainsyncFindIntersect(
				func(
					cbCtx ochainsync.CallbackContext,
					points []ocommon.Point,
				) (ocommon.Point, ochainsync.Tip, error) {
					return o.chainsyncServerFindIntersect(
						ctx,
						limiter,
						cbCtx,
						points,
					)
				},
				activityEnabled,
			),
		),
		ochainsync.WithRequestNextFunc(
			o.instrumentChainsyncRequestNext(requestNext),
		),
		// Increase intersect timeout from the 10s default. Downstream
		// peers may send FindIntersect with many points during initial
		// sync, and processing can be slow under load.
		ochainsync.WithIntersectTimeout(30 * time.Second),
		// Set idle timeout to 1 hour. The spec default (3673s per
		// Table 3.8) is similar, but we set it explicitly to keep
		// server connections alive during periods of low block
		// production (e.g. DevNets with low activeSlotsCoeff).
		ochainsync.WithIdleTimeout(1 * time.Hour),
		ochainsync.WithBlockTimeout(o.config.ChainsyncBlockTimeout),
	}
}

func (o *Ouroboros) chainsyncClientConnOpts() []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithAwaitReplyFunc(o.chainsyncClientAwaitReply),
		ochainsync.WithRollForwardRawFunc(
			o.instrumentChainsyncRollForwardRaw(
				o.chainsyncClientRollForwardRaw,
			),
		),
		ochainsync.WithRollBackwardFunc(
			o.instrumentChainsyncRollBackward(o.chainsyncClientRollBackward),
		),
		// Keep the request window large enough to cover network latency. The
		// ledger's header-capacity gate bounds admission and verification work.
		ochainsync.WithPipelineLimit(ochainsync.DefaultPipelineLimit),
		ochainsync.WithRecvQueueSize(ochainsync.DefaultRecvQueueSize),
		// Increase the intersect timeout from the 5s default. The
		// upstream peer may need time to process FindIntersect when
		// under load (e.g. fast DevNet block production or initial
		// sync from genesis).
		ochainsync.WithIntersectTimeout(30 * time.Second),
		ochainsync.WithBlockTimeout(o.config.ChainsyncBlockTimeout),
	}
}

// chainsyncClientAwaitReply publishes the protocol's explicit idle proof after
// gouroboros has fenced every preceding pipelined chain update.
func (o *Ouroboros) chainsyncClientAwaitReply(
	ctx ochainsync.CallbackContext,
) error {
	if o.chainsyncState == nil {
		return nil
	}
	client := o.chainsyncState.GetTrackedClient(ctx.ConnectionId)
	if client == nil {
		return nil
	}
	o.chainsyncState.MarkClientSynced(ctx.ConnectionId)
	// While held the cursor is not the point the peer awaits at, so it must
	// not reach chain selection or the ledger.
	if client.ObservabilityOnly || o.networkIngestHeld() ||
		!o.shouldPublishChainsyncToLedger(ctx.ConnectionId) {
		return nil
	}
	if o.config.ChainsyncObserveAwaitReply != nil {
		o.config.ChainsyncObserveAwaitReply(ctx.ConnectionId, client.Cursor)
	}
	if o.eventBus == nil {
		return nil
	}
	o.eventBus.Publish(
		ledger.ChainsyncAwaitReplyEventType,
		event.NewEvent(
			ledger.ChainsyncAwaitReplyEventType,
			ledger.ChainsyncAwaitReplyEvent{
				ConnectionId: ctx.ConnectionId,
			},
		),
	)
	return nil
}

func normalizeIntersectPoints(points []ocommon.Point) []ocommon.Point {
	if len(points) == 0 {
		return nil
	}
	result := make([]ocommon.Point, 0, len(points))
	seen := make(map[string]struct{}, len(points))
	for _, point := range points {
		key := fmt.Sprintf("%d:%x", point.Slot, point.Hash)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, point)
	}
	return result
}

func isOriginPoint(point ocommon.Point) bool {
	return point.Slot == 0 && len(point.Hash) == 0
}

// originOnlyIntersectWarnInterval throttles the "intersect points collapsed to
// origin" warning. The condition that produces it (an in-flight ledger
// rollback) is re-evaluated on every reconnect, and peer governance reconnects
// every second or so, so an unthrottled warning would emit hundreds of lines
// for a single incident.
const originOnlyIntersectWarnInterval = 30 * time.Second

// intersectPointsHaveRealPoint reports whether points contains a point other
// than origin.
//
// It is the sole definition of the condition the rollback anchor exists to
// rescue, shared by finalizeChainsyncIntersectPoints and by the gate on the
// anchor lookup in buildDefaultChainsyncIntersectPoints. Those two must agree:
// if the gate were ever narrower than the rescue, the lookup would be skipped
// for a list the rescue would have acted on, and the node would send the
// origin-only request this whole path exists to prevent.
func intersectPointsHaveRealPoint(points []ocommon.Point) bool {
	for _, point := range points {
		if !isOriginPoint(point) {
			return true
		}
	}
	return false
}

// finalizeChainsyncIntersectPoints appends origin as the last-resort intersect
// point and refuses to offer origin *alone* while the local chain holds a
// non-origin tip.
//
// Origin is always appended so FindIntersect still succeeds against a peer that
// follows a divergent fork (e.g. a multi-producer DevNet) -- without it such
// peers have no common point at all. But an origin-ONLY list is a different
// request: it asks the peer to replay the chain from genesis. A synced node
// cannot accept that reply. Genesis-era headers fail leader-eligibility
// verification (the genesis-era producer has no entry in the epoch-0 stake
// snapshot), which publishes ConnectionRecycleRequestedEvent and tears the
// connection down milliseconds after it opened, so the node makes no chainsync
// progress with any peer for as long as the condition lasts.
//
// The condition does occur on a healthy node: while a rollback's metadata
// truncation is in flight the ledger tip names a block the chain rewind has
// already deleted, and the ledger can return no points. In that case the
// ledger can still name a point it has applied, so seed the list with it and
// let origin stay the fallback it was meant to be.
//
// rollbackAnchor MUST come from LedgerState.RollbackWindowIntersectAnchor,
// never from the primary chain tip directly. An empty point list can also mean
// the primary chain is ahead of the ledger on a fork that does not descend
// from the applied ledger tip; seeding from that raw chain tip would advertise
// unapplied fork state and break the primary-chain ancestor invariant.
// The ledger returns hasRollbackAnchor=false for that case, so it stays
// origin-only exactly as before.
//
// Returns the finalized points and whether an origin-only list had to be
// rescued, which the caller logs (throttled).
func finalizeChainsyncIntersectPoints(
	intersectPoints []ocommon.Point,
	rollbackAnchor ocommon.Point,
	hasRollbackAnchor bool,
) ([]ocommon.Point, bool) {
	rescued := false
	if !intersectPointsHaveRealPoint(intersectPoints) &&
		hasRollbackAnchor && !isOriginPoint(rollbackAnchor) {
		intersectPoints = normalizeIntersectPoints(
			append(
				[]ocommon.Point{rollbackAnchor},
				intersectPoints...,
			),
		)
		rescued = true
	}
	// Always include origin as the last intersect point. This
	// ensures FindIntersect succeeds even when the peer follows
	// a different fork (e.g. multi-producer DevNet). Without
	// origin, peers on divergent chains have no common point.
	if len(intersectPoints) == 0 ||
		!isOriginPoint(intersectPoints[len(intersectPoints)-1]) {
		intersectPoints = append(intersectPoints, ocommon.NewPointOrigin())
	}
	return intersectPoints, rescued
}

// warnOriginOnlyIntersectRescued reports, at most once per
// originOnlyIntersectWarnInterval, that we were about to ask a peer to replay
// from genesis on a node that is not at genesis.
func (o *Ouroboros) warnOriginOnlyIntersectRescued(
	connId ouroboros.ConnectionId,
	rollbackAnchor ocommon.Point,
) {
	now := time.Now()
	last := o.lastOriginOnlyIntersectWarn.Load()
	if last != 0 &&
		now.Sub(time.Unix(0, last)) < originOnlyIntersectWarnInterval {
		return
	}
	if !o.lastOriginOnlyIntersectWarn.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	o.config.Logger.Warn(
		"chainsync intersect points collapsed to origin on a non-origin chain, using rollback anchor instead",
		"component",
		"ouroboros",
		"connection_id",
		connId.String(),
		"anchor_slot",
		rollbackAnchor.Slot,
		"anchor_hash",
		hex.EncodeToString(rollbackAnchor.Hash),
		"reason",
		"ledger returned no intersect points (rollback truncation in flight)",
	)
}

// chainsyncResyncDeniesPeer lists the re-sync reasons that also put the peer
// in peer governance's deny list for chainsyncDivergentPeerCooldown.
//
// A peer that is merely behind on our own chain is deliberately absent: the
// ledger now classifies it before any of these reasons is published (see
// LedgerState.chainsyncPeerBehindOnOurChain) and publishes no re-sync at all,
// so the connection is kept and the peer resumes on its own once it catches
// up. Denying such a peer is what turns a lagging upstream into an outage on a
// node whose valency is one. Do not add a behind-peer reason here.
//
// ChainsyncResyncReasonPeerTipBehindMithril stays, even though that peer is
// also just behind: unlike the case above we still close its connection,
// because we cannot follow it below the trust anchor, and without the cooldown
// it is redialed and rejected again within a second, forever.
func chainsyncResyncDeniesPeer(reason string) bool {
	switch reason {
	case event.ChainsyncResyncReasonRollbackExceedsK,
		event.ChainsyncResyncReasonForkResolutionExceedsK,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		event.ChainsyncResyncReasonDeferredHeaderValidationFailure,
		event.ChainsyncResyncReasonPeerTipBehindMithril:
		return true
	default:
		return false
	}
}

func (o *Ouroboros) denyDivergentChainsyncPeer(
	connId ouroboros.ConnectionId,
	reason string,
) {
	if o.peerGov == nil ||
		connId.RemoteAddr == nil ||
		!chainsyncResyncDeniesPeer(reason) {
		return
	}
	address := connId.RemoteAddr.String()
	o.peerGov.DenyPeer(address, chainsyncDivergentPeerCooldown)
	o.config.Logger.Warn(
		"temporarily denying chainsync peer whose chain we cannot follow",
		"connection_id", connId.String(),
		"address", address,
		"reason", reason,
		"duration", chainsyncDivergentPeerCooldown,
	)
}

func (o *Ouroboros) buildDefaultChainsyncIntersectPoints(
	ctx context.Context,
	connId ouroboros.ConnectionId,
) ([]ocommon.Point, error) {
	if o.ledgerState == nil {
		return nil, errors.New("ledger state not available")
	}
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil {
		return nil, fmt.Errorf(
			"failed to lookup connection ID: %s",
			connId.String(),
		)
	}
	if conn.ChainSync() == nil || conn.ChainSync().Client == nil {
		return nil, fmt.Errorf(
			"ChainSync client not available on connection: %s",
			connId.String(),
		)
	}
	intersectPoints, err := o.ledgerState.IntersectPoints(
		ctx,
		chainsyncIntersectPointCount,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"LedgerState.IntersectPoints failed: %w",
			err,
		)
	}
	// Determine start point if we have no stored chain points
	if len(intersectPoints) == 0 {
		if o.config.IntersectTip {
			// Start initial chainsync from current chain tip
			tip, err := conn.ChainSync().Client.GetCurrentTip()
			if err != nil {
				return nil, fmt.Errorf(
					"ChainSync.Client.GetCurrentTip failed: %w",
					err,
				)
			}
			intersectPoints = append(intersectPoints, tip.Point)
		} else if len(o.config.IntersectPoints) > 0 {
			// Start initial chainsync at specific point(s)
			intersectPoints = append(
				intersectPoints,
				o.config.IntersectPoints...,
			)
		}
	}
	intersectPoints = normalizeIntersectPoints(intersectPoints)
	// The anchor is consulted only to rescue a list with no real point, so
	// look it up only in that case. Unconditionally is both wasted work on
	// every chainsync client start and a way to lose a healthy peer: the
	// common path is served from the in-memory chain without touching the
	// database, while the anchor lookup always reads it, so a transient
	// storage fault there would fail a start that had good points to offer
	// and close the connection under it.
	var (
		rollbackAnchor    ocommon.Point
		hasRollbackAnchor bool
	)
	if !intersectPointsHaveRealPoint(intersectPoints) {
		rollbackAnchor, hasRollbackAnchor, err = o.ledgerState.RollbackWindowIntersectAnchor(
			ctx,
		)
		if err != nil {
			// Surfaced like any other intersect-point failure above: a
			// storage fault must fail the chainsync start so the caller
			// retries, not be downgraded into "no anchor" and sent to the
			// peer as origin-only. Reached only when the list is already
			// origin-only, which is exactly when the answer decides what
			// goes on the wire.
			return nil, fmt.Errorf(
				"LedgerState.RollbackWindowIntersectAnchor failed: %w",
				err,
			)
		}
	}
	intersectPoints, rescued := finalizeChainsyncIntersectPoints(
		intersectPoints,
		rollbackAnchor,
		hasRollbackAnchor,
	)
	if rescued {
		o.warnOriginOnlyIntersectRescued(connId, rollbackAnchor)
	}
	return intersectPoints, nil
}

func (o *Ouroboros) syncChainsyncClient(
	connId ouroboros.ConnectionId,
	intersectPoints []ocommon.Point,
) error {
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if conn.ChainSync() == nil || conn.ChainSync().Client == nil {
		return fmt.Errorf(
			"ChainSync client not available on connection: %s",
			connId.String(),
		)
	}
	intersectPoints = normalizeIntersectPoints(intersectPoints)
	if len(intersectPoints) == 0 {
		intersectPoints = []ocommon.Point{ocommon.NewPointOrigin()}
	}
	if o.peerGov != nil {
		o.peerGov.SetPeerHotByConnId(connId)
	}
	return conn.ChainSync().Client.Sync(intersectPoints)
}

func (o *Ouroboros) chainsyncClientStart(connId ouroboros.ConnectionId) error {
	intersectPoints, err := o.buildDefaultChainsyncIntersectPoints(
		context.Background(),
		connId,
	)
	if err != nil {
		return fmt.Errorf(
			"build default chainsync intersect points for start: %w",
			err,
		)
	}
	if err := o.syncChainsyncClient(connId, intersectPoints); err != nil {
		return fmt.Errorf("sync chainsync client: %w", err)
	}
	return nil
}

func (o *Ouroboros) chainsyncServerFindIntersect(
	ctx context.Context,
	limiter *chainsyncFindIntersectRateLimiter,
	cbCtx ochainsync.CallbackContext,
	points []ocommon.Point,
) (ocommon.Point, ochainsync.Tip, error) {
	var retPoint ocommon.Point
	o.config.Logger.Debug(
		"chainsync server: FindIntersect callback entered",
		"component", "ouroboros",
		"connection_id", cbCtx.ConnectionId.String(),
		"num_points", len(points),
	)
	tip := o.ledgerState.Tip()
	o.config.Logger.Debug(
		"chainsync server: got tip",
		"component", "ouroboros",
		"tip_slot", tip.Point.Slot,
	)
	// Reject oversized point lists before performing any intersection
	// lookup. Without this cap a peer could send an arbitrarily large list
	// and force avoidable CPU/database work until the protocol timeout. We
	// respond with IntersectNotFound (via ErrIntersectNotFound) rather than
	// tearing down the connection, which keeps the failure cheap and avoids
	// reconnect churn.
	if len(points) > chainsyncMaxFindIntersectPoints {
		o.config.Logger.Warn(
			"chainsync server: rejecting FindIntersect with too many points",
			"component", "ouroboros",
			"connection_id", cbCtx.ConnectionId.String(),
			"num_points", len(points),
			"max_points", chainsyncMaxFindIntersectPoints,
		)
		return retPoint, tip, ochainsync.ErrIntersectNotFound
	}
	// Deduplicate before charging the work budget or performing any lookup.
	// GetIntersectPoint's running-best-match scan only skips a point once a
	// higher-or-equal-slot match has already been found, so a peer resending
	// the same point many times (or an equal-slot point with a different
	// hash) would otherwise force one redundant database lookup per repeat.
	// The intersection result is independent of point order (the highest
	// matching slot always wins), so deduplicating here changes no outcome.
	points = normalizeIntersectPoints(points)
	if !limiter.Allow(len(points)) {
		o.config.Logger.Warn(
			"chainsync server: rejecting FindIntersect over per-connection work budget",
			"component",
			"ouroboros",
			"connection_id",
			cbCtx.ConnectionId.String(),
			"num_points",
			len(points),
		)
		return retPoint, tip, ochainsync.ErrIntersectNotFound
	}
	intersectPoint, err := o.ledgerState.GetIntersectPoint(ctx, points)
	if err != nil {
		o.config.Logger.Error(
			"chainsync server: GetIntersectPoint error",
			"component", "ouroboros",
			"error", err,
		)
		return retPoint, tip, fmt.Errorf("get intersect point: %w", err)
	}
	o.config.Logger.Debug(
		"chainsync server: GetIntersectPoint done",
		"component", "ouroboros",
		"found", intersectPoint != nil,
	)
	if intersectPoint == nil {
		return retPoint, tip, ochainsync.ErrIntersectNotFound
	}
	// Add our client to the chainsync state
	_, err = o.chainsyncState.AddClient(
		cbCtx.ConnectionId,
		*intersectPoint,
		cbCtx.Server,
	)
	if err != nil {
		return retPoint, tip, fmt.Errorf(
			"add chainsync client for connection %s: %w",
			cbCtx.ConnectionId.String(),
			err,
		)
	}
	retPoint = *intersectPoint
	// Count only an intersection actually served: rejected or unmatched
	// requests must not keep an otherwise idle inbound peer from pruning.
	o.recordServedActivity(cbCtx.ConnectionId)
	return retPoint, tip, nil
}

// refreshTip returns the current ledger tip, ensuring it is never behind
// the block being sent. The chain iterator delivers blocks before the
// ledger processes them, so the tip can be stale. A tip slot behind the
// block slot is a protocol violation that causes peers to disconnect.
func (o *Ouroboros) refreshTip(next *chain.ChainIteratorResult) ochainsync.Tip {
	tip := o.ledgerState.Tip()
	if !next.Rollback && next.Point.Slot > tip.Point.Slot {
		tip = ochainsync.Tip{
			Point:       next.Point,
			BlockNumber: next.Block.Number,
		}
	}
	return tip
}

// chainsyncServerAheadOfLedger reports whether next rolls forward to a block
// the ledger has not applied yet. The chain iterator runs ahead of the ledger
// during sync, by seconds and more across an epoch boundary.
func (o *Ouroboros) chainsyncServerAheadOfLedger(
	next *chain.ChainIteratorResult,
) bool {
	return next != nil && !next.Rollback &&
		next.Point.Slot > o.ledgerState.Tip().Point.Slot
}

func (o *Ouroboros) chainsyncServerRequestNextPeer(
	ctx ochainsync.CallbackContext,
) error {
	return o.chainsyncServerRequestNext(ctx, false, false)
}

func (o *Ouroboros) chainsyncServerRequestNextLocal(
	ctx ochainsync.CallbackContext,
) error {
	return o.chainsyncServerRequestNext(ctx, true, false)
}

func (o *Ouroboros) chainsyncServerRequestNextTrustedLocal(
	ctx ochainsync.CallbackContext,
) error {
	return o.chainsyncServerRequestNext(ctx, true, true)
}

// chainsyncServerRequestNext serves the next chain event to a downstream
// client.
//
// With waitForLedger set (node-to-client connections) a roll-forward is held
// until the ledger has applied the block. A local client follows chainsync to
// read ledger state at the points it receives, and cardano-node serves its
// local clients only blocks on its selected chain, which it adopts after
// validating them against the ledger, so every point it delivers can be
// acquired by LocalStateQuery. Serving a block the ledger has not reached yet
// makes Acquire of that point fail with AcquireFailurePointNotOnChain.
// Peers (node-to-node) are still served ahead of the ledger.
func (o *Ouroboros) chainsyncServerRequestNext(
	ctx ochainsync.CallbackContext,
	waitForLedger bool,
	trackActivity ...bool,
) error {
	endActivity := o.beginTrustedNtCActivity(
		activityTrackingEnabled(trackActivity),
		ctx.ConnectionId,
		ochainsync.ProtocolIdNtC,
	)
	activityContinues := false
	defer func() {
		if !activityContinues {
			endActivity()
		}
	}()
	o.recordServedActivity(ctx.ConnectionId)
	// Create/retrieve chainsync state for connection
	tip := o.ledgerState.Tip()
	clientState, err := o.chainsyncState.AddClient(
		ctx.ConnectionId,
		tip.Point,
		ctx.Server,
	)
	if err != nil {
		return fmt.Errorf(
			"add chainsync client for connection %s: %w",
			ctx.ConnectionId.String(),
			err,
		)
	}
	// LookupClient snapshots this same field under clientState's own lock,
	// scoped to this one connection rather than the whole chainsync State,
	// so a slow RollBackward send here cannot stall AddClient/LookupClient/
	// RemoveClient for other connections. Held through the send and state
	// transition so observers cannot read the pending flag after
	// RollBackward is visible but before it is cleared.
	clientState.LockRollbackState()
	if clientState.NeedsInitialRollback {
		o.config.Logger.Debug(
			"chainsync server: initial rollback",
			"connection_id", ctx.ConnectionId.String(),
			"cursor_slot", clientState.Cursor.Slot,
		)
		err := ctx.Server.RollBackward(
			clientState.Cursor,
			tip,
		)
		if err != nil {
			clientState.UnlockRollbackState()
			return fmt.Errorf("chainsync server: initial rollback: %w", err)
		}
		clientState.NeedsInitialRollback = false
		clientState.UnlockRollbackState()
		return nil
	}
	clientState.UnlockRollbackState()
	// Check for available block
	next, err := clientState.ChainIter.Next(false)
	if err != nil {
		if !errors.Is(err, chain.ErrIteratorChainTip) {
			return fmt.Errorf("chainsync server: next chain event: %w", err)
		}
	}
	// A roll-forward the ledger has not reached is held behind AwaitReply
	// rather than waited for here: the client's CanAwait timeout is 10s and
	// the ledger can lag longer than that.
	var pending *chain.ChainIteratorResult
	if waitForLedger && o.chainsyncServerAheadOfLedger(next) {
		pending = next
		next = nil
	}
	if next != nil {
		tip = o.refreshTip(next)
		if next.Rollback {
			err = ctx.Server.RollBackward(
				next.Point,
				tip,
			)
		} else {
			blockCbor, blockErr := o.chainsyncServerBlockCbor(ctx, next.Block)
			if blockErr != nil {
				// Do not RollForward an incomplete CertRB; return the error so
				// the connection is torn down and the client retries from its
				// last point once the endorser closure is available.
				return blockErr
			}
			err = ctx.Server.RollForward(
				next.Block.Type,
				blockCbor,
				tip,
			)
		}
		if err != nil {
			return fmt.Errorf("chainsync server: send chain event: %w", err)
		}
		return nil
	}
	// Send AwaitReply
	o.config.Logger.Debug(
		"chainsync server: sending AwaitReply",
		"connection_id", ctx.ConnectionId.String(),
		"tip_slot", tip.Point.Slot,
	)
	if err := ctx.Server.AwaitReply(); err != nil {
		return fmt.Errorf("chainsync server: send AwaitReply: %w", err)
	}
	// Wait for next block and send
	rawConn, connDone, closeErr := o.connManager.GetConnectionWithDoneAndError(
		ctx.ConnectionId,
	)
	if rawConn == nil {
		return fmt.Errorf("connection %s not found", ctx.ConnectionId.String())
	}
	activityContinues = true
	go func() {
		defer endActivity()
		o.chainsyncServerAwaitNext(
			ctx,
			connWithDone{conn: rawConn, done: connDone, closeErr: closeErr},
			clientState,
			pending,
			waitForLedger,
		)
	}()
	return nil
}

// chainsyncServerAwaitNext is the post-AwaitReply waiter. It runs as its own
// goroutine once the server has parked the peer in MustReply, and either serves
// the block the iterator eventually yields or drops the transport. pending is
// a result already taken from the iterator and held for the ledger; when it is
// nil the waiter reads the iterator. With waitForLedger set a roll-forward is
// served only once the ledger has applied it (see chainsyncServerRequestNext).
func (o *Ouroboros) chainsyncServerAwaitNext(
	ctx ochainsync.CallbackContext,
	conn managedConnection,
	clientState *chainsync.ChainsyncClientState,
	pending *chain.ChainIteratorResult,
	waitForLedger bool,
) {
	next := pending
	var nextErr error
	if next == nil {
		// Wait for next block in a separate goroutine so we can
		// also monitor the connection for errors. This avoids
		// leaking the monitor goroutine when Next returns first.
		done := make(chan struct{})
		go func() {
			defer close(done)
			next, nextErr = clientState.ChainIter.Next(true)
		}()
		select {
		case <-done:
			// Iterator returned
		case <-conn.Done():
			clientState.ChainIter.Cancel()
			o.closeAbandonedChainsyncServerConn(ctx, conn)
			return
		}
	}
	if nextErr == nil && next != nil && waitForLedger &&
		o.chainsyncServerAheadOfLedger(next) {
		waitCtx, cancel := context.WithCancel(context.Background())
		applied := make(chan error, 1)
		go func() {
			applied <- o.ledgerState.AwaitAppliedSlot(waitCtx, next.Point.Slot)
		}()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-conn.Done():
				cancel()
				o.closeAbandonedChainsyncServerConn(ctx, conn)
				return
			case <-ticker.C:
				if clientState.ChainIter.HasPendingRollback() {
					cancel()
					next, nextErr = clientState.ChainIter.Next(false)
					o.chainsyncServerServeAwaited(ctx, conn, next, nextErr)
					return
				}
			case err := <-applied:
				if err != nil {
					cancel()
					o.closeAbandonedChainsyncServerConn(ctx, conn)
					return
				}
				if clientState.ChainIter.HasPendingRollback() {
					cancel()
					next, nextErr = clientState.ChainIter.Next(false)
					o.chainsyncServerServeAwaited(ctx, conn, next, nextErr)
					return
				}
				appliedTip := o.ledgerState.Tip().Point
				if appliedTip.Slot == next.Point.Slot &&
					!bytes.Equal(appliedTip.Hash, next.Point.Hash) {
					// A different block reached the same slot. Wait for the
					// iterator's rollback event and serve that instead.
					continue
				}
				cancel()
				o.chainsyncServerServeAwaited(ctx, conn, next, nextErr)
				return
			}
		}
	}
	o.chainsyncServerServeAwaited(ctx, conn, next, nextErr)
}

// closeAbandonedChainsyncServerConn drops the transport when the connection is
// torn down while the waiter holds the peer in MustReply. Abandoning the wait
// leaves the peer parked with the server holding agency, so the transport has
// to be dropped: nothing else unparks it. See closeChainsyncServerConn. The
// connection manager has already consumed the connection's error, so the
// close is idempotent.
func (o *Ouroboros) closeAbandonedChainsyncServerConn(
	ctx ochainsync.CallbackContext,
	conn managedConnection,
) {
	closeReason := error(errChainsyncAwaitConnectionClosed)
	if closeErr, ok := conn.(interface{ CloseError() error }); ok {
		if actualErr := closeErr.CloseError(); actualErr != nil {
			closeReason = fmt.Errorf(
				"%w: %w",
				errChainsyncAwaitConnectionClosed,
				actualErr,
			)
		}
	}
	o.closeChainsyncServerConn(
		conn,
		ctx.ConnectionId.String(),
		fmt.Errorf(
			"connection torn down while peer awaited a reply: %w",
			closeReason,
		),
	)
}

// errChainsyncAwaitConnectionClosed is the reason logged when the connection
// is torn down while the waiter is parked.
var errChainsyncAwaitConnectionClosed = errors.New("connection closed")

var errChainsyncCandidateRejected = errors.New(
	"chain selection rejected peer tip",
)

// chainsyncServerServeAwaited delivers the chain iterator result that resolved
// a post-AwaitReply wait, or drops the transport when there is nothing to
// deliver.
//
// Once MsgAwaitReply is on the wire the server holds agency in MustReply. A
// peer parked there cannot make progress and cannot detect abandonment except
// through gouroboros' randomized MustReply timer (135-269s), after which it
// tears the connection down, reconnects, re-intersects, receives one
// MsgRollBackward and parks again -- a stable loop that keeps a node pinned
// behind a healthy upstream with no ERROR logged on either side. So every path
// out of here either sends a reply or closes the connection; returning
// silently is not a third option.
func (o *Ouroboros) chainsyncServerServeAwaited(
	ctx ochainsync.CallbackContext,
	conn io.Closer,
	next *chain.ChainIteratorResult,
	nextErr error,
) {
	if nextErr == nil {
		o.recordServedActivity(ctx.ConnectionId)
	}
	if nextErr != nil {
		if errors.Is(nextErr, context.Canceled) {
			// The only cancellations of a server client's iterator come from
			// chainsync.State.RemoveClient (driven by connection-closed
			// handling) and from the error-channel exit above, both of which
			// have already dropped the transport. There is no parked peer left
			// to unpark, so this stays a quiet, expected unwind.
			o.config.Logger.Debug(
				"chainsync server: await unwound by connection teardown",
				"connection_id", ctx.ConnectionId.String(),
			)
			return
		}
		o.closeChainsyncServerConn(
			conn,
			ctx.ConnectionId.String(),
			fmt.Errorf(
				"chain iterator failed while peer awaited a reply: %w",
				nextErr,
			),
		)
		return
	}
	if next == nil {
		// Defensive: ChainIterator.Next maps an empty non-error result to
		// ErrIteratorChainTip, so this is not reachable through the real
		// iterator. Should it ever become reachable, dropping the transport is
		// still the only way to release the parked peer.
		o.closeChainsyncServerConn(
			conn,
			ctx.ConnectionId.String(),
			errors.New(
				"chain iterator returned no block while peer awaited a reply",
			),
		)
		return
	}
	tip := o.refreshTip(next)
	if next.Rollback {
		if err := ctx.Server.RollBackward(
			next.Point,
			tip,
		); err != nil {
			o.reportChainsyncServerAsyncError(
				conn,
				ctx.ConnectionId.String(),
				"RollBackward",
				err,
			)
		}
		return
	}
	blockCbor, blockErr := o.chainsyncServerBlockCbor(ctx, next.Block)
	if blockErr != nil {
		// Do not RollForward an incomplete CertRB. This runs after the
		// callback returned AwaitReply, so actively close the transport
		// (an error-channel send alone does not) to unpark the client
		// from AwaitReply so it reconnects and retries the point once
		// the endorser closure is available.
		o.closeChainsyncServerConn(
			conn,
			ctx.ConnectionId.String(),
			blockErr,
		)
		return
	}
	if err := ctx.Server.RollForward(
		next.Block.Type,
		blockCbor,
		tip,
	); err != nil {
		o.reportChainsyncServerAsyncError(
			conn,
			ctx.ConnectionId.String(),
			"RollForward",
			err,
		)
	}
}

func (o *Ouroboros) reportChainsyncServerAsyncError(
	conn io.Closer,
	connectionID string,
	operation string,
	err error,
) {
	if errors.Is(err, context.Canceled) {
		return
	}
	o.config.Logger.Error(
		"chainsync server: async send failed",
		"connection_id", connectionID,
		"operation", operation,
		"error", err,
	)
	if closeErr := conn.Close(); closeErr != nil {
		o.config.Logger.Debug(
			"chainsync server: failed to close connection after async send error",
			"connection_id",
			connectionID,
			"operation",
			operation,
			"error",
			closeErr,
		)
	}
}

// closeChainsyncServerConn tears down the connection after the async serving
// goroutine declines to serve a block (e.g. a certifying ranking block whose
// endorser closure did not resolve). This runs after the RequestNext callback
// has already returned AwaitReply, so gouroboros cannot propagate the error
// through the callback-owned teardown path the synchronous path relies on. A
// direct Close tears down the bearer so the client reconnects and retries the
// point. It also closes the gouroboros-owned error channel, which wakes the
// connmanager watcher without racing a Dingo send against channel closure.
func (o *Ouroboros) closeChainsyncServerConn(
	conn io.Closer,
	connectionID string,
	err error,
) {
	o.config.Logger.Warn(
		"chainsync server: closing connection to force client retry",
		"connection_id", connectionID,
		"error", err,
	)
	if closeErr := conn.Close(); closeErr != nil {
		o.config.Logger.Debug(
			"chainsync server: connection close failed",
			"connection_id", connectionID,
			"error", closeErr,
		)
	}
}

func (o *Ouroboros) chainsyncClientRollBackward(
	ctx ochainsync.CallbackContext,
	point ocommon.Point,
	tip ochainsync.Tip,
) (err error) {
	_, span := tracing.Start(
		context.Background(),
		"chainsync.roll_backward",
		attribute.String("connection.id", ctx.ConnectionId.String()),
		tracing.Uint64("block.slot", point.Slot),
		attribute.String("block.hash", hex.EncodeToString(point.Hash)),
	)
	defer func() { tracing.End(span, err) }()
	// The peer's protocol cursor moves whether or not the rollback is tracked
	// below, so a withheld header can no longer stand in for it.
	o.invalidateDeferredHeaderResolution(ctx.ConnectionId)
	if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
		return ochainsync.ErrStopSyncProcess
	}
	if !o.reconcileChainsyncIngressAdmission(
		ctx.ConnectionId,
		o.shouldPublishChainsyncToLedger(ctx.ConnectionId),
	) {
		return nil
	}
	if o.chainsyncState != nil && !o.chainsyncState.UpdateClientRollback(
		ctx.ConnectionId, point, tip,
	) {
		return nil
	}
	// Observe the rollback for chain selection FIRST — it trims the peer's
	// observed frontier (ApplyRollback), which can change its corroboration
	// status and registers an untracked peer's tip, so the apply gate below
	// must reflect it. If the hook handles it synchronously (Genesis selection
	// mode), skip the async publish to avoid a double update; otherwise publish
	// for the async subscriber. This mirrors the roll-forward
	// ChainsyncObservePeerTip ordering.
	rollbackEvent := chainselection.PeerRollbackEvent{
		ConnectionId: ctx.ConnectionId,
		Point:        point,
		Tip:          tip,
	}
	observedSync := false
	if o.config.ChainsyncObservePeerRollback != nil {
		observedSync = o.config.ChainsyncObservePeerRollback(rollbackEvent)
	}
	if !observedSync {
		o.eventBus.Publish(
			chainselection.PeerRollbackEventType,
			event.NewEvent(
				chainselection.PeerRollbackEventType,
				rollbackEvent,
			),
		)
	}
	// Apply gate: withhold an uncorroborated peer's rollback from the ledger,
	// mirroring the roll-forward apply gate. The observation above ran first, so
	// this reflects the post-rollback corroboration state.
	if !o.shouldApplyChainsyncToLedger(ctx.ConnectionId) {
		o.config.Logger.Debug(
			"chainsync: rollback withheld (not apply eligible)",
			"component", "ouroboros",
			"slot", point.Slot,
			"connection_id", ctx.ConnectionId.String(),
		)
		return nil
	}
	// Generate event. This stream is ordering-critical: dropping a
	// rollback/header event can strand the ledger pipeline, so use blocking
	// delivery to apply backpressure instead of lossy buffer overflow.
	if err := o.eventBus.PublishBlocking(
		ledger.ChainsyncEventType,
		event.NewEvent(
			ledger.ChainsyncEventType,
			ledger.ChainsyncEvent{
				ConnectionId: ctx.ConnectionId,
				Rollback:     true,
				Point:        point,
				Tip:          tip,
			},
		),
	); err != nil {
		return fmt.Errorf("chainsync: publish rollback event: %w", err)
	}
	return nil
}

// chainsyncCursorFarBehindLocalTip reports whether a header with the given
// block number trails the local tip by more than the security parameter while
// the peer's advertised tip does not. Only that shape is a stale cursor that
// re-intersecting can repair. A rollback or fork within k is ordinary, and a
// peer whose own tip is that far behind is merely lagging on the chain: it is
// kept attached and left unselected until it catches up. Without a ledger the
// answer is false.
//
// The rule only applies while chain selection has no best peer. During bulk
// sync a peer slower than the one feeding the ledger trails the applied tip
// by more than k while its advertised tip is ahead; it is merely lagging, and
// closing it would only churn connections. With no best peer there is no
// feeder to lag behind and nothing else advances the tip, so a stale cursor
// is the stall.
func (o *Ouroboros) chainsyncCursorFarBehindLocalTip(
	headerBlockNumber uint64,
	peerTipBlockNumber uint64,
) bool {
	if o.chainsyncLocalTipBlock == nil {
		return false
	}
	if o.chainsyncHasBestPeer != nil && o.chainsyncHasBestPeer() {
		return false
	}
	tipBlock, k := o.chainsyncLocalTipBlock()
	farBehind := func(blockNumber uint64) bool {
		return tipBlock > blockNumber && tipBlock-blockNumber > k
	}
	return farBehind(headerBlockNumber) && !farBehind(peerTipBlockNumber)
}

// claimBehindTipRecycle reports whether a behind-local-tip recycle may be
// requested at now, and records it when so. It admits one recycle per
// chainsyncBehindTipRecycleCooldown across all connections.
func (o *Ouroboros) claimBehindTipRecycle(now time.Time) bool {
	o.behindTipRecycleMu.Lock()
	defer o.behindTipRecycleMu.Unlock()
	if !o.behindTipRecycleAt.IsZero() &&
		now.Sub(o.behindTipRecycleAt) < chainsyncBehindTipRecycleCooldown {
		return false
	}
	o.behindTipRecycleAt = now
	return true
}

// releaseBehindTipRecycle returns the window taken by claimBehindTipRecycle at
// now when no recycle was scheduled for it. No other claim can land inside an
// open window, so the previous claim had already lapsed and clearing is exact.
func (o *Ouroboros) releaseBehindTipRecycle(now time.Time) {
	o.behindTipRecycleMu.Lock()
	defer o.behindTipRecycleMu.Unlock()
	if o.behindTipRecycleAt.Equal(now) {
		o.behindTipRecycleAt = time.Time{}
	}
}

func (o *Ouroboros) chainsyncClientRollForwardAt(
	ctx ochainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip ochainsync.Tip,
	arrivalTime time.Time,
) (err error) {
	switch v := blockData.(type) {
	case gledger.BlockHeader:
		// Every path that does not accept this header clears the
		// connection's accepted tip, so a later deferred header cannot count
		// as a linear extension of one that never reached the ledger queue.
		tipAccepted := false
		defer func() {
			if !tipAccepted {
				o.forgetAcceptedHeaderTip(ctx.ConnectionId)
			}
		}()
		if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
			return ochainsync.ErrStopSyncProcess
		}
		blockSlot := v.SlotNumber()
		blockHash := v.Hash().Bytes()
		_, span := tracing.Start(
			context.Background(),
			"chainsync.roll_forward",
			attribute.String("connection.id", ctx.ConnectionId.String()),
			tracing.Uint64("block.slot", blockSlot),
			attribute.String("block.hash", hex.EncodeToString(blockHash)),
		)
		defer func() { tracing.End(span, err) }()
		point := ocommon.NewPoint(blockSlot, blockHash)
		// Genesis Limit on Patience: the peer is charged up to the header's
		// network arrival, not for this node's decoding, admission waits,
		// verification, or ledger backpressure below. A header that clears
		// verification resumes the leak when the callback returns.
		patienceGrant := false
		patienceResume := false
		if o.chainsyncState != nil {
			o.chainsyncState.PatienceMessageArrived(
				ctx.ConnectionId,
				arrivalTime,
			)
			defer func() {
				if patienceGrant {
					o.chainsyncState.PatienceHeaderAccepted(
						ctx.ConnectionId,
						v.BlockNumber(),
						point.Slot == tip.Point.Slot &&
							bytes.Equal(point.Hash, tip.Point.Hash),
					)
				} else if patienceResume {
					o.chainsyncState.PatienceMessageProcessed(ctx.ConnectionId)
				}
			}()
		}
		// Extract VRF output from block header once for chain
		// selection tie-breaking (used in both dedup and normal
		// paths below).
		vrfOutput := praos.GetVRFOutput(v)
		praosView, _ := praos.GetPraosTiebreakerView(v)
		// Ingress eligibility is the sole gate for feeding the ledger
		// and chain selection. reconcileChainsyncIngressAdmission
		// defers to ChainsyncIngressEligible (peergov), which already
		// filters random inbound peers via chainSelectionEligible.
		// A full-duplex inbound from a configured upstream remains
		// eligible here, so the node doesn't stall when the relay
		// dials us first after a crash.
		ingressEligible := o.reconcileChainsyncIngressAdmission(
			ctx.ConnectionId,
			o.shouldPublishChainsyncToLedger(ctx.ConnectionId),
		)
		patienceResume = ingressEligible
		if ingressEligible && o.chainsyncHeaderBackpressure != nil {
			admissionCtx, cancelAdmission := chainsyncAdmissionContext(ctx)
			defer cancelAdmission()
			admittedHeadroom := ledger.ChainsyncEventBufferSize + 1 +
				o.maxTrackedChainsyncClients()
			if err := o.chainsyncHeaderBackpressure(
				admissionCtx,
				admittedHeadroom,
			); err != nil {
				return fmt.Errorf("chainsync: await header capacity: %w", err)
			}
		}
		// Each wait below can span the start of a hold; re-check after it.
		if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
			return ochainsync.ErrStopSyncProcess
		}
		o.config.Logger.Debug(
			"chainsync: header received",
			"component", "ouroboros",
			"slot", blockSlot,
			"tip_slot", tip.Point.Slot,
			"connection_id", ctx.ConnectionId.String(),
			"ingress_eligible", ingressEligible,
		)
		chainsyncEvent := ledger.ChainsyncEvent{
			ConnectionId: ctx.ConnectionId,
			ArrivalTime:  arrivalTime,
			Point:        point,
			Type:         blockType,
			BlockHeader:  v,
			Tip:          tip,
		}
		// An ingress-eligible connection whose cursor trails the local tip by
		// more than k while its peer is near the tip can never contribute: the
		// ledger drops its headers as stale and chain selection skips the
		// peer, while the connection holds an eligible slot. This also covers
		// an observer promoted just above with a cursor left far behind.
		if ingressEligible && o.chainsyncCursorFarBehindLocalTip(
			v.BlockNumber(),
			tip.BlockNumber,
		) {
			// A connection with a due or fired re-intersection is already
			// being recycled; its in-flight headers must not take the shared
			// window from another stale peer. A later-onset future-header
			// timer withholds every header until it fires, so it is replaced.
			now := time.Now()
			if o.headerResyncDueOrFired(ctx.ConnectionId, now) ||
				!o.claimBehindTipRecycle(now) {
				return nil
			}
			// The connection may already have left the connection manager,
			// or another schedule may have won; neither recycles anything.
			if !o.scheduleHeaderResync(
				ctx.ConnectionId,
				now,
				event.ChainsyncResyncReasonBehindLocalTip,
			) {
				o.releaseBehindTipRecycle(now)
				return nil
			}
			o.config.Logger.Debug(
				"chainsync: cursor far behind the local tip",
				"component", "ouroboros",
				"slot", blockSlot,
				"block_number", v.BlockNumber(),
				"peer_tip_block_number", tip.BlockNumber,
				"connection_id", ctx.ConnectionId.String(),
			)
			return nil
		}
		// Enforce the future-header boundary on this peer's protocol callback,
		// before any observed-tip, cursor, dedup, or ledger mutation. A
		// permitted wait therefore blocks only this peer and never the shared
		// ledger ChainSync dispatch mutex/goroutine.
		if ingressEligible && o.chainsyncHeaderAdmission != nil {
			admitted, err := o.admitChainsyncHeader(ctx, chainsyncEvent)
			if err != nil || !admitted {
				return err
			}
			if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
				return ochainsync.ErrStopSyncProcess
			}
		}
		// Verify header crypto (VRF/KES and, once local state has caught up,
		// leader eligibility) before this header is allowed to influence
		// Genesis chain-selection density or corroboration. Without this
		// gate, an untrusted peer-reported header could steer fork selection
		// using data that has not passed the same checks as the applied
		// chain. This runs for every ingress-eligible peer, not
		// only the one currently apply-eligible: a competing candidate's
		// headers never reach the ledger's own chainsync header-queue
		// verification, since that only runs for headers actually applied.
		//
		// Verification is skipped only for a slot an imported Mithril
		// snapshot already covers; a coarse bulk
		// historical/catch-up loading toggle no longer exempts it, so a
		// Mithril-restored bootstrap is unaffected but ordinary fast sync is
		// not. A deferred result (local state has not caught up to this
		// header's slot yet) does not penalize the peer and keeps the header out
		// of observed history. A deferred header that extends the local chain
		// still reaches the selector and the ledger queue (see
		// deferredHeaderExtendsLocalChain); any other is withheld from both,
		// and once an applied ledger update makes its validation context
		// advance, a fresh intersection replays the peer's linked suffix
		// through this verifier. A header past the forecast horizon cannot
		// reach blockfetch yet and is re-intersected separately. A header whose
		// fork ancestry cannot be authenticated is also excluded from
		// observation, but the connection keeps it for later ancestry lookups
		// and neither resyncs nor recycles. A definite crypto/eligibility failure
		// excludes the header and recycles the connection.
		selectionEligible := ingressEligible
		deferredVerification := false
		var verificationTipBaseline ochainsync.Tip
		if ingressEligible && o.chainSelectionShouldVerifyHeaderCrypto != nil &&
			o.chainSelectionShouldVerifyHeaderCrypto(blockSlot) {
			if o.ledgerState != nil {
				verificationTipBaseline = o.ledgerState.Tip()
			}
			verificationGeneration, verifyErr := o.chainSelectionVerifyHeaderCrypto(
				ctx.ConnectionId,
				v,
			)
			if errors.Is(verifyErr, ledger.ErrHeaderBeyondForecastHorizon) &&
				o.chainsyncHeaderAdmission != nil {
				// Admission waited for the horizon, so it has since moved
				// back (a ledger rollback). Wait for it once more.
				admitted, err := o.admitChainsyncHeader(ctx, chainsyncEvent)
				if err != nil || !admitted {
					return err
				}
				if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
					return ochainsync.ErrStopSyncProcess
				}
				if o.ledgerState != nil {
					verificationTipBaseline = o.ledgerState.Tip()
				}
				verificationGeneration, verifyErr = o.chainSelectionVerifyHeaderCrypto(
					ctx.ConnectionId,
					v,
				)
			}
			if verifyErr == nil {
				chainsyncEvent.HeaderCryptoVerificationGeneration = verificationGeneration
			}
			if errors.Is(verifyErr, ledger.ErrHeaderBeyondForecastHorizon) {
				// The header cannot be validated yet, so it must not extend
				// this peer's candidate or reach blockfetch. Withhold it
				// without penalty and re-intersect, which re-delivers it
				// through admission.
				o.config.Logger.Debug(
					"chainsync: header past the forecast horizon withheld from chain selection",
					"component", "ouroboros",
					"slot", blockSlot,
					"connection_id", ctx.ConnectionId.String(),
				)
				o.scheduleFutureHeaderResync(ctx.ConnectionId, time.Now())
				return nil
			}
			if verifyErr != nil {
				if ledger.IsHeaderVerificationWithheld(verifyErr) {
					selectionEligible = false
				} else if ledger.IsHeaderVerificationDeferred(verifyErr) {
					selectionEligible = false
					deferredVerification = true
					o.config.Logger.Debug(
						"chainsync: header producer authorization deferred",
						"component", "ouroboros",
						"slot", blockSlot,
						"connection_id", ctx.ConnectionId.String(),
						"error", verifyErr,
					)
				} else {
					selectionEligible = false
					patienceResume = false
					o.config.Logger.Warn(
						"chainsync: excluding header from chain selection after verification failure",
						"component", "ouroboros",
						"slot", blockSlot,
						"connection_id", ctx.ConnectionId.String(),
						"error", verifyErr,
					)
					o.eventBus.Publish(
						ledger.ConnectionRecycleRequestedEventType,
						event.NewEvent(
							ledger.ConnectionRecycleRequestedEventType,
							ledger.ConnectionRecycleRequestedEvent{
								ConnectionId: ctx.ConnectionId,
								Reason:       "header_verification_failure",
							},
						),
					)
					if o.chainsyncState != nil {
						o.chainsyncState.MarkClientFailed(ctx.ConnectionId)
					}
					ingressEligible = false
				}
			}
		}
		extendsLocalChain := ingressEligible &&
			o.deferredHeaderExtendsLocalChain(ctx.ConnectionId, v)
		if deferredVerification {
			if ingressEligible && o.chainsyncState != nil {
				// Keep bounded raw ancestry only; ledger validation checks the
				// complete linked path before it can affect candidate selection.
				o.chainsyncState.RecordObservedHeader(
					chainsync.ObservedHeader{
						ConnectionId: ctx.ConnectionId,
						Point:        point,
						BlockHeader:  v,
						ArrivalTime:  arrivalTime,
						Tip:          tip,
						BlockNumber:  v.BlockNumber(),
						Type:         blockType,
					},
				)
			}
			if !extendsLocalChain {
				// Do not advance peer cursors, dedup state, or publish to
				// selector/ledger until this header passes strict verification.
				o.markDeferredHeaderRecovery(
					ctx.ConnectionId,
					verificationTipBaseline,
					point,
					tip,
					false,
				)
				o.updateChainsyncMetrics(ctx.ConnectionId, tip)
				return nil
			}
			// A linear extension cannot steer selection toward a competing
			// fork, and the selector must keep following the peer that is
			// feeding the queue or apply eligibility stalls for as long as
			// the ledger is behind the header's epoch.
			selectionEligible = true
		}
		// Observe the tip for chain selection FIRST, so the apply-eligibility
		// decision below reflects this header. Only ingress-eligible peers are
		// observed; random inbound peers reporting ephemeral tips are filtered
		// by peergov and skipped here.
		observedTip := ochainsync.Tip{
			Point:       point,
			BlockNumber: v.BlockNumber(),
		}
		var peerTipUpdate *chainselection.PeerTipUpdateEvent
		if selectionEligible {
			update := chainselection.PeerTipUpdateEvent{
				ConnectionId: ctx.ConnectionId,
				Tip:          tip,
				ObservedTip:  observedTip,
				VRFOutput:    vrfOutput,
				PraosView:    praosView,
				AdmissionID:  o.chainsyncAdmissionSequence.Add(1),
				HeaderCryptoVerificationGeneration: chainsyncEvent.
					HeaderCryptoVerificationGeneration,
				// The parent hash is what lets the selector tell a connected
				// header chain from a sequence of advancing numbers.
				ObservedPrevHash: v.PrevHash().Bytes(),
				ObservedBoundary: blockType == gledger.BlockTypeByronEbb,
			}
			disposition := chainselection.PeerTipUpdateUnhandled
			if o.config.ChainsyncObservePeerTip != nil {
				disposition = o.config.ChainsyncObservePeerTip(update)
			}
			switch disposition {
			case chainselection.PeerTipUpdateUnhandled:
				if o.config.ChainsyncResolvePeerTip != nil {
					ingressEligible = false
					break
				}
				peerTipUpdate = &update
				o.eventBus.Publish(
					chainselection.PeerTipUpdateEventType,
					event.NewEvent(chainselection.PeerTipUpdateEventType, update),
				)
			case chainselection.PeerTipUpdatePending:
				ingressEligible = false
			case chainselection.PeerTipUpdateAccepted:
				peerTipUpdate = &update
			case chainselection.PeerTipUpdateRejected:
				return errChainsyncCandidateRejected
			default:
				return fmt.Errorf("invalid peer tip disposition: %d", disposition)
			}
		}
		// Apply-eligibility, evaluated after observation so it reflects this
		// header. A peer can be ingress-eligible yet not apply-eligible (an
		// uncorroborated Genesis fast source): its tips are observed but its
		// blocks are withheld from the ledger.
		applyEligible := selectionEligible &&
			o.shouldApplyChainsyncToLedger(ctx.ConnectionId)
		// A header withheld here is lost to the ledger: the peer's cursor moves
		// past it. Hold only a selection-eligible header denied by Genesis
		// corroboration; corroboration cannot override failed authentication.
		if selectionEligible && ingressEligible && !applyEligible &&
			o.config.ChainsyncAwaitCorroboration != nil {
			admissionCtx, cancelAdmission := chainsyncAdmissionContext(ctx)
			holdCtx, cancelHold := context.WithTimeout(
				admissionCtx,
				o.corroborationHoldTimeout,
			)
			eligible, err := o.config.ChainsyncAwaitCorroboration(
				holdCtx,
				ctx.ConnectionId,
			)
			holdExpired := errors.Is(err, context.DeadlineExceeded) &&
				admissionCtx.Err() == nil
			cancelHold()
			cancelAdmission()
			switch {
			case holdExpired:
				// Keep the chainsync cursor. Re-intersecting immediately from
				// the applied tip discards this peer's candidate history and
				// repeats the same hold; another peer needs that history to
				// corroborate the withheld header when it catches up.
			case err != nil:
				return fmt.Errorf("wait for genesis corroboration: %w", err)
			}
			if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
				return ochainsync.ErrStopSyncProcess
			}
			applyEligible = eligible
		}
		// Limit on Eagerness: the tip was observed above, so the limit already
		// accounts for this header. Wait before recording cross-peer dedup state:
		// if the connection closes while held, another peer must still be able
		// to deliver the same header to the ledger.
		if applyEligible && o.config.ChainsyncAwaitEagerness != nil {
			var appliedTip func() ochainsync.Tip
			if o.ledgerState != nil {
				appliedTip = o.ledgerState.Tip
			}
			admissionCtx, cancelAdmission := chainsyncAdmissionContext(ctx)
			err := o.config.ChainsyncAwaitEagerness(
				admissionCtx,
				ctx.ConnectionId,
				v.BlockNumber(),
				appliedTip,
			)
			cancelAdmission()
			if err != nil {
				// The header never reaches the ledger queue.
				tipAccepted = false
				return fmt.Errorf("wait for eagerness limit: %w", err)
			}
			if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
				// Dropped by the held-ingress check, so never queued.
				tipAccepted = false
				return ochainsync.ErrStopSyncProcess
			}
		}
		patienceGrant = selectionEligible && ingressEligible
		// Record the cross-peer dedup entry only for headers we will actually
		// apply. A header withheld from an uncorroborated peer must not suppress
		// a later delivery from a corroborated, apply-eligible peer.
		isNew := true
		if o.chainsyncState != nil {
			if applyEligible {
				isNew = o.chainsyncState.RecordHeader(ctx.ConnectionId, point)
			}
		}
		if ingressEligible && !deferredVerification && o.chainsyncState != nil {
			o.chainsyncState.RecordObservedHeader(
				chainsync.ObservedHeader{
					ConnectionId: ctx.ConnectionId,
					HeaderCryptoVerificationGeneration: chainsyncEvent.
						HeaderCryptoVerificationGeneration,
					Point:       point,
					Type:        blockType,
					BlockHeader: v,
					ArrivalTime: arrivalTime,
					Tip:         tip,
				},
			)
		}
		if !ingressEligible {
			o.config.Logger.Debug(
				"chainsync: header dropped (not ingress eligible)",
				"component", "ouroboros",
				"slot", blockSlot,
				"connection_id", ctx.ConnectionId.String(),
			)
			o.updateChainsyncMetrics(ctx.ConnectionId, tip)
			return nil
		}
		if o.chainsyncState != nil {
			o.chainsyncState.RecordClientDelivery(ctx.ConnectionId, point)
		}
		if !selectionEligible {
			o.updateChainsyncMetrics(ctx.ConnectionId, tip)
			return nil
		}
		// Header-sync strategy gate: cross-peer deduplication (isNew) has run
		// above; the configured strategy now decides whether this eligible
		// peer is permitted to drive ledger ingress. Primary lets any eligible
		// peer publish new headers and the active peer replay duplicates first
		// seen elsewhere (prior behavior); parallel lets every eligible peer
		// publish new headers but never replays duplicates; round-robin admits
		// only the current rotation driver.
		if o.chainsyncState != nil &&
			!o.chainsyncState.ShouldPublishHeader(
				ctx.ConnectionId,
				point,
				isNew,
			) {
			// The staged candidate for this point is not resolved here: it
			// follows the connection whose copy reaches the ledger.
			dropReason := "duplicate"
			if isNew {
				dropReason = "not ingress driver"
			}
			o.config.Logger.Debug(
				"chainsync: header dropped",
				"component", "ouroboros",
				"reason", dropReason,
				"strategy", o.chainsyncState.HeaderSyncStrategy().String(),
				"slot", blockSlot,
				"connection_id", ctx.ConnectionId.String(),
			)
			o.forgetAcceptedHeaderTip(ctx.ConnectionId)
			o.updateChainsyncMetrics(ctx.ConnectionId, tip)
			return nil
		}
		// Apply gate: a peer's tips have already been observed for chain
		// selection above, but its headers are applied to the ledger only when
		// apply-eligible (computed above, after observation). This withholds
		// blocks from an uncorroborated Genesis fast source (it is observed but
		// cannot steer the ledger) while letting corroboration still form from
		// the observed tips. The header was recorded WITHOUT dedup above, so a
		// later corroborated peer can still publish this point.
		if !applyEligible {
			o.config.Logger.Debug(
				"chainsync: header withheld (not apply eligible)",
				"component", "ouroboros",
				"slot", blockSlot,
				"connection_id", ctx.ConnectionId.String(),
			)
			o.forgetAcceptedHeaderTip(ctx.ConnectionId)
			o.updateChainsyncMetrics(ctx.ConnectionId, tip)
			return nil
		}
		// The only target ledger may later publish is paired with this exact
		// delivered header and its apply-eligibility decision. Do not make
		// ledger recover it from mutable selector state.
		chainsyncEvent.SyncTarget = observedTip
		if o.config.ChainsyncSyncTarget != nil {
			if target, ok := o.config.ChainsyncSyncTarget(
				chainselection.PeerTipUpdateEvent{
					ConnectionId: ctx.ConnectionId,
					Tip:          tip,
					ObservedTip:  observedTip,
					VRFOutput:    vrfOutput,
					PraosView:    praosView,
				},
			); ok {
				chainsyncEvent.SyncTarget = target
			}
		}
		chainsyncEvent.SyncTargetTrusted = true
		chainsyncEvent.PeerTipUpdate = peerTipUpdate
		if o.chainsyncState != nil {
			chainsyncEvent.InvalidatePeerHeaderHistory = func() {
				o.chainsyncState.ClearObservedHeaderHistoryFrom(
					ctx.ConnectionId,
					point,
				)
			}
		}
		if peerTipUpdate != nil {
			resolvePeerTip := func(admitted bool) {
				update := *peerTipUpdate
				if o.config.ChainsyncResolvePeerTip != nil {
					o.config.ChainsyncResolvePeerTip(update, admitted)
					return
				}
				if !admitted && o.eventBus != nil {
					update.Rejected = true
					o.eventBus.Publish(
						chainselection.PeerTipUpdateEventType,
						event.NewEvent(
							chainselection.PeerTipUpdateEventType,
							update,
						),
					)
				}
			}
			chainsyncEvent.PeerTipAdmission = func(admitted bool) {
				if admitted {
					if o.chainsyncState != nil {
						o.chainsyncState.AdmitClientTip(
							ctx.ConnectionId,
							point,
							tip,
						)
					}
					resolvePeerTip(true)
					return
				}
				o.acceptedHeaderMu.Lock()
				if bytes.Equal(o.acceptedHeaderTips[ctx.ConnectionId], blockHash) {
					delete(o.acceptedHeaderTips, ctx.ConnectionId)
				}
				o.acceptedHeaderMu.Unlock()
				if o.ledgerState != nil {
					o.markDeferredHeaderRecovery(
						ctx.ConnectionId,
						o.ledgerState.Tip(),
						point,
						tip,
						true,
					)
				}
				resolvePeerTip(false)
			}
		}
		if ingressEligible && extendsLocalChain {
			o.recordAcceptedHeaderTip(ctx.ConnectionId, blockHash)
			tipAccepted = true
		}
		if err := o.eventBus.PublishBlocking(
			ledger.ChainsyncEventType,
			event.NewEvent(
				ledger.ChainsyncEventType,
				chainsyncEvent,
			),
		); err != nil {
			if chainsyncEvent.InvalidatePeerHeaderHistory != nil {
				chainsyncEvent.InvalidatePeerHeaderHistory()
			}
			if chainsyncEvent.PeerTipAdmission != nil {
				chainsyncEvent.PeerTipAdmission(false)
			}
			return fmt.Errorf("chainsync: publish roll-forward event: %w", err)
		}
		// Update ChainSync performance metrics for peer scoring
		o.updateChainsyncMetrics(ctx.ConnectionId, tip)
	default:
		return fmt.Errorf("unexpected block data type: %T", v)
	}
	return nil
}

// networkIngestHeld reports whether the ledger holds network ingest.
func (o *Ouroboros) networkIngestHeld() bool {
	return o.ledgerState != nil && o.ledgerState.NetworkIngestHeld()
}

// recordHeldChainsyncTip reports whether connId's chainsync messages are
// dropped: network ingest is held, or the connection existed during a hold
// and has not closed yet. A dropped message skips every admission wait,
// verification, chain selection and the ledger publish -- each of which would
// otherwise wait on, or act on, a ledger that is not following the peer --
// and only the tip the peer reported is kept, for PeerTipSlot and peer
// scoring. The caller returns ochainsync.ErrStopSyncProcess, which stops the
// client requesting further headers; the tip is then refreshed by
// heldTipRefresher.
func (o *Ouroboros) recordHeldChainsyncTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
) bool {
	held := o.networkIngestHeld()
	if !held && (o.chainsyncState == nil ||
		!o.chainsyncState.ConnectionIngestHeld(connId)) {
		return false
	}
	if o.chainsyncState != nil {
		o.chainsyncState.RecordHeldClientTip(connId, tip)
	}
	o.updateChainsyncMetrics(connId, tip)
	if held {
		o.ensureHeldTipRefresher()
	}
	return true
}

// admitChainsyncHeader runs the future-header admission for one delivered
// header and reports whether it may proceed. A header it withholds is not a
// peer fault; the connection re-intersects so it is delivered again.
func (o *Ouroboros) admitChainsyncHeader(
	ctx ochainsync.CallbackContext,
	e ledger.ChainsyncEvent,
) (bool, error) {
	blockSlot := e.Point.Slot
	var forecastWitness *chainselection.PeerTipUpdateEvent
	if o.config.ChainsyncPrepareForecastWitness != nil {
		e.ForecastHorizonWait = func() {
			praosView, _ := praos.GetPraosTiebreakerView(e.BlockHeader)
			update := chainselection.PeerTipUpdateEvent{
				ConnectionId:     e.ConnectionId,
				Tip:              e.Tip,
				ObservedTip:      ochainsync.Tip{Point: e.Point, BlockNumber: e.BlockHeader.BlockNumber()},
				VRFOutput:        praos.GetVRFOutput(e.BlockHeader),
				PraosView:        praosView,
				AdmissionID:      o.chainsyncAdmissionSequence.Add(1),
				ObservedPrevHash: e.BlockHeader.PrevHash().Bytes(),
				ObservedBoundary: e.Type == gledger.BlockTypeByronEbb,
			}
			forecastWitness = &update
			for _, connId := range o.config.ChainsyncPrepareForecastWitness(update) {
				o.scheduleHeaderResync(
					connId,
					time.Now(),
					event.ChainsyncResyncReasonForecastCorroborationRecovery,
				)
			}
		}
	}
	admissionCtx, cancel := chainsyncAdmissionContext(ctx)
	defer cancel()
	accepted, err := o.chainsyncHeaderAdmission(admissionCtx, e)
	if forecastWitness != nil && o.config.ChainsyncRemoveForecastWitness != nil {
		o.config.ChainsyncRemoveForecastWitness(*forecastWitness)
	}
	if errors.Is(err, ledger.ErrChainsyncHeaderNeedsResync) {
		// No queued or in-flight blockfetch can publish state that advances
		// the forecast, so replaying from the local chain is the only way to
		// deliver this header after the node has made progress. This is not a
		// peer fault.
		o.config.Logger.Debug(
			"chainsync: no local block work can advance the forecast horizon",
			"component", "ouroboros",
			"slot", blockSlot,
			"connection_id", ctx.ConnectionId.String(),
		)
		o.scheduleHeaderResync(
			ctx.ConnectionId,
			time.Now(),
			event.ChainsyncResyncReasonForecastHorizonNoWork,
		)
		return false, nil
	}
	if errors.Is(err, ledger.ErrChainsyncHeaderDisconnected) {
		// Blocks the node already has cannot connect this header, so no wait
		// can release it. Return rather than hold the protocol until its state
		// timer closes the connection, and restart from the local chain.
		o.config.Logger.Debug(
			"chainsync: header does not connect to the local chain",
			"component", "ouroboros",
			"slot", blockSlot,
			"connection_id", ctx.ConnectionId.String(),
		)
		o.scheduleHeaderResync(
			ctx.ConnectionId,
			time.Now(),
			event.ChainsyncResyncReasonDisconnectedHeaderAdmission,
		)
		return false, nil
	}
	if err != nil {
		o.config.Logger.Warn(
			"chainsync: future-header admission failed closed",
			"component", "ouroboros",
			"slot", blockSlot,
			"connection_id", ctx.ConnectionId.String(),
			"error", err,
		)
		return false, fmt.Errorf("chainsync: future-header admission: %w", err)
	}
	if !accepted {
		// Dropping the header without turning ambiguous local/remote clock
		// skew into a connection penalty. Re-intersect at the earliest dropped
		// header's onset so the protocol cursor cannot permanently strand the
		// accepted chain.
		if o.chainsyncHeaderSlotTime != nil {
			onset, onsetErr := o.chainsyncHeaderSlotTime(blockSlot)
			if onsetErr == nil {
				o.scheduleFutureHeaderResync(ctx.ConnectionId, onset)
			} else {
				o.config.Logger.Error(
					"chainsync: failed to schedule future-header recovery",
					"component", "ouroboros",
					"slot", blockSlot,
					"connection_id", ctx.ConnectionId.String(),
					"error", onsetErr,
				)
			}
		}
		return false, nil
	}
	if o.futureHeaderResyncPending(ctx.ConnectionId) {
		o.config.Logger.Debug(
			"chainsync: header withheld pending future-header re-intersection",
			"component", "ouroboros",
			"slot", blockSlot,
			"connection_id", ctx.ConnectionId.String(),
		)
		return false, nil
	}
	return true, nil
}

// shouldPublishChainsyncToLedger reports whether headers from connId should
// feed the ledger / chain selector. When ChainsyncIngressEligible is wired,
// it is always authoritative. When no policy is wired we fall back to the
// tracked client's recorded direction: outbound-started chainsync defaults
// to eligible (legacy behaviour) while inbound-started chainsync defaults
// to observability-only. The inbound default is intentionally fail-closed
// so a misconfigured node cannot accept chain headers from random inbound
// peers that happen to negotiate full-duplex.
func (o *Ouroboros) shouldPublishChainsyncToLedger(
	connId ouroboros.ConnectionId,
) bool {
	if o.config.ChainsyncIngressEligible != nil {
		return o.config.ChainsyncIngressEligible(connId)
	}
	if o.chainsyncState == nil {
		return false
	}
	outbound, exists := o.chainsyncState.ClientStartedAsOutbound(connId)
	return exists && outbound
}

// shouldApplyChainsyncToLedger reports whether an ingress-eligible peer's
// headers/rollbacks may be APPLIED to the ledger. It is the second, stricter
// gate (see ChainsyncApplyEligible): it runs after the peer's tips have already
// been observed for chain selection, so an uncorroborated Genesis fast source is
// observed but its blocks are withheld. When no policy is wired, every ingress-
// eligible peer is apply-eligible.
func (o *Ouroboros) shouldApplyChainsyncToLedger(
	connId ouroboros.ConnectionId,
) bool {
	if o.config.ChainsyncApplyEligible == nil {
		return true
	}
	return o.config.ChainsyncApplyEligible(connId)
}

// isInboundChainsyncClient returns true if the chainsync client for
// connId was started on an inbound connection. This uses the tracked
// client's recorded direction instead of connmanager.IsInboundConnection,
// making it immune to ConnectionId collisions under listen-port reuse.
// Returns true (treat as inbound) if the client is not tracked.
//
//nolint:unused // Retained as a test helper and for future diagnostics.
func (o *Ouroboros) isInboundChainsyncClient(
	connId ouroboros.ConnectionId,
) bool {
	if o.chainsyncState == nil {
		return false
	}
	outbound, exists := o.chainsyncState.ClientStartedAsOutbound(connId)
	if !exists {
		// Unknown client — treat as inbound (conservative: don't
		// feed untracked connections into the ledger).
		return true
	}
	return !outbound
}

func (o *Ouroboros) maxTrackedChainsyncClients() int {
	maxClients := defaultMaxChainsyncClients
	if o.chainsyncState != nil && o.chainsyncState.MaxClients() > 0 {
		maxClients = o.chainsyncState.MaxClients()
	}
	return maxClients
}

func (o *Ouroboros) registerTrackedChainsyncClient(
	connId ouroboros.ConnectionId,
	ingressEligible bool,
	startedAsOutbound bool,
) bool {
	if o.chainsyncState == nil {
		return false
	}
	if ingressEligible {
		if o.chainsyncState.TryAddClientConnIdWithDirection(
			connId,
			o.maxTrackedChainsyncClients(),
			startedAsOutbound,
		) {
			return true
		}
		if o.chainsyncState.HasClientConnId(connId) {
			o.chainsyncState.SetClientStartedAsOutbound(
				connId,
				startedAsOutbound,
			)
			observabilityOnly, exists := o.chainsyncState.ClientObservabilityOnly(
				connId,
			)
			if !exists {
				return false
			}
			if observabilityOnly {
				return o.reconcileChainsyncIngressAdmission(connId, true)
			}
			return true
		}
		// Every slot is taken. Track the peer as an observer rather than
		// leaving it without a client: the next admission reconcile promotes
		// it as soon as a slot frees, with no reconnect.
		return o.chainsyncState.TryAddObservedClientConnIdWithDirection(
			connId,
			startedAsOutbound,
		)
	}
	if o.chainsyncState.TryAddObservedClientConnIdWithDirection(
		connId,
		startedAsOutbound,
	) {
		return true
	}
	return o.reconcileChainsyncIngressAdmission(connId, false)
}

func (o *Ouroboros) reconcileChainsyncIngressAdmission(
	connId ouroboros.ConnectionId,
	desiredEligible bool,
) bool {
	if o.chainsyncState == nil {
		return desiredEligible
	}
	observabilityOnly, exists := o.chainsyncState.ClientObservabilityOnly(
		connId,
	)
	if !exists {
		return false
	}
	if desiredEligible {
		if !observabilityOnly {
			return true
		}
		if !o.chainsyncState.SetClientObservabilityOnly(connId, false) {
			return false
		}
		observabilityOnly, exists = o.chainsyncState.ClientObservabilityOnly(
			connId,
		)
		return exists && !observabilityOnly
	}
	if !observabilityOnly {
		_ = o.chainsyncState.SetClientObservabilityOnly(connId, true)
	}
	return false
}

// updateChainsyncMetrics calculates and updates ChainSync performance metrics
// for the given peer connection. This is called on each RollForward event.
func (o *Ouroboros) updateChainsyncMetrics(
	connId ouroboros.ConnectionId,
	peerTip ochainsync.Tip,
) {
	if o.peerGov == nil || o.ledgerState == nil {
		return
	}

	now := time.Now()

	// Get or create stats for this connection
	o.chainsyncMutex.Lock()
	stats, exists := o.chainsyncStats[connId]
	if !exists {
		stats = &chainsyncPeerStats{
			lastObservationTime: now,
			headerCount:         0,
		}
		o.chainsyncStats[connId] = stats
	}

	// Increment header count
	stats.headerCount++

	// Calculate header rate over the observation period
	// We update the peer score periodically (at least 1 second between updates)
	// to avoid excessive computation on every header
	elapsed := now.Sub(stats.lastObservationTime)
	if elapsed < time.Second {
		o.chainsyncMutex.Unlock()
		return
	}

	// Calculate headers per second
	headerRate := float64(stats.headerCount) / elapsed.Seconds()

	// Reset counters for next observation period
	stats.headerCount = 0
	stats.lastObservationTime = now
	o.chainsyncMutex.Unlock()

	// Calculate tip delta (our tip slot - peer's tip slot)
	// Positive means peer is behind us, negative means peer is ahead
	ourTip := o.ledgerState.Tip()
	// Use signed subtraction to handle the delta correctly
	// Slots are uint64, but the difference fits in int64 for reasonable cases
	// Cap at math.MaxInt64 to avoid overflow
	var tipDelta int64
	if ourTip.Point.Slot >= peerTip.Point.Slot {
		diff := ourTip.Point.Slot - peerTip.Point.Slot
		if diff > math.MaxInt64 {
			tipDelta = math.MaxInt64
		} else {
			tipDelta = int64(diff) //nolint:gosec // overflow handled above
		}
	} else {
		diff := peerTip.Point.Slot - ourTip.Point.Slot
		if diff > math.MaxInt64 {
			tipDelta = math.MinInt64
		} else {
			tipDelta = -int64(diff) //nolint:gosec // overflow handled above
		}
	}

	// Update peer scoring
	o.peerGov.UpdatePeerChainSyncObservation(connId, headerRate, tipDelta)
}

// SubscribeChainsyncResync registers an EventBus subscriber that
// handles chainsync re-sync events. Resyncs close the affected
// connections after resetting local dedup state so peer governance
// reconnects with fresh intersect points. Local authoritative
// rollbacks additionally rewind tracked client cursors and attempt
// ledger-side recovery before recycling affected connections as a
// fallback.
func (o *Ouroboros) SubscribeChainsyncResync(ctx context.Context) {
	if o.eventBus == nil {
		return
	}
	o.subscribeTracked(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
			released := e.Reason == event.ChainsyncResyncReasonNetworkIngestReleased
			// The release re-intersects every client, so a resync requested
			// while held -- by a timer armed before the hold, say -- would
			// only close a peer the hold means to keep.
			if !released && o.networkIngestHeld() {
				o.config.Logger.Debug(
					"ignoring chainsync resync while network ingest is held",
					"component", "ouroboros",
					"connection_id", e.ConnectionId.String(),
					"reason", e.Reason,
				)
				return
			}
			var connIds []ouroboros.ConnectionId
			if e.ConnectionId != (ouroboros.ConnectionId{}) {
				connIds = append(connIds, e.ConnectionId)
			} else if o.chainsyncState != nil {
				connIds = o.chainsyncState.RewindTrackedClientsTo(e.Point)
				if e.Reason == event.ChainsyncResyncReasonLocalLedgerRollback &&
					len(connIds) == 0 {
					connIds = o.chainsyncState.GetClientConnIds()
				}
				if released {
					// Observers too: their cursor and chain-selection view
					// froze with the hold, and one may be promoted later.
					connIds = o.chainsyncState.GetClientConnIds()
				}
			}
			if o.chainsyncState != nil {
				if e.Point.Slot > 0 || len(e.Point.Hash) > 0 {
					o.chainsyncState.ClearSeenHeadersFrom(e.Point.Slot)
				} else {
					o.chainsyncState.ClearSeenHeaders()
				}
			}
			if e.Reason == event.ChainsyncResyncReasonLocalLedgerRollback {
				if o.ledgerState == nil {
					return
				}
				recovery := o.ledgerState.RecoverAfterLocalRollback(
					ctx,
					connIds,
					e.Point,
				)
				if recovery.Recovered || len(connIds) == 0 {
					return
				}
				if recovery.SkipConnectionClose {
					o.config.Logger.Info(
						"skipping connection closure: chain already past rollback point",
						"component",
						"ouroboros",
						"rollback_slot",
						e.Point.Slot,
						"chain_tip_slot",
						recovery.PrimaryChainTipSlot,
					)
					return
				}
				// No recoverable peer history — close the affected
				// connections so peer governance reconnects and starts
				// fresh chainsync from the updated intersect points.
				// Previous approach tried Stop→Start→Sync on each
				// connection, but Stop() blocks for up to 30s when the
				// protocol is in MustReply state (waiting for a server
				// response). All connections would timeout and be closed
				// anyway, wasting 30s during which stale events
				// accumulated and prevented recovery after reconnect.
				// RecoverAfterLocalRollback already cleaned up blockfetch
				// state, so there are no in-flight lookups to break.
				o.config.Logger.Info(
					"local rollback had no recoverable peer history, closing connections for fresh chainsync",
					"component",
					"ouroboros",
					"rollback_slot",
					e.Point.Slot,
					"connection_count",
					len(connIds),
				)
				for _, connId := range connIds {
					if o.chainsyncState != nil {
						o.chainsyncState.ClearObservedHeaderHistory(connId)
					}
					if o.connManager == nil {
						continue
					}
					conn := o.connManager.GetConnectionById(connId)
					if conn == nil {
						continue
					}
					o.config.Logger.Info(
						"closing connection for fresh chainsync after local rollback",
						"component",
						"ouroboros",
						"connection_id",
						connId.String(),
					)
					conn.Close()
				}
				return
			}
			if len(connIds) == 0 {
				return
			}
			// Close the connection rather than re-intersecting in place.
			// A live client always has RequestNext replies in flight, and
			// MsgFindIntersect sent over them leaves the protocol in
			// Intersect when those replies arrive, which tears the
			// connection down anyway. Closing lets peer governance
			// reconnect with a fresh bearer and updated intersect points.
			for _, connId := range connIds {
				o.denyDivergentChainsyncPeer(connId, e.Reason) //nolint:contextcheck // address normalization bounds its own DNS lookup
				if o.chainsyncState != nil {
					o.chainsyncState.ClearObservedHeaderHistory(connId)
				}
				if o.connManager == nil {
					continue
				}
				conn := o.connManager.GetConnectionById(connId)
				if conn == nil {
					continue
				}
				o.config.Logger.Info(
					"closing connection for fresh chainsync",
					"connection_id", connId.String(),
					"reason", e.Reason,
				)
				conn.Close()
			}
		},
	)
}

func (o *Ouroboros) instrumentChainsyncFindIntersect(
	fn func(ochainsync.CallbackContext, []ocommon.Point) (ocommon.Point, ochainsync.Tip, error),
	trackActivity bool,
) func(ochainsync.CallbackContext, []ocommon.Point) (ocommon.Point, ochainsync.Tip, error) {
	return func(
		ctx ochainsync.CallbackContext,
		points []ocommon.Point,
	) (ocommon.Point, ochainsync.Tip, error) {
		endActivity := o.beginTrustedNtCActivity(
			trackActivity,
			ctx.ConnectionId,
			ochainsync.ProtocolIdNtC,
		)
		defer endActivity()
		start := time.Now()
		p, t, err := fn(ctx, points)
		o.recordProtocolMessage("chainsync", err, time.Since(start))
		// The instrumented callback's error passes through unchanged.
		return p, t, err
	}
}

// instrumentChainsyncRequestNext wraps the RequestNext callback. Note
// that chainsyncServerRequestNext does some synchronous work (initial
// rollback, AddClient bookkeeping) then dispatches the Next-block fetch
// to a goroutine. The metric outcome reflects only the synchronous path;
// errors during async block delivery are logged but not surfaced here.
func (o *Ouroboros) instrumentChainsyncRequestNext(
	fn func(ochainsync.CallbackContext) error,
) func(ochainsync.CallbackContext) error {
	return func(ctx ochainsync.CallbackContext) error {
		start := time.Now()
		err := fn(ctx)
		o.recordProtocolMessage("chainsync", err, time.Since(start))
		// The instrumented callback's error passes through unchanged.
		return err
	}
}

func (o *Ouroboros) instrumentChainsyncRollBackward(
	fn func(ochainsync.CallbackContext, ocommon.Point, ochainsync.Tip) error,
) func(ochainsync.CallbackContext, ocommon.Point, ochainsync.Tip) error {
	return func(
		ctx ochainsync.CallbackContext,
		point ocommon.Point,
		tip ochainsync.Tip,
	) error {
		start := time.Now()
		err := fn(ctx, point, tip)
		o.recordProtocolMessage("chainsync", err, time.Since(start))
		// The instrumented callback's error passes through unchanged.
		return err
	}
}

// decodeChainsyncHeader decodes a chain-sync block header, choosing the decoder
// by block type. On the Musashi prototype network, blocks tagged Conway (block
// type 7) carry the Leios header extension (leios_certified/leios_announcement)
// in place — a structurally extended Babbage header that gouroboros' strict
// Conway header decoder rejects. Decode those via the Dijkstra header path,
// which handles the trailing extension, so the strict Conway decoder that every
// real Conway network relies on is left untouched. All other networks and block
// types decode exactly as before.
func (o *Ouroboros) decodeChainsyncHeader(
	blockType uint,
	raw []byte,
) (header gledger.BlockHeader, err error) {
	defer func() {
		if err == nil && header != nil {
			header.Hash()
		}
	}()
	if o.config.NetworkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic &&
		blockType == gledger.BlockTypeConway {
		return gdijkstra.NewDijkstraBlockHeaderFromCbor(raw)
	}
	header, err = gledger.NewBlockHeaderFromCbor(blockType, raw)
	if err == nil || blockType != gledger.BlockTypeByronEbb {
		// The caller wraps decode errors with the block type.
		return header, err
	}
	// Some dingo peers have sent a complete Byron EBB in the NtN header
	// payload. A complete EBB is an array of its header, body, and extra data,
	// so the header decoder rejects it at ConsensusData. Decode that one legacy
	// representation as a block and return its header; the regular path above
	// remains the only path for all other block types and correctly encoded EBB
	// headers.
	block, blockErr := gledger.NewBlockFromCbor(blockType, raw)
	if blockErr != nil {
		// Report the header decode error; the EBB fallback is only a retry.
		return nil, err
	}
	return block.Header(), nil
}

// chainsyncClientRollForwardRaw decodes the raw header itself (via
// decodeChainsyncHeader, through the shared decode cache) and forwards the
// decoded header to the shared RollForward handler. dingo takes the raw
// callback so it can apply the Musashi-scoped Conway-with-Leios-header
// decode; using the decoded callback would let gouroboros' strict decode fail
// before dingo can intervene.
//
// Every chainsync-connected peer delivers a header for each new point at
// roughly the same time, so -- like blockfetchClientBlockRaw -- the decode is
// keyed by content hash and shared across connections instead of repeated
// once per connection.
func (o *Ouroboros) chainsyncClientRollForwardRaw(
	ctx ochainsync.CallbackContext,
	blockType uint,
	blockData []byte,
	tip ochainsync.Tip,
) error {
	// A held header is dropped, so it is not worth decoding.
	if o.recordHeldChainsyncTip(ctx.ConnectionId, tip) {
		return ochainsync.ErrStopSyncProcess
	}
	// Record arrival at the raw network callback boundary. Header decoding can
	// block behind another peer's in-flight decode of the same bytes, so a
	// timestamp taken by the decoded handler would already include local work.
	arrivalNow := o.chainsyncArrivalNow
	if arrivalNow == nil {
		arrivalNow = time.Now
	}
	arrivalTime := arrivalNow()
	key := hashDecodeInput(blockType, blockData)
	cacheBytes := decodeCacheChargeForRaw(len(blockData))
	if !hasCborArrayEnvelope(blockData) {
		cacheBytes = int(^uint(0) >> 1)
	}
	header, err := decodeWithPanicSafeMetricsSized(
		o.headerDecodeCache,
		key,
		cacheBytes,
		true,
		func() (gledger.BlockHeader, error) {
			return o.decodeChainsyncHeader(blockType, blockData)
		},
		o.recordHeaderDecodeCacheOutcome,
	)
	if err != nil {
		return fmt.Errorf(
			"decode chain-sync header (block type %d): %w",
			blockType,
			err,
		)
	}
	if header == nil {
		// decodeCache's contract is (nil value, non-nil err) on failure, but
		// that is a convention on decodeFn, not something the generic cache
		// itself enforces -- guard explicitly rather than trust it silently.
		return fmt.Errorf(
			"decode chain-sync header (block type %d): decoded nil header with no error",
			blockType,
		)
	}
	return o.chainsyncClientRollForwardAt(
		ctx,
		blockType,
		header,
		tip,
		arrivalTime,
	)
}

func (o *Ouroboros) instrumentChainsyncRollForwardRaw(
	fn func(ochainsync.CallbackContext, uint, []byte, ochainsync.Tip) error,
) func(ochainsync.CallbackContext, uint, []byte, ochainsync.Tip) error {
	return func(
		ctx ochainsync.CallbackContext,
		blockType uint,
		blockData []byte,
		tip ochainsync.Tip,
	) error {
		start := time.Now()
		err := fn(ctx, blockType, blockData, tip)
		o.recordProtocolMessage("chainsync", err, time.Since(start))
		// The instrumented callback's error passes through unchanged.
		return err
	}
}
