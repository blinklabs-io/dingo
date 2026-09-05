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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
)

// leiosFetchRequestContext returns a context bounding a single leios-fetch
// client request, derived from parent so a cancelled caller (shutdown, or a
// backfill whose ledger-side budget has elapsed) does not leave a request
// parked. gouroboros no longer applies a protocol-level timeout to the block
// and block-txs states, because a timeout there fires SendError and tears down
// the whole multiplexed connection (killing the chainsync/blockfetch on the
// same bearer). Each request must therefore carry its own deadline. When an
// attempt deadline is set (the by-point backfill path) the request is bounded
// by it; otherwise (tip-driven fetches) it falls back to
// leiosFetchResponseTimeout. On expiry the request returns a normal error and
// the caller fails over to another peer, leaving the shared connection intact.
//
// Expiry is not free: the gouroboros client abandons its request slot, which
// stays busy until the peer's late response drains it, so every later request
// on that connection reports ErrRequestSlotAbandoned. That is why the backfill
// classifies an abandoned slot as a dead connection rather than a cooldown
// (see classifyLeiosFetchFailure).
func leiosFetchRequestContext(
	parent context.Context,
	deadline time.Time,
) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if !deadline.IsZero() {
		return context.WithDeadline(parent, deadline)
	}
	return context.WithTimeout(
		parent,
		leiosFetchResponseTimeout,
	)
}

// leiosBackfillConnCursor rotates the starting connection across backfill
// requests so concurrent fetches spread over the available relay connections
// instead of contending on a single connection's fetch guard.
var leiosBackfillConnCursor atomic.Uint64

// leiosBackfillConnCooldown is how long the backfill connection selector skips a
// leios-fetch connection after a failed or timed-out fetch, so it prefers
// healthy connections instead of repeatedly retrying a stalled or flaky one. It
// only falls back to a cooled-down connection when no healthy connection is
// available, so every connection is still eventually tried.
const leiosBackfillConnCooldown = 20 * time.Second

// leiosBackfillConnCooldownMax caps the escalated per-connection backfill
// cooldown so a persistently-failing connection is deprioritized aggressively
// but its cooldown never grows without bound (it is still eventually retried
// when no healthy connection is available).
const leiosBackfillConnCooldownMax = 5 * time.Minute

// leiosBackfillConnCooldownMaxShift bounds the exponential cooldown escalation
// (base << shift) so the shift can never overflow the duration; the cap above
// is reached well before this bound (20s << 4 = 320s > 5m), so this is only a
// safety ceiling on the shift amount.
const leiosBackfillConnCooldownMaxShift = 5

// leiosBackfillPerAttemptTimeout bounds how long a single by-point backfill
// attempt on one connection may run before it is abandoned so
// FetchEndorserBlockByPoint can fail over to another connection. Without it, a
// slow-but-alive relay that keeps dribbling transactions within the leios-fetch
// protocol per-message timeout (so that timeout never fires) parks the whole
// ledger apply loop on one peer for minutes (issue #2819). It is deliberately
// well under the ledger-side leiosBackfillMaxWait (2m) so several connections
// can be tried within one await window, yet comfortably above the few seconds a
// legitimately large endorser block takes to serve, so a healthy fetch is never
// cut short.
const leiosBackfillPerAttemptTimeout = 30 * time.Second

// leiosBackfillTotalBudget bounds one FetchEndorserBlockByPoint call when the
// caller supplies no deadline of its own. It matches the ledger-side
// leiosBackfillMaxWait so a caller that waits for this fetch and a caller that
// bounds it agree on how long a single by-point fetch may run.
const leiosBackfillTotalBudget = 2 * time.Minute

// leiosBackfillConnDeclineCooldown is the cooldown for a peer that answered a
// by-point request promptly and correctly with a typed decline (MsgNoBlock /
// MsgNoBlockTxs). Such a peer is healthy, it simply does not hold this endorser
// block (or not yet all of its transactions), so it is only briefly
// deprioritized -- it must stay a candidate for every other endorser block.
// Contrast leiosBackfillConnCooldown, which is for a peer that stalled or
// served wrong bytes.
//
// It is applied through markFetchDeclined, not markFetchFailed: a full,
// well-formed protocol round trip is evidence that the connection works, so it
// must not feed the consecutive-failure escalation that grows the cooldown to
// leiosBackfillConnCooldownMax. Repeatedly asking a small peer set for endorser
// blocks it does not hold would otherwise sideline every honest peer for five
// minutes.
const leiosBackfillConnDeclineCooldown = 2 * time.Second

var errLeiosBackfillConnBusy = errors.New(
	"leios backfill: connection fetch already in progress",
)

// errLeiosEndorserBlockDeclinedByAllPeers wraps the failure of a by-point fetch
// in which every attempted leios-fetch peer answered with a typed decline. It
// distinguishes "no connected peer holds this endorser block" (the network has
// the data or it is genuinely gone; retrying sooner will not help) from "our
// connections are stalling or broken", which is what the ledger's certified
// closure error otherwise looks like in a field log.
var errLeiosEndorserBlockDeclinedByAllPeers = errors.New(
	"leios backfill: endorser block declined by every leios-fetch peer",
)

// leiosFetchFailureClass is how a failed by-point fetch attempt on one
// connection is classified. dingo previously folded every outcome into one
// undifferentiated error with one cooldown, which meant a momentarily busy
// connection, a peer that does not hold the block, and a connection whose
// leios-fetch protocol can never answer again were all treated identically --
// so the one case that needs the connection replaced instead got a cooldown and
// was retried forever (dingo #3552).
type leiosFetchFailureClass int

const (
	// leiosFetchFailureNone is a successful attempt.
	leiosFetchFailureNone leiosFetchFailureClass = iota
	// leiosFetchFailureBusy means another fetch held this connection's guard.
	// Not a peer fault and not even an attempt: no cooldown, no failover
	// weight, and the connection stays a first-class candidate.
	leiosFetchFailureBusy
	// leiosFetchFailureDeclined means the peer answered the manifest request
	// promptly with MsgNoBlock: it does not hold this endorser block at all.
	// The peer is healthy, and this is a definitive answer about the block.
	leiosFetchFailureDeclined
	// leiosFetchFailureTxsUnavailable means the peer answered the transaction
	// request with MsgNoBlockTxs. The peer is healthy, but unlike MsgNoBlock
	// this is NOT a definitive answer about whether it holds the endorser
	// block: dingo's own leios-fetch server sends MsgNoBlockTxs both when it
	// has no manifest for the point and when it holds the manifest with a
	// still-incomplete transaction cache (leiosfetchServerBlockTxsRequest in
	// ouroboros/leiosfetch.go), and the wire message carries no reason. Ordinary
	// in-progress diffusion is therefore indistinguishable from absence, so this
	// is retryable and is never counted as "no peer holds this endorser block".
	leiosFetchFailureTxsUnavailable
	// leiosFetchFailureDead means this connection's leios-fetch protocol cannot
	// complete any further request. The gouroboros client's request slot is
	// left busy-and-abandoned when a request's context expires before the peer
	// answers, and only that late response can drain it, so every subsequent
	// request returns ErrRequestSlotAbandoned. No cooldown can repair it: the
	// connection must be replaced.
	leiosFetchFailureDead
	// leiosFetchFailureTransient is everything else -- a stalled peer, a
	// deadline overrun, wrong or incomplete bytes. Escalating cooldown, retry
	// later.
	leiosFetchFailureTransient
)

// classifyLeiosFetchFailure maps a by-point fetch attempt's error to the
// failover weight it deserves.
func classifyLeiosFetchFailure(err error) leiosFetchFailureClass {
	switch {
	case err == nil:
		return leiosFetchFailureNone
	case errors.Is(err, errLeiosBackfillConnBusy):
		return leiosFetchFailureBusy
	case errors.Is(err, oleiosfetch.ErrRequestSlotAbandoned),
		errors.Is(err, protocol.ErrProtocolShuttingDown):
		return leiosFetchFailureDead
	case errors.Is(err, oleiosfetch.ErrBlockNotFound):
		return leiosFetchFailureDeclined
	case errors.Is(err, oleiosfetch.ErrBlockTxsNotFound):
		return leiosFetchFailureTxsUnavailable
	default:
		return leiosFetchFailureTransient
	}
}

// leiosBackfillAttemptBudget divides the remaining fetch budget across the
// connections still to be tried. Splitting it means a multi-peer failover still
// bounds each peer at leiosBackfillPerAttemptTimeout (issue #2819), while the
// last remaining candidate -- the normal case on a topology with a single Leios
// relay -- gets the whole remainder instead of having its only attempt truncated
// at 30s with nothing to fail over to, which is what turned a slow relay into a
// permanent wedge (dingo #3552).
func leiosBackfillAttemptBudget(
	remaining time.Duration,
	candidatesLeft int,
) time.Duration {
	if remaining <= 0 {
		return 0
	}
	if candidatesLeft < 1 {
		candidatesLeft = 1
	}
	share := remaining / time.Duration(candidatesLeft)
	share = max(share, leiosBackfillPerAttemptTimeout)
	return min(share, remaining)
}

// leiosBackfillAffinityWindow is how recently a connection must have served a
// backfill fetch to be preferred (positive peer affinity) over never-tried
// connections. It complements the per-connection failure cooldown: cooldown
// pushes recently-failed connections to the back, affinity pulls
// recently-succeeded ones to the front. It is generous because a connection that
// served an endorser block is known-good; the preference only reorders the
// attempt sequence, never excludes a connection.
const leiosBackfillAffinityWindow = 2 * time.Minute

// FetchEndorserBlockByPoint fetches the endorser block identified by
// (ebSlot, ebHash) -- its manifest and all transaction bodies -- over
// leios-fetch and caches it, so EndorserBlockTxsByHash subsequently returns it.
//
// Unlike the tip path (which waits for the relay to diffuse an endorser block
// it is already pushing), this requests the block by point. The prototype relay
// serves any endorser block by point on demand (MsgLeiosBlockRequest /
// MsgLeiosBlockTxsRequest), including deeply historical ones, so this backfills
// the endorser-resident outputs of older ranking blocks during catch-up rather
// than leaving the UTxO set incomplete and trusting the chain. It satisfies
// ledger.EndorserBlockFetcherFunc.
//
// ctx bounds the whole call: every connection attempt, and the gap between
// them, come out of one budget (the caller's deadline, or
// leiosBackfillTotalBudget when it has none), so a caller that also waits for
// the result cannot be outlived by the fetch it started.
//
// Each attempt's outcome is classified (classifyLeiosFetchFailure) rather than
// folded into one cooldown:
//
//   - busy: another fetch holds this connection's guard. Not an attempt; the
//     connection keeps its place and its budget share is not consumed.
//   - declined: the peer answered the manifest request with MsgNoBlock.
//     Healthy peer, brief fixed cooldown. If every candidate resolved and every
//     attempted peer declined, the returned error wraps
//     errLeiosEndorserBlockDeclinedByAllPeers so the ledger can say "no
//     connected peer holds this endorser block" instead of reporting an
//     undiagnosed unavailability.
//   - txs unavailable: the peer answered the transaction request with
//     MsgNoBlockTxs, which does not distinguish absence from in-progress
//     diffusion. Healthy peer, same brief fixed cooldown, but it never
//     contributes to the all-declined verdict.
//   - dead: this connection's leios-fetch request slot is permanently
//     abandoned. A cooldown cannot repair it, so the connection is recycled
//     (one request per connection) and ordered last; the replacement dialled by
//     peer governance is what makes failover real (dingo #3552).
//   - transient: escalating per-connection cooldown, as before.
func (o *Ouroboros) FetchEndorserBlockByPoint(
	ctx context.Context,
	ebSlot uint64,
	ebHash []byte,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// The caller's point comes from the ranking block being applied, so it is
	// authoritative for this endorser block's slot. Reconcile any entry cached
	// from a peer offer before an announcement corroborated it: a matching
	// entry is promoted (and published) here, a contradicting one is evicted so
	// the fetch below replaces it rather than serving a poisoned slot to the
	// ledger (issue #3513).
	if publish := o.bindLeiosEndorserBlockSlot(ebHash, ebSlot); publish != nil {
		publish()
	}
	// The lookup is keyed by (slot, hash): loadLeiosEBFromDB's blob reload
	// only satisfies this specific occurrence when its persisted slot
	// actually matches ebSlot, so a stale reload of a different occurrence
	// cannot satisfy this check (issue #3513 review).
	if data, ok := o.lookupLeiosEndorserBlock(ebSlot, ebHash); ok &&
		data.completeTxCache() && data.slotVerified {
		return nil
	}
	if o.connManager == nil {
		return errors.New("leios backfill: no connection manager")
	}
	connIds := o.connManager.LeiosFetchConnectionIds()
	if len(connIds) == 0 {
		return errors.New("leios backfill: no leios-fetch connection available")
	}
	overall, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		overall = time.Now().Add(leiosBackfillTotalBudget)
	}
	point := ocommon.Point{Slot: ebSlot, Hash: ebHash}
	//nolint:gosec // bounded by len(connIds), so it fits in int
	start := int(leiosBackfillConnCursor.Add(1) % uint64(len(connIds)))
	// Order the connections for this attempt: recently-successful ones first
	// (positive affinity), then other healthy ones, then ones cooling down from a
	// recent failed fetch, then ones whose leios-fetch protocol is dead, each
	// partition kept in round-robin order so concurrent backfills still spread. A
	// cooled or dead connection is still eventually tried, so a transiently-flaky
	// one is skipped, not starved.
	order := leiosBackfillConnOrder(
		connIds,
		start,
		time.Now(),
		leiosBackfillAffinityWindow,
		o.leiosFetchGuardFor,
	)
	var lastErr error
	attempted := 0
	declined := 0
	// unresolved records that at least one candidate never answered the block
	// query definitively: it was busy with another fetch, had no usable
	// leios-fetch client, was never reached because the budget or the caller's
	// context ended first, or answered MsgNoBlockTxs (which does not
	// distinguish absence from in-progress diffusion -- see
	// leiosFetchFailureTxsUnavailable). The all-peers-declined verdict below is
	// withheld in that case: operators act on it as "no connected peer holds
	// this endorser block", and a candidate that never answered is not evidence
	// of that.
	unresolved := false
	remainingCandidates := len(order)
	for _, connId := range order {
		remainingCandidates--
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			// This candidate and every one after it goes unqueried.
			unresolved = true
			break
		}
		conn := o.connManager.GetConnectionById(connId)
		if conn == nil || conn.LeiosFetch() == nil ||
			conn.LeiosFetch().Client == nil {
			unresolved = true
			continue
		}
		budget := leiosBackfillAttemptBudget(
			time.Until(overall),
			remainingCandidates+1,
		)
		if budget <= 0 {
			if lastErr == nil {
				lastErr = errors.New("leios backfill: fetch budget exhausted")
			}
			// This candidate and every one after it goes unqueried.
			unresolved = true
			break
		}
		// fetchEndorserBlockOnConn records the cooldown outcome
		// (markFetchFailed/markFetchOK) itself, under the connection's fetch
		// guard, so concurrent backfill fetches on the same connection publish
		// their cooldown state in fetch-completion order. Doing it here, after
		// the guard is released, would let a slow failure's mark land after a
		// newer success's mark and wrongly cool down a healthy connection.
		err := o.fetchEndorserBlockOnConn(
			ctx,
			connId,
			conn.LeiosFetch().Client,
			point,
			budget,
		)
		// The recycle request is published here, with the connection's fetch
		// guard already released, so no lock is held across an event-bus
		// publish.
		if classifyLeiosFetchFailure(err) == leiosFetchFailureDead &&
			o.leiosFetchGuardFor(connId).takeRecycleEvent() {
			o.requestLeiosFetchConnRecycle(connId, point, err)
		}
		if err != nil {
			lastErr = err
			switch classifyLeiosFetchFailure(err) {
			case leiosFetchFailureNone:
				// classifyLeiosFetchFailure returns None only for nil errors;
				// keep the explicit arm for the repository's exhaustive-switch check.
				continue
			case leiosFetchFailureBusy:
				// Not an attempt: the connection was serving another fetch, so
				// this peer never answered the query for this endorser block.
				unresolved = true
			case leiosFetchFailureDeclined:
				attempted++
				declined++
			case leiosFetchFailureTxsUnavailable:
				// A real attempt, but MsgNoBlockTxs is not evidence that the
				// peer lacks the block.
				attempted++
				unresolved = true
			case leiosFetchFailureDead, leiosFetchFailureTransient:
				// A dead or transient failure is a real attempt that said
				// nothing about what this peer holds. Named rather than
				// folded into a default so the exhaustive linter reports any
				// class added to the taxonomy later at this site instead of
				// letting it fall silently into the
				// attempted-but-uninformative bucket.
				attempted++
			}
			continue
		}
		if data, ok := o.lookupLeiosEndorserBlock(ebSlot, ebHash); ok &&
			data.completeTxCache() && data.slotVerified {
			return nil
		}
		attempted++
		lastErr = errors.New(
			"leios backfill: fetch completed but cache incomplete",
		)
	}
	if lastErr == nil {
		lastErr = errors.New("leios backfill: fetch failed")
	}
	if attempted > 0 && declined == attempted && !unresolved {
		return fmt.Errorf(
			"%w: %d peer(s): %w",
			errLeiosEndorserBlockDeclinedByAllPeers,
			declined,
			lastErr,
		)
	}
	return lastErr
}

// requestLeiosFetchConnRecycle asks the connection manager to close a
// connection whose leios-fetch protocol can no longer answer, so peer
// governance dials a replacement and the next by-point fetch has a usable
// bearer. Publishing the ledger-owned recycle event (the node translates it for
// connmanager) keeps this consistent with the chainsync verification-failure
// path. Exactly one request is published per connection.
//
// Called with no fetch guard held: fetchEndorserBlockOnConn reserves the event
// while holding the guard and this function publishes it after unlock.
func (o *Ouroboros) requestLeiosFetchConnRecycle(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	cause error,
) {
	if o.config.Logger != nil {
		o.config.Logger.Warn(
			"recycling connection with an unusable leios-fetch protocol",
			"component", "network",
			"protocol", "leios-fetch",
			"connection_id", connId.String(),
			"slot", point.Slot,
			"error", cause,
		)
	}
	if o.eventBus == nil {
		return
	}
	o.eventBus.Publish(
		ledger.ConnectionRecycleRequestedEventType,
		event.NewEvent(
			ledger.ConnectionRecycleRequestedEventType,
			ledger.ConnectionRecycleRequestedEvent{
				ConnectionId: connId,
				Reason:       "leios_fetch_request_slot_abandoned",
			},
		),
	)
}

// fetchEndorserBlockOnConn fetches the manifest (if not already cached) and all
// transaction bodies for point on a single connection, holding that
// connection's fetch guard so the strict request/response leios-fetch client is
// never used concurrently with a tip-driven fetch. A connection whose guard is
// already held is skipped so time queued behind that fetch cannot consume an
// unbounded amount of the caller's failover window. It records the connection's
// cooldown outcome while the guard is still held, so backfill fetches on the
// same connection publish their cooldown state in fetch-completion order rather
// than racing; which cooldown depends on the failure class
// (classifyLeiosFetchFailure), so a peer that merely does not hold the block is
// not penalized like one that stalled.
//
// budget bounds this one connection's attempt; the caller allocates it from the
// whole call's remaining budget.
func (o *Ouroboros) fetchEndorserBlockOnConn(
	ctx context.Context,
	connId ouroboros.ConnectionId,
	client *oleiosfetch.Client,
	point ocommon.Point,
	budget time.Duration,
) (err error) {
	g := o.leiosFetchGuardFor(connId)
	// The strict leios-fetch client cannot accept a second request while a
	// tip-driven or backfill fetch is in progress. Do not wait here: the
	// per-attempt deadline below would otherwise start only after the wait and
	// failover could stall indefinitely behind the existing fetch. Busy is not a
	// peer failure, so return before installing the cooldown outcome defer.
	if !g.mu.TryLock() {
		return errLeiosBackfillConnBusy
	}
	defer g.mu.Unlock()
	// Bound this connection's attempt so a slow-but-alive relay cannot park the
	// whole backfill on one peer (issue #2819); on expiry the tx fetch returns a
	// deadline error, this attempt is marked failed, and FetchEndorserBlockByPoint
	// moves on to the next connection. Busy connections are skipped above, so the
	// deadline can cover only serving time without leaving lock acquisition
	// unbounded.
	deadline := time.Now().Add(budget)
	// Runs before the deferred Unlock above (LIFO), so the cooldown state is
	// published while the guard is still held and stays ordered with the fetch.
	defer func() {
		// Caller cancellation is not evidence of a peer failure. In particular,
		// ledger shutdown and apply cancellation must not cool down a healthy
		// connection.
		if errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
			return
		}
		switch classifyLeiosFetchFailure(err) {
		case leiosFetchFailureNone:
			g.markFetchOK()
		case leiosFetchFailureDeclined, leiosFetchFailureTxsUnavailable:
			// The peer completed a full protocol round trip and simply does not
			// hold this block (or not yet all of its transactions). That is
			// evidence the connection works, so it takes a fixed short cooldown
			// and does not feed the consecutive-failure escalation: it must stay
			// a candidate for every other endorser block instead of being
			// sidelined for minutes for answering honestly.
			g.markFetchDeclined(
				time.Now(),
				leiosBackfillConnDeclineCooldown,
			)
		case leiosFetchFailureDead:
			// The cooldown is immaterial (the connection is being recycled) but
			// it keeps this connection last in the ordering until it is gone.
			g.markFetchFailed(
				time.Now(),
				leiosBackfillConnCooldownMax,
			)
			if g.markProtocolDead() {
				g.recycleEventPending.Store(true)
			}
		case leiosFetchFailureBusy, leiosFetchFailureTransient:
			// A stalled peer, a deadline overrun, wrong or incomplete bytes.
			// leiosFetchFailureBusy cannot occur here (the TryLock guard above
			// returns before this defer is installed) and has no behavior of
			// its own; it is named rather than folded into a default for the
			// same reason as in FetchEndorserBlockByPoint.
			g.markFetchFailed(time.Now(), leiosBackfillConnCooldown)
		}
	}()
	// Keyed by (slot, hash): a cached or blob-reloaded entry for a different
	// occurrence of this hash lives under its own key and is simply not
	// found here, so it cannot be mistaken for this attempt's authoritative
	// point (issue #3513 review).
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	if !ok {
		reqCtx, cancel := leiosFetchRequestContext(ctx, deadline)
		resp, err := client.BlockRequest(reqCtx, point)
		cancel()
		if err != nil {
			return fmt.Errorf("manifest fetch: %w", err)
		}
		blk, ok := resp.(*oleiosfetch.MsgBlock)
		if !ok {
			return fmt.Errorf(
				"unexpected leios-fetch block response %T",
				resp,
			)
		}
		if err := o.storeLeiosEndorserBlock(
			point,
			blk.BlockRaw,
			nil,
			leiosStoreAuthoritative,
		); err != nil {
			return fmt.Errorf("store manifest: %w", err)
		}
		data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		if !ok || data == nil {
			return errors.New("manifest stored but not found in cache")
		}
	} else if !data.slotVerified {
		// The entry already exists -- e.g. cached from a peer offer that
		// raced ahead of its announcement -- but was never bound to an
		// authoritative slot. point is ledger-derived and authoritative for
		// this hash, and the bytes are already held, so bind it in place
		// rather than falling through: completeTxCache() below would
		// otherwise return nil on every connection this backfill tries
		// without any of them ever verifying the entry, since none would
		// take the !ok branch above (issue #3513 review).
		if publish := o.bindLeiosEndorserBlockSlot(point.Hash, point.Slot); publish != nil {
			publish()
		}
		data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
		if !ok || data == nil {
			return errors.New(
				"manifest evicted while binding to authoritative point",
			)
		}
	}
	if data.txCount == 0 || data.completeTxCache() {
		return nil
	}
	txs, err := o.fetchLeiosEbTxsBatchedUntil(
		ctx,
		client,
		point,
		data.txCount,
		data.blockRaw,
		deadline,
	)
	if err != nil {
		return fmt.Errorf(
			"tx fetch (%d/%d): %w",
			len(txs),
			data.txCount,
			err,
		)
	}
	if err := validateLeiosEndorserBlockTxs(data.blockRaw, txs); err != nil {
		return fmt.Errorf("validate tx references: %w", err)
	}
	if err := o.storeLeiosEndorserBlock(
		point,
		data.blockRaw,
		txs,
		leiosStoreAuthoritative,
	); err != nil {
		return fmt.Errorf("store txs: %w", err)
	}
	return nil
}

// leiosBackfillConnOrder orders connIds for a by-point backfill attempt.
// Connections that recently served an endorser block (positive affinity) come
// first, then other healthy connections, then connections cooling down from a
// recent failed fetch, then connections whose leios-fetch protocol has been
// diagnosed dead (their request slot is permanently abandoned, so an attempt can
// only burn the caller's grace period; they are still last-resort candidates
// rather than excluded, so a misdiagnosis cannot black out the backfill).
// Each partition preserves the round-robin order starting
// at start, so concurrent backfills still spread across proven peers rather than
// all hammering the single most-recent one (the prototype relay returns empty
// manifests / resets when hammered). Because a connection becomes "proven" after
// one success, the proven partition normally holds most healthy connections and
// the ordering is near-uniform; the transient case of one proven among many
// fresh resolves as the fresh ones each serve a fetch. guardFor returns the
// fetch guard for a connection.
func leiosBackfillConnOrder(
	connIds []ouroboros.ConnectionId,
	start int,
	now time.Time,
	affinityWindow time.Duration,
	guardFor func(ouroboros.ConnectionId) *leiosFetchGuard,
) []ouroboros.ConnectionId {
	n := len(connIds)
	if n == 0 {
		return nil
	}
	proven := make([]ouroboros.ConnectionId, 0, n)
	fresh := make([]ouroboros.ConnectionId, 0, n)
	cooled := make([]ouroboros.ConnectionId, 0, n)
	dead := make([]ouroboros.ConnectionId, 0, n)
	for off := range connIds {
		connId := connIds[(start+off)%n]
		g := guardFor(connId)
		switch {
		case g.isProtocolDead():
			dead = append(dead, connId)
		case g.inCooldown(now):
			cooled = append(cooled, connId)
		case g.recentlySucceeded(now, affinityWindow):
			proven = append(proven, connId)
		default:
			fresh = append(fresh, connId)
		}
	}
	order := make([]ouroboros.ConnectionId, 0, n)
	order = append(order, proven...)
	order = append(order, fresh...)
	order = append(order, cooled...)
	order = append(order, dead...)
	return order
}
