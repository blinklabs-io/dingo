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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
)

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
func (o *Ouroboros) FetchEndorserBlockByPoint(
	ebSlot uint64,
	ebHash []byte,
) error {
	if data, ok := o.lookupLeiosEndorserBlock(ebHash); ok &&
		data.completeTxCache() {
		return nil
	}
	if o.ConnManager == nil {
		return errors.New("leios backfill: no connection manager")
	}
	connIds := o.ConnManager.LeiosFetchConnectionIds()
	if len(connIds) == 0 {
		return errors.New("leios backfill: no leios-fetch connection available")
	}
	point := ocommon.Point{Slot: ebSlot, Hash: ebHash}
	//nolint:gosec // bounded by len(connIds), so it fits in int
	start := int(leiosBackfillConnCursor.Add(1) % uint64(len(connIds)))
	// Order the connections for this attempt: recently-successful ones first
	// (positive affinity), then other healthy ones, then ones cooling down from a
	// recent failed fetch, each partition kept in round-robin order so concurrent
	// backfills still spread. A cooled connection is still eventually tried, so a
	// transiently-flaky one is skipped, not starved.
	order := leiosBackfillConnOrder(
		connIds,
		start,
		time.Now(),
		leiosBackfillAffinityWindow,
		o.leiosFetchGuardFor,
	)
	var lastErr error
	for _, connId := range order {
		conn := o.ConnManager.GetConnectionById(connId)
		if conn == nil || conn.LeiosFetch() == nil ||
			conn.LeiosFetch().Client == nil {
			continue
		}
		// fetchEndorserBlockOnConn records the cooldown outcome
		// (markFetchFailed/markFetchOK) itself, under the connection's fetch
		// guard, so concurrent backfill fetches on the same connection publish
		// their cooldown state in fetch-completion order. Doing it here, after
		// the guard is released, would let a slow failure's mark land after a
		// newer success's mark and wrongly cool down a healthy connection.
		if err := o.fetchEndorserBlockOnConn(
			connId,
			conn.LeiosFetch().Client,
			point,
		); err != nil {
			lastErr = err
			continue
		}
		if data, ok := o.lookupLeiosEndorserBlock(ebHash); ok &&
			data.completeTxCache() {
			return nil
		}
		lastErr = errors.New(
			"leios backfill: fetch completed but cache incomplete",
		)
	}
	if lastErr == nil {
		lastErr = errors.New("leios backfill: fetch failed")
	}
	return lastErr
}

// fetchEndorserBlockOnConn fetches the manifest (if not already cached) and all
// transaction bodies for point on a single connection, holding that
// connection's fetch guard so the strict request/response leios-fetch client is
// never used concurrently with a tip-driven fetch. It records the connection's
// cooldown outcome (markFetchFailed on error, markFetchOK on success) while the
// guard is still held, so concurrent backfill fetches on the same connection
// publish their cooldown state in fetch-completion order rather than racing.
func (o *Ouroboros) fetchEndorserBlockOnConn(
	connId ouroboros.ConnectionId,
	client *oleiosfetch.Client,
	point ocommon.Point,
) (err error) {
	g := o.leiosFetchGuardFor(connId)
	g.mu.Lock()
	defer g.mu.Unlock()
	// Bound this connection's attempt so a slow-but-alive relay cannot park the
	// whole backfill on one peer (issue #2819); on expiry the tx fetch returns a
	// deadline error, this attempt is marked failed, and FetchEndorserBlockByPoint
	// moves on to the next connection. Measured from lock acquisition so it covers
	// only serving time on this connection, not time queued behind another fetch.
	deadline := time.Now().Add(leiosBackfillPerAttemptTimeout)
	// Runs before the deferred Unlock above (LIFO), so the cooldown state is
	// published while the guard is still held and stays ordered with the fetch.
	defer func() {
		if err != nil {
			g.markFetchFailed(time.Now(), leiosBackfillConnCooldown)
		} else {
			g.markFetchOK()
		}
	}()
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	if !ok {
		resp, err := client.BlockRequest(point)
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
		); err != nil {
			return fmt.Errorf("store manifest: %w", err)
		}
		if data, ok = o.lookupLeiosEndorserBlock(point.Hash); !ok {
			return errors.New("manifest stored but not found in cache")
		}
	}
	if data.txCount == 0 || data.completeTxCache() {
		return nil
	}
	txs, err := o.fetchLeiosEbTxsBatchedUntil(
		client,
		point,
		data.txCount,
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
	if err := o.storeLeiosEndorserBlock(point, data.blockRaw, txs); err != nil {
		return fmt.Errorf("store txs: %w", err)
	}
	return nil
}

// leiosBackfillConnOrder orders connIds for a by-point backfill attempt.
// Connections that recently served an endorser block (positive affinity) come
// first, then other healthy connections, then connections cooling down from a
// recent failed fetch. Each partition preserves the round-robin order starting
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
	for off := range connIds {
		connId := connIds[(start+off)%n]
		g := guardFor(connId)
		switch {
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
	return order
}
