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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
)

// leiosForgedEBEntry holds one locally-forged endorser block ready to
// be announced to peers via LeiosNotify.
type leiosForgedEBEntry struct {
	point ocommon.Point
}

// leiosForgedEBLog is an append-only log of locally-forged EBs with
// per-connection cursors owned by the log itself.
//
// Head entries are pruned whenever every registered connection's cursor
// has advanced past them, so memory scales with the largest per-connection
// backlog rather than total uptime. When no connections are registered the
// log is always empty. A new connection registers at the current tail and
// does not receive EBs forged before it connected. Connections are
// removed via removeConn, which triggers an immediate prune.
//
// The wake channel is closed and replaced on every append so all server
// goroutines waiting for new entries unblock at once.
type leiosForgedEBLog struct {
	mu      sync.Mutex
	items   []leiosForgedEBEntry
	base    int            // logical index of items[0]
	cursors map[string]int // connKey → next logical index to serve
	wakeCh  chan struct{}
}

func newLeiosForgedEBLog() *leiosForgedEBLog {
	return &leiosForgedEBLog{
		cursors: make(map[string]int),
		wakeCh:  make(chan struct{}),
	}
}

// append adds an entry, prunes head entries that all registered connections
// have advanced past (or all entries when none are registered), and signals
// all server goroutines waiting for new entries to wake and retry.
func (l *leiosForgedEBLog) append(entry leiosForgedEBEntry) {
	l.mu.Lock()
	l.items = append(l.items, entry)
	l.pruneLocked()
	wake := l.wakeCh
	l.wakeCh = make(chan struct{})
	l.mu.Unlock()
	close(wake)
}

// next returns the next unserved entry for connKey and the current wake
// channel. If no entry is available it returns (nil, wakeCh); the caller
// should wait on wakeCh and retry. A connKey that has never called next
// is registered at the current tail so it does not receive stale EBs.
func (l *leiosForgedEBLog) next(connKey string) (*leiosForgedEBEntry, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cursor, exists := l.cursors[connKey]
	if !exists {
		// New connection: start at the current tail.
		cursor = l.base + len(l.items)
		l.cursors[connKey] = cursor
	}
	idx := cursor - l.base
	if idx < len(l.items) {
		entry := l.items[idx]
		l.cursors[connKey] = cursor + 1
		l.pruneLocked()
		return &entry, l.wakeCh
	}
	return nil, l.wakeCh
}

// removeConn unregisters a connection cursor and prunes newly freed entries.
func (l *leiosForgedEBLog) removeConn(connKey string) {
	l.mu.Lock()
	delete(l.cursors, connKey)
	l.pruneLocked()
	l.mu.Unlock()
}

// registerConn pre-registers connKey at the current tail so that EBs
// appended between connection open and the peer's first RequestNext are
// not pruned before the cursor is established. It is a no-op when connKey
// is already registered (e.g. on reconnect within the same session).
func (l *leiosForgedEBLog) registerConn(connKey string) {
	l.mu.Lock()
	if _, exists := l.cursors[connKey]; !exists {
		l.cursors[connKey] = l.base + len(l.items)
	}
	l.mu.Unlock()
}

// leiosEBLogMaxEntries is the maximum number of forged-EB entries the log
// retains. When the log grows beyond this limit, the oldest entries are
// evicted and any lagging cursors are advanced to the new base. This
// bounds memory even when a pre-registered or slow peer never calls next.
const leiosEBLogMaxEntries = 64

// pruneLocked drops head entries whose logical index falls below every
// registered connection's cursor (i.e. all connections have advanced past
// them, whether by consuming the entry or by registering after it). When
// no connections are registered the entire log is pruned. If the log still
// exceeds leiosEBLogMaxEntries after cursor-based pruning, the oldest
// entries are evicted and lagging cursors are advanced to the new base.
// Callers must hold l.mu.
func (l *leiosForgedEBLog) pruneLocked() {
	if len(l.items) == 0 {
		return
	}
	// Start at the tail: if no cursors constrain it, prune the full log.
	minCursor := l.base + len(l.items)
	for _, c := range l.cursors {
		if c < minCursor {
			minCursor = c
		}
	}
	prunable := minCursor - l.base
	// Size cap: if the log still exceeds leiosEBLogMaxEntries after
	// cursor-based pruning, evict the excess from the head. Any cursor
	// that falls behind the new base (e.g. a pre-registered idle peer)
	// is advanced to the new base so it does not pin future entries.
	if capped := len(l.items) - prunable - leiosEBLogMaxEntries; capped > 0 {
		prunable += capped
		newBase := l.base + prunable
		for k, c := range l.cursors {
			if c < newBase {
				l.cursors[k] = newBase
			}
		}
	}
	if prunable <= 0 {
		return
	}
	// Zero pruned slots so the GC can reclaim the point.Hash []byte
	// backing arrays before the backing slice is eventually reallocated.
	clear(l.items[:prunable])
	l.items = l.items[prunable:]
	l.base += prunable
}

// BroadcastEndorserBlock stores a locally-forged EB and notifies waiting
// LeiosNotify server goroutines so they can announce it to peers. txBodies are
// the referenced transactions' raw CBOR in manifest order; they are stored in
// the endorser block's tx cache so the EB can be served to peers over
// leios-fetch (completeTxCache() then holds and leiosfetchServerBlockTxsRequest
// can answer). It satisfies forging.EndorserBlockBroadcaster.
func (o *Ouroboros) BroadcastEndorserBlock(
	slot uint64,
	hash []byte,
	data []byte,
	txBodies [][]byte,
) error {
	point := ocommon.Point{Slot: slot, Hash: hash}
	// Match the on-the-wire form fetched EB transactions are stored in: each
	// transaction is a CBOR byte string wrapping its CBOR (LeiosTx =
	// encodeBytes(txCbor)), so served forged-EB bodies decode identically.
	var txsRaw []cbor.RawMessage
	if len(txBodies) > 0 {
		txsRaw = make([]cbor.RawMessage, 0, len(txBodies))
		for i, body := range txBodies {
			wrapped, err := cbor.Encode(body)
			if err != nil {
				return fmt.Errorf("encode forged EB tx %d: %w", i, err)
			}
			txsRaw = append(txsRaw, cbor.RawMessage(wrapped))
		}
	}
	if err := o.storeLeiosEndorserBlock(point, data, txsRaw); err != nil {
		return fmt.Errorf("store forged endorser block: %w", err)
	}
	o.leiosEBLog.append(leiosForgedEBEntry{point: point})
	return nil
}

func (o *Ouroboros) leiosnotifyServerConnOpts() []oleiosnotify.LeiosNotifyOptionFunc {
	return []oleiosnotify.LeiosNotifyOptionFunc{
		oleiosnotify.WithRequestNextFunc(
			o.instrumentLeiosnotifyRequestNext(o.leiosnotifyServerRequestNext),
		),
	}
}

func (o *Ouroboros) leiosnotifyClientConnOpts() []oleiosnotify.LeiosNotifyOptionFunc {
	return []oleiosnotify.LeiosNotifyOptionFunc{
		oleiosnotify.WithNotificationFunc(
			o.instrumentLeiosnotifyNotification(o.leiosnotifyClientNotification),
		),
		// Disable the Busy-state timeout. LeiosNotify is a push-based
		// notification protocol where the server only sends when it has
		// something to announce. Idle waits of arbitrary length are
		// normal and should not kill the connection.
		oleiosnotify.WithTimeout(time.Duration(0)),
	}
}

func (o *Ouroboros) leiosnotifyClientStart(connId ouroboros.ConnectionId) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf(
			"failed to lookup connection ID: %s",
			leiosConnectionIdString(connId),
		)
	}
	if conn.LeiosNotify() == nil {
		// Peer does not support LeiosNotify; skip cursor registration.
		return nil
	}
	connKey := leiosConnectionIdString(connId)
	// Pre-register the server-side cursor now that we know the peer
	// supports LeiosNotify. This ensures EBs forged between here and
	// the peer's first RequestNext are not pruned.
	o.leiosEBLog.registerConn(connKey)
	if err := conn.LeiosNotify().Client.Sync(); err != nil {
		o.leiosEBLog.removeConn(connKey)
		return err
	}
	return nil
}

func leiosConnectionIdString(connId ouroboros.ConnectionId) string {
	if connId.LocalAddr == nil || connId.RemoteAddr == nil {
		return "<unknown>"
	}
	return connId.String()
}

func (o *Ouroboros) instrumentLeiosnotifyNotification(
	fn func(oleiosnotify.CallbackContext, protocol.Message) error,
) func(oleiosnotify.CallbackContext, protocol.Message) error {
	return func(ctx oleiosnotify.CallbackContext, msg protocol.Message) error {
		start := time.Now()
		err := fn(ctx, msg)
		o.recordProtocolMessage("leiosnotify", err, time.Since(start))
		return err
	}
}

func (o *Ouroboros) instrumentLeiosnotifyRequestNext(
	fn func(oleiosnotify.CallbackContext) (protocol.Message, error),
) func(oleiosnotify.CallbackContext) (protocol.Message, error) {
	return func(ctx oleiosnotify.CallbackContext) (protocol.Message, error) {
		start := time.Now()
		msg, err := fn(ctx)
		o.recordProtocolMessage("leiosnotify", err, time.Since(start))
		return msg, err
	}
}

// leiosTipPrefetchMaxLagSlots is how far behind the wall-clock head the applied
// ledger may be before dingo stops prefetching endorser blocks offered over
// leios-notify. Notify offers describe endorser blocks at the live head; while
// the ledger is replaying a deep backlog those blocks would expire from the
// endorser-block cache (10 minute TTL, ~600 slots at 1s slots) long before the
// ledger reaches them, and prefetching them only starves the chain-driven
// historical backfill for the relay's few connections. While behind, the ledger
// fetches the endorser block each ranking block references by point as it
// applies the chain, matching the prototype's ranking-block-driven fetch.
const leiosTipPrefetchMaxLagSlots = 600

// leiosTipPrefetchEnabled reports whether the node is caught up enough that
// prefetching a head endorser block offered over leios-notify is worthwhile (it
// will be applied before it expires from the cache). It is false during a deep
// catch-up so all fetch capacity serves the historical backfill.
func (o *Ouroboros) leiosTipPrefetchEnabled() bool {
	if o.LedgerState == nil {
		return true
	}
	return o.LedgerState.SlotsBehindHead() <= leiosTipPrefetchMaxLagSlots
}

func (o *Ouroboros) leiosnotifyClientNotification(
	ctx oleiosnotify.CallbackContext,
	msg protocol.Message,
) error {
	conn := o.ConnManager.GetConnectionById(ctx.ConnectionId)
	connId := leiosConnectionIdString(ctx.ConnectionId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId)
	}
	switch m := msg.(type) {
	case *oleiosnotify.MsgBlockOffer:
		// While the ledger is deeply behind the head, do not prefetch this
		// head endorser block: it would expire before the ledger reaches it and
		// would starve the chain-driven historical backfill for connections. The
		// ledger fetches the endorser blocks it needs by point as it catches up.
		if !o.leiosTipPrefetchEnabled() {
			return nil
		}
		if conn.LeiosFetch() == nil || conn.LeiosFetch().Client == nil {
			return errors.New("leios-fetch client unavailable")
		}
		client := conn.LeiosFetch().Client
		point := m.Point
		// Fetch the manifest off the handler so a slow fetch cannot head-of-line
		// block later offers on this connection. The transactions arrive as a
		// separate notify offer (MsgBlockTxsOffer): the prototype diffuses an
		// endorser block's manifest and its transactions as two distinct offers,
		// and fetching the transactions before the txs-offer arrives makes the
		// relay reset the connection, so tx-body fetch is driven from the
		// txs-offer below. Failures are best-effort: a transient manifest fetch
		// error must not tear down the shared connection.
		o.dispatchLeiosFetch(ctx.ConnectionId, func() {
			resp, err := client.BlockRequest(point)
			if err != nil {
				o.config.Logger.Debug(
					"leios EB manifest fetch failed",
					"error", err,
					"connection_id", connId,
					"slot", point.Slot,
				)
				return
			}
			respBlock, ok := resp.(*leiosfetch.MsgBlock)
			if !ok {
				o.config.Logger.Debug(
					"unexpected leios-fetch Block response type",
					"type", fmt.Sprintf("%T", resp),
					"connection_id", connId,
					"slot", point.Slot,
				)
				return
			}
			if err := o.storeLeiosEndorserBlock(
				point,
				respBlock.BlockRaw,
				nil,
			); err != nil {
				o.config.Logger.Debug(
					"failed to store leios EB manifest",
					"error", err,
					"connection_id", connId,
					"slot", point.Slot,
				)
				return
			}
			txCount := 0
			if data, ok := o.lookupLeiosEndorserBlock(point.Hash); ok {
				txCount = data.txCount
			}
			o.config.Logger.Info(
				fmt.Sprintf(
					"fetched EB manifest %d.%x size %d txs %d",
					point.Slot,
					point.Hash,
					len(respBlock.BlockRaw),
					txCount,
				),
				"component", "network",
				"protocol", "leios-fetch",
				"role", "client",
				"connection_id", connId,
			)
		})
	case *oleiosnotify.MsgBlockTxsOffer:
		// The peer is offering the transactions for this endorser block. Fetch
		// them over leios-fetch (off the handler, serialized per connection, and
		// deduped across connections) so the EB becomes complete and its outputs
		// can be applied to the ledger. Best-effort and gated
		// (EnableLeiosTxFetch): a failure must not tear down the shared
		// connection.
		if !o.config.EnableLeiosTxFetch {
			return nil
		}
		// See MsgBlockOffer above: skip head-block prefetch while deeply behind.
		if !o.leiosTipPrefetchEnabled() {
			return nil
		}
		if conn.LeiosFetch() == nil || conn.LeiosFetch().Client == nil {
			return nil
		}
		client := conn.LeiosFetch().Client
		point := m.Point
		// Common case: a repeated offer for an EB already fully fetched (or
		// empty). Skip without spawning a fetch.
		if data, ok := o.lookupLeiosEndorserBlock(point.Hash); ok &&
			(data.txCount == 0 || data.completeTxCache()) {
			return nil
		}
		// The relay offers each EB on every connection; claim it so it is
		// fetched once. The claim is released when the fetch finishes (or below
		// if the per-connection bound is reached).
		hashKey := string(point.Hash)
		if _, loaded := o.leiosFetchInProgress.LoadOrStore(
			hashKey,
			struct{}{},
		); loaded {
			return nil
		}
		if !o.dispatchLeiosFetch(ctx.ConnectionId, func() {
			defer o.leiosFetchInProgress.Delete(hashKey)
			data, ok := o.lookupLeiosEndorserBlock(point.Hash)
			if !ok {
				// Manifest not cached yet (txs offered before/without a block
				// offer): fetch the manifest first to learn the tx count.
				resp, err := client.BlockRequest(point)
				if err != nil {
					o.config.Logger.Debug(
						"leios EB manifest fetch failed on txs offer",
						"error", err,
						"connection_id", connId,
						"slot", point.Slot,
					)
					return
				}
				respBlock, ok := resp.(*leiosfetch.MsgBlock)
				if !ok {
					return
				}
				if err := o.storeLeiosEndorserBlock(
					point,
					respBlock.BlockRaw,
					nil,
				); err != nil {
					o.config.Logger.Debug(
						"failed to store leios EB manifest on txs offer",
						"error", err,
						"connection_id", connId,
						"slot", point.Slot,
					)
					return
				}
				if data, ok = o.lookupLeiosEndorserBlock(point.Hash); !ok {
					return
				}
			}
			if data.txCount == 0 || data.completeTxCache() {
				return
			}
			txs, err := o.fetchLeiosEbTxsBatched(client, point, data.txCount)
			if err != nil {
				o.config.Logger.Debug(
					"leios EB transaction fetch failed",
					"error", err,
					"connection_id", connId,
					"slot", point.Slot,
					"hash", hex.EncodeToString(point.Hash),
					"fetched", len(txs),
					"tx_count", data.txCount,
				)
				return
			}
			if err := o.storeLeiosEndorserBlock(
				point,
				data.blockRaw,
				txs,
			); err != nil {
				o.config.Logger.Debug(
					"failed to store leios EB transactions",
					"error", err,
					"connection_id", connId,
					"slot", point.Slot,
				)
				return
			}
			o.config.Logger.Info(
				fmt.Sprintf(
					"fetched EB txs %d.%x %d/%d",
					point.Slot,
					point.Hash,
					len(txs),
					data.txCount,
				),
				"component", "network",
				"protocol", "leios-fetch",
				"role", "client",
				"connection_id", connId,
			)
		}) {
			// Per-connection bound reached: release the claim so a later offer
			// (on this or another connection) can retry.
			o.leiosFetchInProgress.Delete(hashKey)
		}
	case *oleiosnotify.MsgVotesOffer:
		// The Leios prototype diffuses full votes inline over leios-notify
		// (rather than the standalone leios-votes protocol). Feed them to the
		// vote manager, which validates each vote (structure, window, committee
		// membership, dedup, BLS) and builds an endorser-block certificate on
		// quorum. (m.Votes carries vote IDs offered by non-prototype peers and
		// is fetched separately; only pushed FullVotes are handled here.)
		if o.LeiosVotes == nil {
			return nil
		}
		for _, vote := range m.FullVotes {
			if err := o.LeiosVotes.HandleVote(connId, vote); err != nil {
				o.config.Logger.Debug(
					"failed to handle pushed leios vote",
					"component", "network",
					"protocol", "leios-notify",
					"connection_id", connId,
					"slot", vote.SlotNo,
					"voter_id", vote.VoterId,
					"error", err,
				)
			}
		}
	}
	return nil
}

// leiosBlockTxsRequester is the subset of the leios-fetch client used to fetch
// endorser-block transactions. It is an interface so the re-request logic below
// can be unit-tested without a live connection.
type leiosBlockTxsRequester interface {
	BlockTxsRequest(
		point ocommon.Point,
		bitmaps map[uint16]uint64,
	) (protocol.Message, error)
}

const (
	leiosTxFetchWindowSize = 64
	leiosTxFetchMaxWindows = 1 << 16
	// leiosTxFetchWindowsPerRequest bounds how many 64-tx windows of
	// still-missing transactions are requested in a single BlockTxsRequest.
	// The leios-fetch state machine is strict request/response (no protocol
	// pipelining), so overlapping round-trips means asking for more per
	// request: the request bitmap carries several windows at once and the
	// relay serves up to its per-message cap, cutting the round-trips a large
	// endorser block needs from O(txCount/64) toward O(txCount/cap). It stays
	// bounded rather than requesting the whole block, because the prototype
	// relay resets the connection when asked for everything at once.
	leiosTxFetchWindowsPerRequest = 8
)

// leiosTxFetchTailPoll is how often the fetch re-requests an endorser block's
// still-missing tail while stalled, within OuroborosConfig.LeiosTxFetchTailBudget.
// It is a re-check cadence, not a protocol parameter.
const leiosTxFetchTailPoll = 300 * time.Millisecond

// leiosFetchMaxInflightPerConn bounds how many leios-fetch operations may be
// queued or running on a single connection. The per-connection mutex
// serializes them (the client is strict request/response), so this caps the
// goroutines a burst of offers can spawn; excess offers are dropped (the relay
// re-offers, and the same EB is fetched on whichever connection is free).
const leiosFetchMaxInflightPerConn = 4

// leiosFetchGuard serializes leios-fetch client operations on one connection
// and bounds how many are outstanding.
type leiosFetchGuard struct {
	mu       sync.Mutex
	inflight atomic.Int32
	// cooledUntilNano is a unix-nano deadline before which the backfill
	// connection selector should skip this connection after a failed or
	// timed-out fetch, so it prefers healthy connections instead of repeatedly
	// retrying a stalled or flaky one.
	cooledUntilNano atomic.Int64
}

// markFetchFailed puts this connection on a short cooldown after a failed or
// timed-out backfill fetch.
func (g *leiosFetchGuard) markFetchFailed(now time.Time, d time.Duration) {
	g.cooledUntilNano.Store(now.Add(d).UnixNano())
}

// markFetchOK clears any cooldown after a successful fetch on this connection.
func (g *leiosFetchGuard) markFetchOK() {
	g.cooledUntilNano.Store(0)
}

// inCooldown reports whether this connection is still cooling down from a
// recent failed fetch as of now.
func (g *leiosFetchGuard) inCooldown(now time.Time) bool {
	until := g.cooledUntilNano.Load()
	return until > 0 && now.UnixNano() < until
}

func (o *Ouroboros) leiosFetchGuardFor(
	connId ouroboros.ConnectionId,
) *leiosFetchGuard {
	g, _ := o.leiosFetchGuards.LoadOrStore(connId, &leiosFetchGuard{})
	return g.(*leiosFetchGuard)
}

// dispatchLeiosFetch runs fn (a leios-fetch client operation) asynchronously,
// serialized against other fetches on the same connection so the strict
// request/response client is never used concurrently, and bounded per
// connection. It returns immediately so the leios-notify handler is never
// blocked on a multi-second fetch (which otherwise head-of-line blocks every
// later offer on the connection). Returns false if the per-connection bound is
// reached and the work was dropped.
func (o *Ouroboros) dispatchLeiosFetch(
	connId ouroboros.ConnectionId,
	fn func(),
) bool {
	g := o.leiosFetchGuardFor(connId)
	if g.inflight.Load() >= leiosFetchMaxInflightPerConn {
		return false
	}
	g.inflight.Add(1)
	go func() {
		defer g.inflight.Add(-1)
		g.mu.Lock()
		defer g.mu.Unlock()
		fn()
	}()
	return true
}

// leiosTxFetchMaxRoundsPerWindow bounds how many BlockTxsRequest rounds are
// spent completing a single window, so a relay that never serves a particular
// transaction cannot loop forever. A response that serves no new transaction
// already aborts the window (the progress==0 guard below), so this cap only
// bounds slow-but-steady progress: a relay that dribbles a single new
// transaction per round needs one round per transaction, so the cap is the
// window size. A lower cap would abort fetches that were still making valid
// partial progress and leave the endorser block permanently incomplete.
const leiosTxFetchMaxRoundsPerWindow = leiosTxFetchWindowSize

// fetchLeiosEbTxsBatched fetches all txCount transactions of an endorser block
// over leios-fetch, requesting up to leiosTxFetchWindowsPerRequest 64-tx
// windows of still-missing transactions per BlockTxsRequest. Batching several
// windows per request overlaps the relay's per-response work and cuts the
// round-trips a large endorser block needs (a sequential one-window-per-request
// fetch took 5-17s for 1000+ tx blocks, far past the Leios diffusion window).
// The batch stays bounded — not the whole block — because the prototype relay
// resets the connection if asked for all transactions at once, and the relay
// serves a request only partially when the response would exceed its
// per-message size cap, so still-missing transactions are re-requested until
// complete. Which transactions a response carried is taken from the response's
// own bitmaps, falling back to "the relay served a prefix of the requested
// indices in ascending order" when the response omits them. Transactions are
// placed at their absolute index, so the result is in index order; on an error
// or a no-progress request it returns the contiguous prefix fetched so far,
// letting callers treat the fetch as best-effort.
func (o *Ouroboros) fetchLeiosEbTxsBatched(
	client leiosBlockTxsRequester,
	point ocommon.Point,
	txCount int,
) ([]cbor.RawMessage, error) {
	if client == nil {
		return nil, errors.New("leios-fetch client unavailable")
	}
	if txCount <= 0 {
		return nil, nil
	}
	numWindows := (txCount-1)/leiosTxFetchWindowSize + 1
	if numWindows > leiosTxFetchMaxWindows {
		return nil, fmt.Errorf(
			"leios-fetch tx count %d requires %d bitmap windows, max %d",
			txCount,
			numWindows,
			leiosTxFetchMaxWindows,
		)
	}
	result := make([]cbor.RawMessage, txCount)
	// The no-progress guard below guarantees termination (each non-final round
	// places at least one new transaction, and there are txCount of them); this
	// is an absolute backstop against a relay that dribbles already-held txs.
	maxRounds := numWindows * leiosTxFetchMaxRoundsPerWindow
	// tailStall marks when the fetch first stalled (a round served no new
	// transactions). The relay diffuses an endorser block's transactions over
	// several seconds, so the last (partial) window may not be served yet;
	// rather than abort on the first miss, re-request it until the tail-retry
	// budget elapses. Reset on any progress.
	var tailStall time.Time
	for round := 0; ; round++ {
		needed := leiosNeededBitmap(
			result,
			txCount,
			leiosTxFetchWindowsPerRequest,
		)
		if len(needed) == 0 {
			break // every transaction fetched
		}
		if round >= maxRounds {
			return leiosCollectTxs(result), fmt.Errorf(
				"leios-fetch could not complete %d transactions after %d rounds",
				txCount, round,
			)
		}
		resp, err := client.BlockTxsRequest(point, needed)
		if err != nil {
			return leiosCollectTxs(result), err
		}
		respTxs, ok := resp.(*leiosfetch.MsgBlockTxs)
		if !ok {
			return leiosCollectTxs(result), fmt.Errorf(
				"unexpected leios-fetch BlockTxs response type %T", resp,
			)
		}
		served := leiosBitmapTxIndices(respTxs.Bitmaps)
		if len(served) != len(respTxs.TxsRaw) {
			// Response omitted bitmaps: assume the relay served a prefix of the
			// requested indices in ascending order.
			served = leiosBitmapTxIndices(needed)
		}
		progress := 0
		for k, raw := range respTxs.TxsRaw {
			if k >= len(served) {
				break
			}
			idx := served[k]
			if idx >= 0 && idx < txCount && result[idx] == nil {
				result[idx] = slices.Clone(raw)
				progress++
			}
		}
		if progress == 0 {
			// No new transactions this round. With no tail-retry budget (e.g.
			// unit tests) abort immediately, preserving prior behavior.
			// Otherwise keep re-requesting the still-diffusing tail until the
			// budget elapses.
			if o.config.LeiosTxFetchTailBudget <= 0 {
				return leiosCollectTxs(result), errors.New(
					"leios-fetch served no new transactions",
				)
			}
			if tailStall.IsZero() {
				tailStall = time.Now()
			} else if time.Since(tailStall) >= o.config.LeiosTxFetchTailBudget {
				return leiosCollectTxs(result), errors.New(
					"leios-fetch served no new transactions within tail budget",
				)
			}
			time.Sleep(leiosTxFetchTailPoll)
			continue
		}
		tailStall = time.Time{}
	}
	return leiosCollectTxs(result), nil
}

// leiosNeededBitmap returns the still-missing transaction indices grouped into
// up to maxWindows lowest-indexed 64-tx windows, as a leios-fetch request
// bitmap. Selecting the lowest windows first keeps the fetched prefix
// contiguous (so leiosCollectTxs yields the longest usable run as the fetch
// progresses).
func leiosNeededBitmap(
	result []cbor.RawMessage,
	txCount, maxWindows int,
) map[uint16]uint64 {
	numWindows := (txCount-1)/leiosTxFetchWindowSize + 1
	bitmap := make(map[uint16]uint64)
	for w := 0; w < numWindows && len(bitmap) < maxWindows; w++ {
		if mask := leiosWindowNeededMask(result, w, txCount); mask != 0 {
			bitmap[uint16(w)] = mask // #nosec G115 -- w < numWindows <= 1<<16
		}
	}
	return bitmap
}

// leiosWindowNeededMask returns the bitmap of transaction indices in 64-tx
// window w that are within txCount and not yet present in result.
func leiosWindowNeededMask(result []cbor.RawMessage, w, txCount int) uint64 {
	var mask uint64
	base := w * 64
	for bit := 0; bit < 64 && base+bit < txCount; bit++ {
		if result[base+bit] == nil {
			mask |= 1 << uint(bit)
		}
	}
	return mask
}

// leiosBitmapTxIndices returns the transaction indices (window*64 + bit) of all
// set bits across the bitmap windows, in ascending index order.
func leiosBitmapTxIndices(bitmaps map[uint16]uint64) []int {
	windows := make([]uint16, 0, len(bitmaps))
	for w := range bitmaps {
		windows = append(windows, w)
	}
	slices.Sort(windows)
	var idx []int
	for _, w := range windows {
		mask := bitmaps[w]
		for bit := range 64 {
			if mask&(1<<uint(bit)) != 0 {
				idx = append(idx, int(w)*64+bit)
			}
		}
	}
	return idx
}

// leiosCollectTxs returns the contiguous run of fetched transactions from the
// start. A gap (a still-missing transaction) ends the run: endorser blocks are
// only usable when complete and their transactions are positional, so a prefix
// keeps the indices aligned.
func leiosCollectTxs(result []cbor.RawMessage) []cbor.RawMessage {
	out := make([]cbor.RawMessage, 0, len(result))
	for _, r := range result {
		if r == nil {
			break
		}
		out = append(out, r)
	}
	return out
}

func (o *Ouroboros) leiosnotifyServerRequestNext(
	ctx oleiosnotify.CallbackContext,
) (protocol.Message, error) {
	if ctx.Server == nil {
		return nil, nil
	}
	connKey := leiosConnectionIdString(ctx.ConnectionId)
	done := ctx.Server.DoneChan()

	// If the connection is already closing, return without touching the
	// cursor map. This prevents re-registering a stale cursor after
	// removeConn has already run (which would block future log pruning).
	select {
	case <-done:
		return nil, nil
	default:
	}

	for {
		entry, wakeCh := o.leiosEBLog.next(connKey)
		if entry != nil {
			return &oleiosnotify.MsgBlockOffer{Point: entry.point}, nil
		}
		select {
		case <-wakeCh:
			// new EB appended — re-check
		case <-done:
			return nil, nil
		}
	}
}
