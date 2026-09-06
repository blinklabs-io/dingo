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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	cardano "github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// Max number of blocks to fetch in a single blockfetch call
	// This prevents us exceeding the configured recv queue size in the block-fetch protocol
	blockfetchBatchSize = 500

	// When we're still meaningfully behind tip, wait for a header runway
	// before starting blockfetch so each batch amortises peer round-trip
	// and protocol overhead over many blocks instead of trickling 1-8
	// blocks per request. The cap was 64 historically (a fork-boundary
	// safety value) — too small for catchup, where the chain can fall
	// thousands of blocks behind and we want to fetch hundreds-per-batch.
	// `desiredBlockfetchBatchHeaders` scales up to this cap based on the
	// observed block-gap; near tip it falls back to small batches for
	// low latency.
	blockfetchMinBatchHeadersWhenBehind = 8
	blockfetchMaxBatchHeadersWhenBehind = 500
	blockfetchMinBatchGapSlots          = 64

	// Maximum number of definitive failures to obtain one queued header range
	// before the queue is dropped. Both failure shapes count against the same
	// range, because a peer that rolled the queued block back can produce
	// either one: a NoBlocks reply (surfacing synchronously as a
	// GetBlockRange error) when its range server rejects the start point,
	// or a StartBatch/BatchDone pair carrying no blocks. Transport, shutdown,
	// and wiring errors from GetBlockRange do not count: they do not establish
	// that the peer cannot serve the range.
	//
	// The count is keyed to the range start point rather than being a
	// global consecutive streak, and it deliberately survives both
	// deliveries for other ranges and header-queue churn. Failures against
	// one unfetchable range are minutes apart, and in between the node
	// fetches normally from other peers while forks, connection switches
	// and header mismatches repeatedly clear and refill the queue with the
	// same header. A global streak is reset by all of that, so it fires
	// only when failures happen to land back to back: two identical DevNet
	// runs produced 1 firing against 169 wedge events and 9 against 81.
	// Keying by range makes the same bound deterministic. A miss on a
	// different range starts its own count, so a peer that is briefly
	// behind is not punished, and a delivered block for the stuck range
	// discards its record entirely.
	blockfetchMaxSameRangeFailures = 3

	// Warn after this many consecutive watchdog expirations while the same
	// protocol request is still blocked outside the ledger mutex. The request
	// remains protected from duplicate retries, but a permanently wedged peer
	// must become visible at normal log level.
	blockfetchInFlightTimeoutWarnThreshold = 3

	// Number of received blockfetch blocks to buffer before committing them.
	// Keep this small so downstream iterators still see fresh blocks promptly.
	blockfetchCommitBatchSize = 8

	deferredHeaderValidationSyncStatePrefix = "deferred_header_validation:"
	deferredHeaderValidationSyncStateValue  = "true"

	// shadowBlockfetchPrimarySlowThreshold is the fixed-cutoff fallback
	// used to gate shadow blockfetch dispatch when no peer-population
	// median latency is available (e.g. a fresh node with too few
	// samples). It approximates the upper edge of "healthy primary"
	// for typical preview/preprod peers.
	shadowBlockfetchPrimarySlowThreshold = 250 * time.Millisecond

	// shadowBlockfetchMedianMultiplier scales the observed median peer
	// EWMA latency to derive the "primary is in the slow tail" cutoff.
	// Primary with EWMA above (median * multiplier) gets a shadow buddy;
	// primaries closer to the median ride alone. Adapts the gate to the
	// peer population instead of using a fixed value.
	shadowBlockfetchMedianMultiplier = 1.5

	// shadowBlockfetchMedianMinSamples is the minimum number of peers
	// with at least one EWMA sample required to trust the median-based
	// gate. Below this, fall back to shadowBlockfetchPrimarySlowThreshold.
	shadowBlockfetchMedianMinSamples = 3

	// shadowBlockfetchMaxHeaders is the header-queue depth threshold
	// below which a shadow blockfetch is dispatched. Near tip the queue
	// is short (1–4 headers), so shadow dispatch is cheap and the
	// latency win is real. For bulk catch-up batches the extra request
	// adds noise without meaningful benefit.
	shadowBlockfetchMaxHeaders = 4

	// Default/fallback slot threshold for blockfetch batches
	blockfetchBatchSlotThresholdDefault = 2500 * 20

	// Timeout for updates on a blockfetch operation. This is based on a 2s BatchStart
	// and a 2s Block timeout for blockfetch
	blockfetchBusyTimeout = 30 * time.Second

	// Interval for rate-limiting non-active connection drop messages
	dropEventLogInterval = 60 * time.Second

	// Interval for periodic sync progress reporting
	syncProgressLogInterval = 30 * time.Second

	// Rollback loop detection thresholds
	rollbackLoopThreshold = 2               // number of rollbacks to same slot before breaking loop
	rollbackLoopWindow    = 5 * time.Minute // time window for rollback loop detection

	// Number of consecutive header mismatches before triggering
	// a chainsync re-sync to recover from persistent forks.
	// A higher threshold gives tryResolveFork more chances to
	// find the common ancestor and reduces disruptive resyncs
	// in multi-producer networks where short forks are expected.
	headerMismatchResyncThreshold = 20

	maxPeerHeaderHistoryPerConn = 256
	// Genesis fork resolution can need more than the normal K-sized history to
	// compare a candidate's density, but retaining decoded headers for an
	// entire slot window makes memory proportional to an attacker-controlled
	// number of blocks. Store wire header bytes lazily and cap each peer's
	// retained history independently of the configured slot window. The default
	// Genesis quorum is one fast source plus two corroborators, so this leaves a
	// bounded 24 MiB wire-history allowance across the three required peers.
	// A path that does not fit this budget falls back to a fresh intersection.
	maxPeerHeaderHistoryBytesPerConn = 8 << 20
	peerHeaderHistoryRecordOverhead  = 512

	// Match ouroboros-consensus' default maximum permissible clock skew. A
	// header within this window is held until its slot begins; an earlier
	// resolvable header is dropped and recovered by a peer-local re-intersection
	// without treating ambiguous local clock skew as a peer fault.
	defaultHeaderClockSkew = 2 * time.Second
)

// ErrRollbackLoopDetected is returned by handleEventChainsyncRollback when
// the same peer repeatedly requests a rollback to the same slot within the
// rollback loop detection window. The rollback is skipped to break the loop,
// and the caller should trigger a chainsync re-sync to recover.
var ErrRollbackLoopDetected = errors.New(
	"rollback loop detected: same slot rolled back too many times within window",
)

var ErrRollbackExceedsMithrilBoundary = errors.New(
	"rollback exceeds Mithril trust boundary",
)

// ErrNoAppliedAncestorBelowContestedSlot reports that a rollback target shares
// the applied tip's slot with a different hash and no applied ancestor below
// that slot could be found to rewind to. The contested slot's effects cannot be
// truncated in place, so the rollback fails loudly rather than reporting a
// repair that left the UTxO set diverged.
var ErrNoAppliedAncestorBelowContestedSlot = errors.New(
	"no applied ancestor below contested slot",
)

type peerHeaderRecord struct {
	// event carries only the metadata needed to reconstruct a ChainsyncEvent.
	// Production records leave BlockHeader nil and decode headerCbor only when
	// recovery actually needs it. In-package synthetic headers may provide an
	// event with a header and use that fallback when no CBOR is available.
	event      ChainsyncEvent
	headerCbor []byte
	prevHash   []byte
	decodeType uint
	bytes      int
}

type peerHeaderChain struct {
	order         []string
	byHash        map[string]peerHeaderRecord
	retainedBytes int
}

// peerHeaderHistoryPathCacheEntry memoizes one retained header's walk toward
// a locally known ancestor while replaying a rollback. The cache is scoped to
// one recovery pass, so decoded headers are released with the pass and cannot
// outlive the peer history budget.
type peerHeaderHistoryPathCacheEntry struct {
	ancestor       ocommon.Point
	distance       int
	hasRecord      bool
	ok             bool
	depthExhausted bool
	event          ChainsyncEvent
	nextHash       string
}

type peerHeaderHistoryPathStep struct {
	event    ChainsyncEvent
	key      string
	nextHash string
}

func (ls *LedgerState) handleEventChainsync(evt event.Event) {
	// Registered before the mutex is taken so defer's LIFO order runs it
	// after the unlock: publishing under ls.chainsyncMutex deadlocks the
	// node. See pendingPublishes.
	var pending pendingPublishes
	defer pending.flush()
	e, ok := evt.Data.(ChainsyncEvent)
	if !ok {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		ls.logUnexpectedChainsyncEventData("ChainsyncEvent", evt)
		return
	}
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	if !ls.isConnectionLive(e.ConnectionId) {
		ls.config.Logger.Debug(
			"ignoring chainsync event from closed connection",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"rollback", e.Rollback,
			"slot", e.Point.Slot,
		)
		ls.discardBufferedPeerHeaders(e.ConnectionId)
		delete(ls.peerHeaderHistory, connIdKey(e.ConnectionId))
		return
	}
	if e.Rollback {
		if err := ls.handleEventChainsyncRollback(e, &pending); err != nil {
			if errors.Is(err, ErrRollbackLoopDetected) {
				// The rollback was skipped to break a pathological
				// loop. Trigger a chainsync re-sync so the peer
				// can negotiate a fresh intersection rather than
				// continuing to send the same rollback point.
				ls.resetChainsyncResyncState()
				ls.setChainsyncState(SyncingChainsyncState)
				// Queued rather than published here: this runs with
				// ls.chainsyncMutex held, and this event's subscriber
				// takes that same mutex via RecoverAfterLocalRollback.
				pending.add(
					ls.config.EventBus,
					event.ChainsyncResyncEventType,
					event.NewEvent(
						event.ChainsyncResyncEventType,
						event.ChainsyncResyncEvent{
							ConnectionId: e.ConnectionId,
							Reason:       event.ChainsyncResyncReasonRollbackLoop,
						},
					),
				)
				return
			}
			ls.config.Logger.Error(
				"failed to handle rollback",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			if ls.config.FatalErrorFunc != nil {
				ls.config.FatalErrorFunc(err)
			}
			return
		}
	} else if e.BlockHeader != nil {
		if err := ls.handleEventChainsyncBlockHeaderWithPending(e, &pending); err != nil {
			// Header queue full is expected during bulk sync when
			// pipelined headers arrive faster than blockfetch can
			// drain them. Log at DEBUG to avoid log spam.
			if errors.Is(err, chain.ErrHeaderQueueFull) {
				ls.config.Logger.Debug(
					"failed to handle block header",
					"component", "ledger",
					"error", err,
					"slot", e.Point.Slot,
					"hash", hex.EncodeToString(e.Point.Hash),
				)
				return
			}
			ls.config.Logger.Error(
				"failed to handle block header",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			pending.add(
				ls.config.EventBus,
				LedgerErrorEventType,
				event.NewEvent(
					LedgerErrorEventType,
					LedgerErrorEvent{
						Error:     err,
						Operation: "block_header",
						Point:     e.Point,
					},
				),
			)
			return
		}
	}
}

func (ls *LedgerState) isConnectionLive(
	connId ouroboros.ConnectionId,
) bool {
	if ls.config.ConnectionLiveFunc == nil {
		return true
	}
	return ls.config.ConnectionLiveFunc(connId)
}

func (ls *LedgerState) setChainsyncState(
	state ChainsyncState,
) ChainsyncState {
	ls.Lock()
	previous := ls.chainsyncState
	ls.chainsyncState = state
	ls.Unlock()
	return previous
}

func (ls *LedgerState) setChainsyncStateIf(
	current ChainsyncState,
	next ChainsyncState,
) bool {
	ls.Lock()
	defer ls.Unlock()
	if ls.chainsyncState != current {
		return false
	}
	ls.chainsyncState = next
	return true
}

func (ls *LedgerState) validationStateSnapshot() (bool, uint64) {
	ls.RLock()
	defer ls.RUnlock()
	return ls.validationEnabled, ls.mithrilLedgerSlot
}

func (ls *LedgerState) mithrilLedgerSlotSnapshot() uint64 {
	ls.RLock()
	defer ls.RUnlock()
	return ls.mithrilLedgerSlot
}

func headerValidationPointKey(point ocommon.Point) string {
	return fmt.Sprintf("%d:%s", point.Slot, hex.EncodeToString(point.Hash))
}

func deferredHeaderValidationSyncStateKey(point ocommon.Point) string {
	return deferredHeaderValidationSyncStatePrefix + headerValidationPointKey(
		point,
	)
}

func (ls *LedgerState) markDeferredHeaderValidation(point ocommon.Point) {
	ls.deferredHeaderValidationMu.Lock()
	defer ls.deferredHeaderValidationMu.Unlock()
	if ls.deferredHeaderValidation == nil {
		ls.deferredHeaderValidation = make(map[string]struct{})
	}
	ls.deferredHeaderValidation[headerValidationPointKey(point)] = struct{}{}
}

func (ls *LedgerState) clearDeferredHeaderValidation(point ocommon.Point) {
	ls.deferredHeaderValidationMu.Lock()
	defer ls.deferredHeaderValidationMu.Unlock()
	delete(ls.deferredHeaderValidation, headerValidationPointKey(point))
}

func (ls *LedgerState) consumeDeferredHeaderValidation(
	point ocommon.Point,
) bool {
	ls.deferredHeaderValidationMu.Lock()
	defer ls.deferredHeaderValidationMu.Unlock()
	key := headerValidationPointKey(point)
	if _, ok := ls.deferredHeaderValidation[key]; !ok {
		return false
	}
	delete(ls.deferredHeaderValidation, key)
	return true
}

// evictStaleDeferredHeadersLocked drops deferred-header entries that are
// provably abandoned (issue #3727, finding 5). A canonical deferred header is
// consumed by consumeDeferredHeaderValidation when its block is applied, so an
// entry still present once the cursor has passed its slot is on a fork; keeping
// it forever would pin its epoch's pool_stake_snapshot rows forever.
//
// Eviction is gated on the ROLLBACK HORIZON, not on the bare tip. Eviction
// removes the durable sync_state marker as well as the in-memory entry, and
// that marker is the only thing that makes deferredHeaderValidationRequired
// return true at apply. If chain selection later rolls back and re-adopts a
// fork containing an evicted point, its block applies with required == false,
// so verifyDeferredBlockHeaderState returns nil and the block is adopted with
// its stateful leader-eligibility check never run -- a validation bypass that
// does not exist on main, where the entry and marker survive. Evicting only
// below tipSlot-rollbackHorizon (the stability window, 3k/f slots, past which
// chain selection cannot re-adopt a fork) makes "abandoned" mean unreachable
// rather than merely behind the cursor, so no entry that a rollback could
// resurrect is ever dropped (issue #3717 review: evicted-point re-adoption
// skips the deferred header check).
//
// Entries with an unparseable key are dropped regardless of the horizon: they
// can never be validated, so no re-adoption can make use of them. Returns the
// evicted map keys so the caller can delete their persisted markers. The caller
// must hold deferredHeaderValidationMu; tipSlot and rollbackHorizon are read by
// the caller BEFORE taking it, because both ls.loadTipSnapshot's era-derived
// window (calculateStabilityWindow) and the tip read take ls.RWMutex, and
// taking that under deferredHeaderValidationMu would invert the order block
// apply uses (ls lock -> deferredHeaderValidationMu).
func (ls *LedgerState) evictStaleDeferredHeadersLocked(
	tipSlot uint64,
	rollbackHorizon uint64,
) []string {
	if len(ls.deferredHeaderValidation) == 0 {
		return nil
	}
	// The whole chain is still inside the horizon, so nothing is unreachable
	// yet. Unparseable keys are still evicted below.
	var cutoff uint64
	if tipSlot > rollbackHorizon {
		cutoff = tipSlot - rollbackHorizon
	}
	var evicted []string
	for key := range ls.deferredHeaderValidation {
		slot, err := slotFromHeaderValidationKey(key)
		// STRICTLY below the cutoff: a block that deep cannot be re-adopted,
		// so an entry still present is abandoned and safe to forget. Anything
		// at or above the cutoff keeps both its entry and its marker so a
		// rollback-then-re-adopt still finds required == true at apply.
		if err != nil || slot < cutoff {
			evicted = append(evicted, key)
			delete(ls.deferredHeaderValidation, key)
		}
	}
	return evicted
}

// deletePersistedDeferredMarkers removes the sync_state markers for a set of
// evicted deferred-header map keys. It runs after the in-memory eviction has
// already released the retention pin, so a delete failure only leaves a dead
// marker row to be re-cleaned on a later pass.
//
// It re-checks each key under deferredHeaderValidationMu and skips any that is
// present in the in-memory set again: between eviction (which happens for a key
// whose slot is below the rollback horizon) and this cleanup, that same point
// can be re-deferred and re-persisted -- markDeferredHeaderValidation and
// persistDeferredHeaderValidation run together on the chainsync path. Deleting
// the marker for a currently-live entry would drop the durable pin for an
// active deferred header, so on the next restart repopulate would miss it and
// cleanup could prune the snapshot it needs (issue #3727, finding:
// evicted-marker delete races a re-defer).
//
// The membership test alone cannot close that window, because the lock is
// released before the delete (see below): a re-defer landing between the test
// and the delete would have its fresh marker deleted, and a restart would then
// find no marker, so deferredHeaderValidationRequired returns false and
// verifyDeferredBlockHeaderState skips the stateful check for a header that is
// still outstanding. So the delete is made effectively conditional by
// re-testing membership AFTER it and RE-PERSISTING the marker for any key that
// came back, rather than by holding the lock across the delete. Re-persisting
// is idempotent (SetSyncState of the same key/value) and runs with no lock
// held, so it closes the window without reintroducing the lock inversion
// (issue #3717 review / cubic P1: re-admission after the live check).
func (ls *LedgerState) deletePersistedDeferredMarkers(mapKeys []string) error {
	if len(mapKeys) == 0 || ls.db == nil || ls.db.Metadata() == nil {
		return nil
	}
	// The membership test is taken under the lock, but the lock is RELEASED
	// before DeleteSyncState: that delete opens the single sqlite write
	// connection (nil txn -> Transaction(true)), and block apply holds that
	// connection before taking this mutex via consumeDeferredHeaderValidation.
	// Holding the mutex across the delete inverts the lock order (mutex->write-
	// conn here vs. write-conn->mutex on apply) and deadlocks the node on the
	// single write connection -- the same inversion as the prune path (issue
	// #3717). A point re-deferred (and re-persisted) between the membership test
	// and the delete keeps its live pin in the in-memory set for the running
	// process, so the retention floor still covers it; only its durable marker
	// could be dropped, and solely if the re-defer lands in that narrow window
	// AND the node then restarts before the next cleanup re-persists it. That
	// residual is accepted in exchange for never inverting the lock order.
	var cleanupErr error
	for _, k := range mapKeys {
		ls.deferredHeaderValidationMu.Lock()
		_, live := ls.deferredHeaderValidation[k]
		ls.deferredHeaderValidationMu.Unlock()
		if live {
			// Re-admitted since eviction: its marker is now backing a live pin.
			continue
		}
		// A restore failure inside deleteDeferredMarkerUnlessReadmitted (a point
		// re-admitted during its delete whose marker could not be re-persisted)
		// is a LOST DURABLE PIN and must not be swallowed (issue #3717 review):
		// it is joined and propagated so the caller
		// (PrunePoolSnapshotsWithRetentionFloor -> the retention guard ->
		// cleanupOldSnapshots) surfaces the failed cleanup instead of continuing
		// with a marker that a restart would miss. Remaining keys are still
		// processed so one failure does not leak the other stale markers.
		if err := ls.deleteDeferredMarkerUnlessReadmitted(k); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	return cleanupErr
}

// afterDeferredMarkerDeleteHook is a test seam. It is nil in production (a
// single nil-check, zero cost) and, when set, runs inside
// deleteDeferredMarkerUnlessReadmitted immediately AFTER DeleteSyncState returns
// and BEFORE the membership re-test. It lets a test inject a re-admission
// (markDeferredHeaderValidation) that lands DURING the delete window rather than
// before the call, so the TOCTOU restore path is actually exercised: an
// implementation that tested membership before deleting and skipped the delete
// would not restore the marker and must fail such a test (issue #3717 review).
var afterDeferredMarkerDeleteHook func()

// deferredMarkerRestoreMaxAttempts bounds the retry on re-persisting the marker
// for a point re-admitted during its delete. A handful of attempts rides out a
// transient sqlite-busy without spinning; exhausting them is treated as a lost
// durable pin and propagated.
const deferredMarkerRestoreMaxAttempts = 3

// deleteDeferredMarkerUnlessReadmitted deletes one evicted key's persisted
// marker, then restores it if the key was re-admitted to the in-memory
// deferred set while the delete was in flight.
//
// Neither DB call runs under deferredHeaderValidationMu (that would invert the
// lock order against block apply — issue #3717), so the caller's membership
// test cannot be atomic with the delete. The delete is instead made effectively
// conditional after the fact: re-testing membership and re-persisting is
// idempotent, and it leaves the durable marker present for exactly the keys
// that are live when this returns. Without the restore, a point re-deferred in
// that window keeps its in-memory entry but loses its marker, and after a
// restart repopulateDeferredHeaderValidation misses it — so
// deferredHeaderValidationRequired returns false and
// verifyDeferredBlockHeaderState skips the stateful check for a header that is
// still outstanding.
//
// A failed DeleteSyncState leaves the durable marker in place, so the retention
// pin is NOT lost — the stale row is simply re-cleaned on a later pass — and is
// logged best-effort. A failed RESTORE is different: it drops the durable pin
// for a header that is live again in the in-memory set, so after a restart the
// stateful check would be skipped. It is retried a bounded number of times and,
// if it still fails, PROPAGATED to the caller (issue #3717 review: a restoration
// failure must not pass silently). The in-memory entry survives regardless, so
// the running process keeps the pin; propagation surfaces the lost DURABLE pin
// so the node fails the cleanup rather than continuing toward a restart that
// would lose it.
func (ls *LedgerState) deleteDeferredMarkerUnlessReadmitted(k string) error {
	syncKey := deferredHeaderValidationSyncStatePrefix + k
	if err := ls.db.DeleteSyncState(syncKey, nil); err != nil {
		ls.config.Logger.Warn(
			"failed to delete stale deferred-header marker",
			"key", syncKey,
			"error", err,
			"component", "ledger",
		)
		return nil
	}
	if hook := afterDeferredMarkerDeleteHook; hook != nil {
		hook()
	}
	ls.deferredHeaderValidationMu.Lock()
	_, readmitted := ls.deferredHeaderValidation[k]
	ls.deferredHeaderValidationMu.Unlock()
	if !readmitted {
		return nil
	}
	var restoreErr error
	for attempt := 1; attempt <= deferredMarkerRestoreMaxAttempts; attempt++ {
		restoreErr = ls.db.SetSyncState(
			syncKey,
			deferredHeaderValidationSyncStateValue,
			nil,
		)
		if restoreErr == nil {
			return nil
		}
		ls.config.Logger.Warn(
			"failed to restore re-deferred header marker; retrying",
			"key", syncKey,
			"attempt", attempt,
			"error", restoreErr,
			"component", "ledger",
		)
	}
	return fmt.Errorf(
		"restore re-deferred header marker %q after %d attempts: %w",
		syncKey,
		deferredMarkerRestoreMaxAttempts,
		restoreErr,
	)
}

// repopulateDeferredHeaderValidation rebuilds the in-memory deferred-header set
// from the persisted markers at startup (issue #3727, finding 3), so the
// snapshot retention floor (PrunePoolSnapshotsWithRetentionFloor) covers
// headers still awaiting apply from before the restart -- otherwise the first
// post-restart epoch cleanup could prune a pool-stake snapshot such a header
// needs. Abandoned markers loaded here are harmless: the retention guard evicts
// any whose slot the apply cursor has already passed on its next run.
//
// This MUST fail closed. A scan failure that is swallowed leaves the in-memory
// set empty, so the retention floor does not cover pre-restart deferred headers
// and the first post-restart cleanup can prune a snapshot one of them needs.
// Once the apply cursor then passes that header, stateful verification finds
// the snapshot gone and hard-rejects it instead of deferring -- the exact
// misclassification this PR exists to prevent. Apply-time re-validation from
// the per-point marker cannot save it: the snapshot is already gone. So a load
// failure is surfaced to LedgerState.Start, which aborts startup; the operator
// retries rather than running with an unpinned retention floor (issue #3727,
// finding: swallowed marker-scan failure reopens the pruned-snapshot bug).
func (ls *LedgerState) repopulateDeferredHeaderValidation() error {
	if ls.db == nil || ls.db.Metadata() == nil {
		return nil
	}
	keys, err := ls.db.ListSyncStateKeysByPrefix(
		deferredHeaderValidationSyncStatePrefix,
		nil,
	)
	if err != nil {
		return fmt.Errorf(
			"repopulate deferred-header set from persisted markers: %w",
			err,
		)
	}
	if len(keys) == 0 {
		return nil
	}
	ls.deferredHeaderValidationMu.Lock()
	if ls.deferredHeaderValidation == nil {
		ls.deferredHeaderValidation = make(map[string]struct{}, len(keys))
	}
	restored := 0
	for _, syncKey := range keys {
		mapKey := strings.TrimPrefix(
			syncKey,
			deferredHeaderValidationSyncStatePrefix,
		)
		if mapKey == "" || mapKey == syncKey {
			continue
		}
		ls.deferredHeaderValidation[mapKey] = struct{}{}
		restored++
	}
	ls.deferredHeaderValidationMu.Unlock()
	if restored > 0 {
		ls.config.Logger.Info(
			"repopulated deferred-header set from persisted markers",
			"count", restored,
			"component", "ledger",
		)
	}
	return nil
}

func (ls *LedgerState) persistDeferredHeaderValidation(
	point ocommon.Point,
	txn *database.Txn,
) error {
	if ls.db == nil || ls.db.Metadata() == nil {
		return nil
	}
	if err := ls.db.SetSyncState(
		deferredHeaderValidationSyncStateKey(point),
		deferredHeaderValidationSyncStateValue,
		txn,
	); err != nil {
		return fmt.Errorf("set deferred header validation marker: %w", err)
	}
	return nil
}

func (ls *LedgerState) clearPersistentDeferredHeaderValidation(
	point ocommon.Point,
	txn *database.Txn,
) error {
	if ls.db == nil || ls.db.Metadata() == nil {
		return nil
	}
	if err := ls.db.DeleteSyncState(
		deferredHeaderValidationSyncStateKey(point),
		txn,
	); err != nil {
		return fmt.Errorf("delete deferred header validation marker: %w", err)
	}
	return nil
}

func (ls *LedgerState) deferredHeaderValidationRequired(
	point ocommon.Point,
	txn *database.Txn,
) (bool, error) {
	required := ls.consumeDeferredHeaderValidation(point)
	if ls.db == nil || ls.db.Metadata() == nil {
		return required, nil
	}
	value, err := ls.db.GetSyncState(
		deferredHeaderValidationSyncStateKey(point),
		txn,
	)
	if err != nil {
		return false, fmt.Errorf(
			"read deferred header validation marker: %w",
			err,
		)
	}
	return required || value == deferredHeaderValidationSyncStateValue, nil
}

func (ls *LedgerState) verifyDeferredBlockHeaderState(
	txn *database.Txn,
	point ocommon.Point,
	block gledger.Block,
) error {
	required, err := ls.deferredHeaderValidationRequired(point, txn)
	if err != nil {
		return err
	}
	if !required {
		return nil
	}
	if err := ls.verifyBlockHeaderStateWithEpochAdvance(block, true, false); err != nil {
		// Typed so the pipeline can rewind past this block instead of
		// restarting onto it forever: the block is already persisted, so
		// this failure is deterministic rather than transient. The block is
		// still rejected — see headerValidationError.
		return &headerValidationError{
			BlockPoint: point,
			Cause: fmt.Errorf(
				"deferred block header verification failed at slot %d: %w",
				point.Slot,
				err,
			),
		}
	}
	if err := ls.clearPersistentDeferredHeaderValidation(point, txn); err != nil {
		return err
	}
	return nil
}

func (ls *LedgerState) handleEventBlockfetch(evt event.Event) {
	// Registered before the mutex is taken so defer's LIFO order runs it
	// after the unlock. RecoverAfterLocalRollback nests this mutex inside
	// chainsyncMutex, so publishing while holding it deadlocks the same
	// way. See pendingPublishes.
	var pending pendingPublishes
	defer pending.flush()
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	e, ok := evt.Data.(BlockfetchEvent)
	if !ok {
		ls.logUnexpectedChainsyncEventData("BlockfetchEvent", evt)
		return
	}
	if e.BatchDone {
		if err := ls.handleEventBlockfetchBatchDone(e, &pending); err != nil {
			ls.config.Logger.Error(
				"failed to handle blockfetch batch done",
				"component", "ledger",
				"error", err,
			)
			pending.add(
				ls.config.EventBus,
				LedgerErrorEventType,
				event.NewEvent(
					LedgerErrorEventType,
					LedgerErrorEvent{
						Error:     err,
						Operation: "blockfetch_batch_done",
					},
				),
			)
		}
	} else if e.Block != nil {
		if err := ls.handleEventBlockfetchBlockDeferred(e, &pending); err != nil {
			if strings.Contains(
				err.Error(),
				"block header crypto verification failed",
			) && ls.config.EventBus != nil {
				ls.config.Logger.Warn(
					"recycling connection after header verification failure",
					"component", "ledger",
					"connection_id", e.ConnectionId.String(),
					"slot", e.Point.Slot,
					"hash", hex.EncodeToString(e.Point.Hash),
				)
				pending.add(
					ls.config.EventBus,
					ConnectionRecycleRequestedEventType,
					event.NewEvent(
						ConnectionRecycleRequestedEventType,
						ConnectionRecycleRequestedEvent{
							ConnectionId: e.ConnectionId,
							Reason:       "block_header_verification_failure",
						},
					),
				)
			}
			ls.config.Logger.Error(
				"failed to handle block",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			pending.add(
				ls.config.EventBus,
				LedgerErrorEventType,
				event.NewEvent(
					LedgerErrorEventType,
					LedgerErrorEvent{
						Error:     err,
						Operation: "blockfetch_block",
						Point:     e.Point,
					},
				),
			)
		}
	}
}

func (ls *LedgerState) logUnexpectedChainsyncEventData(
	expectedType string,
	evt event.Event,
) {
	ls.config.Logger.Warn(
		"received unexpected event data type",
		"component", "ledger",
		"expected", expectedType,
		"data_type", fmt.Sprintf("%T", evt.Data),
		"event_type", evt.Type,
		"event_timestamp", evt.Timestamp,
		"event", evt,
	)
}

func (ls *LedgerState) handleChainSwitchEvent(evt event.Event) {
	e, ok := evt.Data.(chainselection.ChainSwitchEvent)
	if !ok {
		return
	}
	// Registered before the mutex is taken so defer's LIFO order runs it
	// after the unlock. See pendingPublishes.
	var pending pendingPublishes
	defer pending.flush()
	var replayConnId ouroboros.ConnectionId
	effectiveConnId := e.NewConnectionId
	var effectiveObservedTip ochainsync.Tip
	var requestFreshCursor bool
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	ls.chainsyncBlockfetchMutex.Lock()
	if ls.config.GetActiveConnectionFunc != nil {
		if !ls.isConnectionLive(effectiveConnId) {
			activeConnId := ls.config.GetActiveConnectionFunc()
			if activeConnId == nil || !ls.isConnectionLive(*activeConnId) {
				ls.chainsyncBlockfetchMutex.Unlock()
				ls.clearActiveUpstream()
				ls.config.Logger.Warn(
					"ignoring chain switch without a live connection",
					"component", "ledger",
					"connection_id", e.NewConnectionId.String(),
				)
				return
			}
			ls.config.Logger.Info(
				"chain switch target is not live, using live active best peer",
				"component", "ledger",
				"requested_connection_id", effectiveConnId.String(),
				"active_connection_id", activeConnId.String(),
			)
			ls.clearQueuedHeaders()
			effectiveConnId = *activeConnId
		}
	}
	replayConnId, err := ls.handoffPipelineOnSwitchLocked(
		effectiveConnId,
		&pending,
	)
	if err != nil {
		// The target connection may have closed between chain selection
		// and event processing. Retry with the current active best peer
		// before giving up.
		if ls.config.GetActiveConnectionFunc != nil {
			if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil &&
				!sameConnectionId(*activeConnId, effectiveConnId) {
				ls.config.Logger.Info(
					"chain switch target unavailable, retrying with active best peer",
					"component",
					"ledger",
					"failed_connection_id",
					effectiveConnId.String(),
					"active_connection_id",
					activeConnId.String(),
					"error",
					err,
				)
				if retryConnId, retryErr := ls.handoffPipelineOnSwitchLocked(*activeConnId, &pending); retryErr == nil {
					replayConnId = retryConnId
					effectiveConnId = *activeConnId
					err = nil
				}
			}
		}
		if err != nil {
			ls.config.Logger.Warn(
				"failed to hand off chainsync pipeline on chain switch, resetting pipeline",
				"component",
				"ledger",
				"connection_id",
				e.NewConnectionId.String(),
				"error",
				err,
			)
			// Clear orphaned headers and stale connection refs so the
			// pipeline can accept headers from reconnected peers instead
			// of stalling permanently.
			ls.clearQueuedHeaders()
			ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
		}
	}
	if err == nil {
		effectiveObservedTip, _ = ls.chainSwitchObservedTipForConnection(
			e,
			effectiveConnId,
		)
		requestFreshCursor = ls.chainSwitchNeedsFreshCursorLocked(
			e,
			effectiveConnId,
		)
	}
	ls.chainsyncBlockfetchMutex.Unlock()
	if err != nil {
		return
	}
	if ls.config.GetActiveConnectionFunc != nil {
		if !ls.isConnectionLive(effectiveConnId) {
			ls.clearActiveUpstream()
			return
		}
	}
	ls.publishActiveUpstream(effectiveConnId)
	if requestFreshCursor {
		ls.config.Logger.Info(
			"chain switch selected peer is ahead without queued headers, requesting fresh chainsync cursor",
			"component",
			"ledger",
			"connection_id",
			effectiveConnId.String(),
			"switch_connection_id",
			e.NewConnectionId.String(),
			"local_tip_slot",
			ls.PrimaryChainTip().Point.Slot,
			"peer_tip_slot",
			effectiveObservedTip.Point.Slot,
		)
		ls.requestChainsyncResync(
			effectiveConnId,
			event.ChainsyncResyncReasonChainSwitchCursorAhead,
			&pending,
		)
		return
	}
	if connIdKey(replayConnId) != "" {
		ls.replayBufferedHeadersAsync(replayConnId)
	}
}

func (ls *LedgerState) handleConnectionClosedEvent(evt event.Event) {
	e, ok := evt.Data.(ConnectionClosedEvent)
	if !ok {
		return
	}
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	if sameConnectionId(ls.selectedBlockfetchConnId, e.ConnectionId) {
		ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
	}
	if sameConnectionId(ls.shadowBlockfetchConnId, e.ConnectionId) {
		ls.shadowBlockfetchConnId = ouroboros.ConnectionId{}
	}
	ls.bufferedHeaderMutex.Lock()
	delete(ls.bufferedHeaderEvents, connIdKey(e.ConnectionId))
	ls.bufferedHeaderMutex.Unlock()
	delete(ls.peerHeaderHistory, connIdKey(e.ConnectionId))
	// Cancel in-flight blockfetch if the dead connection owns it.
	// Without this, chainsyncBlockfetchReadyChan stays non-nil and
	// new headers from reconnected peers are queued behind a batch
	// that will never complete, causing a permanent pipeline stall.
	if sameConnectionId(ls.activeBlockfetchConnId, e.ConnectionId) &&
		ls.chainsyncBlockfetchReadyChan != nil {
		ls.config.Logger.Info(
			"canceling blockfetch on closed connection",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
		)
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	}
	if sameConnectionId(ls.headerPipelineConnId, e.ConnectionId) {
		ls.clearQueuedHeaders()
	}
	if ls.config.GetActiveConnectionFunc != nil {
		activeConnId := ls.config.GetActiveConnectionFunc()
		if activeConnId == nil ||
			sameConnectionId(*activeConnId, e.ConnectionId) {
			ls.clearActiveUpstream()
		}
	}
	// Keep the admitted-header frontier as a monotonic high-water mark across
	// disconnects. Consumers hide it while no active connection exists, but a
	// reconnect must not replace it with an older peer's first admitted header
	// and weaken the forging sync gate.
}

func (ls *LedgerState) handleEventChainsyncAwaitReply(evt event.Event) {
	// See pendingPublishes: nothing may be published while
	// ls.chainsyncMutex is held.
	var pending pendingPublishes
	defer pending.flush()
	e, ok := evt.Data.(ChainsyncAwaitReplyEvent)
	if !ok {
		ls.logUnexpectedChainsyncEventData(
			"ChainsyncAwaitReplyEvent",
			evt,
		)
		return
	}
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	if !ls.isConnectionLive(e.ConnectionId) {
		ls.config.Logger.Debug(
			"ignoring await-reply event from closed connection",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
		)
		return
	}
	if ls.chain == nil || ls.chain.HeaderCount() == 0 {
		return
	}
	if ls.config.GetActiveConnectionFunc == nil {
		return
	}
	activeConnId := ls.config.GetActiveConnectionFunc()
	if activeConnId == nil ||
		!sameConnectionId(*activeConnId, e.ConnectionId) {
		return
	}
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	if ls.chainsyncBlockfetchReadyChan != nil ||
		ls.blockfetchContinuationPending ||
		ls.chain.HeaderCount() == 0 {
		return
	}
	ls.selectedBlockfetchConnId = e.ConnectionId
	ls.config.Logger.Debug(
		"selected chainsync peer entered await reply, flushing queued headers to blockfetch",
		"component",
		"ledger",
		"connection_id",
		e.ConnectionId.String(),
		"header_count",
		ls.chain.HeaderCount(),
	)
	if err := ls.startQueuedBlockfetchLocked(e.ConnectionId, &pending); err != nil {
		ls.config.Logger.Error(
			"failed to start blockfetch after await reply",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"error", err,
		)
		pending.add(
			ls.config.EventBus,
			LedgerErrorEventType,
			event.NewEvent(
				LedgerErrorEventType,
				LedgerErrorEvent{
					Error:     err,
					Operation: "await_reply_blockfetch",
				},
			),
		)
	}
}

// detectConnectionSwitch checks for an active connection change and logs a
// summary of dropped rollback events when a switch is detected. It returns the
// current active connection ID and whether connection filtering is configured.
// When configured is false, callers should skip all connection-based filtering.
func (ls *LedgerState) detectConnectionSwitch(
	pending *pendingPublishes,
) (
	activeConnId *ouroboros.ConnectionId,
	configured bool,
) {
	if ls.config.GetActiveConnectionFunc == nil {
		return nil, false
	}
	activeConnId = ls.config.GetActiveConnectionFunc()
	if activeConnId != nil &&
		(ls.lastActiveConnId == nil ||
			!sameConnectionId(*ls.lastActiveConnId, *activeConnId)) {
		if ls.lastActiveConnId != nil {
			ls.config.Logger.Info(
				"active connection changed",
				"component", "ledger",
				"previous_connection_id", ls.lastActiveConnId.String(),
				"new_connection_id", activeConnId.String(),
				"dropped_rollbacks", ls.dropRollbackCount,
			)
			ls.dropRollbackCount = 0
			ls.headerMismatchCount = 0
			ls.chainsyncBlockfetchMutex.Lock()
			replayConnId, err := ls.handoffPipelineOnSwitchLocked(
				*activeConnId,
				pending,
			)
			if err != nil {
				ls.config.Logger.Warn(
					"failed to hand off chainsync pipeline after active connection change, resetting pipeline",
					"component",
					"ledger",
					"connection_id",
					activeConnId.String(),
					"error",
					err,
				)
				ls.clearQueuedHeaders()
				ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
			}
			ls.chainsyncBlockfetchMutex.Unlock()
			if err == nil && connIdKey(replayConnId) != "" {
				ls.replayBufferedHeadersAsync(replayConnId)
			}
			// Clear per-connection state (e.g., header dedup cache)
			// so the new connection can re-deliver blocks from the
			// intersection without them being filtered as duplicates.
			if ls.config.ConnectionSwitchFunc != nil {
				ls.config.ConnectionSwitchFunc()
			}
		}
		ls.lastActiveConnId = activeConnId
		// Preserve rollbackHistory across connection switches so the
		// loop detector can still catch repeated rollbacks from the
		// same peer/session. The detector keys on exact rollback
		// point + connection, so peer switches do not poison healthy
		// fork convergence.
	}
	// Re-read the authoritative selection after handoff. A close or switch can
	// occur while the handoff is running; never reactivate the retained frontier
	// for the connection that was live only at the start of this function.
	if ls.config.GetActiveConnectionFunc != nil {
		activeConnId = ls.config.GetActiveConnectionFunc()
		if activeConnId != nil && !ls.isConnectionLive(*activeConnId) {
			activeConnId = nil
		}
	}
	if activeConnId != nil {
		// A new selection starts with an unknown target. Only admitted header
		// work may publish a peer-advertised target.
		ls.publishActiveUpstream(*activeConnId)
	} else {
		ls.clearActiveUpstream()
	}
	return activeConnId, true
}

func (ls *LedgerState) handoffPipelineOnSwitchLocked(
	newConnId ouroboros.ConnectionId,
	pending *pendingPublishes,
) (ouroboros.ConnectionId, error) {
	ls.selectedBlockfetchConnId = newConnId
	headerCount := 0
	if ls.chain != nil {
		headerCount = ls.chain.HeaderCount()
	}

	if connIdKey(newConnId) == "" {
		return ouroboros.ConnectionId{}, nil
	}

	ls.bufferedHeaderMutex.Lock()
	hasBufferedHeadersForNewConn := len(
		ls.bufferedHeaderEvents[connIdKey(newConnId)],
	) > 0
	ls.bufferedHeaderMutex.Unlock()

	// When a blockfetch batch is already in progress on a different connection,
	// let it complete rather than canceling it. The fetched blocks are canonical
	// regardless of which peer serves them. Canceling here when peers alternate
	// rapidly (equal-tip switching) prevents blockfetch from ever completing.
	// The selectedBlockfetchConnId update above ensures the NEXT batch uses the
	// new connection after the current one finishes.

	if connIdKey(ls.headerPipelineConnId) != "" &&
		!sameConnectionId(ls.headerPipelineConnId, newConnId) {
		if ls.chainsyncBlockfetchReadyChan == nil &&
			headerCount > 0 &&
			hasBufferedHeadersForNewConn {
			ls.config.Logger.Debug(
				"dropping stale queued header fragment on chain switch",
				"component", "ledger",
				"previous_owner_connection_id",
				ls.headerPipelineConnId.String(),
				"new_connection_id", newConnId.String(),
				"queued_headers", headerCount,
			)
			ls.clearQueuedHeaders()
			return newConnId, nil
		}
		ls.config.Logger.Debug(
			"releasing stale header pipeline owner on chain switch",
			"component", "ledger",
			"previous_owner_connection_id",
			ls.headerPipelineConnId.String(),
			"new_connection_id", newConnId.String(),
			"queued_headers", headerCount,
		)
		ls.headerPipelineConnId = ouroboros.ConnectionId{}
		// Purge the header dedup cache for slots beyond the current
		// block tip. The new connection may have already delivered
		// headers that were deduplicated against the old owner's
		// headers at the ouroboros layer. Without purging, those
		// headers can never be re-delivered, leaving a gap that
		// stalls the pipeline until genuinely new blocks arrive.
		if ls.config.ClearSeenHeadersFromFunc != nil {
			ls.config.ClearSeenHeadersFromFunc(ls.Tip().Point.Slot)
		}
	}

	if ls.chainsyncBlockfetchReadyChan == nil &&
		!ls.blockfetchContinuationPending &&
		headerCount > 0 {
		ls.config.Logger.Debug(
			"restarting queued blockfetch on selected connection",
			"component", "ledger",
			"connection_id", newConnId.String(),
			"header_count", headerCount,
		)
		if err := ls.startQueuedBlockfetchLocked(newConnId, pending); err != nil {
			return ouroboros.ConnectionId{}, fmt.Errorf(
				"restart queued blockfetch on switch: %w",
				err,
			)
		}
		return ouroboros.ConnectionId{}, nil
	}

	if ls.chainsyncBlockfetchReadyChan == nil &&
		hasBufferedHeadersForNewConn {
		return newConnId, nil
	}

	return ouroboros.ConnectionId{}, nil
}

func (ls *LedgerState) chainSwitchNeedsFreshCursorLocked(
	e chainselection.ChainSwitchEvent,
	connId ouroboros.ConnectionId,
) bool {
	if connIdKey(e.PreviousConnectionId) == "" ||
		connIdKey(connId) == "" {
		return false
	}
	if ls.chainsyncBlockfetchReadyChan != nil {
		return false
	}
	if ls.chain != nil && ls.chain.HeaderCount() > 0 {
		return false
	}
	ls.bufferedHeaderMutex.Lock()
	hasBuffered := len(ls.bufferedHeaderEvents[connIdKey(connId)]) > 0
	ls.bufferedHeaderMutex.Unlock()
	if hasBuffered {
		return false
	}
	newObservedTip, ok := ls.chainSwitchObservedTipForConnection(e, connId)
	if !ok {
		return false
	}
	localTip := ls.PrimaryChainTip()
	if newObservedTip.BlockNumber > localTip.BlockNumber {
		return true
	}
	return newObservedTip.BlockNumber == localTip.BlockNumber &&
		newObservedTip.Point.Slot > localTip.Point.Slot
}

func (ls *LedgerState) chainSwitchObservedTipForConnection(
	e chainselection.ChainSwitchEvent,
	connId ouroboros.ConnectionId,
) (ochainsync.Tip, bool) {
	if sameConnectionId(connId, e.NewConnectionId) {
		return chainSwitchNewObservedTip(e), true
	}
	if ls.config.GetPeerObservedTipFunc == nil {
		return ochainsync.Tip{}, false
	}
	return ls.config.GetPeerObservedTipFunc(connId)
}

// chainSwitchNewObservedTip returns the peer frontier that chain selection
// actually compared.
//
// The fallback is keyed on NewObservedTipSet, not on the frontier being
// zero-valued. A zero frontier is a real observation meaning the peer delivered
// nothing, which is exactly the advertising-only peer this path must not trust:
// inferring "absent" from it handed such a peer's advertised outlier to ledger
// cursor recovery. Only a producer that never populated the field -- an older
// event, or a direct unit-test or integration constructor -- falls back to the
// advertised tip.
func chainSwitchNewObservedTip(
	e chainselection.ChainSwitchEvent,
) ochainsync.Tip {
	if e.NewObservedTipSet {
		return e.NewObservedTip
	}
	return e.NewTip
}

func (ls *LedgerState) bufferHeaderEvent(e ChainsyncEvent) {
	// Reached from the chainsync dispatch goroutine, which holds only
	// chainsyncMutex: claimHeaderPipelineOwnership released
	// chainsyncBlockfetchMutex on return, so this write is otherwise
	// unprotected against nextBufferedHeaderConnId's iteration.
	ls.bufferedHeaderMutex.Lock()
	defer ls.bufferedHeaderMutex.Unlock()
	if ls.bufferedHeaderEvents == nil {
		ls.bufferedHeaderEvents = make(
			map[string][]ChainsyncEvent,
		)
	}
	key := connIdKey(e.ConnectionId)
	events := ls.bufferedHeaderEvents[key]
	if len(events) > 0 {
		last := events[len(events)-1]
		if last.Point.Slot == e.Point.Slot &&
			bytes.Equal(last.Point.Hash, e.Point.Hash) {
			return
		}
	}
	const maxBufferedHeadersPerConn = 128
	if len(events) >= maxBufferedHeadersPerConn {
		events = append(events[1:], e)
	} else {
		events = append(events, e)
	}
	ls.bufferedHeaderEvents[key] = events
}

func (ls *LedgerState) clearQueuedHeaders() {
	ls.chain.ClearHeaders()
	// The blockfetch range-failure record is deliberately NOT cleared here.
	// Fork resolution, connection switches and header mismatches clear the
	// queue constantly, and the peer then re-offers the same unfetchable
	// header; forgetting the record on every clear is what stopped the
	// bound from ever being reached.
	ls.headerPipelineConnId = ouroboros.ConnectionId{}
	// Purge the header dedup cache for slots beyond the current
	// block tip. Queued headers that were recorded in the dedup
	// cache but just discarded would otherwise block re-delivery
	// on subsequent connections, causing a permanent pipeline stall.
	if ls.config.ClearSeenHeadersFromFunc != nil {
		ls.config.ClearSeenHeadersFromFunc(ls.Tip().Point.Slot)
	}
}

func (ls *LedgerState) recordPeerHeaderHistory(e ChainsyncEvent) {
	if e.BlockHeader == nil || len(e.Point.Hash) == 0 {
		return
	}
	if ls.peerHeaderHistory == nil {
		ls.peerHeaderHistory = make(map[string]*peerHeaderChain)
	}
	key := connIdKey(e.ConnectionId)
	history := ls.peerHeaderHistory[key]
	if history == nil {
		history = &peerHeaderChain{
			order: make([]string, 0, maxPeerHeaderHistoryPerConn),
			byHash: make(map[string]peerHeaderRecord,
				maxPeerHeaderHistoryPerConn),
		}
		ls.peerHeaderHistory[key] = history
	}
	hashKey := hex.EncodeToString(e.Point.Hash)
	if _, ok := history.byHash[hashKey]; ok {
		return
	}
	headerCbor := append([]byte(nil), e.BlockHeader.Cbor()...)
	prevHash := append([]byte(nil), e.BlockHeader.PrevHash().Bytes()...)
	decodeType := e.Type
	// Musashi carries the Dijkstra header extension under the Conway wire
	// block type. Preserve the decoder selected by the protocol callback when
	// the compact record is rehydrated after a fork.
	if e.BlockHeader.Era().Id == gledger.EraIdDijkstra {
		decodeType = gledger.BlockTypeDijkstra
	}
	recordBytes := peerHeaderHistoryRecordOverhead +
		len(headerCbor) + len(prevHash) + len(e.Point.Hash)
	if recordBytes > maxPeerHeaderHistoryBytesPerConn {
		return
	}
	for len(history.order) > 0 &&
		(history.retainedBytes+recordBytes > maxPeerHeaderHistoryBytesPerConn ||
			len(history.order) >= ls.peerHeaderHistoryLimit()) {
		evictKey := history.order[0]
		history.order = history.order[1:]
		if evicted, ok := history.byHash[evictKey]; ok {
			history.retainedBytes -= evicted.bytes
			delete(history.byHash, evictKey)
		}
	}
	metadata := ChainsyncEvent{
		ConnectionId: e.ConnectionId,
		Point: ocommon.Point{
			Slot: e.Point.Slot,
			Hash: append([]byte(nil), e.Point.Hash...),
		},
		BlockNumber: e.BlockNumber,
		Type:        e.Type,
	}
	if len(headerCbor) == 0 {
		// Synthetic headers used by some in-package callers do not expose CBOR.
		// Keep their decoded value, but charge the same conservative record
		// overhead so this compatibility path cannot bypass the bound.
		metadata.BlockHeader = e.BlockHeader
	}
	record := peerHeaderRecord{
		event:      metadata,
		headerCbor: headerCbor,
		prevHash:   prevHash,
		decodeType: decodeType,
		bytes:      recordBytes,
	}
	history.order = append(history.order, hashKey)
	history.byHash[hashKey] = record
	history.retainedBytes += recordBytes
}

func (r peerHeaderRecord) chainsyncEvent() (ChainsyncEvent, bool) {
	if r.event.BlockHeader != nil {
		return r.event, true
	}
	decodeType := r.decodeType
	if decodeType == 0 {
		decodeType = r.event.Type
	}
	header, err := gledger.NewBlockHeaderFromCbor(decodeType, r.headerCbor)
	if err != nil {
		return ChainsyncEvent{}, false
	}
	r.event.BlockHeader = header
	return r.event, true
}

func (ls *LedgerState) genesisSelectionState() (bool, uint64) {
	if ls.config.GenesisSelectionStateFunc == nil {
		return false, 0
	}
	active, window := ls.config.GenesisSelectionStateFunc()
	return active && window > 0, window
}

func (ls *LedgerState) peerHeaderHistoryLimit() int {
	limit := maxPeerHeaderHistoryPerConn
	active, window := ls.genesisSelectionState()
	if !active || window <= uint64(limit) {
		return limit
	}
	if window > uint64(math.MaxInt) {
		return math.MaxInt
	}
	// The MaxInt check above makes this conversion safe on both 32- and
	// 64-bit platforms.
	return int(window) //nolint:gosec // G115: window is bounded by MaxInt
}

// headerAtOrImmediatelyBeforeTip recognizes only points already accepted into
// the current queued chain: its exact tip or the tip's direct observed parent.
// Other earlier headers may be genuine competing candidates and must continue
// through fork resolution.
func (ls *LedgerState) headerAtOrImmediatelyBeforeTip(
	e ChainsyncEvent,
) bool {
	headerTip := ls.chain.HeaderTip()
	if pointMatches(e.Point, headerTip.Point) {
		return true
	}
	if len(e.Point.Hash) == 0 || len(headerTip.Point.Hash) == 0 {
		return false
	}
	tipHashKey := hex.EncodeToString(headerTip.Point.Hash)
	for _, history := range ls.peerHeaderHistory {
		tipRecord, ok := history.byHash[tipHashKey]
		if ok && bytes.Equal(tipRecord.prevHash, e.Point.Hash) {
			return true
		}
	}
	return false
}

// headerAlreadyOnPrimaryChain identifies a replayed header that is already
// present on the authoritative primary chain but is behind its current tip.
// This shape is normal while a from-genesis ledger catches up after restart:
// the block store can be far ahead of the applied ledger, and a peer may
// replay a historical header while the local chain tip has already advanced.
// It is not a fork and must not contribute to headerMismatchCount.
//
// Only a point confirmed present in the primary-chain index suppresses fork
// handling. A lookup failure is not evidence of a duplicate, so it is logged
// and reported as a non-match.
//
// The caller holds chainsyncMutex, so the guards below keep storage reads off
// paths that cannot need them. The origin point carries no hash, and a header
// beyond the localTip snapshot is not observed in the primary-chain index at
// handler entry. The O(1) local hash index then rejects an unknown fork header
// before a point lookup could fall through to a configured Bark archive. On a
// hash-index hit, the returned block ID is checked through the point-only
// primary-chain index path, avoiding a second block-CBOR read. A hash-index
// miss also probes the exact local point key for pre-index databases, but that
// bounded compatibility lookup cannot fall through to Bark's archive.
func (ls *LedgerState) headerAlreadyOnPrimaryChain(
	e ChainsyncEvent,
	localTip ochainsync.Tip,
) bool {
	if ls.db == nil || len(e.Point.Hash) == 0 {
		return false
	}
	if e.Point.Slot > localTip.Point.Slot {
		return false
	}
	block, err := ls.blockByHash(e.Point.Hash)
	if errors.Is(err, models.ErrBlockNotFound) {
		// Blocks written before the hash index was introduced still have an
		// exact point key and metadata. Probe that local path without allowing
		// Bark to fall through to its archive on an unknown fork hash.
		blockID, localErr := database.BlockIDByPointLocal(ls.db, e.Point)
		if errors.Is(localErr, models.ErrBlockNotFound) {
			return false
		}
		if localErr != nil {
			ls.config.Logger.Debug(
				"could not check legacy historical header by local point",
				"component", "ledger",
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
				"error", localErr,
			)
			return false
		}
		contains, localErr := ls.primaryChainContainsBlockID(blockID, e.Point)
		if localErr != nil {
			ls.config.Logger.Debug(
				"could not check legacy historical header against primary chain",
				"component",
				"ledger",
				"slot",
				e.Point.Slot,
				"hash",
				hex.EncodeToString(e.Point.Hash),
				"error",
				localErr,
			)
			return false
		}
		return contains
	}
	if err != nil {
		ls.config.Logger.Debug(
			"could not prefilter historical header by hash index",
			"component", "ledger",
			"slot", e.Point.Slot,
			"hash", hex.EncodeToString(e.Point.Hash),
			"error", err,
		)
		return false
	}
	contains, err := ls.primaryChainContainsBlock(block, e.Point)
	if err != nil {
		ls.config.Logger.Debug(
			"could not check historical header against primary chain",
			"component", "ledger",
			"slot", e.Point.Slot,
			"hash", hex.EncodeToString(e.Point.Hash),
			"error", err,
		)
		return false
	}
	return contains
}

func (ls *LedgerState) findPeerForkPath(
	e ChainsyncEvent,
	initialPrevHash []byte,
) (*ocommon.Point, []ChainsyncEvent, error) {
	prevHash := append([]byte(nil), initialPrevHash...)
	history := ls.peerHeaderHistory[connIdKey(e.ConnectionId)]
	pathReversed := []ChainsyncEvent{e}
	visited := map[string]struct{}{
		hex.EncodeToString(e.Point.Hash): {},
	}
	limit := ls.peerHeaderHistoryLimit()
	for depth := 0; depth < limit &&
		len(prevHash) > 0; depth++ {
		// Resolve the ancestor by O(1) hash index only: database.BlockByHash
		// no longer falls back to a sequential blob scan on a miss, so this
		// lock-held walk over mostly-unpersisted peer-header hashes stays cheap
		// for current stores. Blocks persisted before the hash index was added
		// may still miss until the operator backfills the index.
		ancestorBlock, err := ls.blockByHash(prevHash)
		if err == nil {
			point := ocommon.NewPoint(
				ancestorBlock.Slot,
				ancestorBlock.Hash,
			)
			slices.Reverse(pathReversed)
			return &point, pathReversed, nil
		}
		if !errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil, fmt.Errorf(
				"lookup ancestor hash %x: %w",
				prevHash,
				err,
			)
		}
		hashKey := hex.EncodeToString(prevHash)
		var (
			record peerHeaderRecord
			ok     bool
		)
		if history != nil {
			record, ok = history.byHash[hashKey]
		}
		if !ok && ls.config.PeerHeaderLookupFunc != nil {
			lookupEvent, lookupPrevHash, found := ls.config.PeerHeaderLookupFunc(
				e.ConnectionId,
				prevHash,
			)
			if found {
				record = peerHeaderRecord{
					event:    lookupEvent,
					prevHash: lookupPrevHash,
				}
				ok = true
			}
		}
		if !ok {
			return nil, nil, nil
		}
		recordEvent, ok := record.chainsyncEvent()
		if !ok {
			// A retained header that cannot be reconstructed is treated like a
			// missing history entry. Re-intersection is safer than comparing or
			// replaying an incomplete candidate path.
			return nil, nil, nil
		}
		if _, seen := visited[hashKey]; seen {
			return nil, nil, nil
		}
		visited[hashKey] = struct{}{}
		pathReversed = append(pathReversed, recordEvent)
		prevHash = append(prevHash[:0], record.prevHash...)
	}
	return nil, nil, nil
}

func (ls *LedgerState) localGenesisDensity(
	ancestorPoint ocommon.Point,
	window uint64,
) (uint64, error) {
	iter, err := ls.chain.FromPoint(ancestorPoint, false)
	if err != nil {
		return 0, fmt.Errorf(
			"iterate local candidate after intersection %d: %w",
			ancestorPoint.Slot,
			err,
		)
	}
	defer iter.Cancel()

	windowEnd := ancestorPoint.Slot + window
	if windowEnd < ancestorPoint.Slot {
		windowEnd = math.MaxUint64
	}
	slots := make([]uint64, 0)
	for {
		result, nextErr := iter.Next(false)
		if errors.Is(nextErr, chain.ErrIteratorChainTip) {
			break
		}
		if nextErr != nil {
			return 0, fmt.Errorf(
				"iterate local candidate after intersection %d: %w",
				ancestorPoint.Slot,
				nextErr,
			)
		}
		if result.Rollback {
			return 0, errors.New(
				"local candidate changed while computing Genesis density",
			)
		}
		if result.Point.Slot > windowEnd {
			break
		}
		slots = append(slots, result.Point.Slot)
	}
	return chainselection.DensityFromIntersection(
		ancestorPoint.Slot,
		window,
		slots,
	), nil
}

func genesisForkPathDensity(
	ancestorPoint ocommon.Point,
	window uint64,
	forkPath []ChainsyncEvent,
) uint64 {
	slots := make([]uint64, 0, len(forkPath))
	for _, forkEvent := range forkPath {
		slots = append(slots, forkEvent.Point.Slot)
	}
	return chainselection.DensityFromIntersection(
		ancestorPoint.Slot,
		window,
		slots,
	)
}

func (ls *LedgerState) blockByHash(hash []byte) (models.Block, error) {
	if ls.lookupBlockByHash != nil {
		return ls.lookupBlockByHash(hash)
	}
	return database.BlockByHash(ls.db, hash)
}

func netAddrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}

// connIdKey builds the key from nil-safe per-address strings because
// ConnectionId.String() panics when either net.Addr field is nil.
func connIdKey(connId ouroboros.ConnectionId) string {
	if connId.LocalAddr == nil && connId.RemoteAddr == nil {
		return ""
	}
	return netAddrString(connId.LocalAddr) +
		"<->" +
		netAddrString(connId.RemoteAddr)
}

func sameConnectionId(a, b ouroboros.ConnectionId) bool {
	return connIdKey(a) == connIdKey(b)
}

func desiredBlockfetchBatchHeaders(
	gapSlots uint64,
	gapBlocks uint64,
	maxHeaders int,
) int {
	if maxHeaders <= 0 {
		return 0
	}
	if gapBlocks == 0 {
		if gapSlots == 0 {
			return 0
		}
		return min(1, maxHeaders)
	}
	// Scale the header runway with the size of the catchup gap. A node
	// that's hundreds of blocks behind benefits from batching close to
	// the blockfetch protocol limit (500), while near-tip cases keep
	// small batches for low latency. The previous values (max 8 when
	// gapBlocks > 64) starved the blockfetch pipeline during catchup —
	// every blockfetch round-trip carried only a handful of blocks even
	// though `chain.HeaderRange(blockfetchBatchSize)` is willing to span
	// up to 500.
	var minHeaders int
	switch {
	case gapBlocks > 1000:
		minHeaders = 256
	case gapBlocks > 256:
		minHeaders = 128
	case gapBlocks > 64:
		minHeaders = 32
	case gapBlocks > 16:
		minHeaders = 8
	case gapBlocks > 4:
		minHeaders = 2
	default:
		minHeaders = int(gapBlocks)
	}
	minHeaders = min(minHeaders, blockfetchMaxBatchHeadersWhenBehind)
	return min(minHeaders, maxHeaders)
}

// requestChainsyncResync asks the chainsync layer to renegotiate an
// intersection with a peer.
//
// pending must be the caller's queue when ls.chainsyncMutex is held, and
// may be nil otherwise. This event's subscriber calls
// RecoverAfterLocalRollback, which takes that mutex, so publishing inline
// from under the lock is the deadlock pendingPublishes exists to break.
func (ls *LedgerState) requestChainsyncResync(
	connId ouroboros.ConnectionId,
	reason string,
	pending *pendingPublishes,
) {
	ls.headerMismatchCount = 0
	ls.rollbackHistory = nil
	ls.bufferedHeaderMutex.Lock()
	delete(ls.bufferedHeaderEvents, connIdKey(connId))
	ls.bufferedHeaderMutex.Unlock()
	pending.add(
		ls.config.EventBus,
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: connId,
				Reason:       reason,
			},
		),
	)
}

func (ls *LedgerState) staleSelectedOwnerWouldBufferHeader(
	e ChainsyncEvent,
) bool {
	return connIdKey(ls.selectedBlockfetchConnId) != "" &&
		ls.chainsyncBlockfetchReadyChan == nil &&
		(ls.chain == nil || ls.chain.HeaderCount() == 0) &&
		!sameConnectionId(ls.selectedBlockfetchConnId, e.ConnectionId)
}

func (ls *LedgerState) logIdleSelectedOwnerRelease(e ChainsyncEvent) {
	ls.config.Logger.Debug(
		"releasing idle selected blockfetch owner before header admission",
		"component", "ledger",
		"selected_connection_id", ls.selectedBlockfetchConnId.String(),
		"event_connection_id", e.ConnectionId.String(),
		"slot", e.Point.Slot,
	)
}

func (ls *LedgerState) clearIdleSelectedOwner() {
	if ls.chainsyncBlockfetchReadyChan == nil &&
		(ls.chain == nil || ls.chain.HeaderCount() == 0) {
		ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
	}
}

// claimHeaderPipelineOwnership atomically determines the current header
// pipeline owner for e and, for every outcome that changes
// headerPipelineConnId (and selectedBlockfetchConnId, for the
// compatible-fork case), writes it under the same chainsyncBlockfetchMutex
// hold as the read that decided it. Every other mutator of
// headerPipelineConnId (discardBufferedPeerHeaders, clearQueuedHeaders,
// handleEventBlockfetch's batch-done path, etc.) already holds this same
// mutex around its own reads/writes; splitting "decide the owner" from
// "write the decision" into two separate lock/unlock cycles (as this used
// to do, via the now-inlined currentHeaderPipelineOwner) left a window
// where one of those other mutators could run in between and have its
// change silently overwritten by this function's own, now-stale decision.
func (ls *LedgerState) claimHeaderPipelineOwnership(
	e ChainsyncEvent,
) (ownerConnId ouroboros.ConnectionId, shouldBuffer bool, acceptedDifferentConnection bool) {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()

	if ls.staleSelectedOwnerWouldBufferHeader(e) {
		ls.logIdleSelectedOwnerRelease(e)
		ls.clearIdleSelectedOwner()
	}

	var owner ouroboros.ConnectionId
	switch {
	case ls.chainsyncBlockfetchReadyChan != nil:
		if connIdKey(ls.headerPipelineConnId) != "" {
			owner = ls.headerPipelineConnId
		} else if connIdKey(ls.activeBlockfetchConnId) != "" {
			owner = ls.activeBlockfetchConnId
		} else {
			owner = ouroboros.ConnectionId{}
		}
	case ls.chain != nil && ls.chain.HeaderCount() > 0:
		owner = ls.headerPipelineConnId
	default:
		// Once the shared header queue drains, there is no live pipeline
		// owner. A stale selected blockfetch peer must not monopolize
		// future headers while the pipeline is idle; whichever peer
		// delivers the next usable header gets to seed the next batch.
		ls.headerPipelineConnId = ouroboros.ConnectionId{}
		owner = ouroboros.ConnectionId{}
	}

	if owner == (ouroboros.ConnectionId{}) {
		ls.headerPipelineConnId = e.ConnectionId
		return owner, false, false
	}
	if sameConnectionId(owner, e.ConnectionId) {
		ls.headerPipelineConnId = e.ConnectionId
		return owner, false, false
	}
	if ls.headerFitsCurrentPipeline(e) {
		ls.headerPipelineConnId = e.ConnectionId
		ls.selectedBlockfetchConnId = e.ConnectionId
		return owner, false, true
	}
	ls.headerPipelineConnId = owner
	return owner, true, false
}

func (ls *LedgerState) shouldBufferHeaderEvent(e ChainsyncEvent) bool {
	ownerConnId, shouldBuffer, acceptedDifferentConnection := ls.claimHeaderPipelineOwnership(
		e,
	)
	if acceptedDifferentConnection {
		ls.config.Logger.Debug(
			"accepting compatible header from different connection",
			"component", "ledger",
			"event_connection_id", e.ConnectionId.String(),
			"previous_owner_connection_id", ownerConnId.String(),
			"slot", e.Point.Slot,
		)
	}
	if !shouldBuffer {
		return false
	}
	ls.bufferHeaderEvent(e)
	ls.config.Logger.Debug(
		"buffering header from non-owner connection",
		"component", "ledger",
		"event_connection_id", e.ConnectionId.String(),
		"owner_connection_id", ownerConnId.String(),
		"slot", e.Point.Slot,
	)
	return true
}

func (ls *LedgerState) headerFitsCurrentPipeline(e ChainsyncEvent) bool {
	if ls.chain == nil || e.BlockHeader == nil {
		return false
	}
	prevHash := e.BlockHeader.PrevHash().Bytes()
	headerTip := ls.chain.HeaderTip()
	if len(headerTip.Point.Hash) == 0 {
		return len(prevHash) == 0
	}
	return bytes.Equal(prevHash, headerTip.Point.Hash)
}

func (ls *LedgerState) nextBufferedHeaderConnId() (
	ouroboros.ConnectionId,
	bool,
) {
	ls.bufferedHeaderMutex.Lock()
	defer ls.bufferedHeaderMutex.Unlock()
	if key := connIdKey(ls.selectedBlockfetchConnId); key != "" {
		if events := ls.bufferedHeaderEvents[key]; len(events) > 0 {
			return events[len(events)-1].ConnectionId, true
		}
	}
	var (
		bestConn ouroboros.ConnectionId
		bestTip  uint64
		found    bool
	)
	for _, events := range ls.bufferedHeaderEvents {
		if len(events) == 0 {
			continue
		}
		tipSlot := events[len(events)-1].Tip.Point.Slot
		if !found || tipSlot > bestTip {
			bestConn = events[len(events)-1].ConnectionId
			bestTip = tipSlot
			found = true
		}
	}
	return bestConn, found
}

func (ls *LedgerState) replayBufferedHeadersAsync(
	connId ouroboros.ConnectionId,
) {
	// Hold replayMu so Close cannot start Wait between our closed check
	// and Add(1); otherwise Wait could observe a zero counter and the
	// subsequent Add(1) would panic with "WaitGroup misuse: Add called
	// concurrently with Wait" (#2107).
	ls.replayMu.Lock()
	if ls.closed.Load() {
		ls.replayMu.Unlock()
		return
	}
	ls.replayWG.Add(1)
	ls.replayMu.Unlock()
	go func() {
		defer ls.replayWG.Done()
		var pending pendingPublishes
		defer pending.flush()
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		// Re-check after acquiring the mutex in case Close started
		// while we were waiting for the lock; the DB reads inside
		// handleEventChainsyncBlockHeader (BlockByHash etc.) will
		// panic with "DB Closed" once the owner closes the DB.
		if ls.closed.Load() {
			return
		}
		if ls.headerPipelineConnId != (ouroboros.ConnectionId{}) ||
			ls.chain.HeaderCount() > 0 {
			return
		}
		if err := ls.replayBufferedHeaderEvents(connId, &pending); err != nil {
			ls.config.Logger.Warn(
				"failed to replay buffered header events",
				"component", "ledger",
				"connection_id", connId.String(),
				"error", err,
			)
		}
	}()
}

func (ls *LedgerState) replayBufferedHeaderEvents(
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	key := connIdKey(connId)
	// Take the events and clear the entry under the lock, then replay
	// outside it: the replay calls back into
	// handleEventChainsyncBlockHeaderWithPending, which can buffer more
	// headers and would deadlock on a lock still held here.
	ls.bufferedHeaderMutex.Lock()
	if len(ls.bufferedHeaderEvents[key]) == 0 {
		ls.bufferedHeaderMutex.Unlock()
		return nil
	}
	events := append(
		[]ChainsyncEvent(nil),
		ls.bufferedHeaderEvents[key]...,
	)
	delete(ls.bufferedHeaderEvents, key)
	ls.bufferedHeaderMutex.Unlock()
	for _, evt := range events {
		if err := ls.handleEventChainsyncBlockHeaderWithPending(evt, pending); err != nil {
			return err
		}
	}
	return nil
}

// discardBufferedPeerHeaders is called from handleEventChainsync's dispatch
// goroutine, which holds only chainsyncMutex -- clearQueuedHeaders mutates
// headerPipelineConnId, which every other mutator (handleChainSwitchEvent,
// handleEventBlockfetch's batch-done path, etc.) guards with
// chainsyncBlockfetchMutex. Taking it here too, self-contained, closes that
// gap without widening handleEventBlockfetch's own critical section to
// cover chainsyncMutex as well.
//
// The bufferedHeaderEvents delete belongs inside that lock for the same
// reason: handleEventBlockfetch holds chainsyncBlockfetchMutex while
// nextBufferedHeaderConnId ranges over the map, so deleting from this
// goroutine under a different mutex is a concurrent iteration and write.
func (ls *LedgerState) discardBufferedPeerHeaders(
	connId ouroboros.ConnectionId,
) {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	ls.bufferedHeaderMutex.Lock()
	delete(ls.bufferedHeaderEvents, connIdKey(connId))
	ls.bufferedHeaderMutex.Unlock()
	if sameConnectionId(ls.headerPipelineConnId, connId) {
		ls.clearQueuedHeaders()
	}
}

func (ls *LedgerState) handleEventChainsyncRollback(
	e ChainsyncEvent,
	pending *pendingPublishes,
) error {
	// Filter events from non-active connections when chain selection is enabled
	if activeConnId, configured := ls.detectConnectionSwitch(pending); configured {
		if activeConnId == nil {
			// No active connection selected yet. Allow the rollback
			// to proceed — the downstream security-parameter-K check
			// and rollback-loop detector still guard against deep or
			// repeated rollbacks. Blanket-dropping here caused
			// pipeline stalls after Mithril bootstrap when chain
			// selection had not yet promoted a peer.
			ls.config.Logger.Debug(
				"no active connection, processing rollback event",
				"connection_id", e.ConnectionId.String(),
				"slot", e.Point.Slot,
				"local_tip_slot", ls.chain.Tip().Point.Slot,
			)
		} else if !sameConnectionId(*activeConnId, e.ConnectionId) {
			ls.discardBufferedPeerHeaders(e.ConnectionId)
			// Event is from non-active connection, skip
			// Rate-limit this message to once per dropEventLogInterval
			now := time.Now()
			if now.Sub(ls.dropRollbackLastLog) >= dropEventLogInterval {
				suppressed := ls.dropRollbackCount
				ls.dropRollbackCount = 0
				ls.dropRollbackLastLog = now
				ls.config.Logger.Debug(
					"dropping rollback from non-active connection and clearing buffered peer headers",
					"component", "ledger",
					"event_connection_id", e.ConnectionId.String(),
					"active_connection_id", activeConnId.String(),
					"slot", e.Point.Slot,
					"cleared_buffered_headers", true,
					"suppressed_since_last_log", suppressed,
				)
			} else {
				ls.dropRollbackCount++
			}
			return nil
		}
	}

	// Rollback loop detection: track recent rollbacks and skip if
	// the same peer repeats the same rollback point too frequently
	// within the detection window.
	now := time.Now()
	connKey := connIdKey(e.ConnectionId)
	ls.rollbackHistory = append(ls.rollbackHistory, rollbackRecord{
		point: ocommon.Point{
			Slot: e.Point.Slot,
			Hash: append([]byte(nil), e.Point.Hash...),
		},
		connKey:   connKey,
		timestamp: now,
	})
	// Prune entries older than the detection window
	cutoff := now.Add(-rollbackLoopWindow)
	pruned := ls.rollbackHistory[:0]
	for _, r := range ls.rollbackHistory {
		if !r.timestamp.Before(cutoff) {
			pruned = append(pruned, r)
		}
	}
	ls.rollbackHistory = pruned
	// Count repeated rollbacks to this exact point from the same
	// connection. Different peers can legitimately converge on the
	// same rollback point during chain selection, and those rollbacks
	// must not be suppressed.
	var slotCount int
	for _, r := range ls.rollbackHistory {
		if r.connKey == connKey && pointMatches(r.point, e.Point) {
			slotCount++
		}
	}
	if slotCount >= rollbackLoopThreshold {
		// Exempt rollbacks to slots where we forged a block — fork
		// resolution on our own block is normal Ouroboros behavior
		// (slot battles), not a pathological loop.
		skipRollback := true
		if checker := ls.loadForgedBlockChecker(); checker != nil {
			if _, forged := checker.WasForgedByUs(e.Point.Slot); forged {
				ls.config.Logger.Info(
					"allowing rollback on forged slot (slot battle resolution)",
					"component", "ledger",
					"slot", e.Point.Slot,
					"count", slotCount,
				)
				skipRollback = false
			}
		}
		// A rollback the node can actually apply (target block present,
		// within the security parameter K, at/above the Mithril anchor)
		// must be crossed even when the same point repeats. Suppressing a
		// crossable rollback wedges the node behind a legitimately advancing
		// peer and turns a transient fork into an unrecoverable reconnect
		// loop (issue #2790). Only a rollback the node genuinely cannot
		// cross is a true loop to break; those fall through to the skip +
		// #2728 escalation below.
		if skipRollback && ls.rollbackIsAppliable(e.Point) {
			ls.config.Logger.Info(
				"allowing repeated crossable rollback to converge to peer",
				"component", "ledger",
				"slot", e.Point.Slot,
				"count", slotCount,
			)
			skipRollback = false
		}
		if skipRollback {
			// A peer that is merely behind on our own chain repeats
			// the same intersect on every reconnect by construction,
			// so it reaches this threshold without anything having
			// diverged. Skipping its rollback is right; reporting it
			// as unrecoverable divergence (which advises the operator
			// to re-bootstrap from a Mithril snapshot) and forcing a
			// fresh connection are not.
			if depth, behind := ls.chainsyncPeerBehindOnOurChain(
				e,
			); behind {
				ls.noteChainsyncPeerBehind(
					e,
					depth,
					"rollback loop detected",
				)
				return nil
			}
			// Surface the stuck condition through the point-keyed tracker
			// that survives the resync reset+reconnect cycle. Without this
			// the skip silently breaks the loop and the #2728 escalation
			// (operator error + metric) never fires, hiding a node that
			// cannot self-recover (issue #2790 bullet 6).
			ls.reportUnrecoverableRollbackIfStuck(
				e.Point,
				event.ChainsyncResyncReasonRollbackLoop,
				e.ConnectionId,
			)
			ls.config.Logger.Warn(
				"rollback loop detected, skipping rollback to break loop",
				"component", "ledger",
				"slot", e.Point.Slot,
				"count", slotCount,
				"window", rollbackLoopWindow,
			)
			return ErrRollbackLoopDetected
		}
	}

	// A rollback to the current tip is a no-op — the peer's
	// FindIntersect resolved to the same point we already sit at.
	// Skip the rollback entirely to avoid publishing a spurious
	// "local ledger rollback" resync event that would close all
	// connections and create a reconnect loop.
	localTip := ls.chain.HeaderTip()
	if e.Point.Slot == localTip.Point.Slot &&
		bytes.Equal(e.Point.Hash, localTip.Point.Hash) {
		ls.config.Logger.Debug(
			"rollback to current tip is no-op, skipping",
			"component", "ledger",
			"slot", e.Point.Slot,
			"connection_id", e.ConnectionId.String(),
		)
		return nil
	}

	// A rollback point ahead of our local tip is invalid for the
	// current chain view and typically indicates intersect drift.
	// Trigger a chainsync re-sync instead of failing hard.
	if e.Point.Slot > localTip.Point.Slot {
		ls.config.Logger.Warn(
			"received rollback point ahead of local tip, triggering chainsync re-sync",
			"component",
			"ledger",
			"rollback_slot",
			e.Point.Slot,
			"local_tip_slot",
			localTip.Point.Slot,
			"connection_id",
			e.ConnectionId.String(),
		)
		ls.resetChainsyncResyncState()
		ls.setChainsyncState(SyncingChainsyncState)
		pending.add(
			ls.config.EventBus,
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					ConnectionId: e.ConnectionId,
					Reason:       event.ChainsyncResyncReasonRollbackAhead,
				},
			),
		)
		return nil
	}

	if ls.setChainsyncStateIf(
		SyncingChainsyncState,
		RollbackChainsyncState,
	) {
		ls.config.Logger.Info(
			fmt.Sprintf(
				"ledger: rolling back to %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
	}
	if err := ls.rollbackChainAndStateDeferred(e.Point, pending); err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			// Missing rollback point can happen when local state and peer
			// chainsync cursor drift. Recover by forcing re-intersect.
			ls.config.Logger.Warn(
				"rollback point not found locally, triggering chainsync re-sync",
				"component",
				"ledger",
				"slot",
				e.Point.Slot,
				"hash",
				hex.EncodeToString(e.Point.Hash),
				"connection_id",
				e.ConnectionId.String(),
			)
			// The per-connection loop detector cannot catch this: the
			// reset below wipes rollbackHistory and the resync forces a
			// fresh connection, so its counter never accumulates. Track
			// the point itself so a persistently un-crossable rollback
			// surfaces as an operator error + metric (see issue #2728).
			ls.reportUnrecoverableRollbackIfStuck(
				e.Point,
				event.ChainsyncResyncReasonRollbackNotFound,
				e.ConnectionId,
			)
			ls.resetChainsyncResyncState()
			ls.setChainsyncState(SyncingChainsyncState)
			pending.add(
				ls.config.EventBus,
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: e.ConnectionId,
						Reason:       event.ChainsyncResyncReasonRollbackNotFound,
					},
				),
			)
			return nil
		}
		if errors.Is(err, chain.ErrRollbackExceedsSecurityParam) {
			reconciled, reconcileErr := ls.reconcileLivePrimaryChainLedgerDivergence(
				"chainsync rollback exceeds security parameter K",
				e.ConnectionId,
			)
			if reconcileErr != nil {
				return fmt.Errorf(
					"reconcile primary chain and ledger after over-K rollback: %w",
					reconcileErr,
				)
			}
			if reconciled {
				ls.resetChainsyncResyncState()
				ls.setChainsyncState(SyncingChainsyncState)
				return nil
			}
			// A peer whose advertised tip is a strict ancestor of
			// ours holds a prefix of our chain: it is behind, not
			// forked, and the over-K depth is only our intersect
			// ladder's granularity (see
			// chainsyncPeerBehindOnOurChain). Keep it attached and
			// unselected until it catches up instead of rejecting,
			// denying and escalating it — with a single configured
			// upstream, evicting it is a self-inflicted outage.
			if depth, behind := ls.chainsyncPeerBehindOnOurChain(
				e,
			); behind {
				ls.noteChainsyncPeerBehind(
					e,
					depth,
					"rollback exceeds security parameter K",
				)
				// No rollback occurred, so we are still syncing.
				ls.setChainsyncState(SyncingChainsyncState)
				return nil
			}
			// The peer's chain has diverged beyond K blocks from
			// ours. This is a security violation — we must not
			// follow a chain that requires rolling back more than
			// K blocks. Trigger a chainsync re-sync so the peer
			// governance can reconnect and negotiate a fresh
			// intersection rather than waiting for a protocol
			// timeout.
			ls.config.Logger.Error(
				"chainsync rollback exceeds security "+
					"parameter K, rejecting peer chain",
				"component", "ledger",
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
				"connection_id", e.ConnectionId.String(),
			)
			ls.reportUnrecoverableRollbackIfStuck(
				e.Point,
				event.ChainsyncResyncReasonRollbackExceedsK,
				e.ConnectionId,
			)
			// Restore state: no rollback actually occurred, so
			// we are still syncing. Leaving RollbackChainsyncState
			// would cause a spurious "switched to fork" log and
			// fork metric increment on the next block header.
			ls.setChainsyncState(SyncingChainsyncState)
			pending.add(
				ls.config.EventBus,
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: e.ConnectionId,
						Reason:       event.ChainsyncResyncReasonRollbackExceedsK,
					},
				),
			)
			return nil
		}
		if errors.Is(err, ErrRollbackExceedsMithrilBoundary) {
			// The Mithril snapshot is the local trust anchor. Blocks at
			// or below its boundary were certified as a single ledger
			// state, so we cannot reconstruct intermediate UTxO states
			// for a replacement fork below that point. Refuse the
			// rollback and force a fresh intersection instead.
			//
			// The peer's reported tip distinguishes two situations that
			// both surface here as a rollback below the boundary:
			//   - tip below the boundary: the peer is simply behind
			//     (still syncing or stuck) and its FindIntersect matched
			//     an old rung of our intersect ladder — stale, not a
			//     competing fork;
			//   - tip at/above the boundary: the peer's chain does not
			//     contain our certified boundary block (always offered
			//     as an intersect point), so it genuinely diverges below
			//     the trust anchor.
			// A zero tip means the peer's tip is unknown; treat it as
			// divergent to fail safe.
			mithrilLedgerSlot := ls.mithrilLedgerSlotSnapshot()
			reason := event.ChainsyncResyncReasonRollbackExceedsMithril
			peerTipSlot := e.Tip.Point.Slot
			if peerTipSlot > 0 && peerTipSlot < mithrilLedgerSlot {
				reason = event.ChainsyncResyncReasonPeerTipBehindMithril
				ls.config.Logger.Warn(
					"chainsync peer tip behind Mithril trust boundary, treating peer chain as stale",
					"component",
					"ledger",
					"slot",
					e.Point.Slot,
					"hash",
					hex.EncodeToString(e.Point.Hash),
					"peer_tip_slot",
					peerTipSlot,
					"mithril_ledger_slot",
					mithrilLedgerSlot,
					"connection_id",
					e.ConnectionId.String(),
				)
			} else {
				ls.config.Logger.Error(
					"chainsync rollback exceeds Mithril trust boundary, rejecting peer chain",
					"component", "ledger",
					"slot", e.Point.Slot,
					"hash", hex.EncodeToString(e.Point.Hash),
					"peer_tip_slot", peerTipSlot,
					"mithril_ledger_slot", mithrilLedgerSlot,
					"connection_id", e.ConnectionId.String(),
				)
			}
			ls.reportUnrecoverableRollbackIfStuck(
				e.Point,
				reason,
				e.ConnectionId,
			)
			ls.resetChainsyncResyncState()
			ls.setChainsyncState(SyncingChainsyncState)
			pending.add(
				ls.config.EventBus,
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: e.ConnectionId,
						Reason:       reason,
					},
				),
			)
			return nil
		}
		return fmt.Errorf("chain rollback failed: %w", err)
	}
	// The rollback applied: we crossed to the peer's point, so any prior
	// un-crossable-rollback tracking is stale. Clear it so a later,
	// unrelated divergence starts counting from zero (see issue #2728).
	ls.clearUnrecoverableRollbacks()
	// Crossing to the point is forward progress toward the peer's chain, so
	// drop this point's per-connection loop-detector records too. A rollback
	// we actually applied must not count toward the repeat-loop threshold on
	// a later, legitimate rollback to the same fork point; leaving the
	// records accumulates crossable rollbacks until the loop detector
	// suppresses one, wedging the node in a reconnect loop behind a
	// legitimately advancing peer (issue #2790).
	ls.clearRollbackHistoryForPoint(e.Point)
	return nil
}

// rollbackIsAppliable reports whether rollbackChainAndStateDeferred(point) would
// succeed right now, without mutating any state. It mirrors the pre-checks
// rollbackChainAndStateDeferred relies on for its block-not-found / exceeds-K /
// exceeds-Mithril failures: the point must sit at or above the Mithril trust
// anchor, and the chain must be able to roll back to it (target block present
// and within the security parameter K, verified via chain.ValidateRollback).
//
// The loop detector uses this to decide whether a repeated rollback is a
// crossable point that must be applied (issue #2790) rather than a genuinely
// un-crossable loop to break.
//
// Callers must hold chainsyncMutex.
func (ls *LedgerState) rollbackIsAppliable(point ocommon.Point) bool {
	if ls.chain == nil {
		return false
	}
	mithrilLedgerSlot := ls.mithrilLedgerSlotSnapshot()
	if mithrilLedgerSlot > 0 && point.Slot < mithrilLedgerSlot {
		return false
	}
	return ls.chain.ValidateRollback(point) == nil
}

// clearRollbackHistoryForPoint removes per-connection loop-detector records
// for a rollback point the node has successfully applied. A crossable rollback
// we actually crossed made forward progress toward the peer's chain, so it
// must not count toward the repeat-loop threshold that breaks pathological
// loops. Rollbacks the node genuinely cannot cross never reach this path and
// are tracked separately by the survives-reset unrecoverableRollbacks map.
//
// Callers must hold chainsyncMutex.
func (ls *LedgerState) clearRollbackHistoryForPoint(point ocommon.Point) {
	if len(ls.rollbackHistory) == 0 {
		return
	}
	filtered := ls.rollbackHistory[:0]
	for _, r := range ls.rollbackHistory {
		if pointMatches(r.point, point) {
			continue
		}
		filtered = append(filtered, r)
	}
	ls.rollbackHistory = filtered
}

// resetChainsyncResyncState clears chainsync-local recovery state before a
// re-sync. It mutates rollbackHistory, headerMismatchCount, and queued
// chain/blockfetch state by calling chain.ClearHeaders and
// blockfetchRequestRangeCleanup (while holding chainsyncBlockfetchMutex).
// Callers must hold chainsyncMutex before invoking this method to avoid races
// with other chainsync operations.
func (ls *LedgerState) resetChainsyncResyncState() {
	ls.rollbackHistory = nil
	ls.headerMismatchCount = 0
	ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Lock()
	// clearQueuedHeaders mutates headerPipelineConnId, which every other
	// mutator guards with chainsyncBlockfetchMutex -- moved inside this
	// lock (rather than called before it, as this used to) to close that
	// gap. bufferedHeaderEvents has its own lock; see bufferedHeaderMutex.
	ls.bufferedHeaderMutex.Lock()
	ls.bufferedHeaderEvents = nil
	ls.bufferedHeaderMutex.Unlock()
	ls.clearQueuedHeaders()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

func pointMatches(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

func observedHeaderTip(e ChainsyncEvent) ochainsync.Tip {
	if e.BlockHeader == nil {
		return e.Tip
	}
	return ochainsync.Tip{
		Point:       e.Point,
		BlockNumber: e.BlockHeader.BlockNumber(),
	}
}

func (ls *LedgerState) localTipPraosView(
	localTip ochainsync.Tip,
) praos.PraosTiebreakerView {
	if ls == nil || ls.db == nil || len(localTip.Point.Hash) == 0 {
		return praos.PraosTiebreakerView{}
	}
	block, err := database.BlockByHash(ls.db, localTip.Point.Hash)
	if err != nil {
		if ls.config.Logger != nil {
			ls.config.Logger.Debug(
				"local tip Praos view unavailable: block lookup failed",
				"component", "ledger",
				"slot", localTip.Point.Slot,
				"hash", hex.EncodeToString(localTip.Point.Hash),
				"error", err,
			)
		}
		return praos.PraosTiebreakerView{}
	}
	decoded, err := block.Decode()
	if err != nil {
		if ls.config.Logger != nil {
			ls.config.Logger.Debug(
				"local tip Praos view unavailable: block decode failed",
				"component", "ledger",
				"slot", localTip.Point.Slot,
				"hash", hex.EncodeToString(localTip.Point.Hash),
				"error", err,
			)
		}
		return praos.PraosTiebreakerView{}
	}
	if decoded == nil {
		return praos.PraosTiebreakerView{}
	}
	view, _ := praos.GetPraosTiebreakerView(decoded.Header())
	return view
}

func (ls *LedgerState) compareIncomingHeaderToLocalTip(
	e ChainsyncEvent,
	localTip ochainsync.Tip,
) praos.ChainComparisonResult {
	observedTip := observedHeaderTip(e)
	if observedTip.BlockNumber == 0 && e.Tip.BlockNumber > 0 {
		observedTip = e.Tip
	}

	incomingView, _ := praos.GetPraosTiebreakerView(e.BlockHeader)
	result := praos.ComparePraosTips(
		observedTip,
		localTip,
		incomingView,
		ls.localTipPraosView(localTip),
	)
	if result != praos.ChainEqual {
		return result
	}

	// If the peer has advertised a further tip than the header just delivered,
	// use that only when block number alone decides the comparison. We do not
	// have the advertised tip's Praos select view until its header arrives.
	if e.Tip.BlockNumber > observedTip.BlockNumber {
		switch {
		case e.Tip.BlockNumber > localTip.BlockNumber:
			return praos.ChainABetter
		case e.Tip.BlockNumber < localTip.BlockNumber:
			return praos.ChainBBetter
		}
	}
	return result
}

func (ls *LedgerState) earlierHeaderCanBeatLocalTip(
	e ChainsyncEvent,
	localTip ochainsync.Tip,
) bool {
	observedTip := observedHeaderTip(e)
	if observedTip.BlockNumber > localTip.BlockNumber {
		return true
	}
	if observedTip.BlockNumber < localTip.BlockNumber {
		return false
	}
	incomingView, _ := praos.GetPraosTiebreakerView(e.BlockHeader)
	return praos.ComparePraosTips(
		observedTip,
		localTip,
		incomingView,
		ls.localTipPraosView(localTip),
	) == praos.ChainABetter
}

type LocalRollbackRecoveryResult struct {
	Recovered           bool
	SkipConnectionClose bool
	PrimaryChainTipSlot uint64
}

func peerHeaderHistoryPathFromCache(
	steps []peerHeaderHistoryPathStep,
	cache map[string]peerHeaderHistoryPathCacheEntry,
	cachedKey string,
	cachedDistance int,
) ([]ChainsyncEvent, bool) {
	pathReversed := make(
		[]ChainsyncEvent,
		0,
		len(steps)+cachedDistance,
	)
	for _, step := range steps {
		pathReversed = append(pathReversed, step.event)
	}
	key := cachedKey
	for remaining := cachedDistance; remaining > 0; remaining-- {
		entry, ok := cache[key]
		if !ok || !entry.ok || !entry.hasRecord ||
			entry.distance != remaining {
			return nil, false
		}
		pathReversed = append(pathReversed, entry.event)
		key = entry.nextHash
	}
	slices.Reverse(pathReversed)
	return pathReversed, true
}

func cachePeerHeaderHistoryPath(
	steps []peerHeaderHistoryPathStep,
	cache map[string]peerHeaderHistoryPathCacheEntry,
	ancestor ocommon.Point,
	suffixDistance int,
) {
	for i, step := range steps {
		cache[step.key] = peerHeaderHistoryPathCacheEntry{
			ancestor:  ancestor,
			distance:  len(steps) - i + suffixDistance,
			hasRecord: true,
			ok:        true,
			event:     step.event,
			nextHash:  step.nextHash,
		}
	}
}

// cachePeerHeaderHistoryDepthExhausted retains links from a walk that reached
// the depth bound without treating them as unavailable. A later, shorter
// suffix can reuse the links while the loop still charges each one against its
// own depth bound.
func cachePeerHeaderHistoryDepthExhausted(
	steps []peerHeaderHistoryPathStep,
	cache map[string]peerHeaderHistoryPathCacheEntry,
) {
	for _, step := range steps {
		cache[step.key] = peerHeaderHistoryPathCacheEntry{
			hasRecord:      true,
			ok:             true,
			depthExhausted: true,
			event:          step.event,
			nextHash:       step.nextHash,
		}
	}
}

func markPeerHeaderHistoryPathUnavailable(
	steps []peerHeaderHistoryPathStep,
	cache map[string]peerHeaderHistoryPathCacheEntry,
	key string,
) {
	cache[key] = peerHeaderHistoryPathCacheEntry{}
	for _, step := range steps {
		cache[step.key] = peerHeaderHistoryPathCacheEntry{}
	}
}

// findPeerForkPathCached resolves one retained history walk and memoizes each
// visited link. Rollback recovery may try every retained suffix head when the
// requested point is absent; sharing these links keeps that fallback linear in
// the retained history instead of repeatedly walking the same suffix while
// chainsyncMutex is held.
func (ls *LedgerState) findPeerForkPathCached(
	e ChainsyncEvent,
	initialPrevHash []byte,
	expectedAncestor ocommon.Point,
	history *peerHeaderChain,
	cache map[string]peerHeaderHistoryPathCacheEntry,
) (*ocommon.Point, []ChainsyncEvent, error) {
	if len(initialPrevHash) == 0 {
		return nil, nil, nil
	}
	prevHash := append([]byte(nil), initialPrevHash...)
	steps := make([]peerHeaderHistoryPathStep, 0)
	visited := make(map[string]struct{})
	limit := ls.peerHeaderHistoryLimit()
	for depth := 0; depth < limit && len(prevHash) > 0; depth++ {
		key := hex.EncodeToString(prevHash)
		if _, seen := visited[key]; seen {
			markPeerHeaderHistoryPathUnavailable(steps, cache, key)
			return nil, nil, nil
		}
		visited[key] = struct{}{}
		if entry, ok := cache[key]; ok {
			if entry.depthExhausted {
				if !entry.ok || !entry.hasRecord {
					markPeerHeaderHistoryPathUnavailable(steps, cache, key)
					return nil, nil, nil
				}
				nextHash, err := hex.DecodeString(entry.nextHash)
				if err != nil {
					markPeerHeaderHistoryPathUnavailable(steps, cache, key)
					return nil, nil, fmt.Errorf(
						"decode cached peer header hash %q: %w",
						entry.nextHash,
						err,
					)
				}
				steps = append(steps, peerHeaderHistoryPathStep{
					event:    entry.event,
					key:      key,
					nextHash: entry.nextHash,
				})
				prevHash = nextHash
				continue
			}
			if !entry.ok || !entry.hasRecord || entry.distance <= 0 {
				markPeerHeaderHistoryPathUnavailable(steps, cache, key)
				return nil, nil, nil
			}
			cachePeerHeaderHistoryPath(
				steps,
				cache,
				entry.ancestor,
				entry.distance,
			)
			if len(steps)+entry.distance >= limit {
				return nil, nil, nil
			}
			if !pointMatches(entry.ancestor, expectedAncestor) {
				return &entry.ancestor, nil, nil
			}
			path, ok := peerHeaderHistoryPathFromCache(
				steps,
				cache,
				key,
				entry.distance,
			)
			if !ok {
				markPeerHeaderHistoryPathUnavailable(steps, cache, key)
				return nil, nil, nil
			}
			return &entry.ancestor, path, nil
		}

		ancestorBlock, err := ls.blockByHash(prevHash)
		if err == nil {
			ancestor := ocommon.NewPoint(ancestorBlock.Slot, ancestorBlock.Hash)
			cachePeerHeaderHistoryPath(steps, cache, ancestor, 0)
			if !pointMatches(ancestor, expectedAncestor) {
				return &ancestor, nil, nil
			}
			pathReversed := make([]ChainsyncEvent, 0, len(steps))
			for _, step := range steps {
				pathReversed = append(pathReversed, step.event)
			}
			slices.Reverse(pathReversed)
			return &ancestor, pathReversed, nil
		}
		if !errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil, fmt.Errorf(
				"lookup ancestor hash %x: %w",
				prevHash,
				err,
			)
		}

		var (
			record peerHeaderRecord
			found  bool
		)
		if history != nil {
			record, found = history.byHash[key]
		}
		if !found && ls.config.PeerHeaderLookupFunc != nil {
			lookupEvent, lookupPrevHash, ok := ls.config.PeerHeaderLookupFunc(
				e.ConnectionId,
				prevHash,
			)
			if ok {
				record = peerHeaderRecord{
					event:    lookupEvent,
					prevHash: lookupPrevHash,
				}
				found = true
			}
		}
		if !found {
			markPeerHeaderHistoryPathUnavailable(steps, cache, key)
			return nil, nil, nil
		}
		recordEvent, ok := record.chainsyncEvent()
		if !ok {
			markPeerHeaderHistoryPathUnavailable(steps, cache, key)
			return nil, nil, nil
		}
		recordPrev := append([]byte(nil), record.prevHash...)
		steps = append(steps, peerHeaderHistoryPathStep{
			event:    recordEvent,
			key:      key,
			nextHash: hex.EncodeToString(recordPrev),
		})
		prevHash = recordPrev
	}
	cachePeerHeaderHistoryDepthExhausted(steps, cache)
	return nil, nil, nil
}

func (ls *LedgerState) recoverPeerHeaderHistoryFromPointLocked(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) (int, error) {
	history := ls.peerHeaderHistory[connIdKey(connId)]
	if history == nil || len(history.order) == 0 {
		return 0, nil
	}
	pathCache := make(map[string]peerHeaderHistoryPathCacheEntry,
		len(history.order))
	for _, v := range slices.Backward(history.order) {
		record, ok := history.byHash[v]
		if !ok {
			continue
		}
		recordEvent, ok := record.chainsyncEvent()
		if !ok || recordEvent.Point.Slot <= point.Slot {
			continue
		}
		ancestorPoint, forkPath, err := ls.findPeerForkPathCached(
			recordEvent,
			record.prevHash,
			point,
			history,
			pathCache,
		)
		if err != nil {
			return 0, err
		}
		if ancestorPoint == nil || !pointMatches(*ancestorPoint, point) {
			continue
		}
		forkPath = append(forkPath, recordEvent)
		// Anything at or below the chain's current header tip is already
		// applied. Without this guard, a forkPath entry whose hash equals
		// the header tip causes AddBlockHeader to fail the prev-hash
		// check (header tip's prev != header tip), which aborts recovery
		// and breaks the chainsync session permanently. Use the larger
		// of the rollback point and the live header tip so concurrent
		// progress past `point` does not regress recovery.
		cutoffSlot := point.Slot
		if tipSlot := ls.chain.HeaderTip().Point.Slot; tipSlot > cutoffSlot {
			cutoffSlot = tipSlot
		}
		added := 0
		for _, evt := range forkPath {
			if evt.Point.Slot <= cutoffSlot {
				continue
			}
			if err := ls.chain.AddBlockHeader(evt.BlockHeader); err != nil {
				// clearQueuedHeaders (and the headerPipelineConnId write
				// below) mutate a field every other mutator guards with
				// chainsyncBlockfetchMutex -- take it here too, scoped
				// tightly around just these writes.
				ls.chainsyncBlockfetchMutex.Lock()
				ls.clearQueuedHeaders()
				ls.chainsyncBlockfetchMutex.Unlock()
				return 0, err
			}
			added++
		}
		if added == 0 {
			continue
		}
		ls.chainsyncBlockfetchMutex.Lock()
		ls.headerPipelineConnId = connId
		ls.chainsyncBlockfetchMutex.Unlock()
		ls.selectedBlockfetchConnId = connId
		return ls.chain.HeaderCount(), nil
	}
	return 0, nil
}

// RecoverAfterLocalRollback resets chainsync-local queued state after a ledger
// rollback, then replays any peer-local header history that still fits the new
// tip. This keeps rollback recovery local to the node instead of re-entering
// FindIntersect on live ChainSync sessions. The result reports whether peer
// history was replayed and whether connection closure should be skipped because
// the primary chain tip is already past the completed rollback point.
func (ls *LedgerState) RecoverAfterLocalRollback(
	connIds []ouroboros.ConnectionId,
	point ocommon.Point,
) LocalRollbackRecoveryResult {
	var pending pendingPublishes
	defer pending.flush()
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()

	if ls.chain == nil {
		return LocalRollbackRecoveryResult{}
	}
	ls.RLock()
	lastLocalRollbackSeq := ls.lastLocalRollbackSeq
	lastLocalRollbackPoint := ls.lastLocalRollbackPoint
	ls.RUnlock()
	if lastLocalRollbackSeq > 0 &&
		pointMatches(lastLocalRollbackPoint, point) {
		if chainTipSlot := ls.PrimaryChainTipSlot(); chainTipSlot > point.Slot {
			return LocalRollbackRecoveryResult{
				SkipConnectionClose: true,
				PrimaryChainTipSlot: chainTipSlot,
			}
		}
	}
	ls.resetChainsyncResyncState()

	preferredConnIds := make([]ouroboros.ConnectionId, 0, len(connIds)+1)
	seenConnIds := make(map[string]struct{}, len(connIds)+1)
	if ls.config.GetActiveConnectionFunc != nil {
		if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil {
			key := connIdKey(*activeConnId)
			if key != "" {
				preferredConnIds = append(preferredConnIds, *activeConnId)
				seenConnIds[key] = struct{}{}
			}
		}
	}
	for _, connId := range connIds {
		key := connIdKey(connId)
		if key == "" {
			continue
		}
		if _, ok := seenConnIds[key]; ok {
			continue
		}
		preferredConnIds = append(preferredConnIds, connId)
		seenConnIds[key] = struct{}{}
	}

	for _, connId := range preferredConnIds {
		headerCount, err := ls.recoverPeerHeaderHistoryFromPointLocked(
			connId,
			point,
		)
		if err != nil {
			ls.config.Logger.Warn(
				"failed to recover peer header history after local rollback",
				"component", "ledger",
				"connection_id", connId.String(),
				"slot", point.Slot,
				"error", err,
			)
			continue
		}
		if headerCount == 0 {
			continue
		}
		ls.config.Logger.Info(
			"replayed peer header history after local rollback",
			"component", "ledger",
			"connection_id", connId.String(),
			"rollback_slot", point.Slot,
			"header_count", headerCount,
		)
		ls.chainsyncBlockfetchMutex.Lock()
		var startErr error
		if ls.chainsyncBlockfetchReadyChan == nil &&
			!ls.blockfetchContinuationPending {
			startErr = ls.startQueuedBlockfetchLocked(connId, &pending)
			if startErr != nil {
				// Recovery connection may have closed. Retry with
				// the current active best peer before giving up,
				// otherwise the pipeline stalls until restart.
				if ls.config.GetActiveConnectionFunc != nil {
					if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil &&
						!sameConnectionId(*activeConnId, connId) {
						ls.config.Logger.Info(
							"local rollback recovery connection unavailable, retrying with active best peer",
							"component",
							"ledger",
							"failed_connection_id",
							connId.String(),
							"active_connection_id",
							activeConnId.String(),
							"error",
							startErr,
						)
						// Retarget the selection too, not just this
						// request: nextBlockfetchConnId prefers
						// selectedBlockfetchConnId, so leaving it on the
						// failed recovery connection sends the next batch
						// of a multi-batch replay straight back to it.
						// Matches the fallback in handleEventChainsync.
						ls.selectedBlockfetchConnId = *activeConnId
						startErr = ls.startQueuedBlockfetchLocked(
							*activeConnId,
							&pending,
						)
					}
				}
				if startErr != nil {
					ls.config.Logger.Warn(
						"failed to start blockfetch after local rollback recovery",
						"component",
						"ledger",
						"connection_id",
						connId.String(),
						"error",
						startErr,
					)
				}
			}
		}
		ls.chainsyncBlockfetchMutex.Unlock()
		if startErr != nil {
			// Do not report recovery when the replayed headers could not
			// be fetched. The caller must close these sessions so peer
			// governance can establish a fresh chainsync intersection.
			continue
		}
		return LocalRollbackRecoveryResult{Recovered: true}
	}
	// Every preferred connection failed. Clear the selection rather than
	// leaving it on a connection known not to serve this range, matching what
	// handleEventChainsync does when its own fallbacks are exhausted. Each
	// successful replay reassigns this field before its blockfetch attempt, so
	// nothing downstream currently reads a stale value -- but that makes the
	// invariant depend on every future writer reassigning first, which is not a
	// property worth relying on.
	ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
	return LocalRollbackRecoveryResult{}
}

func (ls *LedgerState) handleEventChainsyncBlockHeaderWithPending(
	e ChainsyncEvent,
	pending *pendingPublishes,
) error {
	// Detect connection switch so pipeline ownership is handed off
	// even when the first post-switch event is a header rather than
	// a rollback. Without this, headers from a newly-selected active
	// connection are buffered indefinitely because the pipeline owner
	// still points to the old (dead) connection.
	ls.detectConnectionSwitch(pending)

	// Verify header crypto before accepting it into the header queue.
	// Skip during historical sync (validationEnabled=false) because
	// historical blocks were already validated by the network and the
	// epoch nonce may not be fully computed yet (e.g. Byron→Shelley).
	// Also skip headers covered by a Mithril snapshot: those slots were
	// verified by the certificate chain during import, and the restored
	// database intentionally does not keep every historical epoch nonce.
	headerCryptoVerified := false
	headerValidationRequired, headerTrusted := ls.chainsyncHeaderCryptoPolicy(
		e.Point.Slot,
	)
	if headerValidationRequired {
		if err := ls.verifyBlockHeaderOnlyCrypto(e.BlockHeader); err != nil {
			if errors.Is(err, errHeaderVerificationDeferred) {
				ls.config.Logger.Debug(
					"deferring chainsync header crypto verification until blockfetch",
					"component",
					"ledger",
					"slot",
					e.Point.Slot,
					"hash",
					hex.EncodeToString(e.Point.Hash),
					"error",
					err,
				)
			} else {
				if ls.config.EventBus != nil {
					ls.config.Logger.Warn(
						"recycling connection after header verification failure",
						"component", "ledger",
						"connection_id", e.ConnectionId.String(),
						"slot", e.Point.Slot,
						"hash", hex.EncodeToString(e.Point.Hash),
					)
					pending.add(
						ls.config.EventBus,
						ConnectionRecycleRequestedEventType,
						event.NewEvent(
							ConnectionRecycleRequestedEventType,
							ConnectionRecycleRequestedEvent{
								ConnectionId: e.ConnectionId,
								Reason:       "header_verification_failure",
							},
						),
					)
				}
				return fmt.Errorf(
					"block header crypto verification failed: %w",
					err,
				)
			}
		} else {
			headerCryptoVerified = true
			headerTrusted = true
		}
	}

	if ls.setChainsyncState(SyncingChainsyncState) == RollbackChainsyncState {
		ls.config.Logger.Info(
			fmt.Sprintf(
				"ledger: switched to fork at %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
		ls.metrics.forks.Add(1)
	}
	ls.recordPeerHeaderHistory(e)
	if ls.shouldBufferHeaderEvent(e) {
		return nil
	}
	// Allow us to build up a few blockfetch batches worth of headers,
	// but never exceed the chain's actual header queue capacity.
	allowedHeaderCount := min(
		blockfetchBatchSize*4,
		ls.chain.MaxQueuedHeaders(),
	)
	headerCount := ls.chain.HeaderCount()

	// Add header to chain
	ls.config.Logger.Debug(
		"chainsync header handler entered",
		"component", "ledger",
		"slot", e.Point.Slot,
		"tip_slot", e.Tip.Point.Slot,
		"header_count", headerCount,
		"connection_id", e.ConnectionId.String(),
	)
	var err error
	if headerCryptoVerified {
		err = ls.chain.AddVerifiedBlockHeader(e.BlockHeader)
	} else {
		err = ls.chain.AddBlockHeader(e.BlockHeader)
	}
	if err != nil {
		if notFitErr, ok := errors.AsType[chain.BlockNotFitChainTipError](err); ok {
			if ls.headerAtOrImmediatelyBeforeTip(e) {
				ls.config.Logger.Debug(
					"ignoring duplicate or reordered roll forward",
					"component", "ledger",
					"slot", e.Point.Slot,
					"connection_id", e.ConnectionId.String(),
				)
				return nil
			}
			localTip := ls.chain.Tip()
			genesisActive, _ := ls.genesisSelectionState()
			// A header behind the local tip is stale only if the
			// Praos comparison also says it cannot beat the local
			// tip. At equal block number, cardano-node resolves the
			// fork by Praos select view, even when the winning
			// header is at an earlier slot. Without both select
			// views, keep the old stale behavior for earlier-slot
			// headers and let a future observed header prove the
			// advertised tip.
			if e.Point.Slot < localTip.Point.Slot &&
				!genesisActive &&
				!ls.earlierHeaderCanBeatLocalTip(
					e,
					localTip,
				) {
				ls.config.Logger.Debug(
					"ignoring stale roll forward behind local tip",
					"component", "ledger",
					"slot", e.Point.Slot,
					"local_tip_slot", localTip.Point.Slot,
					"block_prev_hash", notFitErr.BlockPrevHash(),
					"chain_tip_hash", notFitErr.TipHash(),
					"connection_id", e.ConnectionId.String(),
				)
				return nil
			}
			// A header still on the authoritative primary chain is a
			// historical replay, not a competing candidate. Checked after
			// the in-memory discards above because it reads the block
			// store while chainsyncMutex is held.
			if ls.headerAlreadyOnPrimaryChain(e, localTip) {
				ls.config.Logger.Debug(
					"ignoring historical primary-chain roll forward",
					"component", "ledger",
					"slot", e.Point.Slot,
					"local_tip_slot", localTip.Point.Slot,
					"connection_id", e.ConnectionId.String(),
				)
				return nil
			}
			// Header doesn't fit current chain tip. Clear stale queued
			// headers so subsequent headers are evaluated against the
			// block tip rather than perpetuating the mismatch.
			//
			// clearQueuedHeaders mutates headerPipelineConnId, which
			// every other mutator guards with chainsyncBlockfetchMutex
			// -- take it here too, scoped tightly around just this call.
			ls.chainsyncBlockfetchMutex.Lock()
			ls.clearQueuedHeaders()
			ls.chainsyncBlockfetchMutex.Unlock()
			ls.headerMismatchCount++
			ls.config.Logger.Debug(
				"block header does not fit chain tip",
				"component", "ledger",
				"slot", e.Point.Slot,
				"block_prev_hash", notFitErr.BlockPrevHash(),
				"chain_tip_hash", notFitErr.TipHash(),
				"consecutive_mismatches", ls.headerMismatchCount,
			)
			// The incoming header's prevHash is the block it extends
			// from — the common ancestor. If that block exists on our
			// chain and the peer's chain is ahead, we roll back to
			// the common ancestor so chainsync can continue.
			resolved, resolveErr := ls.tryResolveFork(
				e, notFitErr, pending,
			)
			if resolveErr != nil {
				if ls.headerMismatchCount > 0 {
					ls.headerMismatchCount--
				}
				return fmt.Errorf(
					"failed resolving fork after header mismatch: %w",
					resolveErr,
				)
			}
			if resolved {
				ls.recordAdmittedHeaderFrontier(
					e,
					headerTrusted,
				)
				return nil
			}
			// Fallback: after several consecutive mismatches where
			// we couldn't find the common ancestor, trigger a
			// chainsync re-sync by closing the connection so the
			// peer governance reconnects and negotiates a fresh
			// intersection.
			if ls.headerMismatchCount >= headerMismatchResyncThreshold &&
				ls.config.EventBus != nil {
				ls.config.Logger.Info(
					"persistent chain fork detected, triggering chainsync re-sync",
					"component",
					"ledger",
					"connection_id",
					e.ConnectionId.String(),
					"consecutive_mismatches",
					ls.headerMismatchCount,
				)
				ls.requestChainsyncResync(
					e.ConnectionId,
					event.ChainsyncResyncReasonPersistentFork,
					pending,
				)
			}
			return nil
		}
		return fmt.Errorf("failed adding chain block header: %w", err)
	}
	// Reset mismatch counter on successful header addition
	ls.headerMismatchCount = 0
	ls.recordAdmittedHeaderFrontier(
		e,
		headerTrusted,
	)
	// Wait for additional block headers before fetching block bodies if we're
	// far enough out from upstream tip
	// Use security window as slot threshold if available
	headersReady := headerCount + 1
	// Use the primary chain header tip so the slot and block-number gaps reflect
	// fetched-but-unprocessed blocks that are already queued in ls.chain.
	localChainTip := ochainsync.Tip{}
	if ls.chain != nil {
		localChainTip = ls.chain.HeaderTip()
	}
	localTipSlot := localChainTip.Point.Slot
	blockGap := uint64(0)
	if e.Tip.BlockNumber > localChainTip.BlockNumber {
		blockGap = e.Tip.BlockNumber - localChainTip.BlockNumber
	}
	if e.Tip.Point.Slot > localTipSlot &&
		e.Tip.Point.Slot-localTipSlot >= blockfetchMinBatchGapSlots {
		minBatchHeaders := desiredBlockfetchBatchHeaders(
			e.Tip.Point.Slot-localTipSlot,
			blockGap,
			allowedHeaderCount,
		)
		if headersReady < minBatchHeaders {
			ls.config.Logger.Debug(
				"accumulating minimum header batch before blockfetch",
				"component", "ledger",
				"slot", e.Point.Slot,
				"tip_slot", e.Tip.Point.Slot,
				"local_tip_slot", localTipSlot,
				"header_count", headersReady,
				"minimum_header_count", minBatchHeaders,
			)
			return nil
		}
	}
	slotThreshold := ls.calculateStabilityWindow()
	if e.Point.Slot < e.Tip.Point.Slot &&
		(e.Tip.Point.Slot-e.Point.Slot > slotThreshold) &&
		headersReady < allowedHeaderCount {
		ls.config.Logger.Debug(
			"accumulating headers (far from tip)",
			"component", "ledger",
			"slot", e.Point.Slot,
			"tip_slot", e.Tip.Point.Slot,
			"threshold", slotThreshold,
			"header_count", headersReady,
		)
		return nil
	}
	// We use the blockfetch lock to ensure we aren't starting a batch at the same
	// time as blockfetch starts a new one to avoid deadlocks
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	// Don't start fetch if there's already one in progress
	if ls.chainsyncBlockfetchReadyChan != nil ||
		ls.blockfetchContinuationPending {
		ls.config.Logger.Debug(
			"blockfetch in progress, queuing header",
			"component", "ledger",
			"slot", e.Point.Slot,
			"header_count", ls.chain.HeaderCount(),
		)
		return nil
	}
	// Mark blockfetch as in progress
	ls.selectedBlockfetchConnId = e.ConnectionId
	initialConnId := ls.selectInitialBlockfetchConn(e.ConnectionId)
	ls.config.Logger.Debug(
		"starting blockfetch",
		"component", "ledger",
		"connection_id", initialConnId.String(),
		"header_count", ls.chain.HeaderCount(),
	)
	err = ls.startQueuedBlockfetchLocked(initialConnId, pending)
	if err != nil {
		// The chosen connection's blockfetch protocol may have shut
		// down. Try the header source connection if it's different;
		// otherwise try the active best peer before giving up.
		if !sameConnectionId(e.ConnectionId, initialConnId) {
			ls.config.Logger.Warn(
				"blockfetch start failed, retrying on header source connection",
				"component", "ledger",
				"failed_connection_id", initialConnId.String(),
				"retry_connection_id", e.ConnectionId.String(),
				"error", err,
			)
			if retryErr := ls.startQueuedBlockfetchLocked(e.ConnectionId, pending); retryErr == nil {
				return nil
			}
		}
		// Header source also failed (or was the same connection).
		// Try the current active best peer as a last resort before
		// clearing state — otherwise every subsequent header will
		// fail the same stale lookup.
		if ls.config.GetActiveConnectionFunc != nil {
			if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil &&
				!sameConnectionId(*activeConnId, initialConnId) &&
				!sameConnectionId(*activeConnId, e.ConnectionId) {
				ls.config.Logger.Info(
					"blockfetch connections unavailable, retrying with active best peer",
					"component",
					"ledger",
					"failed_connection_id",
					initialConnId.String(),
					"active_connection_id",
					activeConnId.String(),
					"error",
					err,
				)
				ls.selectedBlockfetchConnId = *activeConnId
				if retryErr := ls.startQueuedBlockfetchLocked(*activeConnId, pending); retryErr == nil {
					return nil
				}
			}
		}
		// All fallbacks exhausted. Clear stale state so the next
		// header can start a fresh blockfetch attempt.
		ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		ls.requestChainsyncResync(
			initialConnId,
			fmt.Sprintf("blockfetch start failed: %v", err),
			pending,
		)
		return nil
	}
	return nil
}

// AwaitChainsyncHeaderAdmission enforces the Ouroboros ChainSync future-header
// rule against the timestamp recorded at network ingress. It must run from the
// per-peer ChainSync callback before the header updates observed-tip, dedup, or
// ledger state; callers must not invoke it while holding the node-wide
// chainsync dispatch mutex.
//
// A header received no more than defaultHeaderClockSkew before its slot waits
// for slot onset and is accepted. A header received earlier is deliberately
// dropped by returning (false, nil): local clock skew cannot by itself justify
// penalizing the peer. ErrPastHorizon is also accepted as a deferred decision,
// matching headerVerificationEpoch; without a forecast the header cannot be
// proven future. Other conversion failures fail closed. A zero timestamp is
// retained for compatibility with synthetic/internal events that never crossed
// the network ingress path. As with other Go APIs that accept a context, ctx
// must not be nil.
func (ls *LedgerState) AwaitChainsyncHeaderAdmission(
	ctx context.Context,
	e ChainsyncEvent,
) (bool, error) {
	if e.ArrivalTime.IsZero() || ls.slotClock == nil || e.BlockHeader == nil {
		return true, nil
	}
	headerSlot := e.BlockHeader.SlotNumber()
	slotTime, err := ls.slotClock.SlotToTime(headerSlot)
	if err != nil {
		if errors.Is(err, hardfork.ErrPastHorizon) {
			return true, nil
		}
		return false, fmt.Errorf("resolve header slot onset: %w", err)
	}
	earlyBy := slotTime.Sub(e.ArrivalTime)
	if earlyBy <= 0 {
		return true, nil
	}
	if earlyBy > defaultHeaderClockSkew {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"dropping chainsync header received before permitted clock-skew window",
				"component",
				"ledger",
				"connection_id",
				connIdKey(e.ConnectionId),
				"slot",
				headerSlot,
				"early_by",
				earlyBy,
				"permitted_clock_skew",
				defaultHeaderClockSkew,
				"possible_local_clock_skew",
				true,
			)
		}
		return false, nil
	}
	if ctx == nil {
		return false, errors.New("chainsync header admission context is nil")
	}
	if err := ls.slotClock.waitUntil(ctx, slotTime); err != nil {
		return false, fmt.Errorf("wait for header slot onset: %w", err)
	}
	return true, nil
}

// chainsyncHeaderCryptoPolicy distinguishes headers trusted by an explicitly
// disabled validation path (historical sync or Mithril coverage) from headers
// whose crypto check must wait for an epoch nonce. Both skip verification at
// chainsync time, but only the former may advance shared sync state.
func (ls *LedgerState) chainsyncHeaderCryptoPolicy(
	slot uint64,
) (verifyNow bool, trustedWithoutVerification bool) {
	validationEnabled, mithrilLedgerSlot := ls.validationStateSnapshot()
	if !validationEnabled {
		return false, true
	}
	if mithrilLedgerSlot != 0 && slot <= mithrilLedgerSlot {
		return false, true
	}
	if !ls.hasCachedEpochNonceForSlot(slot) {
		return false, false
	}
	return true, false
}

// recordAdmittedHeaderFrontier advances shared sync state only when the
// delivered header is now the locally admitted queue frontier. The peer's
// advertised tip is a separate, untrusted field in the ChainSync message;
// validating this header does not authenticate that claim.
func (ls *LedgerState) recordAdmittedHeaderFrontier(
	e ChainsyncEvent,
	headerTrusted bool,
) {
	if ls.chain == nil || !headerTrusted {
		return
	}
	admittedPoint := ls.chain.HeaderTip().Point
	if !pointMatches(admittedPoint, e.Point) {
		return
	}
	ls.advanceUpstreamTipSlot(admittedPoint.Slot)
	ls.publishAdmittedUpstreamTarget(e)
}

func (ls *LedgerState) shouldVerifyChainsyncHeaderCrypto(slot uint64) bool {
	return ls.shouldEnforceBlockPipelineCrypto(slot)
}

// shouldEnforceBlockPipelineCrypto mirrors the serial header path's
// validation-state gates for blocks read back from the primary chain. The
// pipeline workers still run for every submitted block, but their result must
// not reject trusted historical/Mithril data or a block whose epoch nonce is
// intentionally unavailable until ledger apply catches up.
func (ls *LedgerState) shouldEnforceBlockPipelineCrypto(slot uint64) bool {
	validationEnabled, mithrilLedgerSlot := ls.validationStateSnapshot()
	if !validationEnabled {
		return false
	}
	if mithrilLedgerSlot != 0 && slot <= mithrilLedgerSlot {
		return false
	}
	return ls.hasCachedEpochNonceForSlot(slot)
}

func (ls *LedgerState) hasCachedEpochNonceForSlot(slot uint64) bool {
	epoch, err := ls.epochForSlot(slot)
	return err == nil && len(epoch.Nonce) > 0
}

// tryResolveFork attempts to resolve a chain fork when an incoming header
// doesn't fit the local chain tip. The incoming header's prevHash identifies
// a block that exists on our local chain. If the peer's immediate prevHash
// is already unknown to us, the connection's chainsync cursor has drifted
// out of continuity with the local header queue and the correct recovery is
// a fresh FindIntersect on that connection rather than repeated mismatch
// counting.
//
// Returns true if the fork was resolved (chain rolled back or a re-sync was
// requested), false if the common ancestor was not found yet. Unexpected
// internal failures are returned as errors so callers do not treat them as
// ordinary header mismatches.
func (ls *LedgerState) tryResolveFork(
	e ChainsyncEvent,
	notFitErr chain.BlockNotFitChainTipError,
	pending *pendingPublishes,
) (bool, error) {
	localTip := ls.chain.Tip()
	praosComparison := ls.compareIncomingHeaderToLocalTip(
		e,
		localTip,
	)
	genesisActive, genesisWindow := ls.genesisSelectionState()
	// Once the selector has converged back to Praos, preserve the normal
	// length-first decision and avoid the more expensive fork-path density
	// walk for a candidate that cannot win.
	if !genesisActive && praosComparison != praos.ChainABetter {
		return false, nil
	}

	// Walk backward through the peer's recently seen header chain until
	// we find a hash that exists locally. The current header's prevHash is
	// only the common ancestor when the peer is handing us the first header
	// after the fork point; once the winning fork is several headers ahead,
	// we need the peer's recent ancestry to locate the real rollback point.
	prevHashBytes, err := hex.DecodeString(notFitErr.BlockPrevHash())
	if err != nil {
		ls.config.Logger.Warn(
			"failed to decode block prev hash for fork resolution",
			"component", "ledger",
			"error", err,
			"block_prev_hash", notFitErr.BlockPrevHash(),
		)
		return false, nil
	}
	ancestorPoint, forkPath, err := ls.findPeerForkPath(e, prevHashBytes)
	if err != nil {
		return false, fmt.Errorf(
			"unexpected error looking up common ancestor for prev hash %s: %w",
			notFitErr.BlockPrevHash(),
			err,
		)
	}
	if ancestorPoint == nil {
		// The peer's header stream is not continuous with our local chain
		// view or we have not yet seen enough of its ancestry to resolve
		// the fork locally. Request a chainsync re-sync so the intersect
		// protocol finds the common point with the peer.
		// Keep the candidate rollback point across the reset/reconnect
		// cycle; otherwise repeated stale-ancestor resyncs lose the only
		// point-keyed evidence that the same divergence is recurring.
		ls.reportUnrecoverableRollbackIfStuck(
			e.Point,
			event.ChainsyncResyncReasonRollbackNotFound,
			e.ConnectionId,
		)
		ls.config.Logger.Debug(
			"common ancestor not found locally, triggering chainsync re-sync",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"block_prev_hash", notFitErr.BlockPrevHash(),
		)
		ls.requestChainsyncResync(
			e.ConnectionId,
			event.ChainsyncResyncReasonRollbackNotFound,
			pending,
		)
		return true, nil
	}
	if genesisActive {
		peerDensity := genesisForkPathDensity(
			*ancestorPoint,
			genesisWindow,
			forkPath,
		)
		localDensity, densityErr := ls.localGenesisDensity(
			*ancestorPoint,
			genesisWindow,
		)
		if densityErr != nil {
			return false, densityErr
		}
		ls.config.Logger.Debug(
			"compared fork density from common intersection",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"intersection_slot", ancestorPoint.Slot,
			"genesis_window_slots", genesisWindow,
			"peer_density", peerDensity,
			"local_density", localDensity,
		)
		switch {
		case peerDensity < localDensity:
			return false, nil
		case peerDensity == localDensity &&
			praosComparison != praos.ChainABetter:
			// Equal density converges on the usual Praos length/VRF
			// comparison, matching selector peer-ranking behavior.
			return false, nil
		}
	}
	ancestorBlock, err := ls.blockByHash(ancestorPoint.Hash)
	if err != nil {
		return false, fmt.Errorf(
			"failed to reload common ancestor block %s: %w",
			hex.EncodeToString(ancestorPoint.Hash),
			err,
		)
	}

	rollbackPoint := *ancestorPoint

	// If the ancestor IS the local tip, the peer's fork segment extends
	// our chain directly — no rollback is needed. Skip the rollback to
	// avoid publishing a "local ledger rollback" event that would
	// trigger recovery and close all connections unnecessarily.
	if rollbackPoint.Slot == localTip.Point.Slot &&
		bytes.Equal(rollbackPoint.Hash, localTip.Point.Hash) {
		ls.config.Logger.Info(
			"fork extends from current tip, adding headers without rollback",
			"component", "ledger",
			"local_tip_slot", localTip.Point.Slot,
			"peer_tip_slot", e.Tip.Point.Slot,
			"fork_path_headers", len(forkPath),
			"connection_id", e.ConnectionId.String(),
		)
		for _, forkEvent := range forkPath {
			if err := ls.chain.AddBlockHeader(forkEvent.BlockHeader); err != nil {
				ls.config.Logger.Warn(
					"failed to queue header from fork extension",
					"component", "ledger",
					"error", err,
					"slot", forkEvent.Point.Slot,
					"connection_id", forkEvent.ConnectionId.String(),
				)
				// The failure (typically ErrHeaderQueueFull) leaves
				// whatever was already queued -- from this loop's earlier
				// iterations or from prior events -- exactly where it was.
				// If nothing is currently fetching it, kick a fetch so the
				// backlog still drains instead of stalling forever; see
				// ensureBlockfetchDrainingAfterForkQueueFailure.
				ls.ensureBlockfetchDrainingAfterForkQueueFailure(
					e.ConnectionId,
					pending,
				)
				return false, nil
			}
		}
		ls.headerMismatchCount = 0
		ls.rollbackHistory = nil
		if ls.config.BlockfetchRequestRangeFunc != nil &&
			ls.chain.HeaderCount() > 0 {
			ls.chainsyncBlockfetchMutex.Lock()
			if err := ls.restartQueuedBlockfetchAfterForkLocked(e.ConnectionId, pending); err != nil {
				ls.config.Logger.Warn(
					"failed to start blockfetch after fork extension",
					"component", "ledger",
					"error", err,
					"connection_id", e.ConnectionId.String(),
				)
			}
			ls.chainsyncBlockfetchMutex.Unlock()
		}
		return true, nil
	}

	ls.config.Logger.Info(
		"fork detected: rolling back to common ancestor",
		"component", "ledger",
		"local_tip_slot", localTip.Point.Slot,
		"peer_tip_slot", e.Tip.Point.Slot,
		"ancestor_slot", ancestorBlock.Slot,
		"ancestor_hash", hex.EncodeToString(ancestorBlock.Hash),
		"connection_id", e.ConnectionId.String(),
	)

	if err := ls.rollbackChainAndStateDeferred(rollbackPoint, pending); err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			// The ancestor resolved but the chain no longer holds it at that
			// index, so rolling back would splice a continuation onto a parent
			// the chain does not have (issue #3005). Re-intersect instead of
			// treating this as an internal failure.
			ls.config.Logger.Warn(
				"fork ancestor is no longer on the local chain, triggering chainsync re-sync",
				"component",
				"ledger",
				"error",
				err,
				"ancestor_slot",
				ancestorBlock.Slot,
				"ancestor_hash",
				hex.EncodeToString(ancestorBlock.Hash),
				"local_tip_slot",
				ls.chain.Tip().Point.Slot,
				"connection_id",
				e.ConnectionId.String(),
			)
			ls.headerMismatchCount = 0
			ls.rollbackHistory = nil
			ls.requestChainsyncResync(
				e.ConnectionId,
				event.ChainsyncResyncReasonRollbackNotFound,
				pending,
			)
			return true, nil
		}
		if errors.Is(err, chain.ErrRollbackExceedsSecurityParam) {
			reconciled, reconcileErr := ls.reconcileLivePrimaryChainLedgerDivergence(
				"fork resolution exceeds security parameter K",
				e.ConnectionId,
			)
			if reconcileErr != nil {
				return false, fmt.Errorf(
					"reconcile primary chain and ledger after over-K fork resolution: %w",
					reconcileErr,
				)
			}
			if reconciled {
				ls.resetChainsyncResyncState()
				ls.setChainsyncState(SyncingChainsyncState)
				return true, nil
			}
			// Fork exceeds security parameter K. We must not
			// follow a chain that requires rolling back more
			// than K blocks — this is a fundamental Ouroboros
			// security guarantee. Trigger a chainsync re-sync
			// immediately rather than waiting for
			// headerMismatchResyncThreshold retries.
			ls.config.Logger.Error(
				"fork exceeds security parameter K, "+
					"rejecting fork resolution",
				"component", "ledger",
				"ancestor_slot", ancestorBlock.Slot,
				"local_tip_slot",
				ls.chain.Tip().Point.Slot,
				"peer_tip_slot", e.Tip.Point.Slot,
			)
			// Reset mismatch state so the fallback path in the
			// caller does not fire a duplicate resync event.
			ls.headerMismatchCount = 0
			ls.rollbackHistory = nil
			pending.add(
				ls.config.EventBus,
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: e.ConnectionId,
						Reason:       event.ChainsyncResyncReasonForkResolutionExceedsK,
					},
				),
			)
			return true, nil
		} else {
			ls.config.Logger.Error(
				"failed to roll back to common ancestor",
				"component", "ledger",
				"error", err,
				"ancestor_slot", ancestorBlock.Slot,
			)
			return false, fmt.Errorf(
				"failed to roll back to common ancestor: %w",
				err,
			)
		}
	}
	// This fork rollback is genuine forward progress. Clear evidence from
	// stale-ancestor / failed-rollback cycles so an unrelated later fork does
	// not inherit the old divergence count.
	ls.clearUnrecoverableRollbacks()

	// Mark state as rollback so the next block header event logs
	// "switched to fork" and increments the fork metric.
	ls.setChainsyncState(RollbackChainsyncState)

	// Rollback succeeded — re-add the known peer fork segment from the
	// common ancestor forward. Re-adding only the latest mismatching header
	// works for one-block forks but fails once the winning fork is already
	// several headers ahead.
	for _, forkEvent := range forkPath {
		if err := ls.chain.AddBlockHeader(forkEvent.BlockHeader); err != nil {
			ls.config.Logger.Warn(
				"failed to queue header after fork rollback",
				"component", "ledger",
				"error", err,
				"slot", forkEvent.Point.Slot,
				"connection_id", forkEvent.ConnectionId.String(),
			)
			// The failure (typically ErrHeaderQueueFull) leaves whatever
			// was already queued -- from this loop's earlier iterations,
			// or from the rollback above having preserved existing queued
			// headers -- exactly where it was. If nothing is currently
			// fetching it, kick a fetch so the backlog still drains
			// instead of stalling forever; see
			// ensureBlockfetchDrainingAfterForkQueueFailure.
			ls.ensureBlockfetchDrainingAfterForkQueueFailure(
				e.ConnectionId,
				pending,
			)
			// Do not reset mismatch state — let the caller know the
			// resolution failed so subsequent mismatch tracking proceeds.
			return false, nil
		}
	}
	ls.headerMismatchCount = 0
	ls.rollbackHistory = nil
	if ls.config.BlockfetchRequestRangeFunc != nil &&
		ls.chain.HeaderCount() > 0 {
		ls.chainsyncBlockfetchMutex.Lock()
		if err := ls.restartQueuedBlockfetchAfterForkLocked(e.ConnectionId, pending); err != nil {
			ls.config.Logger.Warn(
				"failed to start blockfetch after fork rollback",
				"component", "ledger",
				"error", err,
				"connection_id", e.ConnectionId.String(),
			)
		}
		ls.chainsyncBlockfetchMutex.Unlock()
	}
	return true, nil
}

// handleEventBlockfetchBlockDeferred is handleEventBlockfetchBlock that threads
// the caller's pendingPublishes queue into flushPendingBlockfetchBlocksDeferred,
// so chain.update events emitted while chainsyncBlockfetchMutex is held are
// published only after it is released. A nil pubs preserves the standalone
// immediate-publish behaviour for test callers.
func (ls *LedgerState) handleEventBlockfetchBlockDeferred(
	e BlockfetchEvent,
	pubs *pendingPublishes,
) error {
	// Process blocks in small commit batches so they appear on the
	// chain promptly without paying a full blob transaction cost for
	// every single block. We still flush well before BatchDone to
	// avoid downstream ChainSync idle timeouts.
	if ls.chainsyncBlockfetchReadyChan == nil {
		return nil
	}
	fromPrimary := sameConnectionId(e.ConnectionId, ls.activeBlockfetchConnId)
	fromShadow := connIdKey(ls.shadowBlockfetchConnId) != "" &&
		sameConnectionId(e.ConnectionId, ls.shadowBlockfetchConnId)
	if !fromPrimary && !fromShadow {
		return nil
	}
	// Deduplicate: if the other peer already delivered this block,
	// discard silently. The winner records their latency sample.
	blockHashKey := hex.EncodeToString(e.Point.Hash)
	if ls.shadowBlockReceivedHashes != nil {
		if _, seen := ls.shadowBlockReceivedHashes[blockHashKey]; seen {
			return nil
		}
	} else {
		ls.shadowBlockReceivedHashes = make(map[string]struct{})
	}
	ls.shadowBlockReceivedHashes[blockHashKey] = struct{}{}
	// Record first-block latency for the winning peer.
	if !ls.firstBlockReceived && !ls.activeBlockfetchStart.IsZero() {
		ls.firstBlockReceived = true
		latency := time.Since(ls.activeBlockfetchStart)
		if ls.config.RecordBlockfetchLatencyFunc != nil {
			ls.config.RecordBlockfetchLatencyFunc(e.ConnectionId, latency)
		}
	}

	// Verify block header cryptographic proofs (VRF, KES).
	// Skip during historical sync (validationEnabled=false) because
	// historical blocks were already validated by the network.
	validationEnabled, _ := ls.validationStateSnapshot()
	if validationEnabled {
		var verifyErr error
		// Chainsync may already have verified the queued header before
		// blockfetch started. When the fetched block matches that first
		// verified queued header by point, a second verification is
		// redundant. Chain insertion still checks that the block matches the
		// queued header hash before accepting it.
		headerAlreadyVerified := ls.chain.FirstVerifiedHeaderMatchesPoint(
			e.Point,
		)
		if !headerAlreadyVerified &&
			!ls.hasCachedEpochNonceForSlot(e.Point.Slot) {
			if err := ls.flushPendingBlockfetchBlocksDeferred(pubs); err != nil {
				return err
			}
			headerAlreadyVerified = ls.chain.FirstVerifiedHeaderMatchesPoint(
				e.Point,
			)
		}
		if !headerAlreadyVerified {
			verifyErr = ls.verifyBlockHeaderCryptoBeforeApply(e.Block)
		} else {
			verifyErr = ls.verifyBlockHeaderStateWithEpochAdvance(
				e.Block,
				true,
				true,
			)
		}
		if verifyErr != nil {
			if errors.Is(verifyErr, errHeaderVerificationDeferred) {
				ls.markDeferredHeaderValidation(e.Point)
				if err := ls.persistDeferredHeaderValidation(e.Point, nil); err != nil {
					ls.clearDeferredHeaderValidation(e.Point)
					return err
				}
				ls.config.Logger.Debug(
					"deferring stateful block header verification until ledger apply",
					"component",
					"ledger",
					"slot",
					e.Point.Slot,
					"hash",
					hex.EncodeToString(e.Point.Hash),
					"error",
					verifyErr,
				)
			} else {
				return fmt.Errorf(
					"block header crypto verification failed: %w",
					verifyErr,
				)
			}
		}
	}
	ls.pendingBlockfetchEvents = append(ls.pendingBlockfetchEvents, e)
	ls.batchBlocksReceived++
	// If this block is the one a tracked range was failing to obtain, that
	// range is fetchable after all and its failure record is stale.
	ls.noteBlockfetchRangeProgress(e.Point)
	if len(ls.pendingBlockfetchEvents) >= blockfetchCommitBatchSize {
		if err := ls.flushPendingBlockfetchBlocksDeferred(pubs); err != nil {
			return err
		}
	}
	// Reset timeout timer since we received a block
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Reset(blockfetchBusyTimeout)
	}
	return nil
}

func (ls *LedgerState) nextBlockfetchConnId() (ouroboros.ConnectionId, bool) {
	if connIdKey(ls.selectedBlockfetchConnId) != "" {
		return ls.selectedBlockfetchConnId, true
	}
	if connIdKey(ls.activeBlockfetchConnId) == "" {
		return ouroboros.ConnectionId{}, false
	}
	return ls.activeBlockfetchConnId, true
}

func (ls *LedgerState) nextBlockfetchConnIdExcept(
	excludedConnId ouroboros.ConnectionId,
) (ouroboros.ConnectionId, bool) {
	if connIdKey(ls.selectedBlockfetchConnId) != "" &&
		!sameConnectionId(ls.selectedBlockfetchConnId, excludedConnId) {
		return ls.selectedBlockfetchConnId, true
	}
	if connIdKey(ls.activeBlockfetchConnId) == "" ||
		sameConnectionId(ls.activeBlockfetchConnId, excludedConnId) {
		return ouroboros.ConnectionId{}, false
	}
	return ls.activeBlockfetchConnId, true
}

func (ls *LedgerState) restartQueuedBlockfetchAfterForkLocked(
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	if ls.chainsyncBlockfetchReadyChan != nil {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
			ls.chainsyncBlockfetchTimeoutTimer = nil
		}
		ls.chainsyncBlockfetchTimerGeneration++
		ls.chainsyncBlockfetchReadyMutex.Lock()
		if ls.chainsyncBlockfetchReadyChan != nil {
			close(ls.chainsyncBlockfetchReadyChan)
			ls.chainsyncBlockfetchReadyChan = nil
		}
		ls.chainsyncBlockfetchReadyMutex.Unlock()
		if err := ls.flushPendingBlockfetchBlocksDeferred(pending); err != nil {
			ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
			ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
			return fmt.Errorf(
				"failed to flush stale blockfetch batch before restart: %w",
				err,
			)
		}
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	}
	ls.selectedBlockfetchConnId = connId
	return ls.startQueuedBlockfetchLocked(connId, pending)
}

// ensureBlockfetchDrainingAfterForkQueueFailure restarts blockfetch for
// already-queued headers when nothing is currently fetching them. It is
// called after tryResolveFork fails to append a fork-resolution header path
// onto the chain's header queue (most commonly because the queue is already
// at ErrHeaderQueueFull capacity): the success paths in tryResolveFork
// restart blockfetch via restartQueuedBlockfetchAfterForkLocked, but a
// partial failure previously just returned without doing so. Once the queue
// is completely full and no blockfetch is in flight, every later header --
// from any peer, fork or not -- fails chain.AddBlockHeader's capacity check
// before it can ever reach the "should I start a fetch" logic, so nothing
// would ever schedule a new blockfetch again and the backlog would never
// drain (found via issue #1894 phase 3 live-sync testing: the block-pipeline
// validate stage's extra CPU cost makes the header queue reachable at
// capacity more easily in practice, but this is a general, pre-existing gap
// in tryResolveFork, not specific to that flag -- reproduced live under
// BlockPipelineValidateEnabled, decode-only phase 1, and the pre-pipeline
// baseline alike).
//
// Unlike restartQueuedBlockfetchAfterForkLocked, this must not unconditionally
// interrupt an already-running batch: the same "queue full" outcome fires
// repeatedly, once per rejected header, while a healthy batch is still
// draining the existing backlog, and tearing that batch down every time
// would thrash forever instead of ever completing one.
func (ls *LedgerState) ensureBlockfetchDrainingAfterForkQueueFailure(
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) {
	if ls.config.BlockfetchRequestRangeFunc == nil ||
		ls.chain.HeaderCount() == 0 {
		return
	}
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	if ls.chainsyncBlockfetchReadyChan != nil ||
		ls.blockfetchContinuationPending {
		return
	}
	// Go through startQueuedBlockfetchOnLocked, not startQueuedBlockfetchLocked
	// directly, so a successful start here retargets selectedBlockfetchConnId
	// the same guarded way every other connection-switching path does: capture
	// the selection before the call (which releases chainsyncBlockfetchMutex
	// around the network request), and only move it to connId afterward if no
	// concurrent path already moved it elsewhere. Calling
	// startQueuedBlockfetchLocked directly here left selectedBlockfetchConnId
	// unretargeted after a successful start, so the next batch's connection
	// pick (nextBlockfetchConnId, which prefers selectedBlockfetchConnId) could
	// still land on a stale peer instead of the one actually now fetching.
	if err := ls.startQueuedBlockfetchOnLocked(connId, pending); err != nil {
		ls.config.Logger.Warn(
			"failed to start blockfetch after fork-resolution queue overflow, "+
				"dropping queued headers and requesting chainsync re-sync",
			"component", "ledger",
			"error", err,
			"connection_id", connId.String(),
		)
		// Nothing else schedules a fetch for these headers once the queue
		// is full and no blockfetch is in flight (see doc comment above):
		// leaving them queued after this failure would strand them
		// permanently. Clear them and ask for a fresh intersect instead,
		// matching noteBlockfetchRangeUnavailable's equivalent recovery.
		ls.clearQueuedHeaders()
		ls.requestChainsyncResync(
			connId,
			event.ChainsyncResyncReasonForkQueueOverflowRestartFailed,
			pending,
		)
	}
}

// blockfetchNoBlocksErrorText is the error text emitted by the gouroboros
// blockfetch client for MsgNoBlocks. That client currently exposes NoBlocks
// as a plain error rather than a sentinel, so keep the classification at this
// adapter boundary and do not treat every synchronous request error as a
// range-unavailable result.
const blockfetchNoBlocksErrorText = "block(s) not found"

func isBlockfetchNoBlocksError(err error) bool {
	return err != nil && strings.HasSuffix(
		strings.TrimSpace(err.Error()),
		blockfetchNoBlocksErrorText,
	)
}

// blockfetchRangeFailureState counts definitive failures to obtain one
// specific queued range, identified by its start point. Keying by point is
// what lets the count survive the unrelated traffic that separates real
// failures.
type blockfetchRangeFailureState struct {
	slot  uint64
	hash  string
	count int
}

func (s blockfetchRangeFailureState) matches(point ocommon.Point) bool {
	return s.count > 0 &&
		s.slot == point.Slot &&
		s.hash == string(point.Hash)
}

// noteBlockfetchRangeProgress discards the failure record when the range it
// was tracking has now been delivered. Earlier misses against a range that
// turned out to be fetchable must not combine with a later unrelated miss, so
// a peer that was only briefly behind is never punished. Deliveries for any
// other range leave the record alone: they say nothing about whether the stuck
// range can be obtained.
func (ls *LedgerState) noteBlockfetchRangeProgress(point ocommon.Point) {
	if ls.blockfetchRangeFailure.matches(point) {
		ls.blockfetchRangeFailure = blockfetchRangeFailureState{}
	}
}

// noteBlockfetchRangeUnavailable records one definitive failed attempt to
// obtain the queued header range starting at start and, once that same range
// has failed blockfetchMaxSameRangeFailures times, drops the queue and asks
// for a fresh intersect. It reports whether it dropped the queue.
//
// Dropping the headers is the purpose rather than a side effect: while a
// header sits at the head of the queue, Chain.AddBlock rejects every locally
// forged block as not matching the first pending header, so a header whose
// body no peer can serve halts block production until it is cleared. This
// happens routinely after a tip slot battle, where two producers each roll
// their own block back in favour of the other's and neither can then serve
// the body the other queued.
func (ls *LedgerState) noteBlockfetchRangeUnavailable(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	reason string,
	pending *pendingPublishes,
) bool {
	if ls.chain == nil || ls.chain.HeaderCount() == 0 {
		return false
	}
	if start.Slot == 0 && len(start.Hash) == 0 {
		start, _ = ls.chain.HeaderRange(blockfetchBatchSize)
	}
	if ls.blockfetchRangeFailure.matches(start) {
		ls.blockfetchRangeFailure.count++
	} else {
		ls.blockfetchRangeFailure = blockfetchRangeFailureState{
			slot:  start.Slot,
			hash:  string(start.Hash),
			count: 1,
		}
	}
	if ls.blockfetchRangeFailure.count < blockfetchMaxSameRangeFailures {
		return false
	}
	ls.config.Logger.Warn(
		"blockfetch could not obtain queued range, dropping queued headers and requesting chainsync re-sync",
		"component",
		"ledger",
		"connection_id",
		connId.String(),
		"remaining_headers",
		ls.chain.HeaderCount(),
		"range_start_slot",
		start.Slot,
		"range_start_hash",
		hex.EncodeToString(start.Hash),
		"range_failures",
		ls.blockfetchRangeFailure.count,
		"reason",
		reason,
	)
	// Start a fresh count: the peer may re-offer the same header, and it
	// must earn another full set of failures before the queue is dropped
	// again rather than being dropped on every later miss.
	ls.blockfetchRangeFailure = blockfetchRangeFailureState{}
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.clearQueuedHeaders()
	ls.requestChainsyncResync(
		connId,
		event.ChainsyncResyncReasonBlockfetchRangeUnavailable,
		pending,
	)
	return true
}

// startQueuedBlockfetchOnLocked starts the queued range on connId and, if that
// succeeds, retargets the blockfetch selection to it.
//
// Every path that moves the fetch to a different connection has to go through
// this rather than calling startQueuedBlockfetchLocked directly:
// nextBlockfetchConnId prefers selectedBlockfetchConnId over
// activeBlockfetchConnId, and the batch-completion continuation uses it to pick
// the connection for the next batch. Starting on one connection while the
// selection still names another therefore recovers a single batch from the
// working peer and sends the next one back to the peer that just failed.
//
// Callers that already own the selection (handoffPipelineOnSwitchLocked,
// handleEventChainsyncBlockHeaderWithPending, and the recovery replay, each of
// which assigns it first) may call startQueuedBlockfetchLocked directly. Note
// that selectInitialBlockfetchConn is the identity function today, so that path
// starts on the connection it just selected; if it ever returns a different
// connection it needs this helper too.
//
// One current caller gains nothing from it: the timeout handler's alternate
// connection comes from nextBlockfetchConnIdExcept, which can only return a
// connection other than the excluded one when that connection already is the
// selection -- a failed attempt has by then overwritten activeBlockfetchConnId
// with the excluded connection, so the retarget there cannot change anything. It
// is routed through here anyway so the invariant lives in one place rather than
// depending on that reachability argument staying true.
func (ls *LedgerState) startQueuedBlockfetchOnLocked(
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	// Retarget only once the start has actually succeeded. Moving the selection
	// on an attempt would discard what the caller's own fallback needs:
	// nextBlockfetchConnIdExcept picks the next candidate by excluding the
	// connection that just failed and preferring the current selection, so
	// pointing the selection at the failed connection first collapses that
	// lookup onto activeBlockfetchConnId instead.
	//
	// Captured first because startQueuedBlockfetchLocked releases
	// chainsyncBlockfetchMutex around the network request: a connection switch
	// or close can install a newer selection while we are outside it, and this
	// retarget must not overwrite that. If the selection moved, the concurrent
	// writer's choice is the current one and wins.
	before := ls.selectedBlockfetchConnId
	if err := ls.startQueuedBlockfetchLocked(connId, pending); err != nil {
		return err
	}
	if sameConnectionId(ls.selectedBlockfetchConnId, before) {
		ls.selectedBlockfetchConnId = connId
	}
	return nil
}

func (ls *LedgerState) startQueuedBlockfetchLocked(
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	return ls.startQueuedBlockfetchLockedWithWaitSignal(connId, pending, nil)
}

// startQueuedBlockfetchLockedWithWaitSignal is the same scheduling path with
// an optional test synchronization signal for the prior-request drain.
func (ls *LedgerState) startQueuedBlockfetchLockedWithWaitSignal(
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
	waitStarted chan<- struct{},
) error {
	// The caller owns chainsyncBlockfetchMutex. Keep the reservation and
	// timeout state under that lock, but never hold it across the network
	// request below. BlockFetch delivers blocks from its protocol receive
	// goroutine, and that delivery waits for this same mutex in
	// handleEventBlockfetch. A request on a busy client can wait for the
	// previous delivery to finish, so calling GetBlockRange while holding the
	// mutex creates a lock cycle:
	//
	//   chainsync -> GetBlockRange.acquireBusy -> blockfetch event -> ledger
	//   blockfetch mutex
	//
	// The lock is temporarily released around each external request and is
	// reacquired before any state is inspected or changed. Callers still own
	// the lock when this function returns.
	if err := ls.waitForBlockfetchRequestLockedWithSignal(connId, waitStarted); err != nil {
		return err
	}
	if ls.ctx != nil {
		if err := ls.ctx.Err(); err != nil {
			return fmt.Errorf("blockfetch request canceled: %w", err)
		}
	}
	if ls.chain.HeaderCount() == 0 {
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		return nil
	}
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = connId
	// Reset per-batch shadow state. The normal batch-completion path
	// goes through blockfetchRequestRangeCleanup, which clears these,
	// but restartQueuedBlockfetchAfterForkLocked re-enters this
	// function without that helper, so resetting unconditionally here
	// prevents stale shadow IDs and dedup hashes from leaking across a
	// fork-restart.
	ls.shadowBlockfetchConnId = ouroboros.ConnectionId{}
	ls.shadowBlockReceivedHashes = nil
	ls.batchBlocksReceived = 0
	ls.activeBlockfetchStart = time.Now()
	ls.firstBlockReceived = false
	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	ls.blockfetchRequestGeneration++
	primaryRequestGeneration := ls.blockfetchRequestGeneration
	ls.blockfetchPrimaryRequestGeneration = primaryRequestGeneration
	primaryRequestDone := ls.beginBlockfetchRequestLocked(connId)
	ls.armBlockfetchTimeoutLocked(connId)
	batchReadyChan := ls.chainsyncBlockfetchReadyChan
	ls.chainsyncBlockfetchMutex.Unlock()
	if err := ls.blockfetchRequestRangeStart(
		connId,
		headerStart,
		headerEnd,
	); err != nil {
		ls.chainsyncBlockfetchMutex.Lock()
		ls.endBlockfetchRequestLocked(connId, primaryRequestDone)
		if ls.blockfetchPrimaryRequestGeneration == primaryRequestGeneration {
			ls.blockfetchPrimaryRequestGeneration = 0
			ls.resetBlockfetchInFlightTimeoutsLocked()
		}
		// A blockfetch callback may have completed this batch, or a
		// callback may have started its continuation, while the request
		// was outside the lock. In either case the request error is stale
		// and must not tear down the newer batch.
		if ls.chainsyncBlockfetchReadyChan != batchReadyChan {
			return nil
		}
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		// A peer whose range server rejects the start point answers
		// NoBlocks, which gouroboros resolves into this synchronous error
		// rather than a BatchDone event. Several callers only log what we
		// return (notably the fork-resolution restarts), so genuine NoBlocks
		// must be recorded here, at the single point every queued-range
		// request passes through. Other synchronous errors are transport,
		// shutdown, or wiring failures and must not poison this range's
		// unavailable count.
		if isBlockfetchNoBlocksError(err) {
			ls.noteBlockfetchRangeUnavailable(
				connId,
				headerStart,
				fmt.Sprintf("blockfetch request returned NoBlocks: %v", err),
				pending,
			)
		}
		return err
	}
	ls.chainsyncBlockfetchMutex.Lock()
	ls.endBlockfetchRequestLocked(connId, primaryRequestDone)
	if ls.blockfetchPrimaryRequestGeneration == primaryRequestGeneration {
		ls.blockfetchPrimaryRequestGeneration = 0
		ls.resetBlockfetchInFlightTimeoutsLocked()
	}
	// The request can synchronously release a completed batch before it
	// returns (for example when a peer answers with an immediate empty
	// response). Do not dispatch shadow work for a batch that no longer
	// exists.
	if ls.chainsyncBlockfetchReadyChan != batchReadyChan {
		return nil
	}
	// Near tip: dispatch the same range to one shadow peer if any
	// tracked peer has already seen the first block header. Whichever
	// peer responds first wins; duplicates are dropped by hash. Skip
	// when the primary peer is already fast — the duplicate decode
	// cost on the loser path isn't worth the marginal latency win.
	//
	// Threshold: prefer median-based when enough peers have samples
	// (only shadow when the primary is in the slow tail of the
	// observed population). Fall back to the fixed 250ms cutoff when
	// the population is too small to trust a median.
	primaryFastEnough := false
	cutoffLabel := "fallback"
	hasPrimarySample := false
	if ls.config.BlockfetchLatencyFunc != nil {
		if primaryLatency, ok := ls.config.BlockfetchLatencyFunc(
			connId,
		); ok && primaryLatency > 0 {
			hasPrimarySample = true
			cutoff := shadowBlockfetchPrimarySlowThreshold
			if ls.config.BlockfetchLatencyMedianFunc != nil {
				if median, samples := ls.config.BlockfetchLatencyMedianFunc(); samples >= shadowBlockfetchMedianMinSamples &&
					median > 0 {
					cutoff = time.Duration(
						float64(median) *
							shadowBlockfetchMedianMultiplier,
					)
					cutoffLabel = "median"
				}
			}
			if primaryLatency < cutoff {
				primaryFastEnough = true
			}
		}
	}
	nearTip := ls.chain.HeaderCount() <= shadowBlockfetchMaxHeaders
	if nearTip {
		gatePath := ""
		gateCutoff := cutoffLabel
		if !hasPrimarySample {
			gateCutoff = "no_sample"
		}
		switch {
		case primaryFastEnough:
			gatePath = "skipped_fast"
		case ls.config.PeersWithBlockFunc == nil ||
			ls.config.BlockfetchRequestRangeFunc == nil:
			// Wiring not present — count as skip but separate label.
			gatePath = "skipped_unwired"
		default:
			dispatched := false
			for _, shadowConn := range ls.config.PeersWithBlockFunc(
				connId,
				headerStart,
			) {
				if connIdKey(shadowConn) == "" ||
					sameConnectionId(shadowConn, connId) {
					continue
				}
				if ls.blockfetchShadowRequestsInFlight != nil {
					if _, inFlight := ls.blockfetchShadowRequestsInFlight[connIdKey(shadowConn)]; inFlight {
						continue
					}
				}
				// Publish the shadow owner before releasing the mutex so an
				// immediately returned block is accepted by the blockfetch
				// handler. Restore it if the request itself fails.
				ls.shadowBlockfetchConnId = shadowConn
				if ls.blockfetchShadowRequestsInFlight == nil {
					ls.blockfetchShadowRequestsInFlight = make(
						map[string]struct{},
					)
				}
				shadowConnKey := connIdKey(shadowConn)
				ls.blockfetchShadowRequestsInFlight[shadowConnKey] = struct{}{}
				shadowRequestDone := ls.beginBlockfetchRequestLocked(shadowConn)
				ls.chainsyncBlockfetchMutex.Unlock()
				err := ls.config.BlockfetchRequestRangeFunc(
					shadowConn,
					headerStart,
					headerEnd,
				)
				ls.chainsyncBlockfetchMutex.Lock()
				ls.endBlockfetchRequestLocked(shadowConn, shadowRequestDone)
				delete(ls.blockfetchShadowRequestsInFlight, shadowConnKey)
				if ls.chainsyncBlockfetchReadyChan != batchReadyChan {
					return nil
				}
				if err != nil {
					ls.shadowBlockfetchConnId = ouroboros.ConnectionId{}
					ls.config.Logger.Debug(
						"shadow blockfetch dispatch failed, trying next candidate",
						"component",
						"ledger",
						"shadow_connection_id",
						shadowConn.String(),
						"error",
						err,
					)
					continue
				}
				ls.config.Logger.Debug(
					"dispatched shadow blockfetch",
					"component", "ledger",
					"primary_connection_id", connId.String(),
					"shadow_connection_id", shadowConn.String(),
					"header_start_slot", headerStart.Slot,
				)
				dispatched = true
				break // one shadow is enough
			}
			if dispatched {
				gatePath = "dispatched"
			} else {
				gatePath = "skipped_no_peer"
			}
		}
		// Tests construct LedgerState without metrics; guard against
		// a nil CounterVec so the production codepath stays simple.
		if gatePath != "" && ls.metrics.shadowGateDecisions != nil {
			ls.metrics.shadowGateDecisions.WithLabelValues(
				gatePath,
				gateCutoff,
			).Inc()
		}
	}
	return nil
}

// startQueuedBlockfetchFromEventLocked schedules a continuation without
// running the synchronous blockfetch request on the ledger.blockfetch
// subscriber. GetBlockRange does not return until the peer sends BatchDone;
// invoking it from handleEventBlockfetchBatchDone would block the only
// subscriber that can consume that BatchDone and the following block events.
//
// The caller owns chainsyncBlockfetchMutex. The continuation worker acquires
// it before entering startQueuedBlockfetchLocked, so the pending flag closes
// the small gap where a chainsync handler could otherwise start a competing
// batch after the current handler releases the lock.
func (ls *LedgerState) startQueuedBlockfetchFromEventLocked(
	connId ouroboros.ConnectionId,
	resyncConnId ouroboros.ConnectionId,
	reason string,
) {
	if ls.closed.Load() || ls.blockfetchContinuationPending {
		return
	}
	ls.blockfetchContinuationPending = true
	ls.blockfetchContinuationMu.Lock()
	if ls.closed.Load() {
		ls.blockfetchContinuationPending = false
		ls.blockfetchContinuationMu.Unlock()
		return
	}
	ls.blockfetchContinuationWG.Add(1)
	ls.blockfetchContinuationMu.Unlock()
	go func() {
		defer ls.blockfetchContinuationWG.Done()
		var pending pendingPublishes
		ls.chainsyncBlockfetchMutex.Lock()
		if ls.closed.Load() {
			ls.blockfetchContinuationPending = false
			ls.chainsyncBlockfetchMutex.Unlock()
			return
		}
		ls.blockfetchContinuationPending = false
		err := ls.startQueuedBlockfetchOnLocked(connId, &pending)
		if err != nil {
			// A continuation can race a peer disconnect just as the previous
			// batch completes. Try the next tracked peer before abandoning the
			// queued headers, matching the synchronous continuation path.
			retryConnId := ls.selectRetryBlockfetchConn(connId)
			if connIdKey(retryConnId) != "" &&
				!sameConnectionId(retryConnId, connId) {
				err = ls.startQueuedBlockfetchOnLocked(
					retryConnId,
					&pending,
				)
			}
			if err != nil {
				ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
				ls.clearQueuedHeaders()
				ls.requestChainsyncResync(
					resyncConnId,
					fmt.Sprintf("%s: %v", reason, err),
					&pending,
				)
			}
		}
		ls.chainsyncBlockfetchMutex.Unlock()
		pending.flush()
	}()
}

// flushPendingBlockfetchBlocksDeferred is flushPendingBlockfetchBlocks that
// queues each committed block's chain.update onto pubs instead of letting the
// chain publish it inline. The blockfetch drain runs under
// chainsyncBlockfetchMutex; an inline, back-pressured chain.update publish
// there can park the drain with the mutex held, which then blocks
// handleEventChainsync on the same mutex and fills the ledger.chainsync buffer
// -- the preview drain deadlock. Queueing on the caller's pendingPublishes
// moves publication to after the mutex is released. A nil pubs publishes
// immediately (the unlocked / test path), per pendingPublishes' nil-receiver
// contract.
func (ls *LedgerState) flushPendingBlockfetchBlocksDeferred(
	pubs *pendingPublishes,
) error {
	if len(ls.pendingBlockfetchEvents) == 0 {
		return nil
	}
	pending := ls.pendingBlockfetchEvents
	ls.pendingBlockfetchEvents = ls.pendingBlockfetchEvents[:0]
	// Commit each block before exposing it on the primary chain. The chain tip
	// is used immediately by fork detection, so batching blob writes behind an
	// already-advanced in-memory tip can strand the node on a fork when ancestor
	// lookups hit uncommitted state.
	for _, pendingEvent := range pending {
		evt, addBlockErr := ls.chain.AddBlockWithPointDeferred(
			pendingEvent.Block,
			pendingEvent.Point,
			nil,
		)
		if addBlockErr == nil {
			// Defer this block's chain.update past chainsyncBlockfetchMutex
			// rather than publishing inline. AddBlockWithPointDeferred has
			// already enqueued evt on the chain's shared sequencer under
			// c.mutex (so it is ordered against a concurrent rollback in true
			// mutation order); register the chain so the caller drains that
			// sequencer once the mutex is released. See
			// flushPendingBlockfetchBlocksDeferred, pendingPublishes and
			// chain.Chain.PublishPendingChainUpdates.
			if evt.Type != "" {
				pubs.drainChain(ls.chain)
			}
			// Audit only after the body has extended the queued chain. A body
			// from an abandoned fetch may still be delivered after a fork
			// restart; auditing it here would poison producedTxs with stale
			// fork transactions.
			validationEnabled, _ := ls.validationStateSnapshot()
			ls.auditContinuationBlock(pendingEvent, validationEnabled)
			ls.checkSlotBattle(pendingEvent, nil)
			continue
		}
		ls.clearDeferredHeaderValidation(pendingEvent.Point)
		if err := ls.clearPersistentDeferredHeaderValidation(
			pendingEvent.Point,
			nil,
		); err != nil {
			return err
		}
		var notFitErr chain.BlockNotFitChainTipError
		var notMatchErr chain.BlockNotMatchHeaderError
		ignored := errors.As(addBlockErr, &notFitErr) ||
			errors.As(addBlockErr, &notMatchErr)
		if !ignored {
			return fmt.Errorf(
				"failed processing block event: add chain block: %w",
				addBlockErr,
			)
		}
		ls.config.Logger.Warn(
			fmt.Sprintf(
				"ignoring blockfetch block: %s",
				addBlockErr,
			),
		)
		if errors.As(addBlockErr, &notMatchErr) {
			ls.clearQueuedHeaders()
		}
		ls.checkSlotBattle(pendingEvent, addBlockErr)
	}
	ls.chain.NotifyIterators()
	return nil
}

// GenesisBlockHash returns the Byron genesis hash from config, which is used
// as the block hash for the synthetic genesis block that holds genesis UTxO data.
// This mirrors how the Shelley epoch nonce uses the Shelley genesis hash.
func GenesisBlockHash(cfg *cardano.CardanoNodeConfig) ([32]byte, error) {
	if cfg == nil || cfg.ByronGenesisHash == "" {
		return [32]byte{}, errors.New(
			"byron genesis hash not available in config",
		)
	}
	hashBytes, err := hex.DecodeString(cfg.ByronGenesisHash)
	if err != nil {
		return [32]byte{}, fmt.Errorf("decode Byron genesis hash: %w", err)
	}
	if len(hashBytes) != 32 {
		return [32]byte{}, fmt.Errorf(
			"invalid Byron genesis hash length: expected 32 bytes, got %d",
			len(hashBytes),
		)
	}
	var hash [32]byte
	copy(hash[:], hashBytes)
	return hash, nil
}

// genesisStakeDelegations converts the delegators returned by the Shelley
// genesis parser into the metadata representation used by SetGenesisStaking.
// InitialPools includes both the legacy staking fields and the Musashi
// extraConfig fields, so using its result keeps those bootstrap formats in
// sync.
func genesisStakeDelegations(
	poolDelegators map[string][]lcommon.Address,
) (map[string]string, error) {
	ret := make(map[string]string)
	poolIDs := make([]string, 0, len(poolDelegators))
	for poolID := range poolDelegators {
		poolIDs = append(poolIDs, poolID)
	}
	slices.Sort(poolIDs)
	for _, poolID := range poolIDs {
		delegators := poolDelegators[poolID]
		for _, delegator := range delegators {
			stakeKeyHash := (&delegator).StakeKeyHash()
			stakeKeyHex := hex.EncodeToString(stakeKeyHash[:])
			if existingPoolID, ok := ret[stakeKeyHex]; ok {
				if existingPoolID == poolID {
					continue
				}
				return nil, fmt.Errorf(
					"stake key hash %s delegated to multiple genesis pools %s and %s",
					stakeKeyHex,
					existingPoolID,
					poolID,
				)
			}
			ret[stakeKeyHex] = poolID
		}
	}
	return ret, nil
}

func (ls *LedgerState) createGenesisBlock() error {
	// Get the Byron genesis hash to use as the synthetic block hash.
	// This mirrors how the Shelley epoch nonce uses the Shelley genesis hash.
	genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
	if err != nil {
		return fmt.Errorf("get genesis block hash: %w", err)
	}

	if ls.currentTip.Point.Slot > 0 {
		// Validate existing chain data matches the current genesis config.
		// If genesis CBOR exists in the blob store with the expected hash,
		// the database was created with a matching genesis. Older databases
		// may still be missing the slot-0 network-state baseline.
		if ls.db.HasGenesisCbor(0, genesisHash[:]) {
			if err := ls.ensureGenesisConstitution(nil); err != nil {
				return err
			}
			return ls.ensureGenesisNetworkState()
		}
		// Check if genesis CBOR exists but with a different hash.
		// This indicates the database was created for a different
		// network (e.g., mainnet DB with preview config) — fail fast.
		if ls.db.HasAnyGenesisCbor(0) {
			ls.config.Logger.Warn(
				"slot-0 CBOR exists but does not match synthetic genesis hash, creating genesis block",
				"component",
				"ledger",
				"expected_hash",
				hex.EncodeToString(genesisHash[:]),
			)
		}
		// Genesis CBOR missing (e.g., after Mithril bootstrap which
		// imports ledger state and ImmutableDB blocks but does not
		// create the synthetic genesis block). Fall through to
		// create it now. All storage operations are idempotent.
		ls.config.Logger.Info(
			"genesis block CBOR missing, creating it now",
			"component", "ledger",
		)
	}

	txn := ls.db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Record genesis UTxOs
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		byronGenesisUtxos, err := byronGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Byron genesis UTxOs: %w", err)
		}
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		shelleyGenesisUtxos, err := shelleyGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Shelley genesis UTxOs: %w", err)
		}
		if len(byronGenesisUtxos)+len(shelleyGenesisUtxos) == 0 {
			return errors.New("failed to generate genesis UTxOs")
		}
		ls.config.Logger.Info(
			fmt.Sprintf("creating %d genesis UTxOs (%d Byron, %d Shelley)",
				len(byronGenesisUtxos)+len(shelleyGenesisUtxos),
				len(byronGenesisUtxos),
				len(shelleyGenesisUtxos),
			),
			"component", "ledger",
		)

		// Group genesis UTxOs by transaction hash
		genesisUtxos := slices.Concat(byronGenesisUtxos, shelleyGenesisUtxos)
		genesisReserves, err := genesisReserveBalance(
			shelleyGenesis.MaxLovelaceSupply,
			genesisUtxos,
		)
		if err != nil {
			return fmt.Errorf("calculate genesis reserves: %w", err)
		}
		txUtxos := make(map[[32]byte][]lcommon.Utxo)
		for i := range genesisUtxos {
			txHash := genesisUtxos[i].Id.Id()
			var txHashArray [32]byte
			copy(txHashArray[:], txHash.Bytes())

			// Generate CBOR for genesis UTxO outputs since they don't have original CBOR
			cborData, err := cbor.Encode(genesisUtxos[i].Output)
			if err != nil {
				return fmt.Errorf("encode genesis UTxO output to CBOR: %w", err)
			}

			// Create a new Utxo with CBOR-encoded output
			var newOutput lcommon.TransactionOutput
			switch output := genesisUtxos[i].Output.(type) {
			case byron.ByronTransactionOutput:
				newByronOutput := output
				(&newByronOutput).SetCbor(cborData)
				newOutput = newByronOutput
			case shelley.ShelleyTransactionOutput:
				newShelleyOutput := output
				(&newShelleyOutput).SetCbor(cborData)
				newOutput = newShelleyOutput
			default:
				return fmt.Errorf("unsupported genesis UTxO output type: %T", genesisUtxos[i].Output)
			}

			txUtxos[txHashArray] = append(txUtxos[txHashArray], lcommon.Utxo{
				Id:     genesisUtxos[i].Id,
				Output: newOutput,
			})
		}

		// Build synthetic genesis block with proper structure:
		// Block -> Transactions -> Outputs (UTxOs)
		//
		// CBOR structure:
		// [                                    // block: array of transactions
		//   {0: tx_hash, 1: [output, ...]},    // transaction 1
		//   {0: tx_hash, 1: [output, ...]},    // transaction 2
		//   ...
		// ]
		//
		// We track byte offsets for each output within this structure.
		utxoOffsets := make(map[database.UtxoRef]database.CborOffset)

		// Sort transaction hashes for deterministic ordering
		txHashes := make([][32]byte, 0, len(txUtxos))
		for txHash := range txUtxos {
			txHashes = append(txHashes, txHash)
		}
		slices.SortFunc(txHashes, func(a, b [32]byte) int {
			return bytes.Compare(a[:], b[:])
		})

		// Build the block structure manually to track exact byte offsets
		// We need to know where each output CBOR starts within the block
		blockCbor, err := buildGenesisBlockCbor(
			txHashes,
			txUtxos,
			utxoOffsets,
			genesisHash,
		)
		if err != nil {
			return fmt.Errorf("build genesis block cbor: %w", err)
		}

		// Store synthetic genesis block CBOR.
		// We use SetGenesisCbor to avoid creating a block index entry that
		// would cause the chain iterator to include it (genesis is already
		// handled separately during initialization).
		if err := ls.db.SetGenesisCbor(0, genesisHash[:], blockCbor, txn); err != nil {
			return fmt.Errorf("store genesis cbor: %w", err)
		}

		// Store each genesis transaction with its UTxOs
		for txHashArray, utxos := range txUtxos {
			if err := ls.db.SetGenesisTransaction(
				txHashArray[:],
				genesisHash[:],
				utxos,
				utxoOffsets,
				txn,
			); err != nil {
				return fmt.Errorf(
					"set genesis transaction %x: %w",
					txHashArray[:8],
					err,
				)
			}
		}

		// The initial reserves are the maximum supply that was not placed in
		// circulation by either genesis configuration. All later epoch, MIR,
		// governance, and donation updates build from this slot-0 baseline.
		if err := ls.db.Metadata().SetNetworkState(
			0,
			genesisReserves,
			0,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("set genesis network state: %w", err)
		}

		// The delayed reward calculation reads the ADA pots row for epoch
		// newEpoch-1, so the boundary into epoch 1 reads epoch 0's. Every
		// later epoch's row is written by saveRewardAdaPotsForEpoch at its
		// own rollover; epoch 0 has no rollover, so its row is the same
		// slot-0 baseline written above. Fees are 0 because no epoch
		// precedes epoch 0.
		if err := ls.saveGenesisRewardAdaPots(
			genesisReserves,
			txn.Metadata(),
		); err != nil {
			return err
		}

		ls.config.Logger.Info(
			fmt.Sprintf("stored %d genesis transactions with %d total UTxOs",
				len(txUtxos),
				len(genesisUtxos),
			),
			"component", "ledger",
			"treasury", 0,
			"reserves", genesisReserves,
		)

		// Load genesis staking data (pool registrations + delegations)
		genesisPools, poolDelegators, err := shelleyGenesis.InitialPools()
		if err != nil {
			return fmt.Errorf("parse genesis staking: %w", err)
		}
		genesisStake, err := genesisStakeDelegations(poolDelegators)
		if err != nil {
			return fmt.Errorf("parse genesis stake delegations: %w", err)
		}
		if len(genesisPools) > 0 ||
			len(genesisStake) > 0 {
			ls.config.Logger.Info(
				fmt.Sprintf(
					"loading genesis staking: %d pools, %d delegations",
					len(genesisPools),
					len(genesisStake),
				),
				"component", "ledger",
			)
			if err := ls.db.SetGenesisStaking(
				genesisPools,
				genesisStake,
				uint64(shelleyGenesis.ProtocolParameters.KeyDeposit),
				genesisHash[:],
				txn,
			); err != nil {
				return fmt.Errorf("set genesis staking: %w", err)
			}
		}

		// Load Conway genesis bootstrap data (initial DReps and
		// stake/vote delegations). The conway-genesis.json may declare
		// pre-existing DReps and delegations for test networks; mainnet
		// has none.
		conwayGenesis := ls.config.CardanoNodeConfig.ConwayGenesis()
		if conwayGenesis != nil &&
			(len(conwayGenesis.InitialDReps) > 0 ||
				len(conwayGenesis.Delegs) > 0) {
			ls.config.Logger.Info(
				fmt.Sprintf(
					"loading genesis governance: %d initial dreps, %d delegations",
					len(conwayGenesis.InitialDReps),
					len(conwayGenesis.Delegs),
				),
				"component", "ledger",
			)
			if err := ls.db.SetGenesisGovernance(
				conwayGenesis.InitialDReps,
				conwayGenesis.Delegs,
				genesisHash[:],
				txn,
			); err != nil {
				return fmt.Errorf("set genesis governance: %w", err)
			}
		}

		// The Conway genesis constitution is the chain's enacted
		// constitution until a NewConstitution action replaces it, and
		// guardrails validation needs it from the first block.
		if err := ls.ensureGenesisConstitution(txn); err != nil {
			return err
		}

		return nil
	})
	return err
}

// ensureGenesisConstitution records the Conway genesis constitution as the
// chain's slot-0 constitution when the ledger holds no constitution yet.
//
// The lookup returns the highest non-deleted added_slot, so a constitution
// enacted by a later NewConstitution action, or imported from a ledger-state
// snapshot, is always preferred over this slot-0 row. Re-running against a
// store that already holds a constitution writes nothing, which keeps
// restart and genesis replay idempotent.
func (ls *LedgerState) ensureGenesisConstitution(txn *database.Txn) error {
	genesisConstitution, err := governance.ConstitutionFromGenesis(
		ls.config.CardanoNodeConfig.ConwayGenesis(),
	)
	if err != nil {
		return fmt.Errorf("parse genesis constitution: %w", err)
	}
	if genesisConstitution == nil {
		return nil
	}
	existing, err := ls.db.GetConstitution(txn)
	if err != nil {
		return fmt.Errorf("get existing constitution: %w", err)
	}
	if existing != nil {
		return nil
	}
	if err := ls.db.SetConstitution(genesisConstitution, txn); err != nil {
		return fmt.Errorf("set genesis constitution: %w", err)
	}
	ls.config.Logger.Info(
		"recorded Conway genesis constitution",
		"component", "ledger",
		"anchor_url", genesisConstitution.AnchorURL,
		"guardrails_script_hash",
		hex.EncodeToString(genesisConstitution.PolicyHash),
	)
	return nil
}

// ensureGenesisNetworkState initializes the slot-0 treasury/reserves baseline
// for a matching pre-existing genesis database that has no network-state rows.
func (ls *LedgerState) ensureGenesisNetworkState() error {
	state, err := ls.db.Metadata().GetNetworkState(nil)
	if err != nil {
		return fmt.Errorf("get existing network state: %w", err)
	}
	pots, err := ls.db.Metadata().GetRewardAdaPots(0, nil)
	if err != nil {
		return fmt.Errorf("get existing epoch 0 reward ADA pots: %w", err)
	}
	if state != nil && pots != nil {
		return nil
	}

	byronUtxos, err := ls.config.CardanoNodeConfig.ByronGenesis().GenesisUtxos()
	if err != nil {
		return fmt.Errorf("generate Byron genesis UTxOs: %w", err)
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	shelleyUtxos, err := shelleyGenesis.GenesisUtxos()
	if err != nil {
		return fmt.Errorf("generate Shelley genesis UTxOs: %w", err)
	}
	reserves, err := genesisReserveBalance(
		shelleyGenesis.MaxLovelaceSupply,
		slices.Concat(byronUtxos, shelleyUtxos),
	)
	if err != nil {
		return fmt.Errorf("calculate genesis reserves: %w", err)
	}
	if state == nil {
		if err := ls.db.Metadata().SetNetworkState(
			0, reserves, 0, nil,
		); err != nil {
			return fmt.Errorf("set missing genesis network state: %w", err)
		}
		ls.config.Logger.Info(
			"initialized missing genesis network state",
			"component", "ledger",
			"treasury", 0,
			"reserves", reserves,
		)
	}
	// Both rows describe the same slot-0 baseline, but they are written by
	// different code paths and a database created before the epoch 0 ADA pots
	// row existed can carry one without the other. Seeding it is not a repair
	// of a mis-synced chain: the row is derived entirely from genesis
	// configuration, and after the 0->1 boundary has passed nothing reads it.
	if pots == nil {
		if err := ls.saveGenesisRewardAdaPots(reserves, nil); err != nil {
			return err
		}
		ls.config.Logger.Info(
			"initialized missing genesis reward ADA pots",
			"component", "ledger",
			"epoch", 0,
			"treasury", 0,
			"reserves", reserves,
		)
	}
	return nil
}

// saveGenesisRewardAdaPots writes the epoch 0 reward ADA pots row: the slot-0
// treasury/reserves baseline with an empty fee pot. It is the pot input the
// boundary into epoch 1 reads, so a network whose epoch 0 is already
// Shelley-era applies that boundary's monetary expansion and treasury tax
// instead of skipping it (dingo #3381).
func (ls *LedgerState) saveGenesisRewardAdaPots(
	reserves uint64,
	txn types.Txn,
) error {
	if err := ls.db.Metadata().SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Treasury:     0,
		Reserves:     types.Uint64(reserves),
		Fees:         0,
		CapturedSlot: 0,
	}, txn); err != nil {
		return fmt.Errorf("save genesis reward ADA pots: %w", err)
	}
	return nil
}

// buildGenesisBlockCbor creates a CBOR structure representing a synthetic
// genesis block containing transactions with outputs. The structure is:
//
//	[                                    // block: array of transactions
//	  {0: tx_hash, 1: [output, ...]},    // transaction 1
//	  {0: tx_hash, 1: [output, ...]},    // transaction 2
//	  ...
//	]
//
// It populates utxoOffsets with the byte offset of each output within the block.
// Unlike a search-based approach, this function tracks exact byte positions during
// CBOR construction to avoid any possibility of false matches.
// The blockHash parameter is the Byron genesis hash used as the synthetic block hash.
func buildGenesisBlockCbor(
	txHashes [][32]byte,
	txUtxos map[[32]byte][]lcommon.Utxo,
	utxoOffsets map[database.UtxoRef]database.CborOffset,
	blockHash [32]byte,
) ([]byte, error) {
	var buf bytes.Buffer

	// Write outer array header for transactions
	writeCborArrayHeader(&buf, len(txHashes))

	for _, txHash := range txHashes {
		utxos := txUtxos[txHash]

		// Sort outputs by index for deterministic ordering
		slices.SortFunc(utxos, func(a, b lcommon.Utxo) int {
			ai, bi := uint64(a.Id.Index()), uint64(b.Id.Index())
			if ai < bi {
				return -1
			} else if ai > bi {
				return 1
			}
			return 0
		})

		// Write map header with 2 entries: {0: txhash, 1: outputs}
		writeCborMapHeader(&buf, 2)

		// Key 0: tx hash
		writeCborUint(&buf, 0)
		writeCborBytes(&buf, txHash[:])

		// Key 1: outputs array
		writeCborUint(&buf, 1)
		writeCborArrayHeader(&buf, len(utxos))

		// Write each output, tracking offsets
		for _, utxo := range utxos {
			outputCbor := utxo.Output.Cbor()
			if len(outputCbor) == 0 {
				var err error
				outputCbor, err = cbor.Encode(utxo.Output)
				if err != nil {
					return nil, fmt.Errorf("encode output: %w", err)
				}
			}

			// Record offset BEFORE writing the output
			offset := buf.Len()
			outputLen := len(outputCbor)

			// Validate sizes fit in uint32 (fail fast instead of silent truncation)
			if offset > math.MaxUint32 {
				return nil, fmt.Errorf(
					"genesis CBOR offset %d exceeds uint32 max",
					offset,
				)
			}
			if outputLen > math.MaxUint32 {
				return nil, fmt.Errorf(
					"genesis output CBOR length %d exceeds uint32 max",
					outputLen,
				)
			}

			buf.Write(outputCbor)

			ref := database.UtxoRef{
				TxId:      txHash,
				OutputIdx: utxo.Id.Index(),
			}
			//nolint:gosec // uint32 bounds checked above
			utxoOffsets[ref] = database.CborOffset{
				BlockSlot:  0,
				BlockHash:  blockHash,
				ByteOffset: uint32(offset),
				ByteLength: uint32(outputLen),
			}
		}
	}

	return buf.Bytes(), nil
}

// writeCborArrayHeader writes a CBOR array header for n elements.
func writeCborArrayHeader(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 4, n) // Major type 4 = array
}

// writeCborMapHeader writes a CBOR map header for n pairs.
func writeCborMapHeader(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 5, n) // Major type 5 = map
}

// writeCborBytes writes a CBOR byte string.
func writeCborBytes(buf *bytes.Buffer, data []byte) {
	writeCborMajorType(buf, 2, len(data)) // Major type 2 = byte string
	buf.Write(data)
}

// writeCborUint writes a CBOR unsigned integer.
func writeCborUint(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 0, n) // Major type 0 = unsigned int
}

// writeCborMajorType writes a CBOR header with the given major type and value.
//
//nolint:gosec // Intentional byte truncation for CBOR encoding of individual octets.
func writeCborMajorType(buf *bytes.Buffer, majorType, n int) {
	header := byte(majorType << 5)
	switch {
	case n < 24:
		buf.WriteByte(header | byte(n))
	case n < 256:
		buf.WriteByte(header | 24)
		buf.WriteByte(byte(n))
	case n < 65536:
		buf.WriteByte(header | 25)
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	case n < 4294967296:
		buf.WriteByte(header | 26)
		buf.WriteByte(byte(n >> 24))
		buf.WriteByte(byte(n >> 16))
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	default:
		// 8-byte encoding for values >= 2^32
		buf.WriteByte(header | 27)
		val := uint64(n)
		buf.WriteByte(byte(val >> 56))
		buf.WriteByte(byte(val >> 48))
		buf.WriteByte(byte(val >> 40))
		buf.WriteByte(byte(val >> 32))
		buf.WriteByte(byte(val >> 24))
		buf.WriteByte(byte(val >> 16))
		buf.WriteByte(byte(val >> 8))
		buf.WriteByte(byte(val))
	}
}

// calculateEpochNonce computes the epoch nonce for epoch N+1, the
// end-of-epoch evolving nonce, and the lastEpochBlockNonce to carry
// into epoch N+1.
//
// The Ouroboros Praos formula is:
//
//	epochNonce(N+1) = candidateNonce(N) ⭒ lastEpochBlockNonce(N)
//
// where lastEpochBlockNonce(N) is the hash of the last block in epoch N.
// If epoch N has no blocks, the consensus state carries the previous
// lastEpochBlockNonce forward.
//
// The ⭒ operator has NeutralNonce as identity:
//
//	x ⭒ NeutralNonce = x
//
// For the very first epoch transition (0→1), lastEpochBlockNonce
// is nil (NeutralNonce), so epochNonce = candidateNonce.
//
// Returns (epochNonce, evolvingNonce, candidateNonce, labNonce, error).
// The caller must store candidateNonce as the new epoch's CandidateNonce
// and labNonce as the new epoch's LastEpochBlockNonce so an empty next
// epoch can carry it forward.
func (ls *LedgerState) calculateEpochNonce(
	txn *database.Txn,
	epochStartSlot uint64,
	currentEra eras.EraDesc,
	currentEpoch models.Epoch,
) ([]byte, []byte, []byte, []byte, error) {
	// No epoch nonce in Byron. NOTE: currentEra is the SOURCE era being
	// rolled over, not necessarily the era the new epoch will run at — a
	// rollover whose source era is Byron but whose destination era (per
	// the caller's later era-transition decision) is Shelley or beyond
	// still returns nil here. applyBoundaryEraTransitions seeds a real
	// nonce for that case once the destination era is known; see the
	// comment there.
	if currentEra.Id == 0 {
		return nil, nil, nil, nil, nil
	}
	if ls.config.CardanoNodeConfig == nil {
		return nil, nil, nil, nil, errors.New("CardanoNodeConfig is nil")
	}
	if ls.config.CardanoNodeConfig.ShelleyGenesisHash == "" {
		return nil, nil, nil, nil, errors.New(
			"could not get Shelley genesis hash",
		)
	}
	genesisHashBytes, err := hex.DecodeString(
		ls.config.CardanoNodeConfig.ShelleyGenesisHash,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"decode genesis hash: %w", err,
		)
	}

	// For the initial epoch creation (no blocks yet), the epoch nonce and
	// initial evolving/candidate nonces are all the genesis nonce, and the
	// carried lastEpochBlockNonce is Neutral (nil). At genesis cardano-ledger
	// initializes praosStateLastEpochBlockNonce to NeutralNonce, so the FIRST
	// from-genesis epoch boundary takes the identity branch: eta = candidate ⭒
	// NeutralNonce = candidate. Devnet confirms cardano's epoch-1 epochNonce ==
	// its candidate (identity). Do NOT seed this with the genesis nonce — that
	// combines instead of using identity and diverges at the first boundary
	// (#2734). The Mithril path never takes this branch (bootstrap epoch imports
	// a non-nil lastEpochBlockNonce).
	if len(currentEpoch.Nonce) == 0 {
		return genesisHashBytes, genesisHashBytes, genesisHashBytes, nil, nil
	}

	// In Ouroboros Praos, the evolving nonce carries across epoch
	// boundaries without resetting (PrtclState is never reset).
	// For migration compatibility (epochs stored before this
	// field existed), fall back to genesis hash.
	prevEvolvingNonce := currentEpoch.EvolvingNonce
	if len(prevEvolvingNonce) == 0 {
		prevEvolvingNonce = genesisHashBytes
	}

	// The candidate nonce also carries across epochs independently
	// of the evolving nonce. When 4k/f >= epochLength (e.g., short
	// devnet epochs), the candidate is never updated by any block
	// and stays at its carried value. Fall back to genesis hash
	// for epochs stored before this field existed.
	prevCandidateNonce := currentEpoch.CandidateNonce
	if len(prevCandidateNonce) == 0 {
		prevCandidateNonce = genesisHashBytes
	}

	// When importing from a snapshot, currentEpoch may carry tip-time
	// nonce state (evolving/candidate already advanced through the
	// imported tip slot). In that case, continue accumulation from the
	// next slot rather than replaying from epoch start.
	computeStartSlot := currentEpoch.StartSlot
	computeEpochLength := uint64(currentEpoch.LengthInSlots)
	epochEndSlot := currentEpoch.StartSlot +
		uint64(currentEpoch.LengthInSlots)
	ls.RLock()
	tipSlot := ls.currentTip.Point.Slot
	tipBlockNonceCopy := append([]byte(nil), ls.currentTipBlockNonce...)
	ls.RUnlock()
	if tipSlot >= currentEpoch.StartSlot &&
		tipSlot < epochEndSlot &&
		len(currentEpoch.CandidateNonce) == 32 &&
		len(currentEpoch.EvolvingNonce) == 32 &&
		len(tipBlockNonceCopy) == 32 &&
		bytes.Equal(currentEpoch.EvolvingNonce, tipBlockNonceCopy) {
		if nextSlot := tipSlot + 1; nextSlot < epochEndSlot {
			computeStartSlot = nextSlot
			computeEpochLength = epochEndSlot - nextSlot
		} else {
			// Tip already at/after epoch end: no additional blocks to fold.
			computeEpochLength = 0
		}
	} else if len(currentEpoch.EvolvingNonce) == 32 {
		// Resume fallback: if epoch nonce state was checkpointed at an
		// earlier slot (snapshot import), locate that anchor by matching
		// stored block nonces and continue from the following slot.
		// If no anchor is found, fall through to the defaults which
		// compute from epoch start — this is always correct (just
		// slower) and handles genesis sync where the epoch's
		// EvolvingNonce was set at creation and never updated.
		nonceRows, nonceErr := ls.db.GetBlockNoncesInSlotRange(
			currentEpoch.StartSlot,
			epochEndSlot,
			txn,
		)
		if nonceErr != nil {
			return nil, nil, nil, nil, fmt.Errorf(
				"fetch block nonces in epoch range: %w",
				nonceErr,
			)
		}
		for _, row := range nonceRows {
			if len(row.Nonce) == 32 &&
				bytes.Equal(currentEpoch.EvolvingNonce, row.Nonce) {
				if row.Slot+1 < epochEndSlot {
					computeStartSlot = row.Slot + 1
					computeEpochLength = epochEndSlot -
						computeStartSlot
				} else {
					computeEpochLength = 0
				}
				break
			}
		}
	}

	// Compute candidateNonce (frozen at stability window cutoff)
	// and evolvingNonce (after all blocks) from the remaining
	// current-epoch blocks. Each block's VRF output is accumulated
	// via the Nonce semigroup (⭒) starting from prevEvolvingNonce.
	// The stability window depends on the SOURCE epoch's era — the
	// one being closed — not the era we are transitioning into. The
	// 3k/f window covers Shelley, Allegra, Mary, Alonzo, and Babbage
	// (Babbage runs Praos but retains the smaller window for
	// backwards compatibility); 4k/f kicks in at Conway. The two
	// formulas only disagree at the Babbage→Conway boundary, but the
	// source-vs-target distinction matters for every transition: by
	// the time this code runs, applyHardForkTransition has already
	// advanced currentEra to the new era, so passing currentEra.Id
	// here would pick the wrong window for the source epoch's blocks
	// and produce an epoch nonce that diverges from peers. The
	// observed symptom at Alonzo→Babbage was every header in the new
	// Praos epoch VRF-failing (#2125); the same shape repeats at
	// Babbage→Conway when this rule is broken.
	candidateNonce, evolvingNonce, err := ls.computeCandidateNonce(
		txn,
		currentEpoch.EraId,
		prevEvolvingNonce,
		prevCandidateNonce,
		computeStartSlot,
		computeEpochLength,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"compute candidate nonce: %w", err,
		)
	}

	// The epoch nonce mixes the frozen candidate with the nonce derived from
	// the last block of the PREVIOUS epoch. In cardano-ledger this is
	// praosStateLastEpochBlockNonce: the value CARRIED in the closing epoch's
	// state (set at the prior epoch boundary) — i.e. the hash of the last block
	// of the epoch *before* the one closing here — NOT the last block of the
	// closing epoch itself. Using the closing epoch's own last block shifts the
	// lab by one epoch and diverges eta from the network, so every leader-VRF
	// check in the new epoch fails (#2734):
	//   epochNonce(N+1) = candidateNonce(N) ⭒ currentEpoch(N).LastEpochBlockNonce
	labForEta := cloneNonce(currentEpoch.LastEpochBlockNonce)

	// The carried lab for the NEXT boundary is stored on the new epoch record:
	// prevHashToNonce(lastBlock.prevHash) = the PARENT hash of the last block of
	// the epoch being closed (a one-block Praos lag), NOT the last block's own
	// hash. See epochLabNonce and #2734 (eta_1349 wedge).
	labNonceToSave, err := ls.epochLabNonce(
		txn,
		currentEpoch.StartSlot,
		epochEndSlot,
		currentEpoch.LastEpochBlockNonce,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// If nil/empty, it's NeutralNonce (identity): result is
	// just candidateNonce.
	if len(labForEta) == 0 {
		// NeutralNonce is the identity element of ⭒:
		//   candidateNonce ⭒ NeutralNonce = candidateNonce
		// So the epoch nonce is just the candidate nonce.
		ls.config.Logger.Debug(
			"epoch nonce computed (NeutralNonce, using candidateNonce)",
			"component", "ledger",
			"epoch_start_slot", epochStartSlot,
			"candidate_nonce",
			hex.EncodeToString(candidateNonce),
			"lab_nonce_to_save",
			hex.EncodeToString(labNonceToSave),
			"epoch_nonce",
			hex.EncodeToString(candidateNonce),
			"evolving_nonce",
			hex.EncodeToString(evolvingNonce),
		)
		return candidateNonce, evolvingNonce, candidateNonce, labNonceToSave, nil
	}

	// candidateNonce ⭒ labForEta
	// = blake2b_256(candidateNonce || labForEta)
	if len(candidateNonce) < 32 ||
		len(labForEta) < 32 {
		return nil, nil, nil, nil, fmt.Errorf(
			"epoch nonce requires 32-byte inputs: "+
				"candidateNonce=%d, labForEta=%d",
			len(candidateNonce),
			len(labForEta),
		)
	}
	result, err := lcommon.CalculateEpochNonce(
		candidateNonce,
		labForEta,
		nil,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"calculate epoch nonce: %w", err,
		)
	}
	ls.config.Logger.Debug(
		"epoch nonce computed",
		"component", "ledger",
		"epoch_start_slot", epochStartSlot,
		"candidate_nonce", hex.EncodeToString(candidateNonce),
		"lab_for_eta",
		hex.EncodeToString(labForEta),
		"lab_nonce_to_save",
		hex.EncodeToString(labNonceToSave),
		"epoch_nonce", hex.EncodeToString(result.Bytes()),
		"evolving_nonce", hex.EncodeToString(evolvingNonce),
	)
	return result.Bytes(), evolvingNonce, candidateNonce, labNonceToSave, nil
}

// processEpochRollover processes an epoch rollover and returns the result without
// mutating LedgerState. This allows callers to capture the computed state in a
// transaction and apply it to in-memory state after the transaction commits.
// Parameters:
//   - txn: database transaction
//   - currentEpoch: current epoch (read-only input)
//   - currentEra: current era descriptor (read-only input)
//   - currentPParams: current protocol parameters (read-only input)
//
// applyEpochDonations moves the treasury donations accumulated during the
// ended epoch into the treasury at the epoch boundary. The donation rows are
// left in place (keyed by slot) so a rollback past the boundary drops both the
// donation rows and the boundary NetworkState row, and re-applying the
// boundary re-derives the same total. It writes the updated treasury at the
// boundary slot, the same slot governance treasury-withdrawal enactment uses,
// so the row reflects withdrawals first and donations second.
func (ls *LedgerState) applyEpochDonations(
	txn *database.Txn,
	endedEpoch uint64,
	boundarySlot uint64,
) error {
	donations, err := ls.db.Metadata().SumNetworkDonationsForEpoch(
		endedEpoch, txn.Metadata(),
	)
	if err != nil {
		return fmt.Errorf(
			"sum donations for epoch %d: %w", endedEpoch, err,
		)
	}
	if donations == 0 {
		return nil
	}
	state, err := ls.db.Metadata().GetNetworkState(txn.Metadata())
	if err != nil {
		return fmt.Errorf("get network state: %w", err)
	}
	var treasury, reserves uint64
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	if err := ls.db.Metadata().SetNetworkState(
		treasury+donations,
		reserves,
		boundarySlot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("set network state with donations: %w", err)
	}
	return nil
}

// cloneProtocolParametersForEra returns an independently owned protocol-
// parameter value using the active era's CBOR decoder.
func cloneProtocolParametersForEra(
	era eras.EraDesc,
	pparams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	if pparams == nil {
		return nil, nil
	}
	// Byron does not persist or decode protocol parameters. A Shelley-shaped
	// fallback can still be present during startup/recovery, but it must not be
	// carried through a Byron rollover or treated as a Byron-owned snapshot.
	if era.Id == eras.ByronEraDesc.Id {
		return nil, nil
	}
	if era.Id == eras.ConwayEraDesc.Id {
		if _, ok := pparams.(*conway.ConwayProtocolParameters); !ok {
			return nil, fmt.Errorf(
				"conway era has protocol parameters type %T",
				pparams,
			)
		}
		ret, err := eras.CloneGovernanceProtocolParameters(pparams)
		if err != nil {
			return nil, fmt.Errorf(
				"clone governance protocol parameters: %w",
				err,
			)
		}
		return ret, nil
	}
	if era.Id == eras.DijkstraEraDesc.Id {
		if _, ok := pparams.(*dijkstra.DijkstraProtocolParameters); !ok {
			return nil, fmt.Errorf(
				"dijkstra era has protocol parameters type %T",
				pparams,
			)
		}
		ret, err := eras.CloneGovernanceProtocolParameters(pparams)
		if err != nil {
			return nil, fmt.Errorf(
				"clone governance protocol parameters: %w",
				err,
			)
		}
		return ret, nil
	}
	if era.DecodePParamsFunc == nil {
		return nil, fmt.Errorf(
			"era %d has no protocol parameter decoder",
			era.Id,
		)
	}
	data, err := cbor.Encode(pparams)
	if err != nil {
		return nil, fmt.Errorf("encode protocol parameters: %w", err)
	}
	ret, err := era.DecodePParamsFunc(data)
	if err != nil {
		return nil, fmt.Errorf("decode protocol parameters: %w", err)
	}
	return ret, nil
}

// processEpochRollover returns all computed state without applying it. The
// caller is responsible for:
//   - Applying the result to in-memory state after successful commit
//   - Starting background cleanup goroutines
//   - Calling Scheduler.ChangeInterval if SchedulerIntervalMs > 0
//
// deferBoundarySnapshot suppresses the authoritative mark-snapshot capture so
// the caller can take it after applying era transitions that this rollover does
// not perform itself. Only the multi-era boundary path sets it: a boundary block
// encoded in the era before the era its header announces needs the rollover to
// enact source-era pparam updates first, so the final era and protocol
// parameters do not exist yet when the capture would normally run. Capturing
// then would durably record the source era's protocol major for an epoch that
// runs at the successor era's, and disagree with the post-commit
// EpochTransitionEvent. When set and the rollover reaches the capture point, the
// result reports BoundarySnapshotDeferred so exactly one capture is taken —
// re-running the capture instead would double-write under the savepoint.
func (ls *LedgerState) processEpochRollover(
	txn *database.Txn,
	currentEpoch models.Epoch,
	currentEra eras.EraDesc,
	currentPParams lcommon.ProtocolParameters,
	deferBoundarySnapshot bool,
) (*EpochRolloverResult, error) {
	// Fail closed at the top of the production rollover path rather than
	// letting a nil config reach one of the several unchecked
	// ls.config.CardanoNodeConfig dereferences below (e.g. the
	// ShelleyGenesis() read further down, or applyBoundaryEraTransitions's
	// post-Byron nonce seeding) -- any of which would panic instead of
	// returning an error. NewLedgerState does not itself require a non-nil
	// CardanoNodeConfig, so this is the boundary that must catch it.
	if ls.config.CardanoNodeConfig == nil {
		return nil, errors.New(
			"process epoch rollover: CardanoNodeConfig is nil",
		)
	}
	epochStartSlot := currentEpoch.StartSlot + uint64(
		currentEpoch.LengthInSlots,
	)
	// Era update functions mutate their concrete protocol-parameter pointer.
	// Give the transaction an independently owned value so a failed or
	// in-flight rollover cannot modify parameters retained by an older snapshot.
	ownedPParams, err := cloneProtocolParametersForEra(
		currentEra,
		currentPParams,
	)
	if err != nil {
		return nil, fmt.Errorf("clone current protocol parameters: %w", err)
	}
	result := &EpochRolloverResult{
		CheckpointWrittenForEpoch: false,
		NewCurrentEra:             currentEra,
		NewCurrentPParams:         ownedPParams,
	}

	// Create initial epoch
	if currentEpoch.SlotLength == 0 {
		// Create initial epoch record
		epochSlotLength, epochLength, err := currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return nil, fmt.Errorf("calculate epoch length: %w", err)
		}
		tmpNonce, tmpEvolvingNonce, tmpCandidateNonce, tmpLabNonce, err := ls.calculateEpochNonce(
			txn,
			0,
			currentEra,
			currentEpoch,
		)
		if err != nil {
			return nil, fmt.Errorf("calculate epoch nonce: %w", err)
		}
		err = ls.db.SetEpoch(
			epochStartSlot,
			0, // epoch
			tmpNonce,
			tmpEvolvingNonce,
			tmpCandidateNonce,
			tmpLabNonce,
			currentEra.Id,
			epochSlotLength,
			epochLength,
			txn,
		)
		if err != nil {
			return nil, fmt.Errorf("set epoch: %w", err)
		}
		// Load epoch info from DB to populate result
		epochs, err := ls.db.GetEpochs(txn)
		if err != nil {
			return nil, fmt.Errorf("load epochs: %w", err)
		}
		result.NewEpochCache = epochs
		if len(epochs) > 0 {
			result.NewCurrentEpoch = epochs[len(epochs)-1]
			eraDesc, ok := ls.eraById(result.NewCurrentEpoch.EraId)
			if !ok || eraDesc == nil {
				return nil, fmt.Errorf(
					"unknown era ID %d",
					result.NewCurrentEpoch.EraId,
				)
			}
			result.NewCurrentEra = *eraDesc
			result.NewEpochNum = float64(result.NewCurrentEpoch.EpochId)
		}
		ls.config.Logger.Debug(
			"added initial epoch to DB",
			"epoch", fmt.Sprintf("%+v", result.NewCurrentEpoch),
			"component", "ledger",
		)
		return result, nil
	}
	// EPOCH→HARDFORK ordering invariant.
	//
	// The reward prefix mirrors cardano-ledger's NEWEPOCH sequence: the delayed
	// reward update is applied first so reward-driven reserves/treasury movement
	// is visible to governance withdrawals and to the end-of-boundary ADA-pot
	// capture. The remainder mirrors cardano-ledger's Conway/Rules/Epoch.hs:
	// 374-379 (EPOCH STS), which dispatches HARDFORK only after enactment +
	// pparams write. The relative order matters because the HARDFORK rule branch
	// is selected from the new pparams' major version — a HARDFORK rule that ran
	// before enactment would observe stale pparams and pick the wrong branch.
	//
	// The order, asserted by TestProcessEpochRollover_OrderingInvariant,
	// TestProcessEpochRollover_RewardOrdering and
	// TestProcessEpochRollover_SnapStakeReadOrdering in
	// chainsync_ordering_test.go and chainsync_snap_ordering_test.go, is:
	//
	//   1. applyStakeRewards             — apply the delayed reward update
	//      (rewards from the snapshot three epochs back): credit spendable
	//      rewards and move undistributed→reserves, unspendable→treasury
	//      before governance reads the treasury. Reference: applyRUpd, the
	//      first step of NEWEPOCH.
	//   2. applyMIRCerts                 — Shelley-era INSTANT rule: apply
	//      Move Instantaneous Rewards certificates accumulated during the
	//      ended epoch. No-op for Conway+ epochs (no MIR certs exist).
	//      Reference: the MIR rule, which Shelley's NEWEPOCH embeds between
	//      applyRUpd and EPOCH — so before SNAP, and before POOLREAP.
	//   3. captureEpochBoundarySnapshotStake — SNAP: read the mark snapshot's
	//      stake. Reference: the first sub-rule of EPOCH.
	//   4. ComputeAndApplyPParamUpdates  — Shelley-style ppuProtocolVersion
	//      voting path; produces newPParams from on-chain pparam-update
	//      proposals.
	//   5. applyPoolRetirements          — embedded Shelley POOLREAP: refund
	//      deposits of pools whose retirement epoch is the new epoch. Runs
	//      after SNAP and before enactment, so its deposits are outside the
	//      mark snapshot but any deposit landing in the treasury is visible
	//      to the treasury withdrawals checked in governance.ProcessEpoch.
	//   6. activateDelegatorInactivityIfNeeded — one-time CIP-0163 activation
	//      before any inactivity-gated boundary calculation.
	//   7. governance.ProcessEpoch       — Conway-style HardForkInitiation /
	//      ParameterChange enactment; may further mutate pparams.
	//   8. SetPParams                    — persist the enacted pparams.
	//   9. IsHardForkTransition check    — detect inter-era boundary from
	//      the now-final pparams.
	//  10. applyIntraEraHardForkRule     — dispatch the per-major-version
	//      HARDFORK STS rule (e.g. pv3 AVVM removal, pv10 DRep clear).
	//  11. saveRewardAdaPotsForEpoch     — capture the new epoch's ADA pots
	//      (reserves/treasury/fees) after all boundary pot mutations so the
	//      next delayed reward calculation has its pot inputs.
	//
	// The mark snapshot ROW is written separately at the end of the rollover
	// (captureEpochBoundarySnapshot), after the new epoch record and its nonce
	// exist; step 3 is only the stake read. Splitting them is what lets the read
	// sit at the reference SNAP point while the write still sees the new epoch.
	//
	// Steps 7 and 8 must observe the post-enactment major version. Step 9 must
	// observe the persisted pparams (not just the in-memory ones) because its
	// body issues SQL within `txn` that may join against `pparams` rows.
	if err := ls.applyStakeRewards(
		txn, currentEpoch.EpochId+1, epochStartSlot,
	); err != nil {
		return nil, fmt.Errorf("apply stake rewards: %w", err)
	}

	// Apply the Shelley-era INSTANT rule: credit MIR certificate rewards
	// accumulated during the ended epoch to registered reward accounts, and
	// apply pot-to-pot transfers between treasury and reserves. This is a
	// no-op for Conway+ epochs because MIR certs are not valid there and no
	// DB rows exist for those slots.
	//
	// Shelley's NEWEPOCH rule embeds MIR between applyRUpd and EPOCH, so MIR
	// precedes both SNAP and POOLREAP: its credits are part of the mark snapshot
	// and its pot movements are visible to POOLREAP, governance and the ADA-pot
	// capture below.
	if err := ls.applyMIRCerts(
		txn, currentEpoch.StartSlot, epochStartSlot,
	); err != nil {
		return nil, fmt.Errorf("apply MIR certs: %w", err)
	}

	// SNAP read point. Everything below this line is a rule cardano-ledger runs
	// after SNAP, and several of them credit reward accounts at epochStartSlot
	// (POOLREAP deposit refunds, enacted treasury withdrawals,
	// proposal-deposit refunds). The mark snapshot's stake is therefore read
	// here — after the delayed reward update and MIR, which precede SNAP, and
	// before any of them — while the snapshot row is written at the end of the
	// rollover where the new epoch record and the post-enactment protocol
	// version exist.
	if err := ls.captureEpochBoundarySnapshotStake(
		txn, currentEpoch, epochStartSlot,
	); err != nil {
		return nil, err
	}

	updateQuorum := 0
	if shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis(); shelleyGenesis != nil {
		updateQuorum = shelleyGenesis.UpdateQuorum
	}
	newPParams, plutusV2CostModelWritten, err := ls.db.ComputeAndApplyPParamUpdates(
		epochStartSlot,
		currentEpoch.EpochId+1, // Target epoch for updates
		currentEra.Id,
		updateQuorum,
		ownedPParams,
		currentEra.DecodePParamsUpdateFunc,
		currentEra.PParamsUpdateFunc,
		currentEra.ParamUpdateHasPlutusV2CostModelFunc,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("apply pparam updates: %w", err)
	}
	if plutusV2CostModelWritten {
		// The classic Shelley-style update system, not CIP-1694 governance,
		// carried a real PlutusV2 cost model this epoch -- the same real-data
		// confirmation the governance-enactment branch below records, just
		// from the pre-Conway path (this is how real mainnet actually
		// received its PlutusV2 cost model, well before Conway governance
		// existed). See blinklabs-io/dingo#3825's PR review.
		if err := ls.markRealV2CostModelObserved(
			currentEpoch.EpochId+1, txn,
		); err != nil {
			return nil, err
		}
		result.RealV2CostModelObserved = true
	}

	// Apply the embedded Shelley POOLREAP transition: refund the deposits of
	// pools whose retirement epoch is the new epoch. The EPOCH rule runs it
	// after SNAP, so these deposits are deliberately outside the mark snapshot
	// read above, and before governance enactment and treasury accounting, so
	// any deposit that lands in the treasury (unregistered/inactive reward
	// account) is visible to the withdrawals checked in
	// governance.ProcessEpoch below.
	if err := ls.applyPoolRetirements(
		txn, currentEpoch.EpochId+1, epochStartSlot,
	); err != nil {
		return nil, fmt.Errorf("apply pool retirements: %w", err)
	}

	// CIP-0163: one-time activation stamp. It must precede governance's
	// inactivity-gated DRep voting-power calculation so every active account
	// receives the same full window starting at the activation boundary. The
	// new epoch row is persisted later in this transaction; the stamp and
	// durable marker still commit or roll back atomically with it.
	if err := ls.activateDelegatorInactivityIfNeeded(
		txn, currentEpoch.EpochId+1,
	); err != nil {
		return nil, fmt.Errorf("activate delegator inactivity: %w", err)
	}

	// Run the CIP-1694 governance tick: enact proposals ratified in the
	// previous epoch (possibly mutating pparams), expire stale proposals,
	// and ratify active proposals whose tallies meet threshold. Any
	// pparams change from enactment is persisted via SetPParams so the
	// next epoch's pparams reflect the enacted state.
	var conwayGenesis *conway.ConwayGenesis
	if ls.config.CardanoNodeConfig != nil {
		conwayGenesis = ls.config.CardanoNodeConfig.ConwayGenesis()
	}
	govOut, err := governance.ProcessEpoch(&governance.EpochInput{
		DB:                    ls.db,
		Txn:                   txn,
		Logger:                ls.config.Logger,
		PrevEpoch:             currentEpoch.EpochId,
		NewEpoch:              currentEpoch.EpochId + 1,
		BoundarySlot:          epochStartSlot,
		PParams:               newPParams,
		UpdateFn:              currentEra.PParamsUpdateFunc,
		ConwayGenesis:         conwayGenesis,
		DelegatorInactivityOn: ls.config.DelegatorInactivityEnabled,
	})
	if err != nil {
		return nil, fmt.Errorf("process governance epoch: %w", err)
	}
	// Move the ending epoch's accumulated treasury donations into the
	// treasury. Per the Conway EPOCH rule, donations are added after enacted
	// treasury withdrawals (handled in governance.ProcessEpoch above), so a
	// withdrawal is checked against the pre-donation treasury and the donation
	// is reflected for subsequent epochs' accounting.
	if err := ls.applyEpochDonations(
		txn, currentEpoch.EpochId, epochStartSlot,
	); err != nil {
		return nil, fmt.Errorf("apply epoch donations: %w", err)
	}
	if govOut.PParamsChanged {
		newPParams = govOut.UpdatedPParams
		pparamsCbor, encErr := cbor.Encode(&newPParams)
		if encErr != nil {
			return nil, fmt.Errorf(
				"encode post-enactment pparams: %w", encErr,
			)
		}
		if err := ls.db.SetPParams(
			pparamsCbor,
			epochStartSlot,
			currentEpoch.EpochId+1,
			currentEra.Id,
			txn,
		); err != nil {
			return nil, fmt.Errorf(
				"persist post-enactment pparams: %w", err,
			)
		}
		if govOut.PlutusV2CostModelWritten {
			if err := ls.markRealV2CostModelObserved(
				currentEpoch.EpochId+1, txn,
			); err != nil {
				return nil, err
			}
			result.RealV2CostModelObserved = true
		}
	}
	result.NewCurrentPParams = newPParams

	// Check if the protocol version changed in a way that
	// triggers a hard fork (era transition)
	oldVer, oldErr := GetProtocolVersion(currentPParams)
	newVer, newErr := GetProtocolVersion(newPParams)
	// Only warn when parameters are present but yield no version. Byron holds
	// nil parameters by design, so a nil value is the expected shape for every
	// rollover in the prefix rather than a fault: warning on it emitted both
	// lines below on each one, 416 of them on a mainnet sync from genesis. A
	// non-nil value that still yields no version is a real anomaly and keeps
	// its warning. Hard-fork detection is skipped either way by the error
	// check below, so nothing here changes which rollovers are examined.
	if oldErr != nil && currentPParams != nil {
		ls.config.Logger.Warn(
			"could not extract protocol version from "+
				"current pparams, skipping hard fork "+
				"detection",
			"error", oldErr,
			"pparams_type",
			fmt.Sprintf("%T", currentPParams),
			"component", "ledger",
		)
	}
	if newErr != nil && newPParams != nil {
		ls.config.Logger.Warn(
			"could not extract protocol version from "+
				"new pparams, skipping hard fork "+
				"detection",
			"error", newErr,
			"pparams_type",
			fmt.Sprintf("%T", newPParams),
			"component", "ledger",
		)
	}
	if oldErr == nil && newErr == nil {
		if ls.isHardForkTransition(oldVer, newVer) {
			fromEra, _ := ls.eraForVersion(oldVer.Major)
			toEra, _ := ls.eraForVersion(newVer.Major)
			result.HardFork = &HardForkInfo{
				OldVersion: oldVer,
				NewVersion: newVer,
				FromEra:    fromEra,
				ToEra:      toEra,
			}
			ls.config.Logger.Info(
				"hard fork detected via protocol "+
					"parameter update",
				"from_era", fromEra,
				"to_era", toEra,
				"old_version",
				fmt.Sprintf(
					"%d.%d",
					oldVer.Major,
					oldVer.Minor,
				),
				"new_version",
				fmt.Sprintf(
					"%d.%d",
					newVer.Major,
					newVer.Minor,
				),
				"epoch",
				currentEpoch.EpochId+1,
				"component", "ledger",
			)
		}
		// Apply cardano-ledger's per-major-version HARDFORK rule. This
		// runs on ANY major-version bump, including intra-era ones like
		// Conway pv9→pv10 (Plomin, mainnet January 2025) that do not
		// trigger an era change, and inter-era ones like Shelley→Allegra
		// (pv2→pv3) that carry a state rewrite. See cardano-ledger
		// Conway/Rules/HardFork.hs and Allegra/Translation.hs.
		if oldVer.Major != newVer.Major {
			if err := ls.applyIntraEraHardForkRule(
				txn, newVer.Major, epochStartSlot, currentEpoch.EpochId+1,
			); err != nil {
				return nil, fmt.Errorf("apply major-version HARDFORK: %w", err)
			}
		}
	}

	// Capture the new epoch's ADA pots (reserves/treasury/fees) after every
	// boundary treasury/reserves mutation above (stake rewards, POOLREAP,
	// governance withdrawals, donations, and any AVVM-removal reserves top-up).
	// This row seeds the delayed reward calculation for a later epoch, so it
	// must observe the fully settled pots for the ended epoch.
	if err := ls.saveRewardAdaPotsForEpoch(
		txn,
		currentEpoch.EpochId+1,
		currentEpoch,
		epochStartSlot,
	); err != nil {
		return nil, fmt.Errorf("save reward ADA pots: %w", err)
	}

	// Create next epoch record
	epochSlotLength, epochLength, err := currentEra.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate epoch length: %w", err)
	}
	tmpNonce, tmpEvolvingNonce, tmpCandidateNonce, tmpLabNonce, err := ls.calculateEpochNonce(
		txn,
		epochStartSlot,
		currentEra,
		currentEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate epoch nonce: %w", err)
	}
	err = ls.db.SetEpoch(
		epochStartSlot,
		currentEpoch.EpochId+1,
		tmpNonce,
		tmpEvolvingNonce,
		tmpCandidateNonce,
		tmpLabNonce,
		currentEra.Id,
		epochSlotLength,
		epochLength,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("set epoch: %w", err)
	}
	// Load epoch info from DB to populate result
	epochs, err := ls.db.GetEpochs(txn)
	if err != nil {
		return nil, fmt.Errorf("load epochs: %w", err)
	}
	result.NewEpochCache = epochs
	if len(epochs) > 0 {
		result.NewCurrentEpoch = epochs[len(epochs)-1]
		eraDesc, ok := ls.eraById(result.NewCurrentEpoch.EraId)
		if !ok || eraDesc == nil {
			return nil, fmt.Errorf(
				"unknown era ID %d",
				result.NewCurrentEpoch.EraId,
			)
		}
		result.NewCurrentEra = *eraDesc
		result.NewEpochNum = float64(result.NewCurrentEpoch.EpochId)
		result.SchedulerIntervalMs = result.NewCurrentEpoch.SlotLength
	}

	ls.config.Logger.Debug(
		"added next epoch to DB",
		"epoch", fmt.Sprintf("%+v", result.NewCurrentEpoch),
		"component", "ledger",
	)

	// SNAP point: capture the authoritative mark snapshot inside this rollover
	// transaction, now that the new epoch record (and its nonce/boundary slot)
	// exist. Runs only for the normal N->N+1 rollover; epoch 0 is seeded by
	// CaptureGenesisSnapshot at startup. A multi-era boundary defers the capture
	// to the caller, which takes it once the remaining era transitions have
	// produced the era and protocol parameters the new epoch actually runs at.
	if deferBoundarySnapshot {
		result.BoundarySnapshotDeferred = true
	} else if err := ls.captureEpochBoundarySnapshot(
		txn, currentEpoch, result,
	); err != nil {
		return nil, err
	}

	return result, nil
}

// splitEraTransitionsForRollover keeps epoch-boundary protocol-parameter
// enactment in the source era. A proposal submitted in the source era is
// enacted at the boundary before the successor-era hard fork transforms the
// protocol parameters. This matters for fields removed by the successor era,
// such as Alonzo's decentralization parameter, which remains present in the
// legacy update CBOR but is not a valid Babbage update field.
func splitEraTransitionsForRollover(
	transitionPath []uint,
) (before, after []uint) {
	return nil, transitionPath
}

// epochBoundarySnapshotSlot is the slot a mark snapshot describes: the last slot
// of the ended epoch, i.e. one before the boundary.
func epochBoundarySnapshotSlot(boundarySlot uint64) uint64 {
	if boundarySlot == 0 {
		return 0
	}
	return boundarySlot - 1
}

// captureEpochBoundarySnapshotStake invokes the optional SNAP-point stake hook
// at the correct place in the boundary sequence: after the two boundary rules
// cardano-ledger applies before SNAP (the delayed reward update and MIR) and
// before POOLREAP and governance enactment, which credit reward accounts at the
// boundary slot after it.
//
// It only reads, so it needs nothing from the not-yet-written new epoch record;
// the boundary identity is fully determined here (the new epoch is
// prevEpoch.EpochId+1 starting at boundarySlot) and matches the event the
// persist half builds from that record.
//
// A failure is isolated with a savepoint so it cannot poison the rollover
// transaction on backends that abort a transaction on SQL error. The persist
// half then uses the boundary-aware historical reconstruction; load mode also
// records the failure so an incomplete capture is surfaced to the operator.
func (ls *LedgerState) captureEpochBoundarySnapshotStake(
	txn *database.Txn,
	prevEpoch models.Epoch,
	boundarySlot uint64,
) error {
	hook := ls.epochBoundarySnapshotStakeHook()
	if hook == nil {
		return nil
	}
	evt := event.EpochTransitionEvent{
		PreviousEpoch: prevEpoch.EpochId,
		NewEpoch:      prevEpoch.EpochId + 1,
		BoundarySlot:  boundarySlot,
		SnapshotSlot:  epochBoundarySnapshotSlot(boundarySlot),
	}
	const savepoint = "epoch_boundary_snapshot_stake"
	if err := txn.SavePoint(savepoint); err != nil {
		ls.config.Logger.Warn(
			"snap-point stake savepoint unavailable; deferring stake read to snapshot persist",
			"error",
			err,
			"epoch",
			evt.NewEpoch,
			"component",
			"ledger",
		)
		return nil
	}
	if err := hook(txn, evt); err != nil {
		if rbErr := txn.RollbackTo(savepoint); rbErr != nil {
			ls.config.Logger.Error(
				"failed to roll back snap-point stake savepoint",
				"error", rbErr,
				"read_error", err,
				"epoch", evt.NewEpoch,
				"component", "ledger",
			)
			return fmt.Errorf(
				"roll back snap-point stake savepoint (read error: %w): %w",
				err, rbErr,
			)
		}
		ls.config.Logger.Warn(
			"snap-point stake read failed; deferring stake read to snapshot persist",
			"error",
			err,
			"epoch",
			evt.NewEpoch,
			"component",
			"ledger",
		)
	}
	return nil
}

// captureEpochBoundarySnapshot invokes the optional authoritative snapshot hook
// inside the epoch-rollover write transaction so the mark snapshot commits
// atomically with the epoch it describes (and the event-driven fallback then
// skips it). The capture is wrapped in a metadata savepoint: a capture failure
// rolls back only the snapshot's own writes and lets the rollover proceed,
// deferring to the fallback rather than wedging the epoch boundary. The capture
// writes only metadata, so a metadata savepoint fully covers it.
func (ls *LedgerState) captureEpochBoundarySnapshot(
	txn *database.Txn,
	prevEpoch models.Epoch,
	result *EpochRolloverResult,
) error {
	hook := ls.epochBoundarySnapshotHook()
	if hook == nil {
		return nil
	}
	newEpoch := result.NewCurrentEpoch
	snapshotSlot := epochBoundarySnapshotSlot(newEpoch.StartSlot)
	evt := event.EpochTransitionEvent{
		PreviousEpoch: prevEpoch.EpochId,
		NewEpoch:      newEpoch.EpochId,
		BoundarySlot:  newEpoch.StartSlot,
		EpochNonce:    newEpoch.Nonce,
		ProtocolVersion: ls.protocolMajorForEvent(
			result.NewCurrentPParams, result.NewCurrentEra,
		),
		SnapshotSlot: snapshotSlot,
	}
	const savepoint = "epoch_boundary_snapshot"
	if err := txn.SavePoint(savepoint); err != nil {
		ls.config.Logger.Warn(
			"epoch-boundary snapshot savepoint unavailable; deferring to fallback capture",
			"error",
			err,
			"epoch",
			newEpoch.EpochId,
			"component",
			"ledger",
		)
		return nil
	}
	if err := hook(txn, evt); err != nil {
		if rbErr := txn.RollbackTo(savepoint); rbErr != nil {
			return fmt.Errorf(
				"roll back epoch-boundary snapshot savepoint (capture error: %w): %w",
				err,
				rbErr,
			)
		}
		ls.config.Logger.Warn(
			"authoritative epoch-boundary snapshot capture failed; deferring to fallback capture",
			"error",
			err,
			"epoch",
			newEpoch.EpochId,
			"component",
			"ledger",
		)
	}
	return nil
}

func (ls *LedgerState) cleanupBlockNoncesBefore(startSlot uint64) {
	if startSlot == 0 {
		return
	}
	ls.config.Logger.Debug(
		fmt.Sprintf(
			"cleaning up non-checkpoint block nonces before slot %d",
			startSlot,
		),
		"component",
		"ledger",
	)
	ls.Lock()
	defer ls.Unlock()
	txn := ls.db.Transaction(true)
	if err := txn.Do(func(txn *database.Txn) error {
		return ls.db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(startSlot, txn)
	}); err != nil {
		ls.config.Logger.Error(
			fmt.Sprintf("failed to clean up old block nonces: %s", err),
			"component", "ledger",
		)
	}
}

// checkSlotBattle checks whether an incoming block from a peer
// occupies a slot for which the local node has already forged a
// block. If so, it emits a SlotBattleEvent and logs a warning.
//
// The addBlockErr parameter is the error (if any) returned by
// chain.AddBlock for the incoming block. A nil error means the
// remote block was accepted onto the chain (remote won); a
// non-nil error means it was rejected (local won).
//
// The caller must hold ls.Lock() (write lock). This method must not
// acquire ls.RLock(), because sync.RWMutex is not reentrant and
// attempting a read lock while holding the write lock deadlocks.
func (ls *LedgerState) checkSlotBattle(
	e BlockfetchEvent,
	addBlockErr error,
) {
	checker := ls.loadForgedBlockChecker()
	if checker == nil {
		return
	}

	incomingSlot := e.Point.Slot
	localHash, forged := checker.WasForgedByUs(incomingSlot)
	if !forged {
		return
	}

	remoteHash := e.Point.Hash

	// Same hash means same block -- not a battle
	if bytes.Equal(localHash, remoteHash) {
		return
	}

	// Determine winner: if the remote block was rejected (addBlockErr
	// != nil), our local block remains on chain, so we won.
	localWon := addBlockErr != nil

	ls.config.Logger.Warn(
		"slot battle detected",
		"component", "ledger",
		"slot", incomingSlot,
		"local_block_hash", hex.EncodeToString(localHash),
		"remote_block_hash", hex.EncodeToString(remoteHash),
		"local_won", localWon,
	)

	// Increment slot battle metric
	if recorder := ls.loadSlotBattleRecorder(); recorder != nil {
		recorder.RecordSlotBattle()
	}

	if ls.config.EventBus != nil {
		ls.config.EventBus.PublishAsync(
			forging.SlotBattleEventType,
			event.NewEvent(
				forging.SlotBattleEventType,
				forging.SlotBattleEvent{
					Slot:            incomingSlot,
					LocalBlockHash:  localHash,
					RemoteBlockHash: remoteHash,
					Won:             localWon,
				},
			),
		)
	}
}

// selectInitialBlockfetchConn starts blockfetch on the same connection that
// delivered the header. This keeps header and block ingress aligned and leaves
// room for future selection logic if a different connection becomes preferable.
func (ls *LedgerState) selectInitialBlockfetchConn(
	headerConnId ouroboros.ConnectionId,
) ouroboros.ConnectionId {
	return headerConnId
}

func (ls *LedgerState) selectRetryBlockfetchConn(
	currentConnId ouroboros.ConnectionId,
) ouroboros.ConnectionId {
	if ls.config.GetActiveConnectionFunc != nil {
		if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil {
			return *activeConnId
		}
	}
	return currentConnId
}

// armBlockfetchTimeoutLocked arms the timeout before the request leaves the
// blockfetch mutex. A peer can send StartBatch and blocks immediately, so the
// timer must exist before the external request is allowed to run.
//
// The caller must hold chainsyncBlockfetchMutex.
func (ls *LedgerState) armBlockfetchTimeoutLocked(
	connId ouroboros.ConnectionId,
) {
	// Stop any existing timer before creating a new one
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}

	// Increment generation counter to invalidate any pending timer callbacks
	ls.chainsyncBlockfetchTimerGeneration++
	currentGeneration := ls.chainsyncBlockfetchTimerGeneration

	// Start timeout timer for blockfetch operation
	// The timer fires if no blocks are received within blockfetchBusyTimeout
	// Each received block resets the timer in handleEventBlockfetchBlock
	ls.chainsyncBlockfetchTimeoutTimer = time.AfterFunc(
		blockfetchBusyTimeout,
		func() {
			var pending pendingPublishes
			defer pending.flush()
			ls.chainsyncBlockfetchMutex.Lock()
			defer ls.chainsyncBlockfetchMutex.Unlock()
			// Check if this timer callback is stale (a newer timer was started)
			if ls.chainsyncBlockfetchTimerGeneration != currentGeneration {
				return
			}
			ls.handleBlockfetchTimeoutLocked(connId, &pending)
		},
	)
}

// waitForBlockfetchRequestLocked drains a previous request before allowing
// the connection to be reused. Blockfetch callbacks identify only their
// connection, so accepting a late block or BatchDone after the same peer has
// been assigned to a new batch would apply the old response to new state.
//
// The caller owns chainsyncBlockfetchMutex. This function returns with it
// locked, including after the bounded wait expires.
func (ls *LedgerState) waitForBlockfetchRequestLockedWithSignal(
	connId ouroboros.ConnectionId,
	waitStarted chan<- struct{},
) error {
	key := connIdKey(connId)
	if key == "" {
		return nil
	}
	var shutdownDone <-chan struct{}
	if ls.ctx != nil {
		if err := ls.ctx.Err(); err != nil {
			return fmt.Errorf("blockfetch request canceled: %w", err)
		}
		shutdownDone = ls.ctx.Done()
	}
	for {
		if ls.ctx != nil {
			if err := ls.ctx.Err(); err != nil {
				return fmt.Errorf("blockfetch request canceled: %w", err)
			}
		}
		requestDone, ok := ls.blockfetchRequestsInFlight[key]
		if !ok {
			return nil
		}
		ls.chainsyncBlockfetchMutex.Unlock()
		if waitStarted != nil {
			close(waitStarted)
			waitStarted = nil
		}
		timer := time.NewTimer(blockfetchBusyTimeout)
		select {
		case <-requestDone:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-shutdownDone:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			ls.chainsyncBlockfetchMutex.Lock()
			return fmt.Errorf("blockfetch request canceled: %w", ls.ctx.Err())
		case <-timer.C:
			ls.chainsyncBlockfetchMutex.Lock()
			return fmt.Errorf(
				"blockfetch connection %s remains busy after %s",
				connId.String(),
				blockfetchBusyTimeout,
			)
		}
		ls.chainsyncBlockfetchMutex.Lock()
	}
}

// beginBlockfetchRequestLocked records a request whose callbacks are still
// allowed to arrive. The caller owns chainsyncBlockfetchMutex.
func (ls *LedgerState) beginBlockfetchRequestLocked(
	connId ouroboros.ConnectionId,
) chan struct{} {
	if ls.blockfetchRequestsInFlight == nil {
		ls.blockfetchRequestsInFlight = make(map[string]chan struct{})
	}
	done := make(chan struct{})
	ls.blockfetchRequestsInFlight[connIdKey(connId)] = done
	return done
}

// endBlockfetchRequestLocked releases a request's connection for reuse. The
// caller owns chainsyncBlockfetchMutex.
func (ls *LedgerState) endBlockfetchRequestLocked(
	connId ouroboros.ConnectionId,
	done chan struct{},
) {
	key := connIdKey(connId)
	if current, ok := ls.blockfetchRequestsInFlight[key]; ok &&
		current == done {
		delete(ls.blockfetchRequestsInFlight, key)
		close(done)
	}
}

func (ls *LedgerState) resetBlockfetchInFlightTimeoutsLocked() {
	ls.blockfetchInFlightTimeoutGeneration = 0
	ls.blockfetchInFlightTimeoutCount = 0
}

// blockfetchRequestRangeStart starts the external request. It must be called
// without chainsyncBlockfetchMutex held; the blockfetch protocol may wait for
// a previous request whose receive callback needs that mutex to publish its
// final block.
func (ls *LedgerState) blockfetchRequestRangeStart(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	if ls.config.BlockfetchRequestRangeFunc == nil {
		return errors.New("blockfetch request range func not configured")
	}
	err := ls.config.BlockfetchRequestRangeFunc(
		connId,
		start,
		end,
	)
	if err != nil {
		return fmt.Errorf("request block range: %w", err)
	}
	return nil
}

func (ls *LedgerState) blockfetchRequestRangeCleanup() {
	// Stop the timeout timer if running and invalidate any pending callbacks
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}
	// Increment generation to ensure any pending timer callbacks are ignored
	ls.chainsyncBlockfetchTimerGeneration++
	// Clear shadow blockfetch state for the completed batch
	ls.shadowBlockfetchConnId = ouroboros.ConnectionId{}
	ls.shadowBlockReceivedHashes = nil
	ls.firstBlockReceived = false
	// Close our blockfetch done signal channel
	ls.chainsyncBlockfetchReadyMutex.Lock()
	defer ls.chainsyncBlockfetchReadyMutex.Unlock()
	if ls.chainsyncBlockfetchReadyChan != nil {
		close(ls.chainsyncBlockfetchReadyChan)
		ls.chainsyncBlockfetchReadyChan = nil
	}
	ls.pendingBlockfetchEvents = ls.pendingBlockfetchEvents[:0]
}

func (ls *LedgerState) handleBlockfetchTimeoutLocked(
	currentConnId ouroboros.ConnectionId,
	pending *pendingPublishes,
) {
	if ls.blockfetchPrimaryRequestGeneration != 0 {
		// The protocol request is still blocked outside the ledger mutex. Do
		// not issue a duplicate range request while it is in flight; the
		// original request may still deliver a valid response. Keep the
		// watchdog alive so a later timeout can reassess the same request.
		if ls.blockfetchInFlightTimeoutGeneration !=
			ls.blockfetchPrimaryRequestGeneration {
			ls.blockfetchInFlightTimeoutGeneration = ls.blockfetchPrimaryRequestGeneration
			ls.blockfetchInFlightTimeoutCount = 0
		}
		if ls.blockfetchInFlightTimeoutCount <
			blockfetchInFlightTimeoutWarnThreshold {
			ls.blockfetchInFlightTimeoutCount++
		}
		logFn := ls.config.Logger.Debug
		if ls.blockfetchInFlightTimeoutCount >=
			blockfetchInFlightTimeoutWarnThreshold {
			logFn = ls.config.Logger.Warn
		}
		logFn(
			"blockfetch request still in flight at timeout; waiting for it to return",
			"component",
			"ledger",
			"connection_id",
			currentConnId.String(),
			"request_generation",
			ls.blockfetchPrimaryRequestGeneration,
			"in_flight_timeout_count",
			ls.blockfetchInFlightTimeoutCount,
			"in_flight_elapsed",
			time.Since(ls.activeBlockfetchStart),
		)
		ls.armBlockfetchTimeoutLocked(currentConnId)
		return
	}
	headerCount := ls.chain.HeaderCount()
	if headerCount == 0 {
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		ls.config.Logger.Info(
			fmt.Sprintf(
				"blockfetch operation timed out after %s",
				blockfetchBusyTimeout,
			),
			"component",
			"ledger",
			"connection_id",
			currentConnId.String(),
		)
		return
	}

	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	retryConnId := ls.selectRetryBlockfetchConn(currentConnId)
	ls.blockfetchRequestRangeCleanup()
	ls.config.Logger.Warn(
		"blockfetch operation timed out, retrying queued range",
		"component", "ledger",
		"previous_connection_id", currentConnId.String(),
		"retry_connection_id", retryConnId.String(),
		"header_start_slot", headerStart.Slot,
		"header_end_slot", headerEnd.Slot,
		"header_count", headerCount,
	)
	if err := ls.startQueuedBlockfetchOnLocked(
		retryConnId,
		pending,
	); err != nil {
		ls.config.Logger.Error(
			"failed to retry blockfetch range after timeout",
			"component", "ledger",
			"connection_id", retryConnId.String(),
			"error", err,
		)
		if nextConnId, ok := ls.nextBlockfetchConnIdExcept(retryConnId); ok {
			ls.config.Logger.Warn(
				"retrying queued range on alternate blockfetch connection",
				"component", "ledger",
				"failed_connection_id", retryConnId.String(),
				"retry_connection_id", nextConnId.String(),
				"header_count", ls.chain.HeaderCount(),
			)
			if retryErr := ls.startQueuedBlockfetchOnLocked(
				nextConnId,
				pending,
			); retryErr != nil {
				ls.config.Logger.Error(
					"failed to restart queued blockfetch after timeout retry failure",
					"component",
					"ledger",
					"connection_id",
					nextConnId.String(),
					"error",
					retryErr,
				)
				if ls.chain.HeaderCount() > 0 {
					pending.add(
						ls.config.EventBus,
						event.ChainsyncResyncEventType,
						event.NewEvent(
							event.ChainsyncResyncEventType,
							event.ChainsyncResyncEvent{
								ConnectionId: retryConnId,
								Reason:       event.ChainsyncResyncReasonBlockfetchTimeoutRetryFailed,
							},
						),
					)
				}
			}
		}
	}
}

func (ls *LedgerState) handleEventBlockfetchBatchDone(
	e BlockfetchEvent,
	pending *pendingPublishes,
) error {
	// Drop batch-done from a stale connection (e.g., after connection switch).
	// Accept it from either the primary or the shadow peer: in the near-tip
	// shadow path the shadow can win the race and emit BatchDone before the
	// slow primary, and waiting for the primary's BatchDone defeats the
	// purpose of dispatching a shadow at all.
	if ls.chainsyncBlockfetchReadyChan == nil {
		return nil
	}
	fromActive := sameConnectionId(e.ConnectionId, ls.activeBlockfetchConnId)
	fromShadow := connIdKey(ls.shadowBlockfetchConnId) != "" &&
		sameConnectionId(e.ConnectionId, ls.shadowBlockfetchConnId)
	if !fromActive && !fromShadow {
		return nil
	}
	// If the shadow wins the race, treat it as the active connection for
	// the rest of this completion path so retry/resync decisions are made
	// against the peer that actually drove the batch to completion.
	if fromShadow && !fromActive {
		ls.activeBlockfetchConnId = ls.shadowBlockfetchConnId
		ls.config.Logger.Debug(
			"shadow blockfetch peer completed batch ahead of primary",
			"component", "ledger",
			"shadow_connection_id", e.ConnectionId.String(),
		)
	}
	// Stop the blockfetch timeout timer and invalidate any pending callbacks
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}
	ls.chainsyncBlockfetchTimerGeneration++
	receivedBlockCount := ls.batchBlocksReceived
	if err := ls.flushPendingBlockfetchBlocksDeferred(pending); err != nil {
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		return err
	}
	// Continue fetching as long as there are queued headers
	remainingHeaders := ls.chain.HeaderCount()
	if remainingHeaders > 0 {
		ls.config.Logger.Debug(
			"batch done, checking for more headers",
			"component", "ledger",
			"remaining_headers", remainingHeaders,
		)
	}
	// A batch that completed without delivering a block while headers stayed
	// queued is one of the two shapes of "could not obtain the queued range"
	// (the other is a NoBlocks reply, recorded in
	// startQueuedBlockfetchLocked). Both feed the same streak.
	if receivedBlockCount == 0 && remainingHeaders > 0 {
		batchStart, _ := ls.chain.HeaderRange(blockfetchBatchSize)
		if ls.noteBlockfetchRangeUnavailable(
			e.ConnectionId,
			batchStart,
			"batch completed without delivering a block",
			pending,
		) {
			return nil
		}
	}
	upstreamTipSlot := ls.UpstreamTipSlot()
	if receivedBlockCount == 0 &&
		remainingHeaders > 0 &&
		upstreamTipSlot > ls.Tip().Point.Slot &&
		upstreamTipSlot-ls.Tip().Point.Slot >= blockfetchMinBatchGapSlots {
		retryConnId := ls.selectRetryBlockfetchConn(e.ConnectionId)
		ls.blockfetchRequestRangeCleanup()
		if connIdKey(retryConnId) != "" &&
			!sameConnectionId(retryConnId, e.ConnectionId) {
			ls.config.Logger.Warn(
				"blockfetch batch returned no blocks, retrying queued range on alternate connection",
				"component",
				"ledger",
				"previous_connection_id",
				e.ConnectionId.String(),
				"retry_connection_id",
				retryConnId.String(),
				"remaining_headers",
				remainingHeaders,
			)
			ls.startQueuedBlockfetchFromEventLocked(
				retryConnId,
				e.ConnectionId,
				"empty blockfetch batch alternate retry failed",
			)
			return nil
		}
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		ls.config.Logger.Warn(
			"blockfetch batch returned no blocks, requesting chainsync re-sync",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"remaining_headers", remainingHeaders,
		)
		ls.requestChainsyncResync(
			e.ConnectionId,
			"empty blockfetch batch",
			pending,
		)
		return nil
	}
	if remainingHeaders == 0 {
		// No more headers to fetch, allow chainsync to collect more
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		if nextConnId, ok := ls.nextBufferedHeaderConnId(); ok {
			ls.replayBufferedHeadersAsync(nextConnId)
		}
		return nil
	}
	// Clean up from blockfetch batch
	ls.blockfetchRequestRangeCleanup()
	nextConnId, ok := ls.nextBlockfetchConnId()
	if !ok {
		ls.config.Logger.Debug(
			"headers pending but no next blockfetch connection is available",
			"component", "ledger",
			"remaining_headers", remainingHeaders,
			"active_blockfetch_connection_id",
			ls.activeBlockfetchConnId.String(),
		)
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		return nil
	}
	// GetBlockRange waits for the BatchDone event that is being handled here.
	// Continue on a worker so this subscriber remains available to drain the
	// next batch's block and BatchDone events.
	ls.startQueuedBlockfetchFromEventLocked(
		nextConnId,
		nextConnId,
		"blockfetch continuation failed",
	)
	return nil
}

// logSyncProgress logs periodic sync progress at INFO level.
// It reports the current slot, admitted upstream header frontier, percentage
// complete, and sync rate in slots per second. syncUpstreamTipSlot is read
// atomically since it is written by the chainsync handler goroutine.
func (ls *LedgerState) logSyncProgress(currentSlot uint64) {
	now := time.Now()
	if now.Sub(ls.syncProgressLastLog) < syncProgressLogInterval {
		return
	}
	upstreamTip := ls.UpstreamTipSlot()
	if upstreamTip == 0 {
		// No upstream tip known yet, skip
		return
	}
	elapsed := now.Sub(ls.syncProgressLastLog).Seconds()
	var slotsPerSec float64
	if elapsed > 0 && ls.syncProgressLastSlot > 0 &&
		currentSlot >= ls.syncProgressLastSlot {
		slotsDelta := currentSlot - ls.syncProgressLastSlot
		slotsPerSec = float64(slotsDelta) / elapsed
	}
	var pct float64
	if upstreamTip > 0 {
		pct = float64(currentSlot) / float64(upstreamTip) * 100
		if pct > 100 {
			pct = 100
		}
	}
	// Suppress progress logging when we're near the tip
	if pct >= 99.9 {
		ls.syncProgressLastLog = now
		ls.syncProgressLastSlot = currentSlot
		return
	}
	ls.config.Logger.Info(
		fmt.Sprintf(
			"sync progress: slot %d/%d (%.1f%%), %.0f slots/sec",
			currentSlot,
			upstreamTip,
			pct,
			slotsPerSec,
		),
		"component", "ledger",
	)
	ls.syncProgressLastLog = now
	ls.syncProgressLastSlot = currentSlot
}

// SyncProgress returns the current sync progress as a value between
// 0.0 (unknown/just started) and 1.0 (fully synced), allowing the peer
// governor to exit bootstrap mode once sync reaches its threshold.
func (ls *LedgerState) SyncProgress() float64 {
	upstreamTip := ls.UpstreamTipSlot()
	if upstreamTip == 0 {
		return 0
	}
	ls.RLock()
	currentSlot := ls.currentTip.Point.Slot
	ls.RUnlock()
	progress := float64(currentSlot) / float64(upstreamTip)
	if progress > 1.0 {
		progress = 1.0
	}
	return progress
}
