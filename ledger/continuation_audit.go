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
	"encoding/hex"
	"errors"
	"strconv"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// The cross-fork continuation audit answers the question issue #3005 could not
// answer from the failure site: when a block reaches the ledger spending an
// input whose producer is nowhere on the local chain, which peer delivered that
// body and which fork was the node following at the time?
//
// Cost and gating. A full per-input producer probe on every fetched body would
// add a database round trip per input to the steady-state blockfetch path, so
// the audit is armed only by a local rollback (rollbackChainAndStateDeferred and the
// replay-recovery rewind) and then runs for a bounded number of blocks. That is
// exactly the fork-churn / recovery regime where the splice appears.
//
// Arming on rollback is also what makes the audit sound. A rollback rewinds the
// primary chain and the ledger to the same point, so from that instant every
// block above the rollback point arrives through the audit. An input can
// therefore be resolved without a chain scan: it is legitimate when its
// producing transaction was created by a block already seen in this window
// (fetched, on the chain, not yet applied), or when the ledger already holds
// the UTxO, or when transaction metadata knows the producer. Anything left over
// has no producer on the local applied chain.
//
// Leios caveat. "A block was fetched, so its transactions are in hand" is true
// of every pre-Leios block and false on the Leios cert-driven path, where a
// certifying ranking block's body is empty: its transactions are the certified
// endorser block's, which arrives over leios-fetch as a separate artifact and
// is applied by LedgerState.applyEndorserBlock at ledger-apply time. The audit
// runs at blockfetch time, so with the ledger pipeline behind the blockfetch
// queue — the normal condition during an endorser-block backlog, and precisely
// the condition a rollback arms the audit in — neither the UTxO nor the
// transaction-metadata fallback can see an endorser-resident producer either.
// The window therefore resolves each audited block's endorser block the same
// way apply does (leiosEndorserBlockForApply plus EndorserBlockProvider) and
// records its transaction ids as producers. When that endorser block has not
// been fetched yet the producer set is knowingly incomplete, and the audit
// says "inconclusive" rather than asserting a missing producer: a report from
// an incomplete set is a false positive, and a false positive here is worse
// than silence because it asserts ledger corruption on a healthy node.
const (
	// continuationAuditBlockBudget bounds how many fetched blocks a single
	// arming inspects. Each local rollback re-arms, so a node churning at the
	// tip keeps the audit live while a healthy node pays nothing.
	continuationAuditBlockBudget = 512
	// continuationAuditMaxProducedTxs bounds the in-window producer set so a
	// long run cannot grow memory without limit. Exceeding it disarms the
	// window rather than risking false reports from a truncated set.
	//
	// Including endorser-block transactions changed the arithmetic: a Leios
	// ranking block body holds a few hundred transactions, but a certified
	// endorser block on the prototype network carries 100-2500, so a full
	// continuationAuditBlockBudget window can offer well past a million
	// producers. The cap is raised to 250k — roughly 16 MB of transient set
	// for a diagnostic — rather than to that ceiling, because unbounded
	// growth is the worse failure. A busy Leios window therefore disarms
	// partway through, which is now explicit: the disarm logs at Warn and
	// increments the audit-outcome counter with result="disarmed_cap", so
	// "the audit stopped covering this node" is visible instead of silent.
	continuationAuditMaxProducedTxs = 250_000
	// continuationAuditMaxReportsPerBlock caps log volume for a body with
	// many unresolvable inputs. The first few identify the fork just as well.
	continuationAuditMaxReportsPerBlock = 4
	// continuationAuditMaxInputsPerBlock bounds diagnostic database work while
	// the chainsync/blockfetch pipeline mutex is held. The audit is diagnostic,
	// so truncating a pathological block is preferable to delaying the
	// blockfetch pipeline behind unbounded per-input probes.
	continuationAuditMaxInputsPerBlock = 32
	// continuationAuditMaxEndorserBlocksPerBlock bounds how many endorser
	// blocks one audited body may cause to be resolved, in the same spirit as
	// continuationAuditMaxInputsPerBlock: resolving one costs a parent-block
	// read plus a hash of every endorser transaction, all under
	// chainsyncBlockfetchMutex. Refs left over stay queued for a later body,
	// and the window reports inconclusive in the meantime rather than
	// reporting from a set it knows is short.
	continuationAuditMaxEndorserBlocksPerBlock = 32
)

// Label values of the dingo_ledger_continuation_audit_outcomes_total metric.
const (
	continuationAuditResultClean                 = "clean"
	continuationAuditResultMissingProducer       = "missing_producer"
	continuationAuditResultInconclusiveEbPending = "inconclusive_eb_pending"
	continuationAuditResultDisarmedCap           = "disarmed_cap"
	continuationAuditResultSkippedBudget         = "skipped_budget"
	continuationAuditResultRefUnresolvable       = "ref_unresolvable"
)

// continuationAuditWindow is the state of one armed audit run. It is published
// through an atomic pointer, and only the blockfetch handler mutates its
// counters, so remaining and blocksSeen need no further synchronization:
// arming always installs a freshly allocated window. That holds for the
// counters of a window the handler did not audit against either -- see
// commitContinuationAuditBodyTo -- because the handler is still the only
// writer.
//
// The two sets a rearm carries forward are the exception, and they have a
// mutex each: producedTxsMutex for the producer set and endorserRefsMutex for
// the pending endorser-block references (see armContinuationAudit). A rearm
// runs on the chainsync dispatch goroutine under chainsyncMutex while the
// blockfetch goroutine is still recording producers and queueing references
// into that same window under chainsyncBlockfetchMutex. Neither of those locks
// covers both accesses, and a concurrent map iteration and write is fatal at
// runtime -- the same hazard bufferedHeaderMutex exists for. Reach either set
// only through the methods below; both mutexes are leaves, taken around the
// field access alone.
type continuationAuditWindow struct {
	producedTxsMutex sync.Mutex
	// producedTxs maps an in-window producer's transaction id to the slot of
	// the block that delivered it. The slot is what lets a rearm tell the
	// producers its own rollback just truncated off the chain from the ones
	// still on it.
	producedTxs map[string]uint64
	forkPoint   ocommon.Point
	forkReason  string
	forkPeer    string
	remaining   int
	blocksSeen  int
	// endorserRefsMutex guards pendingEndorserRefs, pendingEndorserSeen,
	// drainingEndorserRefs and endorserRefsDropped -- the state a rearm
	// reads out of the outgoing window, and the state
	// endorserProducersIncomplete decides from.
	endorserRefsMutex sync.Mutex
	// endorserRefsDropped counts endorser-block references this window gave
	// up on for good: the parent lookup failed for a reason retrying cannot
	// fix, or the endorser block's transactions would not decode. Those
	// holes never close, so they keep the producer set permanently short. A
	// reference that is merely unresolved so far is not counted here — it is
	// still queued, and queued references are what
	// endorserProducersIncomplete reads.
	//
	// A hole carried forward from a prior window counts as one here however
	// many that window dropped, because only the zero/non-zero state is
	// read.
	endorserRefsDropped int
	// lowestDroppedRefSlot is the lowest slot among the ranking blocks whose
	// references were dropped for good, and is meaningful only while
	// endorserRefsDropped is non-zero.
	//
	// A permanent hole is carried across a rearm on the same rule as a
	// producer and a queued reference, and needs a slot for the same reason:
	// a hole in a block the rollback truncated closes by itself, because the
	// peer re-delivers that block and the audit re-queues its reference,
	// while a hole in a block the rollback left on the chain never closes --
	// that block is not re-fetched. The lowest slot answers the carry
	// question exactly: a hole survives the rollback if and only if the
	// lowest one does, and when it does it is still the lowest of those that
	// remain.
	lowestDroppedRefSlot uint64
	// endorserRefErrorLogged keeps a hard parent-resolution failure to one
	// log line per window rather than one per audited body.
	endorserRefErrorLogged bool
	// pendingEndorserRefs are endorser-block references seen in this window
	// whose transactions have not been merged into producedTxs yet, in
	// arrival order. Classifying a block into a reference is header-only and
	// free; turning a reference into producers is not, so it is deferred
	// until an input actually fails to resolve without it. Deduplicated on
	// insert through pendingEndorserSeen, so the same closure referenced by
	// several ranking blocks is queued once.
	pendingEndorserRefs []continuationAuditEndorserRef
	pendingEndorserSeen map[string]struct{}
	// drainingEndorserRefs are the references a drain has taken off
	// pendingEndorserRefs and not put back yet. A drain spends a
	// parent-block read and a provider lookup per reference, so the queue
	// is empty for as long as that takes, and a rearm reading only
	// pendingEndorserRefs would carry none of them forward -- leaving the
	// new window short an endorser block nothing re-queues, which is the
	// hole carrying references forward exists to close.
	drainingEndorserRefs []continuationAuditEndorserRef
	// resolvedEndorserBlocks memoizes the (hash, slot) occurrences whose
	// transaction ids are already in producedTxs, so no endorser block is
	// hashed twice in a window.
	resolvedEndorserBlocks map[string]struct{}
	// endorserResolutions counts references this window actually did work
	// for: a parent-block read and/or a provider lookup. It is the cost the
	// audit adds to the blockfetch path, and it stays zero for a window in
	// which nothing spends an endorser-resident output.
	endorserResolutions int
}

// continuationAuditEndorserRef is one endorser-block reference an audited
// ranking block carries, in the cheapest form that can be resolved later.
//
// A CIP-path block announces its own endorser block, which the header gives up
// directly. A cert-driven block certifies the endorser block its parent
// announced, which needs a parent-block read; only the parent hash is retained
// so the referencing block itself (and its CBOR) does not have to be. Once that
// read happens the ref is rewritten in resolved form, so a still-unfetched
// endorser block costs at most a provider lookup on any later attempt.
type continuationAuditEndorserRef struct {
	certParentHash []byte
	ebHash         lcommon.Blake2b256
	ebSlot         uint64
	resolved       bool
	blockSlot      uint64
	// lastProbedBlock is the value of the window's blocksSeen when this
	// reference was last probed, so one audited body probes it at most once
	// however many of its inputs need the drain. Nothing about the reference
	// can change between two inputs of the same body.
	lastProbedBlock int
}

// key identifies a reference for dedupe: the parent hash while the reference is
// still a deferred certified closure, the (hash, slot) occurrence once
// resolved.
func (r continuationAuditEndorserRef) key() string {
	if !r.resolved {
		return "c" + string(r.certParentHash)
	}
	return "d" + string(r.ebHash.Bytes()) + strconv.FormatUint(r.ebSlot, 10)
}

// endorserProducersIncomplete reports whether the window's producer set is
// knowingly short of an endorser block it references, which makes an
// unresolved input inconclusive rather than evidence of a missing producer.
//
// It is derived rather than latched. A reference that could not be resolved on
// one body is requeued and retried on the next, so a window that recovers —
// the parent block lands, or the endorser block finishes fetching — goes back
// to being able to diagnose a genuine missing producer instead of staying
// inconclusive for the rest of its life. Only a reference given up on for good
// leaves a hole that never closes.
func (w *continuationAuditWindow) endorserProducersIncomplete() bool {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	return len(w.pendingEndorserRefs) > 0 ||
		len(w.drainingEndorserRefs) > 0 ||
		w.endorserRefsDropped > 0
}

// pendingEndorserRefCount is how many references are queued for a later body,
// which is what decides whether an unresolved input is worth a drain.
func (w *continuationAuditWindow) pendingEndorserRefCount() int {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	return len(w.pendingEndorserRefs)
}

// noteEndorserRefDropped records a reference given up on for good, at the slot
// of the ranking block that carried it. Also used to inherit a prior window's
// permanent hole, which is why the slot is a parameter rather than read off a
// reference.
func (w *continuationAuditWindow) noteEndorserRefDropped(blockSlot uint64) {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	if w.endorserRefsDropped == 0 || blockSlot < w.lowestDroppedRefSlot {
		w.lowestDroppedRefSlot = blockSlot
	}
	w.endorserRefsDropped++
}

// droppedEndorserRefAtOrBelow reports the lowest slot this window dropped a
// reference at, when that slot is at or below slot. It is what a rearm asks to
// decide whether a permanent hole survives its rollback.
func (w *continuationAuditWindow) droppedEndorserRefAtOrBelow(
	slot uint64,
) (uint64, bool) {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	if w.endorserRefsDropped == 0 || w.lowestDroppedRefSlot > slot {
		return 0, false
	}
	return w.lowestDroppedRefSlot, true
}

// takeEndorserRefsForDrain hands the queue to a drain and records what it took,
// so a rearm carrying references forward still sees the ones in flight.
func (w *continuationAuditWindow) takeEndorserRefsForDrain() (
	refs []continuationAuditEndorserRef,
) {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	refs = w.pendingEndorserRefs
	w.pendingEndorserRefs = nil
	w.drainingEndorserRefs = refs
	return refs
}

// finishEndorserDrain clears the in-flight record once the drain has put back
// everything it did not finish.
func (w *continuationAuditWindow) finishEndorserDrain() {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	w.drainingEndorserRefs = nil
}

// keepEndorserRefs moves references a drain took but did not probe back onto
// the queue. They never lost their dedupe entry, so this is a one-for-one move
// and cannot duplicate a key -- routing them through queueEndorserRef would see
// their own entry and drop them.
func (w *continuationAuditWindow) keepEndorserRefs(
	refs ...continuationAuditEndorserRef,
) {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	w.pendingEndorserRefs = append(w.pendingEndorserRefs, refs...)
}

// forgetEndorserRefKey drops a reference's dedupe entry, which a drain does
// before probing it so that a requeue can put it back.
func (w *continuationAuditWindow) forgetEndorserRefKey(key string) {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	delete(w.pendingEndorserSeen, key)
}

// endorserRefsForCarryForward copies out the references a rearm may carry: the
// queued ones and the ones a drain has in flight.
//
// A reference can appear in both, and in two forms, when a drain resolved one
// and put it back after its copy in drainingEndorserRefs was taken.
// queueEndorserRef collapses what the slot filter leaves, and the one duplicate
// a key cannot catch -- the deferred and resolved forms of a single reference
// -- costs one extra parent read and then converges on the resolved key. Both
// errors run in the safe direction: a reference too many keeps the new window
// inconclusive, where a reference too few reports a producer that is on the
// chain as missing.
func (w *continuationAuditWindow) endorserRefsForCarryForward() (
	refs []continuationAuditEndorserRef,
) {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	refs = make(
		[]continuationAuditEndorserRef,
		0,
		len(w.pendingEndorserRefs)+len(w.drainingEndorserRefs),
	)
	refs = append(refs, w.pendingEndorserRefs...)
	refs = append(refs, w.drainingEndorserRefs...)
	return refs
}

// queueEndorserRef puts a reference on the pending queue: one the audit has
// just classified out of a body's header, or one a drain took and could not
// finish. Both restore a dedupe entry the queue does not hold.
//
// It dedupes, because resolution can make two references converge. A deferred
// certified reference is keyed by its parent hash, and two ranking blocks with
// different parents can certify the same endorser block: the reference is
// (announced hash, announcing slot), so two blocks at one slot on either side
// of a fork — the state a window is armed in — name the same occurrence. Those
// keys differ while the references are deferred and become equal once resolved.
// Appending unconditionally then queued the same still-unfetched endorser block
// twice, and every later body probed it twice, spending budget other references
// needed.
//
// A caller still holding a reference's dedupe entry uses keepEndorserRefs
// instead: the drain's once-per-body and budget paths never take the entry, so
// routing them through here would see their own and drop them. An
// already-merged occurrence cannot reach here either: the drain consults
// resolvedEndorserBlocks before the provider, which is the only step after
// which a merged reference could be requeued.
func (w *continuationAuditWindow) queueEndorserRef(
	ref continuationAuditEndorserRef,
) {
	w.endorserRefsMutex.Lock()
	defer w.endorserRefsMutex.Unlock()
	key := ref.key()
	if _, ok := w.pendingEndorserSeen[key]; ok {
		return
	}
	w.pendingEndorserSeen[key] = struct{}{}
	w.pendingEndorserRefs = append(w.pendingEndorserRefs, ref)
}

// blockProducerIds is the transaction ids a ranking block body offers as
// in-window producers.
func blockProducerIds(txs []lcommon.Transaction) [][]byte {
	ids := make([][]byte, 0, len(txs))
	for _, tx := range txs {
		ids = append(ids, tx.Hash().Bytes())
	}
	return ids
}

// hasProducer reports whether txId was delivered as a producer in this window.
func (w *continuationAuditWindow) hasProducer(txId string) bool {
	w.producedTxsMutex.Lock()
	defer w.producedTxsMutex.Unlock()
	_, ok := w.producedTxs[txId]
	return ok
}

// producersAtOrBelow copies out the producers delivered by blocks at or below
// slot. The copy is what the next window owns, so the two windows never share
// a map.
func (w *continuationAuditWindow) producersAtOrBelow(
	slot uint64,
) map[string]uint64 {
	w.producedTxsMutex.Lock()
	defer w.producedTxsMutex.Unlock()
	carried := make(map[string]uint64, len(w.producedTxs))
	for txId, producedAt := range w.producedTxs {
		if producedAt <= slot {
			carried[txId] = producedAt
		}
	}
	return carried
}

// armContinuationAudit starts a bounded continuation audit at a rollback point.
// Callers must have rolled back both the primary chain and the ledger to point,
// which is what lets the audit treat blocks it has not yet seen as absent.
//
// Fork churn can call this more than once before the blocks an earlier window
// already vetted are durably applied: chainsync is handled per connection, and
// distinct connections routinely converge on rollback resolutions in quick
// succession while the ledger apply pipeline (deliberately, see
// auditContinuationBlock) lags behind blockfetch. A wholesale replace would
// discard the prior window's producedTxs for any such in-flight block,
// reporting it as a missing producer the moment something after it is
// audited under the new window (issue #4102).
//
// carryForwardProducedTxs decides which of the prior window's producers
// survive that, and a producer survives only when both of these hold.
//
// The prior window's fork point is still resolvable on the primary chain at
// its recorded slot. The primary chain is a single linear structure, so a
// fork point still on it was never removed by an intervening rollback, and
// the producers recorded against it describe this chain rather than one the
// node has since abandoned wholesale.
//
// And the producer's own block is at or below this rollback point. A rollback
// deletes every block above its point, so producers recorded above it are
// exactly the ones this rollback just abandoned: a body spending one of them
// is spending an output no longer on the local chain, which is the splice the
// audit exists to report, and the peer re-delivers the blocks that do belong
// above the point so their producers are re-recorded under the new window.
// Producers at or below the point are untouched by the truncation, stay on
// the chain, and are never re-fetched -- so nothing else would ever put them
// back, and dropping them is what produced the false report in issue #4102.
//
// A membership read that fails establishes neither, so the rearm disarms
// instead of publishing. The prior window's producers may still be on the
// chain, and a window published without them reports a later spend of their
// outputs as a missing producer at ERROR on a node where nothing is wrong.
func (ls *LedgerState) armContinuationAudit(
	point ocommon.Point,
	reason string,
) {
	forkPeer := ""
	if ls.config.GetActiveConnectionFunc != nil {
		if connId := ls.config.GetActiveConnectionFunc(); connId != nil {
			forkPeer = connId.String()
		}
	}
	next := &continuationAuditWindow{
		producedTxs:            make(map[string]uint64),
		pendingEndorserSeen:    make(map[string]struct{}),
		resolvedEndorserBlocks: make(map[string]struct{}),
		forkPoint: ocommon.Point{
			Slot: point.Slot,
			Hash: append([]byte(nil), point.Hash...),
		},
		forkReason: reason,
		forkPeer:   forkPeer,
		remaining:  continuationAuditBlockBudget,
	}
	ls.continuationAuditMutex.Lock()
	defer ls.continuationAuditMutex.Unlock()
	if prior := ls.continuationAudit.Load(); prior != nil {
		if err := ls.carryForwardWindow(prior, next, point); err != nil {
			ls.disarmContinuationAuditUnverified(prior.forkPoint, err)
			return
		}
	}
	ls.publishContinuationAudit(next)
}

// publishContinuationAudit is the single writer of the window pointer, and
// counts the transition so that a later caller can tell whether the pointer it
// is looking at is still the one it left.
//
// Callers must hold ls.continuationAuditMutex.
func (ls *LedgerState) publishContinuationAudit(
	window *continuationAuditWindow,
) {
	ls.continuationAudit.Store(window)
	ls.continuationAuditGen++
}

// disarmContinuationAudit takes the window out of service. Every transition of
// the pointer goes through continuationAuditMutex, so a disarm cannot land
// between an arm reading the outgoing window and publishing its replacement.
func (ls *LedgerState) disarmContinuationAudit() {
	ls.continuationAuditMutex.Lock()
	defer ls.continuationAuditMutex.Unlock()
	ls.publishContinuationAudit(nil)
}

// disarmContinuationAuditUnverified takes the window out of service because
// the primary-chain membership read at point failed, and says so at Warn.
//
// Membership is what makes a block's transactions producers at all, so a read
// that fails leaves the producer set short by an unknown amount. A window that
// stays in service from there reports a spend of any output it is short of as
// a missing producer at ERROR, which asserts ledger corruption on a node where
// the only fault was the read. Every audit decision errs toward silence, and
// that is the silent option. Publishing nil moves the pointer generation, so a
// recovery rewind settling afterwards does not restore a window over it.
//
// Callers must hold ls.continuationAuditMutex.
func (ls *LedgerState) disarmContinuationAuditUnverified(
	point ocommon.Point,
	err error,
) {
	ls.publishContinuationAudit(nil)
	ls.config.Logger.Warn(
		"disarming cross-fork continuation audit: primary chain membership check failed",
		"component",
		"ledger",
		"error",
		err,
		"slot",
		point.Slot,
		"hash",
		hex.EncodeToString(point.Hash),
	)
}

// continuationAuditOnPrimaryChain is the primary-chain membership read every
// audit decision about a producer rests on. See primaryChainContainsPoint.
func (ls *LedgerState) continuationAuditOnPrimaryChain(
	point ocommon.Point,
) (bool, error) {
	if ls.continuationAuditContainsPoint != nil {
		return ls.continuationAuditContainsPoint(point)
	}
	return ls.primaryChainContainsPoint(point)
}

// carryForwardWindow moves what prior knows about blocks this rollback did not
// truncate into next, and carries nothing when prior's fork point is no longer
// on the primary chain. See armContinuationAudit for why those two conditions
// are what make a carried-forward producer legitimate.
//
// Primary-chain membership is the test, not blob presence. Abandoned-fork
// blocks stay in the append-only blob store and stay reachable by hash, so a
// hash lookup would accept a fork point the node has since walked away from
// and carry its producers onto a chain they were never part of.
// primaryChainContainsPoint compares the point encoded at the block's index
// entry, which is what identifies the current primary chain.
//
// Endorser-block references travel with the producers, on the same rule and
// keyed by the same slot: a reference names the ranking block that carries it,
// so a reference at or below the point belongs to a block the rollback left on
// the chain. Dropping it would leave the new window unable to resolve an
// endorser-resident producer that nothing re-queues -- the ranking block is
// not re-fetched, because the chain still holds it -- which is the same false
// report in the shape endorser transactions take. The already-merged memo is
// deliberately not carried: re-merging a closure is idempotent (a repeat keeps
// the lowest slot) and costs one hashing pass, where a stale memo would
// suppress a merge the new window needs.
//
// A reference given up on for good travels on that rule too, as the slot it
// was dropped at. It is not a queued reference and never becomes one, so only
// endorserRefsDropped remembers that the producer set is short of it -- and a
// hole in a block the rollback left on the chain never closes, because that
// block is not re-fetched.
//
// It returns the membership read's error, having carried nothing. next is then
// not fit to publish: see armContinuationAudit.
func (ls *LedgerState) carryForwardWindow(
	prior *continuationAuditWindow,
	next *continuationAuditWindow,
	point ocommon.Point,
) error {
	onChain, err := ls.continuationAuditOnPrimaryChain(prior.forkPoint)
	if err != nil {
		return err
	}
	if !onChain {
		return nil
	}
	next.producedTxs = prior.producersAtOrBelow(point.Slot)
	// A permanent hole travels on the same rule. Dropping it would let the
	// new window report an endorser-resident producer as missing on exactly
	// the closure the prior window knew it could not resolve, which is the
	// false report this carry-forward exists to prevent.
	if slot, ok := prior.droppedEndorserRefAtOrBelow(point.Slot); ok {
		next.noteEndorserRefDropped(slot)
	}
	for _, ref := range prior.endorserRefsForCarryForward() {
		if ref.blockSlot > point.Slot {
			continue
		}
		// lastProbedBlock counts against the window that recorded it, and
		// next starts its own count.
		ref.lastProbedBlock = 0
		next.queueEndorserRef(ref)
	}
	return nil
}

// auditContinuationBlock checks that every input a freshly fetched body spends
// has a producer on the local applied chain, and logs loudly when one does not.
// It is a diagnostic only: it never rejects a block, because the cross-fork
// splice it detects is prevented upstream in chain.Chain.rollbackPointBlock and
// any body that still slips through must reach the ledger's own validation and
// the #2973 / #3008 guards unchanged.
//
// It is also skipped while block validation is off, which is how historical
// catch-up runs: the splice this diagnoses is a live tip-band failure, and
// bulk sync fetches far too many bodies per second to pay for the probes.
//
// Callers must hold ls.chainsyncBlockfetchMutex.
func (ls *LedgerState) auditContinuationBlock(
	e BlockfetchEvent,
	validationEnabled bool,
) {
	if !validationEnabled {
		return
	}
	window := ls.continuationAudit.Load()
	if window == nil || window.remaining <= 0 || e.Block == nil {
		return
	}
	// Bodies at or below the fork point are in-flight leftovers from the
	// batch the rollback abandoned. They cannot extend the chain, so auditing
	// them would only add noise -- but one of them may still be a producer
	// this window is missing, so record before returning.
	if e.Point.Slot <= window.forkPoint.Slot {
		ls.recordLateProducers(window, e)
		return
	}
	window.remaining--
	window.blocksSeen++
	txs := e.Block.Transactions()
	// Record this block's producers before checking its inputs. A later
	// transaction may spend an output created by an earlier one in the same
	// block, and treating the whole block as a producer errs toward silence
	// rather than toward a false report. The block's Leios endorser block, if
	// it has one, contributes producers too: on the cert-driven path those are
	// the only transactions a certifying ranking block brings, its own body
	// being empty.
	if !ls.commitContinuationAuditBody(window, e, blockProducerIds(txs)) {
		return
	}
	reports := 0
	inputsAudited := 0
	endorserBudget := continuationAuditMaxEndorserBlocksPerBlock
	for _, tx := range txs {
		for _, input := range collectReferencedInputs(tx) {
			if inputsAudited >= continuationAuditMaxInputsPerBlock {
				return
			}
			inputsAudited++
			if reports >= continuationAuditMaxReportsPerBlock {
				return
			}
			resolved, err := ls.continuationInputHasProducer(window, input)
			if err != nil {
				ls.config.Logger.Debug(
					"cross-fork continuation audit could not resolve input",
					"component", "ledger",
					"error", err,
					"slot", e.Point.Slot,
					"input", input.String(),
				)
				continue
			}
			// Only now, with the cheap paths exhausted, is it worth
			// paying for the window's endorser blocks: an audited body
			// that spends nothing endorser-resident never triggers a
			// parent-block read or a transaction hash.
			if !resolved && window.pendingEndorserRefCount() > 0 {
				if !ls.drainContinuationAuditEndorserRefs(
					window,
					&endorserBudget,
					e.Point.Slot,
				) {
					window.remaining = 0
					return
				}
				resolved = window.hasProducer(
					string(input.Id().Bytes()),
				)
			}
			if resolved {
				ls.countContinuationAuditOutcome(
					continuationAuditResultClean,
				)
				continue
			}
			reports++
			// An unresolved input proves nothing while the window's
			// producer set has a known hole: the producer may sit in a
			// certified endorser block the node has fetched but not yet
			// handed to the audit. Say so quietly instead of asserting
			// ledger corruption on a healthy node.
			if window.endorserProducersIncomplete() {
				ls.countContinuationAuditOutcome(
					continuationAuditResultInconclusiveEbPending,
				)
				ls.config.Logger.Debug(
					"cross-fork continuation audit inconclusive: certified endorser block not fetched yet",
					"component", "ledger",
					"block_slot", e.Point.Slot,
					"block_hash", hex.EncodeToString(e.Point.Hash),
					"tx_hash", tx.Hash().String(),
					"input", input.String(),
					"producer_tx_hash", input.Id().String(),
					"blocks_since_fork", window.blocksSeen,
				)
				continue
			}
			ls.metrics.continuationInputUnresolved.Inc()
			ls.countContinuationAuditOutcome(
				continuationAuditResultMissingProducer,
			)
			ls.config.Logger.Error(
				"continuation block spends an input with no producer on the local applied chain",
				"component",
				"ledger",
				"peer",
				e.ConnectionId.String(),
				"block_slot",
				e.Point.Slot,
				"block_hash",
				hex.EncodeToString(e.Point.Hash),
				"block_prev_hash",
				e.Block.PrevHash().String(),
				"tx_hash",
				tx.Hash().String(),
				"input",
				input.String(),
				"producer_tx_hash",
				input.Id().String(),
				"fork_rollback_slot",
				window.forkPoint.Slot,
				"fork_rollback_hash",
				hex.EncodeToString(window.forkPoint.Hash),
				"fork_reason",
				window.forkReason,
				"fork_peer",
				window.forkPeer,
				"blocks_since_fork",
				window.blocksSeen,
			)
		}
	}
}

// recordLateProducers records the producers of a body at or below the window's
// fork point, when that body is still on the primary chain at its own point.
//
// It closes the one hole a carry-forward rearm leaves. A body is added to the
// chain and audited as two steps of the blockfetch drain, under
// chainsyncBlockfetchMutex; a chainsync rollback arms the next window from the
// dispatch goroutine under chainsyncMutex, and neither lock covers both. A
// rearm landing between those two steps snapshots a producer set that does not
// hold the just-added body yet, and the body then arrives here below the new
// fork point and would be dropped -- although its block survived the rollback
// that armed this window, so its outputs are on the chain and their next
// audited spend would be reported as a missing producer. That false report is
// the one this window carries producers forward to remove.
//
// Chain membership is what makes recording it sound, and it is re-read rather
// than assumed: a body from an abandoned fetch, or one a later rollback has
// since removed, must not become a producer. The read is paid only for bodies
// at or below the fork point, which are the leftovers of one rollback rather
// than the steady-state blockfetch path.
//
// The body's inputs are not audited and no block budget is spent: it cannot
// extend the chain above the fork point, which is what the audit inspects.
//
// A read that fails disarms whichever window is published by then. The body
// may be on the chain, and any window armed since the one it was fetched under
// was carried forward without it.
//
// Callers must hold ls.chainsyncBlockfetchMutex.
func (ls *LedgerState) recordLateProducers(
	window *continuationAuditWindow,
	e BlockfetchEvent,
) {
	onChain, err := ls.continuationAuditOnPrimaryChain(e.Point)
	if err != nil {
		ls.continuationAuditMutex.Lock()
		defer ls.continuationAuditMutex.Unlock()
		ls.disarmContinuationAuditUnverified(e.Point, err)
		return
	}
	if !onChain {
		return
	}
	ls.commitContinuationAuditBody(
		window,
		e,
		blockProducerIds(e.Block.Transactions()),
	)
}

// commitContinuationAuditBody records what an audited body offers -- its
// producer ids and the endorser block it references -- against the window that
// is published now, which is not always the window the caller has been
// auditing against. It reports whether that is still the caller's window and
// whether the window is still armed.
//
// Recording and arming are mutually exclusive under continuationAuditMutex, so
// a rearm either copies these producers out of the caller's window or publishes
// before them and receives them here. Without that ordering a rearm landing
// between the caller's Load and its record snapshots a set the block is not in
// yet and the record lands in a window nothing reads again: the producer is
// lost although its block is on the chain, and the next audited spend of its
// outputs is the false missing-producer report this window exists to prevent.
//
// A publication that actually raced is the only case that pays for a chain
// read. The rollback that armed the published window may have deleted this
// block, and primary-chain membership at the block's own point is what settles
// it -- membership is also the whole justification for calling a block a
// producer, and it does not depend on which window is published. When nothing
// raced, the caller's window is the published one and no read is made. A read
// that fails disarms the published window rather than leaving it in service
// without this body's producers.
//
// A false return means the caller must stop auditing this body. Its window is
// no longer the one this body's producers went into, so a later transaction in
// the same body spending an earlier one's output resolves against a set the
// body is not in and reads as having no producer at all -- the false report,
// reintroduced from the other side. Nothing is lost by stopping: a rearm
// publishes a window whose fork point is at or above this body, which is
// exactly the body the audit does not inspect, and a truncated body is not on
// the chain to inspect.
//
// The endorser-block reference travels with the producers for the same reason
// and on the same membership test. A cert-driven ranking block's body is empty,
// so the reference is the only thing it offers; queueing it against a window
// nothing reads again leaves the published window unable to resolve an
// endorser-resident producer, which is that same false report in the shape
// Leios transactions take.
func (ls *LedgerState) commitContinuationAuditBody(
	window *continuationAuditWindow,
	e BlockfetchEvent,
	ids [][]byte,
) bool {
	ref, hasRef := ls.continuationAuditEndorserRefFor(e)
	ls.continuationAuditMutex.Lock()
	defer ls.continuationAuditMutex.Unlock()
	published := ls.continuationAudit.Load()
	if published == nil {
		// Disarmed while this body was being audited. There is no window
		// left to record into, and one that is out of service must not
		// report.
		return false
	}
	if published != window {
		onChain, err := ls.continuationAuditOnPrimaryChain(e.Point)
		switch {
		case err != nil:
			ls.disarmContinuationAuditUnverified(e.Point, err)
		case onChain:
			ls.commitContinuationAuditBodyTo(published, e, ids, ref, hasRef)
		}
		return false
	}
	return ls.commitContinuationAuditBodyTo(window, e, ids, ref, hasRef)
}

// commitContinuationAuditBodyTo queues the body's endorser reference and
// records its producers against target, and reports whether target is still
// armed. Reaching the producer cap disarms whichever window reached it, which
// is not always the window the caller audited against.
//
// Callers must hold ls.continuationAuditMutex.
func (ls *LedgerState) commitContinuationAuditBodyTo(
	target *continuationAuditWindow,
	e BlockfetchEvent,
	ids [][]byte,
	ref continuationAuditEndorserRef,
	hasRef bool,
) bool {
	if hasRef {
		ls.queueContinuationAuditEndorserRef(target, ref)
	}
	if ls.recordContinuationAuditProducers(target, ids, e.Point.Slot) {
		return true
	}
	target.remaining = 0
	return false
}

// continuationInputHasProducer reports whether an input's producing
// transaction is reachable from the local chain: created by a block seen in
// this audit window, still present as an unspent UTxO, or recorded in
// transaction metadata as applied.
func (ls *LedgerState) continuationInputHasProducer(
	window *continuationAuditWindow,
	input lcommon.TransactionInput,
) (bool, error) {
	producerId := input.Id().Bytes()
	if window.hasProducer(string(producerId)) {
		return true, nil
	}
	utxo, err := ls.db.UtxoByRef(producerId, input.Index(), nil)
	if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
		return false, err
	}
	if utxo != nil {
		return true, nil
	}
	producerTx, err := ls.db.GetTransactionByHash(producerId, nil)
	if err != nil {
		return false, err
	}
	return producerTx != nil, nil
}

// recordContinuationAuditProducers adds producer transaction ids to the
// window's set, and reports whether the set stayed inside
// continuationAuditMaxProducedTxs.
//
// Only ids the set does not already hold count toward the cap. The cap bounds
// the size of the producer set, not the number of ids offered to it, and
// charging a repeat against it would disarm a window whose set never grew.
// Repeats are ordinary on the Leios path: the same transaction can appear in
// more than one endorser block — applyEndorserBlock carries
// deduplicateEndorserBlockTransactionIndexes for exactly that — and an endorser
// block is content-addressed, so the same closure can be referenced from more
// than one ranking block inside a window.
//
// The cap is enforced per insert rather than per block, so the set is never
// larger than the cap. A block that runs the set into the cap partway through
// leaves those ids recorded, which is harmless: the window is disarmed and its
// set is never consulted again.
func (ls *LedgerState) recordContinuationAuditProducers(
	window *continuationAuditWindow,
	ids [][]byte,
	slot uint64,
) bool {
	size, ok := window.recordProducers(ids, slot)
	if ok {
		return true
	}
	ls.config.Logger.Warn(
		"disarming cross-fork continuation audit: producer set at capacity",
		"component", "ledger",
		"blocks_audited", window.blocksSeen,
		"produced_txs", size,
		"max_produced_txs", continuationAuditMaxProducedTxs,
	)
	ls.countContinuationAuditOutcome(
		continuationAuditResultDisarmedCap,
	)
	return false
}

// recordProducers is the map half of recordContinuationAuditProducers: it adds
// ids at slot under producedTxsMutex and reports the resulting set size and
// whether the set stayed inside the cap. The logging and the outcome counter
// stay outside the lock, which is a leaf.
//
// An id the set already holds keeps the lowest slot it has been offered at.
// The slot answers "which rollback would take this producer off the chain",
// and a producer delivered by blocks at two slots leaves the chain only when
// the lower one is truncated. Raising it instead would drop a producer whose
// block a rearm did not remove, which is the false report this window records
// slots to prevent. Repeats are ordinary here: the same transaction can appear
// in more than one endorser block, and the same closure can be certified from
// more than one ranking block inside a window.
func (w *continuationAuditWindow) recordProducers(
	ids [][]byte,
	slot uint64,
) (int, bool) {
	w.producedTxsMutex.Lock()
	defer w.producedTxsMutex.Unlock()
	for _, id := range ids {
		key := string(id)
		if recordedAt, ok := w.producedTxs[key]; ok {
			if slot < recordedAt {
				w.producedTxs[key] = slot
			}
			continue
		}
		if len(w.producedTxs) >= continuationAuditMaxProducedTxs {
			return len(w.producedTxs), false
		}
		w.producedTxs[key] = slot
	}
	return len(w.producedTxs), true
}

// continuationAuditEndorserRefFor classifies, in header-only work, the endorser
// block an audited ranking block applies, so it can be turned into producers
// later if any input needs it.
//
// The reference is selected exactly as the apply path selects it: the block's
// own announcement when LeiosApplyEndorserBlockTxs is set (the CIP path,
// bound to the block's own slot), the parent's announcement otherwise (the
// Musashi cert-driven path). The cert-driven case retains only the parent hash;
// leiosCertifiedAnnouncementFromParent — the same helper
// leiosEndorserBlockForApply uses — turns it into a reference when the time
// comes, so the two cannot select different endorser blocks.
//
// Non-Leios chains are unaffected: no endorser-block provider is configured,
// and a header that neither announces nor certifies an endorser block queues
// nothing.
func (ls *LedgerState) continuationAuditEndorserRefFor(
	e BlockfetchEvent,
) (continuationAuditEndorserRef, bool) {
	var ref continuationAuditEndorserRef
	if ls.config.EndorserBlockProvider == nil || e.Block == nil {
		return ref, false
	}
	if ls.config.LeiosApplyEndorserBlockTxs {
		referencer, ok := e.Block.Header().(leiosEndorserBlockReferencer)
		if !ok {
			return ref, false
		}
		ebHash, _, announced := referencer.LeiosAnnouncement()
		if !announced {
			return ref, false
		}
		return continuationAuditEndorserRef{
			ebHash:    ebHash,
			ebSlot:    e.Block.SlotNumber(),
			resolved:  true,
			blockSlot: e.Point.Slot,
		}, true
	}
	certifier, ok := e.Block.Header().(leiosEndorserBlockCertifier)
	if !ok {
		return ref, false
	}
	certified, present := certifier.LeiosCertified()
	if !present || !certified {
		return ref, false
	}
	return continuationAuditEndorserRef{
		certParentHash: e.Block.PrevHash().Bytes(),
		blockSlot:      e.Point.Slot,
	}, true
}

// queueContinuationAuditEndorserRef queues a classified reference against the
// window that will be asked to resolve it, unless that window has already
// merged the occurrence it names.
// resolvedEndorserBlocks needs no lock of its own: the blockfetch handler is
// its only reader and its only writer, whichever window it reaches.
func (ls *LedgerState) queueContinuationAuditEndorserRef(
	window *continuationAuditWindow,
	ref continuationAuditEndorserRef,
) {
	if _, ok := window.resolvedEndorserBlocks[ref.key()]; ok {
		return
	}
	window.queueEndorserRef(ref)
}

// drainContinuationAuditEndorserRefs merges queued endorser blocks into the
// window's producer set, spending at most *budget resolutions, and reports
// whether the window is still armed.
//
// Each endorser block is resolved and hashed at most once per window: the
// queue is deduplicated on insert and the (hash, slot) occurrences already
// merged are memoized, so the same closure referenced by several ranking
// blocks costs one parent read, one provider lookup and one hashing pass, not
// one per referencing block.
//
// A reference whose endorser block is not cached yet stays queued in resolved
// form — a later attempt costs only the provider lookup — and marks the window
// incomplete, so unresolved inputs read as inconclusive rather than as missing
// producers.
//
// A provider lookup is not guaranteed to be I/O-free. EndorserBlockProvider
// resolves to the ouroboros Leios cache, which on an in-memory miss falls back
// to a blob-store manifest read, a decode and a transaction load. The audit is
// armed exactly during an endorser-block backlog and holds
// chainsyncBlockfetchMutex throughout, so an occurrence whose in-memory entry
// has expired turns into that read here. It is bounded — at most *budget of
// them per audited body, and once per (hash, slot) for the life of the window
// — but it is a disk read, not merely a map hit.
//
// *budget is owned by the audited body, not by one drain: a body drains once
// per unresolved input and every drain spends the same allowance, which is why
// it is passed by pointer. Once exhausted it is burned to -1 and stays there
// for the rest of the body, so skipped_budget counts audited bodies whose
// budget ran out — what its Help string promises — and not unresolved inputs.
func (ls *LedgerState) drainContinuationAuditEndorserRefs(
	window *continuationAuditWindow,
	budget *int,
	auditedSlot uint64,
) bool {
	pending := window.takeEndorserRefsForDrain()
	defer window.finishEndorserDrain()
	armed := true
	for i, ref := range pending {
		if !armed || *budget <= 0 {
			// Requeue everything not reached, in one move, and stop.
			// The remainder is a contiguous suffix, so there is nothing
			// left for a further iteration to do: continuing here once
			// appended an overlapping suffix per remaining reference,
			// which grew the queue quadratically and made later bodies
			// probe the same reference several times. These references
			// were never taken off pendingEndorserSeen, so this is a
			// one-for-one move and cannot duplicate a key.
			window.keepEndorserRefs(pending[i:]...)
			if armed && *budget == 0 {
				ls.countContinuationAuditOutcome(
					continuationAuditResultSkippedBudget,
				)
				ls.config.Logger.Debug(
					"cross-fork continuation audit deferred endorser blocks past its per-block budget",
					"component", "ledger",
					"slot", auditedSlot,
					"deferred", len(pending)-i,
					"budget", continuationAuditMaxEndorserBlocksPerBlock,
				)
				// The budget belongs to the audited body, not to this
				// drain: one body triggers one drain per unresolved
				// input, all sharing it. Burn it to -1 so the later
				// drains of the same body take this branch without
				// reporting a second stop for the one exhaustion.
				*budget = -1
			}
			break
		}
		// One probe per reference per audited body: nothing that could
		// change the outcome happens between two inputs of the same body.
		// Its dedupe entry was never taken, so this moves the reference
		// back one-for-one rather than going through queueEndorserRef.
		if ref.lastProbedBlock == window.blocksSeen {
			window.keepEndorserRefs(ref)
			continue
		}
		window.forgetEndorserRefKey(ref.key())
		ref.lastProbedBlock = window.blocksSeen
		*budget--
		window.endorserResolutions++
		if !ref.resolved {
			ebHash, ebSlot, _, announced, err := ls.leiosCertifiedAnnouncementFromParent(
				ref.certParentHash,
			)
			switch {
			case errors.Is(err, models.ErrBlockNotFound):
				// The parent is not in the block store yet. That is a
				// state, not a verdict — it is exactly the read-ahead
				// this audit exists to tolerate — so keep the reference
				// and retry it on the next body.
				ls.config.Logger.Debug(
					"cross-fork continuation audit will retry a certifying block's parent lookup",
					"component", "ledger",
					"slot", ref.blockSlot,
				)
				window.queueEndorserRef(ref)
				continue
			case err != nil:
				// Anything else (I/O, a corrupt hash index) will not fix
				// itself by retrying, so the hole is permanent and the
				// window says so once rather than per body.
				window.noteEndorserRefDropped(ref.blockSlot)
				ls.countContinuationAuditOutcome(
					continuationAuditResultRefUnresolvable,
				)
				if !window.endorserRefErrorLogged {
					window.endorserRefErrorLogged = true
					ls.config.Logger.Warn(
						"cross-fork continuation audit gave up resolving a certifying block's parent",
						"component", "ledger",
						"slot", ref.blockSlot,
						"error", err,
					)
				}
				continue
			}
			if !announced {
				// Resolved, and there is definitively no endorser block:
				// not a hole.
				continue
			}
			ref.ebHash = ebHash
			ref.ebSlot = ebSlot
			ref.resolved = true
		}
		key := ref.key()
		if _, ok := window.resolvedEndorserBlocks[key]; ok {
			continue
		}
		rawTxs, ok := ls.config.EndorserBlockProvider(
			ref.ebHash.Bytes(),
			ref.ebSlot,
		)
		if !ok {
			ls.config.Logger.Debug(
				"cross-fork continuation audit: certified endorser block not fetched yet",
				"component", "ledger",
				"slot", ref.blockSlot,
				"eb_slot", ref.ebSlot,
				"eb_hash", ref.ebHash.String(),
			)
			// Keep it queued in resolved form: the parent read is done,
			// and the block may be fetched before the window ends.
			window.queueEndorserRef(ref)
			continue
		}
		ids, err := endorserBlockTxIds(rawTxs)
		if err != nil {
			window.noteEndorserRefDropped(ref.blockSlot)
			ls.countContinuationAuditOutcome(
				continuationAuditResultRefUnresolvable,
			)
			ls.config.Logger.Debug(
				"cross-fork continuation audit could not read endorser block transaction ids",
				"component", "ledger",
				"slot", ref.blockSlot,
				"eb_slot", ref.ebSlot,
				"eb_hash", ref.ebHash.String(),
				"error", err,
			)
			continue
		}
		window.resolvedEndorserBlocks[key] = struct{}{}
		// The endorser block's transactions are applied by the ranking
		// block that carries the reference, so they leave the chain with
		// that block and not with the endorser block's own (earlier)
		// announcing slot. Record them against the referencing block.
		if !ls.recordContinuationAuditProducers(window, ids, ref.blockSlot) {
			armed = false
		}
	}
	return armed
}

// countContinuationAuditOutcome increments the pre-materialized outcome
// counter for one audit verdict. Metrics are unset in unit tests that build a
// LedgerState directly, so every child is nil-checked.
func (ls *LedgerState) countContinuationAuditOutcome(result string) {
	var counter prometheus.Counter
	switch result {
	case continuationAuditResultClean:
		counter = ls.metrics.continuationAuditClean
	case continuationAuditResultMissingProducer:
		counter = ls.metrics.continuationAuditMissingProducer
	case continuationAuditResultInconclusiveEbPending:
		counter = ls.metrics.continuationAuditInconclusiveEbPending
	case continuationAuditResultDisarmedCap:
		counter = ls.metrics.continuationAuditDisarmedCap
	case continuationAuditResultSkippedBudget:
		counter = ls.metrics.continuationAuditSkippedBudget
	case continuationAuditResultRefUnresolvable:
		counter = ls.metrics.continuationAuditRefUnresolvable
	}
	if counter == nil {
		return
	}
	counter.Inc()
}
