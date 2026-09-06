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
// through an atomic pointer, but only the blockfetch handler mutates it, so the
// counters and the producer set need no further synchronization: arming always
// installs a freshly allocated window.
type continuationAuditWindow struct {
	producedTxs map[string]struct{}
	forkPoint   ocommon.Point
	forkReason  string
	forkPeer    string
	remaining   int
	blocksSeen  int
	// endorserRefsDropped counts endorser-block references this window gave
	// up on for good: the parent lookup failed for a reason retrying cannot
	// fix. Those holes never close, so they keep the producer set
	// permanently short. A reference that is merely unresolved so far is not
	// counted here — it is still queued, and queued references are what
	// endorserProducersIncomplete reads.
	endorserRefsDropped int
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
	return len(w.pendingEndorserRefs) > 0 || w.endorserRefsDropped > 0
}

// requeueEndorserRef returns a reference to the pending queue, restoring the
// dedupe entry the drain removed when it took it, so a later audited body
// retries it and a concurrent duplicate is still suppressed.
func (w *continuationAuditWindow) requeueEndorserRef(
	ref continuationAuditEndorserRef,
) {
	w.pendingEndorserSeen[ref.key()] = struct{}{}
	w.pendingEndorserRefs = append(w.pendingEndorserRefs, ref)
}

// armContinuationAudit starts a bounded continuation audit at a rollback point.
// Callers must have rolled back both the primary chain and the ledger to point,
// which is what lets the audit treat blocks it has not yet seen as absent.
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
	ls.continuationAudit.Store(&continuationAuditWindow{
		producedTxs:            make(map[string]struct{}),
		pendingEndorserSeen:    make(map[string]struct{}),
		resolvedEndorserBlocks: make(map[string]struct{}),
		forkPoint: ocommon.Point{
			Slot: point.Slot,
			Hash: append([]byte(nil), point.Hash...),
		},
		forkReason: reason,
		forkPeer:   forkPeer,
		remaining:  continuationAuditBlockBudget,
	})
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
	// them would only add noise.
	if e.Point.Slot <= window.forkPoint.Slot {
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
	ls.queueContinuationAuditEndorserRef(window, e)
	producers := make([][]byte, 0, len(txs))
	for _, tx := range txs {
		producers = append(producers, tx.Hash().Bytes())
	}
	if !ls.recordContinuationAuditProducers(window, producers) {
		window.remaining = 0
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
			if !resolved && len(window.pendingEndorserRefs) > 0 {
				if !ls.drainContinuationAuditEndorserRefs(
					window,
					&endorserBudget,
					e.Point.Slot,
				) {
					window.remaining = 0
					return
				}
				_, resolved = window.producedTxs[string(input.Id().Bytes())]
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

// continuationInputHasProducer reports whether an input's producing
// transaction is reachable from the local chain: created by a block seen in
// this audit window, still present as an unspent UTxO, or recorded in
// transaction metadata as applied.
func (ls *LedgerState) continuationInputHasProducer(
	window *continuationAuditWindow,
	input lcommon.TransactionInput,
) (bool, error) {
	producerId := input.Id().Bytes()
	if _, ok := window.producedTxs[string(producerId)]; ok {
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
) bool {
	for _, id := range ids {
		key := string(id)
		if _, ok := window.producedTxs[key]; ok {
			continue
		}
		if len(window.producedTxs) >= continuationAuditMaxProducedTxs {
			ls.config.Logger.Warn(
				"disarming cross-fork continuation audit: producer set at capacity",
				"component", "ledger",
				"blocks_audited", window.blocksSeen,
				"produced_txs", len(window.producedTxs),
				"max_produced_txs", continuationAuditMaxProducedTxs,
			)
			ls.countContinuationAuditOutcome(
				continuationAuditResultDisarmedCap,
			)
			return false
		}
		window.producedTxs[key] = struct{}{}
	}
	return true
}

// queueContinuationAuditEndorserRef records, in header-only work, the endorser
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
func (ls *LedgerState) queueContinuationAuditEndorserRef(
	window *continuationAuditWindow,
	e BlockfetchEvent,
) {
	if ls.config.EndorserBlockProvider == nil || e.Block == nil {
		return
	}
	var ref continuationAuditEndorserRef
	if ls.config.LeiosApplyEndorserBlockTxs {
		referencer, ok := e.Block.Header().(leiosEndorserBlockReferencer)
		if !ok {
			return
		}
		ebHash, _, announced := referencer.LeiosAnnouncement()
		if !announced {
			return
		}
		ref = continuationAuditEndorserRef{
			ebHash:    ebHash,
			ebSlot:    e.Block.SlotNumber(),
			resolved:  true,
			blockSlot: e.Point.Slot,
		}
	} else {
		certifier, ok := e.Block.Header().(leiosEndorserBlockCertifier)
		if !ok {
			return
		}
		certified, present := certifier.LeiosCertified()
		if !present || !certified {
			return
		}
		ref = continuationAuditEndorserRef{
			certParentHash: e.Block.PrevHash().Bytes(),
			blockSlot:      e.Point.Slot,
		}
	}
	key := ref.key()
	if _, ok := window.pendingEndorserSeen[key]; ok {
		return
	}
	if _, ok := window.resolvedEndorserBlocks[key]; ok {
		return
	}
	window.pendingEndorserSeen[key] = struct{}{}
	window.pendingEndorserRefs = append(window.pendingEndorserRefs, ref)
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
func (ls *LedgerState) drainContinuationAuditEndorserRefs(
	window *continuationAuditWindow,
	budget *int,
	auditedSlot uint64,
) bool {
	pending := window.pendingEndorserRefs
	window.pendingEndorserRefs = nil
	armed := true
	for i, ref := range pending {
		if !armed || *budget <= 0 {
			// Requeue what was not reached. Hitting the budget means the
			// producer set is knowingly short for now.
			window.pendingEndorserRefs = append(
				window.pendingEndorserRefs,
				pending[i:]...,
			)
			if armed && *budget <= 0 {
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
				// Only report the budget stop once per drain.
				*budget = -1
			}
			continue
		}
		// One probe per reference per audited body: nothing that could
		// change the outcome happens between two inputs of the same body.
		if ref.lastProbedBlock == window.blocksSeen {
			window.requeueEndorserRef(ref)
			continue
		}
		delete(window.pendingEndorserSeen, ref.key())
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
				window.requeueEndorserRef(ref)
				continue
			case err != nil:
				// Anything else (I/O, a corrupt hash index) will not fix
				// itself by retrying, so the hole is permanent and the
				// window says so once rather than per body.
				window.endorserRefsDropped++
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
			window.requeueEndorserRef(ref)
			continue
		}
		ids, err := endorserBlockTxIds(rawTxs)
		if err != nil {
			window.endorserRefsDropped++
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
		if !ls.recordContinuationAuditProducers(window, ids) {
			armed = false
		}
	}
	if *budget < 0 {
		*budget = 0
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
