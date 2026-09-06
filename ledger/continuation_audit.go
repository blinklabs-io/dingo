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

	"github.com/blinklabs-io/dingo/database"
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
)

// Label values of the dingo_ledger_continuation_audit_outcomes_total metric.
const (
	continuationAuditResultClean                 = "clean"
	continuationAuditResultMissingProducer       = "missing_producer"
	continuationAuditResultInconclusiveEbPending = "inconclusive_eb_pending"
	continuationAuditResultDisarmedCap           = "disarmed_cap"
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
	// endorserProducersPending records that at least one audited block in
	// this window referenced a Leios endorser block whose transactions the
	// audit could not obtain, so producedTxs is known to be incomplete and
	// an unresolved input is not evidence of a missing producer. It is
	// sticky for the life of the window: once a hole exists, every later
	// input could be falling into it.
	endorserProducersPending bool
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
		producedTxs: make(map[string]struct{}),
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
	ebTxIds := ls.continuationAuditEndorserProducers(window, e)
	if len(window.producedTxs)+len(txs)+len(ebTxIds) >
		continuationAuditMaxProducedTxs {
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
		window.remaining = 0
		return
	}
	for _, tx := range txs {
		window.producedTxs[string(tx.Hash().Bytes())] = struct{}{}
	}
	for _, id := range ebTxIds {
		window.producedTxs[string(id)] = struct{}{}
	}
	reports := 0
	inputsAudited := 0
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
			if window.endorserProducersPending {
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

// continuationAuditEndorserProducers returns the transaction ids of the Leios
// endorser block the audited ranking block applies, resolving it exactly the
// way ledgerProcessBlock does: leiosEndorserBlockForApply picks the reference
// (the block's own announcement on the CIP path, the parent's announcement on
// the Musashi cert-driven path) and EndorserBlockProvider supplies the
// transactions for that (hash, slot) occurrence.
//
// The endorser block is already in the provider's cache by the time the
// referencing ranking block is fetched — leios-fetch runs well ahead of ledger
// apply — so the common path is a cache lookup plus a hash per endorser
// transaction, with no additional I/O beyond the parent-block lookup the
// cert-driven path needs. When the reference cannot be resolved or the block is
// not cached yet, the window is marked incomplete rather than left to report a
// producer it was never given.
//
// Non-Leios chains are unaffected: no endorser-block provider is configured,
// and a header that neither announces nor certifies an endorser block returns
// no reference.
func (ls *LedgerState) continuationAuditEndorserProducers(
	window *continuationAuditWindow,
	e BlockfetchEvent,
) [][]byte {
	if ls.config.EndorserBlockProvider == nil {
		return nil
	}
	ebHash, ebSlot, _, referenced, err := ls.leiosEndorserBlockForApply(e.Block)
	if err != nil {
		window.endorserProducersPending = true
		ls.config.Logger.Debug(
			"cross-fork continuation audit could not resolve a block's endorser block",
			"component", "ledger",
			"slot", e.Point.Slot,
			"error", err,
		)
		return nil
	}
	if !referenced {
		return nil
	}
	rawTxs, ok := ls.config.EndorserBlockProvider(ebHash.Bytes(), ebSlot)
	if !ok {
		window.endorserProducersPending = true
		ls.config.Logger.Debug(
			"cross-fork continuation audit: certified endorser block not fetched yet",
			"component", "ledger",
			"slot", e.Point.Slot,
			"eb_slot", ebSlot,
			"eb_hash", ebHash.String(),
		)
		return nil
	}
	ids, err := endorserBlockTxIds(rawTxs)
	if err != nil {
		window.endorserProducersPending = true
		ls.config.Logger.Debug(
			"cross-fork continuation audit could not read endorser block transaction ids",
			"component", "ledger",
			"slot", e.Point.Slot,
			"eb_slot", ebSlot,
			"eb_hash", ebHash.String(),
			"error", err,
		)
		return nil
	}
	return ids
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
	}
	if counter == nil {
		return
	}
	counter.Inc()
}
