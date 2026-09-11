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
	"sync"

	"github.com/blinklabs-io/dingo/database"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
const (
	// continuationAuditBlockBudget bounds how many fetched blocks a single
	// arming inspects. Each local rollback re-arms, so a node churning at the
	// tip keeps the audit live while a healthy node pays nothing.
	continuationAuditBlockBudget = 512
	// continuationAuditMaxProducedTxs bounds the in-window producer set so a
	// long run cannot grow memory without limit. Exceeding it disarms the
	// window rather than risking false reports from a truncated set.
	continuationAuditMaxProducedTxs = 100_000
	// continuationAuditMaxReportsPerBlock caps log volume for a body with
	// many unresolvable inputs. The first few identify the fork just as well.
	continuationAuditMaxReportsPerBlock = 4
	// continuationAuditMaxInputsPerBlock bounds diagnostic database work while
	// the chainsync/blockfetch pipeline mutex is held. The audit is diagnostic,
	// so truncating a pathological block is preferable to delaying the
	// blockfetch pipeline behind unbounded per-input probes.
	continuationAuditMaxInputsPerBlock = 32
)

// continuationAuditWindow is the state of one armed audit run. It is published
// through an atomic pointer, and only the blockfetch handler mutates its
// counters, so remaining and blocksSeen need no further synchronization:
// arming always installs a freshly allocated window.
//
// The producer set is the exception, and producedTxsMutex is why. A rearm
// carries the outgoing window's producers forward (see armContinuationAudit),
// and it runs on the chainsync dispatch goroutine under chainsyncMutex while
// the blockfetch goroutine is still recording producers into that same window
// under chainsyncBlockfetchMutex. Neither of those locks covers both accesses,
// and a concurrent map iteration and write is fatal at runtime -- the same
// hazard bufferedHeaderMutex exists for. Reach producedTxs only through the
// methods below; the mutex is a leaf, taken around map access alone.
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
}

// recordProducers records every transaction in a body as an in-window
// producer at slot, and reports the resulting producer count. It records
// nothing and reports false when the body would take the set past
// continuationAuditMaxProducedTxs.
func (w *continuationAuditWindow) recordProducers(
	txs []lcommon.Transaction,
	slot uint64,
) (int, bool) {
	w.producedTxsMutex.Lock()
	defer w.producedTxsMutex.Unlock()
	if len(w.producedTxs)+len(txs) > continuationAuditMaxProducedTxs {
		return len(w.producedTxs), false
	}
	for _, tx := range txs {
		w.producedTxs[string(tx.Hash().Bytes())] = slot
	}
	return len(w.producedTxs), true
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
	producedTxs := make(map[string]uint64)
	if prior := ls.continuationAudit.Load(); prior != nil {
		if carried := ls.carryForwardProducedTxs(prior, point); carried != nil {
			producedTxs = carried
		}
	}
	ls.continuationAudit.Store(&continuationAuditWindow{
		producedTxs: producedTxs,
		forkPoint: ocommon.Point{
			Slot: point.Slot,
			Hash: append([]byte(nil), point.Hash...),
		},
		forkReason: reason,
		forkPeer:   forkPeer,
		remaining:  continuationAuditBlockBudget,
	})
}

// carryForwardProducedTxs returns the producers prior recorded from blocks at
// or below point, and nil when prior's fork point is no longer resolvable on
// the primary chain at its recorded slot. See armContinuationAudit for why
// those two conditions are what make a carried-forward producer legitimate.
func (ls *LedgerState) carryForwardProducedTxs(
	prior *continuationAuditWindow,
	point ocommon.Point,
) map[string]uint64 {
	stillOnChain, err := ls.blockByHash(prior.forkPoint.Hash)
	if err != nil || stillOnChain.Slot != prior.forkPoint.Slot {
		return nil
	}
	return prior.producersAtOrBelow(point.Slot)
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
	// rather than toward a false report.
	producedTxs, recorded := window.recordProducers(txs, e.Point.Slot)
	if !recorded {
		ls.config.Logger.Debug(
			"disarming cross-fork continuation audit: producer set at capacity",
			"component", "ledger",
			"blocks_audited", window.blocksSeen,
			"produced_txs", producedTxs,
		)
		window.remaining = 0
		return
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
				continue
			}
			reports++
			ls.metrics.continuationInputUnresolved.Inc()
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
