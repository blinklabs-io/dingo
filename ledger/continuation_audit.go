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
)

// The cross-fork continuation audit answers the question issue #3005 could not
// answer from the failure site: when a block reaches the ledger spending an
// input whose producer is nowhere on the local chain, which peer delivered that
// body and which fork was the node following at the time?
//
// Cost and gating. A full per-input producer probe on every fetched body would
// add a database round trip per input to the steady-state blockfetch path, so
// the audit is armed only by a local rollback (rollbackChainAndState and the
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
	// rather than toward a false report.
	if len(window.producedTxs)+len(txs) > continuationAuditMaxProducedTxs {
		ls.config.Logger.Debug(
			"disarming cross-fork continuation audit: producer set at capacity",
			"component", "ledger",
			"blocks_audited", window.blocksSeen,
			"produced_txs", len(window.producedTxs),
		)
		window.remaining = 0
		return
	}
	for _, tx := range txs {
		window.producedTxs[string(tx.Hash().Bytes())] = struct{}{}
	}
	reports := 0
	for _, tx := range txs {
		for _, input := range collectReferencedInputs(tx) {
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
				"component", "ledger",
				"peer", e.ConnectionId.String(),
				"block_slot", e.Point.Slot,
				"block_hash", hex.EncodeToString(e.Point.Hash),
				"block_prev_hash",
				e.Block.PrevHash().String(),
				"tx_hash", tx.Hash().String(),
				"input", input.String(),
				"producer_tx_hash", input.Id().String(),
				"fork_rollback_slot", window.forkPoint.Slot,
				"fork_rollback_hash",
				hex.EncodeToString(window.forkPoint.Hash),
				"fork_reason", window.forkReason,
				"fork_peer", window.forkPeer,
				"blocks_since_fork", window.blocksSeen,
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
