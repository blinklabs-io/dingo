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
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/governance"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// Buffer pools for memory reuse
// Pre-allocation capacities (currently 10) may need tuning for high-throughput scenarios
var (
	ledgerDeltaPool = sync.Pool{
		New: func() any {
			return &LedgerDelta{}
		},
	}
	transactionRecordSlicePool = sync.Pool{
		New: func() any {
			// Pre-allocate with reasonable capacity and return pointer to slice
			s := make([]TransactionRecord, 0, 10)
			return &s
		},
	}
	certDepositsMapPool = sync.Pool{
		New: func() any {
			return make(map[int]uint64)
		},
	}
	ledgerDeltaBatchPool = sync.Pool{
		New: func() any {
			return &LedgerDeltaBatch{
				deltas: make([]*LedgerDelta, 0, 10),
			}
		},
	}
)

type TransactionRecord struct {
	Tx    lcommon.Transaction
	Index int
}

type LedgerDelta struct {
	Point        ocommon.Point
	BlockEraId   uint
	BlockNumber  uint64
	Transactions []TransactionRecord
	Offsets      *database.BlockIngestionResult // pre-computed CBOR offsets for this block
	txSlicePtr   *[]TransactionRecord           // store original pointer from pool
	// Speculative marks a delta of Leios endorser-block transactions (as
	// opposed to a ranking block's own transactions). Only meaningful when
	// endorser-block conflict resolution is enabled (Musashi). It selects the
	// apply-time conflict policy: speculative transactions whose inputs are
	// already spent are skipped, whereas an authoritative (non-speculative)
	// ranking-block transaction revokes a conflicting speculative spend. See
	// LedgerStateConfig.LeiosTolerateEndorserConflicts.
	Speculative bool
}

func NewLedgerDelta(
	point ocommon.Point,
	blockEraId uint,
	blockNumber uint64,
) *LedgerDelta {
	delta := ledgerDeltaPool.Get().(*LedgerDelta)
	delta.Point = point
	delta.BlockEraId = blockEraId
	delta.BlockNumber = blockNumber
	delta.Offsets = nil // Reset offsets from previous use
	delta.Speculative = false
	slicePtr := transactionRecordSlicePool.Get().(*[]TransactionRecord)
	delta.Transactions = (*slicePtr)[:0] // Reset slice
	delta.txSlicePtr = slicePtr          // Store original pointer
	return delta
}

func (d *LedgerDelta) Release() {
	// Return the transaction slice to the pool
	if d.txSlicePtr != nil {
		// Reset slice and put original pointer back to pool
		*d.txSlicePtr = (*d.txSlicePtr)[:0]
		transactionRecordSlicePool.Put(d.txSlicePtr)
		d.txSlicePtr = nil
		d.Transactions = nil
	}
	// Clear offsets to avoid retaining large memory across blocks
	d.Offsets = nil
	// Return the delta to the pool
	ledgerDeltaPool.Put(d)
}

func (d *LedgerDelta) addTransaction(
	tx lcommon.Transaction,
	index int,
) {
	// Collect transaction
	d.Transactions = append(
		d.Transactions,
		TransactionRecord{Tx: tx, Index: index},
	)
}

func (d *LedgerDelta) apply(ls *LedgerState, txn *database.Txn) error {
	// resolveConflicts enables the Musashi endorser-block conflict policy in the
	// SetTransaction error path below; it is false (and inert) on every other
	// network. appliedEbHashes collects the speculative endorser-block
	// transactions actually applied, recorded as revocable provenance after the
	// batch so a later authoritative ranking-block transaction can override them.
	resolveConflicts := ls.config.LeiosTolerateEndorserConflicts
	appliedTxs := make([]bool, len(d.Transactions))
	var appliedEbHashes [][]byte
	var knownSpeculativeTxHashes map[string]struct{}
	if d.Speculative && resolveConflicts && len(d.Transactions) > 0 {
		hashes := make([][]byte, 0, len(d.Transactions))
		for _, tr := range d.Transactions {
			hashes = append(hashes, tr.Tx.Hash().Bytes())
		}
		existing, err := ls.db.GetExistingTransactionHashes(hashes, txn)
		if err != nil {
			return fmt.Errorf(
				"check existing endorser-block transactions: %w",
				err,
			)
		}
		knownSpeculativeTxHashes = make(
			map[string]struct{},
			len(existing)+len(d.Transactions),
		)
		for _, h := range existing {
			knownSpeculativeTxHashes[string(h)] = struct{}{}
		}
	}
	for i, tr := range d.Transactions {
		if tr.Index < 0 || tr.Index > math.MaxUint32 {
			return fmt.Errorf("transaction index out of range: %d", tr.Index)
		}
		if d.Speculative && resolveConflicts {
			txHash := tr.Tx.Hash()
			if _, exists := knownSpeculativeTxHashes[string(txHash.Bytes())]; exists {
				ls.config.Logger.Debug(
					"skipped already-applied endorser-block transaction",
					"component", "ledger",
					"tx_hash", txHash.String(),
					"slot", d.Point.Slot,
				)
				continue
			}
		}

		// Extract protocol parameter updates
		updateEpoch, paramUpdates := tr.Tx.ProtocolParameterUpdates()

		// Calculate certificate deposits
		certs := tr.Tx.Certificates()
		certDeposits := certDepositsMapPool.Get().(map[int]uint64)
		// Clear the map
		for k := range certDeposits {
			delete(certDeposits, k)
		}
		for i, cert := range certs {
			deposit, err := ls.calculateCertificateDeposit(cert, d.BlockEraId)
			if err != nil {
				// Return the map to pool before returning error
				certDepositsMapPool.Put(certDeposits)
				return fmt.Errorf("calculate certificate deposit: %w", err)
			}
			certDeposits[i] = deposit
		}

		savepoint := ""
		if d.Speculative && resolveConflicts {
			savepoint = fmt.Sprintf(
				"skipped_endorser_tx_%d_%d",
				d.Point.Slot,
				i,
			)
			if err := txn.SavePoint(savepoint); err != nil {
				certDepositsMapPool.Put(certDeposits)
				return fmt.Errorf(
					"create skipped endorser-block transaction savepoint: %w",
					err,
				)
			}
		}

		setErr := ls.db.SetTransaction(
			tr.Tx,
			d.Point,
			uint32(tr.Index), //nolint:gosec
			updateEpoch,
			paramUpdates,
			certDeposits,
			d.Offsets,
			txn,
		)
		// Musashi endorser-block conflict resolution (issue #2699): the
		// prototype's successive endorser blocks carry mutually-conflicting,
		// never-confirmed transactions, so a "UTxO already spent" here is
		// expected rather than fatal. Inert on every other network.
		if setErr != nil && resolveConflicts &&
			errors.Is(setErr, types.ErrUtxoConflict) {
			if d.Speculative {
				// Best-effort: drop a speculative endorser-block transaction
				// whose input an earlier endorser block or a ranking block
				// already spent, rather than aborting the batch. Clean up any
				// staged writes from the failed attempt before treating it as
				// skipped.
				if savepoint != "" {
					if err := txn.RollbackTo(savepoint); err != nil {
						certDepositsMapPool.Put(certDeposits)
						return fmt.Errorf(
							"rollback skipped endorser-block transaction: %w",
							err,
						)
					}
				}
				if err := ls.db.CleanupSkippedEndorserTransaction(
					tr.Tx,
					txn,
				); err != nil {
					certDepositsMapPool.Put(certDeposits)
					return fmt.Errorf(
						"cleanup skipped endorser-block transaction: %w",
						err,
					)
				}
				certDepositsMapPool.Put(certDeposits)
				ls.config.Logger.Debug(
					"skipped conflicting endorser-block transaction",
					"component", "ledger",
					"tx_hash", tr.Tx.Hash().String(),
					"slot", d.Point.Slot,
				)
				continue
			}
			// Authoritative ranking-block transaction: revoke the speculative
			// endorser-block spends blocking its inputs (and their closure),
			// then retry once. If nothing revocable is found the conflict is a
			// genuine ranking-block double-spend and the original error stands.
			revoked, rerr := ls.revokeConflictingEndorserSpends(tr.Tx, txn)
			if rerr != nil {
				certDepositsMapPool.Put(certDeposits)
				return fmt.Errorf("resolve endorser-block conflict: %w", rerr)
			}
			if revoked > 0 {
				ls.config.Logger.Debug(
					"revoked speculative endorser-block spends for ranking-block transaction",
					"component", "ledger",
					"tx_hash", tr.Tx.Hash().String(),
					"slot", d.Point.Slot,
					"revoked", revoked,
				)
				setErr = ls.db.SetTransaction(
					tr.Tx,
					d.Point,
					uint32(tr.Index), //nolint:gosec
					updateEpoch,
					paramUpdates,
					certDeposits,
					d.Offsets,
					txn,
				)
			}
		}
		// Return the map to pool
		certDepositsMapPool.Put(certDeposits)
		if setErr != nil {
			return fmt.Errorf("record transaction: %w", setErr)
		}
		appliedTxs[i] = true

		// Record a successfully-applied speculative endorser-block transaction
		// as revocable provenance (Musashi only), so a later authoritative
		// ranking-block transaction can override it.
		if d.Speculative && resolveConflicts {
			txHash := tr.Tx.Hash().Bytes()
			appliedEbHashes = append(appliedEbHashes, txHash)
			knownSpeculativeTxHashes[string(txHash)] = struct{}{}
		}

		// Process governance proposals and votes for valid Conway-era transactions
		if tr.Tx.IsValid() {
			if err := d.processGovernance(ls, tr.Tx, txn); err != nil {
				return fmt.Errorf("process governance: %w", err)
			}
		}
	}

	// Record the applied speculative endorser-block transactions as revocable
	// provenance for later ranking-block conflict resolution (Musashi only).
	if len(appliedEbHashes) > 0 {
		if err := ls.db.AddEndorserTransactions(
			appliedEbHashes,
			d.Point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf("record endorser transaction provenance: %w", err)
		}
	}

	// Accumulate Conway treasury donations from this block. Donations move
	// into the treasury at the next epoch boundary (see processEpochRollover);
	// they are recorded here keyed by block slot so a rollback drops them.
	// Only valid transactions contribute: an invalid (phase-2 failed)
	// transaction consumes collateral and its body, including any donation,
	// is not applied.
	var donation uint64
	for i, tr := range d.Transactions {
		if !appliedTxs[i] {
			continue
		}
		if !tr.Tx.IsValid() {
			continue
		}
		if don := tr.Tx.Donation(); don != nil && don.Sign() > 0 {
			donation += don.Uint64()
		}
	}
	if donation > 0 {
		ls.RLock()
		epoch := ls.currentEpoch.EpochId
		ls.RUnlock()
		if err := ls.db.Metadata().AddNetworkDonation(
			d.Point.Slot,
			epoch,
			donation,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("record network donation: %w", err)
		}
	}

	// Emit transaction events only after all processing succeeds,
	// so subscribers never see an "applied" event for a transaction
	// whose governance processing failed and caused the apply to abort.
	if ls.config.EventBus != nil {
		for i, tr := range d.Transactions {
			if !appliedTxs[i] {
				continue
			}
			ls.config.EventBus.PublishAsync(
				TransactionEventType,
				event.NewEvent(
					TransactionEventType,
					TransactionEvent{
						Transaction: tr.Tx,
						Point:       d.Point,
						BlockNumber: d.BlockNumber,
						TxIndex:     uint32(tr.Index), //nolint:gosec
						Rollback:    false,
					},
				),
			)
		}
	}

	return nil
}

// processGovernance handles governance proposals and votes from a transaction.
// This is called during delta application for valid Conway-era transactions.
// Proposals and votes are only present in Conway-era transactions, so this is
// a no-op for pre-Conway eras.
func (d *LedgerDelta) processGovernance(
	ls *LedgerState,
	tx lcommon.Transaction,
	txn *database.Txn,
) error {
	proposals := tx.ProposalProcedures()
	votes := tx.VotingProcedures()

	// Early return if no governance data to process
	if len(proposals) == 0 && len(votes) == 0 {
		return nil
	}

	// Determine current epoch and Conway protocol parameters.
	// These are needed for both proposals (govActionLifetime) and
	// votes (dRepInactivityPeriod for activity tracking).
	ls.RLock()
	currentEpoch := ls.currentEpoch.EpochId
	pparams := ls.currentPParams
	ls.RUnlock()

	conwayPParams, ok := pparams.(*conway.ConwayProtocolParameters)
	if !ok {
		return fmt.Errorf(
			"governance requires Conway protocol parameters, got %T",
			pparams,
		)
	}

	// Process governance proposals
	if len(proposals) > 0 {
		if err := governance.ProcessProposals(
			tx,
			d.Point,
			currentEpoch,
			conwayPParams.GovActionValidityPeriod,
			ls.db,
			txn,
		); err != nil {
			return fmt.Errorf("process governance proposals: %w", err)
		}
	}

	// Process governance votes
	if len(votes) > 0 {
		if err := governance.ProcessVotes(
			tx,
			d.Point,
			currentEpoch,
			conwayPParams.DRepInactivityPeriod,
			ls.db,
			txn,
		); err != nil {
			return fmt.Errorf("process governance votes: %w", err)
		}
	}

	return nil
}

type LedgerDeltaBatch struct {
	deltas []*LedgerDelta
}

func NewLedgerDeltaBatch() *LedgerDeltaBatch {
	batch := ledgerDeltaBatchPool.Get().(*LedgerDeltaBatch)
	batch.deltas = batch.deltas[:0] // Reset slice
	return batch
}

func (b *LedgerDeltaBatch) Release() {
	// Release all individual deltas back to their pools
	for i, delta := range b.deltas {
		if delta != nil {
			delta.Release()
			b.deltas[i] = nil // Avoid double-release
		}
	}
	// Clear the batch slice
	b.deltas = b.deltas[:0]
	// Return the batch to the pool
	ledgerDeltaBatchPool.Put(b)
}

func (b *LedgerDeltaBatch) addDelta(delta *LedgerDelta) {
	b.deltas = append(b.deltas, delta)
}

func (b *LedgerDeltaBatch) apply(ls *LedgerState, txn *database.Txn) error {
	for _, delta := range b.deltas {
		if delta == nil {
			continue // Skip nil deltas (shouldn't happen in normal operation)
		}
		err := delta.apply(ls, txn)
		if err != nil {
			return err
		}
	}
	return nil
}
