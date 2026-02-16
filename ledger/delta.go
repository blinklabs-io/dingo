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
	"fmt"
	"math"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
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
	for _, tr := range d.Transactions {
		if tr.Index < 0 || tr.Index > math.MaxUint32 {
			return fmt.Errorf("transaction index out of range: %d", tr.Index)
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

		err := ls.db.SetTransaction(
			tr.Tx,
			d.Point,
			uint32(tr.Index), //nolint:gosec
			updateEpoch,
			paramUpdates,
			certDeposits,
			d.Offsets,
			txn,
		)
		// Return the map to pool
		certDepositsMapPool.Put(certDeposits)
		if err != nil {
			return fmt.Errorf("record transaction: %w", err)
		}

		// Process governance proposals and votes for valid Conway-era transactions
		if tr.Tx.IsValid() {
			if err := d.processGovernance(ls, tr.Tx, txn); err != nil {
				return fmt.Errorf("process governance: %w", err)
			}
		}
	}

	// Emit transaction events only after all processing succeeds,
	// so subscribers never see an "applied" event for a transaction
	// whose governance processing failed and caused the apply to abort.
	if ls.config.EventBus != nil {
		for _, tr := range d.Transactions {
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
		if err := processGovernanceProposals(
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
		if err := processGovernanceVotes(
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
