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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
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
	donation     uint64
	txSlicePtr   *[]TransactionRecord // store original pointer from pool
	// skipConsumedInputRecovery applies transaction effects without the
	// consumed-utxo recovery/repair pass (see Database.SetTransactionWithOpts).
	// Set only for the Leios Musashi endorser-block apply, which mirrors the
	// reference ledger's ValidateNone closure apply; false for all ranking-block
	// deltas so their behavior is unchanged.
	skipConsumedInputRecovery bool
	// strictConsumedInputs refuses to recover an absent consumed-input producer
	// from the blob store and treats it as a hard error instead (issue #3005).
	// Set only when the block is applied in the steady-state, at-tip, validated
	// context, where every consumed input's producer must already be applied and
	// live. See BatchedTxIngestOpts.StrictAppliedInputConservation.
	strictConsumedInputs bool
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
	delta.donation = 0
	delta.skipConsumedInputRecovery = false
	delta.strictConsumedInputs = false
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
	d.donation = 0
	d.skipConsumedInputRecovery = false
	d.strictConsumedInputs = false
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
	return d.applyWithDonationRecording(ls, txn, true)
}

func (d *LedgerDelta) applyWithoutRecordingDonations(
	ls *LedgerState,
	txn *database.Txn,
) error {
	return d.applyWithDonationRecording(ls, txn, false)
}

func (d *LedgerDelta) applyWithDonationRecording(
	ls *LedgerState,
	txn *database.Txn,
	recordDonations bool,
) error {
	// Keep one immutable protocol-parameter snapshot for every certificate in
	// this delta. A parameter publication between certificates must not mix
	// deposit values in one database operation. Load it lazily because
	// certificate-free validation deltas may run before snapshots are
	// initialized during startup.
	var pparams lcommon.ProtocolParameters
	var snapshotLoaded bool
	appliedTxs := make([]bool, len(d.Transactions))
	for i, tr := range d.Transactions {
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
		if len(certs) > 0 && !snapshotLoaded {
			snapshot := ls.loadConsensusSnapshot()
			if snapshot == nil {
				certDepositsMapPool.Put(certDeposits)
				return errors.New(
					"calculate certificate deposit: consensus snapshot unavailable",
				)
			}
			pparams = snapshot.currentPParams
			snapshotLoaded = true
		}
		for i, cert := range certs {
			deposit, err := ls.calculateCertificateDeposit(
				cert,
				d.BlockEraId,
				pparams,
			)
			if err != nil {
				// Return the map to pool before returning error
				certDepositsMapPool.Put(certDeposits)
				return fmt.Errorf("calculate certificate deposit: %w", err)
			}
			certDeposits[i] = deposit
		}

		setErr := ls.db.SetTransactionWithOpts(
			tr.Tx,
			d.Point,
			uint32(tr.Index), //nolint:gosec
			updateEpoch,
			paramUpdates,
			certDeposits,
			d.Offsets,
			txn,
			database.BatchedTxIngestOpts{
				SkipConsumedInputRecovery:      d.skipConsumedInputRecovery,
				StrictAppliedInputConservation: d.strictConsumedInputs,
				SkipWithdrawalWitnessWrite:     !ls.config.DelegatorInactivityEnabled,
			},
		)
		// Return the map to pool
		certDepositsMapPool.Put(certDeposits)
		if setErr != nil {
			if errors.Is(setErr, models.ErrRewardWithdrawalExceedsBalance) {
				return &txValidationError{
					BlockPoint: d.Point,
					TxHash:     append([]byte(nil), tr.Tx.Hash().Bytes()...),
					Inputs:     collectReferencedInputs(tr.Tx),
					Cause:      setErr,
				}
			}
			return fmt.Errorf("record transaction: %w", setErr)
		}
		appliedTxs[i] = true

		// Process governance proposals and votes for valid Conway-era transactions
		if tr.Tx.IsValid() {
			if err := d.processGovernance(ls, tr.Tx, txn); err != nil {
				return fmt.Errorf("process governance: %w", err)
			}
		}
	}

	// CIP-0163: renew reward-account expirations for the credentials witnessed
	// by this block's transactions. This runs after every transaction's effects
	// (including stake-key registrations that create account rows) have been
	// written to the same DB transaction above, so a credential registered by
	// this block already has a row for RenewAccountExpirations to update, and
	// the renewal commits or rolls back atomically with the block. It is a
	// no-op when the delegator-inactivity gate is off. The renewal is idempotent
	// and monotonic, so applying it per delta (once per block outside
	// validation, once per transaction while validating, where each tx is its
	// own delta) yields the same final expiration as applying it once per block.
	if ls.config.DelegatorInactivityEnabled {
		ls.RLock()
		currentEpoch := ls.currentEpoch.EpochId
		ls.RUnlock()
		witnessTxs := make([]lcommon.Transaction, 0, len(d.Transactions))
		for i, tr := range d.Transactions {
			if !appliedTxs[i] {
				continue
			}
			witnessTxs = append(witnessTxs, tr.Tx)
		}
		if err := ls.renewWitnessedAccountExpirations(
			txn,
			currentEpoch,
			witnessTxs,
		); err != nil {
			return fmt.Errorf("renew witnessed account expirations: %w", err)
		}
	}

	if recordDonations {
		if err := d.recordNetworkDonations(ls, txn, appliedTxs); err != nil {
			return err
		}
	} else {
		if err := d.accumulateNetworkDonations(appliedTxs); err != nil {
			return err
		}
	}

	// Stage transaction events only after all delta processing succeeds, then
	// publish them once the database transaction commits durably. A later delta
	// failure, rollback, or commit failure discards the callback, so subscribers
	// never derive state from an Apply that did not persist. AfterCommit runs
	// callbacks in registration order, and this callback walks transactions in
	// index order before handing each event to the ledger.tx ordered lane. See
	// publishTransactionEvent.
	applyEvents := make([]TransactionEvent, 0, len(d.Transactions))
	for i, tr := range d.Transactions {
		if !appliedTxs[i] {
			continue
		}
		applyEvents = append(applyEvents, TransactionEvent{
			Transaction: tr.Tx,
			Point:       d.Point,
			BlockNumber: d.BlockNumber,
			TxIndex:     uint32(tr.Index), //nolint:gosec
			Rollback:    false,
		})
	}
	if len(applyEvents) > 0 {
		txn.AfterCommit(func() {
			if ls.beforeTransactionApplyPublish != nil {
				ls.beforeTransactionApplyPublish()
			}
			for _, evt := range applyEvents {
				ls.publishTransactionEvent(evt)
			}
		})
	}

	return nil
}

// addUint64 returns a+b, or an error instead of wrapping when the sum
// would overflow uint64. Treasury donation and block-donation accumulators
// must fail closed on a corrupt or adversarial value rather than silently
// wrap the recorded amount.
func addUint64(a, b uint64) (uint64, error) {
	if b > ^uint64(0)-a {
		return 0, fmt.Errorf("donation sum overflows uint64: %d + %d", a, b)
	}
	return a + b, nil
}

func (d *LedgerDelta) donate(amount uint64) error {
	sum, err := addUint64(d.donation, amount)
	if err != nil {
		return fmt.Errorf("accumulate donation: %w", err)
	}
	d.donation = sum
	return nil
}

func (d *LedgerDelta) accumulateNetworkDonations(appliedTxs []bool) error {
	// Accumulate Conway treasury donations from this block. Donations move
	// into the treasury at the next epoch boundary (see processEpochRollover);
	// they are recorded here keyed by block slot so a rollback drops them.
	// Only valid transactions contribute: an invalid (phase-2 failed)
	// transaction consumes collateral and its body, including any donation,
	// is not applied.
	var donation uint64
	for i, tr := range d.Transactions {
		if appliedTxs != nil && !appliedTxs[i] {
			continue
		}
		if !tr.Tx.IsValid() {
			continue
		}
		don := tr.Tx.Donation()
		if don == nil || don.Sign() <= 0 {
			continue
		}
		if !don.IsUint64() {
			return fmt.Errorf(
				"treasury donation exceeds uint64 range: %s",
				don.String(),
			)
		}
		var err error
		donation, err = addUint64(donation, don.Uint64())
		if err != nil {
			return fmt.Errorf("accumulate treasury donation: %w", err)
		}
	}
	return d.donate(donation)
}

func (d *LedgerDelta) recordNetworkDonations(
	ls *LedgerState,
	txn *database.Txn,
	appliedTxs []bool,
) error {
	// Accumulate Conway treasury donations from this block. Donations move
	// into the treasury at the next epoch boundary (see processEpochRollover);
	// they are recorded here keyed by block slot so a rollback drops them.
	// Only valid transactions contribute: an invalid (phase-2 failed)
	// transaction consumes collateral and its body, including any donation,
	// is not applied.
	if err := d.accumulateNetworkDonations(appliedTxs); err != nil {
		return err
	}
	donation := d.donation
	if donation == 0 {
		return nil
	}

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

	return nil
}

// processGovernance handles governance proposals, votes, and DRep activity
// certificates from a transaction.
// This is called during delta application for valid Conway-era transactions.
// These items are only present in Conway-era transactions, so this is a no-op
// for pre-Conway eras.
func (d *LedgerDelta) processGovernance(
	ls *LedgerState,
	tx lcommon.Transaction,
	txn *database.Txn,
) error {
	proposals := tx.ProposalProcedures()
	votes := tx.VotingProcedures()
	hasDRepActivityCerts := governance.HasDRepActivityCertificates(tx)

	// Early return if no governance data to process
	if len(proposals) == 0 && len(votes) == 0 && !hasDRepActivityCerts {
		return nil
	}

	// Determine current epoch and Conway protocol parameters.
	// These are needed for both proposals (govActionLifetime) and
	// votes (dRepInactivityPeriod for activity tracking).
	ls.RLock()
	currentEpoch := ls.currentEpoch.EpochId
	pparams := ls.currentPParams
	ls.RUnlock()

	conwayPParams := conwayProtocolParameters(pparams)
	if conwayPParams == nil {
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

	if hasDRepActivityCerts {
		if err := governance.ProcessDRepActivityCertificates(
			tx,
			currentEpoch,
			conwayPParams.DRepInactivityPeriod,
			ls.db,
			txn,
		); err != nil {
			return fmt.Errorf("process DRep activity certificates: %w", err)
		}
	}

	return nil
}

func conwayProtocolParameters(
	pparams lcommon.ProtocolParameters,
) *conway.ConwayProtocolParameters {
	switch p := pparams.(type) {
	case *conway.ConwayProtocolParameters:
		return p
	case *dijkstra.DijkstraProtocolParameters:
		if p == nil {
			return nil
		}
		return &p.ConwayProtocolParameters
	default:
		return nil
	}
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
