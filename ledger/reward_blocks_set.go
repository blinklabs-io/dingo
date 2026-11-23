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
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// rewardBlockEpochWindow is how many epochs before the newest the set keeps
// block counts and fees for: the reward round of an epoch boundary reads the
// two epochs before it, and a rollback across a boundary needs one more.
const rewardBlockEpochWindow = 2

// RewardBlockChange is one change to the per-epoch block counts and fees the
// live UTxO set keeps for reward calculation, applied in order.
type RewardBlockChange struct {
	// Slot is the slot of the commit, which a rollback past it undoes.
	Slot  uint64
	Epoch uint64
	// Open makes Epoch's counts and fees known and zero where the set has
	// neither.
	Open bool
	// Pool counts one block it minted at Slot in Epoch, unless the epoch's
	// counts are unknown or a block at or after Slot is already counted.
	Pool []byte
	// Overlay marks Pool's block as one in a slot TPraos reserves for the
	// overlay schedule, which counts in the all-slot totals only.
	Overlay bool
	// Fee adds to Epoch's fees, unless they are unknown.
	Fee uint64
	// PruneBelow, when non-zero, drops every epoch below it from the set.
	PruneBelow uint64
}

// RewardEpochBlocks is one epoch's block counts and fees as the set holds
// them. An epoch whose counts or fees the set cannot vouch for has them
// unknown, which is not the same answer as zero.
type RewardEpochBlocks struct {
	CountsKnown bool
	// Total is the blocks counted in the epoch, and Next the first slot a
	// block may still be counted at.
	Total uint64
	Next  uint64
	// Pools are the blocks each pool minted, by pool key hash.
	Pools     map[string]uint64
	FeesKnown bool
	Fees      uint64
	// AllKnown, AllTotal, AllNext and AllPools are Known, Total, Next and
	// Pools with the slots TPraos reserves for the overlay schedule counted
	// too, as the epoch-boundary capture counts.
	AllKnown bool
	AllTotal uint64
	AllNext  uint64
	AllPools map[string]uint64
}

// apply applies c to the epoch it names, as the set's store does.
func (e *RewardEpochBlocks) apply(c RewardBlockChange) {
	switch {
	case c.PruneBelow > 0:
		*e = RewardEpochBlocks{}
	case c.Open:
		if !e.CountsKnown {
			e.CountsKnown, e.Total, e.Next = true, 0, 0
			e.Pools = map[string]uint64{}
		}
		if !e.AllKnown {
			e.AllKnown, e.AllTotal, e.AllNext = true, 0, 0
			e.AllPools = map[string]uint64{}
		}
		if !e.FeesKnown {
			e.FeesKnown, e.Fees = true, 0
		}
	case len(c.Pool) > 0:
		if e.AllKnown && c.Slot >= e.AllNext && e.AllTotal != math.MaxUint64 {
			if e.AllPools == nil {
				e.AllPools = map[string]uint64{}
			}
			e.AllTotal++
			e.AllNext = c.Slot + 1
			e.AllPools[string(c.Pool)]++
		}
		if c.Overlay {
			return
		}
		if !e.CountsKnown || c.Slot < e.Next || e.Total == math.MaxUint64 {
			return
		}
		if e.Pools == nil {
			e.Pools = map[string]uint64{}
		}
		e.Total++
		e.Next = c.Slot + 1
		e.Pools[string(c.Pool)]++
	case c.Fee > 0:
		if e.FeesKnown && e.Fees <= math.MaxUint64-c.Fee {
			e.Fees += c.Fee
		}
	}
}

func cloneRewardEpochBlocks(e RewardEpochBlocks) RewardEpochBlocks {
	e.Pools = maps.Clone(e.Pools)
	e.AllPools = maps.Clone(e.AllPools)
	return e
}

// RewardBlocksReader serves reward block counts and fees from a live UTxO
// set that computes them.
type RewardBlocksReader interface {
	RewardBlocksLoaded() (bool, error)
	// RewardBlocksEpoch returns epoch's rows, as a rollback to asOf would
	// leave them when asOf is set.
	RewardBlocksEpoch(epoch uint64, asOf *uint64) (RewardEpochBlocks, error)
	// RewardBlocksEpochs lists the epochs the set holds any row of.
	RewardBlocksEpochs() ([]uint64, error)
}

// ImportedBlockEpoch is the block counts a snapshot import carried for one
// epoch, as imported, with the slot the import captured them at.
type ImportedBlockEpoch struct {
	Pools        map[string]uint64
	Total        uint64
	CapturedSlot uint64
}

// ImportedBlocksReader serves the imported block counts a live UTxO set
// retains apart from the windowed reward block rows.
type ImportedBlocksReader interface {
	ImportedBlocksLoaded() (bool, error)
	// ImportedBlocks lists the imported counts by epoch.
	ImportedBlocks() (map[uint64]ImportedBlockEpoch, error)
}

func (ls *LedgerState) rewardBlocksReader(txn ledgerdb.Txn) RewardBlocksReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(RewardBlocksReader)
	if !ok {
		return nil
	}
	return r
}

// rewardBlocksServing returns the reader when reward block reads are
// answered from the set, nil when the set does not serve them.
func (ls *LedgerState) rewardBlocksServing(
	txn ledgerdb.Txn,
) RewardBlocksReader {
	r := ls.rewardBlocksReader(txn)
	if r == nil ||
		!ls.setServes(&ls.rewardBlocksSetLoaded, r.RewardBlocksLoaded) {
		return nil
	}
	return r
}

// rewardBlocksView returns epoch's rows as txn sees the set: its committed
// rows as the lowest staged rollback leaves them, with the staged changes the
// rollbacks did not drop applied in order.
func (ls *LedgerState) rewardBlocksView(
	r RewardBlocksReader,
	txn ledgerdb.Txn,
	epoch uint64,
) (RewardEpochBlocks, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ceiling := uint64(math.MaxUint64)
	var changes []RewardBlockChange
	for _, o := range ls.stagedEpochStateOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			ceiling = min(ceiling, to)
			changes = slices.DeleteFunc(
				changes,
				func(c RewardBlockChange) bool {
					return c.Slot > to
				},
			)
		}
		changes = append(changes, o.rewardBlocks...)
	}
	var at *uint64
	if ceiling != math.MaxUint64 {
		at = &ceiling
	}
	view, err := r.RewardBlocksEpoch(epoch, at)
	if err != nil {
		return RewardEpochBlocks{}, err
	}
	view = cloneRewardEpochBlocks(view)
	for _, c := range changes {
		if c.PruneBelow > 0 && epoch >= c.PruneBelow {
			continue
		}
		if c.PruneBelow == 0 && c.Epoch != epoch {
			continue
		}
		view.apply(c)
	}
	return view, nil
}

// rewardBlockCountsFromSet answers rewardBlockCounts from the set. The
// totals count every block of the epoch, the pools only the ones asked for,
// as the metadata store does. ok is false when the set holds no counts for
// the epoch.
func (ls *LedgerState) rewardBlockCountsFromSet(
	r RewardBlocksReader,
	txn ledgerdb.Txn,
	epoch uint64,
	poolKeys []lcommon.PoolKeyHash,
) (counts map[string]uint64, total uint64, ok bool, err error) {
	view, err := ls.rewardBlocksView(r, txn, epoch)
	if err != nil || !view.CountsKnown {
		return nil, 0, false, err
	}
	counts = make(map[string]uint64, len(poolKeys))
	for _, k := range poolKeys {
		counts[string(k[:])] = view.Pools[string(k[:])]
	}
	return counts, view.Total, true, nil
}

func sameCounts(a, b map[string]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// rewardEpochFeesFor is rewardEpochFees answered from the set when the set
// serves reward block rows.
func (ls *LedgerState) rewardEpochFeesFor(
	txn ledgerdb.Txn,
	epoch models.Epoch,
) (uint64, error) {
	if epoch.LengthInSlots == 0 {
		return 0, nil
	}
	if r := ls.rewardBlocksServing(txn); r != nil {
		view, err := ls.rewardBlocksView(r, txn, epoch.EpochId)
		if err != nil {
			return 0, err
		}
		if view.FeesKnown {
			return view.Fees, nil
		}
		return 0, drainedUnserved(nodesettings.FamilyRewardBlocksFees)
	}
	return 0, drainedUnserved(nodesettings.FamilyRewardBlocksFees)
}

// rewardEpochOfSlot resolves the epoch containing slot from the published
// epoch cache, or from the candidate's own cache while txn validates a fork
// candidate that may open epochs the published one lacks. ok is false before the ledger has published a cache holding
// any epoch, when there is no epoch to count in; a cache that holds epochs but
// covers none of slot is an error.
func (ls *LedgerState) rewardEpochOfSlot(
	txn ledgerdb.Txn,
	slot uint64,
) (epoch models.Epoch, ok bool, err error) {
	// A fork candidate may open epochs the published cache does not hold yet.
	if snapshot := ls.forkValidationConsensus(txn); snapshot != nil {
		epoch, err = epochForSlotInCache(snapshot.epochCache, slot)
		return epoch, err == nil, err
	}
	if ls.loadConsensusSnapshot() == nil {
		return epoch, false, nil
	}
	epoch, err = ls.epochForSlot(slot)
	if errors.Is(err, errEpochCacheEmpty) {
		return epoch, false, nil
	}
	return epoch, err == nil, err
}

// stageBlockMinted stages the block a pool minted at slot for the set's
// per-epoch counts. A slot TPraos reserved for the overlay schedule is not a
// block the reward round counts, so it counts in the all-slot totals only.
func (ls *LedgerState) stageBlockMinted(
	txn ledgerdb.Txn,
	pool lcommon.PoolKeyHash,
	slot uint64,
) {
	if ls.rewardBlocksReader(txn) == nil {
		return
	}
	epoch, ok, err := ls.rewardEpochOfSlot(txn, slot)
	if err != nil {
		ls.failAccountStaging(txn, fmt.Errorf("resolve block epoch: %w", err))
		return
	}
	if !ok {
		return
	}
	overlay := false
	pparams := ls.ProtocolParamsForSlot(slot)
	if snapshot := ls.forkValidationConsensus(txn); snapshot != nil {
		pparams = snapshot.currentPParams
	}
	if pparams != nil {
		d := decentralizationParamRat(pparams)
		overlay = d != nil && rewardIsOverlaySlot(epoch.StartSlot, d, slot)
	}
	ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			RewardBlocks: []RewardBlockChange{{
				Slot: slot, Epoch: epoch.EpochId, Pool: bytes.Clone(pool.Bytes()),
				Overlay: overlay,
			}},
		},
	)
}

// stageEpochOpened stages the opening of a newly written epoch and the prune
// of the epochs the window no longer holds. An epoch older than the ledger's
// current one is a rewrite of history the set already has rows for.
func (ls *LedgerState) stageEpochOpened(txn ledgerdb.Txn, epoch models.Epoch) {
	if ls.rewardBlocksReader(txn) == nil {
		return
	}
	if snapshot := ls.loadConsensusSnapshot(); snapshot != nil &&
		epoch.EpochId < snapshot.currentEpoch.EpochId {
		return
	}
	changes := []RewardBlockChange{{
		Slot: epoch.StartSlot, Epoch: epoch.EpochId, Open: true,
	}}
	if epoch.EpochId > rewardBlockEpochWindow {
		changes = append(changes, RewardBlockChange{
			Slot:       epoch.StartSlot,
			PruneBelow: epoch.EpochId - rewardBlockEpochWindow,
		})
	}
	ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			RewardBlocks: changes,
		},
	)
}

// stageTransactionFee stages the fee a transaction contributes to the fee pot
// of the epoch of its execution slot: its fee when valid, its collateral when
// not. A closure transaction belongs to the epoch of its context slot and is
// undone with the block that certified it.
func (ls *LedgerState) stageTransactionFee(
	txn ledgerdb.Txn,
	tx lcommon.Transaction,
	slot uint64,
	opts database.BatchedTxIngestOpts,
) {
	if ls.rewardBlocksReader(txn) == nil {
		return
	}
	fee, err := ls.transactionFeePot(txn, tx)
	if err != nil {
		ls.failAccountStaging(txn, fmt.Errorf("resolve transaction fee: %w", err))
		return
	}
	if fee == 0 {
		return
	}
	execSlot := slot
	if opts.LedgerContextSlot != nil {
		execSlot = *opts.LedgerContextSlot
	}
	epoch, ok, err := ls.rewardEpochOfSlot(txn, execSlot)
	if err != nil {
		ls.failAccountStaging(txn, fmt.Errorf("resolve fee epoch: %w", err))
		return
	}
	if !ok {
		return
	}
	ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			RewardBlocks: []RewardBlockChange{{
				Slot: slot, Epoch: epoch.EpochId, Fee: fee,
			}},
		},
	)
}

// transactionFeePot is the fee-pot contribution the metadata store records
// for tx: the declared fee of a valid transaction, the collateral fee of an
// invalid one. A collateral input the set does not hold adds nothing, as an
// input the store cannot find does.
func (ls *LedgerState) transactionFeePot(
	txn ledgerdb.Txn,
	tx lcommon.Transaction,
) (uint64, error) {
	if tx.IsValid() {
		if fee := tx.Fee(); fee != nil {
			return fee.Uint64(), nil
		}
		return 0, nil
	}
	r, _ := ls.readSet(txn).(UtxoSetReader)
	return collateralFeePot(
		tx,
		func(in lcommon.TransactionInput) (*big.Int, bool, error) {
			id := in.Id()
			cbor, found, err := ls.utxoSetGet(r, txn, id[:], in.Index())
			if err != nil || !found {
				return nil, false, err
			}
			out, err := gledger.NewTransactionOutputFromCbor(cbor)
			if err != nil {
				return nil, false, fmt.Errorf(
					"decode collateral input %x#%d: %w", id[:], in.Index(), err,
				)
			}
			return out.Amount(), true, nil
		},
	)
}

// collateralFeePot computes a phase-2-invalid transaction's collateral fee:
// the declared total collateral when there is one, else the collateral inputs
// less the collateral return. It mirrors the metadata store's computation,
// whose rows the set's totals are compared with.
func collateralFeePot(
	tx lcommon.Transaction,
	lookup func(lcommon.TransactionInput) (*big.Int, bool, error),
) (uint64, error) {
	if tx.IsValid() {
		return 0, nil
	}
	if total := tx.TotalCollateral(); total != nil && total.Sign() > 0 {
		if !total.IsUint64() {
			return 0, errors.New("total collateral exceeds uint64")
		}
		return total.Uint64(), nil
	}
	var total uint64
	seen := make(map[string]struct{})
	for _, input := range tx.Collateral() {
		key := fmt.Sprintf("%x:%d", input.Id().Bytes(), input.Index())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		amount, found, err := lookup(input)
		if err != nil {
			return 0, err
		}
		if !found {
			continue
		}
		if amount == nil || amount.Sign() < 0 || !amount.IsUint64() {
			return 0, errors.New("malformed collateral input amount")
		}
		if amount.Uint64() > math.MaxUint64-total {
			return 0, errors.New("collateral input sum overflow")
		}
		total += amount.Uint64()
	}
	if out := tx.CollateralReturn(); out != nil {
		if amount := out.Amount(); amount != nil && amount.Sign() > 0 {
			if !amount.IsUint64() || amount.Uint64() > total {
				return 0, nil
			}
			total -= amount.Uint64()
		}
	}
	return total, nil
}

// RewardBlocksFromStore reads the block counts and fees of the newest
// epochs from the metadata store, as reward calculation reads them there: the
// counts observed above a Mithril anchor merged with the snapshot's, without
// the slots TPraos reserves for the overlay schedule, and each epoch's fees
// with the imported part of the anchor's epoch.
func RewardBlocksFromStore(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) (map[uint64]RewardEpochBlocks, error) {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	all, err := ldb.GetEpochs(txn)
	if err != nil {
		return nil, fmt.Errorf("read epochs: %w", err)
	}
	var newest uint64
	for _, e := range all {
		newest = max(newest, e.EpochId)
	}
	out := make(map[uint64]RewardEpochBlocks)
	for _, e := range all {
		if e.LengthInSlots == 0 || e.EpochId+rewardBlockEpochWindow < newest {
			continue
		}
		row, err := rewardEpochBlocksFromStore(ldb, txn, e)
		if err != nil {
			return nil, fmt.Errorf("epoch %d: %w", e.EpochId, err)
		}
		out[e.EpochId] = row
	}
	return out, nil
}

func rewardEpochBlocksFromStore(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	epoch models.Epoch,
) (RewardEpochBlocks, error) {
	var row RewardEpochBlocks
	start := epoch.StartSlot
	end := start + uint64(epoch.LengthInSlots) - 1
	d, err := persistedDecentralization(ldb, epoch, txn)
	if err != nil {
		return row, err
	}
	issuers, err := ldb.GetPoolBlockIssuersInSlotRange(start, end, txn)
	if err != nil {
		return row, fmt.Errorf("get block issuers: %w", err)
	}
	counts := make(map[string]uint64)
	allCounts := make(map[string]uint64)
	var total, next, allTotal, allNext uint64
	for _, i := range issuers {
		allTotal++
		allCounts[string(i.PoolKeyHash)]++
		allNext = max(allNext, i.Slot+1)
		if rewardIsOverlaySlot(start, d, i.Slot) {
			continue
		}
		total++
		counts[string(i.PoolKeyHash)]++
		next = max(next, i.Slot+1)
	}
	// The merge adds the snapshot's counts for the pools it is handed, so it
	// is handed every pool the snapshot counted.
	imported, _, importedKnown, err := ldb.GetImportedPoolBlockCounts(
		epoch.EpochId, txn,
	)
	if err != nil {
		return row, fmt.Errorf("get imported block counts: %w", err)
	}
	if importedKnown {
		for pool := range imported {
			if _, ok := counts[pool]; !ok {
				counts[pool] = 0
			}
			if _, ok := allCounts[pool]; !ok {
				allCounts[pool] = 0
			}
		}
	}
	merged, mergedTotal, known, err := ldb.MergeImportedPoolBlockCounts(
		epoch.EpochId, start, counts, total, txn,
	)
	if err != nil {
		return row, fmt.Errorf("merge imported block counts: %w", err)
	}
	if known {
		row.CountsKnown, row.Total, row.Next = true, mergedTotal, next
		row.Pools = make(map[string]uint64, len(merged))
		for pool, n := range merged {
			if n > 0 {
				row.Pools[pool] = n
			}
		}
	}
	allMerged, allMergedTotal, allKnown, err := ldb.MergeImportedPoolBlockCounts(
		epoch.EpochId,
		start,
		allCounts,
		allTotal,
		txn,
	)
	if err != nil {
		return row, fmt.Errorf("merge imported block counts: %w", err)
	}
	if allKnown {
		row.AllKnown, row.AllTotal, row.AllNext = true, allMergedTotal, allNext
		row.AllPools = make(map[string]uint64, len(allMerged))
		for pool, n := range allMerged {
			if n > 0 {
				row.AllPools[pool] = n
			}
		}
	}
	fees, err := rewardEpochFees(ldb, txn, epoch)
	if err != nil {
		return row, fmt.Errorf("sum fees: %w", err)
	}
	row.FeesKnown, row.Fees = true, fees
	return row, nil
}

// persistedDecentralization is the decentralization parameter the epoch's
// persisted protocol parameters carry: the one block counting keeps for a
// performance epoch. An epoch with no persisted parameters or an era that
// has no such parameter has none.
func persistedDecentralization(
	ldb ledgerdb.LedgerDB,
	epoch models.Epoch,
	txn ledgerdb.Txn,
) (*big.Rat, error) {
	era := eras.GetEraById(epoch.EraId)
	if era == nil || era.DecodePParamsFunc == nil {
		return nil, nil
	}
	pparams, err := ldb.GetPParams(
		epoch.EpochId, era.Id, era.DecodePParamsFunc, txn,
	)
	if err != nil || pparams == nil {
		return nil, err
	}
	return decentralizationParamRat(pparams), nil
}
