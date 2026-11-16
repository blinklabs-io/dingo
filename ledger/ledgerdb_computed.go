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
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const (
	computedEpoch           = "epoch"
	computedEpochSummary    = "epoch_summary"
	computedPParamEnactment = "pparam_enactment"
)

// verifyGuardStaged is verifyGuard for a write that stages its result and
// then compares it: txn's batch exists before the guard is taken, so
// staging does not itself read as a write racing the comparison.
func (ls *LedgerState) verifyGuardStaged(txn ledgerdb.Txn) verifyReadGuard {
	if txn != nil && ls.verifying() {
		ls.utxoSetBatchForTxn(txn, func(*utxoSetBatch) {})
	}
	return ls.verifyGuard(txn)
}

// compareComputed counts a result the set computed against what the metadata
// store wrote, in verify mode only.
func (ls *LedgerState) compareComputed(
	g verifyReadGuard,
	result string,
	same bool,
	detail error,
) {
	if !ls.verifying() {
		return
	}
	if detail == nil && !same && ls.verifyReadRaced(g) {
		return
	}
	ls.metrics.setComputedVerified.WithLabelValues(result).Inc()
	if same && detail == nil {
		return
	}
	n := ls.setComputedMismatches.Add(1)
	ls.metrics.setComputedMismatches.WithLabelValues(result).Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set computed result differs from the metadata store",
		"component", "ledger",
		"result", result,
		"error", detail,
		"mismatches", n,
	)
}

// sameEpochRow compares epoch rows apart from their store row IDs and the
// nil-or-empty form of a nonce.
func sameEpochRow(a, b models.Epoch) bool {
	return a.EpochId == b.EpochId && a.StartSlot == b.StartSlot &&
		a.EraId == b.EraId && a.SlotLength == b.SlotLength &&
		a.LengthInSlots == b.LengthInSlots &&
		bytes.Equal(a.Nonce, b.Nonce) &&
		bytes.Equal(a.EvolvingNonce, b.EvolvingNonce) &&
		bytes.Equal(a.CandidateNonce, b.CandidateNonce) &&
		bytes.Equal(a.LastEpochBlockNonce, b.LastEpochBlockNonce)
}

// pparamEnactmentReader returns the set's proposal reader when the set holds
// every proposal, so the enactment can be computed from it.
func (ls *LedgerState) pparamEnactmentReader(
	txn ledgerdb.Txn,
) PParamUpdateReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(PParamUpdateReader)
	if !ok {
		return nil
	}
	if !ls.pparamUpdatesSetLoaded.Load() {
		loaded, err := r.PParamUpdatesLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.pparamUpdatesSetLoaded.Store(true)
	}
	return r
}

// setEpochGetter reads epoch records from the set when it holds them, and
// through the LedgerDB otherwise.
func (s stagingLedgerDB) setEpochGetter(
	txn ledgerdb.Txn,
) func(uint64) (*models.Epoch, error) {
	er, _ := s.ls.epochStateReader(txn)
	if er == nil {
		return func(id uint64) (*models.Epoch, error) {
			return s.GetEpoch(id, txn)
		}
	}
	return func(id uint64) (*models.Epoch, error) {
		view, err := s.ls.epochStateView(er, txn)
		if err != nil {
			return nil, err
		}
		e, ok := view[id]
		if !ok {
			return nil, nil
		}
		return &e, nil
	}
}

// computeEnactment computes the update the boundary into epoch enacts from
// the set's proposals and epoch records, with the function the metadata
// store runs. It works on a copy of currentPParams: the era's update
// function mutates its argument, and the metadata store's write takes the
// caller's value.
func (s stagingLedgerDB) computeEnactment(
	r PParamUpdateReader,
	txn ledgerdb.Txn,
	epoch uint64,
	era uint,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(
		lcommon.ProtocolParameters,
		any,
	) (lcommon.ProtocolParameters, error),
	hasPlutusV2CostModelFunc func(any) bool,
) (*database.ClassicPParamEnactment, error) {
	if epoch == 0 {
		return nil, nil
	}
	eraDesc := eras.GetEraById(era)
	if eraDesc == nil {
		return nil, fmt.Errorf("unknown era %d", era)
	}
	owned, err := cloneProtocolParametersForEra(*eraDesc, currentPParams)
	if err != nil {
		return nil, fmt.Errorf("copy pparams for enactment: %w", err)
	}
	if owned == nil {
		// Without parameters in force there is nothing to apply an update to.
		return nil, nil
	}
	rows, err := s.ls.pparamUpdateView(r, txn, epoch-1)
	if err != nil {
		return nil, err
	}
	// The proposals' order within a slot is the order stored.
	for i := range rows {
		rows[i].ID = uint(i) + 1
	}
	return database.ComputeClassicPParamEnactmentFromRows(
		rows, epoch, quorum, owned,
		decodeFunc, updateFunc, hasPlutusV2CostModelFunc,
		s.setEpochGetter(txn),
	)
}

// sameEnactment compares the set's enactment with the rows and result the
// metadata store's write produced.
func sameEnactment(
	computed *database.ClassicPParamEnactment,
	written []models.PParams,
	plutusV2 bool,
	slot, epoch uint64,
	era uint,
) bool {
	if computed == nil {
		return len(written) == 0 && !plutusV2
	}
	if len(written) != 1 || plutusV2 != computed.PlutusV2CostModelWritten {
		return false
	}
	w := written[0]
	return bytes.Equal(w.Cbor, computed.Cbor) &&
		w.AddedSlot == slot && w.Epoch == epoch && w.EraId == era
}

// ComputeAndApplyPParamUpdates writes the enactment to the metadata store
// and stages the rows the set computed from its own proposals. Where the set
// does not hold every proposal, it stages the rows the store wrote. Verify
// mode compares the two and returns the store's result; set mode returns the
// set's.
func (s stagingLedgerDB) ComputeAndApplyPParamUpdates(
	ctx context.Context,
	slot, epoch uint64,
	era uint,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(
		lcommon.ProtocolParameters,
		any,
	) (lcommon.ProtocolParameters, error),
	hasPlutusV2CostModelFunc func(any) bool,
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, bool, error) {
	drained := s.pparamsDrained()
	r := s.ls.pparamEnactmentReader(txn)
	if drained && r == nil {
		return nil, false, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	verifying := s.ls.verifying() && !drained
	var before []models.PParams
	if r == nil || verifying {
		var err error
		before, err = s.LedgerDB.ListPParamsForEra(era, txn)
		if err != nil {
			return nil, false, fmt.Errorf("read pparams before update: %w", err)
		}
	}
	g := s.ls.verifyGuardStaged(txn) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	var (
		computed *database.ClassicPParamEnactment
		setErr   error
	)
	if r != nil {
		computed, setErr = s.computeEnactment(
			r, txn, epoch, era, quorum, currentPParams,
			decodeFunc, updateFunc, hasPlutusV2CostModelFunc,
		)
	}
	var (
		res lcommon.ProtocolParameters
		cm  bool
		err error
	)
	if !drained {
		res, cm, err = s.LedgerDB.ComputeAndApplyPParamUpdates(
			ctx, slot, epoch, era, quorum, currentPParams,
			decodeFunc, updateFunc, hasPlutusV2CostModelFunc, txn,
		)
		if err != nil {
			return res, cm, err
		}
	}
	var written []models.PParams
	if r == nil || verifying {
		after, err := s.LedgerDB.ListPParamsForEra(era, txn)
		if err != nil {
			return nil, false, fmt.Errorf("read pparams after update: %w", err)
		}
		if len(after) > len(before) {
			written = slices.Clone(after[len(before):])
		}
	}
	if r != nil && verifying {
		s.ls.compareComputed(
			g, computedPParamEnactment,
			setErr == nil &&
				sameEnactment(computed, written, cm, slot, epoch, era),
			setErr,
		)
	}
	_, mode := s.ls.utxoSetReader()
	if r != nil && setErr != nil && (mode == UtxoSetReadsSet || drained) {
		return nil, false, fmt.Errorf("compute pparam enactment: %w", setErr)
	}
	switch {
	case r != nil && setErr == nil:
		if computed != nil {
			s.ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
				PParams: []models.PParams{{
					Cbor: slices.Clone(computed.Cbor), AddedSlot: slot,
					Epoch: epoch, EraId: era,
				}},
			})
		}
	case len(written) > 0:
		s.ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			PParams: written,
		})
	}
	// The enactment reads the proposals of the epoch before its own, which
	// it writes nothing to, so reading them back gives what it selected from.
	if !drained {
		s.verifyPParamUpdates(g, txn, epoch)
	}
	if r != nil && setErr == nil && (mode == UtxoSetReadsSet || drained) {
		if computed == nil {
			return currentPParams, false, nil
		}
		return computed.PParams, computed.PlutusV2CostModelWritten, nil
	}
	return res, cm, nil
}

// SaveEpochSummary writes the summary to the metadata store and stages the
// row the upsert leaves: the caller's, with snapshot_ready kept once set.
// The prior row comes from the set; where the set does not hold the
// summaries, the stored row is read back.
func (s stagingLedgerDB) SaveEpochSummary(
	summary *models.EpochSummary,
	txn ledgerdb.Txn,
) error {
	drained := s.ls.familyDrained(nodesettings.FamilySummaries)
	er, sr, mode := s.ls.epochSummaryReader(txn)
	if er == nil || sr == nil {
		if drained {
			return drainedUnserved(nodesettings.FamilySummaries)
		}
		return s.saveEpochSummaryReadBack(summary, txn)
	}
	g := s.ls.verifyGuardStaged(txn)
	s.ls.utxoSetApplyMu.RLock()
	prior, setErr := s.ls.summaryViewLocked(er, sr, txn, summary.Epoch)
	s.ls.utxoSetApplyMu.RUnlock()
	if setErr != nil {
		if mode == UtxoSetReadsSet || drained {
			return fmt.Errorf("read epoch summary %d from the set: %w",
				summary.Epoch, setErr)
		}
		return s.saveEpochSummaryReadBack(summary, txn)
	}
	if !drained {
		if err := s.LedgerDB.SaveEpochSummary(summary, txn); err != nil {
			return err
		}
	}
	computed := *summary
	computed.EpochNonce = slices.Clone(summary.EpochNonce)
	computed.SnapshotReady = summary.SnapshotReady ||
		prior != nil && prior.SnapshotReady
	if s.ls.verifying() && !drained {
		row, err := s.LedgerDB.GetEpochSummary(summary.Epoch, txn)
		s.ls.compareComputed(
			g, computedEpochSummary,
			err == nil && sameSummaryPtr(&computed, row), err,
		)
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		Summaries: []models.EpochSummary{computed},
	})
	return nil
}

// saveEpochSummaryReadBack stages the row as the metadata store holds it
// after the upsert.
func (s stagingLedgerDB) saveEpochSummaryReadBack(
	summary *models.EpochSummary,
	txn ledgerdb.Txn,
) error {
	if err := s.LedgerDB.SaveEpochSummary(summary, txn); err != nil {
		return err
	}
	row, err := s.LedgerDB.GetEpochSummary(summary.Epoch, txn)
	if err != nil {
		return fmt.Errorf("read back epoch summary %d: %w", summary.Epoch, err)
	}
	if row == nil {
		return fmt.Errorf(
			"read back epoch summary %d: not found",
			summary.Epoch,
		)
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		Summaries: []models.EpochSummary{*row},
	})
	return nil
}

// SetEpoch writes the epoch to the metadata store and stages the row the
// upsert leaves, which is its arguments: every column is replaced.
func (s stagingLedgerDB) SetEpoch(
	slot, epoch uint64,
	nonce, evolvingNonce, candidateNonce, lastEpochBlockNonce []byte,
	era, slotLength, lengthInSlots uint,
	txn ledgerdb.Txn,
) error {
	g := s.ls.verifyGuardStaged(txn)
	drained := s.ls.familyDrained(nodesettings.FamilyEpochs)
	if !drained {
		if err := s.LedgerDB.SetEpoch(
			slot, epoch, nonce, evolvingNonce, candidateNonce,
			lastEpochBlockNonce, era, slotLength, lengthInSlots, txn,
		); err != nil {
			return err
		}
	}
	computed := models.Epoch{
		Nonce:               slices.Clone(nonce),
		EvolvingNonce:       slices.Clone(evolvingNonce),
		CandidateNonce:      slices.Clone(candidateNonce),
		LastEpochBlockNonce: slices.Clone(lastEpochBlockNonce),
		EpochId:             epoch,
		StartSlot:           slot,
		EraId:               era,
		SlotLength:          slotLength,
		LengthInSlots:       lengthInSlots,
	}
	if s.ls.verifying() && !drained {
		row, err := s.LedgerDB.GetEpoch(epoch, txn)
		s.ls.compareComputed(
			g, computedEpoch,
			err == nil && row != nil && sameEpochRow(computed, *row), err,
		)
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		Epochs: []models.Epoch{computed},
	})
	s.ls.stageEpochOpened(txn, computed) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}
