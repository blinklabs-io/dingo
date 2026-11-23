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
// function mutates its argument.
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

// ComputeAndApplyPParamUpdates stages the rows the set computed from its own
// proposals and returns the set's result. A set that does not hold every
// proposal cannot enact.
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
	r := s.ls.pparamEnactmentReader(txn)
	if r == nil {
		return nil, false, s.ls.setFamilyUnserved(nodesettings.FamilyPParamsUpdates)
	}
	computed, err := s.computeEnactment(
		r, txn, epoch, era, quorum, currentPParams,
		decodeFunc, updateFunc, hasPlutusV2CostModelFunc,
	)
	if err != nil {
		return nil, false, fmt.Errorf("compute pparam enactment: %w", err)
	}
	if computed == nil {
		return currentPParams, false, nil
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		PParams: []models.PParams{{
			Cbor: slices.Clone(computed.Cbor), AddedSlot: slot,
			Epoch: epoch, EraId: era,
		}},
	})
	return computed.PParams, computed.PlutusV2CostModelWritten, nil
}

// SaveEpochSummary stages the caller's summary, with snapshot_ready kept
// once set. The prior row comes from the set.
func (s stagingLedgerDB) SaveEpochSummary(
	summary *models.EpochSummary,
	txn ledgerdb.Txn,
) error {
	er, sr, _ := s.ls.epochSummaryReader(txn)
	if er == nil || sr == nil {
		return drainedUnserved(nodesettings.FamilySummaries)
	}
	s.ls.utxoSetApplyMu.RLock()
	prior, setErr := s.ls.summaryViewLocked(er, sr, txn, summary.Epoch)
	s.ls.utxoSetApplyMu.RUnlock()
	if setErr != nil {
		return fmt.Errorf("read epoch summary %d from the set: %w",
			summary.Epoch, setErr)
	}
	computed := *summary
	computed.EpochNonce = slices.Clone(summary.EpochNonce)
	computed.SnapshotReady = summary.SnapshotReady ||
		prior != nil && prior.SnapshotReady
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		Summaries: []models.EpochSummary{computed},
	})
	return nil
}

// SetEpoch stages the epoch row its arguments describe: every column is
// replaced.
func (s stagingLedgerDB) SetEpoch(
	slot, epoch uint64,
	nonce, evolvingNonce, candidateNonce, lastEpochBlockNonce []byte,
	era, slotLength, lengthInSlots uint,
	txn ledgerdb.Txn,
) error {
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
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		Epochs: []models.Epoch{computed},
	})
	s.ls.stageEpochOpened(txn, computed) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}
