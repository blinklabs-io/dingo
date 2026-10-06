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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// queryShelleyCurrentProtocolParams answers GetCurrentPParams.
//
// at is Query's pinned point (unpinned = live). When the pinned point falls
// in the live tip's current epoch, the live in-memory snapshot answers it
// directly (protocol parameters only change at epoch boundaries, so within
// one epoch "as of at" and "live right now" are the same value). Otherwise
// this falls back to the persisted per-epoch pparams row
// (database.Database.GetPParams / loadPersistedProtocolParameters) that
// SetPParams already writes on every era transition and every
// governance-enacted parameter update -- the same row Blockfrost's
// epoch-parameters endpoint and koios-parity's reward comparison already
// read historically. An epoch with no persisted row (never had a parameter
// change recorded, or one pruned by DeletePParamsAfterSlot after a
// rollback) returns ErrHistoricalStateUnavailable rather than silently
// answering with a live value that may no longer match what was true at
// that point.
//
// Takes the whole QueryPoint, not a bare slot -- see resolveAsOfEpoch's
// doc comment for why a bare uint64 would silently mistreat a real point
// pinned at slot 0 as live.
//
// The live epoch and the live-case parameters both come from one
// loadConsensusSnapshot() call, not two separate reads: an earlier version
// compared the live epoch (from txn's database transaction) against
// targetEpoch, then separately called GetCurrentPParamsForReporting (which
// loads its own, possibly later, published snapshot). An epoch-boundary
// commit landing between those two reads could pass targetEpoch == liveEpoch
// while returning the parameters of the epoch that had already replaced it.
// Deriving both from the same snapshot value closes that window.
//
// The historical path also runs withoutSyntheticV2CostModel, same as the
// live path, but re-derives synthetic-ness from the persisted value itself
// rather than trusting ls.syntheticV2CostModel (which describes only the
// current era's object): transitionToEraFrom persists newPParams verbatim
// into the pparams row, so a historical epoch whose fabricated PlutusV2
// cost model had not yet been replaced by real data carries that same
// fabrication in its persisted CBOR.
func (ls *LedgerState) queryShelleyCurrentProtocolParams(
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	snapshot := ls.loadConsensusSnapshot()
	if !at.pinned() {
		// A QueryView whose epoch ended after it was acquired still has to
		// answer for the epoch it froze, not the one that replaced it. When
		// that epoch has no persisted row the live value is the best answer
		// left, so only that rejection is not surfaced here; any other
		// historical-state error is propagated.
		row, err := ls.snapshotEpoch(txn, at)
		if err != nil {
			return nil, err
		}
		if row != nil && row.EpochId != snapshot.currentEpoch.EpochId {
			result, err := ls.historicalProtocolParameters(
				snapshot, row.EpochId, at, txn,
			)
			if !errors.Is(err, errPParamsRowMissing) {
				return result, err
			}
		}
		return []any{withoutSyntheticV2CostModel(
			snapshot.currentPParams,
			snapshot.syntheticV2CostModelInEffect,
			ls.config.Logger,
		)}, nil
	}
	if txn == nil {
		txn = ls.db.Transaction(false)
		defer txn.Release()
	}
	targetEpoch, found, err := ls.resolveAsOfEpoch(txn, at)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errEpochNotResolved(at)
	}
	if targetEpoch == snapshot.currentEpoch.EpochId {
		return []any{withoutSyntheticV2CostModel(
			snapshot.currentPParams,
			snapshot.syntheticV2CostModelInEffect,
			ls.config.Logger,
		)}, nil
	}
	return ls.historicalProtocolParameters(snapshot, targetEpoch, at, txn)
}

// errPParamsRowMissing marks a historical protocol-parameter lookup that found
// no persisted row for the epoch, as opposed to an unresolvable epoch or era.
var errPParamsRowMissing = errors.New("no persisted protocol-parameter row")

// historicalProtocolParameters answers from targetEpoch's persisted pparams
// row, for a targetEpoch other than the live snapshot's epoch.
func (ls *LedgerState) historicalProtocolParameters(
	snapshot *consensusSnapshot,
	targetEpoch uint64,
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	liveEpoch := snapshot.currentEpoch.EpochId
	epochRow, err := ls.db.GetEpoch(targetEpoch, txn)
	if err != nil {
		return nil, err
	}
	if epochRow == nil {
		return nil, fmt.Errorf(
			"%w: protocol parameters at slot %d (epoch %d) cannot be "+
				"reconstructed -- no epoch record exists for epoch %d",
			ErrHistoricalStateUnavailable,
			at.Slot,
			targetEpoch,
			targetEpoch,
		)
	}
	era := eras.GetEraById(epochRow.EraId)
	if era == nil {
		return nil, fmt.Errorf(
			"%w: protocol parameters at slot %d (epoch %d) cannot be "+
				"reconstructed -- epoch %d names unknown era %d",
			ErrHistoricalStateUnavailable,
			at.Slot,
			targetEpoch,
			targetEpoch,
			epochRow.EraId,
		)
	}
	pparams, err := ls.loadPersistedProtocolParameters(targetEpoch, *era, txn)
	if err != nil {
		return nil, err
	}
	if pparams == nil {
		return nil, fmt.Errorf(
			"%w: %w: protocol parameters at slot %d (epoch %d) cannot be "+
				"reconstructed while the live tip is in epoch %d -- no "+
				"persisted protocol-parameter row exists for epoch %d",
			ErrHistoricalStateUnavailable,
			errPParamsRowMissing,
			at.Slot,
			targetEpoch,
			liveEpoch,
			targetEpoch,
		)
	}
	// pparams is a persisted, historical value -- never ls.currentPParams --
	// so the live case's tracked ls.syntheticV2CostModel flag (which
	// describes only the CURRENT era's object) does not apply to it. Its own
	// CBOR may still carry HardForkBabbage's fabricated PlutusV2 cost model
	// (transitionToEraFrom persists newPParams verbatim, synthetic or not).
	//
	// Prefer the durable cleared-epoch provenance (confirmed: real data
	// landed at or before that epoch) over re-deriving synthetic-ness from
	// the value alone -- the value-based heuristic
	// (resolveSyntheticV2CostModel) cannot tell a real update that happens
	// to re-affirm the exact fabricated default from an actually-synthetic
	// one, while the cleared-epoch marker knows which epoch really
	// confirmed real data. Only fall back to the heuristic for the
	// genuinely ambiguous case: no confirmation recorded at all, or
	// targetEpoch predates the one that confirmed it.
	clearedEpoch, cleared, err := database.SyntheticV2CostModelClearedEpoch(
		ls.db, txn,
	)
	if err != nil {
		return nil, err
	}
	synthetic := resolveSyntheticV2CostModel("", pparams)
	if cleared && targetEpoch >= clearedEpoch {
		synthetic = false
	}
	return []any{withoutSyntheticV2CostModel(
		pparams,
		synthetic,
		ls.config.Logger,
	)}, nil
}

// snapshotProtocolParameters returns the protocol parameters a QueryView's
// frozen epoch ran under. row is the epoch record covering the snapshot tip
// (nil on the direct path); when it names an epoch the live snapshot has
// moved past, the persisted row for that epoch answers. An epoch with no
// persisted row falls back to the live value, as GetCurrentPParams does, and
// any other historical-state error is returned.
func (ls *LedgerState) snapshotProtocolParameters(
	snapshot *consensusSnapshot,
	row *models.Epoch,
	txn *database.Txn,
) (lcommon.ProtocolParameters, error) {
	if row == nil || row.EpochId == snapshot.currentEpoch.EpochId {
		return snapshot.currentPParams, nil
	}
	result, err := ls.historicalProtocolParameters(
		snapshot, row.EpochId, QueryPoint{}, txn,
	)
	if err != nil {
		if errors.Is(err, errPParamsRowMissing) {
			return snapshot.currentPParams, nil
		}
		return nil, err
	}
	if values, ok := result.([]any); ok && len(values) == 1 {
		if pparams, ok := values[0].(lcommon.ProtocolParameters); ok {
			return pparams, nil
		}
	}
	return nil, fmt.Errorf(
		"unexpected protocol parameters result %T for epoch %d",
		result,
		row.EpochId,
	)
}
