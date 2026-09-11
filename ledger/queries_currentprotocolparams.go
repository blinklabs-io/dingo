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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
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
// that point (blinklabs-io/dingo#382).
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
// The historical path deliberately does not run the live path's
// withoutSyntheticV2CostModel stripping: that function corrects a
// presentation quirk of the live, in-memory ls.currentPParams value; the
// persisted CBOR row for a given epoch already reflects whatever
// ls.currentPParams looked like at the moment SetPParams wrote it (see
// transitionToEraFrom, which encodes workingPParams -- already
// post-synthetic-injection when applicable -- into the very row this reads
// back), so it needs no further correction.
func (ls *LedgerState) queryShelleyCurrentProtocolParams(
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	snapshot := ls.loadConsensusSnapshot()
	if !at.pinned() {
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
	targetEpoch, err := ls.resolveAsOfEpoch(txn, at)
	if err != nil {
		return nil, err
	}
	liveEpoch := snapshot.currentEpoch.EpochId
	if targetEpoch == liveEpoch {
		return []any{withoutSyntheticV2CostModel(
			snapshot.currentPParams,
			snapshot.syntheticV2CostModelInEffect,
			ls.config.Logger,
		)}, nil
	}
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
			"%w: protocol parameters at slot %d (epoch %d) cannot be "+
				"reconstructed while the live tip is in epoch %d -- no "+
				"persisted protocol-parameter row exists for epoch %d",
			ErrHistoricalStateUnavailable,
			at.Slot,
			targetEpoch,
			liveEpoch,
			targetEpoch,
		)
	}
	return []any{pparams}, nil
}
