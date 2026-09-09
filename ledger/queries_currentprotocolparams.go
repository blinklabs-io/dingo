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
)

// queryShelleyCurrentProtocolParams answers GetCurrentPParams.
//
// asOfSlot is Query's pinned point (0 = live). Unlike stake distribution,
// there is no persisted historical record of protocol parameters by
// epoch -- currentPParams is a single in-memory value, overwritten in place
// on every governance/epoch transition. So a pinned point can only be
// answered safely when it names a slot in the same epoch the live tip is
// currently in: protocol parameters only change at epoch boundaries, so
// within one epoch "as of asOfSlot" and "live right now" are the same
// value. A pin spanning an epoch boundary since asOfSlot returns
// ErrHistoricalStateUnavailable rather than silently answering with a live
// value that may no longer match what was true at asOfSlot
// (blinklabs-io/dingo#382). Building real historical-by-epoch pparams
// storage (or replaying governance enactments) to lift this restriction is
// a materially larger, not-yet-attempted piece of work.
//
// The live epoch and the returned parameters both come from one
// loadConsensusSnapshot() call, not two separate reads: an earlier version
// compared the live epoch (from txn's database transaction) against
// targetEpoch, then separately called GetCurrentPParamsForReporting (which
// loads its own, possibly later, published snapshot). An epoch-boundary
// commit landing between those two reads could pass targetEpoch == liveEpoch
// while returning the parameters of the epoch that had already replaced it.
// Deriving both from the same snapshot value closes that window.
func (ls *LedgerState) queryShelleyCurrentProtocolParams(
	asOfSlot uint64,
	txn *database.Txn,
) (any, error) {
	snapshot := ls.loadConsensusSnapshot()
	if asOfSlot == 0 {
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
	targetEpoch, err := ls.resolveAsOfEpoch(txn, asOfSlot)
	if err != nil {
		return nil, err
	}
	liveEpoch := snapshot.currentEpoch.EpochId
	if targetEpoch != liveEpoch {
		return nil, fmt.Errorf(
			"%w: protocol parameters at slot %d (epoch %d) cannot be "+
				"reconstructed while the live tip is in epoch %d -- "+
				"historical protocol-parameter values across an epoch "+
				"boundary are not yet supported",
			ErrHistoricalStateUnavailable,
			asOfSlot,
			targetEpoch,
			liveEpoch,
		)
	}
	return []any{withoutSyntheticV2CostModel(
		snapshot.currentPParams,
		snapshot.syntheticV2CostModelInEffect,
		ls.config.Logger,
	)}, nil
}
