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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestVerifyPointQueryable_WithinAllFloors_Accepted covers the accept
// direction (human review, Chris Guiney, dingo#4319/#4320: "VerifyPointQueryable
// has no reference in any _test.go in the repository... only the accept
// direction keeps working clients alive"): a point inside every
// point-aware query type's own retention floor -- on chain, within the
// UTxO/stake/pparams/era windows -- must be accepted so a well-behaved
// client's Acquire actually succeeds, not just so a stale one is rejected.
func TestVerifyPointQueryable_WithinAllFloors_Accepted(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.NoError(t, err)
}

// TestVerifyPointQueryable_PastRetentionFloor_Rejected covers the reject
// direction: a point on-chain but past the stake-snapshot retention floor
// must fail with ErrHistoricalStateUnavailable, the sentinel
// localstatequeryServerAcquire maps to a clean wire-level
// AcquireFailurePointTooOld -- exactly mirroring
// TestPoolStakeDistribution_AsOfSlot_TooOldRejected's scenario, but through
// VerifyPointQueryable (which checks verifyPointOnChain first, unlike a
// bare PoolStakeDistribution call) to prove the whole upfront check
// rejects it, not just the one retention check it happens to hit first.
func TestVerifyPointQueryable_PastRetentionFloor_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 10}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 1000: 10})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1050, repeatedBytes(32, 0x0C)),
	}, nil))

	// Epoch 3 is 7 epochs behind the live epoch (10) -- outside the
	// 3-epoch stake-snapshot retention window.
	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}
