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
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestTachysPoolsIgnoreZeroStakeBuckets(t *testing.T) {
	t.Parallel()

	var positiveID, zeroID common.PoolKeyHash
	positiveID[0] = 1
	zeroID[0] = 2
	pools, err := tachysPoolsFromStakeDistribution(map[string]uint64{
		hex.EncodeToString(positiveID[:]): 9,
		hex.EncodeToString(zeroID[:]):     0,
	}, 9)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, positiveID, pools[0].ID)
	require.Equal(t, uint64(9), pools[0].Stake)
}

func TestTachysPoolsRejectMismatchedSnapshotTotal(t *testing.T) {
	t.Parallel()

	var poolID common.PoolKeyHash
	poolID[0] = 1
	_, err := tachysPoolsFromStakeDistribution(map[string]uint64{
		hex.EncodeToString(poolID[:]): 9,
	}, 10)
	require.ErrorContains(t, err, "total mismatch")
}

// A consensus publication that lands while the distribution is being read
// says nothing about the header, so verification must defer rather than
// report an unavailable stake snapshot, which hard-rejects once the ledger
// apply cursor has reached the header's slot.
func TestTachysEligibilityDefersWhenContextChangesDuringLoad(t *testing.T) {
	t.Parallel()

	const epochID = uint64(3)
	db := newTestDB(t)
	meta := db.Metadata()
	snapshotEpoch := praos.StakeSnapshotEpoch(epochID)
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			markSnapshot(t, snapshotEpoch, ssPool1Hex, oneTrillion),
		},
		nil,
	))
	require.NoError(t, meta.SaveEpochSummary(
		epochSummary(snapshotEpoch, oneTrillion),
		nil,
	))
	ls := &LedgerState{db: db}
	ls.consensus.Store(&consensusSnapshot{
		generation:        1,
		contextGeneration: 1,
		currentEpoch:      models.Epoch{EpochId: epochID},
	})
	ls.tachysDistributionLoadedHook = func() {
		ls.consensus.Store(&consensusSnapshot{
			generation:        2,
			contextGeneration: 2,
			currentEpoch:      models.Epoch{EpochId: epochID},
		})
	}
	block := &envelopeTestBlock{header: &envelopeTestHeader{
		era:  babbage.EraBabbage,
		slot: 300,
	}}

	err := ls.verifyTachysLeaderEligibility(
		t.Context(),
		block,
		epochID,
		electingSnapshot{},
		make([]byte, common.Blake2b256Size),
	)
	require.Error(t, err)
	require.True(t, IsHeaderVerificationDeferred(err), err)
	require.NotErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
}
