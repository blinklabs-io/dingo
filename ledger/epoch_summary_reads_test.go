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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// memSummarySet is a live set that holds epoch summaries and no epochs or
// snapshots.
type memSummarySet struct {
	*memNonceSet
	summaries map[uint64]models.EpochSummary
}

func (*memSummarySet) EpochStateLoaded() (bool, error)     { return true, nil }
func (*memSummarySet) Epochs() ([]models.Epoch, error)     { return nil, nil }
func (*memSummarySet) EpochSummariesLoaded() (bool, error) { return true, nil }
func (*memSummarySet) EraSwitch() (*database.MidEpochEraSwitch, error) {
	return nil, nil
}

func (*memSummarySet) PoolStakeSnapshots(
	uint64,
	string,
	int,
) ([]*models.PoolStakeSnapshot, error) {
	return nil, nil
}

func (*memSummarySet) PoolStakeSnapshotsCapturedIn(
	uint64,
	uint64,
) ([]*models.PoolStakeSnapshot, error) {
	return nil, nil
}

func (s *memSummarySet) EpochSummary(epoch uint64) (*models.EpochSummary, error) {
	sm, ok := s.summaries[epoch]
	if !ok {
		return nil, nil
	}
	return &sm, nil
}

// The producing-pool check behind the header path reads the total active
// stake through the ledger's LedgerDB, so a set that answers reads is the
// source rather than the metadata store.
func TestPoolAbsentFromPopulatedSnapshotReadsTheSet(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	ls.config.UtxoSet = &memSummarySet{
		memNonceSet: &memNonceSet{nonces: map[string]models.BlockNonce{}},
		summaries: map[uint64]models.EpochSummary{
			1: {Epoch: 1, TotalActiveStake: types.Uint64(500), SnapshotReady: true},
		},
	}
	snap := electingSnapshot{epoch: 1, kind: models.PoolStakeSnapshotTypeMark}
	require.True(t, ls.poolAbsentFromPopulatedSnapshot(snap),
		"the set's ready summary shows a populated snapshot")

}
