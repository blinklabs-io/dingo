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
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// countingEpochSet holds epochs and one snapshot row per epoch and type, and
// counts the Epochs reads, each of which decodes every epoch row in the real
// set.
type countingEpochSet struct {
	*memNonceSet
	epochs      []models.Epoch
	epochsReads atomic.Int64
}

var _ EpochStateReader = (*countingEpochSet)(nil)

func (*countingEpochSet) EpochStateLoaded() (bool, error) { return true, nil }
func (*countingEpochSet) EraSwitch() (*database.MidEpochEraSwitch, error) {
	return nil, nil
}

func (s *countingEpochSet) Epochs() ([]models.Epoch, error) {
	s.epochsReads.Add(1)
	return s.epochs, nil
}

func (s *countingEpochSet) PoolStakeSnapshots(
	epoch uint64,
	typ string,
) ([]*models.PoolStakeSnapshot, error) {
	return []*models.PoolStakeSnapshot{{
		Epoch: epoch, SnapshotType: typ, PoolKeyHash: []byte{byte(epoch)},
		CapturedSlot: epoch * 100,
	}}, nil
}

func (s *countingEpochSet) PoolStakeSnapshotsCapturedIn(
	from, through uint64,
) ([]*models.PoolStakeSnapshot, error) {
	var rows []*models.PoolStakeSnapshot
	for _, epoch := range s.epochs {
		for _, typ := range rewardSnapshotTypes {
			snapshots, err := s.PoolStakeSnapshots(epoch.EpochId, typ)
			if err != nil {
				return nil, err
			}
			for _, snapshot := range snapshots {
				if snapshot.CapturedSlot >= from &&
					snapshot.CapturedSlot <= through {
					rows = append(rows, snapshot)
				}
			}
		}
	}
	return rows, nil
}

// The fact copy reads the epoch family once, however many epochs the set
// holds, rather than once per epoch and snapshot type.
func TestPoolStakeSnapshotFactRowsReadEpochsOnce(t *testing.T) {
	t.Parallel()
	const epochCount = 6
	set := &countingEpochSet{
		memNonceSet: &memNonceSet{nonces: map[string]models.BlockNonce{}},
	}
	for e := uint64(1); e <= epochCount; e++ {
		set.epochs = append(set.epochs, models.Epoch{
			EpochId: e, StartSlot: e * 100,
		})
	}
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	ls.config.UtxoSet = set

	rows, err := ls.poolStakeSnapshotFactRows(
		metadata.FactTable{}, 0, epochCount*100,
	)
	require.NoError(t, err)
	require.Len(t, rows.Rows, epochCount*len(rewardSnapshotTypes))
	require.LessOrEqual(t, set.epochsReads.Load(), int64(2),
		"Epochs reads for %d epochs", epochCount)
}

// countingSummarySet is countingEpochSet with one summary row per epoch.
type countingSummarySet struct {
	*countingEpochSet
}

func (*countingSummarySet) EpochSummariesLoaded() (bool, error) {
	return true, nil
}

func (*countingSummarySet) EpochSummary(
	epoch uint64,
) (*models.EpochSummary, error) {
	return &models.EpochSummary{Epoch: epoch}, nil
}

// A summary read with no rollback staged leaves the epoch family alone, so a
// caller walking every epoch does not decode it once per epoch.
func TestEpochSummaryReadsSkipEpochsWithoutRollback(t *testing.T) {
	t.Parallel()
	const epochCount = 6
	set := &countingSummarySet{countingEpochSet: &countingEpochSet{
		memNonceSet: &memNonceSet{nonces: map[string]models.BlockNonce{}},
	}}
	for e := uint64(1); e <= epochCount; e++ {
		set.epochs = append(set.epochs, models.Epoch{
			EpochId: e, StartSlot: e * 100,
		})
	}
	ls, db := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	ls.config.UtxoSet = set
	store := stagingLedgerDB{LedgerDB: dbadapter.New(db), ls: ls}

	for e := uint64(1); e <= epochCount; e++ {
		sm, err := store.GetEpochSummary(e, nil)
		require.NoError(t, err)
		require.NotNil(t, sm)
		require.Equal(t, e, sm.Epoch)
	}
	require.Less(t, set.epochsReads.Load(), int64(epochCount),
		"Epochs reads for %d summary reads", epochCount)
}
