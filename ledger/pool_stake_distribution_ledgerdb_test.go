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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// A bounded pool stake distribution is read from the LedgerDB set, the only
// store of the snapshot rows it reports.
func TestPoolStakeDistributionBoundedReadsLedgerDB(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	OpenLiveSetForTest(t, db)
	const snapshotEpoch = 0
	poolA := seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0x11), repeatedBytes(32, 0xAA),
		3_000_000, snapshotEpoch,
	)
	poolB := seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0x22), repeatedBytes(32, 0xBB),
		1_000_000, snapshotEpoch,
	)
	ls := newPoolDistr2Ledger(t, db)

	require.Nil(t, db.Metadata(), "the snapshot rows have no SQL home")

	dist, err := ls.PoolStakeDistributionBounded(
		t.Context(), nil, QueryPoint{}, nil, 2,
	)
	require.NoError(t, err)
	stakes := map[string]uint64{}
	for _, p := range dist.Pools {
		stakes[string(p.PoolKeyHash[:])] = p.Stake
	}
	require.Equal(t, map[string]uint64{
		string(poolA[:]): 3_000_000,
		string(poolB[:]): 1_000_000,
	}, stakes)

	_, err = ls.PoolStakeDistributionBounded(
		t.Context(), nil, QueryPoint{}, nil, 1,
	)
	require.ErrorIs(t, err, ErrPoolStakeDistributionLimitExceeded)
}

// countingSnapshotSet is a live set holding one mark snapshot. It records the
// most rows a single read took from it.
type countingSnapshotSet struct {
	*memSummarySet
	rows    []*models.PoolStakeSnapshot
	maxRead int
}

func (s *countingSnapshotSet) PoolStakeSnapshots(
	_ uint64,
	_ string,
	limit int,
) ([]*models.PoolStakeSnapshot, error) {
	rows := s.rows
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	s.maxRead = max(s.maxRead, len(rows))
	return rows, nil
}

// A bounded snapshot read takes no more rows from LedgerDB than its bound,
// whatever the size of the snapshot.
func TestBoundedSnapshotReadStopsAtItsBound(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	set := &countingSnapshotSet{memSummarySet: &memSummarySet{
		memNonceSet: &memNonceSet{nonces: map[string]models.BlockNonce{}},
	}}
	for _, key := range []byte{0x11, 0x22, 0x33, 0x44, 0x55} {
		set.rows = append(set.rows, &models.PoolStakeSnapshot{
			Epoch:        1,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  repeatedBytes(28, key),
			TotalStake:   1_000_000,
		})
	}
	ls.config.UtxoSet = set

	rows, err := ls.ledgerDB().GetPoolStakeSnapshotsByEpochBounded(
		1, snapshotTypeMark, 2, nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, repeatedBytes(28, 0x11), rows[0].PoolKeyHash)
	require.Equal(t, repeatedBytes(28, 0x22), rows[1].PoolKeyHash)
	require.Equal(t, 2, set.maxRead)
}
