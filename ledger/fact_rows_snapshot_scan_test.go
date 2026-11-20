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
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// scanCountingSet is a live set holding pool stake snapshots for many epochs
// that counts how many times a snapshot read reaches it.
type scanCountingSet struct {
	*memNonceSet
	epochs    []models.Epoch
	snapshots []*models.PoolStakeSnapshot
	scans     atomic.Int64
}

func (*scanCountingSet) EpochStateLoaded() (bool, error) { return true, nil }
func (s *scanCountingSet) Epochs() ([]models.Epoch, error) {
	return s.epochs, nil
}
func (*scanCountingSet) EraSwitch() (*database.MidEpochEraSwitch, error) {
	return nil, nil
}

func (s *scanCountingSet) PoolStakeSnapshots(
	epoch uint64,
	typ string,
) ([]*models.PoolStakeSnapshot, error) {
	s.scans.Add(1)
	var out []*models.PoolStakeSnapshot
	for _, sn := range s.snapshots {
		if sn.Epoch == epoch && (typ == "" || sn.SnapshotType == typ) {
			c := *sn
			out = append(out, &c)
		}
	}
	return out, nil
}

func (s *scanCountingSet) PoolStakeSnapshotsCapturedIn(
	from, through uint64,
) ([]*models.PoolStakeSnapshot, error) {
	s.scans.Add(1)
	var out []*models.PoolStakeSnapshot
	for _, sn := range s.snapshots {
		if sn.CapturedSlot >= from && sn.CapturedSlot <= through {
			c := *sn
			out = append(out, &c)
		}
	}
	return out, nil
}

// One segment's copy of the pool stake snapshot facts reads the set's
// snapshots once, however many epochs the set holds, and exports only the
// rows captured inside the segment.
func TestPoolStakeSnapshotFactRowsScanOncePerSegment(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	const epochCount = 12
	set := &scanCountingSet{
		memNonceSet: &memNonceSet{nonces: map[string]models.BlockNonce{}},
	}
	types := append([]string{}, rewardSnapshotTypes...)
	types = append(types, "unrelated")
	for e := uint64(1); e <= epochCount; e++ {
		set.epochs = append(set.epochs, models.Epoch{
			EpochId: e, StartSlot: e * 100,
		})
		for i, typ := range types {
			for _, pool := range []byte{1, 2} {
				set.snapshots = append(set.snapshots, &models.PoolStakeSnapshot{
					Epoch: e, SnapshotType: typ, PoolKeyHash: []byte{pool},
					CapturedSlot: e*100 + uint64(i),
				})
			}
		}
	}
	ls.config.UtxoSet = set

	rows, err := ls.poolStakeSnapshotFactRows(
		metadata.FactTable{}, 500, 599,
	)
	require.NoError(t, err)

	var got []string
	for _, row := range rows.Rows {
		got = append(got, fmt.Sprintf("%d/%s/%x", row[0], row[1], row[2]))
	}
	var want []string
	for _, typ := range rewardSnapshotTypes {
		for _, pool := range []byte{1, 2} {
			want = append(want, fmt.Sprintf("5/%s/%x", typ, []byte{pool}))
		}
	}
	require.ElementsMatch(t, want, got,
		"only the reward snapshot types captured inside the segment export")
	require.EqualValues(t, 1, set.scans.Load(),
		"the copy reads the set's snapshots once, not once per epoch and type")
}

// The all-epoch snapshot view agrees with the per-epoch view under staged
// saves, deletes and a rollback, including a stored row a staged save moves
// out of the range and one it moves into it.
func TestSnapshotsCapturedInViewMatchesPerEpochView(t *testing.T) {
	t.Parallel()
	types := append([]string{}, rewardSnapshotTypes...)
	types = append(types, "unrelated")
	set := &scanCountingSet{}
	for e := uint64(1); e <= 5; e++ {
		set.epochs = append(set.epochs, models.Epoch{
			EpochId: e, StartSlot: e * 100,
		})
		for i, typ := range types {
			for _, pool := range []byte{1, 2} {
				set.snapshots = append(set.snapshots, &models.PoolStakeSnapshot{
					Epoch: e, SnapshotType: typ, PoolKeyHash: []byte{pool},
					CapturedSlot: e*100 + uint64(i),
				})
			}
		}
	}
	save := func(epoch uint64, typ string, pool byte, slot uint64) SnapshotChange {
		return SnapshotChange{Kind: SnapshotSave, Snapshot: &models.PoolStakeSnapshot{
			Epoch: epoch, SnapshotType: typ, PoolKeyHash: []byte{pool},
			CapturedSlot: slot,
		}}
	}
	mark := models.PoolStakeSnapshotTypeMark
	ls := overlayLedger(
		&utxoSetBatch{
			epochs: []models.Epoch{{EpochId: 6, StartSlot: 600}},
			snapshots: []SnapshotChange{
				save(3, mark, 1, 999),
				save(2, models.PoolStakeSnapshotTypeSet, 3, 350),
				save(6, mark, 1, 380),
			},
		},
		&utxoSetBatch{snapshots: []SnapshotChange{{
			Kind: SnapshotDeleteForEpoch, Epoch: 3,
			Type: models.PoolStakeSnapshotTypeGo,
		}}},
		rollbackBatch(550),
		&utxoSetBatch{snapshots: []SnapshotChange{
			{Kind: SnapshotDeleteBeforeEpoch, Epoch: 2},
			save(1, models.PoolStakeSnapshotTypeActive, 9, 320),
		}},
	)
	key := func(s *models.PoolStakeSnapshot) string {
		return fmt.Sprintf("%d/%s/%x@%d",
			s.Epoch, s.SnapshotType, s.PoolKeyHash, s.CapturedSlot)
	}
	for _, rng := range [][2]uint64{{0, 1000}, {300, 399}, {350, 350}, {500, 599}} {
		var want []string
		for e := uint64(0); e <= 7; e++ {
			for _, typ := range types {
				rows, err := ls.snapshotView(set, nil, e, typ)
				require.NoError(t, err)
				for _, s := range rows {
					if inSlotRange(s.CapturedSlot, rng[0], rng[1]) {
						want = append(want, key(s))
					}
				}
			}
		}
		rows, err := ls.snapshotsCapturedInView(set, nil, rng[0], rng[1])
		require.NoError(t, err)
		got := make([]string, 0, len(rows))
		for _, s := range rows {
			got = append(got, key(s))
		}
		require.NotEmpty(t, want, "range %v", rng)
		require.ElementsMatch(t, want, got, "range %v", rng)
	}
}
