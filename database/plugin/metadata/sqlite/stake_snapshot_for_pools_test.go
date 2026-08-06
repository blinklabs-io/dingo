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

package sqlite

import (
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotPoolKeyHash builds a distinct 28-byte pool key hash for index i.
func snapshotPoolKeyHash(i int) []byte {
	hash := make([]byte, 28)
	binary.BigEndian.PutUint32(hash, uint32(i)+1)
	return hash
}

// seedMarkSnapshots writes `count` mark-snapshot rows, pool i holding stake
// (i+1)*1000, and returns their key hashes in order.
func seedMarkSnapshots(
	t *testing.T,
	store interface {
		SavePoolStakeSnapshots([]*models.PoolStakeSnapshot, types.Txn) error
	},
	epoch uint64,
	count int,
) [][]byte {
	t.Helper()
	snapshots := make([]*models.PoolStakeSnapshot, 0, count)
	hashes := make([][]byte, 0, count)
	for i := range count {
		hash := snapshotPoolKeyHash(i)
		hashes = append(hashes, hash)
		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:        epoch,
			SnapshotType: "mark",
			PoolKeyHash:  hash,
			TotalStake:   types.Uint64(uint64(i+1) * 1000),
		})
	}
	require.NoError(t, store.SavePoolStakeSnapshots(snapshots, nil))
	return hashes
}

// TestGetPoolStakeSnapshotsForPoolsReturnsOnlyRequested covers the bounded
// read behind a GetPoolDistr2 pool filter.
//
// A pool the snapshot has no row for comes back absent rather than at zero
// stake: the two are different answers, and the caller distinguishes them.
func TestGetPoolStakeSnapshotsForPoolsReturnsOnlyRequested(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const epoch = 7
	hashes := seedMarkSnapshots(t, store, epoch, 4)
	absent := snapshotPoolKeyHash(99)

	got, err := store.GetPoolStakeSnapshotsForPools(
		epoch,
		"mark",
		[][]byte{hashes[1], hashes[3], absent},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 2, "only the pools the snapshot holds are returned")

	byPool := map[string]uint64{}
	for _, snapshot := range got {
		byPool[string(snapshot.PoolKeyHash)] = uint64(snapshot.TotalStake)
	}
	assert.Equal(t, uint64(2000), byPool[string(hashes[1])])
	assert.Equal(t, uint64(4000), byPool[string(hashes[3])])
	assert.NotContains(t, byPool, string(absent))

	// An empty filter is not a request for everything.
	empty, err := store.GetPoolStakeSnapshotsForPools(epoch, "mark", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// TestGetPoolStakeSnapshotsForPoolsChunksBeyondParameterLimit covers a filter
// spanning more than one chunk.
//
// The store contracts to 999 parameters per statement on SQLite and spends two
// before the first pool key hash, so a filter naming more than 997 pools is
// split. What this pins is the merge across that split: every requested pool
// comes back exactly once, where an off-by-one would drop or duplicate the
// pools either side of it. It does not rest on the driver rejecting a longer
// statement -- SQLite itself accepts 32766 bound parameters, so a set this
// size would be accepted chunked or not.
func TestGetPoolStakeSnapshotsForPoolsChunksBeyondParameterLimit(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const epoch = 3
	// Comfortably past the 997 that fit in one statement, so the split lands
	// mid-request rather than exactly at the end.
	const poolCount = 1500
	hashes := seedMarkSnapshots(t, store, epoch, poolCount)

	got, err := store.GetPoolStakeSnapshotsForPools(
		epoch,
		"mark",
		hashes,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, poolCount,
		"every requested pool must survive the chunk boundary")

	byPool := make(map[string]uint64, len(got))
	for _, snapshot := range got {
		byPool[string(snapshot.PoolKeyHash)] = uint64(snapshot.TotalStake)
	}
	require.Len(t, byPool, poolCount, "no pool may be returned twice")
	// Spot-check either side of the boundary rather than all 1500.
	for _, i := range []int{0, 996, 997, 998, poolCount - 1} {
		assert.Equal(t, uint64(i+1)*1000, byPool[string(hashes[i])],
			"pool %d must carry its own stake", i)
	}
}

// TestGetPoolStakeSnapshotsForPoolsDeduplicatesRepeatedHashes covers a pool
// named more than once in the same filter.
//
// This list is not the node's to choose: it arrives from the wire as the pool
// filter on GetPoolDistr2, and PoolFilter hands back the client's items
// verbatim without collapsing repeats. A client may therefore name a pool
// twice, and nothing upstream stops it.
//
// A single `IN (...)` would match that pool's row once regardless. Chunking
// breaks that when the two mentions fall either side of a split, since each
// chunk matches independently and the results are concatenated. The store's
// contract is one row per distinct pool it holds, so the repeats collapse
// before the list is chunked rather than after.
func TestGetPoolStakeSnapshotsForPoolsDeduplicatesRepeatedHashes(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const epoch = 4
	const poolCount = 1500
	hashes := seedMarkSnapshots(t, store, epoch, poolCount)

	// The first pool named again at the end, so its two mentions straddle the
	// 997-parameter split. Within one chunk SQL collapses them anyway, which
	// is the behaviour being restored.
	requested := append(append([][]byte{}, hashes...), hashes[0])

	got, err := store.GetPoolStakeSnapshotsForPools(
		epoch,
		"mark",
		requested,
		nil,
	)
	require.NoError(t, err)

	counts := make(map[string]int, len(got))
	for _, snapshot := range got {
		counts[string(snapshot.PoolKeyHash)]++
	}
	assert.Equal(t, 1, counts[string(hashes[0])],
		"a pool named twice must still yield one row, as a single IN list "+
			"would have done")
	assert.Equal(t, poolCount, len(got),
		"the result is one row per distinct pool held, not per mention")
}
