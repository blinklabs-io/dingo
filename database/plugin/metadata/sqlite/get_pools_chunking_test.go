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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPoolsChunksBeyondParameterLimit covers a pool set spanning more than
// one chunk of GetPools' IN list.
//
// What this pins is the merge across the boundary, not the driver's reaction
// to a long statement: it asserts every requested pool comes back exactly
// once, which an off-by-one in the chunk arithmetic would break by dropping or
// duplicating the pools either side of the split. It deliberately does not
// assert that an unchunked read would fail -- the store contracts to a
// conservative 999 parameters for SQLite while the driver itself accepts
// 32766, so a set this size would be accepted either way and a test resting on
// rejection would pass whether or not the chunking existed.
func TestGetPoolsChunksBeyondParameterLimit(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	// Past the store's 999-parameter chunk by enough that the split lands
	// mid-request rather than exactly on the end.
	const poolCount = 1200
	hashes := make([]lcommon.PoolKeyHash, 0, poolCount)
	for i := range poolCount {
		raw := make([]byte, 28)
		binary.BigEndian.PutUint32(raw, uint32(i)+1)
		pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(raw))
		hashes = append(hashes, pkh)

		vrf := make([]byte, 32)
		binary.BigEndian.PutUint32(vrf, uint32(i)+1)
		require.NoError(t, store.ImportPool(
			&models.Pool{
				PoolKeyHash:   raw,
				VrfKeyHash:    vrf,
				RewardAccount: raw,
			},
			&models.PoolRegistration{
				PoolKeyHash:   raw,
				VrfKeyHash:    vrf,
				RewardAccount: raw,
				AddedSlot:     uint64(i),
			},
			nil,
		))
	}

	pools, err := store.GetPools(hashes, nil)
	require.NoError(t, err)
	require.Len(t, pools, poolCount,
		"every requested pool must survive the chunk boundary")

	seen := make(map[string]struct{}, len(pools))
	for _, pool := range pools {
		seen[string(pool.PoolKeyHash)] = struct{}{}
	}
	require.Len(t, seen, poolCount, "no pool may be returned twice")
	// Spot-check either side of the boundary rather than all 1200.
	for _, i := range []int{0, 998, 999, 1000, poolCount - 1} {
		assert.Contains(t, seen, string(hashes[i].Bytes()),
			"pool %d must be present across the chunk boundary", i)
	}
}
