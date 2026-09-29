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

//go:build dingo_extra_plugins

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// poolKeyHashFill returns a 28-byte pool-key hash filled with a single byte,
// which sorts by that byte across sqlite, PostgreSQL, and MySQL alike (all
// three order a fixed-length binary column bytewise).
func poolKeyHashFill(b byte) []byte {
	h := make([]byte, 28)
	for i := range h {
		h[i] = b
	}
	return h
}

// TestGetRewardStakeInputsInPoolKeyHashRangeAcrossBackends proves the chunked
// precompute's pool-batch query returns exactly the rows within an inclusive
// [lo, hi] pool_key_hash range -- neither leaking a neighboring pool's rows
// nor dropping a boundary pool's own rows -- identically on sqlite,
// PostgreSQL, and MySQL. This is the differential proof for the one query
// the chunked design adds; TestRewardPrecomputeWriteCannotOutliveConcurrentRollback
// covers the concurrency property (guard-then-write ordering against a
// racing rollback) on the same three backends.
func TestGetRewardStakeInputsInPoolKeyHashRangeAcrossBackends(t *testing.T) {
	t.Parallel()

	backends := []rewardRaceBackend{
		{name: "sqlite", open: openSQLiteRewardRaceBackend},
		{name: "postgres", open: openPostgresRewardRaceBackend},
		{name: "mysql", open: openMySQLRewardRaceBackend},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			db, _, _ := backend.open(t)
			meta := db.Metadata()

			const epoch = uint64(5)
			pools := []byte{0x11, 0x22, 0x33, 0x44}
			var inputs []*models.RewardStakeInput
			for _, pool := range pools {
				poolKey := poolKeyHashFill(pool)
				for i := range 2 {
					inputs = append(inputs, &models.RewardStakeInput{
						Epoch:         epoch,
						PoolKeyHash:   poolKey,
						CredentialTag: 0,
						StakingKey: poolKeyHashFillWithSuffix(
							pool, byte(i),
						),
						Stake:        100,
						Registered:   true,
						CapturedSlot: 10,
						BoundarySlot: 9,
					})
				}
			}
			require.NoError(t, meta.SaveRewardStakeInputs(inputs, nil))

			assertRange := func(
				lo, hi byte, wantPools ...byte,
			) {
				t.Helper()
				rows, err := meta.GetRewardStakeInputsInPoolKeyHashRange(
					epoch, poolKeyHashFill(lo), poolKeyHashFill(hi), nil,
				)
				require.NoError(t, err)
				gotPools := make(map[byte]int)
				for _, row := range rows {
					require.Len(t, row.PoolKeyHash, 28)
					gotPools[row.PoolKeyHash[0]]++
				}
				wantCounts := make(map[byte]int, len(wantPools))
				for _, p := range wantPools {
					wantCounts[p] = 2
				}
				require.Equal(t, wantCounts, gotPools)
			}

			// A single pool at the low end of the stored set.
			assertRange(0x11, 0x11, 0x11)
			// A single pool at the high end.
			assertRange(0x44, 0x44, 0x44)
			// A contiguous middle range covering two pools.
			assertRange(0x22, 0x33, 0x22, 0x33)
			// The whole stored range.
			assertRange(0x11, 0x44, 0x11, 0x22, 0x33, 0x44)
			// A range strictly between two stored pools: no rows at all,
			// not the nearest neighbor's.
			assertRange(0x23, 0x32)
			// A range entirely below every stored pool.
			assertRange(0x01, 0x10)
			// A range entirely above every stored pool.
			assertRange(0x45, 0xf0)
		})
	}
}

// poolKeyHashFillWithSuffix gives each pool's rows distinct staking keys
// (the fill byte plus an index byte at the end) so SaveRewardStakeInputs'
// (epoch, pool_key_hash, credential_tag, staking_key) uniqueness never
// collides two rows meant to coexist within the same pool.
func poolKeyHashFillWithSuffix(fill, suffix byte) []byte {
	h := poolKeyHashFill(fill)
	h[len(h)-1] = suffix
	return h
}
