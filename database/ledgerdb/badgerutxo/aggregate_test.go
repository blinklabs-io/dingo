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

package badgerutxo_test

import (
	"math/rand/v2"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// testAgg sums an output's second byte under the key of its first byte mod
// 3; key 0 adds to no sum.
func testAgg(cbor []byte) ([]byte, uint64, bool) {
	if len(cbor) < 2 || cbor[0]%3 == 0 {
		return nil, 0, false
	}
	return []byte{cbor[0] % 3}, uint64(cbor[1]) + 1, true
}

func requireSums(t *testing.T, s *badgerutxo.Store) {
	t.Helper()
	want := map[string]uint64{}
	require.NoError(t, s.Each(func(o badgerutxo.Output) error {
		if k, n, ok := testAgg(o.Cbor); ok {
			want[string(k)] += n
		}
		return nil
	}))
	got := map[string]uint64{}
	require.NoError(t, s.EachSum(func(k []byte, n uint64) error {
		got[string(k)] = n
		return nil
	}))
	require.Equal(t, want, got)
}

// TestSumsFollowEveryChangeRollbackAndRebuild keeps the running sums equal to
// a recomputation over the live outputs through random batches, duplicate
// adds, rollbacks, a reopen and a rebuild.
func TestSumsFollowEveryChangeRollbackAndRebuild(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(7, 7))
	dir := t.TempDir()
	s, err := badgerutxo.Open(dir, badgerutxo.WithAggregator(testAgg))
	require.NoError(t, err)
	bootstrap(t, s, point(1, 1), out(1, 0), out(2, 1), out(3, 2))
	requireSums(t, s)
	var points []ocommon.Point
	for i := 2; i <= 30; i++ {
		var live []badgerutxo.Ref
		require.NoError(t, s.Each(func(o badgerutxo.Output) error {
			live = append(live, o.Ref)
			return nil
		}))
		var c badgerutxo.Change
		for _, r := range live {
			if rng.IntN(4) == 0 {
				c.Removed = append(c.Removed, r)
			}
		}
		c.Added = append(c.Added, out(byte(i), uint32(rng.IntN(5))))
		if len(live) > 0 {
			c.Added = append(c.Added, badgerutxo.Output{
				Ref: live[0], Cbor: []byte{byte(rng.IntN(256)), byte(i)},
			})
		}
		tip := point(uint64(i*10), byte(i))
		c.Point = tip
		_, err := s.ApplyBatch(&tip, []badgerutxo.Change{c})
		require.NoError(t, err)
		points = append(points, tip)
		requireSums(t, s)
	}
	for _, i := range []int{25, 12, 3} {
		_, err := s.Rollback(points[i])
		require.NoError(t, err)
		requireSums(t, s)
	}
	require.NoError(t, s.Close())
	s, err = badgerutxo.Open(dir, badgerutxo.WithAggregator(testAgg))
	require.NoError(t, err)
	defer s.Close()
	requireSums(t, s)
	bootstrap(t, s, point(5, 5), out(4, 4), out(5, 5))
	requireSums(t, s)
}
