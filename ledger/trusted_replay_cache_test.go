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

	"github.com/blinklabs-io/dingo/utxoref"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestTrimTrustedReplayUtxosBoundsCache(t *testing.T) {
	t.Parallel()

	utxos := make(map[utxoref.Key]lcommon.Utxo, trustedReplayUtxoCacheLimit+32)
	for idx := range trustedReplayUtxoCacheLimit + 32 {
		utxos[utxoref.Key{
			TxId:  lcommon.Blake2b256{byte(idx >> 8), byte(idx)},
			Index: uint32(idx), //nolint:gosec
		}] = lcommon.Utxo{}
	}
	trimTrustedReplayUtxos(utxos)
	require.Len(t, utxos, trustedReplayUtxoCacheLimit)
}

func TestTrustedReplayOpCertCounterCachePromotesOnlyCommittedState(t *testing.T) {
	t.Parallel()

	pool := lcommon.PoolKeyHash{1}
	committed := map[lcommon.PoolKeyHash]opCertCounterSnapshot{
		pool: {sequence: 2, found: true},
	}
	failed := &trustedReplayOpCertCounterCache{
		committed: committed,
		staged:    make(map[lcommon.PoolKeyHash]opCertCounterSnapshot),
	}
	failed.advance(pool, 3)
	require.Equal(t, uint64(2), committed[pool].sequence)

	committedTxn := &trustedReplayOpCertCounterCache{
		committed: committed,
		staged:    make(map[lcommon.PoolKeyHash]opCertCounterSnapshot),
	}
	committedTxn.advance(pool, 4)
	committedTxn.promote()
	require.Equal(t, uint64(4), committed[pool].sequence)
}

func TestTrustedReplayOpCertCounterCacheSeedsOnMiss(t *testing.T) {
	t.Parallel()

	pool := lcommon.PoolKeyHash{1}
	cache := &trustedReplayOpCertCounterCache{
		committed: make(map[lcommon.PoolKeyHash]opCertCounterSnapshot),
		staged:    make(map[lcommon.PoolKeyHash]opCertCounterSnapshot),
	}

	cache.advance(pool, 7)
	counter, ok := cache.lookup(pool)
	require.True(t, ok)
	require.True(t, counter.found)
	require.Equal(t, uint64(7), counter.sequence)
}

func TestTrimTrustedReplayOpCertCountersBoundsCache(t *testing.T) {
	t.Parallel()

	counters := make(
		map[lcommon.PoolKeyHash]opCertCounterSnapshot,
		trustedReplayOpCertCounterCacheLimit+32,
	)
	for idx := range trustedReplayOpCertCounterCacheLimit + 32 {
		counters[lcommon.PoolKeyHash{
			byte(idx >> 16), byte(idx >> 8), byte(idx),
		}] = opCertCounterSnapshot{sequence: uint64(idx), found: true}
	}

	trimTrustedReplayOpCertCounters(counters)
	require.Len(t, counters, trustedReplayOpCertCounterCacheLimit)
}

func TestTrustedReplayBoundaryCounterStateCachesPerSlot(t *testing.T) {
	t.Parallel()

	state := &trustedReplayBoundaryCounterState{}
	checks := 0
	check := func() (bool, error) {
		checks++
		return false, nil
	}

	imported, err := state.hasCountersAtSlot(42, check)
	require.NoError(t, err)
	require.False(t, imported)
	imported, err = state.hasCountersAtSlot(42, check)
	require.NoError(t, err)
	require.False(t, imported)
	require.Equal(t, 1, checks)

	imported, err = state.hasCountersAtSlot(43, func() (bool, error) {
		checks++
		return true, nil
	})
	require.NoError(t, err)
	require.True(t, imported)
	require.Equal(t, 2, checks)
}
