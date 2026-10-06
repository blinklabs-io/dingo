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

package snapshot

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestStakeFetchPoolsRejectsMalformedDelegatedPool covers the delegated pool
// keys that widen the stake fetch to every delegated credential. Dropping a
// malformed key would leave that pool's delegators out of TotalActiveStake.
func TestStakeFetchPoolsRejectsMalformedDelegatedPool(t *testing.T) {
	t.Parallel()
	active := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x01}, 28))
	_, _, err := stakeFetchPools(
		[]lcommon.PoolKeyHash{active},
		[][]byte{bytes.Repeat([]byte{0x02}, 27)},
	)
	require.ErrorContains(t, err, "delegated pool key")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
}

func TestStakeFetchPoolsSkipsEmptyDelegatedPool(t *testing.T) {
	t.Parallel()
	active := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x01}, 28))
	delegated := bytes.Repeat([]byte{0x03}, 28)
	fetch, activeSet, err := stakeFetchPools(
		[]lcommon.PoolKeyHash{active},
		[][]byte{nil, delegated},
	)
	require.NoError(t, err)
	require.Equal(t, [][]byte{active[:], delegated}, fetch)
	require.Len(t, activeSet, 1)
}

type activePoolKeysStore struct {
	metadata.MetadataStore
	keys [][]byte
}

func (s activePoolKeysStore) GetEpochBoundaryActivePoolKeyHashes(
	uint64,
	uint64,
	types.Txn,
) ([][]byte, error) {
	return s.keys, nil
}

// TestGetActivePoolsAtSlotRejectsMalformedPoolKey covers the active pool set
// the stake distribution is built over. Skipping a malformed key would drop
// that pool from leader election and rewards.
func TestGetActivePoolsAtSlotRejectsMalformedPoolKey(t *testing.T) {
	t.Parallel()
	calc := &Calculator{}
	pools, err := calc.getActivePoolsAtBoundary(
		context.Background(),
		activePoolKeysStore{keys: [][]byte{
			bytes.Repeat([]byte{0x04}, 28),
			bytes.Repeat([]byte{0x05}, 27),
		}},
		nil,
		100,
		0,
	)
	require.ErrorContains(t, err, "active pool key at slot 100")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, pools)
}
