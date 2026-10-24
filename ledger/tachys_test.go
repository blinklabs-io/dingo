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
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestTachysPoolsIgnoreZeroStakeBuckets(t *testing.T) {
	t.Parallel()

	var positiveID, zeroID common.PoolKeyHash
	positiveID[0] = 1
	zeroID[0] = 2
	pools, err := tachysPoolsFromStakeDistribution(map[string]uint64{
		hex.EncodeToString(positiveID[:]): 9,
		hex.EncodeToString(zeroID[:]):     0,
	}, 9)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, positiveID, pools[0].ID)
	require.Equal(t, uint64(9), pools[0].Stake)
}

func TestTachysPoolsRejectMismatchedSnapshotTotal(t *testing.T) {
	t.Parallel()

	var poolID common.PoolKeyHash
	poolID[0] = 1
	_, err := tachysPoolsFromStakeDistribution(map[string]uint64{
		hex.EncodeToString(poolID[:]): 9,
	}, 10)
	require.ErrorContains(t, err, "total mismatch")
}
