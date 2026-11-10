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

package sqlstore

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestGetEpochStakesPastTheEnd covers a page that starts at or past the
// registered total: it is empty and still reports the real total, which counts
// registered stakes only.
func TestGetEpochStakesPastTheEnd(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	var inputs []*models.RewardStakeInput
	for i := range 5 {
		key := bytesRepeat(0xe0, 28)
		key[27] = byte(i)
		inputs = append(inputs, &models.RewardStakeInput{
			Epoch: 7, CredentialTag: 0, StakingKey: key,
			PoolKeyHash: bytesRepeat(0xe1, 28),
			Stake:       types.Uint64(uint64(i) + 1),
			// The last stake is deregistered and must not be counted.
			Registered: i < 4,
		})
	}
	require.NoError(t, store.SaveRewardStakeInputs(inputs, nil))

	rows, total, err := store.GetEpochStakes(7, nil, 2, 2, nil)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Len(t, rows, 2)

	for _, offset := range []int{4, 5, 1000} {
		rows, total, err = store.GetEpochStakes(7, nil, 2, offset, nil)
		require.NoError(t, err, offset)
		require.Equal(t, 4, total, offset)
		require.NotNil(t, rows, offset)
		require.Empty(t, rows, offset)
	}

	rows, total, err = store.GetEpochStakes(
		7, bytesRepeat(0xe1, 28), 2, 4, nil,
	)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Empty(t, rows)
}
