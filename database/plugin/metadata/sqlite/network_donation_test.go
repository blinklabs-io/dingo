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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNetworkDonation exercises the donation accumulator: per-epoch summing,
// idempotent-per-slot writes, and rollback via DeleteNetworkDonationsAfterSlot.
func TestNetworkDonation(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	require.NoError(t, store.AddNetworkDonation(100, 5, 1_000_000, nil))
	require.NoError(t, store.AddNetworkDonation(200, 5, 2_000_000, nil))
	require.NoError(t, store.AddNetworkDonation(600, 6, 3_000_000, nil))

	sum5, err := store.SumNetworkDonationsForEpoch(5, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3_000_000), sum5)

	sum6, err := store.SumNetworkDonationsForEpoch(6, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3_000_000), sum6)

	// An epoch with no donations sums to zero (not an error).
	sum7, err := store.SumNetworkDonationsForEpoch(7, nil)
	require.NoError(t, err)
	require.Zero(t, sum7)

	// Re-applying the same slot overwrites rather than double-counting.
	require.NoError(t, store.AddNetworkDonation(200, 5, 5_000_000, nil))
	sum5, err = store.SumNetworkDonationsForEpoch(5, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(6_000_000), sum5) // 1_000_000 + 5_000_000

	// Rollback to slot 150 drops the slot-200 (epoch 5) and slot-600 (epoch 6)
	// rows; the slot-100 row survives.
	require.NoError(t, store.DeleteNetworkDonationsAfterSlot(150, nil))

	sum5, err = store.SumNetworkDonationsForEpoch(5, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_000), sum5)

	sum6, err = store.SumNetworkDonationsForEpoch(6, nil)
	require.NoError(t, err)
	require.Zero(t, sum6)
}
