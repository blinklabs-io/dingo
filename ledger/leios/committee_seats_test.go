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

package leios

import (
	"encoding/hex"
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCommitteeSeatsForEpoch serves seats in voter id order, each weighted by
// its share of the total active stake and carrying the member's BLS key.
func TestCommitteeSeatsForEpoch(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)

	seats, err := fixture.mgr.CommitteeSeatsForEpoch(5)
	require.NoError(t, err)

	require.Len(t, seats, len(fixture.members))
	total := new(big.Rat)
	for i, member := range fixture.members {
		require.Zero(
			t,
			big.NewRat(int64(member.Stake), 550).Cmp(seats[i].Weight),
			"seat %d weight",
			i,
		)
		require.Equal(
			t,
			fixture.registryEntries[hex.EncodeToString(member.PoolKeyHash)],
			hex.EncodeToString(seats[i].PublicKey),
			"seat %d key",
			i,
		)
		total.Add(total, seats[i].Weight)
	}
	// Every pool is seated, so the weights make up the whole stake.
	require.Zero(t, big.NewRat(1, 1).Cmp(total))
}

func TestCommitteeSeatsForEpochPropagatesFailure(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)
	snapshotErr := errors.New("snapshot not ready")
	fixture.stake.setError(snapshotErr)

	seats, err := fixture.mgr.CommitteeSeatsForEpoch(7)
	require.ErrorIs(t, err, snapshotErr)
	require.Nil(t, seats)
}
