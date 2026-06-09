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
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeetsStakeQuorumExactBoundary(t *testing.T) {
	met, err := MeetsStakeQuorum(75, 100, big.NewRat(3, 4))
	require.NoError(t, err)
	assert.True(t, met, "exactly tau * total must meet quorum")
}

func TestMeetsStakeQuorumOneUnder(t *testing.T) {
	met, err := MeetsStakeQuorum(74, 100, big.NewRat(3, 4))
	require.NoError(t, err)
	assert.False(t, met, "one lovelace under tau * total must fail")
}

func TestMeetsStakeQuorumRationalThreshold(t *testing.T) {
	// tau = 2/3 of 100: 66.67 -> 66 fails, 67 passes
	met, err := MeetsStakeQuorum(66, 100, big.NewRat(2, 3))
	require.NoError(t, err)
	assert.False(t, met)
	met, err = MeetsStakeQuorum(67, 100, big.NewRat(2, 3))
	require.NoError(t, err)
	assert.True(t, met)
}

func TestMeetsStakeQuorumZeroThreshold(t *testing.T) {
	met, err := MeetsStakeQuorum(0, 100, big.NewRat(0, 1))
	require.NoError(t, err)
	assert.True(t, met, "tau = 0 is met by zero voted stake")
}

func TestMeetsStakeQuorumLargeValuesNoOverflow(t *testing.T) {
	met, err := MeetsStakeQuorum(
		math.MaxUint64, math.MaxUint64, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	assert.True(t, met)

	met, err = MeetsStakeQuorum(
		math.MaxUint64-1, math.MaxUint64, big.NewRat(1, 1),
	)
	require.NoError(t, err)
	assert.False(t, met)
}

func TestMeetsStakeQuorumInvalidThreshold(t *testing.T) {
	for _, tau := range []*big.Rat{
		nil,
		big.NewRat(-1, 4),
		big.NewRat(5, 4),
	} {
		_, err := MeetsStakeQuorum(50, 100, tau)
		assert.ErrorIs(
			t, err, ErrInvalidQuorumStakeThreshold, "tau=%v", tau,
		)
	}
}

func TestMeetsStakeQuorumZeroTotalStake(t *testing.T) {
	_, err := MeetsStakeQuorum(50, 0, big.NewRat(3, 4))
	assert.ErrorIs(t, err, ErrZeroTotalActiveStake)
}
