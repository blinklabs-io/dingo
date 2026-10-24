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

package consensus

import (
	"testing"

	gconsensus "github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestTachysDistributionIsCanonicalAndImmutable(t *testing.T) {
	t.Parallel()

	var firstID, secondID common.PoolKeyHash
	firstID[0] = 1
	secondID[0] = 2
	input := []gconsensus.TachysPool{
		{ID: secondID, Stake: 7},
		{ID: firstID, Stake: 3},
	}
	distribution, err := NewTachysDistribution(input, 10)
	require.NoError(t, err)
	commitment := distribution.Commitment()

	input[0].Stake = 9
	got := distribution.Pools()
	require.Equal(t, []gconsensus.TachysPool{
		{ID: firstID, Stake: 3},
		{ID: secondID, Stake: 7},
	}, got)
	got[0].Stake = 10
	require.Equal(t, uint64(3), distribution.Pools()[0].Stake)
	require.Equal(t, commitment, distribution.Commitment())

	reordered, err := NewTachysDistribution([]gconsensus.TachysPool{
		{ID: firstID, Stake: 3},
		{ID: secondID, Stake: 7},
	}, 10)
	require.NoError(t, err)
	require.Equal(t, commitment, reordered.Commitment())

	changed, err := NewTachysDistribution([]gconsensus.TachysPool{
		{ID: firstID, Stake: 4},
		{ID: secondID, Stake: 6},
	}, 10)
	require.NoError(t, err)
	require.NotEqual(t, commitment, changed.Commitment())
}

func TestTachysDistributionRejectsInconsistentTotal(t *testing.T) {
	t.Parallel()

	var poolID common.PoolKeyHash
	poolID[0] = 1
	_, err := NewTachysDistribution([]gconsensus.TachysPool{
		{ID: poolID, Stake: 1},
	}, 2)
	require.ErrorContains(t, err, "total stake mismatch")
}
