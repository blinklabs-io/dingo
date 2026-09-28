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

package fixtures

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func TestGenerateConwayChain(t *testing.T) {
	blocks, err := GenerateConwayChain(3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)

	for i, block := range blocks {
		require.NotEmpty(t, block.Cbor())
		require.Empty(t, block.Transactions())
		if i > 0 {
			require.Equal(t, blocks[i-1].Hash(), block.PrevHash())
		}
	}
}

func TestGenerateConwayChainWithTransactions(t *testing.T) {
	blocks, err := GenerateConwayChainWithTransactions(3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)

	for _, block := range blocks {
		require.NotEmpty(t, block.Cbor())
		require.NotEmpty(t, block.Transactions())
	}
}

func TestGenerateConwayChainWithPeriodicTransactions(t *testing.T) {
	blocks, err := GenerateConwayChainWithPeriodicTransactions(6, 2)
	require.NoError(t, err)
	require.Len(t, blocks, 6)
	for i, block := range blocks {
		if i > 0 {
			require.Equal(t, blocks[i-1].Hash(), block.PrevHash())
		}
		if i%3 == 2 {
			require.NotEmpty(t, block.Transactions())
		} else {
			require.Empty(t, block.Transactions())
		}
	}
}

func TestGenerateBabbageChainContract(t *testing.T) {
	blocks, err := GenerateBabbageChain(3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)

	var origin common.Blake2b256
	for i, block := range blocks {
		require.EqualValues(t, babbage.EraIdBabbage, block.Era().Id)
		require.Equal(t, uint64(i+1), block.BlockNumber())
		require.Equal(t, uint64(2+i*20), block.SlotNumber())
		if i == 0 {
			require.Equal(t, origin, block.PrevHash())
			continue
		}
		require.Equal(t, blocks[i-1].Hash(), block.PrevHash())
	}
}

func TestGenerateConwayChainAtContract(t *testing.T) {
	blocks, err := GenerateConwayChainAt(99, 4_000, 3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	for i, block := range blocks {
		require.EqualValues(t, conway.EraIdConway, block.Era().Id)
		require.Equal(t, uint64(99+i), block.BlockNumber())
		require.Equal(t, uint64(4_000+i), block.SlotNumber())
	}
}
