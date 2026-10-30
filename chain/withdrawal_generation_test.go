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

package chain_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// A reader may reuse a positive membership answer while WithdrawalGeneration
// is unchanged, so every path that takes a block or queued header back must
// advance it, and adds that only extend the chain must not.
func TestChainWithdrawalGenerationAdvancesOnEveryWithdrawal(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*chain.Chain, *database.Database) {
		t.Helper()
		db := newTestDB(t)
		cm, err := chain.NewManager(t.Context(), db, nil)
		require.NoError(t, err)
		mustSetLedger(t, cm, 100)
		c := cm.PrimaryChain()
		for _, block := range testBlocks[:3] {
			require.NoError(t, c.AddBlock(t.Context(), block, nil))
		}
		return c, db
	}
	for _, tc := range []struct {
		name     string
		mutate   func(t *testing.T, c *chain.Chain, db *database.Database)
		advances bool
	}{
		{
			name: "block add",
			mutate: func(t *testing.T, c *chain.Chain, _ *database.Database) {
				require.NoError(t, c.AddBlock(t.Context(), testBlocks[3], nil))
			},
		},
		{
			name: "header add and the block it announced",
			mutate: func(t *testing.T, c *chain.Chain, _ *database.Database) {
				require.NoError(t, c.AddBlockHeader(t.Context(), testBlocks[3]))
				require.NoError(t, c.AddBlock(t.Context(), testBlocks[3], nil))
			},
		},
		{
			name: "clear of an empty header queue",
			mutate: func(_ *testing.T, c *chain.Chain, _ *database.Database) {
				c.ClearHeaders()
			},
		},
		{
			name: "block rollback",
			mutate: func(t *testing.T, c *chain.Chain, _ *database.Database) {
				require.NoError(t, c.Rollback(t.Context(), blockPoint(testBlocks[0])))
			},
			advances: true,
		},
		{
			name: "queued header rollback",
			mutate: func(t *testing.T, c *chain.Chain, _ *database.Database) {
				require.NoError(t, c.AddBlockHeader(t.Context(), testBlocks[3]))
				require.NoError(t, c.AddBlockHeader(t.Context(), testBlocks[4]))
				require.NoError(t, c.Rollback(t.Context(), blockPoint(testBlocks[3])))
			},
			advances: true,
		},
		{
			name: "header queue clear",
			mutate: func(t *testing.T, c *chain.Chain, _ *database.Database) {
				require.NoError(t, c.AddBlockHeader(t.Context(), testBlocks[3]))
				c.ClearHeaders()
			},
			advances: true,
		},
		{
			name: "add of a block the queued header did not announce",
			mutate: func(t *testing.T, c *chain.Chain, _ *database.Database) {
				require.NoError(t, c.AddBlockHeader(t.Context(), testBlocks[3]))
				require.NoError(t, c.AddLocalBlock(t.Context(), &MockBlock{
					MockBlockNumber: testBlocks[3].MockBlockNumber,
					MockSlot:        testBlocks[3].MockSlot + 1,
					MockHash:        testHashPrefix + "00ff",
					MockPrevHash:    testBlocks[2].MockHash,
				}))
				require.Zero(t, c.HeaderCount())
			},
			advances: true,
		},
		{
			name: "failed batch restore",
			mutate: func(t *testing.T, c *chain.Chain, _ *database.Database) {
				// testBlocks[5] does not fit testBlocks[3], so the batch fails
				// after the first block was added and the chain is restored.
				err := c.AddBlocks(t.Context(), []ledger.Block{
					testBlocks[3], testBlocks[5],
				})
				require.Error(t, err)
				require.Equal(t, blockPoint(testBlocks[2]), c.Tip().Point)
			},
			advances: true,
		},
		{
			name: "rolled-back caller transaction add",
			mutate: func(t *testing.T, c *chain.Chain, db *database.Database) {
				txn := db.BlockBlobTxn()
				require.NoError(t, c.AddBlock(t.Context(), testBlocks[3], txn))
				require.NoError(t, txn.Rollback())
				require.Equal(t, blockPoint(testBlocks[2]), c.Tip().Point)
			},
			advances: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, db := setup(t)
			before := c.WithdrawalGeneration()
			tc.mutate(t, c, db)
			if tc.advances {
				require.Greater(t, c.WithdrawalGeneration(), before,
					"a withdrawal must advance the generation")
			} else {
				require.Equal(t, before, c.WithdrawalGeneration(),
					"a path that withdraws nothing must leave the generation")
			}
		})
	}
}
