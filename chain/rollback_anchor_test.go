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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// anchorChain holds the first n test blocks (block indexes 1..n) with K=3.
func anchorChain(t *testing.T, n int) (*chain.ChainManager, *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, 3)
	c := cm.PrimaryChain()
	for _, b := range testBlocks[:n] {
		require.NoError(t, c.AddBlock(context.Background(), b, nil))
	}
	return cm, c
}

func TestChainRollbackBoundSurvivesRestart(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, 3)
	c := cm.PrimaryChain()
	for _, b := range testBlocks[:6] {
		require.NoError(t, c.AddBlock(context.Background(), b, nil))
	}
	require.NoError(t, c.Rollback(context.Background(), blockPoint(testBlocks[2])))

	// A fresh manager over the same database must still measure from the
	// highest block index held before the rollback (6), not the lowered tip.
	cm2, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm2, 3)
	c2 := cm2.PrimaryChain()
	err = c2.Rollback(context.Background(), blockPoint(testBlocks[1]))
	require.ErrorIs(t, err, chain.ErrRollbackExceedsSecurityParam)
	require.Equal(t, blockPoint(testBlocks[2]), c2.Tip().Point)
}

func TestChainResetRollbackAnchorToTip(t *testing.T) {
	t.Parallel()

	_, c := anchorChain(t, 6)
	require.NoError(t, c.Rollback(context.Background(), blockPoint(testBlocks[2])))
	target := blockPoint(testBlocks[1])
	require.ErrorIs(
		t,
		c.Rollback(context.Background(), target),
		chain.ErrRollbackExceedsSecurityParam,
	)
	require.NoError(t, c.ResetRollbackAnchorToTip())
	require.NoError(t, c.Rollback(context.Background(), target))
	require.Equal(t, target, c.Tip().Point)
}

func TestChainRollbackAnchorRestoredWhenBatchFails(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, 3)
	c := cm.PrimaryChain()
	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 12)
	require.NoError(t, c.AddBlocks(context.Background(), blocks[:8]))

	// The first two blocks are written and advance the anchor to 10 before
	// the third, whose parent is absent, fails the batch.
	err = c.AddBlocks(
		context.Background(),
		[]ledger.Block{blocks[8], blocks[9], blocks[11]},
	)
	require.Error(t, err)
	require.Equal(t, pointOfBlock(blocks[7]), c.Tip().Point)

	// Index 5 is exactly K behind the pre-batch anchor (8) and past K behind
	// the advanced one.
	require.NoError(
		t,
		c.Rollback(context.Background(), pointOfBlock(blocks[4])),
		"a failed batch must not leave its advanced anchor behind",
	)
}

func TestChainRollbackAnchorRestoredWhenCallerTxnAborts(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, 3)
	c := cm.PrimaryChain()
	for _, b := range testBlocks[:4] {
		require.NoError(t, c.AddBlock(context.Background(), b, nil))
	}
	txn := addOnCallerTxn(t, db, c)
	require.NoError(t, txn.Rollback())
	require.Equal(t, blockPoint(testBlocks[3]), c.Tip().Point)

	// Index 1 is exactly K behind the pre-add anchor (4) and past K behind
	// the one the aborted add advanced (5).
	require.NoError(
		t,
		c.Rollback(context.Background(), blockPoint(testBlocks[0])),
		"an aborted caller transaction must not leave its advanced anchor",
	)
}
