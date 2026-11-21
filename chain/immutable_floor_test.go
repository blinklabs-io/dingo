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
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newImmutableChain(t *testing.T) (*database.Database, *chain.Chain) {
	t.Helper()
	return newImmutableChainWithK(t, 100)
}

func newImmutableChainWithK(
	t *testing.T,
	securityParam int,
) (*database.Database, *chain.Chain) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 21600,
		},
	})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, securityParam)
	c := cm.PrimaryChain()
	blocks := generateTestChain(t, 1, common.Blake2b256{}, 20, 20, 10)
	for _, b := range blocks {
		require.NoError(t, c.AddBlock(context.Background(), b, nil))
	}
	return db, c
}

func TestRollbackRefusesBelowImmutableTip(t *testing.T) {
	t.Parallel()
	db, c := newImmutableChain(t)
	_, err := db.PromoteThrough(context.Background(), 5)
	require.NoError(t, err)

	below := pointOfBlockFromDB(t, db, 4)
	for name, fn := range map[string]func() error{
		"validate":  func() error { return c.ValidateRollback(t.Context(), below) },
		"bounded":   func() error { return c.Rollback(t.Context(), below) },
		"unbounded": func() error { return c.RollbackUnbounded(t.Context(), below) },
	} {
		require.ErrorIs(t, fn(), database.ErrRollbackBelowImmutableTip, name)
	}
	require.Equal(t, uint64(10), c.Tip().BlockNumber)

	// Rolling back to the tip itself keeps it and is allowed.
	require.NoError(t, c.Rollback(t.Context(), pointOfBlockFromDB(t, db, 5)))
	require.Equal(t, uint64(5), c.Tip().BlockNumber)
}

// A bounded rollback below the immutable tip is also deeper than K, and must
// report the security-parameter error callers classify as a resync. The
// unbounded form skips the K bound, so the immutable floor still applies.
func TestBoundedRollbackBelowImmutableTipExceedsSecurityParam(t *testing.T) {
	t.Parallel()
	db, c := newImmutableChainWithK(t, 3)
	_, err := db.PromoteThrough(context.Background(), 5)
	require.NoError(t, err)

	origin := ocommon.Point{}
	below := pointOfBlockFromDB(t, db, 4)
	for name, point := range map[string]ocommon.Point{
		"origin": origin,
		"below":  below,
	} {
		require.ErrorIs(
			t,
			c.ValidateRollback(t.Context(), point),
			chain.ErrRollbackExceedsSecurityParam,
			"validate "+name,
		)
		require.ErrorIs(
			t,
			c.Rollback(t.Context(), point),
			chain.ErrRollbackExceedsSecurityParam,
			"bounded "+name,
		)
		require.ErrorIs(
			t,
			c.RollbackUnbounded(t.Context(), point),
			database.ErrRollbackBelowImmutableTip,
			"unbounded "+name,
		)
	}
	require.Equal(t, uint64(10), c.Tip().BlockNumber)
}

// A target below the immutable tip that is deeper than K reports both
// refusals, so callers that classify on either see it. Once the anchor is
// reset to the tip, a target within K of the anchor reports only the floor.
func TestBoundedRollbackBelowImmutableTipReportsFloor(t *testing.T) {
	t.Parallel()
	db, c := newImmutableChainWithK(t, 3)
	_, err := db.PromoteThrough(context.Background(), 5)
	require.NoError(t, err)
	below := pointOfBlockFromDB(t, db, 4)

	deep := c.ValidateRollback(t.Context(), below)
	require.ErrorIs(t, deep, chain.ErrRollbackExceedsSecurityParam)
	require.ErrorIs(t, deep, database.ErrRollbackBelowImmutableTip)

	require.NoError(
		t, c.Rollback(t.Context(), pointOfBlockFromDB(t, db, 7)),
	)
	require.NoError(t, c.ResetRollbackAnchorToTip())
	shallow := c.ValidateRollback(t.Context(), below)
	require.ErrorIs(t, shallow, database.ErrRollbackBelowImmutableTip)
	require.NotErrorIs(t, shallow, chain.ErrRollbackExceedsSecurityParam)
}

func TestPointAtDepthReadsNoBodies(t *testing.T) {
	t.Parallel()
	db, c := newImmutableChain(t)
	_, err := db.PromoteThrough(context.Background(), 8)
	require.NoError(t, err)
	// Remove the chunk files: a body read for a promoted block would fail.
	require.NoError(t, removeAll(db.ImmutableDir()))
	want := pointOfBlockFromDB(t, db, 3)
	got, found, err := c.PointAtDepth(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, want, got)
}

func removeAll(dir string) error { return os.RemoveAll(dir) }

func pointOfBlockFromDB(
	t *testing.T,
	db *database.Database,
	index uint64,
) ocommon.Point {
	t.Helper()
	p, err := db.BlockPointByIndex(index, nil)
	require.NoError(t, err)
	return p
}

// Chainsync and blockfetch serve through chain iterators and BlockByPoint;
// both must return the original block bytes once bodies live in chunk files,
// across the boundary between promoted and unpromoted blocks.
func TestChainIteratorsServePromotedBlocks(t *testing.T) {
	t.Parallel()
	db, c := newImmutableChain(t)
	want := make([][]byte, 11)
	for i := uint64(1); i <= 10; i++ {
		b, err := db.BlockByIndex(i, nil)
		require.NoError(t, err)
		want[i] = b.Cbor
	}
	_, err := db.PromoteThrough(context.Background(), 6)
	require.NoError(t, err)

	for _, start := range []uint64{1, 4, 6, 7} {
		iter, err := c.FromPoint(pointOfBlockFromDB(t, db, start), true)
		require.NoError(t, err)
		for i := start; i <= 10; i++ {
			res, err := iter.Next(false)
			require.NoError(t, err)
			require.False(t, res.Rollback)
			require.Equal(
				t,
				want[i],
				res.Block.Cbor,
				"from %d, block %d",
				start,
				i,
			)
		}
		iter.Cancel()
	}

	for i := uint64(1); i <= 10; i++ {
		b, err := c.BlockByPoint(
			t.Context(), pointOfBlockFromDB(t, db, i), nil,
		)
		require.NoError(t, err)
		require.Equal(t, want[i], b.Cbor, "by point %d", i)
	}
}
