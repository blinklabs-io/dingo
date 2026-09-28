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
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rollbackWindowFixture reproduces the state a node holds while a rollback's
// metadata truncation is still in flight.
//
// rollbackChainAndStateDeferred rewinds ls.chain (which physically removes the
// rolled-away block rows) before calling ls.rollback, and ls.rollback only
// assigns ls.currentTip once TruncateAfterSlot has committed. On a large
// metadata database that truncation takes tens of seconds, and for that whole
// window the chain tip is the rollback point while ls.currentTip still names
// the block the chain rollback already deleted.
type rollbackWindowFixture struct {
	ls          *LedgerState
	blocks      []models.Block
	rollbackTo  models.Block
	staleLedger ochainsync.Tip
}

func newRollbackWindowFixture(t *testing.T) *rollbackWindowFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 5}))

	tipBlock := blocks[len(blocks)-1]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(tipBlock),
		BlockNumber: tipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip

	// Rewind the chain exactly as rollbackChainAndStateDeferred does, and
	// deliberately do NOT run ls.rollback: this is the in-flight-truncation
	// window.
	rollbackTo := blocks[2]
	require.NoError(t, ls.chain.Rollback(makeTestPoint(rollbackTo)))

	return &rollbackWindowFixture{
		ls:          ls,
		blocks:      blocks,
		rollbackTo:  rollbackTo,
		staleLedger: ledgerTip,
	}
}

// TestRollbackWindowPreconditions pins the exact state the bug depends on, so
// a future change that makes the window unreachable fails here loudly rather
// than silently turning the regression tests below into tautologies.
func TestRollbackWindowPreconditions(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// The chain has been rewound to the rollback point...
	chainTip := f.ls.chain.Tip()
	require.Equal(t, f.rollbackTo.Slot, chainTip.Point.Slot)

	// ...but the ledger tip still names the block that rewind deleted.
	f.ls.RLock()
	ledgerTip := f.ls.currentTip
	f.ls.RUnlock()
	require.Equal(t, f.staleLedger.Point.Slot, ledgerTip.Point.Slot)
	require.Greater(t, ledgerTip.Point.Slot, chainTip.Point.Slot)

	// The ledger tip's block row is gone, which is what makes
	// authoritativeRecentChainPoints bail out.
	_, err := database.BlockByPoint(f.ls.db, ledgerTip.Point)
	require.ErrorIs(t, err, models.ErrBlockNotFound)

	// And the "chain is usable" guard is false, so IntersectPoints takes the
	// authoritative path rather than the chain path.
	require.False(t, f.ls.primaryChainTipAtOrAheadOfLedgerTip())
}

// TestAuthoritativeRecentChainPointsFallsBackToChainTipWhenLedgerTipMissing
// asserts the ledger never reports "I have no chain points" while it demonstrably
// holds a chain. Before the fix this returned an empty slice with a nil error,
// which buildDefaultChainsyncIntersectPoints turned into an origin-only
// MsgFindIntersect.
func TestAuthoritativeRecentChainPointsFallsBackToChainTipWhenLedgerTipMissing(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		points,
		"ledger reported no chain points while holding a chain tip at slot %d",
		f.ls.chain.Tip().Point.Slot,
	)

	// The newest point offered must be the rollback point (the chain tip),
	// not the deleted ledger tip.
	assert.Equal(t, f.rollbackTo.Slot, points[0].Slot)
	assert.Equal(t, f.rollbackTo.Hash, points[0].Hash)

	// No point may name a block that no longer exists.
	for _, point := range points {
		assert.LessOrEqual(
			t,
			point.Slot,
			f.rollbackTo.Slot,
			"offered a point above the rollback point",
		)
	}

	// Recent points below the tip must still be walked, so a peer that has
	// also rewound can intersect deeper than the tip.
	require.Greater(t, len(points), 1, "expected recent points below the tip")
	assert.Equal(t, f.blocks[1].Slot, points[1].Slot)
}

// TestIntersectPointsDoesNotCollapseToEmptyDuringRollbackWindow is the
// end-to-end ledger-level assertion: the entry point chainsync actually calls
// must not return an empty set in this window.
func TestIntersectPointsDoesNotCollapseToEmptyDuringRollbackWindow(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	points, err := f.ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		points,
		"IntersectPoints returned nothing during an in-flight rollback; "+
			"chainsync would offer origin only",
	)
	assert.Equal(t, f.rollbackTo.Slot, points[0].Slot)
	assert.Equal(t, f.rollbackTo.Hash, points[0].Hash)
}

// TestIntersectPointsStillEmptyAtOriginWithNoChain guards the fallback from
// over-reaching: a node that genuinely has no chain must still report no
// points, so a fresh node syncs from origin as designed.
func TestIntersectPointsStillEmptyAtOriginWithNoChain(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 5}))
	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	assert.Empty(t, points)
}

// TestAuthoritativeRecentChainPointsIgnoresChainTipAheadOfLedgerTip pins the
// boundary of the fallback introduced for the rollback window. A chain tip at
// or ahead of the ledger tip is unapplied forward work -- possibly a fork that
// does not descend from the ledger tip at all -- and must NOT be offered as an
// intersect point, which is the invariant the primary-chain ancestor check
// (#2309) exists to protect. Only a chain tip strictly below the ledger tip,
// the signature of an in-flight rewind, qualifies.
func TestAuthoritativeRecentChainPointsIgnoresChainTipAheadOfLedgerTip(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	// Extend the rewound chain past the (missing) ledger tip with a fork
	// block, so the chain tip is now ahead of the ledger tip.
	forkHash := bytes.Repeat([]byte{0xfe}, 32)
	require.NoError(t, f.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        f.staleLedger.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: f.staleLedger.BlockNumber + 1,
			Type:        1,
			PrevHash:    f.rollbackTo.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.Greater(
		t,
		f.ls.chain.Tip().Point.Slot,
		f.staleLedger.Point.Slot,
	)

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.NoError(t, err)
	assert.Empty(
		t,
		points,
		"must not offer unapplied forward chain state as intersect points",
	)
}

// countingWarnHandler counts Warn records carrying a given message.
type countingWarnHandler struct {
	slog.Handler
	message string
	count   *atomic.Int64
}

func (h countingWarnHandler) Handle(
	ctx context.Context,
	record slog.Record,
) error {
	if record.Level == slog.LevelWarn && record.Message == h.message {
		h.count.Add(1)
	}
	return nil
}

func (h countingWarnHandler) Enabled(
	_ context.Context,
	level slog.Level,
) bool {
	return level >= slog.LevelWarn
}

// TestRecentChainPointsFallbackAnchorPropagatesStorageError verifies a real
// storage failure is surfaced rather than silently degraded into "no anchor".
// Swallowing it would turn a transient database fault into an origin-only
// intersect list, i.e. a request that the peer replay the chain from genesis.
func TestRecentChainPointsFallbackAnchorPropagatesStorageError(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// Sanity: the anchor resolves while the database is healthy.
	block, ok, err := f.ls.recentChainPointsFallbackAnchor(f.staleLedger)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, f.rollbackTo.Slot, block.Slot)

	require.NoError(t, dbtest.CloseDatabase(f.ls.db))

	_, ok, err = f.ls.recentChainPointsFallbackAnchor(f.staleLedger)
	require.Error(t, err, "storage failure must not be swallowed")
	assert.False(t, ok)
	assert.NotErrorIs(
		t,
		err,
		models.ErrBlockNotFound,
		"a real storage fault must not be reported as a missing block",
	)
}

// TestAuthoritativeRecentChainPointsPropagatesAnchorStorageError verifies the
// propagated error reaches the caller instead of becoming an empty point list.
func TestAuthoritativeRecentChainPointsPropagatesAnchorStorageError(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)
	require.NoError(t, dbtest.CloseDatabase(f.ls.db))

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.Error(t, err)
	assert.Nil(t, points)
}

// TestIntersectAnchorFallbackWarnIsThrottled verifies the anchor-fallback
// warning does not flood. authoritativeRecentChainPoints runs on every
// chainsync client start, and during the truncation window peer governance
// reconnects roughly once a second across every peer, so an unthrottled
// warning would emit hundreds of lines for a single rollback.
func TestIntersectAnchorFallbackWarnIsThrottled(t *testing.T) {
	f := newRollbackWindowFixture(t)

	var warns atomic.Int64
	f.ls.config.Logger = slog.New(countingWarnHandler{
		Handler: slog.NewJSONHandler(io.Discard, nil),
		message: "ledger tip block missing, anchoring intersect points on primary chain tip",
		count:   &warns,
	})

	for range 50 {
		points, err := f.ls.authoritativeRecentChainPoints(4)
		require.NoError(t, err)
		require.NotEmpty(t, points)
	}

	assert.Equal(
		t,
		int64(1),
		warns.Load(),
		"anchor-fallback warning must be throttled, not emitted per call",
	)
}

// TestRollbackWindowIntersectAnchorReportsRollbackPoint verifies the exported
// anchor, which chainsync relies on, names the rollback point during the
// window.
func TestRollbackWindowIntersectAnchorReportsRollbackPoint(t *testing.T) {
	f := newRollbackWindowFixture(t)

	point, ok, err := f.ls.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, f.rollbackTo.Slot, point.Slot)
	assert.Equal(t, f.rollbackTo.Hash, point.Hash)
}

// TestRollbackWindowIntersectAnchorAbsentWhenLedgerTipPresent verifies the
// anchor is offered only inside the window: a self-consistent ledger whose tip
// row is readable needs no rescue.
func TestRollbackWindowIntersectAnchorAbsentWhenLedgerTipPresent(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// Move the ledger tip onto a block that still exists.
	f.ls.Lock()
	f.ls.currentTip = ochainsync.Tip{
		Point:       makeTestPoint(f.rollbackTo),
		BlockNumber: f.rollbackTo.Number,
	}
	f.ls.Unlock()

	_, ok, err := f.ls.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	assert.False(t, ok)
}
