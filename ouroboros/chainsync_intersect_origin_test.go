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

package ouroboros

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAnchorPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, bytes.Repeat([]byte{0xab}, 32))
}

// TestFinalizeChainsyncIntersectPointsNeverOffersOriginAloneOnNonOriginChain is
// the regression test for the connection-recycle loop: while a rollback's
// metadata truncation is in flight the ledger can return no intersect points,
// and the "always append origin" fallback then made a fully synced node ask its
// peers to replay from genesis. Those genesis-era headers fail leader
// verification and recycle every connection until the truncation commits.
func TestFinalizeChainsyncIntersectPointsNeverOffersOriginAloneOnNonOriginChain(
	t *testing.T,
) {
	anchor := testAnchorPoint(2576716)

	for name, points := range map[string][]ocommon.Point{
		"nil points":      nil,
		"empty points":    {},
		"origin-only set": {ocommon.NewPointOrigin()},
	} {
		t.Run(name, func(t *testing.T) {
			got, rescued := finalizeChainsyncIntersectPoints(
				points,
				anchor,
				true,
			)

			require.NotEmpty(t, got)
			require.True(
				t,
				rescued,
				"expected the origin-only collapse to be reported",
			)
			require.False(
				t,
				len(got) == 1 && isOriginPoint(got[0]),
				"offered origin as the only intersect point while holding a rollback anchor at slot %d",
				anchor.Slot,
			)

			// The anchor leads, origin remains the last resort.
			assert.Equal(t, anchor.Slot, got[0].Slot)
			assert.Equal(t, anchor.Hash, got[0].Hash)
			assert.True(
				t,
				isOriginPoint(got[len(got)-1]),
				"origin must remain the final fallback point",
			)
		})
	}
}

// TestFinalizeChainsyncIntersectPointsKeepsOriginOnlyAtOrigin guards the other
// direction: a node that really is at origin (fresh sync) must still be allowed
// to ask for a full replay, otherwise it could never bootstrap.
func TestFinalizeChainsyncIntersectPointsKeepsOriginOnlyAtOrigin(t *testing.T) {
	got, rescued := finalizeChainsyncIntersectPoints(
		nil,
		ocommon.Point{},
		false,
	)

	require.Len(t, got, 1)
	assert.True(t, isOriginPoint(got[0]))
	assert.False(t, rescued)
}

// TestFinalizeChainsyncIntersectPointsPreservesRealPoints verifies the normal
// path is untouched: a healthy point list keeps its order and still gets origin
// appended as the final fallback for divergent-fork peers.
func TestFinalizeChainsyncIntersectPointsPreservesRealPoints(t *testing.T) {
	points := []ocommon.Point{
		ocommon.NewPoint(300, bytes.Repeat([]byte{0x03}, 32)),
		ocommon.NewPoint(200, bytes.Repeat([]byte{0x02}, 32)),
		ocommon.NewPoint(100, bytes.Repeat([]byte{0x01}, 32)),
	}

	got, rescued := finalizeChainsyncIntersectPoints(
		points,
		testAnchorPoint(300),
		true,
	)

	assert.False(t, rescued)
	require.Len(t, got, len(points)+1)
	for idx, point := range points {
		assert.Equal(t, point.Slot, got[idx].Slot)
		assert.Equal(t, point.Hash, got[idx].Hash)
	}
	assert.True(t, isOriginPoint(got[len(got)-1]))
}

// TestFinalizeChainsyncIntersectPointsDoesNotDoubleAppendOrigin verifies a list
// that already ends in origin is left alone.
func TestFinalizeChainsyncIntersectPointsDoesNotDoubleAppendOrigin(
	t *testing.T,
) {
	points := []ocommon.Point{
		ocommon.NewPoint(300, bytes.Repeat([]byte{0x03}, 32)),
		ocommon.NewPointOrigin(),
	}

	got, rescued := finalizeChainsyncIntersectPoints(
		points,
		testAnchorPoint(300),
		true,
	)

	assert.False(t, rescued)
	require.Len(t, got, 2)
	assert.True(t, isOriginPoint(got[1]))
}

// newTestLedgerStateWithChain builds a ledger whose primary chain holds
// blockCount blocks, so PrimaryChainTip reports a real (non-origin) tip.
func newTestLedgerStateWithChain(
	t *testing.T,
	blockCount uint64,
) (*ledger.LedgerState, *database.Database) {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	var prevHash []byte
	for slot := uint64(1); slot <= blockCount; slot++ {
		hash := bytes.Repeat([]byte{byte(slot)}, 32)
		require.NoError(t, db.BlockCreate(models.Block{
			ID:       slot,
			Slot:     slot,
			Number:   slot,
			Hash:     hash,
			PrevHash: prevHash,
			Type:     1,
			Cbor:     []byte{0x80},
		}, nil))
		prevHash = hash
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2160}),
	)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return ls, db
}

// TestChainsyncNeverAsksPeerToReplayFromGenesisDuringRollback is the
// composition test for the incident: a node holding a real chain whose ledger
// tip block row has been removed (the window between the chain rewind and the
// end of the metadata truncation) must not build an origin-only FindIntersect
// list. It exercises both halves of the fix together -- the ledger anchoring
// its points on the chain tip, and chainsync refusing an origin-only list --
// because either one alone is sufficient to preserve the invariant.
func TestChainsyncNeverAsksPeerToReplayFromGenesisDuringRollback(t *testing.T) {
	o := &Ouroboros{}
	o.ledgerState, _ = newTestLedgerStateWithChain(t, 5)

	chainTip := o.ledgerState.PrimaryChainTip()
	require.False(
		t,
		isOriginPoint(chainTip.Point),
		"fixture must hold a non-origin chain",
	)

	// The ledger tip names a block that is not in the metadata database,
	// which is what a chain rewind leaves behind until ls.rollback's
	// truncation commits and reassigns ls.currentTip.
	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ocommon.NewPoint(2576729, bytes.Repeat([]byte{0xe8}, 32)),
		BlockNumber: 112915,
	})

	points, err := o.ledgerState.IntersectPoints(chainsyncIntersectPointCount)
	require.NoError(t, err)

	anchor, hasAnchor, err := o.ledgerState.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	got, _ := finalizeChainsyncIntersectPoints(
		normalizeIntersectPoints(points),
		anchor,
		hasAnchor,
	)

	require.NotEmpty(t, got)
	require.False(
		t,
		len(got) == 1 && isOriginPoint(got[0]),
		"chainsync would have asked the peer to replay from genesis while holding a chain at slot %d",
		chainTip.Point.Slot,
	)
	assert.False(
		t,
		isOriginPoint(got[0]),
		"the leading intersect point must be a real chain point",
	)
	assert.True(
		t,
		isOriginPoint(got[len(got)-1]),
		"origin must remain the final fallback point",
	)
}

// TestFinalizeChainsyncIntersectPointsRefusesAheadForkAnchor is the regression
// test for the review finding that the origin-only rescue re-introduced the
// #2309 violation one layer up.
//
// When the point list is empty because the primary chain is AHEAD on a fork
// that does not contain the applied ledger tip, the ledger deliberately reports
// no rollback anchor. Seeding from the raw chain tip in that case would
// advertise unapplied fork state. The list must stay origin-only, exactly as
// before the rescue existed.
func TestFinalizeChainsyncIntersectPointsRefusesAheadForkAnchor(t *testing.T) {
	got, rescued := finalizeChainsyncIntersectPoints(
		nil,
		// A caller that wrongly passes a real ahead-fork point must still
		// be refused when the ledger says there is no anchor.
		testAnchorPoint(999999),
		false,
	)

	assert.False(t, rescued, "must not rescue without a ledger-approved anchor")
	require.Len(t, got, 1)
	assert.True(
		t,
		isOriginPoint(got[0]),
		"an ahead-fork chain tip must never seed the intersect list",
	)
}

// ledgerTipHashAbsentFromChain is a hash that deliberately matches no block
// built by newTestLedgerStateWithChain (whose block at slot N has hash
// bytes.Repeat([]byte{byte(N)}, 32)). Using it as the ledger tip point makes
// that tip's metadata row absent AND places it off the primary chain, which is
// the state a chain rewind leaves behind.
//
// This distinction is load-bearing: swapping this for the real block hash
// converts the #2309 test below into the ordinary chain-ahead test beside it,
// which asserts the opposite outcome.
var ledgerTipHashAbsentFromChain = bytes.Repeat([]byte{0xe8}, 32)

// TestIntersectPointsChainAheadWithLedgerTipRowMissingStaysOriginOnly is the
// #2309 case: the primary chain is AHEAD of the ledger tip, and the ledger
// tip's own block row is missing (so the tip is not an ancestor on the primary
// chain either). primaryChainTipAtOrAheadOfLedgerTip's ancestor check fails,
// the authoritative path finds no tip row, and the ahead-gate refuses to anchor
// on unapplied forward work.
//
// Nothing may be advertised: the list must stay origin-only, matching upstream
// TestIntersectPointsDoesNotUsePrimaryChainWhenLedgerTipMissing.
func TestIntersectPointsChainAheadWithLedgerTipRowMissingStaysOriginOnly(
	t *testing.T,
) {
	o := &Ouroboros{}
	o.ledgerState, _ = newTestLedgerStateWithChain(t, 5)

	ledgerTipPoint := ocommon.NewPoint(2, ledgerTipHashAbsentFromChain)
	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ledgerTipPoint,
		BlockNumber: 2,
	})

	// Pin the preconditions, so this cannot silently become the
	// chain-ahead-with-a-real-tip case below.
	_, err := o.ledgerState.GetBlock(ledgerTipPoint)
	require.Error(t, err, "fixture requires the ledger tip row to be absent")
	require.Greater(
		t,
		o.ledgerState.PrimaryChainTip().Point.Slot,
		ledgerTipPoint.Slot,
		"fixture requires the primary chain to be ahead of the ledger tip",
	)

	points, err := o.ledgerState.IntersectPoints(chainsyncIntersectPointCount)
	require.NoError(t, err)
	require.Empty(
		t,
		points,
		"unapplied ahead-fork state must not be advertised (#2309)",
	)

	anchor, hasAnchor, err := o.ledgerState.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	require.False(
		t,
		hasAnchor,
		"ledger must refuse to anchor on a chain tip ahead of its own tip",
	)

	got, rescued := finalizeChainsyncIntersectPoints(
		normalizeIntersectPoints(points),
		anchor,
		hasAnchor,
	)
	assert.False(t, rescued)
	require.Len(t, got, 1)
	assert.True(t, isOriginPoint(got[0]))
}

// TestIntersectPointsChainAheadWithLedgerTipRowPresentAdvertisesChainPoints is
// the complementary case, and the one that must NOT be conflated with the
// #2309 test above: the primary chain is ahead of the ledger tip, but the
// ledger tip is a real block on that chain. The chain is then a valid forward
// extension, primaryChainTipAtOrAheadOfLedgerTip's ancestor check passes, and
// the chain's real points are advertised with origin appended as the usual
// last resort.
//
// No rescue is involved here: the ledger already returned real points, so the
// rollback anchor is absent and must stay absent.
func TestIntersectPointsChainAheadWithLedgerTipRowPresentAdvertisesChainPoints(
	t *testing.T,
) {
	o := &Ouroboros{}
	o.ledgerState, _ = newTestLedgerStateWithChain(t, 5)

	// The real block-2 hash produced by the fixture, so the row exists and
	// the tip is an ancestor on the primary chain.
	ledgerTipPoint := ocommon.NewPoint(2, bytes.Repeat([]byte{0x02}, 32))
	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ledgerTipPoint,
		BlockNumber: 2,
	})

	_, err := o.ledgerState.GetBlock(ledgerTipPoint)
	require.NoError(t, err, "fixture requires the ledger tip row to be present")

	points, err := o.ledgerState.IntersectPoints(chainsyncIntersectPointCount)
	require.NoError(t, err)
	require.Len(t, points, 5, "the chain's real points must be advertised")
	assert.Equal(t, uint64(5), points[0].Slot, "newest chain point leads")

	anchor, hasAnchor, err := o.ledgerState.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	assert.False(
		t,
		hasAnchor,
		"no rollback is in flight, so no anchor may be offered",
	)

	got, rescued := finalizeChainsyncIntersectPoints(
		normalizeIntersectPoints(points),
		anchor,
		hasAnchor,
	)
	assert.False(t, rescued, "real points need no rescue")
	require.Len(t, got, len(points)+1)
	assert.Equal(t, uint64(5), got[0].Slot)
	assert.True(
		t,
		isOriginPoint(got[len(got)-1]),
		"origin remains the appended last resort",
	)
}

// TestRollbackWindowIntersectAnchorPropagatesStorageError verifies the anchor
// lookup surfaces a storage fault instead of reporting "no anchor". The
// chainsync call site fails the client start on this error; downgrading it
// would send the peer an origin-only intersect list, i.e. a request to replay
// the chain from genesis.
func TestRollbackWindowIntersectAnchorPropagatesStorageError(t *testing.T) {
	o := &Ouroboros{}
	ls, db := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls

	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ocommon.NewPoint(2, ledgerTipHashAbsentFromChain),
		BlockNumber: 2,
	})

	// Sanity: healthy database answers without error.
	_, _, err := ls.RollbackWindowIntersectAnchor()
	require.NoError(t, err)

	require.NoError(t, dbtest.CloseDatabase(db))

	_, hasAnchor, err := ls.RollbackWindowIntersectAnchor()
	require.Error(t, err, "storage failure must not be reported as no anchor")
	assert.False(t, hasAnchor)
}
