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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildDefaultChainsyncIntersectPointsSurvivesAnchorStorageFaultWhenPointsAreGood
// is the regression test for the rollback anchor being looked up on every
// chainsync client start rather than only when it can matter.
//
// The healthy path is served from the in-memory chain: with the primary chain
// tip at or ahead of the ledger tip, LedgerState.IntersectPoints answers out of
// chain.IntersectPoints and never reads the database. The anchor lookup always
// reads it. So an unconditional lookup gave a database fault a way to fail a
// chainsync start that had a full list of real points to offer -- and since
// this branch makes HandleOutboundConnEvent close the connection when the
// client fails to start, a transient storage fault would tear down a healthy
// peer over an answer that would have been discarded.
//
// The closed database here stands in for that transient fault. What the test
// pins is that the fault cannot reach a start whose intersect list is already
// good.
func TestBuildDefaultChainsyncIntersectPointsSurvivesAnchorStorageFaultWhenPointsAreGood(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, _, connId := newOutboundStartTestOuroboros(t, logger, bus)
	ls, db := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls

	// A healthy node: the ledger tip is a block the chain holds, so the
	// primary chain tip is not behind it and intersect points come from the
	// in-memory chain.
	chainTip := ls.PrimaryChainTip()
	require.Equal(t, uint64(5), chainTip.Point.Slot)
	ls.SetTipForTesting(ochainsync.Tip{
		Point:       chainTip.Point,
		BlockNumber: 5,
	})

	// Sanity: with the database up, the start builds real points.
	healthy, err := o.buildDefaultChainsyncIntersectPoints(connId)
	require.NoError(t, err)
	require.True(
		t,
		intersectPointsHaveRealPoint(healthy),
		"fixture must produce a list with real points",
	)

	// The anchor lookup reads the database; the healthy path does not.
	require.NoError(t, dbtest.CloseDatabase(db))
	_, _, anchorErr := ls.RollbackWindowIntersectAnchor()
	require.Error(
		t,
		anchorErr,
		"fixture must make the anchor lookup fail",
	)

	points, err := o.buildDefaultChainsyncIntersectPoints(connId)
	require.NoError(
		t,
		err,
		"a storage fault in a lookup that cannot affect the result must not fail the chainsync start",
	)

	// The deeper points come from the database, so the fault legitimately
	// shortens the list. What must survive is the part that decides what
	// goes on the wire: a real leading point, and origin only as the last
	// resort rather than the whole request.
	require.True(
		t,
		intersectPointsHaveRealPoint(points),
		"the start must still offer a real point, not an origin-only request",
	)
	assert.Equal(t, chainTip.Point.Slot, points[0].Slot)
	assert.Equal(t, chainTip.Point.Hash, points[0].Hash)
	assert.True(
		t,
		isOriginPoint(points[len(points)-1]),
		"origin must remain the appended last resort",
	)
	assert.Equal(t, healthy[0], points[0])
}

// TestIntersectPointsHaveRealPointMatchesTheRescueCondition pins the predicate
// that the anchor-lookup gate and the rescue both use. They have to be the
// same test: a gate narrower than the rescue would skip the lookup for a list
// the rescue would have acted on, and the node would send the origin-only
// FindIntersect this path exists to prevent.
func TestIntersectPointsHaveRealPointMatchesTheRescueCondition(t *testing.T) {
	anchor := testAnchorPoint(1000)
	for _, tc := range []struct {
		name   string
		points []ocommon.Point
		real   bool
	}{
		{"empty", nil, false},
		{"origin only", []ocommon.Point{ocommon.NewPointOrigin()}, false},
		{
			"origin repeated",
			[]ocommon.Point{
				ocommon.NewPointOrigin(),
				ocommon.NewPointOrigin(),
			},
			false,
		},
		{"real point", []ocommon.Point{testAnchorPoint(42)}, true},
		{
			"real point then origin",
			[]ocommon.Point{
				testAnchorPoint(42),
				ocommon.NewPointOrigin(),
			},
			true,
		},
		{
			"origin then real point",
			[]ocommon.Point{
				ocommon.NewPointOrigin(),
				testAnchorPoint(42),
			},
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(
				t,
				tc.real,
				intersectPointsHaveRealPoint(tc.points),
			)
			// The rescue fires exactly when the predicate is false,
			// given a usable anchor. That equivalence is what lets the
			// gate skip the lookup whenever the predicate is true.
			_, rescued := finalizeChainsyncIntersectPoints(
				tc.points,
				anchor,
				true,
			)
			require.Equal(
				t,
				!tc.real,
				rescued,
				"the rescue must fire exactly when the gate would allow the lookup",
			)
		})
	}
}
