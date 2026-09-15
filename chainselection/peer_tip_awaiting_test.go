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

package chainselection

import (
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
)

// TestAwaitingFirstHeaderAfterPostIntersectRollback covers the state a peer is
// in for the whole window between reconnecting and its first RollForward: the
// delivered frontier is the point the session intersected at, carrying no
// block number, while the advertised tip is far ahead. Callers comparing
// against SelectionTip need to be able to tell that frontier apart from a
// delivered one, because it is evidence of nothing but the intersection.
func TestAwaitingFirstHeaderAfterPostIntersectRollback(t *testing.T) {
	pt := NewPeerChainTip(
		newTestConnectionId(1),
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("observed")},
			BlockNumber: 10,
		},
		nil,
	)
	assert.False(
		t,
		pt.AwaitingFirstHeader(),
		"a peer registered from a delivered header is not awaiting one",
	)

	advertised := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 900, Hash: []byte("network")},
		BlockNumber: 90,
	}
	pt.ApplyRollback(
		ocommon.Point{Slot: 90, Hash: []byte("intersect")},
		advertised,
	)

	assert.True(
		t,
		pt.AwaitingFirstHeader(),
		"a rollback outside the delivered history leaves a bare frontier",
	)
	assert.Equal(t, advertised, pt.Tip, "the advertised tip stays available")
}

// TestAwaitingFirstHeaderClearsOnDeliveredHeader verifies the flag is confined
// to that window: the first RollForward restores a delivered frontier with a
// block number, and the peer is no longer awaiting anything.
func TestAwaitingFirstHeaderClearsOnDeliveredHeader(t *testing.T) {
	pt := NewPeerChainTip(
		newTestConnectionId(1),
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("observed")},
			BlockNumber: 10,
		},
		nil,
	)
	pt.ApplyRollback(
		ocommon.Point{Slot: 90, Hash: []byte("intersect")},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 900, Hash: []byte("network")},
			BlockNumber: 90,
		},
	)
	assert.True(t, pt.AwaitingFirstHeader())

	delivered := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 91, Hash: []byte("block-9")},
		BlockNumber: 9,
	}
	pt.UpdateTipWithObserved(
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 900, Hash: []byte("network")},
			BlockNumber: 90,
		},
		delivered,
		nil,
	)

	assert.False(t, pt.AwaitingFirstHeader())
	assert.Equal(t, delivered, pt.SelectionTip())
}

// TestAwaitingFirstHeaderNilReceiver keeps the accessor safe for callers that
// hold a peer tip lookup result without checking it, matching SelectionTip.
func TestAwaitingFirstHeaderNilReceiver(t *testing.T) {
	var pt *PeerChainTip
	assert.False(t, pt.AwaitingFirstHeader())
}
