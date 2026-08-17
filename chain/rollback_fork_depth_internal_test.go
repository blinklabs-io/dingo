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

package chain

import (
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestRollbackForkDepthSaturates keeps issue #3040's underflow fix covered at
// the unit level.
//
// The behavioral test that used to reach this branch
// (TestChainRollbackPointAheadOfTipIsNotDeepFork) now asserts that a rollback
// point above the tip is refused outright as not-on-chain (issue #3005), so it
// no longer drives rollbackForkDepth with such an index. The saturating
// computation still has to be correct: any future caller that reaches it with a
// point above the tip must get zero, not a wrapped-around uint64 that reads as
// a fork deeper than any security parameter and denies every peer.
func TestRollbackForkDepthSaturates(t *testing.T) {
	c := &Chain{
		tipBlockIndex: 4,
		currentTip: ochainsync.Tip{
			Point:       ocommon.Point{Slot: 60, Hash: []byte("tip")},
			BlockNumber: 4,
		},
	}
	point := ocommon.Point{Slot: 100, Hash: []byte("ahead")}

	for _, tc := range []struct {
		name               string
		rollbackBlockIndex uint64
		want               uint64
	}{
		{name: "behind tip", rollbackBlockIndex: 1, want: 3},
		{name: "at tip", rollbackBlockIndex: 4, want: 0},
		{name: "one ahead of tip", rollbackBlockIndex: 5, want: 0},
		{name: "far ahead of tip", rollbackBlockIndex: 1 << 40, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.rollbackForkDepth(point, tc.rollbackBlockIndex)
			if got != tc.want {
				t.Fatalf(
					"rollbackForkDepth(tip=%d, rollback=%d) = %d, want %d",
					c.tipBlockIndex,
					tc.rollbackBlockIndex,
					got,
					tc.want,
				)
			}
		})
	}
}
