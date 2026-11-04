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
	"crypto/sha256"
	"fmt"
	"sync/atomic"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

func repairTip(prefix string, number uint64) ochainsync.Tip {
	tip := loeTip(prefix, number)
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", prefix, number)))
	tip.Point.Hash = hash[:]
	return tip
}

func repairUpdate(
	f *eagernessDeliveryFixture,
	conn ouroboros.ConnectionId,
	number uint64,
) PeerTipUpdateEvent {
	u := f.update(conn, number)
	u.ObservedTip = repairTip("c", number)
	u.ObservedPrevHash = repairTip("c", number-1).Point.Hash
	return u
}

func repairAdmit(
	t *testing.T,
	f *eagernessDeliveryFixture,
	conn ouroboros.ConnectionId,
	number uint64,
) {
	t.Helper()
	u := repairUpdate(f, conn, number)
	require.True(t, f.selector.PreparePeerTipAdmission(u))
	require.True(t, f.selector.CommitPeerTipAdmission(u))
}

func repairPath(from, to uint64) []ochainsync.Tip {
	var path []ochainsync.Tip
	for number := from; number <= to; number++ {
		path = append(path, repairTip("c", number))
	}
	return path
}

func TestEagernessVerifiedPathRepairsMissingDelivery(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peers := []ouroboros.ConnectionId{
				corrConn(111),
				corrConn(112),
				corrConn(113),
				corrConn(114),
			}
			for _, peer := range peers {
				repairAdmit(t, f, peer, 10)
				f.selector.RecordDeliveredPeerTip(repairUpdate(f, peer, 11))
				// Header 12 was withheld locally; a later authenticated header
				// cannot establish the missing link by its endpoint alone.
				f.selector.RecordDeliveredPeerTip(repairUpdate(f, peer, 13))
			}
			f.selector.SetLocalTip(repairTip("c", 15))
			require.EqualValues(t, 15, f.selector.EagernessLimit().BlockNumber)
			path := repairPath(10, 16)
			for _, peer := range peers {
				u := repairUpdate(f, peer, 16)
				anchor, limit, needed := f.selector.EagernessDeliveryRepair(u)
				require.True(
					t,
					needed,
					"a gap must request complete authentication",
				)
				require.Equal(t, repairTip("c", 10), anchor)
				require.GreaterOrEqual(t, limit, len(path))
				require.True(
					t,
					f.selector.RecordVerifiedEagernessPath(
						u,
						anchor,
						path,
						1,
						func() bool { return true },
						func() bool { return true },
					),
				)
				_, _, needed = f.selector.EagernessDeliveryRepair(u)
				require.False(
					t,
					needed,
					"a current complete proof needs no repeated repair",
				)
				require.EqualValues(
					t,
					10,
					f.selector.peerTips[peer].ObservedTip.BlockNumber,
					"repair must not admit or select the authenticated endpoint",
				)
			}
			require.EqualValues(
				t,
				21,
				f.selector.EagernessLimit().BlockNumber,
				"the common verified path must release eagerness after a lost callback",
			)
			path[3].Point.Hash[0] ^= 0xff
			require.EqualValues(t, 21, f.selector.EagernessLimit().BlockNumber,
				"caller mutation must not rewrite retained testimony")
		})
	}
}

func TestEagernessVerifiedPathRejectsChangedProofInputs(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			for _, name := range []string{
				"expired generation", "changed admission", "withdrawn proof", "missing fence",
				"wrong anchor", "wrong endpoint", "height gap", "slot reversal", "empty hash",
			} {
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					f := newEagernessDeliveryFixture(t, 100, quorum)
					peer := corrConn(115)
					repairAdmit(t, f, peer, 10)
					u := repairUpdate(f, peer, 16)
					anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
					require.True(t, needed)
					path := repairPath(10, 16)
					fence := func() bool { return true }
					switch name {
					case "expired generation":
						f.generation.Store(2)
					case "changed admission":
						repairAdmit(t, f, peer, 11)
					case "withdrawn proof":
						fence = func() bool { return false }
					case "missing fence":
						fence = nil
					case "wrong anchor":
						anchor.Point.Hash[0] ^= 0xff
					case "wrong endpoint":
						path[len(path)-1].Point.Hash[0] ^= 0xff
					case "height gap":
						path[3].BlockNumber++
					case "slot reversal":
						path[3].Point.Slot = path[2].Point.Slot
					case "empty hash":
						path[3].Point.Hash = nil
					}
					require.False(
						t,
						f.selector.RecordVerifiedEagernessPath(
							u,
							anchor,
							path,
							1,
							fence,
							func() bool { return true },
						),
						"changed or incomplete proof inputs must not become eagerness evidence",
					)
					require.Empty(t, f.selector.eagernessDeliveries)
				})
			}
		})
	}
}

func TestEagernessVerifiedPathPreservesOlderForkPoint(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 1, quorum)
			peer, rival := corrConn(116), corrConn(117)
			for number := uint64(1); number <= 7; number++ {
				repairAdmit(t, f, peer, number)
				u := repairUpdate(f, rival, number)
				if number > 3 {
					u.ObservedTip = repairTip("fork", number)
					u.ObservedPrevHash = repairTip("fork", number-1).Point.Hash
					if number == 4 {
						u.ObservedPrevHash = repairTip("c", 3).Point.Hash
					}
				}
				require.True(t, f.selector.PreparePeerTipAdmission(u))
				require.True(t, f.selector.CommitPeerTipAdmission(u))
			}
			f.selector.SetLocalTip(repairTip("c", 5))
			u := repairUpdate(f, peer, 13)
			anchor, limit, needed := f.selector.EagernessDeliveryRepair(u)
			require.True(t, needed)
			path := repairPath(7, 13)
			require.Equal(t, len(path), limit)
			require.True(
				t,
				f.selector.RecordVerifiedEagernessPath(
					u,
					anchor,
					path,
					1,
					func() bool { return true },
					func() bool { return true },
				),
			)
			require.EqualValues(
				t,
				8,
				f.selector.EagernessLimit().BlockNumber,
				"joining a repair must preserve the older admitted intersection at block 3",
			)
			u = repairUpdate(f, peer, 14)
			_, _, needed = f.selector.EagernessDeliveryRepair(u)
			require.False(
				t,
				needed,
				"a proof extending beyond the fixed bound must not allocate a repair",
			)
			require.False(
				t,
				f.selector.RecordVerifiedEagernessPath(
					u,
					anchor,
					repairPath(7, 14),
					1,
					func() bool { return true },
					func() bool { return true },
				),
			)
			require.LessOrEqual(
				t,
				f.selector.eagernessDeliveries[peer].fragment.Len(),
				limit,
			)
		})
	}
}

func TestEagernessVerifiedPathRechecksRetainedWithdrawal(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(118)
			repairAdmit(t, f, peer, 10)
			f.selector.SetLocalTip(repairTip("c", 15))
			u := repairUpdate(f, peer, 16)
			anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
			require.True(t, needed)
			var withdrawn atomic.Bool
			retained := func() bool { return !withdrawn.Load() }
			require.True(
				t,
				f.selector.RecordVerifiedEagernessPath(
					u,
					anchor,
					repairPath(10, 16),
					1,
					func() bool { return true },
					retained,
				),
			)
			require.EqualValues(t, 21, f.selector.EagernessLimit().BlockNumber)
			withdrawn.Store(true)
			require.EqualValues(
				t,
				15,
				f.selector.EagernessLimit().BlockNumber,
				"an unpublished withdrawal must invalidate an already retained path",
			)
			f.selector.RecordDeliveredPeerTip(repairUpdate(f, peer, 17))
			require.EqualValues(
				t,
				15,
				f.selector.EagernessLimit().BlockNumber,
				"a later verified endpoint must not refresh a withdrawn path's missing links",
			)
			_, _, needed = f.selector.EagernessDeliveryRepair(
				repairUpdate(f, peer, 17),
			)
			require.True(t, needed)
		})
	}
}

func TestEagernessVerifiedPathChecksFenceUnderSelectorLock(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(119)
			repairAdmit(t, f, peer, 10)
			u := repairUpdate(f, peer, 16)
			anchor, _, needed := f.selector.EagernessDeliveryRepair(u)
			require.True(t, needed)
			checked := false
			current := func() bool {
				checked = true
				if f.selector.mutex.TryRLock() {
					f.selector.mutex.RUnlock()
					return false
				}
				return true
			}
			require.True(
				t,
				f.selector.RecordVerifiedEagernessPath(
					u,
					anchor,
					repairPath(10, 16),
					1,
					current,
					func() bool { return true },
				),
				"the final proof fence must run in the same selector critical section as insertion",
			)
			require.True(t, checked)
		})
	}
}
