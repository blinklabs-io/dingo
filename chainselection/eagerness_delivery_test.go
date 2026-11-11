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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type eagernessDeliveryFixture struct {
	selector   *ChainSelector
	generation atomic.Uint64
	sequence   uint64
}

func newEagernessDeliveryFixture(
	t *testing.T,
	window uint64,
	quorum ...int,
) *eagernessDeliveryFixture {
	t.Helper()
	threshold := 1
	if len(quorum) > 0 {
		threshold = quorum[0]
	}
	f := &eagernessDeliveryFixture{}
	f.generation.Store(1)
	f.selector = NewChainSelector(ChainSelectorConfig{
		GenesisMode: true, SecurityParam: loeTestK,
		GenesisWindowSlots:                        window,
		MinCorroboratingPeers:                     threshold,
		CurrentHeaderCryptoVerificationGeneration: f.generation.Load,
		HeaderCryptoVerificationGenerationCurrent: func(generation uint64) bool {
			return generation == f.generation.Load()
		},
		PinHeaderCryptoVerificationGeneration: func(generation uint64) (func(), bool) {
			return func() {}, generation == f.generation.Load()
		},
	})
	return f
}

func (f *eagernessDeliveryFixture) update(
	conn ouroboros.ConnectionId, number uint64,
) PeerTipUpdateEvent {
	f.sequence++
	return PeerTipUpdateEvent{
		ConnectionId: conn, AdmissionID: f.sequence,
		Tip: loeTip("network", 1000), ObservedTip: loeTip("c", number),
		ObservedPrevHash:                   loeTip("c", number-1).Point.Hash,
		HeaderCryptoVerificationGeneration: f.generation.Load(),
	}
}

func (f *eagernessDeliveryFixture) admit(
	t *testing.T, conn ouroboros.ConnectionId, number uint64,
) {
	t.Helper()
	update := f.update(conn, number)
	require.True(t, f.selector.PreparePeerTipAdmission(update))
	require.True(t, f.selector.CommitPeerTipAdmission(update))
}

func TestEagernessAuthenticatedDeliveryAdvancesWithoutLedgerAdmission(
	t *testing.T,
) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peers := []ouroboros.ConnectionId{
				corrConn(101), corrConn(102), corrConn(103),
				corrConn(104),
			}
			for _, peer := range peers {
				f.admit(t, peer, 10)
			}
			f.selector.SetLocalTip(loeTip("c", 15))
			require.Equal(
				t,
				uint64(15),
				f.selector.EagernessLimit().BlockNumber,
			)
			for number := uint64(11); number <= 16; number++ {
				for _, peer := range peers {
					f.selector.RecordDeliveredPeerTip(f.update(peer, number))
					f.selector.TouchPeerActivity(peer)
				}
			}
			for _, peer := range peers[:3] {
				require.True(
					t,
					f.selector.PreparePeerTipAdmission(f.update(peer, 16)),
				)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				err := f.selector.AwaitEagernessLimit(ctx, peer, 16, nil)
				cancel()
				require.NoError(
					t,
					err,
					"authenticated linked delivery must release the next header without committing it",
				)
			}
			require.Equal(
				t,
				uint64(21),
				f.selector.EagernessLimit().BlockNumber,
				"LoE must derive its limit from the common authenticated fragment",
			)
			for _, peer := range peers {
				tip := f.selector.GetAllPeerTips()[peer]
				require.NotNil(t, tip)
				assert.Equal(t, uint64(10), tip.SelectionTip().BlockNumber,
					"LoE delivery evidence must not promote a selectable tip")
				require.Len(t, tip.admittedTipHistory, 1)
				assert.Equal(
					t,
					loeTip("c", 10).Point,
					tip.admittedTipHistory[0],
				)
			}
		})
	}
}

func TestEagernessAuthenticatedDeliveryRejectsBrokenLinks(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			for _, scenario := range []string{"parent", "gap", "hash", "height", "boundary"} {
				t.Run(scenario, func(t *testing.T) {
					t.Parallel()
					f := newEagernessDeliveryFixture(t, 100, quorum)
					peer := corrConn(111)
					f.admit(t, peer, 10)
					f.selector.SetLocalTip(loeTip("c", 15))
					update := f.update(peer, 11)
					switch scenario {
					case "parent":
						update.ObservedPrevHash = []byte("fork")
					case "gap":
						update = f.update(peer, 12)
						update.ObservedPrevHash = loeTip("c", 10).Point.Hash
					case "hash":
						update = f.update(peer, 10)
						update.ObservedTip.Point.Hash = []byte("fork")
					case "height":
						update.ObservedTip.BlockNumber = 99
					case "boundary":
						update.ObservedBoundary = true
					}
					f.selector.RecordDeliveredPeerTip(update)
					require.Equal(
						t,
						uint64(15),
						f.selector.EagernessLimit().BlockNumber,
						"a broken delivery cannot extend the admitted head",
					)
					// Later self-linked deliveries cannot fill the earlier missing link.
					for number := uint64(12); number <= 16; number++ {
						f.selector.RecordDeliveredPeerTip(
							f.update(peer, number),
						)
					}
					assert.Equal(
						t,
						uint64(15),
						f.selector.EagernessLimit().BlockNumber,
						"unproved delivery must retain the conservative admission fragment",
					)
				})
			}
		})
	}
}

func TestEagernessAuthenticatedDeliveryRequiresCurrentVerification(
	t *testing.T,
) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(121)
			f.admit(t, peer, 10)
			f.selector.SetLocalTip(loeTip("c", 15))
			for number := uint64(11); number <= 16; number++ {
				f.selector.RecordDeliveredPeerTip(f.update(peer, number))
			}
			require.Equal(
				t,
				uint64(21),
				f.selector.EagernessLimit().BlockNumber,
			)
			f.generation.Store(2)
			require.Equal(
				t,
				uint64(15),
				f.selector.EagernessLimit().BlockNumber,
				"an expired observer verdict must not replace the admitted fragment",
			)
			stale := f.update(peer, 17)
			stale.HeaderCryptoVerificationGeneration = 1
			f.selector.RecordDeliveredPeerTip(stale)
			require.Equal(
				t,
				uint64(15),
				f.selector.EagernessLimit().BlockNumber,
				"a stale delivery must not refresh the extension",
			)
			unstamped := f.update(peer, 17)
			unstamped.HeaderCryptoVerificationGeneration = 0
			f.selector.RecordDeliveredPeerTip(unstamped)
			require.Equal(
				t,
				uint64(15),
				f.selector.EagernessLimit().BlockNumber,
				"an unstamped delivery must not refresh an authenticated extension",
			)
			f.selector.RecordDeliveredPeerTip(f.update(peer, 17))
			assert.Equal(
				t,
				uint64(22),
				f.selector.EagernessLimit().BlockNumber,
				"a fresh authenticated linked delivery may refresh the extension",
			)
		})
	}
}

func TestEagernessAuthenticatedDeliveryRejectsReplacedRepeat(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(122)
			f.admit(t, peer, 10)
			f.selector.SetLocalTip(loeTip("c", 15))
			for number := uint64(11); number <= 16; number++ {
				f.selector.RecordDeliveredPeerTip(f.update(peer, number))
			}
			require.Equal(
				t,
				uint64(21),
				f.selector.EagernessLimit().BlockNumber,
			)
			f.generation.Store(2)
			update := f.update(peer, 16)
			update.ObservedTip.Point.Hash = []byte("replacement")
			f.selector.RecordDeliveredPeerTip(update)
			assert.Equal(
				t,
				uint64(15),
				f.selector.EagernessLimit().BlockNumber,
				"a different repeated head cannot refresh an expired continuity proof",
			)
		})
	}
}

func TestEagernessAuthenticatedDeliveryRetainsForkLimit(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			left, right := corrConn(123), corrConn(124)
			f.admit(t, left, 10)
			f.admit(t, right, 10)
			f.selector.SetLocalTip(loeTip("c", 15))
			for number := uint64(11); number <= 16; number++ {
				f.selector.RecordDeliveredPeerTip(f.update(left, number))
				update := f.update(right, number)
				update.ObservedTip = loeTip("fork", number)
				if number > 11 {
					update.ObservedPrevHash = loeTip(
						"fork",
						number-1,
					).Point.Hash
				}
				f.selector.RecordDeliveredPeerTip(update)
			}
			limit := f.selector.EagernessLimit()
			require.True(t, limit.Intersected)
			assert.Equal(t, loeTip("c", 10).Point, limit.Point)
			assert.Equal(
				t,
				uint64(15),
				limit.BlockNumber,
				"authenticated divergent suffixes must still hold at k past their fork",
			)
		})
	}
}

func TestEagernessAuthenticatedDeliveryCopiesAdmissionHistory(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(125)
			for number := uint64(10); number <= 12; number++ {
				f.admit(t, peer, number)
			}
			f.selector.SetLocalTip(loeTip("c", 15))
			for number := uint64(13); number <= 16; number++ {
				f.selector.RecordDeliveredPeerTip(f.update(peer, number))
			}
			require.Equal(
				t,
				uint64(21),
				f.selector.EagernessLimit().BlockNumber,
			)
			f.selector.mutex.Lock()
			f.selector.peerTips[peer].observedPoints[0].Hash[0] = 'x'
			f.selector.mutex.Unlock()
			assert.Equal(
				t,
				uint64(17),
				f.selector.EagernessLimit().BlockNumber,
				"replacing an overlapping admitted point must invalidate the independent delivery proof",
			)
		})
	}
}

func TestEagernessAuthenticatedDeliveryRollbacksAndRemoval(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			for _, path := range []string{"delivery", "tracked", "event", "unknown", "disconnect"} {
				t.Run(path, func(t *testing.T) {
					t.Parallel()
					f := newEagernessDeliveryFixture(t, 100, quorum)
					peer := corrConn(126)
					f.admit(t, peer, 10)
					f.selector.SetLocalTip(loeTip("c", 15))
					for number := uint64(11); number <= 16; number++ {
						f.selector.RecordDeliveredPeerTip(
							f.update(peer, number),
						)
					}
					require.Equal(
						t,
						uint64(21),
						f.selector.EagernessLimit().BlockNumber,
					)
					rollback := PeerRollbackEvent{ConnectionId: peer,
						Point: loeTip(
							"c",
							10,
						).Point, Tip: loeTip("network", 1000)}
					switch path {
					case "delivery":
						f.selector.RecordDeliveredPeerRollback(rollback)
					case "tracked":
						require.True(
							t,
							f.selector.applyRollbackToTrackedPeer(rollback),
						)
					case "event":
						f.selector.HandlePeerRollbackEvent(
							event.NewEvent(PeerRollbackEventType, rollback),
						)
					case "unknown":
						rollback.Point = loeTip("other", 10).Point
						f.selector.RecordDeliveredPeerRollback(rollback)
					case "disconnect":
						f.selector.RemovePeer(peer)
					}
					if path == "disconnect" || path == "unknown" {
						assert.NotContains(
							t,
							f.selector.eagernessDeliveries,
							peer,
							"unusable continuity evidence must be removed",
						)
					} else {
						assert.Equal(t, uint64(15), f.selector.EagernessLimit().BlockNumber,
							"rollback must remove the authenticated suffix from LoE")
					}
				})
			}
		})
	}
}

func TestEagernessAuthenticatedDeliveryIgnoresForecastWitness(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(127)
			f.admit(t, peer, 10)
			f.selector.SetLocalTip(loeTip("c", 15))
			update := f.update(peer, 11)
			update.HeaderCryptoVerificationGeneration = 0
			f.selector.PrepareForecastHorizonWitness(update)
			if quorum > 0 {
				require.Contains(t, f.selector.forecastHorizonWitnesses, peer)
			} else {
				require.Empty(t, f.selector.forecastHorizonWitnesses, "unverified witnesses are policy-only")
			}
			assert.Empty(
				t,
				f.selector.eagernessDeliveries,
				"a forecast witness is not authenticated LoE continuity evidence",
			)
			assert.Equal(t, uint64(15), f.selector.EagernessLimit().BlockNumber)
		})
	}
}

func TestEagernessAuthenticatedDeliveryOwnsDeliveredPoint(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(128)
			f.admit(t, peer, 10)
			f.selector.SetLocalTip(loeTip("c", 15))
			update := f.update(peer, 11)
			f.selector.RecordDeliveredPeerTip(update)
			update.ObservedTip.Point.Hash[0] = 'x'
			f.selector.RecordDeliveredPeerTip(f.update(peer, 12))
			assert.Equal(
				t,
				uint64(17),
				f.selector.EagernessLimit().BlockNumber,
				"mutating caller-owned bytes must not break the retained parent link",
			)
		})
	}
}

func TestEagernessAuthenticatedDeliveryBoundedAndKeptOnExit(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 100, quorum)
			peer := corrConn(129)
			f.admit(t, peer, 10)
			for number := uint64(11); number <= 250; number++ {
				f.selector.SetLocalTip(loeTip("c", number-1))
				f.selector.RecordDeliveredPeerTip(f.update(peer, number))
				f.admit(t, peer, number)
			}
			capacity := f.selector.candidatePointCapacityLocked(true)
			require.Len(t, f.selector.eagernessDeliveries, 1)
			assert.Equal(
				t,
				capacity,
				uint64(f.selector.eagernessDeliveries[peer].fragment.Len()),
				"the continuity proof must retain only the configured candidate window",
			)
			var observers []ouroboros.ConnectionId
			for index := range 20 {
				observer := corrConn(150 + index)
				observers = append(observers, observer)
				f.selector.RecordDeliveredPeerTip(f.update(observer, 250))
			}
			require.Len(t, f.selector.eagernessDeliveries, 1,
				"untracked observers cannot allocate LoE proofs")
			var chain testChain
			for number := uint64(1); number <= 251; number++ {
				chain.add(loeTip("c", number))
			}
			f.selector.config.PointOnLocalChain = chain.onChain
			f.selector.UpdatePeerTip(peer, loeTip("c", 250), nil)
			second := corrConn(130)
			f.selector.UpdatePeerTip(second, loeTip("c", 250), nil)
			f.selector.SetLocalTip(loeTip("c", 250))
			f.selector.SetPeerIdling(peer, loeTip("c", 250).Point)
			f.selector.SetPeerIdling(second, loeTip("c", 250).Point)
			for _, observer := range observers {
				f.selector.SetPeerIdling(observer, loeTip("c", 250).Point)
			}
			require.Equal(t, SelectionModePraos, f.selector.SelectionMode())
			require.False(t, f.selector.EagernessLimit().Active)
			require.Len(t, f.selector.eagernessDeliveries, 1,
				"the bounded continuity history is kept across the exit")
			f.selector.RecordDeliveredPeerTip(f.update(peer, 251))
			require.Len(t, f.selector.eagernessDeliveries, 1)
			assert.LessOrEqual(
				t,
				uint64(f.selector.eagernessDeliveries[peer].fragment.Len()),
				capacity,
				"the history stays bounded in Praos",
			)
		})
	}
}

func TestEagernessAuthenticatedDeliveryPreservesOlderForkPoint(t *testing.T) {
	t.Parallel()
	for _, quorum := range []int{0, 1} {
		t.Run(fmt.Sprintf("quorum=%d", quorum), func(t *testing.T) {
			t.Parallel()
			f := newEagernessDeliveryFixture(t, 1, quorum)
			left, right := corrConn(131), corrConn(132)
			for number := uint64(1); number <= 7; number++ {
				f.admit(t, left, number)
			}
			f.selector.SetLocalTip(loeTip("c", 5))
			for number := uint64(8); number <= 13; number++ {
				f.selector.RecordDeliveredPeerTip(f.update(left, number))
			}
			require.Equal(
				t,
				uint64(7),
				f.selector.eagernessDeliveries[left].fragment.baseBlock,
			)
			require.Equal(
				t,
				uint64(18),
				f.selector.EagernessLimit().BlockNumber,
			)
			for number := uint64(1); number <= 7; number++ {
				update := f.update(right, number)
				if number > 3 {
					update.ObservedTip = loeTip("fork", number)
					if number > 4 {
						update.ObservedPrevHash = loeTip(
							"fork",
							number-1,
						).Point.Hash
					}
				}
				require.True(t, f.selector.PreparePeerTipAdmission(update))
				require.True(t, f.selector.CommitPeerTipAdmission(update))
			}
			limit := f.selector.EagernessLimit()
			require.True(
				t,
				limit.Intersected,
				"a cropped delivery window must preserve the admitted fork point",
			)
			assert.Equal(t, loeTip("c", 3).Point, limit.Point)
			assert.Equal(
				t,
				uint64(8),
				limit.BlockNumber,
				"a cropped delivery window must preserve the admitted fork point",
			)
		})
	}
}

func TestEagernessAuthenticatedDeliveryPinsPublicationAcrossRecording(t *testing.T) {
	t.Parallel()
	f := newEagernessDeliveryFixture(t, 100, 0)
	peer := corrConn(120)
	repairAdmit(t, f, peer, 10)
	var publication sync.RWMutex
	pinned := make(chan struct{}, 1)
	f.selector.config.PinHeaderCryptoVerificationGeneration = func(g uint64) (func(), bool) {
		publication.RLock()
		if g != f.generation.Load() {
			publication.RUnlock()
			return nil, false
		}
		select {
		case pinned <- struct{}{}:
		default:
		}
		return publication.RUnlock, true
	}
	update := repairUpdate(f, peer, 11)
	recorded := make(chan struct{})
	f.selector.mutex.Lock()
	var unlocked sync.Once
	unlock := func() { unlocked.Do(f.selector.mutex.Unlock) }
	t.Cleanup(func() { unlock(); testutil.RequireReceive(t, recorded, 5*time.Second, "recording cleanup") })
	go func() { defer close(recorded); f.selector.RecordDeliveredPeerTip(update) }()
	testutil.RequireReceive(t, pinned, 5*time.Second, "delivery must pin its generation before waiting for the selector")
	published := make(chan struct{})
	go func() { publication.Lock(); f.generation.Store(2); publication.Unlock(); close(published) }()
	testutil.RequireNoReceive(t, published, 100*time.Millisecond, "publication must not overtake the blocked delivery write")
	unlock()
	testutil.RequireReceive(t, recorded, 5*time.Second, "delivery recording must complete")
	testutil.RequireReceive(t, published, 5*time.Second, "publication must proceed after delivery releases its pin")
	f.selector.mutex.RLock()
	delivery := f.selector.deliveredPeerTips[peer]
	head := f.selector.eagernessDeliveries[peer]
	f.selector.mutex.RUnlock()
	require.NotNil(t, delivery)
	require.EqualValues(t, 11, delivery.ObservedTip.BlockNumber)
	require.EqualValues(t, 1, head.generation)
}
