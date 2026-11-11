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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testChain is a mutable set of points standing in for the applied chain.
type testChain struct {
	mu  sync.Mutex
	pts map[string]struct{}
}

func (c *testChain) onChain(p ocommon.Point) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.pts[fmt.Sprintf("%d/%s", p.Slot, p.Hash)]
	return ok
}

func (c *testChain) add(tips ...ochainsync.Tip) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pts == nil {
		c.pts = make(map[string]struct{})
	}
	for _, tip := range tips {
		c.pts[fmt.Sprintf("%d/%s", tip.Point.Slot, tip.Point.Hash)] = struct{}{}
	}
}

var gsmEpoch = time.Unix(1_700_000_000, 0)

// gsmFixture drives a Genesis-mode selector with k=10 (window 30), an
// injected clock, an injected slot-to-time conversion and a local chain the
// test extends with apply. Block n of the shared chain sits at slot n with
// hash "h<n>".
type gsmFixture struct {
	cs       *ChainSelector
	mu       sync.Mutex
	now      time.Time
	slotLen  time.Duration
	slotErr  error
	chain    map[string]struct{}
	local    func(ochainsync.Tip) (PraosTiebreakerView, bool)
	historic []HistoricityDisconnect
	bonusMax time.Duration
	conns    []ouroboros.ConnectionId
}

type gsmOption func(*gsmFixture, *ChainSelectorConfig)

func gsmByron(f *gsmFixture, cfg *ChainSelectorConfig) {
	f.slotLen = 20 * time.Second
	cfg.WallClockSlot = func() (uint64, bool) { return 0, false }
}

func gsmWithObservers(f *gsmFixture, cfg *ChainSelectorConfig) {
	cfg.ChainSyncConnections = func() []ouroboros.ConnectionId {
		f.mu.Lock()
		defer f.mu.Unlock()
		return append([]ouroboros.ConnectionId(nil), f.conns...)
	}
}

func newGSMFixture(t *testing.T, opts ...gsmOption) *gsmFixture {
	t.Helper()
	f := &gsmFixture{
		now:     gsmEpoch.Add(2 * time.Minute),
		slotLen: time.Second,
		chain:   make(map[string]struct{}),
	}
	cfg := ChainSelectorConfig{
		GenesisMode:               true,
		SecurityParam:             10,
		DisableEventSubscriptions: true,
		PointOnLocalChain: func(p ocommon.Point) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			_, ok := f.chain[fmt.Sprintf("%d/%s", p.Slot, p.Hash)]
			return ok
		},
		SlotTime: func(slot uint64) (time.Time, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.slotErr != nil {
				return time.Time{}, f.slotErr
			}
			return gsmEpoch.Add(time.Duration(slot) * f.slotLen), nil
		},
		LocalTipPraosView: func(tip ochainsync.Tip) (PraosTiebreakerView, bool) {
			f.mu.Lock()
			local := f.local
			f.mu.Unlock()
			if local == nil {
				return PraosTiebreakerView{}, false
			}
			return local(tip)
		},
		OnHistoricityDisconnect: func(d HistoricityDisconnect) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.historic = append(f.historic, d)
		},
		RandomBonus: func(maxBonus time.Duration) time.Duration {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.bonusMax = maxBonus
			return 120 * time.Second
		},
	}
	for _, opt := range opts {
		opt(f, &cfg)
	}
	f.cs = NewChainSelector(cfg)
	f.cs.nowFn = func() time.Time {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.now
	}
	return f
}

func (f *gsmFixture) setNow(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

func (f *gsmFixture) historicConns() []ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ouroboros.ConnectionId, 0, len(f.historic))
	for _, d := range f.historic {
		out = append(out, d.ConnectionId)
	}
	return out
}

func gsmTip(n uint64) ochainsync.Tip {
	return genesisTip(n, fmt.Sprintf("h%d", n), n)
}

func (f *gsmFixture) putOnChain(tip ochainsync.Tip) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chain[fmt.Sprintf("%d/%s", tip.Point.Slot, tip.Point.Hash)] = struct{}{}
}

// apply extends the local chain with the given tips and applies the last.
func (f *gsmFixture) apply(tips ...ochainsync.Tip) {
	for _, tip := range tips {
		f.putOnChain(tip)
	}
	f.cs.SetLocalTip(tips[len(tips)-1])
}

func (f *gsmFixture) applyRange(from, to uint64) {
	for n := from; n <= to; n++ {
		f.apply(gsmTip(n))
	}
}

// deliver feeds a peer the shared chain h<from>..h<to>, advertising its
// delivered position, which a peer that is caught up with itself does.
func (f *gsmFixture) deliver(
	t *testing.T,
	conn ouroboros.ConnectionId,
	from, to uint64,
) {
	t.Helper()
	for n := from; n <= to; n++ {
		require.True(t, f.cs.UpdatePeerTip(conn, gsmTip(n), nil))
	}
}

func (f *gsmFixture) idle(conn ouroboros.ConnectionId, tip ochainsync.Tip) {
	f.cs.SetPeerIdling(conn, tip.Point)
}

func (f *gsmFixture) mode() SelectionMode {
	return f.cs.SelectionMode()
}

// exitToPraos applies h1..h5 and lets one idle peer at h5 end Genesis.
func (f *gsmFixture) exitToPraos(t *testing.T) ouroboros.ConnectionId {
	t.Helper()
	peer := corrConn(90)
	f.applyRange(1, 5)
	f.deliver(t, peer, 1, 5)
	f.idle(peer, gsmTip(5))
	require.Equal(t, SelectionModePraos, f.mode())
	return peer
}

// A peer that delivered the same headers as the node and advertises its
// delivered position must not end Genesis selection while another peer is
// still streaming, and an idle peer whose delivered tip is historic is
// disconnected.
func TestGSMLiarIdleAtLowTipWhileHonestStreamsStaysInGenesis(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.setNow(gsmEpoch.Add(10_000 * time.Second))
	honest, liar := corrConn(1), corrConn(2)
	f.applyRange(1, 5)
	f.deliver(t, liar, 1, 5)
	f.deliver(t, honest, 1, 9)
	f.idle(liar, gsmTip(5))

	assert.Equal(t, SelectionModeGenesis, f.mode())
	assert.Equal(t, []ouroboros.ConnectionId{liar}, f.historicConns())
}

func TestGSMHonestDisconnectsLiarHistoricTipDoesNotExit(t *testing.T) {
	t.Parallel()
	for name, opt := range map[string]gsmOption{
		"shelley": func(*gsmFixture, *ChainSelectorConfig) {},
		"byron":   gsmByron,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newGSMFixture(t, opt)
			honest, liar := corrConn(1), corrConn(2)
			f.applyRange(1, 5)
			f.deliver(t, liar, 1, 5)
			f.deliver(t, honest, 1, 9)
			f.idle(liar, gsmTip(5))
			require.Equal(t, SelectionModeGenesis, f.mode())

			// The delivered tip is slot 5; place the clock beyond the
			// cutoff for this era's slot length.
			f.setNow(gsmEpoch.Add(
				5*f.slotLen + DefaultHistoricityCutoff(30) + time.Minute,
			))
			f.cs.RemovePeer(honest)
			f.cs.EvaluateAndSwitch()

			assert.Equal(t, SelectionModeGenesis, f.mode(),
				"an idle peer at a historic tip must not end Genesis")
			assert.Equal(t, []ouroboros.ConnectionId{liar}, f.historicConns())
			if name == "byron" {
				assert.False(t, f.cs.AdvertisedTipStale(gsmTip(5)),
					"the wall-clock rule must stay inactive in Byron")
			}
		})
	}
}

// Control for the historic case: the same single idle peer ends Genesis while
// its tip is young.
func TestGSMIdlePeerAtYoungTipExits(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.exitToPraos(t)
	assert.Empty(t, f.historicConns())
}

func gsmView(issueNo uint64) PraosTiebreakerView {
	return NewPraosTiebreakerViewFull(
		genesisTip(10, "x", 10),
		[]byte("pool"),
		issueNo,
		[]byte("vrf"),
		PraosTiebreakerConfigConway(),
	)
}

func TestGSMEqualHeightForkWinningTiebreakBlocksExit(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		peerIssue uint64
		hasLocal  bool
		want      SelectionMode
	}{
		"fork wins tiebreak":            {2, true, SelectionModeGenesis},
		"fork loses tiebreak":           {0, true, SelectionModePraos},
		"local view unavailable blocks": {0, false, SelectionModeGenesis},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newGSMFixture(t)
			if tc.hasLocal {
				f.local = func(ochainsync.Tip) (PraosTiebreakerView, bool) {
					return gsmView(1), true
				}
			}
			f.applyRange(1, 10)
			peer := corrConn(1)
			f.deliver(t, peer, 1, 9)
			fork := genesisTip(10, "fork10", 10)
			require.True(t, f.cs.updatePeerTipObservedPraosView(
				peer, fork, fork, nil, gsmView(tc.peerIssue),
			))
			f.idle(peer, fork)
			assert.Equal(t, tc.want, f.mode())
		})
	}
}

func TestGSMNonIntersectingPeerBlocksExit(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		shared bool
		want   SelectionMode
	}{
		"no shared point": {false, SelectionModeGenesis},
		"shorter fork":    {true, SelectionModePraos},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newGSMFixture(t)
			f.applyRange(1, 10)
			peer := corrConn(1)
			if tc.shared {
				f.deliver(t, peer, 1, 3)
			}
			var last ochainsync.Tip
			for n := uint64(4); n <= 6; n++ {
				last = genesisTip(n, fmt.Sprintf("f%d", n), n)
				require.True(t, f.cs.UpdatePeerTip(peer, last, nil))
			}
			f.idle(peer, last)
			assert.Equal(t, tc.want, f.mode())
		})
	}
}

// The ChainSync client hands a header to the ledger and requests the next one
// without waiting for admission, so MsgAwaitReply can arrive while the header
// it was sent at is still staged.
func TestGSMIdleSignalBeforeAdmissionCounts(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.applyRange(1, 5)
	peer := corrConn(1)
	var seq uint64
	update := func(n uint64) PeerTipUpdateEvent {
		seq++
		return PeerTipUpdateEvent{
			ConnectionId:     peer,
			AdmissionID:      seq,
			Tip:              genesisTip(1000, "net", 1000),
			ObservedTip:      gsmTip(n),
			ObservedPrevHash: gsmTip(n - 1).Point.Hash,
		}
	}
	for n := uint64(1); n <= 4; n++ {
		u := update(n)
		require.True(t, f.cs.PreparePeerTipAdmission(u))
		require.True(t, f.cs.CommitPeerTipAdmission(u))
	}
	last := update(5)
	require.True(t, f.cs.PreparePeerTipAdmission(last))
	f.idle(peer, gsmTip(5))
	require.Equal(t, SelectionModeGenesis, f.mode(),
		"a header still awaiting admission is not yet delivered")

	require.True(t, f.cs.CommitPeerTipAdmission(last))

	assert.Equal(t, SelectionModePraos, f.mode())
}

func TestGSMAwaitForAnOlderPointIsNotIdle(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.applyRange(1, 5)
	peer := corrConn(1)
	f.deliver(t, peer, 1, 5)
	f.idle(peer, gsmTip(4))
	assert.Equal(t, SelectionModeGenesis, f.mode())
}

func TestGSMNonIdleObserverBlocksExit(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmWithObservers)
	observer, peer := corrConn(2), corrConn(1)
	f.mu.Lock()
	f.conns = []ouroboros.ConnectionId{peer, observer}
	f.mu.Unlock()
	f.applyRange(1, 5)
	f.deliver(t, peer, 1, 5)
	f.idle(peer, gsmTip(5))
	require.Equal(t, SelectionModeGenesis, f.mode(),
		"an observer that has not reported idle must block the exit")

	f.idle(observer, gsmTip(5))

	assert.Equal(t, SelectionModePraos, f.mode())
}

func TestGSMNoConnectionsDoesNotExit(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.applyRange(1, 5)
	f.cs.EvaluateAndSwitch()
	assert.Equal(t, SelectionModeGenesis, f.mode())
}

// An exit keeps each peer's delivered frontier so a return to Genesis has data.
func TestGSMExitKeepsPeerFrontier(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	peer := f.exitToPraos(t)
	f.cs.mutex.RLock()
	defer f.cs.mutex.RUnlock()
	assert.Len(t, f.cs.peerTips[peer].observedPoints, 5)
}

func TestGSMReentersAtDeadlinePlusBonus(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.exitToPraos(t)
	// A fresh selection tip: slot 600 is 10 minutes after the epoch.
	f.apply(genesisTip(600, "h600", 6))
	minStay := gsmEpoch.Add(2*time.Minute + DefaultMaxCaughtUpAge)
	deadline := gsmEpoch.Add(600*time.Second + DefaultMaxCaughtUpAge +
		120*time.Second)

	f.setNow(minStay)
	f.cs.EvaluateAndSwitch()
	require.Equal(t, SelectionModePraos, f.mode())
	f.setNow(deadline.Add(-time.Second))
	f.cs.EvaluateAndSwitch()
	require.Equal(t, SelectionModePraos, f.mode())
	f.setNow(deadline)
	f.cs.EvaluateAndSwitch()
	assert.Equal(t, SelectionModeGenesis, f.mode())
	active, _ := f.cs.GenesisSelectionState()
	assert.True(t, active, "the lock-free state must follow the re-entry")
}

func TestGSMDeadlineFollowsSelectionChange(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.exitToPraos(t)
	f.apply(genesisTip(600, "h600", 6))
	minStay := gsmEpoch.Add(2*time.Minute + DefaultMaxCaughtUpAge)
	f.setNow(minStay)
	f.cs.EvaluateAndSwitch()
	firstDeadline := gsmEpoch.Add(600*time.Second + DefaultMaxCaughtUpAge +
		120*time.Second)

	// A new selection tip 100 s later moves the deadline.
	f.apply(genesisTip(700, "h700", 7))
	f.setNow(firstDeadline)
	f.cs.EvaluateAndSwitch()
	require.Equal(t, SelectionModePraos, f.mode(),
		"the old tip's deadline must not apply to a newer selection")
	f.setNow(firstDeadline.Add(100 * time.Second))
	f.cs.EvaluateAndSwitch()
	assert.Equal(t, SelectionModeGenesis, f.mode())
}

func TestGSMBonusIsBounded(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		draw time.Duration
		want time.Duration
	}{
		"above the bound": {time.Hour, CaughtUpBonusMax},
		"negative":        {-time.Second, 0},
		"inside":          {42 * time.Second, 42 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var asked time.Duration
			cs := NewChainSelector(ChainSelectorConfig{
				RandomBonus: func(m time.Duration) time.Duration {
					asked = m
					return tc.draw
				},
			})
			assert.Equal(t, tc.want, cs.drawBonus())
			assert.Equal(t, CaughtUpBonusMax, asked)
		})
	}
	for range 1000 {
		bonus := defaultRandomBonus(CaughtUpBonusMax)
		require.GreaterOrEqual(t, bonus, time.Duration(0))
		require.LessOrEqual(t, bonus, CaughtUpBonusMax)
	}
}

func TestGSMFailedSlotConversionCountsAsTooOld(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.exitToPraos(t)
	f.apply(genesisTip(600, "h600", 6))
	f.mu.Lock()
	f.slotErr = errors.New("past horizon")
	f.mu.Unlock()
	f.setNow(gsmEpoch.Add(2*time.Minute + DefaultMaxCaughtUpAge))
	f.cs.EvaluateAndSwitch()
	assert.Equal(t, SelectionModeGenesis, f.mode(),
		"an unconvertible selection tip is too old, without the bonus")
}

func TestGSMMinimumPraosStay(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	f.exitToPraos(t)
	// The selection tip (slot 5) is already far older than the age limit.
	f.setNow(gsmEpoch.Add(2*time.Minute + DefaultMaxCaughtUpAge - time.Second))
	f.cs.EvaluateAndSwitch()
	require.Equal(t, SelectionModePraos, f.mode(),
		"Praos must last at least the maximum caught-up age")
	f.setNow(gsmEpoch.Add(2*time.Minute + DefaultMaxCaughtUpAge))
	f.cs.EvaluateAndSwitch()
	assert.Equal(t, SelectionModeGenesis, f.mode())
}

// After a return to Genesis the frontier gathered in Praos drives the gates at
// once, with no further deliveries.
func TestGSMGatesActiveAfterReentry(t *testing.T) {
	t.Parallel()
	t.Run("limit on eagerness", func(t *testing.T) {
		t.Parallel()
		f := newGSMFixture(t, func(_ *gsmFixture, cfg *ChainSelectorConfig) {
			cfg.GenesisMode = false
			cfg.GenesisStateMachine = true
			cfg.SecurityParam = loeTestK
		})
		a, b := newTestConnectionId(1), newTestConnectionId(2)
		feedLoEChain(f.cs, a, "c", 1, 10)
		feedLoEChain(f.cs, b, "c", 1, 10)
		feedLoEChain(f.cs, a, "a", 11, 20)
		feedLoEChain(f.cs, b, "b", 11, 12)
		f.cs.SetLocalTip(loeTip("c", 10))
		require.False(t, f.cs.EagernessLimit().Active)

		f.cs.mutex.Lock()
		f.cs.praosMinStayUntil = time.Time{}
		f.cs.mutex.Unlock()
		f.setNow(gsmEpoch.Add(24 * time.Hour))
		f.cs.TouchPeerActivity(a)
		f.cs.TouchPeerActivity(b)
		f.cs.EvaluateAndSwitch()
		require.Equal(t, SelectionModeGenesis, f.mode())

		limit := f.cs.EagernessLimit()
		assert.True(t, limit.Active)
		assert.True(t, limit.Intersected)
		assert.Equal(t, uint64(10+loeTestK), limit.BlockNumber)
	})
	t.Run("density disconnect and comparison", func(t *testing.T) {
		t.Parallel()
		g := newGDDFixture(t, false)
		g.cs.genesisEnabled = true
		g.cs.config.SlotTime = func(uint64) (time.Time, error) {
			return time.Time{}, errors.New("unavailable")
		}
		dense, sparse := corrConn(1), corrConn(2)
		g.deliver(t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...)
		g.deliver(t, sparse, 0, 10, 60, 100, 200)
		g.evaluate()
		require.Empty(t, g.disconnected(), "no density disconnect in Praos")

		g.advance(time.Hour)
		g.cs.TouchPeerActivity(dense)
		g.cs.TouchPeerActivity(sparse)
		g.cs.SetLocalTip(genesisTip(1, "x", 1))
		g.evaluate()

		assert.Equal(t, SelectionModeGenesis, g.cs.SelectionMode())
		assert.Equal(t, []ouroboros.ConnectionId{sparse}, g.disconnected())
		best := g.cs.SelectBestChain()
		require.NotNil(t, best)
		assert.Equal(t, dense, *best, "density comparison must apply")
	})
}

func TestGSMEntryAndExitEventsAreAccurate(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	exits := make(chan GenesisModeExitedEvent, 2)
	entries := make(chan GenesisModeEnteredEvent, 2)
	bus.SubscribeFunc(GenesisModeExitedEventType, func(evt event.Event) {
		if e, ok := evt.Data.(GenesisModeExitedEvent); ok {
			exits <- e
		}
	})
	bus.SubscribeFunc(GenesisModeEnteredEventType, func(evt event.Event) {
		if e, ok := evt.Data.(GenesisModeEnteredEvent); ok {
			entries <- e
		}
	})
	f := newGSMFixture(t, func(_ *gsmFixture, cfg *ChainSelectorConfig) {
		cfg.EventBus = bus
	})
	f.exitToPraos(t)
	exit := testutil.RequireReceive(t, exits, 2*time.Second, "exit event")
	assert.Equal(t, uint64(5), exit.LocalSlot)
	assert.Equal(t, 1, exit.Peers)
	assert.Equal(t, uint64(30), exit.GenesisWindowSlots)

	f.setNow(gsmEpoch.Add(24 * time.Hour))
	f.cs.EvaluateAndSwitch()
	entry := testutil.RequireReceive(t, entries, 2*time.Second, "entry event")
	assert.Equal(t, uint64(5), entry.LocalSlot)
	assert.Equal(t, uint64(30), entry.GenesisWindowSlots)
}
