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

package leios

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// plus and times evaluate at run time: a constant expression would be exact
// where the controller's float64 arithmetic is not.
func plus(a, b float64) float64  { return a + b }
func times(a, b float64) float64 { return a * b }

func TestDefaultAdaptiveEbConfigIsTheReferences(t *testing.T) {
	t.Parallel()
	require.Equal(t,
		AdaptiveEbConfig{Decrease: 0.8, Increase: 0.02, MinScale: 0.1},
		DefaultAdaptiveEbConfig(),
	)
}

func TestStepAdaptiveEb(t *testing.T) {
	t.Parallel()
	cfg := DefaultAdaptiveEbConfig()
	for _, tc := range []struct {
		name   string
		missed bool
		scale  float64
		want   float64
	}{
		{"a miss multiplies by the decrease", true, 1, 0.8},
		{"a miss compounds", true, 0.8, times(0.8, 0.8)},
		{"a miss never goes below the floor", true, 0.11, 0.1},
		{"the floor holds under a miss", true, 0.1, 0.1},
		{"a quorum made adds the increase", false, 0.8, plus(0.8, 0.02)},
		{"a quorum made stops at one", false, 0.99, 1},
		{"one stays one", false, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, StepAdaptiveEb(cfg, tc.missed, tc.scale))
		})
	}
}

// Misses sink to the floor, quorums made restore the whole capacity, and one
// miss takes (1 - decrease) / increase quorums to undo, and one more for
// rounding, as the reference's own properties state.
func TestStepAdaptiveEbSequences(t *testing.T) {
	t.Parallel()
	cfg := DefaultAdaptiveEbConfig()

	scale := 1.0
	for range 100 {
		scale = StepAdaptiveEb(cfg, true, scale)
	}
	require.Equal(t, cfg.MinScale, scale)

	// From the floor it takes 45 quorums to be back, and a few more are
	// harmless.
	for range 45 {
		scale = StepAdaptiveEb(cfg, false, scale)
	}
	require.Equal(t, 1.0, scale)

	cut := StepAdaptiveEb(cfg, true, 1)
	require.Less(t, cut, 1.0)
	recovered := cut
	for range 11 {
		recovered = StepAdaptiveEb(cfg, false, recovered)
	}
	require.Equal(t, 1.0, recovered)

	bounded := 1.0
	for i := range 500 {
		bounded = StepAdaptiveEb(cfg, i%7 < 3, bounded)
		require.GreaterOrEqual(t, bounded, cfg.MinScale)
		require.LessOrEqual(t, bounded, 1.0)
	}
}

type adaptiveVotesCall struct {
	slot   uint64
	rbHash lcommon.Blake2b256
	ebHash lcommon.Blake2b256
}

// fakeAnnouncementVotes answers with the votes set for an announcing block.
type fakeAnnouncementVotes struct {
	mu      sync.Mutex
	votes   map[lcommon.Blake2b256]AnnouncementVotes
	err     error
	calls   []adaptiveVotesCall
	queried chan adaptiveVotesCall
}

func (f *fakeAnnouncementVotes) AnnouncementVotes(
	slot uint64,
	rbHash lcommon.Blake2b256,
	ebHash lcommon.Blake2b256,
) (AnnouncementVotes, error) {
	f.mu.Lock()
	call := adaptiveVotesCall{slot: slot, rbHash: rbHash, ebHash: ebHash}
	f.calls = append(f.calls, call)
	votes, err := f.votes[rbHash], f.err
	f.mu.Unlock()
	if f.queried != nil {
		f.queried <- call
	}
	return votes, err
}

func (f *fakeAnnouncementVotes) set(
	rbHash lcommon.Blake2b256,
	stake uint64,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.votes[rbHash] = AnnouncementVotes{
		VerifiedStake:    stake,
		TotalActiveStake: 100,
		Threshold:        big.NewRat(2, 3),
	}
}

func (f *fakeAnnouncementVotes) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

const (
	// quorumStake meets two thirds of the fixture's total stake and
	// missedStake is the largest stake that does not.
	quorumStake = 67
	missedStake = 66
	// certificationGap is the fixture's minimum slots between an
	// announcement and the block that may certify it.
	certificationGap = 10
)

type adaptiveFixture struct {
	c         *AdaptiveEbController
	votes     *fakeAnnouncementVotes
	caughtUp  atomic.Bool
	gapErr    error
	announced map[byte]lcommon.Blake2b256
	logs      *bytes.Buffer
	tip       atomic.Pointer[ocommon.Point]
}

// chainTip is the tip of the fixture's chain: the block of the most recent
// update, as the chain moves its tip before it publishes the update.
func (f *adaptiveFixture) chainTip() ocommon.Point {
	if tip := f.tip.Load(); tip != nil {
		return *tip
	}
	return ocommon.Point{}
}

func (f *adaptiveFixture) setTip(point ocommon.Point) {
	f.tip.Store(&point)
}

// publish moves the tip to the block and publishes its update on the bus.
func (f *adaptiveFixture) publish(
	bus *event.EventBus,
	evt chain.ChainBlockEvent,
) {
	f.setTip(evt.Point)
	bus.Publish(chain.ChainUpdateEventType,
		event.NewEvent(chain.ChainUpdateEventType, evt))
}

func newAdaptiveFixture(t *testing.T) *adaptiveFixture {
	t.Helper()
	f := &adaptiveFixture{
		votes: &fakeAnnouncementVotes{
			votes: make(map[lcommon.Blake2b256]AnnouncementVotes),
		},
		announced: make(map[byte]lcommon.Blake2b256),
		logs:      &bytes.Buffer{},
	}
	f.caughtUp.Store(true)
	// The bus is only used by the Start test.
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	c, err := NewAdaptiveEbController(AdaptiveEbControllerConfig{
		Logger: slog.New(slog.NewJSONHandler(
			f.logs,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		)),
		EventBus: bus,
		Config:   DefaultAdaptiveEbConfig(),
		Votes:    f.votes,
		CertificationGap: func(uint64) (uint64, error) {
			return certificationGap, f.gapErr
		},
		CaughtUp: f.caughtUp.Load,
		ChainTip: f.chainTip,
		inspect: func(evt chain.ChainBlockEvent) (adaptiveBlock, bool) {
			b := adaptiveBlock{
				hash:     lcommon.NewBlake2b256(evt.Point.Hash),
				prevHash: lcommon.NewBlake2b256(evt.Block.PrevHash),
				slot:     evt.Point.Slot,
			}
			if eb, ok := f.announced[evt.Point.Hash[0]]; ok {
				b.announced = &eb
			}
			return b, true
		},
	})
	require.NoError(t, err)
	f.c = c
	return f
}

func blockHash(n byte) lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(bytes.Repeat([]byte{n}, 32))
}

func blockEvent(n, prev byte, slot uint64) chain.ChainBlockEvent {
	return chain.ChainBlockEvent{
		Point: ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{n}, 32)},
		Block: models.Block{
			Hash:     bytes.Repeat([]byte{n}, 32),
			PrevHash: bytes.Repeat([]byte{prev}, 32),
			Slot:     slot,
		},
	}
}

func (f *adaptiveFixture) apply(data any) {
	if evt, ok := data.(chain.ChainBlockEvent); ok {
		f.setTip(evt.Point)
	}
	f.c.handleChainUpdate(event.NewEvent(chain.ChainUpdateEventType, data))
}

// announce adds block n at slot, extending prev, announcing an endorser
// block.
func (f *adaptiveFixture) announce(n, prev byte, slot uint64) {
	f.announced[n] = blockHash(100 + n)
	f.apply(blockEvent(n, prev, slot))
}

// sample adds the block that follows an announcing block n and with it
// samples the announcement.
func (f *adaptiveFixture) sample(n, prev byte, slot uint64, stake uint64) {
	f.votes.set(blockHash(prev), stake)
	f.apply(blockEvent(n, prev, slot))
}

func TestAdaptiveEbControllerStartsWithTheWholeCapacity(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	require.Equal(t, 1.0, f.c.Scale())
}

func TestAdaptiveEbControllerCutsOnAMissedQuorum(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale())

	f.announce(3, 2, 200)
	f.sample(4, 3, 200+certificationGap, missedStake)
	require.Equal(t, times(0.8, 0.8), f.c.Scale())

	require.Equal(t, []adaptiveVotesCall{
		{slot: 100, rbHash: blockHash(1), ebHash: blockHash(101)},
		{slot: 200, rbHash: blockHash(3), ebHash: blockHash(103)},
	}, f.votes.calls, "the votes are read for the announcing block")
}

func TestAdaptiveEbControllerRecoversOnQuorumsMade(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale())

	f.announce(3, 2, 200)
	f.sample(4, 3, 200+certificationGap, quorumStake)
	require.Equal(t, plus(0.8, 0.02), f.c.Scale())

	// The whole capacity is the ceiling.
	prev, slot := byte(4), uint64(300)
	for i := range 20 {
		n := byte(10 + 2*i)
		f.announce(n, prev, slot)
		f.sample(n+1, n, slot+certificationGap, quorumStake)
		prev, slot = n+1, slot+100
	}
	require.Equal(t, 1.0, f.c.Scale())
}

// A quorum made counts towards recovery even when the next block came too
// soon for the announcement to be certifiable, and a miss then is not a
// miss yet.
func TestAdaptiveEbControllerCertifiability(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale())

	f.announce(3, 2, 200)
	f.sample(4, 3, 200+certificationGap-1, missedStake)
	require.Equal(t, 0.8, f.c.Scale(), "too soon to certify: not a miss")

	f.announce(5, 4, 300)
	f.sample(6, 5, 300+certificationGap-1, quorumStake)
	require.Equal(t, plus(0.8, 0.02), f.c.Scale(), "a quorum made is counted")

	f.announce(7, 6, 400)
	f.sample(8, 7, 400+certificationGap, missedStake)
	require.Equal(
		t,
		times(plus(0.8, 0.02), 0.8),
		f.c.Scale(),
		"the gap is inclusive",
	)
}

func TestAdaptiveEbControllerQuorumBoundary(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap, quorumStake)
	require.Equal(t, 1.0, f.c.Scale(), "meeting the threshold is a quorum")

	f.announce(3, 2, 200)
	f.sample(4, 3, 200+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale(), "one stake short is a miss")
}

// Only the block that extends an announcing block samples it: another tip
// after a rollback, or a gap in what was observed, samples nothing.
func TestAdaptiveEbControllerSamplesOnlyTheNextBlock(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)

	// The announcing block is not the one the next block extends.
	f.announce(1, 0, 100)
	f.votes.set(blockHash(1), missedStake)
	f.apply(blockEvent(2, 9, 100+certificationGap))
	require.Equal(t, 1.0, f.c.Scale())
	require.Zero(t, f.votes.callCount())

	// A block that announced nothing has nothing to sample.
	f.apply(blockEvent(3, 2, 200))
	f.apply(blockEvent(4, 3, 300))
	require.Equal(t, 1.0, f.c.Scale())
	require.Zero(t, f.votes.callCount())

	// An announcement is sampled once, by its own successor.
	f.announce(5, 4, 400)
	f.sample(6, 5, 400+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale())
	f.apply(blockEvent(7, 6, 500))
	require.Equal(t, 0.8, f.c.Scale())
	require.Equal(t, 1, f.votes.callCount())
}

func TestAdaptiveEbControllerRollbackSamplesNothingAndKeepsTheScale(
	t *testing.T,
) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale())

	f.announce(3, 2, 200)
	f.apply(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: 150, Hash: bytes.Repeat([]byte{2}, 32)},
	})
	require.Equal(t, 0.8, f.c.Scale(), "a rollback does not rewind the scale")

	// The replacement block extends the rollback point, not the announcing
	// block that was abandoned.
	f.votes.set(blockHash(3), missedStake)
	f.apply(blockEvent(4, 2, 200+certificationGap))
	require.Equal(t, 0.8, f.c.Scale())
	require.Equal(t, 1, f.votes.callCount())

	// The chain is followed again from the first block after it.
	f.announce(5, 4, 300)
	f.sample(6, 5, 300+certificationGap, missedStake)
	require.Equal(t, times(0.8, 0.8), f.c.Scale())
}

// The threshold is met at exactly its stake, not only above it.
func TestAdaptiveEbControllerQuorumAtExactlyTheThreshold(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.votes.mu.Lock()
	f.votes.votes[blockHash(1)] = AnnouncementVotes{
		VerifiedStake:    50,
		TotalActiveStake: 100,
		Threshold:        big.NewRat(1, 2),
	}
	f.votes.mu.Unlock()
	f.apply(blockEvent(2, 1, 100+certificationGap))
	require.Equal(t, 1.0, f.c.Scale(), "half of the stake meets one half")

	f.announce(3, 2, 200)
	f.votes.mu.Lock()
	f.votes.votes[blockHash(3)] = AnnouncementVotes{
		VerifiedStake:    49,
		TotalActiveStake: 100,
		Threshold:        big.NewRat(1, 2),
	}
	f.votes.mu.Unlock()
	f.apply(blockEvent(4, 3, 200+certificationGap))
	require.Equal(t, 0.8, f.c.Scale())
}

func TestAdaptiveEbControllerOnlySamplesWhenCaughtUp(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.caughtUp.Store(false)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap, missedStake)
	require.Equal(t, 1.0, f.c.Scale())
	require.Zero(t, f.votes.callCount(), "a syncing node has no votes to read")

	f.caughtUp.Store(true)
	f.announce(3, 2, 200)
	f.sample(4, 3, 200+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale())
}

// Without the votes or the gap or the threshold the outcome is not known,
// and an unknown outcome neither cuts nor recovers.
func TestAdaptiveEbControllerUnknownOutcomeIsNotASample(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale())

	f.votes.err = errors.New("committee unavailable")
	f.announce(3, 2, 200)
	f.sample(4, 3, 200+certificationGap, quorumStake)
	require.Equal(t, 0.8, f.c.Scale(), "no committee: neither miss nor quorum")
	f.votes.err = nil

	f.gapErr = errors.New("no parameters")
	f.announce(5, 4, 300)
	f.sample(6, 5, 300+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale(), "no gap: not certifiable, not a miss")
	f.announce(7, 6, 400)
	f.sample(8, 7, 400+certificationGap, quorumStake)
	require.Equal(
		t,
		plus(0.8, 0.02),
		f.c.Scale(),
		"a quorum made counts regardless",
	)
	f.gapErr = nil

	f.announce(9, 8, 500)
	f.votes.mu.Lock()
	f.votes.votes[blockHash(9)] = AnnouncementVotes{VerifiedStake: 10}
	f.votes.mu.Unlock()
	f.apply(blockEvent(10, 9, 500+certificationGap))
	require.Equal(t, plus(0.8, 0.02), f.c.Scale(), "no threshold: unknown")
}

func TestAdaptiveEbControllerFloor(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	prev := byte(0)
	for i := range 40 {
		n := byte(1 + 2*i)
		f.announce(n, prev, uint64(100*(i+1)))
		f.sample(n+1, n, uint64(100*(i+1))+certificationGap, missedStake)
		prev = n + 1
	}
	require.Equal(t, 0.1, f.c.Scale())
}

// Each sample is logged with the fields the reference traces.
func TestAdaptiveEbControllerLogsEachSample(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.sample(2, 1, 100+certificationGap-1, missedStake)
	f.announce(3, 2, 200)
	f.sample(4, 3, 200+certificationGap, missedStake)

	out := f.logs.String()
	require.Contains(t, out, `"msg":"leios adaptive endorsements"`)
	require.Contains(t, out, `"participation":0.66`)
	require.Contains(t, out, `"missed_quorum":true`)
	require.Contains(t, out, `"certifiable":false`)
	require.Contains(t, out, `"certifiable":true`)
	require.Contains(t, out, `"eb_capacity_scale":0.8`)
}

// The controller follows the chain updates the node publishes, in order.
func TestAdaptiveEbControllerFollowsChainUpdates(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	f.votes.queried = make(chan adaptiveVotesCall, 4)
	c, err := NewAdaptiveEbController(AdaptiveEbControllerConfig{
		EventBus: bus,
		Config:   DefaultAdaptiveEbConfig(),
		Votes:    f.votes,
		CertificationGap: func(uint64) (uint64, error) {
			return certificationGap, nil
		},
		CaughtUp: func() bool { return true },
		ChainTip: f.chainTip,
		inspect:  f.c.inspect,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, c.Start(ctx))
	t.Cleanup(func() { _ = c.Stop() })

	f.announced[1] = blockHash(101)
	f.votes.set(blockHash(1), missedStake)
	f.publish(bus, blockEvent(1, 0, 100))
	f.publish(bus, blockEvent(2, 1, 100+certificationGap))

	call := testutil.RequireReceive(t, f.votes.queried, 5*time.Second,
		"the announcement's votes are read")
	require.Equal(t, blockHash(1), call.rbHash)
	require.Eventually(t, func() bool { return c.Scale() == 0.8 },
		5*time.Second, time.Millisecond)

	// Nothing is followed once stopped, and a start begins with the whole
	// capacity again, as a restart of the node does.
	require.NoError(t, c.Stop())
	require.Equal(t, 0.8, c.Scale())
	require.NoError(t, c.Start(ctx))
	require.Equal(t, 1.0, c.Scale())
}

// A subscription the bus closes ends the event loop. The controller then
// stops following the chain, so its scale no longer says anything: it returns
// to the whole capacity, as without a controller, and can be started again.
func TestAdaptiveEbControllerClosedSubscriptionResetsAndStops(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	f.votes.queried = make(chan adaptiveVotesCall, 4)
	c, err := NewAdaptiveEbController(AdaptiveEbControllerConfig{
		Logger:   slog.New(slog.NewJSONHandler(f.logs, nil)),
		EventBus: bus,
		Config:   DefaultAdaptiveEbConfig(),
		Votes:    f.votes,
		CertificationGap: func(uint64) (uint64, error) {
			return certificationGap, nil
		},
		CaughtUp: func() bool { return true },
		ChainTip: f.chainTip,
		inspect:  f.c.inspect,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, c.Start(ctx))
	t.Cleanup(func() { _ = c.Stop() })

	f.announced[1] = blockHash(101)
	f.votes.set(blockHash(1), missedStake)
	f.publish(bus, blockEvent(1, 0, 100))
	f.publish(bus, blockEvent(2, 1, 100+certificationGap))
	testutil.RequireReceive(t, f.votes.queried, 5*time.Second,
		"the announcement's votes are read")
	require.Eventually(t, func() bool { return c.Scale() == 0.8 },
		5*time.Second, time.Millisecond)

	c.mu.Lock()
	subID := c.subID
	c.mu.Unlock()
	bus.Unsubscribe(chain.ChainUpdateEventType, subID)
	require.Eventually(t, func() bool { return c.Scale() == 1.0 },
		5*time.Second, time.Millisecond,
		"a controller that follows nothing forges with the whole capacity")
	require.Contains(t, f.logs.String(), "subscription closed")

	f.announced[3] = blockHash(103)
	f.votes.set(blockHash(3), missedStake)
	require.NoError(t, c.Start(ctx))
	f.publish(bus, blockEvent(3, 2, 300))
	f.publish(bus, blockEvent(4, 3, 300+certificationGap))
	require.Eventually(t, func() bool { return c.Scale() == 0.8 },
		5*time.Second, time.Millisecond,
		"the restarted controller follows the chain again")
}

// adaptiveTestHeader is a header with the identity the controller reads.
type adaptiveTestHeader struct {
	lcommon.BlockHeader
	hash, prev lcommon.Blake2b256
	slot       uint64
	eb         *lcommon.Blake2b256
}

func (h adaptiveTestHeader) Hash() lcommon.Blake2b256     { return h.hash }
func (h adaptiveTestHeader) PrevHash() lcommon.Blake2b256 { return h.prev }
func (h adaptiveTestHeader) SlotNumber() uint64           { return h.slot }

type adaptiveTestAnnouncingHeader struct{ adaptiveTestHeader }

func (h adaptiveTestAnnouncingHeader) LeiosAnnouncement() (
	lcommon.Blake2b256,
	uint64,
	bool,
) {
	return *h.eb, 1234, h.eb != nil
}

type adaptiveTestBlock struct {
	lcommon.Block
	header lcommon.BlockHeader
}

func (b adaptiveTestBlock) Header() lcommon.BlockHeader { return b.header }

// The controller reads a block's identity and its announcement off the
// header, in the shape the vote manager reads announcements.
func TestAdaptiveBlockOf(t *testing.T) {
	t.Parallel()
	eb := blockHash(9)
	base := adaptiveTestHeader{hash: blockHash(1), prev: blockHash(2), slot: 77}

	plain := adaptiveBlockOf(adaptiveTestBlock{header: base})
	require.Equal(t, adaptiveBlock{
		hash: blockHash(1), prevHash: blockHash(2), slot: 77,
	}, plain)

	base.eb = &eb
	announcing := adaptiveBlockOf(adaptiveTestBlock{
		header: adaptiveTestAnnouncingHeader{base},
	})
	require.NotNil(t, announcing.announced)
	require.Equal(t, eb, *announcing.announced)
	require.Equal(t, blockHash(2), announcing.prevHash)
	require.Equal(t, uint64(77), announcing.slot)

	// A header without an announcement in its extension announces nothing.
	require.Nil(t, adaptiveBlockOf(adaptiveTestBlock{
		header: adaptiveTestHeader{hash: blockHash(1)},
	}).announced)
}

// dijkstraRankingBlock encodes a Dijkstra ranking block whose header carries
// the Leios extension, with the given announcement (nil for none), and
// decodes it back, so the bytes are the ones a node stores.
func dijkstraRankingBlock(
	t *testing.T,
	number, slot uint64,
	prev lcommon.Blake2b256,
	announcement []byte,
) *dijkstra.DijkstraBlock {
	t.Helper()
	certified, err := cbor.Encode(false)
	require.NoError(t, err)
	if announcement == nil {
		announcement, err = cbor.Encode(nil)
		require.NoError(t, err)
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: number,
					Slot:        slot,
					PrevHash:    prev,
					VrfKey:      make([]byte, 32),
					VrfResult: lcommon.VrfResult{
						Output: make([]byte, 64),
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
				Signature: make([]byte, 448),
			},
			LeiosHeaderExtension: []cbor.RawMessage{certified, announcement},
		},
	}
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	raw, err := block.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := dijkstra.NewDijkstraBlockFromCbor(raw)
	require.NoError(t, err)
	return decoded
}

func chainEventOf(block *dijkstra.DijkstraBlock) chain.ChainBlockEvent {
	return chain.ChainBlockEvent{
		Point: ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		},
		Block: models.Block{
			Hash:     block.Hash().Bytes(),
			PrevHash: block.PrevHash().Bytes(),
			Slot:     block.SlotNumber(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			Cbor:     block.Cbor(),
		},
	}
}

// The default decoder reads a real Dijkstra block as the node stores it: its
// identity, and the endorser block its header announces.
func TestInspectChainBlockReadsDijkstraAnnouncement(t *testing.T) {
	t.Parallel()
	ebHash := blockHash(7)
	announcement, err := cbor.Encode([]any{ebHash.Bytes(), uint64(4096)})
	require.NoError(t, err)
	announcer := dijkstraRankingBlock(t, 5, 100, blockHash(1), announcement)
	plain := dijkstraRankingBlock(t, 6, 130, announcer.Hash(), nil)

	got, ok := inspectChainBlock(chainEventOf(announcer))
	require.True(t, ok)
	require.Equal(t, announcer.Hash(), got.hash)
	require.Equal(t, blockHash(1), got.prevHash)
	require.Equal(t, uint64(100), got.slot)
	require.NotNil(t, got.announced)
	require.Equal(t, ebHash, *got.announced)

	got, ok = inspectChainBlock(chainEventOf(plain))
	require.True(t, ok)
	require.Equal(t, plain.Hash(), got.hash)
	require.Equal(t, announcer.Hash(), got.prevHash)
	require.Nil(t, got.announced)

	_, ok = inspectChainBlock(chain.ChainBlockEvent{})
	require.False(t, ok, "a block that does not decode is not a block")
}

// A controller on the default decoder samples the announcement of a real
// Dijkstra block when the block that extends it arrives.
func TestAdaptiveEbControllerSamplesRealDijkstraBlocks(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	c, err := NewAdaptiveEbController(AdaptiveEbControllerConfig{
		EventBus: bus,
		Config:   DefaultAdaptiveEbConfig(),
		Votes:    f.votes,
		CertificationGap: func(uint64) (uint64, error) {
			return certificationGap, nil
		},
		CaughtUp: func() bool { return true },
		ChainTip: f.chainTip,
	})
	require.NoError(t, err)

	announcement, err := cbor.Encode(
		[]any{blockHash(9).Bytes(), uint64(4096)},
	)
	require.NoError(t, err)
	announcer := dijkstraRankingBlock(t, 5, 100, blockHash(1), announcement)
	next := dijkstraRankingBlock(
		t, 6, 100+certificationGap, announcer.Hash(), nil,
	)
	f.votes.set(announcer.Hash(), missedStake)

	for _, block := range []*dijkstra.DijkstraBlock{announcer, next} {
		evt := chainEventOf(block)
		f.setTip(evt.Point)
		c.handleChainUpdate(event.NewEvent(chain.ChainUpdateEventType, evt))
	}
	require.Equal(t, 0.8, c.Scale())
	require.Equal(t, []adaptiveVotesCall{{
		slot:   100,
		rbHash: announcer.Hash(),
		ebHash: blockHash(9),
	}}, f.votes.calls)
}

// An update handled after the chain has moved past its block was queued, as
// updates are while a node syncs: its announcement's votes were never
// collected, so it is not sampled even though the node is now caught up.
func TestAdaptiveEbControllerSamplesOnlyTheChainTip(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	f.announce(1, 0, 100)
	f.votes.set(blockHash(1), missedStake)
	f.setTip(blockEvent(3, 2, 200).Point)
	f.c.handleChainUpdate(event.NewEvent(
		chain.ChainUpdateEventType, blockEvent(2, 1, 100+certificationGap)))
	require.Equal(t, 1.0, f.c.Scale())
	require.Zero(t, f.votes.callCount())

	f.announce(4, 2, 300)
	f.sample(5, 4, 300+certificationGap, missedStake)
	require.Equal(t, 0.8, f.c.Scale(), "the block at the tip is sampled")
}

// A Stop that finds another in progress returns only once the event loop has
// finished its vote query.
func TestAdaptiveEbControllerConcurrentStopWaitsForTheLoop(t *testing.T) {
	t.Parallel()
	f := newAdaptiveFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	f.votes.queried = make(chan adaptiveVotesCall)
	c, err := NewAdaptiveEbController(AdaptiveEbControllerConfig{
		EventBus: bus,
		Config:   DefaultAdaptiveEbConfig(),
		Votes:    f.votes,
		CertificationGap: func(uint64) (uint64, error) {
			return certificationGap, nil
		},
		CaughtUp: func() bool { return true },
		ChainTip: f.chainTip,
		inspect:  f.c.inspect,
	})
	require.NoError(t, err)
	require.NoError(t, c.Start(t.Context()))

	f.announced[1] = blockHash(101)
	f.votes.set(blockHash(1), missedStake)
	f.publish(bus, blockEvent(1, 0, 100))
	f.publish(bus, blockEvent(2, 1, 100+certificationGap))
	// The loop is now blocked inside the vote query until it is received.
	require.Eventually(t, func() bool { return f.votes.callCount() == 1 },
		5*time.Second, time.Millisecond)

	first := make(chan struct{})
	go func() {
		_ = c.Stop()
		close(first)
	}()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.stopping
	}, 5*time.Second, time.Millisecond)
	second := make(chan struct{})
	go func() {
		_ = c.Stop()
		close(second)
	}()
	testutil.RequireNoReceive(t, second, 100*time.Millisecond,
		"a second Stop must wait for the loop")
	testutil.RequireReceive(t, f.votes.queried, 5*time.Second,
		"the vote query completes")
	testutil.RequireReceive(t, first, 5*time.Second, "the first Stop returns")
	testutil.RequireReceive(t, second, 5*time.Second, "the second Stop returns")
}
