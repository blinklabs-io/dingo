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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// committeeCoalesceWindow is how long a test waits to conclude that a second
// committee computation is never going to start. Coalescing makes the second
// provider call impossible rather than merely late -- the leader is held
// inside the provider for the whole window, so no other caller can find a
// populated memo to hit instead -- so this window only has to be long enough
// for a would-be second caller to be scheduled.
const committeeCoalesceWindow = 500 * time.Millisecond

// gatedParamsProvider holds committee computation open inside
// LeiosCommitteeParameters, the first provider call
// committeeAndParamsForEpoch makes, so a test can park additional callers
// behind an in-flight computation deterministically instead of racing them.
//
// The first call signals firstCall and blocks until release is closed. Every
// later call signals extraCall (non-blocking, so the provider is never the
// thing that deadlocks a test) and, unless blockAll is set, returns
// immediately: a test asserting that no second computation starts must not
// depend on the second computation also being blocked.
type gatedParamsProvider struct {
	mu        sync.Mutex
	calls     int
	blockAll  bool
	firstCall chan struct{}
	extraCall chan struct{}
	release   chan struct{}
	firstOnce sync.Once
}

func newGatedParamsProvider() *gatedParamsProvider {
	return &gatedParamsProvider{
		firstCall: make(chan struct{}),
		extraCall: make(chan struct{}, 64),
		release:   make(chan struct{}),
	}
}

func (p *gatedParamsProvider) LeiosCommitteeParameters(uint64) (
	uint16,
	*big.Rat,
	error,
) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	blockAll := p.blockAll
	p.mu.Unlock()
	if n == 1 {
		p.firstOnce.Do(func() { close(p.firstCall) })
	} else {
		select {
		case p.extraCall <- struct{}{}:
		default:
		}
	}
	if n == 1 || blockAll {
		<-p.release
	}
	return 10, big.NewRat(7, 10), nil
}

func (p *gatedParamsProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *gatedParamsProvider) releaseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.release:
	default:
		close(p.release)
	}
}

// panickingStakeProvider panics on its first GetStakeDistribution call and
// serves the distribution normally afterwards, so a test can drive a
// committee computation into a panic and then verify the epoch is still
// computable.
type panickingStakeProvider struct {
	mu     sync.Mutex
	pools  map[string]uint64
	total  uint64
	calls  int
	panics int
}

func (p *panickingStakeProvider) GetStakeDistribution(
	uint64,
) (map[string]uint64, uint64, error) {
	p.mu.Lock()
	p.calls++
	shouldPanic := p.calls <= p.panics
	pools := maps.Clone(p.pools)
	total := p.total
	p.mu.Unlock()
	if shouldPanic {
		panic("stake distribution exploded")
	}
	return pools, total, nil
}

func (p *panickingStakeProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// committeeCall runs CommitteeForEpoch on its own goroutine and reports the
// outcome on a buffered channel, so a test can hold several callers against
// one in-flight computation.
type committeeCall struct {
	committee *Committee
	err       error
}

func startCommitteeCall(
	mgr *VoteManager,
	epoch uint64,
) <-chan committeeCall {
	ch := make(chan committeeCall, 1)
	go func() {
		committee, err := mgr.CommitteeForEpoch(epoch)
		ch <- committeeCall{committee: committee, err: err}
	}()
	return ch
}

// committeeEpochClaimed reports whether an in-flight computation is recorded
// for epoch.
func committeeEpochClaimed(m *VoteManager, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.committeeInFlight[epoch]
	return ok
}

// committeeMemoEntry reads the memoized entry for epoch, if any.
func committeeMemoEntry(m *VoteManager, epoch uint64) *epochEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.committees[epoch]
}

// committeeWaiterCount reports how many callers have parked on the in-flight
// committee computation for epoch.
//
// This is the deterministic observation that coalescing happened: a caller
// that started its own computation instead of joining the leader's never
// registers as a waiter. A timing window alone cannot prove it, because a
// follower the scheduler delayed past the window looks identical to a
// follower that coalesced.
func committeeWaiterCount(m *VoteManager, epoch uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	call, ok := m.committeeInFlight[epoch]
	if !ok {
		return 0
	}
	return call.waiters
}

// Concurrent same-epoch cache miss: the callers that arrive while a committee
// computation is already in flight join it instead of repeating the parameter
// lookup, the stake-distribution read, the committee sort, and the
// proof-of-possession verifications. Every path into
// committeeAndParamsForEpoch is peer-driven, so before coalescing one
// announcement diffused to N peers started N identical computations and
// discarded N-1 of the results.
func TestVoteManagerCommitteeCoalescesConcurrentSameEpochMisses(t *testing.T) {
	t.Parallel()

	const callers = 8
	params := newGatedParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)

	// The leader claims epoch 5 and is held inside the params provider, so
	// no later caller can find a populated memo to hit instead of joining.
	leader := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireReceive(
		t,
		params.firstCall,
		testutil.AsyncWait,
		"leader did not reach the committee params provider",
	)

	followers := make([]<-chan committeeCall, 0, callers-1)
	for range callers - 1 {
		followers = append(followers, startCommitteeCall(fixture.mgr, 5))
	}

	// Every follower must be parked on the leader's computation. This is the
	// load-bearing assertion: it cannot pass for a follower that ran its own
	// computation, whereas the provider-call window below can pass merely
	// because the scheduler was slow.
	testutil.WaitForCondition(
		t,
		func() bool {
			return committeeWaiterCount(fixture.mgr, 5) == callers-1
		},
		testutil.AsyncWait,
		"followers did not join the leader's committee computation",
	)

	// The regression: without coalescing every follower runs its own
	// computation and calls the params provider again.
	testutil.RequireNoReceive(
		t,
		params.extraCall,
		committeeCoalesceWindow,
		"a concurrent same-epoch cache miss started a second committee computation",
	)

	params.releaseAll()

	leaderResult := testutil.RequireReceive(
		t, leader, testutil.AsyncWait, "leader did not return",
	)
	require.NoError(t, leaderResult.err)
	require.NotNil(t, leaderResult.committee)
	for i, follower := range followers {
		got := testutil.RequireReceive(
			t, follower, testutil.AsyncWait, "follower did not return",
		)
		require.NoErrorf(t, got.err, "follower %d", i)
		require.Samef(
			t, leaderResult.committee, got.committee,
			"follower %d must receive the leader's committee", i,
		)
	}
	require.Equal(
		t,
		1,
		params.callCount(),
		"committee parameters must be resolved once per epoch, not once per caller",
	)
	require.Equal(
		t,
		1,
		fixture.stake.callCount(),
		"the stake distribution must be read once per epoch, not once per caller",
	)
}

// Absence case: a single, uncontended cache miss still performs the
// computation exactly once -- coalescing must not turn a lone miss into zero
// computations (a caller that parks on a claim nobody owns) or two.
func TestVoteManagerCommitteeSingleMissComputesOnce(t *testing.T) {
	t.Parallel()

	params := newGatedParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)
	params.releaseAll()

	first, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, 1, params.callCount())
	require.Equal(t, 1, fixture.stake.callCount())

	// And the claim was released, not left held: a second call is served
	// from the memo rather than parking on an in-flight computation that
	// nobody is running.
	second, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Equal(t, 1, params.callCount())
	require.Equal(t, 1, fixture.stake.callCount())
}

// A failed computation releases its waiters with the error and is not
// memoized, so the epoch stays retryable. Caching the failure would pin the
// epoch to a keyless committee, and leaving the claim held would park every
// later caller on a computation that had already finished.
func TestVoteManagerCommitteeFailureReleasesWaiters(t *testing.T) {
	t.Parallel()

	params := newGatedParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)
	snapshotErr := errors.New("snapshot not ready")
	fixture.stake.setError(snapshotErr)

	leader := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireReceive(
		t,
		params.firstCall,
		testutil.AsyncWait,
		"leader did not reach the committee params provider",
	)
	waiter := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireNoReceive(
		t,
		params.extraCall,
		committeeCoalesceWindow,
		"the waiter started its own committee computation",
	)

	params.releaseAll()

	leaderResult := testutil.RequireReceive(
		t, leader, testutil.AsyncWait, "leader did not return",
	)
	require.ErrorIs(t, leaderResult.err, snapshotErr)
	waiterResult := testutil.RequireReceive(
		t, waiter, testutil.AsyncWait, "waiter was not released by the failure",
	)
	require.ErrorIs(
		t, waiterResult.err, snapshotErr,
		"a waiter must receive the leader's failure, not park on it",
	)
	require.Nil(t, waiterResult.committee)

	// Retryable: the failure was not memoized and the claim was released.
	fixture.stake.setError(nil)
	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	require.Equal(t, uint64(10), committee.Size())
}

// Cancellation: a waiter parked on another caller's in-flight computation is
// released when the manager stops. The leader can be blocked inside the stake
// or key provider on a read carrying no deadline, so a waiter that only ever
// woke on the leader's completion would hold a connection's protocol worker
// across shutdown.
func TestVoteManagerCommitteeWaiterReleasedOnStop(t *testing.T) {
	t.Parallel()

	params := newGatedParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)
	t.Cleanup(params.releaseAll)

	leader := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireReceive(
		t,
		params.firstCall,
		testutil.AsyncWait,
		"leader did not reach the committee params provider",
	)
	waiter := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireNoReceive(
		t,
		params.extraCall,
		committeeCoalesceWindow,
		"the waiter started its own committee computation",
	)

	// Stop while the leader is still blocked in the provider.
	require.NoError(t, fixture.mgr.Stop())

	waiterResult := testutil.RequireReceive(
		t, waiter, testutil.AsyncWait, "waiter was not released at shutdown",
	)
	require.ErrorIs(t, waiterResult.err, ErrVoteManagerStopped)
	require.Nil(t, waiterResult.committee)

	// The leader still runs to completion; its result is simply no longer
	// wanted by anyone.
	params.releaseAll()
	leaderResult := testutil.RequireReceive(
		t, leader, testutil.AsyncWait, "leader did not return after the stop",
	)
	require.NoError(t, leaderResult.err)
}

// A panic unwinding through the leader releases its waiters with an error and
// gives up the epoch's claim, rather than leaving the epoch permanently
// uncomputable with every later caller parked on a claim nobody owns. The
// panic itself still reaches the leader's caller: a fault in this node's own
// stake handling must not be laundered into a routine per-epoch error.
func TestVoteManagerCommitteePanicReleasesWaiters(t *testing.T) {
	t.Parallel()

	params := newGatedParamsProvider()
	stake := &panickingStakeProvider{panics: 1}
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			stake.pools = f.stake.pools
			stake.total = f.stake.total
			cfg.StakeProvider = stake
		},
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)

	type panicResult struct {
		recovered any
	}
	leaderPanic := make(chan panicResult, 1)
	go func() {
		defer func() { leaderPanic <- panicResult{recovered: recover()} }()
		_, _ = fixture.mgr.CommitteeForEpoch(5)
	}()
	testutil.RequireReceive(
		t,
		params.firstCall,
		testutil.AsyncWait,
		"leader did not reach the committee params provider",
	)
	waiter := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireNoReceive(
		t,
		params.extraCall,
		committeeCoalesceWindow,
		"the waiter started its own committee computation",
	)

	params.releaseAll()

	got := testutil.RequireReceive(
		t, leaderPanic, testutil.AsyncWait, "leader goroutine did not finish",
	)
	require.NotNil(
		t, got.recovered,
		"the panic must keep unwinding to the leader's caller",
	)
	waiterResult := testutil.RequireReceive(
		t, waiter, testutil.AsyncWait, "waiter was not released by the panic",
	)
	require.ErrorIs(t, waiterResult.err, ErrCommitteeComputationAborted)
	require.Nil(t, waiterResult.committee)

	// The claim was released: the epoch is computable again.
	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	require.Equal(t, uint64(10), committee.Size())
	require.Equal(t, 2, stake.callCount())
}

// The coalescing map is size-bounded like every other admission structure
// here: once committeeInFlightMaxEpochs distinct epochs are computing, a
// further distinct epoch is refused instead of admitted into unbounded
// concurrent work. The refusal is not memoized.
func TestVoteManagerCommitteeInFlightEpochsAreBounded(t *testing.T) {
	t.Parallel()

	params := newGatedParamsProvider()
	params.blockAll = true
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)
	t.Cleanup(params.releaseAll)

	leaders := make([]<-chan committeeCall, 0, committeeInFlightMaxEpochs)
	for i := range uint64(committeeInFlightMaxEpochs) {
		leaders = append(leaders, startCommitteeCall(fixture.mgr, 1000+i))
	}
	testutil.WaitForCondition(
		t,
		func() bool {
			return params.callCount() == committeeInFlightMaxEpochs
		},
		testutil.AsyncWait,
		"not every epoch reached the committee params provider",
	)

	_, err := fixture.mgr.CommitteeForEpoch(2000)
	require.ErrorIs(t, err, ErrCommitteeComputationBacklog)

	params.releaseAll()
	for i, leader := range leaders {
		got := testutil.RequireReceive(
			t, leader, testutil.AsyncWait, "in-flight leader did not return",
		)
		require.NoErrorf(t, got.err, "leader %d", i)
	}

	// Nothing was memoized for the refused epoch, and the backlog cleared,
	// so it is computable now.
	committee, err := fixture.mgr.CommitteeForEpoch(2000)
	require.NoError(t, err)
	require.Equal(t, uint64(10), committee.Size())
}

// A rollback landing while a committee computation is in flight must not have
// its memo clear undone by that computation completing afterwards: the
// in-flight result was derived from a stake snapshot the rollback may have
// invalidated. The value is still delivered to the callers waiting on it, and
// the next caller recomputes from the post-rollback snapshot.
func TestVoteManagerCommitteeRollbackDuringComputationIsNotMemoized(
	t *testing.T,
) {
	t.Parallel()

	params := newGatedParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)

	leader := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireReceive(
		t,
		params.firstCall,
		testutil.AsyncWait,
		"leader did not reach the committee params provider",
	)
	waiter := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireNoReceive(
		t,
		params.extraCall,
		committeeCoalesceWindow,
		"the waiter started its own committee computation",
	)

	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.NewPoint(
			400,
			lcommon.NewBlake2b256([]byte("rollback")).Bytes(),
		),
	})
	params.releaseAll()

	leaderResult := testutil.RequireReceive(
		t, leader, testutil.AsyncWait, "leader did not return",
	)
	require.NoError(t, leaderResult.err)
	require.NotNil(t, leaderResult.committee)
	waiterResult := testutil.RequireReceive(
		t, waiter, testutil.AsyncWait, "waiter was not released",
	)
	require.NoError(t, waiterResult.err)
	require.Same(t, leaderResult.committee, waiterResult.committee)

	// Not memoized: the next caller recomputes rather than reading the
	// pre-rollback committee back out of the memo the rollback cleared.
	require.Equal(t, 1, fixture.stake.callCount())
	recomputed, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	require.Equal(
		t, 2, fixture.stake.callCount(),
		"a rollback must force recomputation, not be undone by an "+
			"in-flight computation completing after it",
	)
	require.NotSame(t, leaderResult.committee, recomputed)
}

// A leader still blocked in a provider when Stop returns must not leave its
// claim behind for the next lifecycle.
//
// Stop closes committeeStopCh, which releases the waiters that exist then, but
// the leader itself outlives the stop. If its claim stayed in the map, a caller
// arriving after the next Start would join a computation belonging to the
// previous lifecycle and park on the fresh stop channel until that leader
// returned -- or forever, since the provider read it is blocked in carries no
// deadline of its own.
func TestVoteManagerCommitteeClaimNotInheritedAcrossRestart(t *testing.T) {
	t.Parallel()

	params := newGatedParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)
	t.Cleanup(params.releaseAll)

	leader := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireReceive(
		t,
		params.firstCall,
		testutil.AsyncWait,
		"leader did not reach the committee params provider",
	)

	// Stop while the leader is still blocked in the provider.
	require.NoError(t, fixture.mgr.Stop())
	require.False(
		t,
		committeeEpochClaimed(fixture.mgr, 5),
		"a stopped lifecycle must not retain the epoch's in-flight claim",
	)

	require.NoError(t, fixture.mgr.Start(context.Background()))
	t.Cleanup(func() { _ = fixture.mgr.Stop() })

	// The new lifecycle's caller must compute for itself. Joining the stopped
	// lifecycle's leader would mean no second provider call ever happens.
	next := startCommitteeCall(fixture.mgr, 5)
	testutil.RequireReceive(
		t,
		params.extraCall,
		testutil.AsyncWait,
		"the new lifecycle's caller joined the stopped lifecycle's computation instead of computing",
	)

	params.releaseAll()
	nextResult := testutil.RequireReceive(
		t, next, testutil.AsyncWait, "the new lifecycle's caller did not return",
	)
	require.NoError(t, nextResult.err)
	leaderResult := testutil.RequireReceive(
		t, leader, testutil.AsyncWait, "leader did not return after the stop",
	)
	require.NoError(t, leaderResult.err)

	// The stopped lifecycle's leader must not have installed its result as
	// the new lifecycle's memo. Clearing the claim stops a new caller
	// joining it; only the generation bump stops it installing.
	memo := committeeMemoEntry(fixture.mgr, 5)
	require.NotNil(
		t,
		memo,
		"the new lifecycle's own computation must be memoized",
	)
	require.Same(
		t,
		nextResult.committee,
		memo.committee,
		"the memo must hold the new lifecycle's committee, not the stopped leader's",
	)
}

// Timings taken from a block producer measured on a 1s-slot Leios network:
// the endorser block is in hand a median of one slot after the announcing
// ranking block's slot, but that ranking block only finishes applying a
// median of 32 slots later, because applying it is what waits on fetching
// the endorser block. The vote window is 10 slots wide.
const (
	headerArmingRbSlot        = 577
	headerArmingEbAcquiredAt  = headerArmingRbSlot + 1
	headerArmingRbAppliedAt   = headerArmingRbSlot + 32
	headerArmingVoteWindow    = 10
	headerArmingSeatedVoterId = 3
)

// syncBuffer is a bytes.Buffer safe for the vote manager's event loop
// goroutine to write log records into while the test reads them.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newHeaderArmingFixture builds a fixture whose local pool is seated on the
// committee with a loaded signing key, with the vote window and wall-clock
// slot under the test's control.
func newHeaderArmingFixture(
	t *testing.T,
	slots *fakeSlotProvider,
	extra ...func(*managerFixture, *VoteManagerConfig),
) *managerFixture {
	t.Helper()
	opts := []func(*managerFixture, *VoteManagerConfig){
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.SlotProvider = slots
			cfg.VoteWindowSlots = headerArmingVoteWindow
		},
	}
	opts = append(opts, extra...)
	fixture := newManagerFixture(t, opts...)
	member := fixture.members[headerArmingSeatedVoterId]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[headerArmingSeatedVoterId]
	require.NotNil(t, key)
	require.NoError(t, fixture.mgr.EnableVoting(poolKeyHash, key))
	return fixture
}

// publishHeaderAnnouncement delivers the announcement the way chainsync
// roll-forward does: from the ranking block's header, before the block body
// has been fetched or applied.
func publishHeaderAnnouncement(
	fixture *managerFixture,
	slot uint64,
	rbHash, ebHash lcommon.Blake2b256,
	seq uint64,
) {
	fixture.eventBus.Publish(
		chain.ChainHeaderEventType,
		event.NewEvent(
			chain.ChainHeaderEventType,
			chain.ChainHeaderAnnouncementEvent{
				Slot:   slot,
				RbHash: rbHash,
				EbHash: ebHash,
				Seq:    seq,
			},
		),
	)
}

// TestVoteManagerVotesFromHeaderArrivalBeforeRankingBlockApplies is the
// regression test for a seated committee member that never emitted a vote.
// The announcement was armed only when the announcing ranking block applied,
// which for an EB-announcing block is a median of 32 slots after its own
// slot -- outside the 10-slot vote window it is measured against. The
// announcement is in the header and available from chainsync roll-forward
// long before that.
func TestVoteManagerVotesFromHeaderArrivalBeforeRankingBlockApplies(
	t *testing.T,
) {
	slots := &fakeSlotProvider{slot: headerArmingRbSlot}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))

	// The ranking block's header arrives from chainsync roll-forward at its
	// own slot. Its body has not been fetched, let alone applied.
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 1)

	// The announced endorser block is acquired one slot later.
	slots.setSlot(headerArmingEbAcquiredAt)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)

	emittedEvent := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"vote emitted while the vote window is still open",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash)
	assert.Equal(
		t,
		uint64(headerArmingSeatedVoterId),
		emitted.Vote.VoterId,
	)
	require.NoError(t, VerifyVoteSignature(
		fixture.keys[headerArmingSeatedVoterId].PublicKey(),
		PrototypeVoteMessageBytes(rbHash),
		emitted.Vote.VoteSignature,
	))

	// The announcing ranking block finally applies 32 slots later. By then
	// the vote window is long closed; the vote must already exist.
	slots.setSlot(headerArmingRbAppliedAt)
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"the post-apply backstop must not emit a second vote",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: headerArmingSeatedVoterId,
		}}),
		1,
		"exactly one vote for the announcement",
	)
}

// TestVoteManagerHeaderAndApplyArmingDoNotDoubleVote pins the idempotency of
// arming the same announcement twice. Both observations happen while the vote
// window is open, so only the per-ranking-block dedup can prevent the second
// vote.
func TestVoteManagerHeaderAndApplyArmingDoNotDoubleVote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"vote emitted from the header path",
	)

	// The apply path observes the same announcement, still inside the vote
	// window, and both an EB re-acquisition and a repeated header
	// observation land on top of it.
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 2)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no duplicate vote for an announcement already voted on",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: headerArmingSeatedVoterId,
		}}),
		1,
		"exactly one vote for the announcement",
	)
}

// TestVoteManagerRolledBackHeaderAnnouncementDoesNotVote covers the risk
// header arming introduces: the announcing ranking block is not applied and
// may never be. If it is rolled away before the endorser block is acquired,
// no vote may be emitted for it.
func TestVoteManagerRolledBackHeaderAnnouncementDoesNotVote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))

	// Arm from the header. ObserveAnnouncement is the entrypoint the header
	// event handler calls; using it directly keeps the rollback ordering
	// below deterministic.
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)

	// A peer vote above the rollback point gives the test an observable
	// signal for the rollback having been processed.
	require.NoError(t, fixture.mgr.HandleVote(
		"conn-a",
		fixture.makeVote(t, 1, headerArmingRbSlot, ebHash),
	))
	fixture.eventBus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(
			chain.ChainUpdateEventType,
			chain.ChainRollbackEvent{
				Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
			},
		),
	)
	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo: headerArmingRbSlot, VoterId: 1,
		}})) == 0
	}, testutil.AsyncWait, "rollback pruned state above the rollback point")

	// The endorser block arrives after the rollback. The announcement it
	// would have satisfied is gone, so nothing is emitted.
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no vote for an announcement rolled off our chain",
	)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: headerArmingSeatedVoterId,
		}}),
	)
}

// TestVoteManagerSlotWindowDeclineIsCountedAndWarned covers the second half
// of the reported failure: the decline was logged at Debug and incremented no
// metric, so a permanently non-voting producer looked green on every health
// signal an operator checks.
func TestVoteManagerSlotWindowDeclineIsCountedAndWarned(t *testing.T) {
	reg := prometheus.NewRegistry()
	logBuf := &syncBuffer{}
	slots := &fakeSlotProvider{slot: 1000}
	fixture := newHeaderArmingFixture(
		t,
		slots,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.PromRegistry = reg
			cfg.Logger = slog.New(slog.NewJSONHandler(
				logBuf,
				&slog.HandlerOptions{Level: slog.LevelWarn},
			))
		},
	)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	// An announcement whose ranking block slot is far behind the wall clock,
	// exactly what the apply-driven path used to hand the emitter.
	staleSlot := uint64(1000 - headerArmingVoteWindow - 100)
	fixture.mgr.HandleEndorserBlock(staleSlot, ebHash)
	fixture.mgr.ObserveAnnouncement(staleSlot, rbHash, ebHash)

	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo:  staleSlot,
		VoterId: headerArmingSeatedVoterId,
	}}))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
				voteNotEmittedSlotWindow,
			),
		),
		"slot-window decline is counted",
	)
	assert.Contains(
		t,
		logBuf.String(),
		"outside vote window",
		"a seated node holding a key warns rather than staying silent",
	)
	assert.True(
		t,
		strings.Contains(logBuf.String(), `"level":"WARN"`),
		"the decline is logged at warn level, got: %s",
		logBuf.String(),
	)
}

// TestVoteManagerNonSeatedOutsideWindowUsesNotSeatedReason ensures committee
// membership is classified before the slot-window shortcut. A configured pool
// that is not selected should not be reported as merely late to vote.
//
// This ordering is also what makes the slot-window branch's seating check
// unnecessary: that branch is only reachable once VoterIdFor has already
// succeeded, so the "this node is seated" warning cannot be attached to a
// pool that holds no seat. The log assertion below pins that half.
func TestVoteManagerNonSeatedOutsideWindowUsesNotSeatedReason(t *testing.T) {
	reg := prometheus.NewRegistry()
	logBuf := &syncBuffer{}
	slots := &fakeSlotProvider{slot: 1000}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.SlotProvider = slots
			cfg.VoteWindowSlots = headerArmingVoteWindow
			cfg.PromRegistry = reg
			cfg.Logger = slog.New(slog.NewJSONHandler(
				logBuf,
				&slog.HandlerOptions{Level: slog.LevelWarn},
			))
		},
	)
	var poolHash lcommon.PoolKeyHash
	decoded, err := hex.DecodeString(testPoolHash(99))
	require.NoError(t, err)
	copy(poolHash[:], decoded)
	require.NoError(
		t,
		fixture.mgr.EnableVoting(
			poolHash,
			fixture.keys[headerArmingSeatedVoterId],
		),
	)

	ebHash := lcommon.NewBlake2b256([]byte("unseated-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("unseated-rb"))
	staleSlot := uint64(1000 - headerArmingVoteWindow - 100)
	fixture.mgr.HandleEndorserBlock(staleSlot, ebHash)
	fixture.mgr.ObserveAnnouncement(staleSlot, rbHash, ebHash)

	assert.Equal(t, float64(1), promtestutil.ToFloat64(
		fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
			voteNotEmittedNotSeated,
		),
	))
	assert.Equal(t, float64(0), promtestutil.ToFloat64(
		fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
			voteNotEmittedSlotWindow,
		),
	))
	assert.NotContains(
		t,
		logBuf.String(),
		"seated on the leios committee",
		"an unseated pool must never take the seated-but-not-voting warning",
	)
}

// TestVoteManagerVotesNotEmittedCountsMissingKey pins a second reason label so
// the counter is usable to tell "not configured" apart from "too late".
func TestVoteManagerVotesNotEmittedCountsMissingKey(t *testing.T) {
	reg := prometheus.NewRegistry()
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.SlotProvider = slots
			cfg.VoteWindowSlots = headerArmingVoteWindow
			cfg.PromRegistry = reg
		},
	)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
				voteNotEmittedNoKey,
			),
		),
	)
}

// publishHeaderInvalidation delivers the counterpart signal: queued headers
// above point left the chain without becoming blocks.
func publishHeaderInvalidation(
	fixture *managerFixture,
	slot uint64,
	reason string,
	seq uint64,
) {
	publishHeaderInvalidationNaming(fixture, slot, reason, seq, nil)
}

// publishHeaderInvalidationNaming is publishHeaderInvalidation for a discard
// the point cannot describe, where the chain grew past the dropped headers and
// they are named individually instead.
func publishHeaderInvalidationNaming(
	fixture *managerFixture,
	slot uint64,
	reason string,
	seq uint64,
	rbHashes []lcommon.Blake2b256,
) {
	fixture.eventBus.Publish(
		chain.ChainHeaderEventType,
		event.NewEvent(
			chain.ChainHeaderEventType,
			chain.ChainHeaderInvalidationEvent{
				Point:    ocommon.Point{Slot: slot},
				RbHashes: rbHashes,
				Reason:   reason,
				Seq:      seq,
			},
		),
	)
}

// TestVoteManagerInvalidatedHeaderAnnouncementDoesNotVote is the ordering
// regression test. The chain rolls back and then re-queues the peer's fork
// headers, so an announcement and the invalidation that voids it are produced
// back to back. They ride one event type precisely so the manager cannot
// observe them out of order: here the announcement is armed first and the
// invalidation follows, and the endorser block arriving afterwards must not
// produce a vote for a ranking block that is no longer on our chain.
func TestVoteManagerInvalidatedHeaderAnnouncementDoesNotVote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("orphaned-rb"))

	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		2,
	)
	// Waiting on the invalidation's sequence is sound where waiting on an
	// announcement's is not: handleChainHeaderInvalidation advances the
	// watermark in the same critical section as its pruning, and both
	// handlers run on the one event-loop goroutine, so the announcement
	// ahead of it has already been applied in full.
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 2
	}, testutil.AsyncWait, "invalidation applied")

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no vote for an announcement the chain invalidated",
	)
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}}))
}

// TestVoteManagerLateRollbackDoesNotDropRearmedAnnouncement is the other half
// of the same hazard. chain.update and the header stream are delivered on
// independent channels, so the ChainRollbackEvent for a fork resolution can
// arrive after the header stream has already replayed the winning fork's
// headers. Pruning announcements on that late rollback would delete the
// replacement chain's announcement and put the node back to not voting.
func TestVoteManagerLateRollbackDoesNotDropRearmedAnnouncement(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("replacement-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("replacement-rb"))

	// Chain-mutation order: roll back to S-1, then admit the replacement
	// chain's announcing header at S.
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		7,
	)
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 8)
	waitForAnnouncement(t, fixture, rbHash)

	// The matching rollback finally arrives on chain.update, carrying the
	// sequence number of the mutation the header stream already moved past.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   7,
	})

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	emitted := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"vote for the replacement chain's announcement",
	)
	vote, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, vote.Vote.AnnouncingRbHash)
}

// TestVoteManagerUnsequencedRollbackStillPrunes keeps the pre-existing
// contract for a rollback that did not come from the chain's sequencer: with
// no sequence number there is nothing to supersede it, so it prunes as before.
func TestVoteManagerUnsequencedRollbackStillPrunes(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("orphaned-rb"))
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 4)
	waitForAnnouncement(t, fixture, rbHash)

	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
	})
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"an unsequenced rollback still prunes announcements",
	)
}

// TestVoteManagerHeaderStreamRecoversFromClosedChannel covers the header
// stream closing under the event loop. It is ordering-critical and the only
// thing that arms a vote inside the window, so losing it silently would put
// the node back to never voting. The loop must keep serving chain events and
// re-arm header delivery instead of exiting.
func TestVoteManagerHeaderStreamRecoversFromClosedChannel(t *testing.T) {
	reg := prometheus.NewRegistry()
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(
		t,
		slots,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.PromRegistry = reg
		},
	)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	// Close the manager's header subscription out from under the loop,
	// exactly as a bus-side detach would.
	fixture.mgr.mu.Lock()
	var headerSubId event.EventSubscriberId
	for _, sub := range fixture.mgr.subs {
		if sub.eventType == chain.ChainHeaderEventType {
			headerSubId = sub.id
		}
	}
	fixture.mgr.mu.Unlock()
	require.NotZero(t, headerSubId)
	fixture.eventBus.Unsubscribe(chain.ChainHeaderEventType, headerSubId)

	testutil.WaitForCondition(t, func() bool {
		return promtestutil.ToFloat64(
			fixture.mgr.metrics.headerStreamResubscribeTotal,
		) == 1
	}, testutil.AsyncWait, "header stream resubscribed")

	// The replacement subscription arms announcements again.
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 3)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	emitted := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"vote emitted after the header stream was recovered",
	)
	vote, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, vote.Vote.AnnouncingRbHash)
}

// TestVoteManagerNotEmittedReasonsMaterialized pins that every reason label
// exists from startup, so rate()/increase() have a series to work with on a
// node that has never emitted a vote -- which is the node this counter is for.
func TestVoteManagerNotEmittedReasonsMaterialized(t *testing.T) {
	reg := prometheus.NewRegistry()
	newManagerFixture(t, func(_ *managerFixture, cfg *VoteManagerConfig) {
		cfg.PromRegistry = reg
	})
	families, err := reg.Gather()
	require.NoError(t, err)
	var labels []string
	for _, family := range families {
		if family.GetName() != "dingo_metrics_leios_votes_not_emitted_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "reason" {
					labels = append(labels, pair.GetValue())
				}
			}
		}
	}
	assert.ElementsMatch(t, voteNotEmittedReasons, labels)
}

// waitForAnnouncement blocks until the announcement record itself is in the
// manager's map.
//
// Waiting on lastHeaderStreamSeq is not equivalent and must not be used for
// this: handleChainHeaderAnnouncement advances that watermark in its own
// critical section and only then calls observeAnnouncement, which resolves the
// epoch and re-acquires mu before inserting the record. A test that proceeds
// on the watermark can therefore read the map before the record exists.
func waitForAnnouncement(
	t *testing.T,
	fixture *managerFixture,
	rbHash lcommon.Blake2b256,
) {
	t.Helper()
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		_, ok := fixture.mgr.announcements[rbHash]
		return ok
	}, testutil.AsyncWait, "announcement record present")
}

// armAndVote drives one announcement from header arrival to an emitted local
// vote and returns the emitted event, so the tests below start from a node
// that has genuinely voted rather than from hand-placed state.
func armAndVote(
	t *testing.T,
	fixture *managerFixture,
	emittedCh <-chan event.Event,
	slot uint64,
	rbHash, ebHash lcommon.Blake2b256,
	seq uint64,
) VoteEmittedEvent {
	t.Helper()
	publishHeaderAnnouncement(fixture, slot, rbHash, ebHash, seq)
	waitForAnnouncement(t, fixture, rbHash)
	fixture.mgr.HandleEndorserBlock(slot, ebHash)
	emitted := testutil.RequireReceive(
		t, emittedCh, testutil.AsyncWait, "local vote emitted",
	)
	vote, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, vote.Vote.AnnouncingRbHash)
	return vote
}

// TestVoteManagerLateRollbackKeepsReplacementVoteAndTally is the second half of
// the cross-stream ordering hazard. Protecting only the announcement is not
// enough: the vote, its tally, its dedup record and the acquired endorser
// block are all keyed by slot, and the replacement chain occupies the same
// slot, so a late rollback would erase a vote this node had already emitted
// correctly -- and, because the announcement survives and is marked voted,
// never re-emit it.
func TestVoteManagerLateRollbackKeepsReplacementVoteAndTally(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("replacement-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("replacement-rb"))

	// Chain-mutation order: roll back to S-1 (seq 7), then admit the
	// replacement chain's announcing header at S (seq 8), which this node
	// votes on.
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		7,
	)
	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, rbHash, ebHash, 8,
	)
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The matching rollback finally arrives on chain.update, carrying the
	// sequence number of the mutation the header stream already moved past.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   7,
	})

	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}),
		1,
		"the replacement chain's vote survives a superseded rollback",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, rbHash)
	assert.Contains(t, fixture.mgr.voteRecords, voteId)
	assert.Contains(t, fixture.mgr.acquiredEbs, ebHash)
	var tallied bool
	for key := range fixture.mgr.tallies {
		if key.announcingRbHash == rbHash {
			tallied = true
		}
	}
	assert.True(t, tallied, "the replacement chain's tally survives")
}

// TestVoteManagerLateRollbackStillPrunesAbandonedChainState is the guard's
// other side: state belonging to the chain the rollback abandons must still be
// removed, even while the replacement chain's state at the same slot is
// protected.
func TestVoteManagerLateRollbackStillPrunesAbandonedChainState(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)

	abandonedRb := lcommon.NewBlake2b256([]byte("abandoned-rb"))
	abandonedEb := lcommon.NewBlake2b256([]byte("abandoned-eb"))
	replacementRb := lcommon.NewBlake2b256([]byte("replacement-rb"))
	replacementEb := lcommon.NewBlake2b256([]byte("replacement-eb"))

	// Armed before the rollback (seq 3) and after it (seq 9), both above
	// the rollback point.
	publishHeaderAnnouncement(
		fixture, headerArmingRbSlot, abandonedRb, abandonedEb, 3,
	)
	publishHeaderAnnouncement(
		fixture, headerArmingRbSlot, replacementRb, replacementEb, 9,
	)
	waitForAnnouncement(t, fixture, abandonedRb)
	waitForAnnouncement(t, fixture, replacementRb)

	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   7,
	})

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(
		t,
		fixture.mgr.announcements,
		abandonedRb,
		"an announcement armed before the rollback is still pruned",
	)
	assert.Contains(
		t,
		fixture.mgr.announcements,
		replacementRb,
		"an announcement armed after the rollback is protected",
	)
}

// TestVoteManagerInvalidationDropsDerivedVoteAndAllowsRevote covers a header
// cleared *after* its endorser block arrived and the vote was emitted --
// blockfetch startup failing on an admitted announcing header, for instance.
// Leaving the vote, tally and dedup record behind would keep the (slot, voter)
// vote id occupied, so the replacement chain's vote at the same slot would be
// read as equivocation and dropped.
func TestVoteManagerInvalidationDropsDerivedVoteAndAllowsRevote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	orphanRb := lcommon.NewBlake2b256([]byte("orphan-rb"))
	orphanEb := lcommon.NewBlake2b256([]byte("orphan-eb"))
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, orphanRb, orphanEb, 1,
	)
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The queue holding that header is discarded.
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationQueueCleared,
		2,
	)
	testutil.WaitForCondition(t, func() bool {
		return len(
			fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}),
		) == 0
	}, testutil.AsyncWait, "the vote derived from the cleared header is dropped")

	fixture.mgr.mu.Lock()
	assert.NotContains(t, fixture.mgr.announcements, orphanRb)
	assert.NotContains(t, fixture.mgr.votedAnnouncements, orphanRb)
	assert.NotContains(t, fixture.mgr.voteRecords, voteId)
	assert.NotContains(t, fixture.mgr.acquiredEbs, orphanEb)
	assert.Empty(t, fixture.mgr.tallies)
	fixture.mgr.mu.Unlock()

	// The vote id is free again, so the replacement chain's announcing
	// block at the same slot is voted on rather than being read as
	// equivocation.
	replacementRb := lcommon.NewBlake2b256([]byte("replacement-rb"))
	replacementEb := lcommon.NewBlake2b256([]byte("replacement-eb"))
	revote := armAndVote(
		t,
		fixture,
		emittedCh,
		headerArmingRbSlot,
		replacementRb,
		replacementEb,
		3,
	)
	assert.Equal(t, replacementRb, revote.Vote.AnnouncingRbHash)
	assert.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)
}

// TestVoteManagerInvalidationKeepsUnrelatedAnnouncementState pins that the
// derived-state cleanup is keyed by announcing ranking block, not swept by
// slot: an announcement the invalidation does not cover keeps its own vote,
// tally, dedup record and acquired endorser block.
func TestVoteManagerInvalidationKeepsUnrelatedAnnouncementState(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	// Below the invalidation point: survives.
	keptRb := lcommon.NewBlake2b256([]byte("kept-rb"))
	keptEb := lcommon.NewBlake2b256([]byte("kept-eb"))
	keptSlot := uint64(headerArmingRbSlot - 5)
	armAndVote(t, fixture, emittedCh, keptSlot, keptRb, keptEb, 1)
	keptVoteId := lcommon.LeiosVoteId{
		SlotNo:  keptSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	// Above the invalidation point: dropped, along with everything derived
	// from it.
	orphanRb := lcommon.NewBlake2b256([]byte("orphan-rb"))
	orphanEb := lcommon.NewBlake2b256([]byte("orphan-eb"))
	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, orphanRb, orphanEb, 2,
	)
	orphanVoteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{
		keptVoteId, orphanVoteId,
	}), 2)

	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		3,
	)
	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{orphanVoteId},
		)) == 0
	}, testutil.AsyncWait, "the invalidated announcement's vote is dropped")

	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{keptVoteId}),
		1,
		"an announcement the invalidation does not cover keeps its vote",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, keptRb)
	assert.Contains(t, fixture.mgr.voteRecords, keptVoteId)
	assert.Contains(t, fixture.mgr.acquiredEbs, keptEb)
	assert.NotContains(t, fixture.mgr.announcements, orphanRb)
	assert.NotContains(t, fixture.mgr.voteRecords, orphanVoteId)
	assert.NotContains(t, fixture.mgr.acquiredEbs, orphanEb)
	var keptTally, orphanTally bool
	for key := range fixture.mgr.tallies {
		switch key.announcingRbHash {
		case keptRb:
			keptTally = true
		case orphanRb:
			orphanTally = true
		}
	}
	assert.True(t, keptTally, "the surviving announcement keeps its tally")
	assert.False(t, orphanTally, "the invalidated announcement's tally is gone")
}

// TestVoteManagerRollbackDeliveredBeforeHeaderStreamIsSafe answers the
// remaining two-channel case directly. chain.update and chain.header are still
// separate subscriptions, so the event loop's select can process a rollback
// before header events that the chain produced earlier. That inversion is now
// harmless rather than prevented: the rollback's own invalidation rides the
// header stream behind the announcement it voids, so the authoritative removal
// still happens in chain-mutation order, and the rollback's slot sweep is
// sequence-guarded so it cannot delete anything the header stream armed later.
func TestVoteManagerRollbackDeliveredBeforeHeaderStreamIsSafe(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("orphan-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("orphan-rb"))

	// Chain-mutation order is: announcing header admitted (seq 3), then
	// rolled back (seq 4). The rollback wins the race to the manager and is
	// applied before either header event.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   4,
	})

	// The header stream then delivers both events, still in order.
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 3)
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		4,
	)
	// The invalidation's sequence is the safe watermark to wait on: it is
	// advanced in the same critical section as the pruning, behind the
	// announcement it voids on the one event-loop goroutine.
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 4
	}, testutil.AsyncWait, "header stream drained")

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no vote for a header the rollback removed, whatever order the two streams arrived in",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, rbHash)
}

// TestVoteManagerLocalBlockInvalidationDropsNamedAnnouncement covers the
// discard a locally forged block causes. The chain grows rather than shrinks,
// so the discarded peer header can sit at or below the new tip and the point
// alone cannot name it -- the invalidation names it explicitly. Without this,
// a producer that admits an announcing header and then forges on the same
// parent keeps that announcement armed and votes for a ranking block that is
// not on its chain, leaving the vote id occupied by an abandoned fork.
func TestVoteManagerLocalBlockInvalidationDropsNamedAnnouncement(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}
	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, peerRb, peerEb, 1,
	)
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The locally forged block lands at a HIGHER slot than the discarded
	// peer header, so a point-based rule would keep the announcement.
	publishHeaderInvalidationNaming(
		fixture,
		headerArmingRbSlot+10,
		chain.HeaderInvalidationLocalBlock,
		2,
		[]lcommon.Blake2b256{peerRb},
	)
	testutil.WaitForCondition(t, func() bool {
		return len(
			fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}),
		) == 0
	}, testutil.AsyncWait, "the discarded header's vote is dropped")

	fixture.mgr.mu.Lock()
	assert.NotContains(t, fixture.mgr.announcements, peerRb)
	assert.NotContains(t, fixture.mgr.votedAnnouncements, peerRb)
	assert.NotContains(t, fixture.mgr.voteRecords, voteId)
	assert.NotContains(t, fixture.mgr.acquiredEbs, peerEb)
	assert.Empty(t, fixture.mgr.tallies)
	fixture.mgr.mu.Unlock()

	// The vote id is free, so the block that actually won the slot can be
	// voted on.
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))
	revote := armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, localRb, localEb, 3,
	)
	assert.Equal(t, localRb, revote.Vote.AnnouncingRbHash)
}

// TestVoteManagerLocalBlockInvalidationKeepsUnnamedAnnouncements pins that a
// named-header invalidation drops only what it names. The forged block's own
// announcement, and any other header still on the chain, must survive -- the
// producer must not invalidate its own vote.
func TestVoteManagerLocalBlockInvalidationKeepsUnnamedAnnouncements(
	t *testing.T,
) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	discardedRb := lcommon.NewBlake2b256([]byte("discarded-rb"))
	discardedEb := lcommon.NewBlake2b256([]byte("discarded-eb"))
	keptRb := lcommon.NewBlake2b256([]byte("kept-rb"))
	keptEb := lcommon.NewBlake2b256([]byte("kept-eb"))
	keptSlot := uint64(headerArmingRbSlot - 5)

	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, discardedRb, discardedEb, 1,
	)
	armAndVote(t, fixture, emittedCh, keptSlot, keptRb, keptEb, 2)
	keptVoteId := lcommon.LeiosVoteId{
		SlotNo:  keptSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	// Point is above both announcements, so only the naming saves the one
	// that is still on the chain.
	publishHeaderInvalidationNaming(
		fixture,
		headerArmingRbSlot+10,
		chain.HeaderInvalidationLocalBlock,
		3,
		[]lcommon.Blake2b256{discardedRb},
	)
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		_, still := fixture.mgr.announcements[discardedRb]
		return !still
	}, testutil.AsyncWait, "named announcement dropped")

	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{keptVoteId}),
		1,
		"an announcement the invalidation does not name keeps its vote",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, keptRb)
	assert.Contains(t, fixture.mgr.voteRecords, keptVoteId)
	assert.Contains(t, fixture.mgr.acquiredEbs, keptEb)
}

// TestVoteManagerSameSlotLocalForgeStillVotes is the regression test for the
// competing same-slot forge. The node votes for a peer's announcing header at
// slot S, occupying the (slot, voter) vote id; it then forges its own
// announcing block at the same slot, which discards that peer header.
//
// Arming the forged block's announcement before the invalidation frees the id
// gets its vote rejected as a duplicate, and nothing retries it -- the producer
// silently misses its own vote. The chain therefore announces a forged block on
// the ordered header stream immediately behind the invalidation, so the id is
// always free by the time the announcement is armed. This test delivers the
// two in that order and requires the local vote.
func TestVoteManagerSameSlotLocalForgeStillVotes(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	// The peer's header wins first and takes the vote id.
	peerVote := armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, peerRb, peerEb, 1,
	)
	assert.Equal(t, peerRb, peerVote.Vote.AnnouncingRbHash)
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The local block's endorser block is already in hand when it is forged.
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, localEb)

	// The apply-driven backstop arms the forged block first, while the id is
	// still held by the peer vote. The attempt is rejected, but it must not
	// mark the announcement as voted.
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, localRb, localEb)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"the vote id is still held by the peer vote",
	)

	// Chain-mutation order on the header stream: the invalidation naming the
	// discarded peer header, then the forged block's own announcement.
	publishHeaderInvalidationNaming(
		fixture,
		headerArmingRbSlot,
		chain.HeaderInvalidationLocalBlock,
		2,
		[]lcommon.Blake2b256{peerRb},
	)
	publishHeaderAnnouncement(
		fixture, headerArmingRbSlot, localRb, localEb, 3,
	)

	emitted := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"the forged block's own vote is emitted once the id is freed",
	)
	local, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, localRb, local.Vote.AnnouncingRbHash)

	// Exactly one vote, and it is the local one.
	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"exactly one vote for the slot",
	)
	raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId})
	require.Len(t, raws, 1)
	var stored lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &stored)
	require.NoError(t, err)
	assert.Equal(t, localEb, stored.EndorserBlockHash)

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, peerRb)
	assert.NotContains(t, fixture.mgr.acquiredEbs, peerEb)
	assert.Contains(t, fixture.mgr.announcements, localRb)
	assert.Contains(t, fixture.mgr.votedAnnouncements, localRb)
	for key := range fixture.mgr.tallies {
		assert.NotEqual(
			t,
			peerRb,
			key.announcingRbHash,
			"the discarded peer announcement's tally is gone",
		)
	}
}

// TestVoteManagerRejectedEmissionDoesNotMarkAnnouncementVoted pins the
// invariant the retry above depends on: an emission attempt refused by the
// vote store must leave the announcement eligible, or freeing the vote id
// later would have nothing to retry.
func TestVoteManagerRejectedEmissionDoesNotMarkAnnouncementVoted(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))

	armAndVote(t, fixture, emittedCh, headerArmingRbSlot, peerRb, peerEb, 1)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, localEb)
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, localRb, localEb)

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, localRb)
	assert.NotContains(
		t,
		fixture.mgr.votedAnnouncements,
		localRb,
		"a refused emission must stay retryable",
	)
}

// TestReplacementHeaderSubscriptionIsUndoneWhenStopWins is the regression test
// for the header-stream resubscribe racing Stop.
//
// replaceHeaderStream checks the lifecycle, releases m.mu to subscribe, then
// takes it again to record the subscription. A Stop landing in that gap has
// already snapshotted and unsubscribed m.subs, which cannot contain the id
// being created, so recording it afterwards left a subscriber that no
// goroutine drains -- the event loop is on its way out. The header stream uses
// SubscriberBackpressureBlock precisely so the bus will not drop its events
// under load, so the orphan is not harmless: the next publisher on this event
// type blocks on it rather than having its event dropped, which stalls whoever
// is publishing chain header lifecycle events.
//
// The interleaving is reproduced exactly rather than by racing goroutines: the
// subscription is created, then Stop runs to completion, then the registration
// step is invoked -- which is the ordering the bug requires.
func TestReplacementHeaderSubscriptionIsUndoneWhenStopWins(t *testing.T) {
	fixture := newManagerFixture(t)
	mgr := fixture.mgr
	require.NoError(t, mgr.Start(context.Background()))

	// The replacement subscription, created while the manager was still
	// running, as replaceHeaderStream creates it.
	subId, ch := mgr.subscribeHeaderStream()
	require.NotNil(t, ch)

	// Stop wins the race. It unsubscribes the subscriptions recorded in
	// m.subs, which cannot include the one just created.
	require.NoError(t, mgr.Stop())
	require.True(
		t,
		fixture.eventBus.HasSubscribers(chain.ChainHeaderEventType),
		"fixture must leave the replacement subscription attached, or there is no race to test",
	)

	// The registration step now runs, as it does on the far side of the
	// subscribe call in replaceHeaderStream.
	require.False(
		t,
		mgr.registerReplacementHeaderSubscription(subId),
		"a manager that stopped must not adopt the replacement stream",
	)

	require.False(
		t,
		fixture.eventBus.HasSubscribers(chain.ChainHeaderEventType),
		"the replacement subscription must be undone, not left for a publisher to block on",
	)

	mgr.mu.Lock()
	subs := mgr.subs
	mgr.mu.Unlock()
	require.Empty(
		t,
		subs,
		"a stopped manager must not carry a header subscription",
	)
}

// TestReplaceHeaderStreamAdoptsTheReplacementWhileRunning is the other side of
// the branch: with no Stop in flight the replacement is adopted and recorded,
// so the test above is pinning a distinction rather than a constant.
func TestReplaceHeaderStreamAdoptsTheReplacementWhileRunning(t *testing.T) {
	fixture := newManagerFixture(t)
	mgr := fixture.mgr
	require.NoError(t, mgr.Start(context.Background()))
	t.Cleanup(func() { _ = mgr.Stop() })

	ch, ok := mgr.replaceHeaderStream()
	require.True(t, ok)
	require.NotNil(t, ch)

	mgr.mu.Lock()
	var headerSubs int
	for _, sub := range mgr.subs {
		if sub.eventType == chain.ChainHeaderEventType {
			headerSubs++
		}
	}
	mgr.mu.Unlock()
	require.Equal(
		t,
		1,
		headerSubs,
		"the replacement must take the place of the previous header subscription, not add to it",
	)
	require.True(
		t,
		fixture.eventBus.HasSubscribers(chain.ChainHeaderEventType),
	)
}

const (
	demoSlot    = headerArmingRbSlot
	demoVoterId = headerArmingSeatedVoterId
)

// newOrderingDemo builds one seated committee member with a loaded key, its
// wall clock parked one slot after the announcing block (so the vote window is
// open throughout), and a channel of the votes it emits.
func newOrderingDemo(t *testing.T) (*managerFixture, <-chan any) {
	t.Helper()
	fixture := newHeaderArmingFixture(
		t,
		&fakeSlotProvider{slot: headerArmingEbAcquiredAt},
	)
	subId, ch := fixture.eventBus.Subscribe(VoteEmittedEventType)
	t.Cleanup(func() {
		fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	})
	out := make(chan any, 16)
	go func() {
		for evt := range ch {
			out <- evt.Data
		}
	}()
	return fixture, out
}

func demoVoteId() lcommon.LeiosVoteId {
	return lcommon.LeiosVoteId{SlotNo: demoSlot, VoterId: demoVoterId}
}

// requireVoteFor drains one emitted vote and requires it to name rbHash.
func requireVoteFor(
	t *testing.T,
	votes <-chan any,
	rbHash lcommon.Blake2b256,
	msg string,
) {
	t.Helper()
	data := testutil.RequireReceive(t, votes, testutil.AsyncWait, msg)
	emitted, ok := data.(VoteEmittedEvent)
	require.True(t, ok, "got %T", data)
	assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash, msg)
}

// TestOrderingDemoAnnouncementThenRollback: (a) the ordinary case. The header
// arrives, the chain then rolls it away, and the endorser block turns up
// afterwards. Rule 1 puts the invalidation behind the announcement on the one
// stream, so by the time the endorser block could trigger a vote the
// announcement is gone.
func TestOrderingDemoAnnouncementThenRollback(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	rb := lcommon.NewBlake2b256([]byte("rolled-away-rb"))
	eb := lcommon.NewBlake2b256([]byte("rolled-away-eb"))

	// Chain mutation 1: the announcing header is admitted.
	publishHeaderAnnouncement(fixture, demoSlot, rb, eb, 1)
	// Chain mutation 2: it is rolled away again.
	publishHeaderInvalidation(
		fixture, demoSlot-1, chain.HeaderInvalidationRollback, 2,
	)
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 2
	}, testutil.AsyncWait, "both header events applied, in order")

	// The endorser block finally arrives. There is nothing left to vote for.
	fixture.mgr.HandleEndorserBlock(demoSlot, eb)
	testutil.RequireNoReceive(
		t, votes, 500*time.Millisecond,
		"no vote for a ranking block that left our chain",
	)
	assert.Empty(t, fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{demoVoteId()},
	))
}

// TestOrderingDemoRollbackThenLateHeader: (b) the guard that makes the second
// topic safe. Fork resolution rolls back and then re-queues the winning fork's
// headers, so the replacement chain is armed BEFORE the rollback's own
// chain.update is delivered. Rule 2 stops that late rollback from deleting the
// replacement chain's announcement and the vote already cast for it.
func TestOrderingDemoRollbackThenLateHeader(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	rb := lcommon.NewBlake2b256([]byte("replacement-rb"))
	eb := lcommon.NewBlake2b256([]byte("replacement-eb"))

	// Chain mutation 7: roll back. Chain mutation 8: admit the replacement
	// chain's announcing header. Both on the one ordered stream.
	publishHeaderInvalidation(
		fixture, demoSlot-1, chain.HeaderInvalidationRollback, 7,
	)
	publishHeaderAnnouncement(fixture, demoSlot, rb, eb, 8)
	waitForAnnouncement(t, fixture, rb)

	fixture.mgr.HandleEndorserBlock(demoSlot, eb)
	requireVoteFor(t, votes, rb, "the replacement chain is voted for")

	// Only now does mutation 7's rollback reach the other topic. It must not
	// undo anything mutation 8 established.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: demoSlot - 1},
		Seq:   7,
	})
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{demoVoteId()}),
		1,
		"a superseded rollback keeps the replacement chain's vote",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, rb)
}

// TestOrderingDemoLocalForgeDisplacingQueuedHeader: (c) the case that used to
// lose the producer its own vote. The node votes for a peer's header at slot S,
// then forges its own announcing block at S, which discards that peer header.
// The peer vote holds the (slot, voter) vote id, so the forged block can only
// vote once the invalidation has freed it -- which rule 3 guarantees, by
// putting the forged block's announcement behind the invalidation on the same
// stream instead of on chain.update.
func TestOrderingDemoLocalForgeDisplacingQueuedHeader(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))

	// The peer's header wins first and takes the vote id.
	publishHeaderAnnouncement(fixture, demoSlot, peerRb, peerEb, 1)
	waitForAnnouncement(t, fixture, peerRb)
	fixture.mgr.HandleEndorserBlock(demoSlot, peerEb)
	requireVoteFor(t, votes, peerRb, "the peer header is voted for first")

	// We forge at the same slot with our own endorser block in hand. The
	// apply-driven backstop arms it early, while the id is still taken; the
	// attempt is refused and must stay retryable.
	fixture.mgr.HandleEndorserBlock(demoSlot, localEb)
	fixture.mgr.ObserveAnnouncement(demoSlot, localRb, localEb)
	testutil.RequireNoReceive(
		t, votes, 300*time.Millisecond,
		"the vote id is still held by the peer vote",
	)

	// Chain mutation order: the peer header is discarded, then the forged
	// block announces itself.
	publishHeaderInvalidationNaming(
		fixture,
		demoSlot,
		chain.HeaderInvalidationLocalBlock,
		2,
		[]lcommon.Blake2b256{peerRb},
	)
	publishHeaderAnnouncement(fixture, demoSlot, localRb, localEb, 3)

	requireVoteFor(t, votes, localRb, "the forged block gets its own vote")
	testutil.RequireNoReceive(
		t, votes, 300*time.Millisecond, "exactly one vote for the slot",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{demoVoteId()}),
		1,
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, peerRb)
	assert.Contains(t, fixture.mgr.announcements, localRb)
}

// TestOrderingDemoHeaderQueueDiscardedOnStall: (d) the peer-stall path. A
// header is admitted and voted for, then the connection dies or blockfetch
// times out and the whole header queue is discarded. No block was ever added,
// so no rollback is published -- the invalidation on the header stream is the
// only thing that voids the announcement, and it must take the vote, tally and
// dedup record with it so the slot can be voted on again.
func TestOrderingDemoHeaderQueueDiscardedOnStall(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	stalledRb := lcommon.NewBlake2b256([]byte("stalled-rb"))
	stalledEb := lcommon.NewBlake2b256([]byte("stalled-eb"))

	publishHeaderAnnouncement(fixture, demoSlot, stalledRb, stalledEb, 1)
	waitForAnnouncement(t, fixture, stalledRb)
	fixture.mgr.HandleEndorserBlock(demoSlot, stalledEb)
	requireVoteFor(t, votes, stalledRb, "the admitted header is voted for")

	// The peer stalls; the header queue is discarded back to the block tip.
	publishHeaderInvalidation(
		fixture, demoSlot-1, chain.HeaderInvalidationQueueCleared, 2,
	)
	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{demoVoteId()},
		)) == 0
	}, testutil.AsyncWait, "the discarded header's vote is dropped")

	fixture.mgr.mu.Lock()
	assert.NotContains(t, fixture.mgr.announcements, stalledRb)
	assert.NotContains(t, fixture.mgr.votedAnnouncements, stalledRb)
	assert.NotContains(t, fixture.mgr.voteRecords, demoVoteId())
	fixture.mgr.mu.Unlock()

	// The vote id is free, so whichever header wins the slot next is voted
	// for rather than dropped as a duplicate.
	nextRb := lcommon.NewBlake2b256([]byte("next-rb"))
	nextEb := lcommon.NewBlake2b256([]byte("next-eb"))
	publishHeaderAnnouncement(fixture, demoSlot, nextRb, nextEb, 3)
	waitForAnnouncement(t, fixture, nextRb)
	fixture.mgr.HandleEndorserBlock(demoSlot, nextEb)
	requireVoteFor(t, votes, nextRb, "the slot can be voted on again")
}

// TestVoteManagerValidateVotingKeyErrorsNamePoolInSingleHex pins the pool id in
// the ValidateVotingKey rejections to its 56-character hex form.
//
// lcommon.PoolKeyHash is an alias for Blake2b224, which implements fmt.Stringer
// with a hex String method, and fmt routes the x verb through String for such
// operands. Formatting the value itself with %x therefore hex-encoded its hex
// string, so an operator whose voting key was rejected could not match the
// logged pool id against the id they registered.
func TestVoteManagerValidateVotingKeyErrorsNamePoolInSingleHex(t *testing.T) {
	fixture := newManagerFixture(t)
	member := fixture.members[3]

	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	registeredHex := hex.EncodeToString(member.PoolKeyHash)
	require.Len(t, registeredHex, 2*lcommon.Blake2b224Size)

	wrongKey, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 999))
	require.NoError(t, err)

	// Key mismatch for a pool that does resolve.
	err = fixture.mgr.ValidateVotingKey(poolKeyHash, wrongKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "public key for pool "+registeredHex)
	require.NotContains(
		t,
		err.Error(),
		hex.EncodeToString([]byte(registeredHex)),
		"pool id must not be hex-encoded twice",
	)

	// Pool with no resolvable key at all.
	var missingPool lcommon.PoolKeyHash
	missingPool[0] = 0xff
	missingHex := hex.EncodeToString(missingPool[:])

	err = fixture.mgr.ValidateVotingKey(missingPool, wrongKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "for pool "+missingHex)
	require.NotContains(
		t,
		err.Error(),
		hex.EncodeToString([]byte(missingHex)),
		"pool id must not be hex-encoded twice",
	)
}

const testSlotsPerEpoch = 100

type fakeStakeProvider struct {
	mu    sync.Mutex
	pools map[string]uint64
	total uint64
	err   error
	calls int
}

func (f *fakeStakeProvider) GetStakeDistribution(
	epoch uint64,
) (map[string]uint64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, 0, f.err
	}
	return maps.Clone(f.pools), f.total, nil
}

func (f *fakeStakeProvider) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeStakeProvider) setError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

type fakeLeiosKeyProvider struct {
	mu            sync.Mutex
	keys          map[string]*lcommon.LeiosKey
	err           error
	failOnCall    int
	failErr       error
	calls         int
	snapshotEpoch uint64
}

type blockingInitialLeiosKeyProvider struct {
	blockedSnapshot uint64
	blockedKeys     map[string]*lcommon.LeiosKey
	blockedErr      error
	currentKeys     map[string]*lcommon.LeiosKey
	currentErr      error
	currentFailCall int
	currentCalls    int
	blockCurrent    bool
	entered         chan struct{}
	release         chan struct{}
	currentEntered  chan struct{}
	currentRelease  chan struct{}
	enteredOnce     sync.Once
	releaseOnce     sync.Once
	currentOnce     sync.Once
	currentRelOnce  sync.Once
	mu              sync.Mutex
}

type blockingFirstLeiosKeyProvider struct {
	keys        map[string]*lcommon.LeiosKey
	err         error
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
	mu          sync.Mutex
	calls       int
}

func newBlockingFirstLeiosKeyProvider() *blockingFirstLeiosKeyProvider {
	return &blockingFirstLeiosKeyProvider{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *blockingFirstLeiosKeyProvider) GetLeiosKeys(
	uint64,
	[]string,
) (map[string]*lcommon.LeiosKey, error) {
	f.mu.Lock()
	f.calls++
	first := f.calls == 1
	keys := maps.Clone(f.keys)
	err := f.err
	f.mu.Unlock()
	if first {
		f.enteredOnce.Do(func() { close(f.entered) })
		<-f.release
	}
	return keys, err
}

func (f *blockingFirstLeiosKeyProvider) releaseFirstLookup() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func newBlockingInitialLeiosKeyProvider(
	blockedSnapshot uint64,
) *blockingInitialLeiosKeyProvider {
	return &blockingInitialLeiosKeyProvider{
		blockedSnapshot: blockedSnapshot,
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
		currentEntered:  make(chan struct{}),
		currentRelease:  make(chan struct{}),
	}
}

func (f *blockingInitialLeiosKeyProvider) GetLeiosKeys(
	snapshotEpoch uint64,
	_ []string,
) (map[string]*lcommon.LeiosKey, error) {
	if snapshotEpoch == f.blockedSnapshot {
		f.enteredOnce.Do(func() { close(f.entered) })
		<-f.release
		return maps.Clone(f.blockedKeys), f.blockedErr
	}
	if f.blockCurrent {
		f.currentOnce.Do(func() { close(f.currentEntered) })
		<-f.currentRelease
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentCalls++
	if f.currentFailCall == f.currentCalls {
		return nil, errors.New("current snapshot temporarily unavailable")
	}
	return maps.Clone(f.currentKeys), f.currentErr
}

func (f *blockingInitialLeiosKeyProvider) releaseInitialLookup() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func (f *blockingInitialLeiosKeyProvider) releaseCurrentLookup() {
	f.currentRelOnce.Do(func() { close(f.currentRelease) })
}

func (f *fakeLeiosKeyProvider) GetLeiosKeys(
	snapshotEpoch uint64,
	_ []string,
) (map[string]*lcommon.LeiosKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.snapshotEpoch = snapshotEpoch
	if f.calls == f.failOnCall {
		return nil, f.failErr
	}
	if f.err != nil {
		return nil, f.err
	}
	return maps.Clone(f.keys), nil
}

type fakeEpochProvider struct {
	currentEpoch uint64
}

func (f *fakeEpochProvider) CurrentEpoch() uint64 {
	return f.currentEpoch
}

func (f *fakeEpochProvider) EpochForSlot(slot uint64) (uint64, error) {
	return slot / testSlotsPerEpoch, nil
}

type fakeSlotProvider struct {
	mu   sync.Mutex
	slot uint64
}

func (f *fakeSlotProvider) CurrentOrTipSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slot
}

// setSlot advances (or rewinds) the wall-clock slot the vote window is
// measured against.
func (f *fakeSlotProvider) setSlot(slot uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slot = slot
}

type fakeParamsProvider struct {
	mu            sync.Mutex
	committeeSize uint16
	tau           *big.Rat
	err           error
}

type blockingParamsProvider struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingParamsProvider() *blockingParamsProvider {
	return &blockingParamsProvider{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *blockingParamsProvider) LeiosCommitteeParameters(uint64) (
	uint16,
	*big.Rat,
	error,
) {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return 10, big.NewRat(7, 10), nil
}

func (f *fakeParamsProvider) LeiosCommitteeParameters(uint64) (
	uint16,
	*big.Rat,
	error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, nil, f.err
	}
	return f.committeeSize, f.tau, nil
}

// managerFixture wires a VoteManager against fake providers. The default
// committee has 10 members with stakes 100,90,...,10 (total active stake
// 550), tau = 7/10 (385 stake required for quorum), current
// epoch 5, and a registry covering every member.
type managerFixture struct {
	mgr             *VoteManager
	eventBus        *event.EventBus
	stake           *fakeStakeProvider
	params          *fakeParamsProvider
	epochs          *fakeEpochProvider
	keys            map[uint64]*VoteSigningKey
	members         []CommitteeMember
	registryEntries map[string]string
}

func newManagerFixture(
	t *testing.T,
	opts ...func(*managerFixture, *VoteManagerConfig),
) *managerFixture {
	t.Helper()
	poolStakes := make(map[string]uint64)
	var total uint64
	for i := range byte(10) {
		stake := uint64(10-i) * 10
		poolStakes[testPoolHash(i+1)] = stake
		total += stake
	}
	expected, err := ComputeCommittee(
		5, 3, poolStakes, total, 10,
	)
	require.NoError(t, err)

	keys := make(map[uint64]*VoteSigningKey)
	registryEntries := make(map[string]string)
	for _, member := range expected.Members {
		key, err := ParseVoteSigningKey(
			fmt.Sprintf("%064x", member.VoterId+1),
		)
		require.NoError(t, err)
		keys[member.VoterId] = key
		registryEntries[hex.EncodeToString(member.PoolKeyHash)] =
			hex.EncodeToString(key.PublicKeyBytes())
	}
	registry, err := NewVoterRegistry(registryEntries)
	require.NoError(t, err)

	fixture := &managerFixture{
		eventBus: event.NewEventBus(nil, nil),
		stake: &fakeStakeProvider{
			pools: poolStakes,
			total: total,
		},
		params: &fakeParamsProvider{
			committeeSize: 10,
			tau:           big.NewRat(7, 10),
		},
		epochs:          &fakeEpochProvider{currentEpoch: 5},
		keys:            keys,
		members:         expected.Members,
		registryEntries: registryEntries,
	}
	cfg := VoteManagerConfig{
		EventBus:       fixture.eventBus,
		StakeProvider:  fixture.stake,
		EpochProvider:  fixture.epochs,
		ParamsProvider: fixture.params,
		Registry:       registry,
	}
	for _, opt := range opts {
		opt(fixture, &cfg)
	}
	mgr, err := NewVoteManager(cfg)
	require.NoError(t, err)
	fixture.mgr = mgr
	require.NoError(t, mgr.Start(context.Background()))
	t.Cleanup(func() {
		_ = mgr.Stop()
	})
	return fixture
}

func (f *managerFixture) makeVote(
	t *testing.T,
	voterId uint64,
	slot uint64,
	ebHash lcommon.Blake2b256,
) lcommon.LeiosVote {
	t.Helper()
	key, ok := f.keys[voterId]
	require.True(t, ok, "no key for voter %d", voterId)
	sig, err := SignVote(key, VoteMessageBytes(slot, ebHash))
	require.NoError(t, err)
	return lcommon.LeiosVote{
		SlotNo:            slot,
		EndorserBlockHash: ebHash,
		VoterId:           voterId,
		VoteSignature:     sig,
	}
}

func (f *managerFixture) makePrototypeVote(
	t *testing.T,
	voterId uint64,
	rbHash lcommon.Blake2b256,
) lcommon.LeiosPrototypeVote {
	t.Helper()
	key, ok := f.keys[voterId]
	require.True(t, ok, "no key for voter %d", voterId)
	sig, err := SignVote(key, PrototypeVoteMessageBytes(rbHash))
	require.NoError(t, err)
	return lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: rbHash,
		VoterId:          voterId,
		VoteSignature:    sig,
	}
}

func TestVoteManagerValidatesDijkstraCertificateStrictly(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t, func(f *managerFixture, cfg *VoteManagerConfig) {
		keys := make(map[string]*lcommon.LeiosKey, len(f.members))
		for _, member := range f.members {
			key := f.keys[member.VoterId]
			proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
			require.NoError(t, err)
			keys[hex.EncodeToString(member.PoolKeyHash)] = &lcommon.LeiosKey{
				PublicKey:       key.PublicKeyBytes(),
				PossessionProof: proof,
			}
		}
		cfg.KeyProvider = &fakeLeiosKeyProvider{keys: keys}
	})

	message := []byte("certificate message")
	signers := make([]byte, lcommon.LeiosSignerBitfieldSize(10))
	signatures := make([][]byte, 0, 10)
	for voterID := range uint64(10) {
		key := fixture.keys[voterID]
		signature, err := SignVote(key, message)
		require.NoError(t, err)
		signatures = append(signatures, signature)
		signers[voterID/8] |= 1 << (7 - voterID%8)
	}
	aggregatedSignature, err := AggregateSignatures(signatures)
	require.NoError(t, err)
	require.NoError(t, fixture.mgr.ValidateDijkstraCertificate(
		5,
		signers,
		aggregatedSignature,
		message,
	))

	require.Error(t, fixture.mgr.ValidateDijkstraCertificate(
		5,
		signers,
		aggregatedSignature,
		[]byte("wrong message"),
	))
	require.Error(t, fixture.mgr.ValidateDijkstraCertificate(
		5,
		signers[:1],
		aggregatedSignature,
		message,
	))

	wrongSizeAggregate := make([]byte, lcommon.LeiosBlsSignatureSize)
	require.Error(t, fixture.mgr.ValidateDijkstraCertificate(
		5,
		signers,
		wrongSizeAggregate,
		message,
	))

	highBits := append([]byte(nil), signers...)
	highBits[len(highBits)-1] |= 1
	require.Error(t, fixture.mgr.ValidateDijkstraCertificate(
		5,
		highBits,
		aggregatedSignature,
		message,
	))

	belowQuorum := make([]byte, lcommon.LeiosSignerBitfieldSize(10))
	belowQuorum[1] = 1 << 6 // only voter 9, with 10 of 550 active stake
	belowQuorumSig, err := SignVote(fixture.keys[9], message)
	require.NoError(t, err)
	belowQuorumAggregate, err := AggregateSignatures([][]byte{belowQuorumSig})
	require.NoError(t, err)
	require.ErrorIs(t, fixture.mgr.ValidateDijkstraCertificate(
		5,
		belowQuorum,
		belowQuorumAggregate,
		message,
	), ErrQuorumNotMet)
}

func TestVoteManagerRejectsKeylessDijkstraCertificateSigner(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t, func(f *managerFixture, cfg *VoteManagerConfig) {
		keys := make(map[string]*lcommon.LeiosKey, len(f.members)-1)
		for _, member := range f.members[1:] {
			key := f.keys[member.VoterId]
			proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
			require.NoError(t, err)
			keys[hex.EncodeToString(member.PoolKeyHash)] = &lcommon.LeiosKey{
				PublicKey:       key.PublicKeyBytes(),
				PossessionProof: proof,
			}
		}
		cfg.KeyProvider = &fakeLeiosKeyProvider{keys: keys}
	})

	message := []byte("certificate message")
	signers := make([]byte, lcommon.LeiosSignerBitfieldSize(10))
	signatures := make([][]byte, 0, 10)
	for voterID := range uint64(10) {
		key := fixture.keys[voterID]
		signature, err := SignVote(key, message)
		require.NoError(t, err)
		signatures = append(signatures, signature)
		signers[voterID/8] |= 1 << (7 - voterID%8)
	}
	aggregatedSignature, err := AggregateSignatures(signatures)
	require.NoError(t, err)
	require.ErrorContains(t, fixture.mgr.ValidateDijkstraCertificate(
		5,
		signers,
		aggregatedSignature,
		message,
	), "no usable key")
}

type nextVotesResult struct {
	votes []lcommon.LeiosVote
	err   error
}

func startNextVotes(
	f *managerFixture,
	done <-chan struct{},
	connKey string,
	count uint64,
) <-chan nextVotesResult {
	ch := make(chan nextVotesResult, 1)
	go func() {
		votes, err := f.mgr.NextVotes(done, connKey, count)
		ch <- nextVotesResult{votes: votes, err: err}
	}()
	return ch
}

func TestNewVoteManagerValidatesConfig(t *testing.T) {
	t.Parallel()

	registry, err := NewVoterRegistry(nil)
	require.NoError(t, err)
	valid := VoteManagerConfig{
		EventBus:       event.NewEventBus(nil, nil),
		StakeProvider:  &fakeStakeProvider{},
		EpochProvider:  &fakeEpochProvider{},
		ParamsProvider: &fakeParamsProvider{},
		Registry:       registry,
	}
	for _, tc := range []struct {
		name   string
		mutate func(*VoteManagerConfig)
	}{
		{"nil event bus", func(c *VoteManagerConfig) { c.EventBus = nil }},
		{"nil stake provider", func(c *VoteManagerConfig) { c.StakeProvider = nil }},
		{"nil epoch provider", func(c *VoteManagerConfig) { c.EpochProvider = nil }},
		{"nil params provider", func(c *VoteManagerConfig) { c.ParamsProvider = nil }},
	} {
		cfg := valid
		tc.mutate(&cfg)
		_, err := NewVoteManager(cfg)
		assert.Error(t, err, tc.name)
	}
	mgr, err := NewVoteManager(valid)
	require.NoError(t, err)
	require.NotNil(t, mgr)
}

func TestVoteManagerHandleVoteAndServe(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	vote := fixture.makeVote(t, 0, 577, ebHash)
	require.NoError(t, fixture.mgr.HandleVote("conn-a", vote))

	done := make(chan struct{})
	defer close(done)
	result := testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		testutil.AsyncWait,
		"vote served to other connection",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)
	assert.Equal(t, vote.SlotNo, result.votes[0].SlotNo)
	assert.Equal(t, vote.VoterId, result.votes[0].VoterId)
	assert.Equal(
		t,
		vote.EndorserBlockHash,
		result.votes[0].EndorserBlockHash,
	)
}

func TestVoteManagerSkipsRepeatedVoteSignatureVerification(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	vote := fixture.makeVote(
		t,
		0,
		577,
		lcommon.NewBlake2b256([]byte("duplicate-vote")),
	)
	var verifyCalls atomic.Int64
	fixture.mgr.verifyVoteSignature = func(
		publicKey *bls12381.G2Affine,
		message []byte,
		signature []byte,
	) error {
		verifyCalls.Add(1)
		return VerifyVoteSignature(publicKey, message, signature)
	}

	require.NoError(t, fixture.mgr.HandleVote("conn-a", vote))
	require.NoError(t, fixture.mgr.HandleVote("conn-b", vote))
	assert.EqualValues(t, 1, verifyCalls.Load())
}

func TestVoteManagerCoalescesConcurrentVoteVerification(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	vote := fixture.makeVote(
		t,
		0,
		577,
		lcommon.NewBlake2b256([]byte("in-flight-duplicate")),
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	var verifyCalls atomic.Int64
	fixture.mgr.verifyVoteSignature = func(
		publicKey *bls12381.G2Affine,
		message []byte,
		signature []byte,
	) error {
		verifyCalls.Add(1)
		close(entered)
		<-release
		return VerifyVoteSignature(publicKey, message, signature)
	}
	done := make(chan error, 1)
	go func() { done <- fixture.mgr.HandleVote("conn-a", vote) }()
	<-entered
	require.NoError(t, fixture.mgr.HandleVote("conn-b", vote))
	assert.EqualValues(t, 1, verifyCalls.Load())
	close(release)
	require.NoError(t, <-done)
}

func TestVoteManagerInvalidSignatureDoesNotReserveVoteIdentity(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	valid := fixture.makeVote(
		t,
		0,
		577,
		lcommon.NewBlake2b256([]byte("invalid-then-valid")),
	)
	invalid := valid
	invalid.VoteSignature = append([]byte(nil), valid.VoteSignature...)
	invalid.VoteSignature[0] ^= 1
	var verifyCalls atomic.Int64
	fixture.mgr.verifyVoteSignature = func(
		publicKey *bls12381.G2Affine,
		message []byte,
		signature []byte,
	) error {
		verifyCalls.Add(1)
		return VerifyVoteSignature(publicKey, message, signature)
	}

	require.NoError(t, fixture.mgr.HandleVote("conn-a", invalid))
	require.NoError(t, fixture.mgr.HandleVote("conn-b", valid))
	assert.EqualValues(t, 2, verifyCalls.Load())
}

func TestVoteManagerBoundsSignatureVerificationPerPeer(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	for idx := range voteVerificationMaxPerPeer {
		vote := lcommon.LeiosVote{
			SlotNo: uint64(idx), VoterId: uint64(idx),
			EndorserBlockHash: lcommon.NewBlake2b256(fmt.Appendf(nil, "vote-%d", idx)),
		}
		reserved, err := fixture.mgr.reserveIncomingVoteVerification("conn-a", vote)
		require.NoError(t, err)
		require.True(t, reserved)
		fixture.mgr.releaseIncomingVoteVerification(vote)
	}
	reserved, err := fixture.mgr.reserveIncomingVoteVerification(
		"conn-a",
		lcommon.LeiosVote{SlotNo: 1000, VoterId: 1000},
	)
	require.ErrorContains(t, err, "peer vote verification budget exhausted")
	require.False(t, reserved)
	reserved, err = fixture.mgr.reserveIncomingVoteVerification(
		"conn-b",
		lcommon.LeiosVote{SlotNo: 1001, VoterId: 1001},
	)
	require.NoError(t, err)
	require.True(t, reserved, "one peer's budget does not consume another peer's quota")
}

func TestVoteManagerDoesNotPenalizePeerForSharedVerificationBudget(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)
	for idx := range voteVerificationMaxProcess {
		peer := fmt.Sprintf("conn-%d", idx/voteVerificationMaxPerPeer)
		vote := lcommon.LeiosVote{
			SlotNo: uint64(idx + 1), VoterId: uint64(idx + 1),
			EndorserBlockHash: lcommon.NewBlake2b256(
				fmt.Appendf(nil, "process-budget-%d", idx),
			),
		}
		reserved, err := fixture.mgr.reserveIncomingVoteVerification(peer, vote)
		require.NoError(t, err)
		require.True(t, reserved)
		fixture.mgr.releaseIncomingVoteVerification(vote)
	}

	blockedVote := lcommon.LeiosVote{SlotNo: voteVerificationMaxProcess + 1}
	reserved, err := fixture.mgr.reserveIncomingVoteVerification(
		"honest-peer",
		blockedVote,
	)
	require.False(t, reserved)
	require.ErrorIs(t, err, errVoteVerificationProcessBudget)
	require.NoError(t, fixture.mgr.rejectIncomingVote(
		"honest-peer",
		"admission",
		blockedVote,
		err,
	))

	invalid := errors.New("invalid vote")
	for range voteInvalidPeerLimit - 1 {
		require.NoError(t, fixture.mgr.rejectIncomingVote(
			"honest-peer", "structural", lcommon.LeiosVote{}, invalid,
		))
	}
	require.ErrorIs(t, fixture.mgr.rejectIncomingVote(
		"honest-peer", "structural", lcommon.LeiosVote{}, invalid,
	), ErrPeerMisbehavior)
}

func TestVoteManagerBoundsPrototypeSignatureVerificationPerPeer(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("prototype-budget-eb"))
	var verifyCalls atomic.Int64
	fixture.mgr.verifyVoteSignature = func(
		_ *bls12381.G2Affine,
		_, _ []byte,
	) error {
		verifyCalls.Add(1)
		return nil
	}

	for idx := range voteVerificationMaxPerPeer {
		slot := uint64(500 + idx)
		rbHash := lcommon.NewBlake2b256(fmt.Appendf(nil, "prototype-rb-%d", idx))
		fixture.mgr.ObserveAnnouncement(slot, rbHash, ebHash)
		require.NoError(t, fixture.mgr.HandlePrototypeVote(
			"conn-a",
			fixture.makePrototypeVote(t, 3, rbHash),
		))
	}

	lastSlot := uint64(500 + voteVerificationMaxPerPeer)
	for idx := range voteInvalidPeerLimit + 1 {
		rbHash := lcommon.NewBlake2b256(
			fmt.Appendf(nil, "prototype-rb-over-budget-%d", idx),
		)
		fixture.mgr.ObserveAnnouncement(lastSlot+uint64(idx), rbHash, ebHash)
		require.NoError(t, fixture.mgr.HandlePrototypeVote(
			"conn-a",
			fixture.makePrototypeVote(t, 3, rbHash),
		))
	}
	assert.EqualValues(t, voteVerificationMaxPerPeer, verifyCalls.Load())

	otherSlot := lastSlot + 1
	otherRbHash := lcommon.NewBlake2b256([]byte("prototype-rb-other-peer"))
	fixture.mgr.ObserveAnnouncement(otherSlot, otherRbHash, ebHash)
	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"conn-b",
		fixture.makePrototypeVote(t, 3, otherRbHash),
	))
	assert.EqualValues(
		t,
		voteVerificationMaxPerPeer+1,
		verifyCalls.Load(),
		"one peer's budget must not prevent another peer's signature verification",
	)
}

func TestVoteManagerDisconnectsPeerAfterRepeatedInvalidVotes(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)
	invalid := lcommon.LeiosVote{}
	for range voteInvalidPeerLimit - 1 {
		require.NoError(t, fixture.mgr.HandleVote("conn-a", invalid))
	}
	err := fixture.mgr.HandleVote("conn-a", invalid)
	require.ErrorIs(t, err, ErrPeerMisbehavior)
	require.NoError(t, fixture.mgr.HandleVote("conn-b", invalid),
		"one connection's invalid-message count must not penalize another")
}

func TestVoteManagerDoesNotEchoToOrigin(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)

	done := make(chan struct{})
	resultCh := startNextVotes(fixture, done, "conn-a", 1)
	testutil.RequireNoReceive(
		t,
		resultCh,
		300*time.Millisecond,
		"own vote must not be echoed back",
	)
	close(done)
	result := testutil.RequireReceive(
		t,
		resultCh,
		testutil.AsyncWait,
		"aborted NextVotes returns",
	)
	assert.Error(t, result.err)
}

func TestVoteManagerNextVotesCursorAdvances(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)

	done := make(chan struct{})
	result := testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		testutil.AsyncWait,
		"first serve",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)

	// The cursor advanced: the same vote is not served again
	secondCh := startNextVotes(fixture, done, "conn-b", 1)
	testutil.RequireNoReceive(
		t,
		secondCh,
		300*time.Millisecond,
		"vote must be served at most once per connection",
	)
	close(done)
	result = testutil.RequireReceive(
		t,
		secondCh,
		testutil.AsyncWait,
		"aborted NextVotes returns",
	)
	assert.Error(t, result.err)
}

func TestVoteManagerRemoveConnectionResetsCursor(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)

	done := make(chan struct{})
	defer close(done)
	result := testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		testutil.AsyncWait,
		"first serve",
	)
	require.NoError(t, result.err)

	// A reconnecting peer starts from the beginning of the retained log
	fixture.mgr.RemoveConnection("conn-b")
	result = testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		testutil.AsyncWait,
		"serve again after cursor reset",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)
}

func TestVoteManagerNextVotesAccumulatesAcrossInserts(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	done := make(chan struct{})
	defer close(done)
	resultCh := startNextVotes(fixture, done, "conn-b", 2)

	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)
	testutil.RequireNoReceive(
		t,
		resultCh,
		300*time.Millisecond,
		"NextVotes must wait for the full count",
	)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 577, ebHash),
		),
	)
	result := testutil.RequireReceive(
		t,
		resultCh,
		testutil.AsyncWait,
		"NextVotes returns once count votes are available",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 2)
	assert.Equal(t, uint64(0), result.votes[0].VoterId)
	assert.Equal(t, uint64(1), result.votes[1].VoterId)
}

func TestVoteManagerStopUnblocksNextVotes(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	done := make(chan struct{})
	defer close(done)
	resultCh := startNextVotes(fixture, done, "conn-b", 1)
	testutil.RequireNoReceive(
		t,
		resultCh,
		200*time.Millisecond,
		"NextVotes waits while no votes stored",
	)
	require.NoError(t, fixture.mgr.Stop())
	result := testutil.RequireReceive(
		t,
		resultCh,
		testutil.AsyncWait,
		"Stop unblocks NextVotes",
	)
	assert.ErrorIs(t, result.err, ErrVoteManagerStopped)
}

func TestVoteManagerDedupIgnoresResubmission(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	vote := fixture.makeVote(t, 0, 577, ebHash)
	require.NoError(t, fixture.mgr.HandleVote("conn-a", vote))
	require.NoError(t, fixture.mgr.HandleVote("conn-c", vote))

	done := make(chan struct{})
	result := testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		testutil.AsyncWait,
		"vote served once",
	)
	require.NoError(t, result.err)
	secondCh := startNextVotes(fixture, done, "conn-b", 1)
	testutil.RequireNoReceive(
		t,
		secondCh,
		300*time.Millisecond,
		"duplicate vote must not be stored twice",
	)
	close(done)
	testutil.RequireReceive(
		t, secondCh, testutil.AsyncWait, "aborted NextVotes returns",
	)
}

func TestVoteManagerEquivocationFirstWins(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHashA := lcommon.NewBlake2b256([]byte("eb-a"))
	ebHashB := lcommon.NewBlake2b256([]byte("eb-b"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHashA),
		),
	)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHashB),
		),
	)

	raws := fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
	)
	require.Len(t, raws, 1)
	var stored lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &stored)
	require.NoError(t, err)
	assert.Equal(
		t, ebHashA, stored.EndorserBlockHash,
		"first vote wins on equivocation",
	)
}

func TestVoteManagerEquivocationDoesNotPenalizeRelayingPeer(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)
	hashA := lcommon.NewBlake2b256([]byte("eb-a"))
	hashB := lcommon.NewBlake2b256([]byte("eb-b"))
	for slot := uint64(600); slot < 600+voteInvalidPeerLimit; slot++ {
		first := fixture.makeVote(t, 0, slot, hashA)
		conflict := fixture.makeVote(t, 0, slot, hashB)
		require.NoError(t, fixture.mgr.HandleVote("first-peer", first))
		require.NoError(t, fixture.mgr.HandleVote("relaying-peer", conflict))
	}
}

func TestVoteManagerRejectsInvalidVotes(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))

	// Voter id out of committee range
	outOfRange := fixture.makeVote(t, 0, 577, ebHash)
	outOfRange.VoterId = 10
	require.NoError(t, fixture.mgr.HandleVote("conn-a", outOfRange))

	// Structurally invalid signature size
	badSize := fixture.makeVote(t, 1, 577, ebHash)
	badSize.VoteSignature = []byte{1, 2, 3}
	require.NoError(t, fixture.mgr.HandleVote("conn-a", badSize))

	// Signature by the wrong key (registry knows the right one)
	wrongKey := fixture.makeVote(t, 2, 577, ebHash)
	wrongKey.VoterId = 3
	require.NoError(t, fixture.mgr.HandleVote("conn-a", wrongKey))

	raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{
		{SlotNo: 577, VoterId: 10},
		{SlotNo: 577, VoterId: 1},
		{SlotNo: 577, VoterId: 3},
	})
	assert.Empty(t, raws, "invalid votes must not be stored")
}

func TestVoteManagerLenientUnknownPubkey(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			registry, err := NewVoterRegistry(nil)
			require.NoError(t, err)
			cfg.Registry = registry
		},
	)
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)

	// Without registered keys the votes pass membership checks and are
	// stored, but cannot contribute verified stake.
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	for voterId := range uint64(10) {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	raws := fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 9}},
	)
	assert.Len(t, raws, 1, "unverified votes are stored leniently")
	testutil.RequireNoReceive(
		t,
		quorumCh,
		300*time.Millisecond,
		"unverified votes alone must not certify",
	)
}

func TestVoteManagerQuorumBuildsCertificate(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// Voters 0..4 hold 100+90+80+70+60 = 400 >= 385 (tau = 7/10 of 550)
	for voterId := range uint64(5) {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	evt := testutil.RequireReceive(
		t,
		quorumCh,
		testutil.AsyncWait,
		"quorum event published",
	)
	quorum, ok := evt.Data.(EbQuorumEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(577), quorum.SlotNo)
	assert.Equal(t, ebHash, quorum.EndorserBlockHash)
	assert.Equal(t, uint64(5), quorum.Epoch)
	assert.Equal(t, uint64(400), quorum.VerifiedStake)
	assert.Equal(t, uint64(400), quorum.ObservedStake)
	assert.Equal(t, uint64(550), quorum.TotalActiveStake)
	require.NotNil(t, quorum.Certificate)

	// The certificate must self-validate against the committee
	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	registry, err := NewVoterRegistry(fixture.registryEntries)
	require.NoError(t, err)
	sigChecked, err := ValidateEbCertificate(
		quorum.Certificate, committee, big.NewRat(7, 10), registry,
	)
	require.NoError(t, err)
	assert.True(t, sigChecked)

	// More votes after certification must not publish again
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 5, 577, ebHash),
		),
	)
	testutil.RequireNoReceive(
		t,
		quorumCh,
		300*time.Millisecond,
		"certificate is built once per endorser block",
	)
}

func TestVoteManagerQuorumRequiresVerifiedStake(t *testing.T) {
	t.Parallel()

	// Registry missing voter 0's key: their stake (100) is observed but
	// not verified.
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			partial := maps.Clone(f.registryEntries)
			for _, member := range f.members {
				if member.VoterId == 0 {
					delete(
						partial,
						hex.EncodeToString(member.PoolKeyHash),
					)
				}
			}
			registry, err := NewVoterRegistry(partial)
			require.NoError(t, err)
			cfg.Registry = registry
		},
	)
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// Observed 400 >= 385 but verified only 300: no certificate
	for voterId := range uint64(5) {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	testutil.RequireNoReceive(
		t,
		quorumCh,
		300*time.Millisecond,
		"observed-but-unverified stake must not certify",
	)

	// Verified 300+50+40 = 390 >= 385: certificate now builds
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 5, 577, ebHash),
		),
	)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 6, 577, ebHash),
		),
	)
	evt := testutil.RequireReceive(
		t,
		quorumCh,
		testutil.AsyncWait,
		"quorum event after verified stake crosses tau",
	)
	quorum, ok := evt.Data.(EbQuorumEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(390), quorum.VerifiedStake)
	assert.Equal(t, uint64(490), quorum.ObservedStake)
	// Voter 0's unverified vote must not be in the signers bitfield
	assert.False(t, quorum.Certificate.Signer(0))
	assert.True(t, quorum.Certificate.Signer(1))
}

func TestVoteManagerOwnVoteEmission(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	member := fixture.members[3]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[3]
	require.NotNil(t, key)
	fixture.mgr.EnableVoting(poolKeyHash, key)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.HandleEndorserBlock(577, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"acquiring an EB before its ranking block is adopted must not emit a vote",
	)
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	emittedEvent := testutil.RequireReceive(
		t, emittedCh, testutil.AsyncWait, "prototype vote emission",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash)
	assert.Equal(t, uint64(3), emitted.Vote.VoterId)
	require.NoError(t, VerifyVoteSignature(
		key.PublicKey(),
		PrototypeVoteMessageBytes(rbHash),
		emitted.Vote.VoteSignature,
	))

	raws := fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 3}},
	)
	require.Len(t, raws, 1)
	var vote lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &vote)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), vote.VoterId)
	assert.Equal(t, ebHash, vote.EndorserBlockHash)
	require.NoError(
		t,
		VerifyVoteSignature(
			key.PublicKey(),
			PrototypeVoteMessageBytes(rbHash),
			vote.VoteSignature,
		),
	)

	// Exactly one vote per EB per voter
	fixture.mgr.HandleEndorserBlock(577, ebHash)
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	raws = fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 3}},
	)
	assert.Len(t, raws, 1)

	// The local vote is served to peers
	done := make(chan struct{})
	defer close(done)
	result := testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		testutil.AsyncWait,
		"own vote served to peers",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)
	assert.Equal(t, uint64(3), result.votes[0].VoterId)
}

func TestVoteManagerDoesNotEmitVoteAfterVotingReconfiguredDuringSigning(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := &fakeLeiosKeyProvider{}
	var member CommitteeMember
	var key *VoteSigningKey
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			key = f.keys[member.VoterId]
			proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
			require.NoError(t, err)
			keyProvider.keys = map[string]*lcommon.LeiosKey{
				hex.EncodeToString(member.PoolKeyHash): {
					PublicKey:       key.PublicKeyBytes(),
					PossessionProof: proof,
				},
			}
			cfg.KeyProvider = keyProvider
		},
	)
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	require.Equal(t, VotingConfigurationEnabled, status)

	subID, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
	signingEntered := make(chan struct{})
	releaseSigningCh := make(chan struct{})
	var signingEnteredOnce sync.Once
	var releaseSigningOnce sync.Once
	releaseSigning := func() {
		releaseSigningOnce.Do(func() { close(releaseSigningCh) })
	}
	defer releaseSigning()
	fixture.mgr.signVote = func(
		signingKey *VoteSigningKey,
		msg []byte,
	) ([]byte, error) {
		signingEnteredOnce.Do(func() { close(signingEntered) })
		<-releaseSigningCh
		return SignVote(signingKey, msg)
	}

	fixture.mgr.mu.Lock()
	initialGeneration := fixture.mgr.votingLookupGeneration
	fixture.mgr.mu.Unlock()
	ebHash := lcommon.NewBlake2b256([]byte("reconfigured-signing-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("reconfigured-signing-rb"))
	fixture.mgr.HandleEndorserBlock(501, ebHash)
	observeDone := make(chan struct{})
	go func() {
		fixture.mgr.ObserveAnnouncement(501, rbHash, ebHash)
		close(observeDone)
	}()
	testutil.RequireReceive(
		t,
		signingEntered,
		testutil.AsyncWait,
		"local vote signing",
	)

	replacementKey := testSigningKey(t, 214)
	var replacementPool lcommon.PoolKeyHash
	replacementPool[0] = 0xfe
	type configureResult struct {
		status VotingConfigurationStatus
		err    error
	}
	configuredCh := make(chan configureResult, 1)
	go func() {
		configuredStatus, configureErr := fixture.mgr.ConfigureVoting(
			replacementPool,
			replacementKey,
		)
		configuredCh <- configureResult{
			status: configuredStatus,
			err:    configureErr,
		}
	}()
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.votingLookupGeneration > initialGeneration &&
			fixture.mgr.votingKey == nil &&
			fixture.mgr.deferredVotingKey == replacementKey &&
			slices.Equal(fixture.mgr.deferredVotingPool, replacementPool[:])
	}, testutil.AsyncWait, "replacement voting configuration installed")

	releaseSigning()
	testutil.RequireReceive(
		t,
		observeDone,
		testutil.AsyncWait,
		"vote emission return",
	)
	result := testutil.RequireReceive(
		t,
		configuredCh,
		testutil.AsyncWait,
		"replacement voting configuration",
	)
	require.NoError(t, result.err)
	require.Equal(t, VotingConfigurationAwaitingKey, result.status)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"stale signed vote must not be published",
	)
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: 501, VoterId: member.VoterId,
	}}))
	fixture.mgr.mu.Lock()
	_, voted := fixture.mgr.votedAnnouncements[rbHash]
	fixture.mgr.mu.Unlock()
	assert.False(t, voted, "stale signed vote must not mutate vote state")
}

func TestVoteManagerQueuesPrototypeVoteUntilAnnouncement(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	vote := fixture.makePrototypeVote(t, 3, rbHash)

	require.NoError(t, fixture.mgr.HandlePrototypeVote("conn-a", vote))
	assert.Empty(t, fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 3}},
	))

	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	raws := fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 3}},
	)
	require.Len(t, raws, 1)
	var resolved lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &resolved)
	require.NoError(t, err)
	assert.Equal(t, uint64(577), resolved.SlotNo)
	assert.Equal(t, ebHash, resolved.EndorserBlockHash)
	assert.Equal(t, vote.VoteSignature, resolved.VoteSignature)
}

// TestVoteManagerPeerPrototypeVoteRequeuedForRelay guards relay re-diffusion: a
// relay stored a peer's vote for its own tally but never queued it back up
// for its other peers, so a block producer behind that relay never observed
// quorum. A newly accepted peer vote must publish VoteReceivedEventType
// (node_leios.go's subscriber feeds this into the origin-aware Ouroboros
// enqueue path) with the exact signed fields and connection key the peer sent.
func TestVoteManagerPeerPrototypeVoteRequeuedForRelay(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	subId, receivedCh := fixture.eventBus.Subscribe(VoteReceivedEventType)
	defer fixture.eventBus.Unsubscribe(VoteReceivedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	vote := fixture.makePrototypeVote(t, 3, rbHash)
	require.NoError(t, fixture.mgr.HandlePrototypeVote("conn-a", vote))

	requeued := testutil.RequireReceive(
		t, receivedCh, testutil.AsyncWait, "peer vote requeued for relay",
	)
	data, ok := requeued.Data.(VoteReceivedEvent)
	require.True(t, ok)
	assert.Equal(t, vote, data.Vote)
	assert.Equal(t, "conn-a", data.OriginConnKey)
}

// TestVoteManagerQueuedPeerPrototypeVoteRequeuedForRelayAfterAnnouncement
// covers the other acceptance path into insertVote: a vote received before
// its announcing ranking block is known is queued, then resolved and
// inserted from ObserveAnnouncement's pending-vote flush rather than from
// HandlePrototypeVote directly. That path must requeue for relay too.
func TestVoteManagerQueuedPeerPrototypeVoteRequeuedForRelayAfterAnnouncement(
	t *testing.T,
) {
	t.Parallel()

	fixture := newManagerFixture(t)
	subId, receivedCh := fixture.eventBus.Subscribe(VoteReceivedEventType)
	defer fixture.eventBus.Unsubscribe(VoteReceivedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	vote := fixture.makePrototypeVote(t, 3, rbHash)

	require.NoError(t, fixture.mgr.HandlePrototypeVote("conn-a", vote))
	testutil.RequireNoReceive(
		t,
		receivedCh,
		300*time.Millisecond,
		"a vote pending its announcing ranking block must not be relayed yet",
	)

	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	requeued := testutil.RequireReceive(
		t,
		receivedCh,
		testutil.AsyncWait,
		"queued peer vote requeued for relay once its ranking block resolves",
	)
	data, ok := requeued.Data.(VoteReceivedEvent)
	require.True(t, ok)
	assert.Equal(t, vote, data.Vote)
	assert.Equal(t, "conn-a", data.OriginConnKey)
}

// TestVoteManagerDuplicatePeerPrototypeVoteNotRequeuedForRelay confirms the
// requeue is gated by insertVote's dedup check, not fired unconditionally --
// a resubmission of a vote already on record must not cause a second
// diffusion round trip.
func TestVoteManagerDuplicatePeerPrototypeVoteNotRequeuedForRelay(
	t *testing.T,
) {
	t.Parallel()

	fixture := newManagerFixture(t)
	subId, receivedCh := fixture.eventBus.Subscribe(VoteReceivedEventType)
	defer fixture.eventBus.Unsubscribe(VoteReceivedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	vote := fixture.makePrototypeVote(t, 3, rbHash)
	require.NoError(t, fixture.mgr.HandlePrototypeVote("conn-a", vote))
	testutil.RequireReceive(
		t, receivedCh, testutil.AsyncWait, "first delivery requeued for relay",
	)

	require.NoError(t, fixture.mgr.HandlePrototypeVote("conn-b", vote))
	testutil.RequireNoReceive(
		t,
		receivedCh,
		300*time.Millisecond,
		"a resubmitted vote already on record must not be requeued again",
	)
}

func TestVoteManagerQueuedInvalidPrototypeVoteDoesNotSuppressValidVote(
	t *testing.T,
) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	valid := fixture.makePrototypeVote(t, 3, rbHash)
	// Neither signature can be checked before the ranking block identifies
	// reserve the voter id and suppress the valid vote.
	for i := range maxPendingPrototypeCandidatesPerVoter + 1 {
		forged := valid
		forged.VoteSignature = make([]byte, lcommon.LeiosBlsSignatureSize)
		copy(forged.VoteSignature, valid.VoteSignature)
		forged.VoteSignature[0] ^= byte(i + 1)
		require.NoError(t, fixture.mgr.HandlePrototypeVote("attacker", forged))
	}
	require.NoError(t, fixture.mgr.HandlePrototypeVote("peer", valid))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: 577, VoterId: 3,
	}})
	require.Len(t, raws, 1)
	var resolved lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &resolved)
	require.NoError(t, err)
	assert.Equal(t, valid.VoteSignature, resolved.VoteSignature)
}

func TestVoteManagerPendingPrototypeVotesFairAtCapacity(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	fixture.mgr.maxRecords = 4
	for i := range 4 {
		rbHash := lcommon.NewBlake2b256(
			[]byte(fmt.Sprintf("attacker-rb-%d", i)),
		)
		require.NoError(t, fixture.mgr.HandlePrototypeVote(
			"attacker",
			fixture.makePrototypeVote(t, uint64(i), rbHash),
		))
	}

	legitimateRb := lcommon.NewBlake2b256([]byte("legitimate-rb"))
	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"legitimate-peer",
		fixture.makePrototypeVote(t, 4, legitimateRb),
	))

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Equal(t, 4, fixture.mgr.pendingVoteCount)
	assert.Equal(t, 3, fixture.mgr.pendingVoteCountByConn["attacker"])
	assert.Equal(t, 1, fixture.mgr.pendingVoteCountByConn["legitimate-peer"])
	assert.Contains(t, fixture.mgr.pendingVotes, legitimateRb)
}

func TestVoteManagerPrototypeQuorumPreservesSigningContext(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)

	// ComputeCommittee orders voter ids descending by stake. The five
	// largest members contribute 400 of 550 stake, crossing the 7/10
	// threshold.
	for voterId := range uint64(5) {
		require.NoError(t, fixture.mgr.HandlePrototypeVote(
			"peer",
			fixture.makePrototypeVote(t, voterId, rbHash),
		))
	}

	evt := testutil.RequireReceive(
		t,
		quorumCh,
		testutil.AsyncWait,
		"prototype quorum certificate",
	)
	quorum, ok := evt.Data.(EbQuorumEvent)
	require.True(t, ok)
	require.NotNil(t, quorum.Certificate)
	assert.Equal(t, rbHash, quorum.AnnouncingRbHash)
	assert.Equal(t, uint64(400), quorum.VerifiedStake)
	sigChecked, err := ValidatePrototypeEbCertificate(
		quorum.Certificate,
		quorum.AnnouncingRbHash,
		committee,
		big.NewRat(7, 10),
		fixture.mgr.registry,
	)
	require.NoError(t, err)
	assert.True(t, sigChecked)
	wrongRbHash := lcommon.NewBlake2b256([]byte("different-rb"))
	_, err = ValidatePrototypeEbCertificate(
		quorum.Certificate,
		wrongRbHash,
		committee,
		big.NewRat(7, 10),
		fixture.mgr.registry,
	)
	require.Error(t, err)
}

func TestVoteManagerPrototypeTalliesAreSeparatedByAnnouncingBlock(
	t *testing.T,
) {
	t.Parallel()

	fixture := newManagerFixture(t)
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)
	ebHash := lcommon.NewBlake2b256([]byte("same-eb"))
	rbHashA := lcommon.NewBlake2b256([]byte("rb-a"))
	rbHashB := lcommon.NewBlake2b256([]byte("rb-b"))
	fixture.mgr.ObserveAnnouncement(577, rbHashA, ebHash)
	fixture.mgr.ObserveAnnouncement(577, rbHashB, ebHash)
	_, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)

	// ComputeCommittee orders voter ids descending by stake (id0=100 down
	// to id9=10). The two groups total 400 stake, but neither announcing
	// block reaches the 385 threshold independently. Their different
	// signed messages must never be aggregated into one certificate.
	for _, tc := range []struct {
		rbHash   lcommon.Blake2b256
		voterIds []uint64
	}{
		{rbHashA, []uint64{0, 1}},    // 190 stake
		{rbHashB, []uint64{2, 3, 4}}, // 210 stake
	} {
		for _, voterId := range tc.voterIds {
			require.NoError(t, fixture.mgr.HandlePrototypeVote(
				"peer",
				fixture.makePrototypeVote(t, voterId, tc.rbHash),
			))
		}
	}
	testutil.RequireNoReceive(
		t, quorumCh, 300*time.Millisecond,
		"different prototype signing contexts must not share a tally",
	)
}

func TestVoteManagerPrototypeRecordRetainedWhileContextTallyLive(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	base := time.Now()
	offset := time.Duration(0)
	fixture.mgr.now = func() time.Time { return base.Add(offset) }
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	_, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	submit := func(voterId uint64) {
		require.NoError(t, fixture.mgr.HandlePrototypeVote(
			"peer",
			fixture.makePrototypeVote(t, voterId, rbHash),
		))
	}

	submit(5)
	offset = 9 * time.Minute
	submit(6) // keep the context-specific tally live
	offset = voteStoreTTL + time.Minute
	submit(5) // must deduplicate even though voter 5's record is old

	fixture.mgr.mu.Lock()
	tally := fixture.mgr.tallies[tallyKey{
		slotNo:           577,
		ebHash:           ebHash,
		announcingRbHash: rbHash,
	}]
	record := fixture.mgr.voteRecords[lcommon.LeiosVoteId{
		SlotNo: 577, VoterId: 5,
	}]
	fixture.mgr.mu.Unlock()
	require.NotNil(t, tally)
	assert.Len(t, tally.verifiedVotes, 2)
	assert.Equal(t, rbHash, record.announcingRbHash)
}

func TestVoteManagerPrototypeUsesRegisteredKey(t *testing.T) {
	t.Parallel()

	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 999))
	require.NoError(t, err)
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			registry, err := NewVoterRegistry(nil)
			require.NoError(t, err)
			cfg.Registry = registry
		},
	)
	poolMember := fixture.members[3]
	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	voterID, ok := committee.VoterIdFor(poolMember.PoolKeyHash)
	require.True(t, ok)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], poolMember.PoolKeyHash)
	require.NoError(t, fixture.mgr.registry.RegisterPublicKey(
		poolKeyHash[:],
		key.PublicKey(),
	))
	rbHash := lcommon.NewBlake2b256([]byte("registered-key-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("registered-key-eb"))
	fixture.mgr.HandleEndorserBlock(577, ebHash)
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	signature, err := SignVote(key, PrototypeVoteMessageBytes(rbHash))
	require.NoError(t, err)

	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"peer",
		lcommon.LeiosPrototypeVote{
			AnnouncingRbHash: rbHash,
			VoterId:          voterID,
			VoteSignature:    signature,
		},
	))
	voteID := lcommon.LeiosVoteId{
		SlotNo: 577, VoterId: voterID,
	}
	raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteID})
	require.Len(t, raws, 1)
	fixture.mgr.mu.Lock()
	stored, ok := fixture.mgr.votesById[voteID]
	if ok {
		storedCopy := *stored
		stored = &storedCopy
	}
	fixture.mgr.mu.Unlock()
	require.True(t, ok)
	assert.Equal(t, "peer", stored.originConn)
	assert.True(t, stored.verified)

	cert, err := BuildEbCertificate(577, ebHash, committee, []VerifiedVote{{
		VoterId:   voterID,
		Signature: signature,
	}})
	require.NoError(t, err)
	sigChecked, err := ValidatePrototypeEbCertificate(
		cert,
		rbHash,
		committee,
		big.NewRat(0, 1),
		fixture.mgr.registry,
	)
	require.NoError(t, err)
	assert.True(t, sigChecked)
}

// TestVoteManagerValidatesAndEnablesVotingForPoolOutsideCommittee proves a
// pool with a real on-chain registered key, but zero stake in the current
// epoch's snapshot (so it can never be a ComputeCommittee member this
// epoch), can still ValidateVotingKey and EnableVoting: both must resolve
// the on-chain key for that specific pool independent of committee
// membership, since committee selection is re-evaluated every epoch and a
// pool not selected today may be selected once stake shifts.
func TestVoteManagerValidatesAndEnablesVotingForPoolOutsideCommittee(
	t *testing.T,
) {
	t.Parallel()

	key := testSigningKey(t, 210)
	proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	var poolKeyHash lcommon.PoolKeyHash
	poolKeyHash[0] = 0xfa // not one of the fixture's 10 staked pools
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(poolKeyHash[:]): {
						PublicKey:       key.PublicKeyBytes(),
						PossessionProof: proof,
					},
				},
			}
		},
	)
	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	_, isMember := committee.VoterIdFor(poolKeyHash[:])
	require.False(
		t,
		isMember,
		"test setup: pool must have no stake and no committee seat",
	)

	require.NoError(t, fixture.mgr.ValidateVotingKey(poolKeyHash, key))
	require.NoError(t, fixture.mgr.EnableVoting(poolKeyHash, key))

	fixture.mgr.mu.Lock()
	votingKey := fixture.mgr.votingKey
	fixture.mgr.mu.Unlock()
	require.NotNil(t, votingKey)
	assert.True(t, votingKey.PublicKey().Equal(key.PublicKey()))
}

// TestVoteManagerResolvesOnChainKeyWithoutRegistryEntry proves the core
// behavior of the Musashi w32 cutover: a committee member with a
// PoP-valid registered key verifies through KeyProvider alone, with no
// Registry entry and no derivation fallback involved.
func TestVoteManagerResolvesOnChainKeyWithoutRegistryEntry(t *testing.T) {
	t.Parallel()

	key := testSigningKey(t, 123)
	proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	var member CommitteeMember
	var keyProvider *fakeLeiosKeyProvider
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			emptyRegistry, regErr := NewVoterRegistry(nil)
			require.NoError(t, regErr)
			cfg.Registry = emptyRegistry
			keyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       key.PublicKeyBytes(),
						PossessionProof: proof,
					},
				},
			}
			cfg.KeyProvider = keyProvider
		},
	)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	sig, err := SignVote(key, VoteMessageBytes(577, ebHash))
	require.NoError(t, err)
	require.NoError(t, fixture.mgr.HandleVote("peer", lcommon.LeiosVote{
		SlotNo:            577,
		EndorserBlockHash: ebHash,
		VoterId:           member.VoterId,
		VoteSignature:     sig,
	}))
	// keyProvider is assigned by the customize closure the fixture builder
	// above invokes synchronously; nilaway does not follow that callback.
	//nolint:nilaway // assigned by the fixture closure above
	keyProvider.mu.Lock()
	resolvedSnapshotEpoch := keyProvider.snapshotEpoch
	keyProvider.mu.Unlock()
	require.Equal(
		t,
		CommitteeSnapshotEpoch(5),
		resolvedSnapshotEpoch,
		"key lookup must use the same snapshot epoch as committee stake",
	)
	fixture.mgr.mu.Lock()
	stored, ok := fixture.mgr.votesById[lcommon.LeiosVoteId{
		SlotNo: 577, VoterId: member.VoterId,
	}]
	fixture.mgr.mu.Unlock()
	require.True(t, ok)
	assert.True(t, stored.verified)
}

// TestVoteManagerReferenceModeIgnoresStaticRegistryForKeylessSeat proves a
// production-shaped manager (non-nil ledger key provider) never promotes a
// keyless seat through the private-harness static registry. The vote remains
// observable for membership/stake diagnostics, but it is not verified and
// cannot contribute to a certificate.
func TestVoteManagerReferenceModeIgnoresStaticRegistryForKeylessSeat(
	t *testing.T,
) {
	t.Parallel()

	var member CommitteeMember
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			// Keep the fixture's populated Registry while wiring the same
			// non-nil key-provider shape production composition uses. The
			// provider deliberately has no registration for member.
			cfg.KeyProvider = &fakeLeiosKeyProvider{}
		},
	)
	rbHash := lcommon.NewBlake2b256([]byte("keyless-static-fallback-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("keyless-static-fallback-eb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"peer",
		fixture.makePrototypeVote(t, member.VoterId, rbHash),
	))
	voteID := lcommon.LeiosVoteId{SlotNo: 577, VoterId: member.VoterId}
	fixture.mgr.mu.Lock()
	stored := fixture.mgr.votesById[voteID]
	tally := fixture.mgr.tallies[tallyKey{
		slotNo:           577,
		ebHash:           ebHash,
		announcingRbHash: rbHash,
	}]
	fixture.mgr.mu.Unlock()

	require.NotNil(t, stored)
	assert.False(
		t,
		stored.verified,
		"static registry must not verify a keyless on-chain seat in reference mode",
	)
	require.NotNil(t, tally)
	assert.Zero(
		t,
		tally.verifiedStake,
		"a static fallback vote must not contribute certificate stake",
	)
}

// TestVoteManagerReferenceModeRejectsLocalStaticFallback proves production
// composition cannot auto-register the local signing key when the pool has no
// usable on-chain registration. Registry-based local voting remains available
// only to managers constructed without a KeyProvider (the private test seam).
func TestVoteManagerReferenceModeRejectsLocalStaticFallback(t *testing.T) {
	t.Parallel()

	var member CommitteeMember
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			cfg.KeyProvider = &fakeLeiosKeyProvider{}
		},
	)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[member.VoterId]

	err := fixture.mgr.ValidateVotingKey(poolKeyHash, key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "on-chain")
	err = fixture.mgr.EnableVoting(poolKeyHash, key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "on-chain")

	fixture.mgr.mu.Lock()
	votingKey := fixture.mgr.votingKey
	fixture.mgr.mu.Unlock()
	assert.Nil(t, votingKey, "a keyless pool must remain non-voting")
}

// TestVoteManagerReferenceModeUsesOnChainKeyOverStaticMismatch exercises both
// sides of the production trust boundary: a configured static key cannot
// verify a vote when it differs from the PoP-verified on-chain registration,
// while the registered key is accepted for the same committee seat.
func TestVoteManagerReferenceModeUsesOnChainKeyOverStaticMismatch(
	t *testing.T,
) {
	t.Parallel()

	onChainKey := testSigningKey(t, 203)
	proof, err := signWithDST(onChainKey, onChainKey.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	var member CommitteeMember
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       onChainKey.PublicKeyBytes(),
						PossessionProof: proof,
					},
				},
			}
		},
	)
	rbHash := lcommon.NewBlake2b256([]byte("on-chain-authority-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("on-chain-authority-eb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	// The fixture's default key is still present in Registry, but conflicts
	// with the on-chain registration and therefore must be rejected.
	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"peer-static",
		fixture.makePrototypeVote(t, member.VoterId, rbHash),
	))
	voteID := lcommon.LeiosVoteId{SlotNo: 577, VoterId: member.VoterId}
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteID}))

	sig, err := SignVote(onChainKey, PrototypeVoteMessageBytes(rbHash))
	require.NoError(t, err)
	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"peer-on-chain",
		lcommon.LeiosPrototypeVote{
			AnnouncingRbHash: rbHash,
			VoterId:          member.VoterId,
			VoteSignature:    sig,
		},
	))
	fixture.mgr.mu.Lock()
	stored := fixture.mgr.votesById[voteID]
	fixture.mgr.mu.Unlock()
	require.NotNil(t, stored)
	assert.True(t, stored.verified)
}

// TestVoteManagerTreatsInvalidPoPOnChainKeyAsAbsent proves an on-chain
// key whose proof of possession does not verify is excluded entirely,
// matching upstream's "invalid proofs are treated as absent" rule: the
// member's vote is still accepted (membership-valid) but stays
// unverified, exactly like a genuinely keyless committee seat.
func TestVoteManagerTreatsInvalidPoPOnChainKeyAsAbsent(t *testing.T) {
	t.Parallel()

	key := testSigningKey(t, 124)
	wrongKey := testSigningKey(t, 125)
	badProof, err := signWithDST(wrongKey, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	var member CommitteeMember
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			emptyRegistry, regErr := NewVoterRegistry(nil)
			require.NoError(t, regErr)
			cfg.Registry = emptyRegistry
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       key.PublicKeyBytes(),
						PossessionProof: badProof,
					},
				},
			}
		},
	)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	sig, err := SignVote(key, VoteMessageBytes(577, ebHash))
	require.NoError(t, err)
	require.NoError(t, fixture.mgr.HandleVote("peer", lcommon.LeiosVote{
		SlotNo:            577,
		EndorserBlockHash: ebHash,
		VoterId:           member.VoterId,
		VoteSignature:     sig,
	}))
	fixture.mgr.mu.Lock()
	stored, ok := fixture.mgr.votesById[lcommon.LeiosVoteId{
		SlotNo: 577, VoterId: member.VoterId,
	}]
	fixture.mgr.mu.Unlock()
	require.True(t, ok)
	assert.False(t, stored.verified)
}

// TestVoteManagerRetriesOnChainKeyResolutionAfterTransientFailure proves a
// transient key-provider failure does not get memoized as "every seat
// keyless" for the epoch: committeeAndParamsForEpoch must fail outright
// (not cache an empty onChainKeys map) so a later, successful call can
// still resolve keys normally once the failure clears.
func TestVoteManagerRetriesOnChainKeyResolutionAfterTransientFailure(
	t *testing.T,
) {
	t.Parallel()

	key := testSigningKey(t, 126)
	proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	var member CommitteeMember
	keyProvider := &fakeLeiosKeyProvider{
		err: errors.New("store temporarily unavailable"),
	}
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			emptyRegistry, regErr := NewVoterRegistry(nil)
			require.NoError(t, regErr)
			cfg.Registry = emptyRegistry
			cfg.KeyProvider = keyProvider
		},
	)

	_, err = fixture.mgr.CommitteeForEpoch(5)
	require.Error(t, err, "a failing key provider must not be papered over")
	fixture.mgr.mu.Lock()
	_, cached := fixture.mgr.committees[5]
	fixture.mgr.mu.Unlock()
	assert.False(t, cached, "a failed resolution must not be memoized")

	keyProvider.mu.Lock()
	keyProvider.err = nil
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: proof,
		},
	}
	keyProvider.mu.Unlock()

	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err, "retrying after the store recovers must succeed")
	require.Equal(t, member.PoolKeyHash, committee.Members[3].PoolKeyHash)
}

func TestVoteManagerValidateConfiguredVotingKey(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	member := fixture.members[3]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)

	require.NoError(
		t,
		fixture.mgr.ValidateVotingKey(
			poolKeyHash,
			fixture.keys[member.VoterId],
		),
	)

	wrongKey, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 999))
	require.NoError(t, err)
	assert.Error(t, fixture.mgr.ValidateVotingKey(poolKeyHash, wrongKey))

	var missingPool lcommon.PoolKeyHash
	missingPool[0] = 0xff
	assert.Error(t, fixture.mgr.ValidateVotingKey(missingPool, wrongKey))
}

func TestVoteManagerDeferredVotingReplaysCurrentEpochAnnouncementsInOrder(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := &fakeLeiosKeyProvider{}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.KeyProvider = keyProvider
		},
	)
	member := fixture.members[3]
	key := fixture.keys[member.VoterId]
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)

	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	assert.Equal(t, VotingConfigurationAwaitingKey, status)
	subID, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Equal(t, member.PoolKeyHash, fixture.mgr.deferredVotingPool)
	fixture.mgr.mu.Unlock()
	staleEB := lcommon.NewBlake2b256([]byte("stale-eb"))
	staleRB := lcommon.NewBlake2b256([]byte("stale-rb"))
	fixture.mgr.HandleEndorserBlock(599, staleEB)
	fixture.mgr.ObserveAnnouncement(599, staleRB, staleEB)

	// Record eligible announcements in inverse slot order so replay cannot
	// accidentally inherit the announcements map's iteration order.
	laterEB := lcommon.NewBlake2b256([]byte("later-eb"))
	laterRB := lcommon.NewBlake2b256([]byte("later-rb"))
	fixture.mgr.HandleEndorserBlock(602, laterEB)
	fixture.mgr.ObserveAnnouncement(602, laterRB, laterEB)
	earlierEB := lcommon.NewBlake2b256([]byte("earlier-eb"))
	earlierRB := lcommon.NewBlake2b256([]byte("earlier-rb"))
	fixture.mgr.HandleEndorserBlock(601, earlierEB)
	fixture.mgr.ObserveAnnouncement(601, earlierRB, earlierEB)

	unacquiredEB := lcommon.NewBlake2b256([]byte("unacquired-eb"))
	unacquiredRB := lcommon.NewBlake2b256([]byte("unacquired-rb"))
	fixture.mgr.ObserveAnnouncement(603, unacquiredRB, unacquiredEB)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"a deferred signing key must not emit a vote",
	)

	proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	keyProvider.mu.Lock()
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: proof,
		},
	}
	keyProvider.mu.Unlock()

	fixture.eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{NewEpoch: 6},
		),
	)
	for _, expectedRbHash := range []lcommon.Blake2b256{earlierRB, laterRB} {
		emittedEvent := testutil.RequireReceive(
			t,
			emittedCh,
			testutil.AsyncWait,
			"replayed vote emission after on-chain key resolution",
		)
		emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
		require.True(t, ok)
		assert.Equal(t, expectedRbHash, emitted.Vote.AnnouncingRbHash)
		assert.Equal(t, member.VoterId, emitted.Vote.VoterId)
	}
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"stale and unacquired announcements must not be replayed",
	)
	fixture.mgr.mu.Lock()
	assert.Same(t, key, fixture.mgr.votingKey)
	assert.True(
		t,
		slices.Equal(fixture.mgr.votingPool, member.PoolKeyHash),
	)
	assert.Nil(t, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
	keyProvider.mu.Lock()
	assert.Equal(t, CommitteeSnapshotEpoch(6), keyProvider.snapshotEpoch)
	keyProvider.mu.Unlock()

	fixture.mgr.HandleEndorserBlock(603, unacquiredEB)
	emittedEvent := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"vote after the deferred announcement becomes acquired",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, unacquiredRB, emitted.Vote.AnnouncingRbHash)
	assert.Equal(t, member.VoterId, emitted.Vote.VoterId)
}

func TestVoteManagerConfigureVotingReplaysPreloadedAnnouncements(
	t *testing.T,
) {
	t.Parallel()

	var member CommitteeMember
	var key *VoteSigningKey
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			key = f.keys[member.VoterId]
			proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
			require.NoError(t, err)
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       key.PublicKeyBytes(),
						PossessionProof: proof,
					},
				},
			}
		},
	)
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)

	subID, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
	ebHash := lcommon.NewBlake2b256([]byte("preloaded-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("preloaded-rb"))
	fixture.mgr.HandleEndorserBlock(501, ebHash)
	fixture.mgr.ObserveAnnouncement(501, rbHash, ebHash)

	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	require.Equal(t, VotingConfigurationEnabled, status)
	emittedEvent := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"preloaded announcement replay during voting configuration",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash)
	assert.Equal(t, member.VoterId, emitted.Vote.VoterId)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"preloaded announcement must be replayed exactly once",
	)
}

func TestVoteManagerConfigureVotingDiscardsStaleLookupAfterActivation(
	t *testing.T,
) {
	t.Parallel()

	testCases := []struct {
		name        string
		staleResult string
	}{
		{name: "absence", staleResult: "absence"},
		{name: "mismatch", staleResult: "mismatch"},
		{name: "provider error", staleResult: "error"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			keyProvider := newBlockingInitialLeiosKeyProvider(
				CommitteeSnapshotEpoch(5),
			)
			defer keyProvider.releaseInitialLookup()
			var member CommitteeMember
			var key *VoteSigningKey
			fixture := newManagerFixture(
				t,
				func(f *managerFixture, cfg *VoteManagerConfig) {
					member = f.members[3]
					key = f.keys[member.VoterId]
					proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
					require.NoError(t, err)
					keyProvider.currentKeys = map[string]*lcommon.LeiosKey{
						hex.EncodeToString(member.PoolKeyHash): {
							PublicKey:       key.PublicKeyBytes(),
							PossessionProof: proof,
						},
					}
					cfg.KeyProvider = keyProvider
				},
			)
			require.NotNil(t, key)
			var poolKeyHash lcommon.PoolKeyHash
			copy(poolKeyHash[:], member.PoolKeyHash)

			switch testCase.staleResult {
			case "mismatch":
				staleKey := testSigningKey(t, 212)
				proof, err := signWithDST(staleKey, staleKey.PublicKeyBytes(), LeiosPoPDST)
				require.NoError(t, err)
				keyProvider.blockedKeys = map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       staleKey.PublicKeyBytes(),
						PossessionProof: proof,
					},
				}
			case "error":
				keyProvider.blockedErr = errors.New(
					"stale snapshot temporarily unavailable",
				)
			}

			subID, emittedCh := fixture.eventBus.Subscribe(
				VoteEmittedEventType,
			)
			defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
			ebHash := lcommon.NewBlake2b256([]byte("overlap-eb"))
			rbHash := lcommon.NewBlake2b256([]byte("overlap-rb"))
			fixture.mgr.HandleEndorserBlock(601, ebHash)
			fixture.mgr.ObserveAnnouncement(601, rbHash, ebHash)

			type configureResult struct {
				status VotingConfigurationStatus
				err    error
			}
			configuredCh := make(chan configureResult, 1)
			go func() {
				status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
				configuredCh <- configureResult{status: status, err: err}
			}()
			testutil.RequireReceive(
				t,
				keyProvider.entered,
				testutil.AsyncWait,
				"initial epoch key lookup",
			)

			fixture.mgr.retryDeferredVoting(6)
			emittedEvent := testutil.RequireReceive(
				t,
				emittedCh,
				testutil.AsyncWait,
				"newer epoch voting activation",
			)
			emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
			require.True(t, ok)
			assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash)

			keyProvider.releaseInitialLookup()
			result := testutil.RequireReceive(
				t,
				configuredCh,
				testutil.AsyncWait,
				"configuration after stale lookup release",
			)
			require.NoError(t, result.err)
			assert.Equal(t, VotingConfigurationSuperseded, result.status)
			testutil.RequireNoReceive(
				t,
				emittedCh,
				100*time.Millisecond,
				"stale lookup release must not emit a duplicate vote",
			)
		})
	}
}

func TestVoteManagerConfigureVotingReportsSupersededDifferentPoolReplacement(
	t *testing.T,
) {
	t.Parallel()

	testCases := []struct {
		name           string
		replacement    string
		expectedStatus VotingConfigurationStatus
		expectError    string
	}{
		{
			name:           "success",
			replacement:    "success",
			expectedStatus: VotingConfigurationEnabled,
		},
		{
			name:           "absence",
			replacement:    "absence",
			expectedStatus: VotingConfigurationAwaitingKey,
		},
		{
			name:           "invalid proof",
			replacement:    "invalid-proof",
			expectedStatus: VotingConfigurationAwaitingKey,
		},
		{
			name:           "mismatch",
			replacement:    "mismatch",
			expectedStatus: VotingConfigurationFailed,
			expectError:    "does not match",
		},
		{
			name:           "provider error",
			replacement:    "provider-error",
			expectedStatus: VotingConfigurationFailed,
			expectError:    "store temporarily unavailable",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			keyProvider := newBlockingFirstLeiosKeyProvider()
			defer keyProvider.releaseFirstLookup()
			fixture := newManagerFixture(
				t,
				func(_ *managerFixture, cfg *VoteManagerConfig) {
					cfg.KeyProvider = keyProvider
				},
			)
			originalMember := fixture.members[3]
			originalKey := fixture.keys[originalMember.VoterId]
			replacementMember := fixture.members[4]
			replacementKey := fixture.keys[replacementMember.VoterId]
			require.NotNil(t, originalKey)
			require.NotNil(t, replacementKey)
			var originalPool lcommon.PoolKeyHash
			copy(originalPool[:], originalMember.PoolKeyHash)
			var replacementPool lcommon.PoolKeyHash
			copy(replacementPool[:], replacementMember.PoolKeyHash)
			require.NotEqual(t, originalPool, replacementPool)

			switch testCase.replacement {
			case "success":
				proof, err := signWithDST(replacementKey, replacementKey.PublicKeyBytes(), LeiosPoPDST)
				require.NoError(t, err)
				keyProvider.keys = map[string]*lcommon.LeiosKey{
					hex.EncodeToString(replacementPool[:]): {
						PublicKey:       replacementKey.PublicKeyBytes(),
						PossessionProof: proof,
					},
				}
			case "invalid-proof":
				keyProvider.keys = map[string]*lcommon.LeiosKey{
					hex.EncodeToString(replacementPool[:]): {
						PublicKey: replacementKey.PublicKeyBytes(),
						PossessionProof: make(
							[]byte,
							lcommon.LeiosBlsSignatureSize,
						),
					},
				}
			case "mismatch":
				mismatchedKey := testSigningKey(t, 215)
				proof, err := signWithDST(mismatchedKey, mismatchedKey.PublicKeyBytes(), LeiosPoPDST)
				require.NoError(t, err)
				keyProvider.keys = map[string]*lcommon.LeiosKey{
					hex.EncodeToString(replacementPool[:]): {
						PublicKey:       mismatchedKey.PublicKeyBytes(),
						PossessionProof: proof,
					},
				}
			case "provider-error":
				keyProvider.err = errors.New(
					"store temporarily unavailable",
				)
			}

			type configureResult struct {
				status VotingConfigurationStatus
				err    error
			}
			originalResultCh := make(chan configureResult, 1)
			go func() {
				status, err := fixture.mgr.ConfigureVoting(
					originalPool,
					originalKey,
				)
				originalResultCh <- configureResult{status: status, err: err}
			}()
			testutil.RequireReceive(
				t,
				keyProvider.entered,
				testutil.AsyncWait,
				"original voting key lookup",
			)

			replacementStatus, replacementErr := fixture.mgr.ConfigureVoting(
				replacementPool,
				replacementKey,
			)
			assert.Equal(t, testCase.expectedStatus, replacementStatus)
			if testCase.expectError == "" {
				require.NoError(t, replacementErr)
			} else {
				require.ErrorContains(
					t,
					replacementErr,
					testCase.expectError,
				)
			}

			keyProvider.releaseFirstLookup()
			originalResult := testutil.RequireReceive(
				t,
				originalResultCh,
				testutil.AsyncWait,
				"superseded original voting configuration",
			)
			require.NoError(t, originalResult.err)
			assert.Equal(
				t,
				VotingConfigurationSuperseded,
				originalResult.status,
			)
		})
	}
}

func TestVoteManagerConfigureVotingDiscardsStaleLookupAfterDeferredRetry(
	t *testing.T,
) {
	t.Parallel()

	testCases := []struct {
		name   string
		result string
	}{
		{
			name:   "absence",
			result: "absence",
		},
		{
			name:   "invalid proof",
			result: "invalid-proof",
		},
		{
			name:   "provider error",
			result: "error",
		},
		{
			name:   "mismatch",
			result: "mismatch",
		},
		{
			name:   "replay preparation failure",
			result: "replay-failure",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			keyProvider := newBlockingInitialLeiosKeyProvider(
				CommitteeSnapshotEpoch(5),
			)
			defer keyProvider.releaseInitialLookup()
			var member CommitteeMember
			var key *VoteSigningKey
			fixture := newManagerFixture(
				t,
				func(f *managerFixture, cfg *VoteManagerConfig) {
					member = f.members[3]
					key = f.keys[member.VoterId]
					cfg.KeyProvider = keyProvider
				},
			)
			require.NotNil(t, key)
			var poolKeyHash lcommon.PoolKeyHash
			copy(poolKeyHash[:], member.PoolKeyHash)
			poolHash := hex.EncodeToString(member.PoolKeyHash)
			validProof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
			require.NoError(t, err)
			validKeys := map[string]*lcommon.LeiosKey{
				poolHash: {
					PublicKey:       key.PublicKeyBytes(),
					PossessionProof: validProof,
				},
			}
			keyProvider.blockedErr = errors.New("stale initial lookup failure")
			switch testCase.result {
			case "invalid-proof":
				keyProvider.currentKeys = map[string]*lcommon.LeiosKey{
					poolHash: {
						PublicKey: key.PublicKeyBytes(),
						PossessionProof: make(
							[]byte,
							lcommon.LeiosBlsSignatureSize,
						),
					},
				}
			case "error":
				keyProvider.currentErr = errors.New(
					"newer snapshot temporarily unavailable",
				)
			case "mismatch":
				otherKey := testSigningKey(t, 213)
				otherProof, signErr := signWithDST(otherKey, otherKey.PublicKeyBytes(), LeiosPoPDST)
				require.NoError(t, signErr)
				keyProvider.currentKeys = map[string]*lcommon.LeiosKey{
					poolHash: {
						PublicKey:       otherKey.PublicKeyBytes(),
						PossessionProof: otherProof,
					},
				}
			case "replay-failure":
				keyProvider.currentKeys = validKeys
				keyProvider.currentFailCall = 2
				ebHash := lcommon.NewBlake2b256([]byte("overlap-deferred-eb"))
				rbHash := lcommon.NewBlake2b256([]byte("overlap-deferred-rb"))
				fixture.mgr.HandleEndorserBlock(601, ebHash)
				fixture.mgr.ObserveAnnouncement(601, rbHash, ebHash)
			}

			type configureResult struct {
				status VotingConfigurationStatus
				err    error
			}
			configuredCh := make(chan configureResult, 1)
			go func() {
				status, configureErr := fixture.mgr.ConfigureVoting(
					poolKeyHash,
					key,
				)
				configuredCh <- configureResult{
					status: status,
					err:    configureErr,
				}
			}()
			testutil.RequireReceive(
				t,
				keyProvider.entered,
				testutil.AsyncWait,
				"initial epoch key lookup",
			)

			fixture.mgr.retryDeferredVoting(6)
			keyProvider.releaseInitialLookup()
			result := testutil.RequireReceive(
				t,
				configuredCh,
				testutil.AsyncWait,
				"configuration after stale lookup release",
			)
			require.NoError(t, result.err)
			assert.Equal(t, VotingConfigurationSuperseded, result.status)
			fixture.mgr.mu.Lock()
			assert.Nil(t, fixture.mgr.votingKey)
			assert.Same(t, key, fixture.mgr.deferredVotingKey)
			fixture.mgr.mu.Unlock()

			keyProvider.mu.Lock()
			keyProvider.currentErr = nil
			keyProvider.currentFailCall = 0
			keyProvider.currentKeys = validKeys
			keyProvider.mu.Unlock()
			fixture.mgr.retryDeferredVoting(7)
			fixture.mgr.mu.Lock()
			assert.Same(t, key, fixture.mgr.votingKey)
			assert.Nil(t, fixture.mgr.deferredVotingKey)
			fixture.mgr.mu.Unlock()
		})
	}
}

func TestVoteManagerConfigureVotingDoesNotBeatNewerInFlightRetry(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := newBlockingInitialLeiosKeyProvider(
		CommitteeSnapshotEpoch(5),
	)
	keyProvider.blockCurrent = true
	defer keyProvider.releaseInitialLookup()
	defer keyProvider.releaseCurrentLookup()
	var member CommitteeMember
	var key *VoteSigningKey
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			key = f.keys[member.VoterId]
			cfg.KeyProvider = keyProvider
		},
	)
	require.NotNil(t, key)
	proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	keyProvider.blockedKeys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: proof,
		},
	}
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)

	type configureResult struct {
		status VotingConfigurationStatus
		err    error
	}
	configuredCh := make(chan configureResult, 1)
	go func() {
		status, configureErr := fixture.mgr.ConfigureVoting(poolKeyHash, key)
		configuredCh <- configureResult{status: status, err: configureErr}
	}()
	testutil.RequireReceive(
		t,
		keyProvider.entered,
		testutil.AsyncWait,
		"initial epoch key lookup",
	)
	retryDone := make(chan struct{})
	go func() {
		fixture.mgr.retryDeferredVoting(6)
		close(retryDone)
	}()
	testutil.RequireReceive(
		t,
		keyProvider.currentEntered,
		testutil.AsyncWait,
		"newer retry key lookup",
	)

	keyProvider.releaseInitialLookup()
	result := testutil.RequireReceive(
		t,
		configuredCh,
		testutil.AsyncWait,
		"configuration while newer retry remains in flight",
	)
	require.NoError(t, result.err)
	assert.Equal(t, VotingConfigurationSuperseded, result.status)
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()

	keyProvider.releaseCurrentLookup()
	testutil.RequireReceive(t, retryDone, testutil.AsyncWait, "newer deferred retry")
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerConfigureVotingReportsReplayPreparationFailure(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := &fakeLeiosKeyProvider{failOnCall: 2}
	var member CommitteeMember
	var key *VoteSigningKey
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			key = f.keys[member.VoterId]
			proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
			require.NoError(t, err)
			keyProvider.keys = map[string]*lcommon.LeiosKey{
				hex.EncodeToString(member.PoolKeyHash): {
					PublicKey:       key.PublicKeyBytes(),
					PossessionProof: proof,
				},
			}
			keyProvider.failErr = errors.New(
				"committee keys temporarily unavailable",
			)
			cfg.KeyProvider = keyProvider
		},
	)
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)

	subID, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
	ebHash := lcommon.NewBlake2b256([]byte("failed-preparation-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("failed-preparation-rb"))
	fixture.mgr.HandleEndorserBlock(501, ebHash)
	fixture.mgr.ObserveAnnouncement(501, rbHash, ebHash)

	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	assert.Equal(t, VotingConfigurationRetryPending, status)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"failed replay preparation must leave voting disabled",
	)
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerDeferredVotingRetriesFailedReplayLookup(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := &fakeLeiosKeyProvider{}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.KeyProvider = keyProvider
		},
	)
	member := fixture.members[3]
	key := fixture.keys[member.VoterId]
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)

	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	require.Equal(t, VotingConfigurationAwaitingKey, status)
	subID, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
	ebHash := lcommon.NewBlake2b256([]byte("replay-provider-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("replay-provider-rb"))
	fixture.mgr.HandleEndorserBlock(501, ebHash)
	fixture.mgr.ObserveAnnouncement(501, rbHash, ebHash)

	proof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	keyProvider.mu.Lock()
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: proof,
		},
	}
	// ConfigureVoting made call 1. The deferred authorization lookup below
	// is call 2; fail call 3, when replay resolves the full committee.
	keyProvider.failOnCall = 3
	keyProvider.failErr = errors.New(
		"committee key store temporarily unavailable",
	)
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(5)

	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"failed replay lookup must not emit a vote",
	)
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	assert.True(
		t,
		slices.Equal(fixture.mgr.deferredVotingPool, member.PoolKeyHash),
	)
	fixture.mgr.mu.Unlock()

	keyProvider.mu.Lock()
	keyProvider.failOnCall = 0
	keyProvider.failErr = nil
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(5)

	emittedEvent := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"announcement replay after committee provider recovery",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash)
	assert.Equal(t, member.VoterId, emitted.Vote.VoterId)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"recovered replay must emit the announcement exactly once",
	)
	fixture.mgr.mu.Lock()
	assert.Same(t, key, fixture.mgr.votingKey)
	assert.Nil(t, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerDeferredVotingRejectsInvalidAuthorization(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := &fakeLeiosKeyProvider{}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.KeyProvider = keyProvider
		},
	)
	member := fixture.members[3]
	key := fixture.keys[member.VoterId]
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)

	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	require.Equal(t, VotingConfigurationAwaitingKey, status)

	keyProvider.mu.Lock()
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: make([]byte, lcommon.LeiosBlsSignatureSize),
		},
	}
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(6)

	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()

	validProof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	keyProvider.mu.Lock()
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: validProof,
		},
	}
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(7)

	fixture.mgr.mu.Lock()
	assert.Same(t, key, fixture.mgr.votingKey)
	assert.Nil(t, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerDeferredVotingRetryRetainsMismatchedKeyUntilRecovery(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := &fakeLeiosKeyProvider{}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.KeyProvider = keyProvider
		},
	)
	member := fixture.members[3]
	key := fixture.keys[member.VoterId]
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	require.Equal(t, VotingConfigurationAwaitingKey, status)

	subID, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
	firstEB := lcommon.NewBlake2b256([]byte("mismatch-first-eb"))
	firstRB := lcommon.NewBlake2b256([]byte("mismatch-first-rb"))
	fixture.mgr.HandleEndorserBlock(601, firstEB)
	fixture.mgr.ObserveAnnouncement(601, firstRB, firstEB)

	mismatchedKey := testSigningKey(t, 211)
	mismatchedProof, err := signWithDST(mismatchedKey, mismatchedKey.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	keyProvider.mu.Lock()
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       mismatchedKey.PublicKeyBytes(),
			PossessionProof: mismatchedProof,
		},
	}
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(6)

	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	assert.True(
		t,
		slices.Equal(fixture.mgr.deferredVotingPool, member.PoolKeyHash),
	)
	fixture.mgr.mu.Unlock()
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: 601, VoterId: member.VoterId,
	}}))
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"a mismatched deferred key must not emit a vote",
	)

	secondEB := lcommon.NewBlake2b256([]byte("mismatch-recovery-eb"))
	secondRB := lcommon.NewBlake2b256([]byte("mismatch-recovery-rb"))
	fixture.mgr.HandleEndorserBlock(701, secondEB)
	fixture.mgr.ObserveAnnouncement(701, secondRB, secondEB)
	validProof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	keyProvider.mu.Lock()
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: validProof,
		},
	}
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(7)

	emittedEvent := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"vote emission after mismatched registration recovers",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, secondRB, emitted.Vote.AnnouncingRbHash)
	fixture.mgr.mu.Lock()
	assert.Same(t, key, fixture.mgr.votingKey)
	assert.Nil(t, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerDeferredVotingRetryRetainsProviderFailureUntilRecovery(
	t *testing.T,
) {
	t.Parallel()

	keyProvider := &fakeLeiosKeyProvider{}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.KeyProvider = keyProvider
		},
	)
	member := fixture.members[3]
	key := fixture.keys[member.VoterId]
	require.NotNil(t, key)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	status, err := fixture.mgr.ConfigureVoting(poolKeyHash, key)
	require.NoError(t, err)
	require.Equal(t, VotingConfigurationAwaitingKey, status)

	subID, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subID)
	firstEB := lcommon.NewBlake2b256([]byte("provider-first-eb"))
	firstRB := lcommon.NewBlake2b256([]byte("provider-first-rb"))
	fixture.mgr.HandleEndorserBlock(601, firstEB)
	fixture.mgr.ObserveAnnouncement(601, firstRB, firstEB)
	keyProvider.mu.Lock()
	keyProvider.err = errors.New("store temporarily unavailable")
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(6)

	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	assert.True(
		t,
		slices.Equal(fixture.mgr.deferredVotingPool, member.PoolKeyHash),
	)
	fixture.mgr.mu.Unlock()
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: 601, VoterId: member.VoterId,
	}}))
	testutil.RequireNoReceive(
		t,
		emittedCh,
		100*time.Millisecond,
		"a failed deferred provider lookup must not emit a vote",
	)

	secondEB := lcommon.NewBlake2b256([]byte("provider-recovery-eb"))
	secondRB := lcommon.NewBlake2b256([]byte("provider-recovery-rb"))
	fixture.mgr.HandleEndorserBlock(701, secondEB)
	fixture.mgr.ObserveAnnouncement(701, secondRB, secondEB)
	validProof, err := signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	keyProvider.mu.Lock()
	keyProvider.err = nil
	keyProvider.keys = map[string]*lcommon.LeiosKey{
		hex.EncodeToString(member.PoolKeyHash): {
			PublicKey:       key.PublicKeyBytes(),
			PossessionProof: validProof,
		},
	}
	keyProvider.mu.Unlock()
	fixture.mgr.retryDeferredVoting(7)

	emittedEvent := testutil.RequireReceive(
		t,
		emittedCh,
		testutil.AsyncWait,
		"vote emission after deferred provider recovery",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, secondRB, emitted.Vote.AnnouncingRbHash)
	fixture.mgr.mu.Lock()
	assert.Same(t, key, fixture.mgr.votingKey)
	assert.Nil(t, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerConfigureVotingRejectsResolvedMismatch(t *testing.T) {
	t.Parallel()

	onChainKey := testSigningKey(t, 210)
	proof, err := signWithDST(onChainKey, onChainKey.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	var member CommitteeMember
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       onChainKey.PublicKeyBytes(),
						PossessionProof: proof,
					},
				},
			}
		},
	)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[member.VoterId]
	require.NotNil(t, key)

	status, err := fixture.mgr.ConfigureVoting(
		poolKeyHash,
		key,
	)
	require.Error(t, err)
	assert.Equal(t, VotingConfigurationFailed, status)
	assert.Contains(t, err.Error(), "does not match")
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Nil(t, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerConfigureVotingPropagatesKeyProviderFailure(
	t *testing.T,
) {
	t.Parallel()

	member := CommitteeMember{}
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				err: errors.New("store temporarily unavailable"),
			}
		},
	)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[member.VoterId]
	require.NotNil(t, key)

	status, err := fixture.mgr.ConfigureVoting(
		poolKeyHash,
		key,
	)
	require.Error(t, err)
	assert.Equal(t, VotingConfigurationFailed, status)
	assert.Contains(t, err.Error(), "store temporarily unavailable")
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Nil(t, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

// TestVoteManagerEnableVotingIgnoresStaleRegistryWhenOnChainKeyMatches proves
// a real on-chain key rotation is not blocked by a private-harness Registry
// entry still holding the pre-rotation key: a non-nil KeyProvider is the
// authoritative, PoP-verified trust source.
func TestVoteManagerEnableVotingIgnoresStaleRegistryWhenOnChainKeyMatches(
	t *testing.T,
) {
	t.Parallel()

	rotatedKey := testSigningKey(t, 200)
	proof, err := signWithDST(rotatedKey, rotatedKey.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	var member CommitteeMember
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       rotatedKey.PublicKeyBytes(),
						PossessionProof: proof,
					},
				},
			}
		},
	)
	// Sanity: the fixture's static registry still carries the
	// pre-rotation key for this pool, which genuinely conflicts with the
	// rotated on-chain key above -- this is the stale-peer-config scenario.
	staleRegistered, ok := fixture.mgr.registry.PublicKeyFor(member.PoolKeyHash)
	require.True(t, ok)
	require.False(t, staleRegistered.Equal(rotatedKey.PublicKey()))

	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	require.NoError(t, fixture.mgr.ValidateVotingKey(poolKeyHash, rotatedKey))
	require.NoError(t, fixture.mgr.EnableVoting(poolKeyHash, rotatedKey))

	fixture.mgr.mu.Lock()
	votingKey := fixture.mgr.votingKey
	fixture.mgr.mu.Unlock()
	require.NotNil(t, votingKey)
	assert.True(t, votingKey.PublicKey().Equal(rotatedKey.PublicKey()))
}

// TestVoteManagerEnableVotingRejectsKeyMismatchingOnChainRegistration
// proves EnableVoting hard-rejects a configured key that disagrees with a
// resolvable on-chain key for the pool, rather than falling back to the
// registry and succeeding with a key that would never actually verify:
// resolveVoterKey (checked by every emission) prefers the same on-chain
// key, so silently enabling voting here would just make every subsequent
// emission fail instead of failing loudly now.
func TestVoteManagerEnableVotingRejectsKeyMismatchingOnChainRegistration(
	t *testing.T,
) {
	t.Parallel()

	onChainKey := testSigningKey(t, 201)
	proof, err := signWithDST(onChainKey, onChainKey.PublicKeyBytes(), LeiosPoPDST)
	require.NoError(t, err)
	wrongKey := testSigningKey(t, 202)
	var member CommitteeMember
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			emptyRegistry, regErr := NewVoterRegistry(nil)
			require.NoError(t, regErr)
			cfg.Registry = emptyRegistry
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(member.PoolKeyHash): {
						PublicKey:       onChainKey.PublicKeyBytes(),
						PossessionProof: proof,
					},
				},
			}
		},
	)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	require.Error(t, fixture.mgr.EnableVoting(poolKeyHash, wrongKey))

	fixture.mgr.mu.Lock()
	votingKey := fixture.mgr.votingKey
	fixture.mgr.mu.Unlock()
	assert.Nil(t, votingKey, "a rejected key must not be enabled")
}

// TestVoteManagerValidateVotingKeyPropagatesKeyProviderFailure proves a
// transient key-provider error is a hard failure, not "no on-chain key
// found": treating the two the same would make ValidateVotingKey silently
// fall back to the static registry during exactly the kind of outage that
// should instead block startup until it clears.
func TestVoteManagerValidateVotingKeyPropagatesKeyProviderFailure(
	t *testing.T,
) {
	t.Parallel()

	member := CommitteeMember{}
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				err: errors.New("store temporarily unavailable"),
			}
		},
	)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	err := fixture.mgr.ValidateVotingKey(
		poolKeyHash,
		fixture.keys[member.VoterId],
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store temporarily unavailable")
}

// TestVoteManagerEnableVotingPropagatesKeyProviderFailure proves the same
// for EnableVoting specifically: a transient failure must not let it fall
// through to registering in the static registry and reporting success,
// since that would leave a pool believing it is voting when the real
// on-chain key (invisible only because of the outage) might disagree --
// and every subsequent emission would then silently reject it once the
// outage clears and the real key resolves.
func TestVoteManagerEnableVotingPropagatesKeyProviderFailure(t *testing.T) {
	t.Parallel()

	member := CommitteeMember{}
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				err: errors.New("store temporarily unavailable"),
			}
		},
	)
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	err := fixture.mgr.EnableVoting(poolKeyHash, fixture.keys[member.VoterId])
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store temporarily unavailable")

	fixture.mgr.mu.Lock()
	votingKey := fixture.mgr.votingKey
	fixture.mgr.mu.Unlock()
	assert.Nil(t, votingKey, "a failed lookup must not enable voting")
}

func TestVoteManagerOwnVoteRequiresCommitteeMembership(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	var poolKeyHash lcommon.PoolKeyHash
	poolKeyHash[0] = 0xee // not a committee member
	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 999))
	require.NoError(t, err)
	fixture.mgr.EnableVoting(poolKeyHash, key)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	fixture.mgr.HandleEndorserBlock(577, ebHash)
	for voterId := range uint64(10) {
		raws := fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: voterId}},
		)
		assert.Empty(t, raws)
	}
}

func TestVoteManagerNoVoteWithoutVotingEnabled(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	fixture.mgr.HandleEndorserBlock(577, ebHash)
	for voterId := range uint64(10) {
		raws := fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: voterId}},
		)
		assert.Empty(t, raws)
	}
}

func TestVoteManagerVotesByIdsSubset(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 577, ebHash),
		),
	)
	raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{
		{SlotNo: 577, VoterId: 0},
		{SlotNo: 577, VoterId: 9}, // unknown: omitted
	})
	require.Len(t, raws, 1)
	var vote lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &vote)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), vote.VoterId)
}

func TestVoteManagerRollbackPrunesVotes(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 510, ebHash),
		),
	)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 590, ebHash),
		),
	)
	callsBefore := fixture.stake.callCount()

	fixture.eventBus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(
			chain.ChainUpdateEventType,
			chain.ChainRollbackEvent{
				Point: ocommon.Point{Slot: 550},
			},
		),
	)

	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 590, VoterId: 1}},
		)) == 0
	}, testutil.AsyncWait, "votes after the rollback point are pruned")
	assert.Len(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 510, VoterId: 0}},
		),
		1,
		"votes at or before the rollback point are retained",
	)

	// The committee memo is cleared: next lookup recomputes
	_, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	assert.Greater(t, fixture.stake.callCount(), callsBefore)
}

func TestVoteManagerEpochTransitionPrunes(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// Epoch 3 vote (slot 350) and epoch 5 vote (slot 577)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 350, ebHash),
		),
	)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 577, ebHash),
		),
	)

	fixture.eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{
				PreviousEpoch: 5,
				NewEpoch:      6,
			},
		),
	)

	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 350, VoterId: 0}},
		)) == 0
	}, testutil.AsyncWait, "votes older than the previous epoch are pruned")
	assert.Len(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 1}},
		),
		1,
		"previous-epoch votes are retained",
	)
}

func TestVoteManagerEpochTransitionPrunesPrototypeStateAndCounts(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	oldRb := lcommon.NewBlake2b256([]byte("old-rb"))
	oldEb := lcommon.NewBlake2b256([]byte("old-eb"))
	currentRb := lcommon.NewBlake2b256([]byte("current-rb"))
	currentEb := lcommon.NewBlake2b256([]byte("current-eb"))

	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"old-peer", fixture.makePrototypeVote(t, 0, oldRb),
	))
	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"current-peer", fixture.makePrototypeVote(t, 1, currentRb),
	))
	now := fixture.mgr.now()
	fixture.mgr.mu.Lock()
	fixture.mgr.announcements[oldRb] = announcementRecord{
		slot: 350, epoch: 3, ebHash: oldEb, seenAt: now,
	}
	fixture.mgr.announcements[currentRb] = announcementRecord{
		slot: 577, epoch: 5, ebHash: currentEb, seenAt: now,
	}
	fixture.mgr.mu.Unlock()
	fixture.mgr.HandleEndorserBlock(350, oldEb)
	fixture.mgr.HandleEndorserBlock(577, currentEb)

	fixture.mgr.handleEpochTransition(event.EpochTransitionEvent{
		PreviousEpoch: 5,
		NewEpoch:      6,
	})

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, oldRb)
	assert.NotContains(t, fixture.mgr.pendingVotes, oldRb)
	assert.NotContains(t, fixture.mgr.acquiredEbs, oldEb)
	assert.Contains(t, fixture.mgr.announcements, currentRb)
	assert.Contains(t, fixture.mgr.pendingVotes, currentRb)
	assert.Contains(t, fixture.mgr.acquiredEbs, currentEb)
	assert.Equal(t, 1, fixture.mgr.pendingVoteCount)
	assert.Empty(t, fixture.mgr.pendingVoteCountByConn["old-peer"])
	assert.Equal(t, 1, fixture.mgr.pendingVoteCountByConn["current-peer"])
}

func TestVoteManagerTTLPrune(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	base := time.Now()
	var offsetMu sync.Mutex
	offset := time.Duration(0)
	fixture.mgr.now = func() time.Time {
		offsetMu.Lock()
		defer offsetMu.Unlock()
		return base.Add(offset)
	}

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)
	offsetMu.Lock()
	offset = voteStoreTTL + time.Minute
	offsetMu.Unlock()
	// Inserting another vote triggers pruning of the expired one
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 578, ebHash),
		),
	)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
		),
		"expired votes are pruned",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 578, VoterId: 1}},
		),
		1,
	)
}

func TestVoteManagerSizePrune(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	fixture.mgr.maxVotes = 2
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	for voterId := range uint64(3) {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	assert.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
		),
		"oldest vote evicted at size bound",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{
			{SlotNo: 577, VoterId: 1},
			{SlotNo: 577, VoterId: 2},
		}),
		2,
	)
}

func TestVoteManagerCommitteeMemoized(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	first, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	second, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	assert.Same(t, first, second)
	assert.Equal(t, 1, fixture.stake.callCount())
	// StakeSnapshotEpoch(5) = 5-1 = 4 (leader/committee stake is end-of-E-2 =
	// mark[E-1]); this shifted from 3 when the E-2 off-by-one was corrected.
	assert.Equal(t, uint64(4), first.SnapshotEpoch)

	_, err = fixture.mgr.CommitteeForEpoch(4)
	require.NoError(t, err)
	assert.Equal(t, 2, fixture.stake.callCount())
}

func TestVoteManagerCommitteeUnavailableNotMemoized(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	fixture.stake.setError(errors.New("snapshot not ready"))
	_, err := fixture.mgr.CommitteeForEpoch(5)
	require.Error(t, err)

	// Recovery: errors are not memoized
	fixture.stake.setError(nil)
	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), committee.Size())
}

func TestVoteManagerParamsValidationFailureSurfaces(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			f.params.err = errors.New("invalid historical Dijkstra parameters")
		},
	)
	_, err := fixture.mgr.CommitteeForEpoch(5)
	require.Error(t, err)

	// Votes are dropped gracefully while params are invalid
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
		),
	)

	// Own-vote emission is also disabled
	member := fixture.members[3]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[3]
	require.NotNil(t, key)
	fixture.mgr.EnableVoting(poolKeyHash, key)
	fixture.mgr.HandleEndorserBlock(577, ebHash)
	fixture.mgr.ObserveAnnouncement(
		577,
		lcommon.NewBlake2b256([]byte("announcing-rb")),
		ebHash,
	)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 3}},
		),
	)
}

func TestVoteManagerExpiredVoteIdCanBeReplaced(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	base := time.Now()
	var offsetMu sync.Mutex
	offset := time.Duration(0)
	fixture.mgr.now = func() time.Time {
		offsetMu.Lock()
		defer offsetMu.Unlock()
		return base.Add(offset)
	}

	ebHashA := lcommon.NewBlake2b256([]byte("eb-a"))
	ebHashB := lcommon.NewBlake2b256([]byte("eb-b"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHashA),
		),
	)
	offsetMu.Lock()
	offset = voteStoreTTL + time.Minute
	offsetMu.Unlock()
	// The first vote has expired: a fresh vote with the same id must
	// replace it rather than being dropped by the stale dedup entry.
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHashB),
		),
	)
	raws := fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
	)
	require.Len(t, raws, 1)
	var stored lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &stored)
	require.NoError(t, err)
	assert.Equal(t, ebHashB, stored.EndorserBlockHash)
}

func TestVoteManagerNextVotesAbortDoesNotSkipVotes(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)

	// Request more votes than available, then abort the wait
	done := make(chan struct{})
	resultCh := startNextVotes(fixture, done, "conn-b", 2)
	testutil.RequireNoReceive(
		t,
		resultCh,
		300*time.Millisecond,
		"NextVotes waits for the full count",
	)
	close(done)
	result := testutil.RequireReceive(
		t,
		resultCh,
		testutil.AsyncWait,
		"aborted NextVotes returns",
	)
	require.Error(t, result.err)

	// The undelivered vote must still be served on the next request
	done2 := make(chan struct{})
	defer close(done2)
	result = testutil.RequireReceive(
		t,
		startNextVotes(fixture, done2, "conn-b", 1),
		testutil.AsyncWait,
		"vote re-served after aborted request",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)
	assert.Equal(t, uint64(0), result.votes[0].VoterId)
}

func TestVoteManagerEvictedVoteDoesNotRecount(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	fixture.mgr.maxVotes = 3
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// Voters 0..3 hold 100+90+80+70 = 340 < 385 (tau = 7/10 of 550).
	// Voter 0's serving entry is size-evicted by voter 3's insert.
	for voterId := range uint64(4) {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	require.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
		),
		"voter 0's serving entry is evicted",
	)

	// Re-delivery of the evicted vote (e.g. a reconnecting peer
	// re-serving its log) must not re-count its stake: an unfixed
	// re-count reaches 440 >= 385 with a duplicate voter id, which
	// wedges certificate building for this EB permanently.
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-b",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)
	testutil.RequireNoReceive(
		t,
		quorumCh,
		300*time.Millisecond,
		"re-delivered vote must not count toward quorum",
	)

	// Genuine quorum: voter 4 brings verified stake to 400 >= 385
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 4, 577, ebHash),
		),
	)
	evt := testutil.RequireReceive(
		t,
		quorumCh,
		testutil.AsyncWait,
		"quorum event after genuine quorum",
	)
	quorum, ok := evt.Data.(EbQuorumEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(400), quorum.VerifiedStake)
	assert.Equal(t, uint64(400), quorum.ObservedStake)
	require.NotNil(t, quorum.Certificate)

	committee, err := fixture.mgr.CommitteeForEpoch(5)
	require.NoError(t, err)
	registry, err := NewVoterRegistry(fixture.registryEntries)
	require.NoError(t, err)
	sigChecked, err := ValidateEbCertificate(
		quorum.Certificate, committee, big.NewRat(7, 10), registry,
	)
	require.NoError(t, err)
	assert.True(t, sigChecked)
}

func TestVoteManagerEvictedVoteEquivocationStillDetected(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	fixture.mgr.maxVotes = 1
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)

	ebHashA := lcommon.NewBlake2b256([]byte("eb-a"))
	ebHashB := lcommon.NewBlake2b256([]byte("eb-b"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHashA),
		),
	)
	// Voter 1's insert evicts voter 0's serving entry
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 577, ebHashA),
		),
	)
	require.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
		),
	)

	// Voter 0 equivocates after eviction: the record must still hold
	// the first vote so the conflicting one is dropped.
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-b",
			fixture.makeVote(t, 0, 577, ebHashB),
		),
	)

	// Voters 2..6 hold 80+70+60+50+40 = 300 < 385 for hashB. A leaked
	// equivocating vote (voter 0's 100) would push it to 400 >= 385
	// and fire a quorum event.
	for voterId := uint64(2); voterId <= 6; voterId++ {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHashB),
			),
		)
	}
	testutil.RequireNoReceive(
		t,
		quorumCh,
		300*time.Millisecond,
		"equivocating vote must not count after serving eviction",
	)
}

func TestVoteManagerRecordsRetainedWhileTallyLive(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	base := time.Now()
	var offsetMu sync.Mutex
	offset := time.Duration(0)
	fixture.mgr.now = func() time.Time {
		offsetMu.Lock()
		defer offsetMu.Unlock()
		return base.Add(offset)
	}
	setOffset := func(d time.Duration) {
		offsetMu.Lock()
		offset = d
		offsetMu.Unlock()
	}
	subId, quorumCh := fixture.eventBus.Subscribe(EbQuorumEventType)
	defer fixture.eventBus.Unsubscribe(EbQuorumEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)
	// A later vote keeps the tally alive past voter 0's record TTL
	setOffset(9 * time.Minute)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 577, ebHash),
		),
	)

	// Voter 0's record is past its TTL but its tally is live, so the
	// record must be retained and the re-delivered vote deduplicated.
	setOffset(voteStoreTTL + time.Minute)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-b",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)

	// Voters 2..4 bring verified stake to exactly 400 >= 385; a
	// re-counted voter 0 would have produced a duplicate voter id and
	// wedged certificate building instead.
	for voterId := uint64(2); voterId <= 4; voterId++ {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	evt := testutil.RequireReceive(
		t,
		quorumCh,
		testutil.AsyncWait,
		"quorum reached with deduplicated stake",
	)
	quorum, ok := evt.Data.(EbQuorumEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(400), quorum.VerifiedStake)

	// Once the tally itself expires, the records go with it and the
	// same vote id is accepted fresh.
	setOffset(2*voteStoreTTL + 5*time.Minute)
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-b",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 0}},
		),
		1,
		"vote accepted fresh after its tally expired",
	)
}

// partialRegistryOpt removes the registered public keys for the given
// voter ids so their votes pass lenient validation as unverified.
func partialRegistryOpt(
	t *testing.T,
	unregistered ...uint64,
) func(*managerFixture, *VoteManagerConfig) {
	t.Helper()
	return func(f *managerFixture, cfg *VoteManagerConfig) {
		partial := maps.Clone(f.registryEntries)
		for _, member := range f.members {
			for _, voterId := range unregistered {
				if member.VoterId == voterId {
					delete(
						partial,
						hex.EncodeToString(member.PoolKeyHash),
					)
				}
			}
		}
		registry, err := NewVoterRegistry(partial)
		require.NoError(t, err)
		cfg.Registry = registry
	}
}

func TestVoteManagerRecordCapacityRejectsNewVotes(t *testing.T) {
	t.Parallel()

	// Voters 0..2 have no registered keys: their votes are unverified
	// and subject to the record admission cap.
	fixture := newManagerFixture(t, partialRegistryOpt(t, 0, 1, 2))
	fixture.mgr.maxRecords = 2
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	for voterId := range uint64(2) {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	// The ledger is full: a new unverified vote id is rejected
	// outright rather than evicting an existing record
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 2, 577, ebHash),
		),
	)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 2}},
		),
		"unverified vote beyond the record capacity is rejected",
	)
	// Recorded votes are unaffected
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{
			{SlotNo: 577, VoterId: 0},
			{SlotNo: 577, VoterId: 1},
		}),
		2,
	)
}

func TestVoteManagerVerifiedVoteBypassesRecordCapacity(t *testing.T) {
	t.Parallel()

	// Voters 0..2 have no registered keys; voter 3 stays registered.
	fixture := newManagerFixture(t, partialRegistryOpt(t, 0, 1, 2))
	fixture.mgr.maxRecords = 2
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// Unverified votes fill the record ledger
	for voterId := range uint64(2) {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, voterId, 577, ebHash),
			),
		)
	}
	// A verified vote must be admitted despite the full ledger:
	// unverifiable noise cannot starve the votes that feed
	// certificates
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 3, 577, ebHash),
		),
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 3}},
		),
		1,
		"verified vote admitted past the record capacity",
	)
	// Unverified votes remain capped
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 2, 577, ebHash),
		),
	)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 2}},
		),
		"unverified vote still rejected at capacity",
	)
}

func TestVoteManagerLocalVoteBypassesRecordCapacity(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	fixture.mgr.maxRecords = 1
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	// A peer vote fills the record ledger
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 0, 577, ebHash),
		),
	)

	// The node's own vote must bypass the capacity cap
	member := fixture.members[3]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[3]
	require.NotNil(t, key)
	fixture.mgr.EnableVoting(poolKeyHash, key)
	fixture.mgr.HandleEndorserBlock(577, ebHash)
	fixture.mgr.ObserveAnnouncement(
		577,
		lcommon.NewBlake2b256([]byte("announcing-rb")),
		ebHash,
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 577, VoterId: 3}},
		),
		1,
		"local vote emitted despite full record ledger",
	)
}

func TestVoteManagerSlotWindowRejects(t *testing.T) {
	t.Parallel()

	// The past bound is the vote window (offset after the EB produce slot at
	// which voting closes); the future bound is the clock-skew tolerance.
	const voteWindow = 10
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			cfg.SlotProvider = &fakeSlotProvider{slot: 1000}
			cfg.VoteWindowSlots = voteWindow
		},
	)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	for _, tc := range []struct {
		name     string
		slot     uint64
		voterId  uint64
		accepted bool
	}{
		{"past edge accepted", 1000 - voteWindow + 1, 0, true},
		{"vote window closed", 1000 - voteWindow, 1, false},
		{"future edge accepted", 1000 + slotWindowFutureTolerance, 2, true},
		{"too far future", 1000 + slotWindowFutureTolerance + 1, 3, false},
	} {
		require.NoError(
			t,
			fixture.mgr.HandleVote(
				"conn-a",
				fixture.makeVote(t, tc.voterId, tc.slot, ebHash),
			),
			tc.name,
		)
		raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{
			{SlotNo: tc.slot, VoterId: tc.voterId},
		})
		if tc.accepted {
			assert.Len(t, raws, 1, tc.name)
		} else {
			assert.Empty(t, raws, tc.name)
		}
	}

	// Out-of-window endorser blocks must not trigger local votes
	member := fixture.members[3]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[3]
	require.NotNil(t, key)
	fixture.mgr.EnableVoting(poolKeyHash, key)
	oldSlot := uint64(1000 - voteWindow - 100)
	fixture.mgr.HandleEndorserBlock(oldSlot, ebHash)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: oldSlot, VoterId: 3}},
		),
		"no local vote for an out-of-window endorser block",
	)
}

func TestVoteManagerRollbackAllowsReVoteForNewChain(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	ebHashA := lcommon.NewBlake2b256([]byte("eb-a"))
	ebHashB := lcommon.NewBlake2b256([]byte("eb-b"))
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 590, ebHashA),
		),
	)

	fixture.eventBus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(
			chain.ChainUpdateEventType,
			chain.ChainRollbackEvent{
				Point: ocommon.Point{Slot: 550},
			},
		),
	)
	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{{SlotNo: 590, VoterId: 1}},
		)) == 0
	}, testutil.AsyncWait, "rolled-back vote is pruned")

	// The rollback also dropped the dedup record, so a vote for the
	// replacement chain's endorser block is accepted rather than being
	// mistaken for equivocation.
	require.NoError(
		t,
		fixture.mgr.HandleVote(
			"conn-a",
			fixture.makeVote(t, 1, 590, ebHashB),
		),
	)
	raws := fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{{SlotNo: 590, VoterId: 1}},
	)
	require.Len(t, raws, 1)
	var stored lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &stored)
	require.NoError(t, err)
	assert.Equal(t, ebHashB, stored.EndorserBlockHash)
}

func TestVoteManagerRollbackRejectsInFlightLocalPrototypeVote(t *testing.T) {
	t.Parallel()

	params := newBlockingParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	member := fixture.members[3]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	// EnableVoting no longer resolves the epoch's committee (see
	// resolveOnChainKeyForPool), so it no longer risks blocking on the
	// params provider here; safe to call through the real public API.
	require.NoError(t, fixture.mgr.EnableVoting(poolKeyHash, fixture.keys[3]))
	rbHash := lcommon.NewBlake2b256([]byte("rolled-back-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("rolled-back-eb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.mgr.HandleEndorserBlock(577, ebHash)
	}()
	testutil.RequireReceive(
		t,
		params.entered,
		testutil.AsyncWait,
		"committee lookup",
	)
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: 550},
	})
	close(params.release)
	testutil.RequireReceive(t, done, testutil.AsyncWait, "in-flight emission exit")

	testutil.RequireNoReceive(
		t, emittedCh, 300*time.Millisecond,
		"rolled-back announcement must not publish a local vote",
	)
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: 577, VoterId: 3,
	}}))
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, rbHash)
	assert.NotContains(t, fixture.mgr.acquiredEbs, ebHash)
	assert.NotContains(t, fixture.mgr.votedAnnouncements, rbHash)
}

func TestVoteManagerRollbackRejectsInFlightResolvedPrototypeVote(t *testing.T) {
	t.Parallel()

	params := newBlockingParamsProvider()
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.ParamsProvider = params
		},
	)
	rbHash := lcommon.NewBlake2b256([]byte("rolled-back-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("rolled-back-eb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)
	vote := fixture.makePrototypeVote(t, 3, rbHash)
	done := make(chan error, 1)
	go func() {
		done <- fixture.mgr.HandlePrototypeVote("peer", vote)
	}()
	testutil.RequireReceive(
		t,
		params.entered,
		testutil.AsyncWait,
		"committee lookup",
	)
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: 550},
	})
	close(params.release)
	require.NoError(t, testutil.RequireReceive(
		t, done, testutil.AsyncWait, "resolved vote exit",
	))

	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: 577, VoterId: 3,
	}}))
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Empty(t, fixture.mgr.tallies)
	assert.Empty(t, fixture.mgr.voteRecords)
}
