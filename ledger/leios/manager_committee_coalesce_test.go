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
	"context"
	"errors"
	"maps"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

func (p *gatedParamsProvider) LeiosCommitteeParameters() (
	*big.Rat,
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
	return big.NewRat(1, 1), big.NewRat(7, 10), nil
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
		5*time.Second,
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
		5*time.Second,
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
		t, leader, 5*time.Second, "leader did not return",
	)
	require.NoError(t, leaderResult.err)
	require.NotNil(t, leaderResult.committee)
	for i, follower := range followers {
		got := testutil.RequireReceive(
			t, follower, 5*time.Second, "follower did not return",
		)
		require.NoErrorf(t, got.err, "follower %d", i)
		require.Samef(
			t, leaderResult.committee, got.committee,
			"follower %d must receive the leader's committee", i,
		)
	}
	require.Equal(
		t, 1, params.callCount(),
		"committee parameters must be resolved once per epoch, not once per caller",
	)
	require.Equal(
		t, 1, fixture.stake.callCount(),
		"the stake distribution must be read once per epoch, not once per caller",
	)
}

// Absence case: a single, uncontended cache miss still performs the
// computation exactly once -- coalescing must not turn a lone miss into zero
// computations (a caller that parks on a claim nobody owns) or two.
func TestVoteManagerCommitteeSingleMissComputesOnce(t *testing.T) {
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
		5*time.Second,
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
		t, leader, 5*time.Second, "leader did not return",
	)
	require.ErrorIs(t, leaderResult.err, snapshotErr)
	waiterResult := testutil.RequireReceive(
		t, waiter, 5*time.Second, "waiter was not released by the failure",
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
		5*time.Second,
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
		t, waiter, 5*time.Second, "waiter was not released at shutdown",
	)
	require.ErrorIs(t, waiterResult.err, ErrVoteManagerStopped)
	require.Nil(t, waiterResult.committee)

	// The leader still runs to completion; its result is simply no longer
	// wanted by anyone.
	params.releaseAll()
	leaderResult := testutil.RequireReceive(
		t, leader, 5*time.Second, "leader did not return after the stop",
	)
	require.NoError(t, leaderResult.err)
}

// A panic unwinding through the leader releases its waiters with an error and
// gives up the epoch's claim, rather than leaving the epoch permanently
// uncomputable with every later caller parked on a claim nobody owns. The
// panic itself still reaches the leader's caller: a fault in this node's own
// stake handling must not be laundered into a routine per-epoch error.
func TestVoteManagerCommitteePanicReleasesWaiters(t *testing.T) {
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
		5*time.Second,
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
		t, leaderPanic, 5*time.Second, "leader goroutine did not finish",
	)
	require.NotNil(
		t, got.recovered,
		"the panic must keep unwinding to the leader's caller",
	)
	waiterResult := testutil.RequireReceive(
		t, waiter, 5*time.Second, "waiter was not released by the panic",
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
		5*time.Second,
		"not every epoch reached the committee params provider",
	)

	_, err := fixture.mgr.CommitteeForEpoch(2000)
	require.ErrorIs(t, err, ErrCommitteeComputationBacklog)

	params.releaseAll()
	for i, leader := range leaders {
		got := testutil.RequireReceive(
			t, leader, 5*time.Second, "in-flight leader did not return",
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
		5*time.Second,
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
		t, leader, 5*time.Second, "leader did not return",
	)
	require.NoError(t, leaderResult.err)
	require.NotNil(t, leaderResult.committee)
	waiterResult := testutil.RequireReceive(
		t, waiter, 5*time.Second, "waiter was not released",
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
		5*time.Second,
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
		5*time.Second,
		"the new lifecycle's caller joined the stopped lifecycle's computation instead of computing",
	)

	params.releaseAll()
	nextResult := testutil.RequireReceive(
		t, next, 5*time.Second, "the new lifecycle's caller did not return",
	)
	require.NoError(t, nextResult.err)
	leaderResult := testutil.RequireReceive(
		t, leader, 5*time.Second, "leader did not return after the stop",
	)
	require.NoError(t, leaderResult.err)

	// The stopped lifecycle's leader must not have installed its result as
	// the new lifecycle's memo. Clearing the claim stops a new caller
	// joining it; only the generation bump stops it installing.
	memo := committeeMemoEntry(fixture.mgr, 5)
	require.NotNil(t, memo, "the new lifecycle's own computation must be memoized")
	require.Same(
		t,
		nextResult.committee,
		memo.committee,
		"the memo must hold the new lifecycle's committee, not the stopped leader's",
	)
}
