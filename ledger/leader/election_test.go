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

package leader

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/consensus/leaderthreshold"
	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ledgerpkg "github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sigmaAuditEpoch      = uint64(11)
	sigmaAuditStartSlot  = uint64(950400)
	sigmaAuditSlotCount  = uint64(6)
	sigmaAuditPoolStake  = uint64(59_000_000)
	sigmaAuditTotalStake = uint64(1_000_000_000)
)

// recordingStakeProvider records the snapshot epoch each half of the sigma
// ratio was queried with, so a test can prove both come from the SAME stake
// snapshot generation. A numerator taken from a later generation than the
// denominator is exactly the one-sided sigma drift reported.
type recordingStakeProvider struct {
	poolStakeEpochs  []uint64
	totalStakeEpochs []uint64
	poolStake        uint64
	totalStake       uint64
}

// GetPoolAndTotalActiveStake records the snapshot epoch for BOTH halves on
// every call. Since the pair is read through one method, so the
// two recorded slices are necessarily the same length and carry the same
// epochs -- which is itself the property TestComputeScheduleDrawsSigmaInputs\
// FromSameSnapshotEpoch asserts.
func (p *recordingStakeProvider) GetPoolAndTotalActiveStake(
	epoch uint64,
	_ []byte,
) (uint64, uint64, error) {
	p.poolStakeEpochs = append(p.poolStakeEpochs, epoch)
	p.totalStakeEpochs = append(p.totalStakeEpochs, epoch)
	return p.poolStake, p.totalStake, nil
}

// sigmaAuditEpochProvider is a minimal EpochInfoProvider. exactCoeff, when
// non-nil, also makes it an ActiveSlotCoeffRatProvider.
type sigmaAuditEpochProvider struct {
	exactCoeff *big.Rat
	floatCoeff float64
}

func (p *sigmaAuditEpochProvider) CurrentEpoch() uint64 { return sigmaAuditEpoch }

func (p *sigmaAuditEpochProvider) EpochNonce(context.Context, uint64) []byte {
	return coeffTestNonce
}

func (p *sigmaAuditEpochProvider) NextEpochNonceReadyEpoch() (uint64, bool) {
	return 0, false
}

func (p *sigmaAuditEpochProvider) EpochSlotRange(
	uint64,
) (EpochSlotRange, error) {
	return EpochSlotRange{
		StartSlot: sigmaAuditStartSlot,
		SlotCount: sigmaAuditSlotCount,
	}, nil
}

func (p *sigmaAuditEpochProvider) EpochForSlot(uint64) (uint64, error) {
	return sigmaAuditEpoch, nil
}

func (p *sigmaAuditEpochProvider) ActiveSlotCoeff() float64 {
	return p.floatCoeff
}

func (p *sigmaAuditEpochProvider) ConsensusModeForEpoch(
	uint64,
) (consensus.ConsensusMode, error) {
	return consensus.ConsensusModeCPraos, nil
}

// ActiveSlotCoeffRat is only reachable through the optional
// ActiveSlotCoeffRatProvider assertion, and only when exactCoeff is set.
func (p *sigmaAuditEpochProvider) ActiveSlotCoeffRat() *big.Rat {
	return p.exactCoeff
}

func newSigmaAuditElection(
	stake StakeDistributionProvider,
	epochs *sigmaAuditEpochProvider,
	logger *slog.Logger,
) *Election {
	return NewElection(
		coeffTestPoolID,
		vrfSeedSource(coeffTestVRFSeed),
		stake,
		epochs,
		nil,
		logger,
	)
}

// TestComputeScheduleDrawsSigmaInputsFromSameSnapshotEpoch proves the pool
// stake numerator and the total active stake denominator are both read from
// the Praos-selected stake snapshot generation for the scheduled epoch
// (praos.StakeSnapshotEpoch, i.e. mark[E-1] = stake at end of E-2, the
// reference node's "set" snapshot / nesPd). Mixing generations would inflate
// or deflate sigma one-sidedly.
func TestComputeScheduleDrawsSigmaInputsFromSameSnapshotEpoch(t *testing.T) {
	stake := &recordingStakeProvider{
		poolStake:  sigmaAuditPoolStake,
		totalStake: sigmaAuditTotalStake,
	}
	election := newSigmaAuditElection(
		stake,
		&sigmaAuditEpochProvider{floatCoeff: 0.05},
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	wantSnapshotEpoch := praos.StakeSnapshotEpoch(sigmaAuditEpoch)
	require.Equal(t, []uint64{wantSnapshotEpoch}, stake.poolStakeEpochs)
	require.Equal(t, []uint64{wantSnapshotEpoch}, stake.totalStakeEpochs)
	require.Equal(t, sigmaAuditPoolStake, schedule.PoolStake)
	require.Equal(t, sigmaAuditTotalStake, schedule.TotalStake)
}

// TestComputeScheduleWipesTheSeedItTook proves the election holds the VRF seed
// only for the duration of a schedule computation: every copy the seed source
// handed out is zeroed once the computation returns, and the election keeps
// no copy of its own to scan for.
func TestComputeScheduleWipesTheSeedItTook(t *testing.T) {
	t.Parallel()

	var issued [][]byte
	source := func() []byte {
		seed := append([]byte(nil), coeffTestVRFSeed...)
		issued = append(issued, seed)
		return seed
	}
	election := NewElection(
		coeffTestPoolID,
		source,
		&recordingStakeProvider{
			poolStake:  sigmaAuditPoolStake,
			totalStake: sigmaAuditTotalStake,
		},
		&sigmaAuditEpochProvider{floatCoeff: 0.05},
		nil,
		slog.New(slog.DiscardHandler),
	)
	require.Empty(t, issued, "construction must not take a seed")

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	require.Len(t, issued, 1)
	require.Equal(
		t,
		make([]byte, len(coeffTestVRFSeed)),
		issued[0],
		"the seed copy must be wiped after the schedule is computed",
	)
}

// TestComputeSchedulePrefersExactGenesisActiveSlotCoeff proves the election
// threads the exact Shelley genesis active slot coefficient into the schedule
// calculation when the epoch provider can supply it, instead of the float64
// value returned by ActiveSlotCoeff().
func TestComputeSchedulePrefersExactGenesisActiveSlotCoeff(t *testing.T) {
	exact := big.NewRat(1, 3)
	election := newSigmaAuditElection(
		&recordingStakeProvider{
			poolStake:  sigmaAuditPoolStake,
			totalStake: sigmaAuditTotalStake,
		},
		&sigmaAuditEpochProvider{
			exactCoeff: exact,
			floatCoeff: 1.0 / 3.0,
		},
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	require.NotNil(t, schedule.Threshold)

	want, err := leaderthreshold.Threshold(
		sigmaAuditPoolStake,
		sigmaAuditTotalStake,
		exact,
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.Equal(t, 0, schedule.Threshold.Cmp(want),
		"threshold must come from the exact genesis rational, not float64")
}

// TestComputeScheduleWithoutExactCoeffUsesFloatFallback keeps providers that
// cannot supply an exact rational working unchanged.
func TestComputeScheduleWithoutExactCoeffUsesFloatFallback(t *testing.T) {
	election := newSigmaAuditElection(
		&recordingStakeProvider{
			poolStake:  sigmaAuditPoolStake,
			totalStake: sigmaAuditTotalStake,
		},
		&sigmaAuditEpochProvider{floatCoeff: 0.05},
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	require.NotNil(t, schedule.Threshold)

	want, err := leaderthreshold.Threshold(
		sigmaAuditPoolStake,
		sigmaAuditTotalStake,
		new(big.Rat).SetFloat64(0.05),
		consensus.ConsensusModeCPraos,
	)
	require.NoError(t, err)
	require.Equal(t, 0, schedule.Threshold.Cmp(want))
}

// TestComputeScheduleLogsAuditableSigmaInputs pins the "leader schedule
// calculated" record as a single, self-contained audit of every input to the
// leader check, so a reported schedule divergence can be diffed against the
// reference node's `query stake-snapshot` / `query protocol-state` without
// re-running the node with extra instrumentation.
func TestComputeScheduleLogsAuditableSigmaInputs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	exact := big.NewRat(1, 20)
	election := newSigmaAuditElection(
		&recordingStakeProvider{
			poolStake:  sigmaAuditPoolStake,
			totalStake: sigmaAuditTotalStake,
		},
		&sigmaAuditEpochProvider{exactCoeff: exact, floatCoeff: 0.05},
		logger,
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	record := findLogRecord(t, &buf, "leader schedule calculated")
	require.EqualValues(t, sigmaAuditEpoch, record["epoch"])
	require.EqualValues(
		t,
		praos.StakeSnapshotEpoch(sigmaAuditEpoch),
		record["snapshot_epoch"],
	)
	require.Equal(t, "mark", record["snapshot_type"])
	require.EqualValues(t, sigmaAuditStartSlot, record["epoch_start_slot"])
	require.EqualValues(t, sigmaAuditSlotCount, record["epoch_slot_count"])
	require.EqualValues(t, sigmaAuditPoolStake, record["pool_stake"])
	require.EqualValues(t, sigmaAuditTotalStake, record["total_stake"])
	require.Equal(t, hex.EncodeToString(coeffTestNonce), record["epoch_nonce"])
	require.Equal(t, "1/20", record["active_slot_coeff"])
	require.Equal(t, "cpraos", record["consensus_mode"])
	require.Equal(
		t,
		schedule.Threshold.Text(16),
		record["leader_threshold"],
	)
}

// findLogRecord returns the first JSON log record whose "msg" matches.
func findLogRecord(
	t *testing.T,
	buf *bytes.Buffer,
	msg string,
) map[string]any {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			continue
		}
		if record["msg"] == msg {
			return record
		}
	}
	require.NoError(t, scanner.Err())
	t.Fatalf("no log record with msg %q in:\n%s", msg, buf.String())
	return nil
}

// generationStakeProvider models a store whose snapshot is re-captured
// between reads: every call returns the NEXT generation. Both generations
// carry the same sigma with different absolute values, so a pair assembled
// from two different calls is detectable as a sigma matching neither.
type generationStakeProvider struct {
	calls int
}

const (
	sigmaGenOnePoolStake  = uint64(59_000_000)
	sigmaGenOneTotalStake = uint64(1_000_000_000)
)

func (p *generationStakeProvider) GetPoolAndTotalActiveStake(
	_ uint64,
	_ []byte,
) (uint64, uint64, error) {
	generation := uint64(1 << p.calls)
	p.calls++
	return sigmaGenOnePoolStake * generation,
		sigmaGenOneTotalStake * generation,
		nil
}

// TestComputeScheduleReadsSigmaPairInOneProviderCall is the regression test
// driven through the real schedule computation.
//
// computeSchedule used to call GetPoolStake and GetTotalActiveStake in
// sequence (election.go:773 and :804), each opening its own db.MetadataTxn in
// the forging adapter. A snapshot re-capture landing between the two produced
// a schedule whose numerator and denominator came from different writes: a
// sigma reproducible from neither snapshot alone.
//
// The provider here advances a generation on every call, which is what a
// re-capture between reads looks like from the caller's side. Two assertions
// pin the fix:
//
//   - exactly ONE paired read happens, so there is no window between halves;
//   - the schedule's stake pair is generation one entire, so sigma is exact.
//
// Under the two-call code the numerator comes from generation one and the
// denominator from generation two, halving sigma, and both assertions fail.
func TestComputeScheduleReadsSigmaPairInOneProviderCall(t *testing.T) {
	stake := &generationStakeProvider{}
	election := newSigmaAuditElection(
		stake,
		&sigmaAuditEpochProvider{floatCoeff: 0.05},
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(), sigmaAuditEpoch,
	)
	require.NoError(t, err)
	require.NotNil(t, schedule)

	require.Equal(t, 1, stake.calls,
		"the sigma numerator and denominator must be read in ONE paired "+
			"call; a second read is a torn-sigma window (dingo #3815)")

	require.Equal(t, sigmaGenOnePoolStake, schedule.PoolStake,
		"numerator must come from the first snapshot generation")
	require.Equal(t, sigmaGenOneTotalStake, schedule.TotalStake,
		"denominator must come from the SAME generation as the numerator")

	// The ratio itself, cross-multiplied so the check is exact rather than
	// float. This is the assertion that fails on a torn pair even if the
	// absolute values above were ever relaxed.
	require.Equal(t,
		schedule.PoolStake*sigmaGenOneTotalStake,
		schedule.TotalStake*sigmaGenOnePoolStake,
		"sigma must equal the fixture's ratio exactly",
	)
}

// electionTestNonce is a 32-byte epoch nonce for election tests.
var electionTestNonce = func() []byte {
	nonce := make([]byte, 32)
	for i := range nonce {
		nonce[i] = byte(i + 42)
	}
	return nonce
}()

func makeDistinctNonce(nonce []byte) []byte {
	distinct := append([]byte(nil), nonce...)
	if len(distinct) > 0 {
		distinct[0] ^= 0xff
	}
	return distinct
}

// electionTestVRFSeed is a 32-byte VRF seed for election tests.
var electionTestVRFSeed = []byte("election_vrf_seed_32_bytes_ok!!!")

// electionSlotsPerEpoch is a small epoch size to keep VRF computation fast
// in tests. VRF Prove is computationally expensive (~0.2s per call), so we
// use a small number of slots to keep test execution reasonable.
const electionSlotsPerEpoch = 10

// mockStakeProvider implements StakeDistributionProvider for testing
type mockStakeProvider struct {
	poolStakes map[string]uint64
	totalStake uint64
	err        error
}

func newMockStakeProvider() *mockStakeProvider {
	return &mockStakeProvider{
		poolStakes: make(map[string]uint64),
	}
}

func (m *mockStakeProvider) GetPoolAndTotalActiveStake(
	_ uint64,
	poolKeyHash []byte,
) (uint64, uint64, error) {
	if m.err != nil {
		return 0, 0, m.err
	}
	return m.poolStakes[string(poolKeyHash)], m.totalStake, nil
}

// mockEpochProvider implements EpochInfoProvider for testing
type mockEpochProvider struct {
	currentEpoch    atomic.Uint64
	epochNonceMu    sync.RWMutex
	epochNonces     map[uint64][]byte
	slotsPerEpoch   uint64
	activeSlotCoeff float64
	nextEpochReady  atomic.Uint64
	// epochForSlot, when set, overrides EpochForSlot's default fixed
	// division so tests can model variable epoch lengths or simulate
	// past-horizon errors.
	epochForSlot func(slot uint64) (uint64, error)
	// epochSlotRange, when set, overrides EpochSlotRange's default fixed
	// range so tests can model Byron-era offsets or variable epoch lengths.
	epochSlotRange func(epoch uint64) (EpochSlotRange, error)
	// consensusModeErr, when set, makes ConsensusModeForEpoch fail so tests
	// can model an unresolvable era forecast.
	consensusModeErr error
}

func newMockEpochProvider() *mockEpochProvider {
	m := &mockEpochProvider{
		slotsPerEpoch:   electionSlotsPerEpoch,
		activeSlotCoeff: 0.05,
		epochNonces:     make(map[uint64][]byte),
	}
	m.currentEpoch.Store(10)
	m.SetEpochNonce(electionTestNonce)
	return m
}

func (m *mockEpochProvider) CurrentEpoch() uint64 {
	return m.currentEpoch.Load()
}

func (m *mockEpochProvider) EpochNonce(_ context.Context, epoch uint64) []byte {
	m.epochNonceMu.RLock()
	nonce, ok := m.epochNonces[epoch]
	m.epochNonceMu.RUnlock()
	if !ok || len(nonce) == 0 {
		return nil
	}
	return append([]byte(nil), nonce...)
}

func (m *mockEpochProvider) NextEpochNonceReadyEpoch() (uint64, bool) {
	nextEpoch := m.nextEpochReady.Load()
	if nextEpoch == 0 {
		return 0, false
	}
	return nextEpoch, true
}

func (m *mockEpochProvider) SlotsPerEpoch() uint64 {
	return m.slotsPerEpoch
}

func (m *mockEpochProvider) EpochSlotRange(
	epoch uint64,
) (EpochSlotRange, error) {
	if m.epochSlotRange != nil {
		return m.epochSlotRange(epoch)
	}
	if m.slotsPerEpoch == 0 {
		return EpochSlotRange{}, errors.New("slotsPerEpoch unset")
	}
	return EpochSlotRange{
		StartSlot: epoch * m.slotsPerEpoch,
		SlotCount: m.slotsPerEpoch,
	}, nil
}

func (m *mockEpochProvider) EpochForSlot(slot uint64) (uint64, error) {
	if m.epochForSlot != nil {
		return m.epochForSlot(slot)
	}
	if m.slotsPerEpoch == 0 {
		return 0, errors.New("slotsPerEpoch unset")
	}
	return slot / m.slotsPerEpoch, nil
}

func (m *mockEpochProvider) ActiveSlotCoeff() float64 {
	return m.activeSlotCoeff
}

func (m *mockEpochProvider) ConsensusModeForEpoch(
	epoch uint64,
) (consensus.ConsensusMode, error) {
	if m.consensusModeErr != nil {
		return 0, m.consensusModeErr
	}
	return consensus.ConsensusModeTPraos, nil
}

func (m *mockEpochProvider) SetEpochNonce(nonce []byte) {
	m.SetEpochNonceForEpoch(m.CurrentEpoch(), nonce)
}

func (m *mockEpochProvider) SetEpochNonceForEpoch(epoch uint64, nonce []byte) {
	m.epochNonceMu.Lock()
	defer m.epochNonceMu.Unlock()
	if len(nonce) == 0 {
		delete(m.epochNonces, epoch)
		return
	}
	m.epochNonces[epoch] = append([]byte(nil), nonce...)
}

type mockScheduleStore struct {
	mu        sync.RWMutex
	schedules map[string]*Schedule
	loadErr   error
	saveErr   error
}

func newMockScheduleStore() *mockScheduleStore {
	return &mockScheduleStore{
		schedules: make(map[string]*Schedule),
	}
}

func (m *mockScheduleStore) LoadSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) (*Schedule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return m.schedules[m.key(epoch, poolId)], nil
}

func (m *mockScheduleStore) SaveSchedule(schedule *Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.schedules[m.key(schedule.Epoch, schedule.PoolId)] = schedule
	return nil
}

func (m *mockScheduleStore) key(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) string {
	return fmt.Sprintf("%d:%x", epoch, poolId[:])
}

func makeElectionNonce(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, 32)
}

// waitForSchedule polls until CurrentSchedule returns non-nil, or fails
// the test after timeout. VRF computation is expensive (~0.2s per slot),
// so we use a generous timeout.
func waitForSchedule(
	t *testing.T,
	election *Election,
	timeout time.Duration,
) *Schedule {
	t.Helper()
	var schedule *Schedule
	require.Eventually(t, func() bool {
		schedule = election.CurrentSchedule()
		return schedule != nil
	}, timeout, 50*time.Millisecond, "schedule should be computed")
	require.NotNil(t, schedule)
	return schedule
}

// vrfSeedSource serves a private copy of seed per call, as the node's
// credentials do, so a test can tell whether the election wipes what it took.
func vrfSeedSource(seed []byte) func() []byte {
	return func() []byte { return append([]byte(nil), seed...) }
}

func TestNewElection(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))
	vrfKey := vrfSeedSource(electionTestVRFSeed)

	stakeProvider := newMockStakeProvider()
	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfKey,
		stakeProvider,
		epochProvider,
		eventBus,
		nil, // nil logger uses default
	)

	require.NotNil(t, election)
	assert.Equal(t, poolId, election.poolId)
	assert.NotNil(t, election.logger)
}

func TestElectionStartStop(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	// Start should succeed
	err := election.Start(context.Background())
	require.NoError(t, err)

	// Start again should be idempotent
	err = election.Start(context.Background())
	require.NoError(t, err)

	// Stop should succeed
	err = election.Stop()
	require.NoError(t, err)

	// Stop again should be idempotent
	err = election.Stop()
	require.NoError(t, err)
}

// TestElectionStopPreventsStaleMonitorFromStoppingALaterStart guards a
// real bug: the ctx-cancellation monitor goroutine Start launches used to
// be untracked by e.wg, so a completed Stop() call could return while
// that goroutine was still alive, watching the now-superseded ctx/stopCh
// from that Start generation. A later Start() on the same *Election (a
// supported, idempotent-per-Stop pattern per TestElectionStartStop above)
// builds a fresh ctx/stopCh but does nothing about a stale monitor left
// over from the previous generation -- if that stale monitor's own parent
// context were ever cancelled afterward, it would call e.Stop() on the
// new, currently-running generation it has no business touching.
func TestElectionStopPreventsStaleMonitorFromStoppingALaterStart(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	parentA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	require.NoError(t, election.Start(parentA))
	require.NoError(t, election.Stop())

	parentB := t.Context()
	require.NoError(t, election.Start(parentB))

	// Cancelling the FIRST generation's now-unrelated parent must never
	// affect the second, currently-running generation -- if Stop above
	// left generation 1's monitor goroutine alive, this would eventually
	// call e.Stop() on generation 2.
	cancelA()

	require.Never(t, func() bool {
		election.mu.RLock()
		defer election.mu.RUnlock()
		return !election.running
	}, 200*time.Millisecond, 10*time.Millisecond)

	require.NoError(t, election.Stop())
}

// TestElectionStopDoesNotDeadlockOnMonitorSelectRace guards the sharper,
// more severe consequence of the same gap: once the monitor goroutine is
// tracked in e.wg (so Stop actually waits for it), a select that picks its
// <-ctx.Done() case instead of <-stopCh purely by luck -- both channels
// can be simultaneously ready by the time the goroutine is actually
// scheduled, since Stop closes stopCh and cancels ctx back to back with
// no yield point in between, and Go's select has no case-priority when
// more than one is ready -- would call e.Stop() a second, concurrent
// time. That second call's own e.wg.Wait() would then deadlock forever
// waiting for this very goroutine to finish, which it never will: it is
// itself blocked inside that same e.Stop() call. Repeats many Start/Stop
// cycles (each call is independent, so a fresh *Election every time)
// specifically to land in that narrow race window rather than relying on
// a single attempt to happen to hit it.
func TestElectionStopDoesNotDeadlockOnMonitorSelectRace(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000
	epochProvider := newMockEpochProvider()

	for i := range 200 {
		eventBus := event.NewEventBus(nil, nil)
		election := NewElection(
			poolId,
			vrfSeedSource(electionTestVRFSeed),
			stakeProvider,
			epochProvider,
			eventBus,
			slog.Default(),
		)
		require.NoError(t, election.Start(context.Background()))

		stopDone := make(chan struct{})
		go func() {
			defer close(stopDone)
			_ = election.Stop()
		}()

		select {
		case <-stopDone:
		case <-time.After(testutil.AsyncWait):
			t.Fatalf("Stop() deadlocked on iteration %d", i)
		}
		eventBus.Stop()
	}
}

// blockingStakeProvider wraps mockStakeProvider's GetPoolStake so a test can
// deterministically pin a schedule computation in flight: the first call
// signals started (closing it) and then blocks until release is closed,
// rather than racing a real computation's completion against a timed Stop
// call.
type blockingStakeProvider struct {
	*mockStakeProvider
	started   chan struct{}
	startOnce sync.Once
	release   chan struct{}
}

func (b *blockingStakeProvider) GetPoolAndTotalActiveStake(
	epoch uint64,
	poolKeyHash []byte,
) (uint64, uint64, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.release
	return b.mockStakeProvider.GetPoolAndTotalActiveStake(epoch, poolKeyHash)
}

// TestElectionStopWaitsForInFlightScheduleComputation guards a real bug:
// Stop used to signal its background goroutines to exit (closing stopCh/
// computeCh, cancelling ctx, unsubscribing) and return immediately,
// without waiting for a schedule computation already in flight to
// actually finish. Schedule computation reads stakeProvider/epochProvider,
// which node.go's initBlockForger binds to whatever ledgerState exists at
// construction time -- so on the live database restore/truncate path
// (node_lifecycle.go), a computation still running when Stop returns could
// keep reading from that ledgerState after the caller closes it moments
// later. Start's own initial-epoch compute request pins the computation in
// flight via blockingStakeProvider, so this needs no timing assumptions
// about a real computation's speed.
func TestElectionStopWaitsForInFlightScheduleComputation(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	inner := newMockStakeProvider()
	inner.totalStake = 10000
	inner.poolStakes[string(poolId[:])] = 1000
	blocking := &blockingStakeProvider{
		mockStakeProvider: inner,
		started:           make(chan struct{}),
		release:           make(chan struct{}),
	}

	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		blocking,
		epochProvider,
		eventBus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, election.Start(context.Background()))

	// Start's own initial schedule-compute request reaches GetPoolStake
	// almost immediately.
	select {
	case <-blocking.started:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("schedule computation never started")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- election.Stop() }()

	select {
	case <-stopDone:
		t.Fatal(
			"Stop returned before the in-flight schedule computation finished",
		)
	case <-time.After(200 * time.Millisecond):
	}

	close(blocking.release)

	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("Stop did not return after the in-flight computation finished")
	}
}

func TestElectionScheduleEarlyEpochs(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(1)
	epochProvider.SetEpochNonceForEpoch(1, electionTestNonce)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	assert.Equal(t, uint64(1), schedule.Epoch)
}

func TestElectionUsesEpochSlotRangeForSchedule(t *testing.T) {
	const (
		epoch              = uint64(299)
		preprodEpochStart  = uint64(127_526_400)
		testEpochSlotCount = uint64(4)
	)

	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(epoch)
	epochProvider.activeSlotCoeff = 1.0
	epochProvider.SetEpochNonceForEpoch(epoch, electionTestNonce)
	epochProvider.epochSlotRange = func(gotEpoch uint64) (EpochSlotRange, error) {
		if gotEpoch != epoch {
			return EpochSlotRange{}, fmt.Errorf(
				"got epoch slot range request for epoch %d, want %d",
				gotEpoch,
				epoch,
			)
		}
		return EpochSlotRange{
			StartSlot: preprodEpochStart,
			SlotCount: testEpochSlotCount,
		}, nil
	}

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 30*time.Second)
	require.Equal(t, epoch, schedule.Epoch)
	slots := schedule.LeaderSlotsSnapshot()
	require.NotEmpty(t, slots)
	for _, slot := range slots {
		assert.GreaterOrEqual(t, slot, preprodEpochStart)
		assert.Less(t, slot, preprodEpochStart+testEpochSlotCount)
	}
}

func TestElectionLoadsPersistedSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	store := newMockScheduleStore()
	persisted := NewSchedule(10, poolId, 1000, 10000, electionTestNonce)
	persisted.AddLeaderSlot(101)
	persisted.AddLeaderSlot(104)
	require.NoError(t, store.SaveSchedule(persisted))

	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000
	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 5*time.Second)
	assert.Equal(
		t,
		persisted.LeaderSlotsSnapshot(),
		schedule.LeaderSlotsSnapshot(),
	)
}

func TestElectionIgnoresStalePersistedSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	store := newMockScheduleStore()
	persisted := NewSchedule(
		10,
		poolId,
		1_000_000,
		1_000_000,
		makeElectionNonce(0x44),
	)
	persisted.AddLeaderSlot(999)
	require.NoError(t, store.SaveSchedule(persisted))

	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.activeSlotCoeff = 1.0
	epochProvider.SetEpochNonce(makeElectionNonce(0x55))

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 30*time.Second)
	require.NotNil(t, schedule)
	assert.NotEqual(t, []uint64{999}, schedule.LeaderSlotsSnapshot())
	assert.True(t, bytes.Equal(makeElectionNonce(0x55), schedule.EpochNonce))

	var persistedAfterLoad *Schedule
	require.Eventually(t, func() bool {
		var err error
		persistedAfterLoad, err = store.LoadSchedule(10, poolId)
		require.NoError(t, err)
		return persistedAfterLoad != nil &&
			bytes.Equal(makeElectionNonce(0x55), persistedAfterLoad.EpochNonce)
	}, testutil.AsyncWait, 50*time.Millisecond)
}

func TestElectionPersistsComputedSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	store := newMockScheduleStore()
	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 30*time.Second)
	require.NotNil(t, schedule)

	var persisted *Schedule
	require.Eventually(t, func() bool {
		var err error
		persisted, err = store.LoadSchedule(schedule.Epoch, poolId)
		require.NoError(t, err)
		return persisted != nil
	}, testutil.AsyncWait, 50*time.Millisecond)
	require.NotNil(t, persisted)
	assert.Equal(
		t,
		schedule.LeaderSlotsSnapshot(),
		persisted.LeaderSlotsSnapshot(),
	)
}

func TestElectionPrecomputesNextEpochAtStartupWhenNonceReady(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	store := newMockScheduleStore()
	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	epochProvider.nextEpochReady.Store(11)
	electionTestNonce11 := makeDistinctNonce(electionTestNonce)
	epochProvider.SetEpochNonceForEpoch(11, electionTestNonce11)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	waitForSchedule(t, election, 30*time.Second)

	require.Eventually(t, func() bool {
		schedule := election.ScheduleForEpoch(11)
		return schedule != nil &&
			bytes.Equal(electionTestNonce11, schedule.EpochNonce)
	}, testutil.AsyncWait, 100*time.Millisecond,
		"next epoch schedule should be precomputed at startup")

	require.Eventually(t, func() bool {
		schedule, err := store.LoadSchedule(11, poolId)
		require.NoError(t, err)
		return schedule != nil &&
			bytes.Equal(electionTestNonce11, schedule.EpochNonce)
	}, testutil.AsyncWait, 50*time.Millisecond,
		"next epoch schedule should be persisted at startup")
}

func TestElectionZeroPoolStake(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	// No pool stake set (zero)

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	electionTestNonce11 := makeDistinctNonce(electionTestNonce)
	epochProvider.SetEpochNonceForEpoch(11, electionTestNonce11)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// With zero pool stake, computeSchedule returns nil (no VRF needed).
	// Verify the schedule stays nil after the background goroutine has
	// time to process the request.
	assert.Never(t, func() bool {
		return election.CurrentSchedule() != nil
	}, 500*time.Millisecond, 50*time.Millisecond,
		"schedule should remain nil with zero pool stake")
}

func TestElectionShouldProduceBlock(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000 // 100% stake

	// Use high active slot coefficient (90%) with small epoch to ensure
	// we reliably get leader slots despite the small sample size.
	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	electionTestNonce11 := makeDistinctNonce(electionTestNonce)
	epochProvider.SetEpochNonceForEpoch(11, electionTestNonce11)
	epochProvider.activeSlotCoeff = 0.9

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	assert.Greater(t, schedule.SlotCount(), 0,
		"pool with 100%% stake and f=0.9 should have at least one leader slot")

	// Verify ShouldProduceBlock returns true for an actual leader slot
	if schedule.SlotCount() > 0 {
		leaderSlot := schedule.LeaderSlots[0]
		assert.True(t, election.ShouldProduceBlock(leaderSlot),
			"ShouldProduceBlock should return true for a known leader slot")
	}
}

func TestElectionNextLeaderSlot(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	epochProvider.activeSlotCoeff = 0.9 // High f for reliable election

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	// No schedule - should return 0, false
	slot, found := election.NextLeaderSlot(0)
	assert.Equal(t, uint64(0), slot)
	assert.False(t, found)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	require.Greater(t, schedule.SlotCount(), 0,
		"should have leader slots with 100%% stake and f=0.9")

	epochStart := uint64(10) * electionSlotsPerEpoch
	slot, found = election.NextLeaderSlot(epochStart)
	assert.True(t, found, "should find a leader slot in the epoch")
	assert.GreaterOrEqual(t, slot, epochStart,
		"leader slot should be at or after epoch start")
}

// TestElectionShouldProduceBlock_UsesEpochForSlot pins that slot→epoch
// lookup goes through EpochInfoProvider.EpochForSlot rather than fixed
// division, so a network with non-uniform epoch lengths or non-zero
// epoch starts still selects the correct cached schedule.
func TestElectionShouldProduceBlock_UsesEpochForSlot(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	epochProvider := newMockEpochProvider()

	// A network where epoch 10 doesn't start at slot 10*slotsPerEpoch.
	// Slot 5_000 lands in epoch 10; slot 5_100 lands in epoch 11. Fixed
	// division (slot/10 = 500 / 510) would land both in the wrong epoch.
	const (
		targetEpoch uint64 = 10
		nextEpoch   uint64 = 11
		inEpoch10   uint64 = 5_000
		inEpoch11   uint64 = 5_100
	)
	epochProvider.epochForSlot = func(slot uint64) (uint64, error) {
		switch {
		case slot >= inEpoch11:
			return nextEpoch, nil
		case slot >= inEpoch10:
			return targetEpoch, nil
		default:
			return 0, errors.New("before any known epoch")
		}
	}

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	// Seed the cache with a schedule for epoch 10 listing inEpoch10 as a
	// leader slot, without going through async compute.
	sched := NewSchedule(
		targetEpoch, poolId, 1, 1, electionTestNonce,
	)
	sched.AddLeaderSlot(inEpoch10)
	election.mu.Lock()
	election.schedules = map[uint64]*Schedule{targetEpoch: sched}
	election.mu.Unlock()

	assert.True(t, election.ShouldProduceBlock(inEpoch10),
		"slot resolving to epoch 10 must hit the cached schedule")
	assert.False(t, election.ShouldProduceBlock(inEpoch11),
		"slot resolving to epoch 11 must miss the cached schedule")
}

// TestElectionShouldProduceBlock_ReturnsFalseOnEpochResolveError pins
// the safe-default: when EpochForSlot can't resolve (slot outside the
// known range), the producer declines rather than running with an
// incorrect epoch index. The compute path is not engaged because we
// don't know which epoch's schedule to request.
func TestElectionShouldProduceBlock_ReturnsFalseOnEpochResolveError(
	t *testing.T,
) {
	poolId := lcommon.PoolKeyHash{}
	epochProvider := newMockEpochProvider()
	epochProvider.epochForSlot = func(uint64) (uint64, error) {
		return 0, errors.New("past horizon")
	}

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		newMockStakeProvider(),
		epochProvider,
		eventBus,
		slog.Default(),
	)

	assert.False(t, election.ShouldProduceBlock(999_999),
		"unresolvable slot must not produce")
}

// TestElectionNextLeaderSlot_UsesEpochForSlot pins that NextLeaderSlot
// queries EpochForSlot rather than fixed division when picking which
// epoch's cached schedule to scan.
func TestElectionNextLeaderSlot_UsesEpochForSlot(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	epochProvider := newMockEpochProvider()
	const (
		targetEpoch uint64 = 10
		epochStart  uint64 = 5_000
		leaderSlot  uint64 = 5_020
	)
	epochProvider.epochForSlot = func(slot uint64) (uint64, error) {
		if slot < epochStart {
			return 0, errors.New("before epoch start")
		}
		return targetEpoch, nil
	}

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		newMockStakeProvider(),
		epochProvider,
		eventBus,
		slog.Default(),
	)
	sched := NewSchedule(
		targetEpoch, poolId, 1, 1, electionTestNonce,
	)
	sched.AddLeaderSlot(leaderSlot)
	election.mu.Lock()
	election.schedules = map[uint64]*Schedule{targetEpoch: sched}
	election.mu.Unlock()

	got, found := election.NextLeaderSlot(epochStart)
	assert.True(t, found, "leader slot in resolved epoch must be found")
	assert.Equal(t, leaderSlot, got)

	// Unresolvable slot: helper returns 0,false rather than scanning the
	// wrong epoch's schedule.
	got, found = election.NextLeaderSlot(0)
	assert.False(t, found, "unresolvable slot must yield no result")
	assert.Equal(t, uint64(0), got)
}

func TestElectionEpochTransition(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	electionTestNonce11 := makeDistinctNonce(electionTestNonce)
	epochProvider.SetEpochNonceForEpoch(11, electionTestNonce11)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Initial schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	assert.Equal(t, uint64(10), schedule.Epoch)

	// Simulate epoch transition by updating provider and sending event
	epochProvider.currentEpoch.Store(11)

	// Publish epoch transition event
	eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{
				PreviousEpoch: 10,
				NewEpoch:      11,
				BoundarySlot:  110,
				EpochNonce:    electionTestNonce,
			},
		),
	)

	// Poll for event to be processed with generous timeout.
	// VRF computation is expensive (~0.2s per slot), so with
	// electionSlotsPerEpoch slots the recalculation takes a few seconds.
	vrfStart := time.Now()
	require.Eventually(t, func() bool {
		schedule := election.CurrentSchedule()
		ready := schedule != nil && schedule.Epoch == 11
		if ready {
			t.Logf(
				"VRF schedule recalculation took %s",
				time.Since(vrfStart),
			)
		}
		return ready
	}, testutil.AsyncWait, 100*time.Millisecond, "schedule should update to epoch 11")

	// Schedule should be updated to new epoch
	schedule = election.CurrentSchedule()
	require.NotNil(t, schedule)
	assert.Equal(t, uint64(11), schedule.Epoch)
}

func TestElectionPrecomputesNextEpochOnNonceReady(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	electionTestNonce11 := makeDistinctNonce(electionTestNonce)
	epochProvider.SetEpochNonceForEpoch(11, electionTestNonce11)

	store := newMockScheduleStore()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	waitForSchedule(t, election, 30*time.Second)
	assert.Nil(t, election.ScheduleForEpoch(11))

	eventBus.Publish(
		event.EpochNonceReadyEventType,
		event.NewEvent(
			event.EpochNonceReadyEventType,
			event.EpochNonceReadyEvent{
				CurrentEpoch: 10,
				ReadyEpoch:   11,
				CutoffSlot:   95,
			},
		),
	)

	require.Eventually(t, func() bool {
		schedule := election.ScheduleForEpoch(11)
		return schedule != nil &&
			bytes.Equal(electionTestNonce11, schedule.EpochNonce)
	}, testutil.AsyncWait, 100*time.Millisecond,
		"next epoch schedule should be precomputed after nonce-ready event")

	require.Eventually(t, func() bool {
		schedule, err := store.LoadSchedule(11, poolId)
		require.NoError(t, err)
		return schedule != nil &&
			bytes.Equal(electionTestNonce11, schedule.EpochNonce)
	}, testutil.AsyncWait, 50*time.Millisecond,
		"next epoch schedule should be persisted")
}

func TestElectionRollbackKeepsCurrentSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.activeSlotCoeff = 1.0

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 30*time.Second)
	leaderSlots := append([]uint64(nil), schedule.LeaderSlotsSnapshot()...)
	require.NotEmpty(t, leaderSlots)
	leaderSlot := leaderSlots[0]

	eventBus.Publish(
		ledgerpkg.PoolStateRestoredEventType,
		event.NewEvent(
			ledgerpkg.PoolStateRestoredEventType,
			ledgerpkg.PoolStateRestoredEvent{Slot: 95},
		),
	)

	require.Eventually(t, func() bool {
		current := election.ScheduleForEpoch(10)
		if current == nil {
			return false
		}
		currentLeaderSlots := current.LeaderSlotsSnapshot()
		return bytes.Equal(schedule.EpochNonce, current.EpochNonce) &&
			assert.ObjectsAreEqual(leaderSlots, currentLeaderSlots) &&
			election.ShouldProduceBlock(leaderSlot)
	}, testutil.AsyncWait, 20*time.Millisecond,
		"rollback should not invalidate a stable current-epoch schedule")
}

func TestElectionRollbackKeepsPrecomputedNextSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.activeSlotCoeff = 1.0
	epochProvider.nextEpochReady.Store(11)
	nextEpochNonce := makeDistinctNonce(electionTestNonce)
	epochProvider.SetEpochNonceForEpoch(11, nextEpochNonce)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	waitForSchedule(t, election, 30*time.Second)
	var nextBefore *Schedule
	require.Eventually(t, func() bool {
		nextBefore = election.ScheduleForEpoch(11)
		return nextBefore != nil
	}, testutil.AsyncWait, 100*time.Millisecond)
	require.NotNil(t, nextBefore)
	expectedSlots := nextBefore.LeaderSlotsSnapshot()
	expectedNonce := append([]byte(nil), nextBefore.EpochNonce...)

	eventBus.Publish(
		ledgerpkg.PoolStateRestoredEventType,
		event.NewEvent(
			ledgerpkg.PoolStateRestoredEventType,
			ledgerpkg.PoolStateRestoredEvent{Slot: 90},
		),
	)

	require.Eventually(t, func() bool {
		schedule := election.ScheduleForEpoch(11)
		return schedule != nil &&
			bytes.Equal(expectedNonce, schedule.EpochNonce) &&
			assert.ObjectsAreEqual(
				expectedSlots,
				schedule.LeaderSlotsSnapshot(),
			)
	}, testutil.AsyncWait, 20*time.Millisecond,
		"rollback should not invalidate a precomputed next-epoch schedule")
}

func TestElectionConcurrentAccess(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Wait for initial schedule so concurrent goroutines exercise
	// both cache-hit and cache-miss paths.
	require.Eventually(t, func() bool {
		return election.CurrentSchedule() != nil
	}, testutil.AsyncWait, 50*time.Millisecond,
		"initial schedule should be computed before concurrent access")

	// Concurrent reads and operations
	done := make(chan bool)
	for i := range 20 {
		go func(slot int) {
			_ = election.ShouldProduceBlock(uint64(slot))
			_ = election.CurrentSchedule()
			_, _ = election.NextLeaderSlot(uint64(slot))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for range 20 {
		<-done
	}
}

// Parent cancellation must join the worker generation before any concurrent
// Stop returns or a new Start can replace the worker channels.
func TestElectionParentCancellationWaitsForGeneration(t *testing.T) {
	pool := lcommon.PoolKeyHash{}
	inner := newMockStakeProvider()
	inner.totalStake = 10000
	inner.poolStakes[string(pool[:])] = 1000
	blocked := &blockingStakeProvider{
		mockStakeProvider: inner,
		started:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	var release sync.Once
	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()
	defer release.Do(func() { close(blocked.release) })
	e := NewElection(
		pool,
		vrfSeedSource(electionTestVRFSeed),
		blocked,
		newMockEpochProvider(),
		bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, e.Start(parent))
	select {
	case <-blocked.started:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("worker did not reach provider")
	}
	cancel()
	// Start must inspect the canceled generation context, even before its
	// coordinator has acquired the election mutex and marked it stopped.
	restarted := make(chan error, 1)
	go func() { restarted <- e.Start(t.Context()) }()
	select {
	case <-restarted:
		t.Fatal("Start replaced an undrained canceled generation")
	case <-time.After(50 * time.Millisecond):
	}
	require.Eventually(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return !e.running
	}, testutil.AsyncWait, time.Millisecond)
	canceled, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	require.ErrorIs(t, e.Start(canceled), context.Canceled)
	stopped := make(chan error, 2)
	for range 2 {
		go func() { stopped <- e.Stop() }()
	}
	select {
	case <-stopped:
		t.Fatal("Stop returned before canceled generation drained")
	case <-time.After(50 * time.Millisecond):
	}
	release.Do(func() { close(blocked.release) })
	for range 2 {
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(testutil.AsyncWait):
			t.Fatal("Stop did not complete after canceled worker drained")
		}
	}
	select {
	case err := <-restarted:
		require.NoError(t, err)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("restart did not complete after canceled worker drained")
	}
	e.mu.RLock()
	running := e.running
	e.mu.RUnlock()
	require.True(t, running, "old generation waiters must not stop the restart")
	go func() { stopped <- e.Stop() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("restarted generation did not stop")
	}
}

// gatedElectionContext signals when Start evaluates its cancellation select.
type gatedElectionContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *gatedElectionContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestElectionCanceledWaiterAfterReplacement(t *testing.T) {
	oldCtx, cancelOld := context.WithCancel(t.Context())
	cancelOld()
	oldDone := make(chan struct{})
	e := &Election{lifecycleCtx: oldCtx, lifecycleDone: oldDone}
	waitCtx, cancelWait := context.WithCancel(t.Context())
	defer cancelWait()
	ctx := &gatedElectionContext{Context: waitCtx, entered: make(chan struct{})}
	afterWait := func() {
		// This waiter selected generation completion before cancellation. A
		// second caller installs a healthy replacement before it reacquires mu.
		e.mu.Lock()
		e.running = true
		e.lifecycleCtx = t.Context()
		e.lifecycleDone = make(chan struct{})
		cancelWait()
		e.mu.Unlock()
	}
	result := make(chan error, 1)
	go func() { result <- e.start(ctx, afterWait) }()
	select {
	case <-ctx.entered:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("Start did not wait for generation completion")
	}
	close(oldDone)
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("Start did not return after generation completion")
	}
}

// TestComputeScheduleDeclinesUnresolvableConsensusMode pins the caller half of
// the fail-closed consensus-mode forecast. The mode selects both the VRF input
// construction and the threshold, so a schedule computed from a substituted
// default is a leader-slot list cardano-node will reject. computeSchedule must
// return the resolution error rather than produce one.
func TestComputeScheduleDeclinesUnresolvableConsensusMode(t *testing.T) {
	t.Parallel()

	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.consensusModeErr = errors.New("era shape unavailable")

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfSeedSource(electionTestVRFSeed),
		stakeProvider,
		epochProvider,
		eventBus,
		slog.New(slog.DiscardHandler),
	)

	schedule, err := election.computeSchedule(
		context.Background(),
		epochProvider.CurrentEpoch(),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "era shape unavailable")
	require.Nil(t, schedule,
		"no schedule may be produced from an unresolved consensus mode")
}

func TestComputeScheduleRejectsMissingVRFSeedProvider(t *testing.T) {
	t.Parallel()
	stake := &recordingStakeProvider{
		poolStake:  sigmaAuditPoolStake,
		totalStake: sigmaAuditTotalStake,
	}
	election := newSigmaAuditElection(
		stake,
		&sigmaAuditEpochProvider{floatCoeff: 0.05},
		slog.New(slog.DiscardHandler),
	)
	election.vrfSeed = nil
	_, err := election.computeSchedule(context.Background(), sigmaAuditEpoch)
	require.ErrorContains(t, err, "pool VRF seed is unavailable")
}
