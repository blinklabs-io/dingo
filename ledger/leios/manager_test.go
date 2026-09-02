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
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	slot uint64
}

func (f *fakeSlotProvider) CurrentOrTipSlot() uint64 {
	return f.slot
}

type fakeParamsProvider struct {
	mu     sync.Mutex
	sigmaC *big.Rat
	tau    *big.Rat
	err    error
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

func (f *blockingParamsProvider) LeiosCommitteeParameters() (
	*big.Rat,
	*big.Rat,
	error,
) {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return big.NewRat(1, 1), big.NewRat(7, 10), nil
}

func (f *fakeParamsProvider) LeiosCommitteeParameters() (
	*big.Rat,
	*big.Rat,
	error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.sigmaC, f.tau, nil
}

// managerFixture wires a VoteManager against fake providers. The default
// committee has 10 members with stakes 100,90,...,10 (total active stake
// 550), sigma_c = 1, tau = 7/10 (385 stake required for quorum), current
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
		5, 3, poolStakes, total, big.NewRat(1, 1),
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
			sigmaC: big.NewRat(1, 1),
			tau:    big.NewRat(7, 10),
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
	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	vote := fixture.makeVote(t, 0, 577, ebHash)
	require.NoError(t, fixture.mgr.HandleVote("conn-a", vote))

	done := make(chan struct{})
	defer close(done)
	result := testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		2*time.Second,
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

func TestVoteManagerDoesNotEchoToOrigin(t *testing.T) {
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
		2*time.Second,
		"aborted NextVotes returns",
	)
	assert.Error(t, result.err)
}

func TestVoteManagerNextVotesCursorAdvances(t *testing.T) {
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
		2*time.Second,
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
		2*time.Second,
		"aborted NextVotes returns",
	)
	assert.Error(t, result.err)
}

func TestVoteManagerRemoveConnectionResetsCursor(t *testing.T) {
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
		2*time.Second,
		"first serve",
	)
	require.NoError(t, result.err)

	// A reconnecting peer starts from the beginning of the retained log
	fixture.mgr.RemoveConnection("conn-b")
	result = testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		2*time.Second,
		"serve again after cursor reset",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)
}

func TestVoteManagerNextVotesAccumulatesAcrossInserts(t *testing.T) {
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
		2*time.Second,
		"NextVotes returns once count votes are available",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 2)
	assert.Equal(t, uint64(0), result.votes[0].VoterId)
	assert.Equal(t, uint64(1), result.votes[1].VoterId)
}

func TestVoteManagerStopUnblocksNextVotes(t *testing.T) {
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
		2*time.Second,
		"Stop unblocks NextVotes",
	)
	assert.ErrorIs(t, result.err, ErrVoteManagerStopped)
}

func TestVoteManagerDedupIgnoresResubmission(t *testing.T) {
	fixture := newManagerFixture(t)
	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	vote := fixture.makeVote(t, 0, 577, ebHash)
	require.NoError(t, fixture.mgr.HandleVote("conn-a", vote))
	require.NoError(t, fixture.mgr.HandleVote("conn-c", vote))

	done := make(chan struct{})
	result := testutil.RequireReceive(
		t,
		startNextVotes(fixture, done, "conn-b", 1),
		2*time.Second,
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
		t, secondCh, 2*time.Second, "aborted NextVotes returns",
	)
}

func TestVoteManagerEquivocationFirstWins(t *testing.T) {
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

func TestVoteManagerRejectsInvalidVotes(t *testing.T) {
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
		2*time.Second,
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
		2*time.Second,
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
		t, emittedCh, 2*time.Second, "prototype vote emission",
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
		2*time.Second,
		"own vote served to peers",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)
	assert.Equal(t, uint64(3), result.votes[0].VoterId)
}

func TestVoteManagerDoesNotEmitVoteAfterVotingReconfiguredDuringSigning(
	t *testing.T,
) {
	keyProvider := &fakeLeiosKeyProvider{}
	var member CommitteeMember
	var key *VoteSigningKey
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			key = f.keys[member.VoterId]
			proof, err := SignVote(key, key.PublicKeyBytes())
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
		2*time.Second,
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
	}, 2*time.Second, "replacement voting configuration installed")

	releaseSigning()
	testutil.RequireReceive(
		t,
		observeDone,
		2*time.Second,
		"vote emission return",
	)
	result := testutil.RequireReceive(
		t,
		configuredCh,
		2*time.Second,
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

// TestVoteManagerPeerPrototypeVoteRequeuedForRelay guards issue #3288: a
// relay stored a peer's vote for its own tally but never queued it back up
// for its other peers, so a block producer behind that relay never observed
// quorum. A newly accepted peer vote must publish VoteReceivedEventType
// (node_leios.go's subscriber feeds this into the origin-aware Ouroboros
// enqueue path) with the exact signed fields and connection key the peer sent.
func TestVoteManagerPeerPrototypeVoteRequeuedForRelay(t *testing.T) {
	fixture := newManagerFixture(t)
	subId, receivedCh := fixture.eventBus.Subscribe(VoteReceivedEventType)
	defer fixture.eventBus.Unsubscribe(VoteReceivedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	vote := fixture.makePrototypeVote(t, 3, rbHash)
	require.NoError(t, fixture.mgr.HandlePrototypeVote("conn-a", vote))

	requeued := testutil.RequireReceive(
		t, receivedCh, 2*time.Second, "peer vote requeued for relay",
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
		2*time.Second,
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
	fixture := newManagerFixture(t)
	subId, receivedCh := fixture.eventBus.Subscribe(VoteReceivedEventType)
	defer fixture.eventBus.Unsubscribe(VoteReceivedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.ObserveAnnouncement(577, rbHash, ebHash)

	vote := fixture.makePrototypeVote(t, 3, rbHash)
	require.NoError(t, fixture.mgr.HandlePrototypeVote("conn-a", vote))
	testutil.RequireReceive(
		t, receivedCh, 2*time.Second, "first delivery requeued for relay",
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
		2*time.Second,
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
	key := testSigningKey(t, 210)
	proof, err := SignVote(key, key.PublicKeyBytes())
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
	key := testSigningKey(t, 123)
	proof, err := SignVote(key, key.PublicKeyBytes())
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
	onChainKey := testSigningKey(t, 203)
	proof, err := SignVote(onChainKey, onChainKey.PublicKeyBytes())
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
	key := testSigningKey(t, 124)
	wrongKey := testSigningKey(t, 125)
	badProof, err := SignVote(wrongKey, key.PublicKeyBytes())
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
	key := testSigningKey(t, 126)
	proof, err := SignVote(key, key.PublicKeyBytes())
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

	proof, err := SignVote(key, key.PublicKeyBytes())
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
			2*time.Second,
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
		2*time.Second,
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
	var member CommitteeMember
	var key *VoteSigningKey
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			key = f.keys[member.VoterId]
			proof, err := SignVote(key, key.PublicKeyBytes())
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
		2*time.Second,
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
					proof, err := SignVote(key, key.PublicKeyBytes())
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
				proof, err := SignVote(
					staleKey,
					staleKey.PublicKeyBytes(),
				)
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
				2*time.Second,
				"initial epoch key lookup",
			)

			fixture.mgr.retryDeferredVoting(6)
			emittedEvent := testutil.RequireReceive(
				t,
				emittedCh,
				2*time.Second,
				"newer epoch voting activation",
			)
			emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
			require.True(t, ok)
			assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash)

			keyProvider.releaseInitialLookup()
			result := testutil.RequireReceive(
				t,
				configuredCh,
				2*time.Second,
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
				proof, err := SignVote(
					replacementKey,
					replacementKey.PublicKeyBytes(),
				)
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
				proof, err := SignVote(
					mismatchedKey,
					mismatchedKey.PublicKeyBytes(),
				)
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
				2*time.Second,
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
				2*time.Second,
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
			validProof, err := SignVote(key, key.PublicKeyBytes())
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
				otherProof, signErr := SignVote(
					otherKey,
					otherKey.PublicKeyBytes(),
				)
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
				2*time.Second,
				"initial epoch key lookup",
			)

			fixture.mgr.retryDeferredVoting(6)
			keyProvider.releaseInitialLookup()
			result := testutil.RequireReceive(
				t,
				configuredCh,
				2*time.Second,
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
	proof, err := SignVote(key, key.PublicKeyBytes())
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
		2*time.Second,
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
		2*time.Second,
		"newer retry key lookup",
	)

	keyProvider.releaseInitialLookup()
	result := testutil.RequireReceive(
		t,
		configuredCh,
		2*time.Second,
		"configuration while newer retry remains in flight",
	)
	require.NoError(t, result.err)
	assert.Equal(t, VotingConfigurationSuperseded, result.status)
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()

	keyProvider.releaseCurrentLookup()
	testutil.RequireReceive(t, retryDone, 2*time.Second, "newer deferred retry")
	fixture.mgr.mu.Lock()
	assert.Nil(t, fixture.mgr.votingKey)
	assert.Same(t, key, fixture.mgr.deferredVotingKey)
	fixture.mgr.mu.Unlock()
}

func TestVoteManagerConfigureVotingReportsReplayPreparationFailure(
	t *testing.T,
) {
	keyProvider := &fakeLeiosKeyProvider{failOnCall: 2}
	var member CommitteeMember
	var key *VoteSigningKey
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			member = f.members[3]
			key = f.keys[member.VoterId]
			proof, err := SignVote(key, key.PublicKeyBytes())
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

	proof, err := SignVote(key, key.PublicKeyBytes())
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
		2*time.Second,
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

	validProof, err := SignVote(key, key.PublicKeyBytes())
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
	mismatchedProof, err := SignVote(
		mismatchedKey,
		mismatchedKey.PublicKeyBytes(),
	)
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
	validProof, err := SignVote(key, key.PublicKeyBytes())
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
		2*time.Second,
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
	validProof, err := SignVote(key, key.PublicKeyBytes())
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
		2*time.Second,
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
	onChainKey := testSigningKey(t, 210)
	proof, err := SignVote(onChainKey, onChainKey.PublicKeyBytes())
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
	rotatedKey := testSigningKey(t, 200)
	proof, err := SignVote(rotatedKey, rotatedKey.PublicKeyBytes())
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
	onChainKey := testSigningKey(t, 201)
	proof, err := SignVote(onChainKey, onChainKey.PublicKeyBytes())
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
	}, 2*time.Second, "votes after the rollback point are pruned")
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
	}, 2*time.Second, "votes older than the previous epoch are pruned")
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
	fixture := newManagerFixture(
		t,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			f.params.err = errors.New(
				"quorum stake threshold must be less than committee stake coverage",
			)
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
		2*time.Second,
		"aborted NextVotes returns",
	)
	require.Error(t, result.err)

	// The undelivered vote must still be served on the next request
	done2 := make(chan struct{})
	defer close(done2)
	result = testutil.RequireReceive(
		t,
		startNextVotes(fixture, done2, "conn-b", 1),
		2*time.Second,
		"vote re-served after aborted request",
	)
	require.NoError(t, result.err)
	require.Len(t, result.votes, 1)
	assert.Equal(t, uint64(0), result.votes[0].VoterId)
}

func TestVoteManagerEvictedVoteDoesNotRecount(t *testing.T) {
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
		2*time.Second,
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
		2*time.Second,
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
	}, 2*time.Second, "rolled-back vote is pruned")

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
		2*time.Second,
		"committee lookup",
	)
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: 550},
	})
	close(params.release)
	testutil.RequireReceive(t, done, 2*time.Second, "in-flight emission exit")

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
		2*time.Second,
		"committee lookup",
	)
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: 550},
	})
	close(params.release)
	require.NoError(t, testutil.RequireReceive(
		t, done, 2*time.Second, "resolved vote exit",
	))

	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: 577, VoterId: 3,
	}}))
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Empty(t, fixture.mgr.tallies)
	assert.Empty(t, fixture.mgr.voteRecords)
}
