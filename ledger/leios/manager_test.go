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
	member := fixture.members[3]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[3]
	require.NotNil(t, key)
	fixture.mgr.EnableVoting(poolKeyHash, key)

	ebHash := lcommon.NewBlake2b256([]byte("eb"))
	fixture.mgr.HandleEndorserBlock(577, ebHash)

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
			VoteMessageBytes(577, ebHash),
			vote.VoteSignature,
		),
	)

	// Exactly one vote per EB per voter
	fixture.mgr.HandleEndorserBlock(577, ebHash)
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
