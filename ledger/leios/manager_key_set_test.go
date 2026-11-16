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
	"encoding/hex"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// epochSwitchedKeyProvider serves the registered keys of an epoch's committee
// snapshot: before is returned for snapshots older than switchAt, after for
// the rest. It models a key rotation registered between two snapshots.
type epochSwitchedKeyProvider struct {
	switchAt uint64
	before   map[string]*lcommon.LeiosKey
	after    map[string]*lcommon.LeiosKey
}

func (p *epochSwitchedKeyProvider) GetLeiosKeys(
	snapshotEpoch uint64,
	_ []string,
) (map[string]*lcommon.LeiosKey, error) {
	if snapshotEpoch < p.switchAt {
		return p.before, nil
	}
	return p.after, nil
}

// newKeySetFixture is newHeaderArmingFixture without legacy single-key voting
// enabled, so a test configures the key set itself.
func newKeySetFixture(
	t *testing.T,
	slots *fakeSlotProvider,
	extra ...func(*managerFixture, *VoteManagerConfig),
) *managerFixture {
	t.Helper()
	opts := []func(*managerFixture, *VoteManagerConfig){
		func(f *managerFixture, cfg *VoteManagerConfig) {
			cfg.SlotProvider = slots
			f.params.headerPeriod = time.Nanosecond
			f.params.votePeriod = 10 * time.Minute
		},
	}
	opts = append(opts, extra...)
	fixture := newManagerFixture(t, opts...)
	_, err := slots.SlotToTime(slots.CurrentOrTipSlot())
	require.NoError(t, err)
	return fixture
}

// armAnnouncement announces and validates an endorser block without waiting
// for votes, so a caller can decide how many to expect.
func armAnnouncement(
	t *testing.T,
	fixture *managerFixture,
	slot uint64,
	rbHash, ebHash lcommon.Blake2b256,
	seq uint64,
) {
	t.Helper()
	publishHeaderAnnouncement(fixture, slot, rbHash, ebHash, seq)
	waitForAnnouncement(t, fixture, rbHash)
	fixture.mgr.HandleValidatedEndorserBlock(slot, rbHash, ebHash)
}

// receiveVotes blocks for exactly n emitted votes, then drains for extras.
func receiveVotes(
	t *testing.T,
	emittedCh <-chan event.Event,
	n int,
) map[uint64]lcommon.LeiosPrototypeVote {
	t.Helper()
	votes := make(map[uint64]lcommon.LeiosPrototypeVote, n)
	for range n {
		emitted := testutil.RequireReceive(
			t, emittedCh, testutil.AsyncWait, "expected local vote",
		)
		data, ok := emitted.Data.(VoteEmittedEvent)
		require.True(t, ok)
		_, dup := votes[data.Vote.VoterId]
		require.False(t, dup, "voter %d voted twice", data.Vote.VoterId)
		votes[data.Vote.VoterId] = data.Vote
	}
	testutil.RequireNoReceive(
		t, emittedCh, 300*time.Millisecond, "unexpected extra local vote",
	)
	return votes
}

func requireVoteSignedBy(
	t *testing.T,
	key *VoteSigningKey,
	rbHash lcommon.Blake2b256,
	vote lcommon.LeiosPrototypeVote,
) {
	t.Helper()
	require.NoError(t, VerifyVoteSignature(
		key.PublicKey(),
		PrototypeVoteMessageBytes(rbHash),
		vote.VoteSignature,
	))
}

func TestVoteManagerKeySetVotesEveryHeldSeat(t *testing.T) {
	t.Parallel()

	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	seatA, seatB := uint64(3), uint64(5)
	stranger := testSigningKey(t, 200)
	// The repeated key must not produce a second vote for its seat.
	require.NoError(t, fixture.mgr.ConfigureVotingKeys([]*VoteSigningKey{
		fixture.keys[seatA],
		stranger,
		fixture.keys[seatB],
		fixture.keys[seatA],
	}))

	rbHash := lcommon.NewBlake2b256([]byte("key-set-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("key-set-eb"))
	armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)

	votes := receiveVotes(t, emittedCh, 2)
	require.Contains(t, votes, seatA)
	require.Contains(t, votes, seatB)
	requireVoteSignedBy(t, fixture.keys[seatA], rbHash, votes[seatA])
	requireVoteSignedBy(t, fixture.keys[seatB], rbHash, votes[seatB])
	for _, voterId := range []uint64{seatA, seatB} {
		raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: voterId,
		}})
		assert.Len(t, raws, 1, "seat %d vote is stored for diffusion", voterId)
	}
}

func TestVoteManagerKeySetWithoutSeatEmitsNothing(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(
		t,
		slots,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.PromRegistry = reg
		},
	)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	require.NoError(t, fixture.mgr.ConfigureVotingKeys([]*VoteSigningKey{
		testSigningKey(t, 200),
		testSigningKey(t, 201),
	}))

	rbHash := lcommon.NewBlake2b256([]byte("no-seat-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("no-seat-eb"))
	armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)

	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"no configured key holds a committee seat",
	)
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(
			fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
				voteNotEmittedNotSeated,
			),
		) == 1
	}, testutil.AsyncWait, 10*time.Millisecond,
		"the missing seat is counted as not_seated")
}

func TestVoteManagerKeySetRotationPairFollowsRegisteredKey(t *testing.T) {
	t.Parallel()

	const seat = uint64(3)
	oldKey := testSigningKey(t, 201)
	newKey := testSigningKey(t, 202)
	oldReg := testLeiosKey(t, 201)
	newReg := testLeiosKey(t, 202)
	firstEpoch, secondEpoch := uint64(5), uint64(6)
	require.Less(
		t,
		CommitteeSnapshotEpoch(firstEpoch),
		CommitteeSnapshotEpoch(secondEpoch),
		"test setup: the epochs need distinct committee snapshots",
	)
	firstSlot := firstEpoch*testSlotsPerEpoch + 77
	secondSlot := secondEpoch*testSlotsPerEpoch + 77

	run := func(t *testing.T, keys ...*VoteSigningKey) *managerFixture {
		t.Helper()
		slots := &fakeSlotProvider{slot: firstSlot + 1}
		fixture := newKeySetFixture(
			t,
			slots,
			func(f *managerFixture, cfg *VoteManagerConfig) {
				pool := hex.EncodeToString(f.members[seat].PoolKeyHash)
				cfg.KeyProvider = &epochSwitchedKeyProvider{
					switchAt: CommitteeSnapshotEpoch(secondEpoch),
					before:   map[string]*lcommon.LeiosKey{pool: oldReg},
					after:    map[string]*lcommon.LeiosKey{pool: newReg},
				}
			},
		)
		require.NoError(t, fixture.mgr.ConfigureVotingKeys(keys))
		return fixture
	}

	t.Run("pair votes in both epochs", func(t *testing.T) {
		t.Parallel()
		fixture := run(t, oldKey, newKey)
		slots := fixture.mgr.slotProvider.(*fakeSlotProvider)
		subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
		defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

		rb1 := lcommon.NewBlake2b256([]byte("rotation-rb-1"))
		eb1 := lcommon.NewBlake2b256([]byte("rotation-eb-1"))
		armAnnouncement(t, fixture, firstSlot, rb1, eb1, 1)
		votes := receiveVotes(t, emittedCh, 1)
		requireVoteSignedBy(t, oldKey, rb1, votes[seat])

		slots.setSlot(secondSlot + 1)
		rb2 := lcommon.NewBlake2b256([]byte("rotation-rb-2"))
		eb2 := lcommon.NewBlake2b256([]byte("rotation-eb-2"))
		armAnnouncement(t, fixture, secondSlot, rb2, eb2, 2)
		votes = receiveVotes(t, emittedCh, 1)
		requireVoteSignedBy(t, newKey, rb2, votes[seat])
	})
	t.Run("pair order does not matter", func(t *testing.T) {
		t.Parallel()
		fixture := run(t, newKey, oldKey)
		slots := fixture.mgr.slotProvider.(*fakeSlotProvider)
		subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
		defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

		rb1 := lcommon.NewBlake2b256([]byte("reversed-rb-1"))
		eb1 := lcommon.NewBlake2b256([]byte("reversed-eb-1"))
		armAnnouncement(t, fixture, firstSlot, rb1, eb1, 1)
		votes := receiveVotes(t, emittedCh, 1)
		requireVoteSignedBy(t, oldKey, rb1, votes[seat])

		slots.setSlot(secondSlot + 1)
		rb2 := lcommon.NewBlake2b256([]byte("reversed-rb-2"))
		eb2 := lcommon.NewBlake2b256([]byte("reversed-eb-2"))
		armAnnouncement(t, fixture, secondSlot, rb2, eb2, 2)
		votes = receiveVotes(t, emittedCh, 1)
		requireVoteSignedBy(t, newKey, rb2, votes[seat])
	})
	t.Run(
		"the new key alone waits for the committee snapshot",
		func(t *testing.T) {
			t.Parallel()
			fixture := run(t, newKey)
			subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
			defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

			rb := lcommon.NewBlake2b256([]byte("early-new-rb"))
			eb := lcommon.NewBlake2b256([]byte("early-new-eb"))
			armAnnouncement(t, fixture, firstSlot, rb, eb, 1)
			testutil.RequireNoReceive(
				t,
				emittedCh,
				300*time.Millisecond,
				"the new key holds no seat before its snapshot governs",
			)
		},
	)
	t.Run("the old key alone goes dark after the rotation", func(t *testing.T) {
		t.Parallel()
		fixture := run(t, oldKey)
		slots := fixture.mgr.slotProvider.(*fakeSlotProvider)
		subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
		defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

		slots.setSlot(secondSlot + 1)
		rb := lcommon.NewBlake2b256([]byte("dark-rb"))
		eb := lcommon.NewBlake2b256([]byte("dark-eb"))
		armAnnouncement(t, fixture, secondSlot, rb, eb, 1)
		testutil.RequireNoReceive(
			t,
			emittedCh,
			300*time.Millisecond,
			"only the currently registered key matches a seat",
		)
	})
}

func TestVoteManagerKeySetVotesOnceForAKeyRegisteredTwice(t *testing.T) {
	t.Parallel()

	shared := testSigningKey(t, 203)
	sharedReg := testLeiosKey(t, 203)
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(
		t,
		slots,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			cfg.KeyProvider = &fakeLeiosKeyProvider{
				keys: map[string]*lcommon.LeiosKey{
					hex.EncodeToString(f.members[6].PoolKeyHash): sharedReg,
					hex.EncodeToString(f.members[2].PoolKeyHash): sharedReg,
				},
			}
		},
	)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	require.NoError(
		t,
		fixture.mgr.ConfigureVotingKeys([]*VoteSigningKey{shared}),
	)

	rbHash := lcommon.NewBlake2b256([]byte("shared-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("shared-eb"))
	armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)

	votes := receiveVotes(t, emittedCh, 1)
	require.Contains(
		t,
		votes,
		uint64(2),
		"one key votes at the smallest seat it holds",
	)
}

func TestVoteManagerKeySetReplaysAnnouncementsReadyBeforeConfiguration(
	t *testing.T,
) {
	t.Parallel()

	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	rbHash := lcommon.NewBlake2b256([]byte("early-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("early-eb"))
	armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	testutil.RequireNoReceive(
		t, emittedCh, 300*time.Millisecond, "no key is configured yet",
	)

	require.NoError(t, fixture.mgr.ConfigureVotingKeys(
		[]*VoteSigningKey{fixture.keys[3]},
	))
	votes := receiveVotes(t, emittedCh, 1)
	requireVoteSignedBy(t, fixture.keys[3], rbHash, votes[3])
}

func TestVoteManagerKeySetReconfigurationReplacesTheSet(t *testing.T) {
	t.Parallel()

	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	require.NoError(t, fixture.mgr.ConfigureVotingKeys(
		[]*VoteSigningKey{fixture.keys[3]},
	))
	require.NoError(t, fixture.mgr.ConfigureVotingKeys(
		[]*VoteSigningKey{fixture.keys[5]},
	))

	rbHash := lcommon.NewBlake2b256([]byte("replace-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("replace-eb"))
	armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	votes := receiveVotes(t, emittedCh, 1)
	require.Contains(t, votes, uint64(5))
}

func TestVoteManagerConfigureVotingKeysRejectsNilKey(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	require.Error(t, fixture.mgr.ConfigureVotingKeys(
		[]*VoteSigningKey{fixture.keys[3], nil},
	))
}

func TestVoteManagerLatestVotingConfigurationWins(t *testing.T) {
	t.Parallel()

	enablePoolBound := func(f *managerFixture, voterId uint64) {
		var pool lcommon.PoolKeyHash
		copy(pool[:], f.members[voterId].PoolKeyHash)
		require.NoError(t, f.mgr.EnableVoting(pool, f.keys[voterId]))
	}

	t.Run("a key set replaces a pool-bound key", func(t *testing.T) {
		t.Parallel()
		slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
		fixture := newKeySetFixture(t, slots)
		subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
		defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
		enablePoolBound(fixture, 3)
		require.NoError(t, fixture.mgr.ConfigureVotingKeys(
			[]*VoteSigningKey{fixture.keys[5]},
		))

		rbHash := lcommon.NewBlake2b256([]byte("set-over-pool-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("set-over-pool-eb"))
		armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)
		votes := receiveVotes(t, emittedCh, 1)
		require.Contains(t, votes, uint64(5))
	})
	t.Run("a pool-bound key replaces a key set", func(t *testing.T) {
		t.Parallel()
		slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
		fixture := newKeySetFixture(t, slots)
		subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
		defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
		require.NoError(t, fixture.mgr.ConfigureVotingKeys(
			[]*VoteSigningKey{fixture.keys[5]},
		))
		enablePoolBound(fixture, 3)

		rbHash := lcommon.NewBlake2b256([]byte("pool-over-set-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("pool-over-set-eb"))
		armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)
		votes := receiveVotes(t, emittedCh, 1)
		require.Contains(t, votes, uint64(3))
	})
}

func TestVoteManagerKeySetVotesOnlySeatsWithoutAStoredVote(t *testing.T) {
	t.Parallel()

	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	keys := []*VoteSigningKey{fixture.keys[3], fixture.keys[5], fixture.keys[7]}
	require.NoError(t, fixture.mgr.ConfigureVotingKeys(keys))

	rbHash := lcommon.NewBlake2b256([]byte("stored-seat-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("stored-seat-eb"))
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	waitForAnnouncement(t, fixture, rbHash)
	// Seat 5's vote reaches the node from a peer before the node votes, as
	// when another process holds the same key: the key set must not sign a
	// second vote for that seat, and must still vote the others.
	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"peer",
		fixture.makePrototypeVote(t, 5, rbHash),
	))
	fixture.mgr.HandleValidatedEndorserBlock(headerArmingRbSlot, rbHash, ebHash)

	votes := receiveVotes(t, emittedCh, 2)
	require.Contains(t, votes, uint64(3))
	require.Contains(t, votes, uint64(7))

	// Re-emitting the announcement, by validation or by reconfiguring the
	// same keys, casts nothing more.
	fixture.mgr.HandleValidatedEndorserBlock(headerArmingRbSlot, rbHash, ebHash)
	require.NoError(t, fixture.mgr.ConfigureVotingKeys(keys))
	testutil.RequireNoReceive(
		t, emittedCh, 300*time.Millisecond, "the announcement was voted",
	)
	for _, voterId := range []uint64{3, 5, 7} {
		raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: voterId,
		}})
		assert.Len(t, raws, 1, "seat %d holds one vote", voterId)
	}
}

func TestVoteManagerConfigureVotingKeysCountsDistinctKeys(t *testing.T) {
	t.Parallel()

	fixture := newManagerFixture(t)
	require.NoError(t, fixture.mgr.ConfigureVotingKeys([]*VoteSigningKey{
		fixture.keys[3],
		fixture.keys[5],
		fixture.keys[3],
	}))
	assert.Equal(t, 2, fixture.mgr.VotingKeyCount())
}

func TestVoteManagerKeySetVoteDoesNotCommitAfterReconfiguration(
	t *testing.T,
) {
	t.Parallel()

	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	require.NoError(t, fixture.mgr.ConfigureVotingKeys(
		[]*VoteSigningKey{fixture.keys[5]},
	))
	var pool lcommon.PoolKeyHash
	copy(pool[:], fixture.members[3].PoolKeyHash)
	// A pool-bound configuration lands while the key-set vote is being
	// signed; the vote was decided under a key set no longer in force.
	fixture.mgr.signVote = func(
		key *VoteSigningKey,
		msg []byte,
	) ([]byte, error) {
		assert.NoError(t, fixture.mgr.EnableVoting(pool, fixture.keys[3]))
		return SignVote(key, msg)
	}

	rbHash := lcommon.NewBlake2b256([]byte("reconfigured-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("reconfigured-eb"))
	armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	testutil.RequireNoReceive(
		t, emittedCh, 300*time.Millisecond, "the key set was replaced",
	)
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo: headerArmingRbSlot, VoterId: 5,
	}}))
}

func TestVoteManagerConfigureVotingReplacesKeySet(t *testing.T) {
	t.Parallel()

	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newKeySetFixture(
		t,
		slots,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			keys := make(map[string]*lcommon.LeiosKey, 2)
			for _, voterId := range []uint64{3, 5} {
				key := f.keys[voterId]
				proof, err := signWithDST(
					key, key.PublicKeyBytes(), LeiosPoPDST,
				)
				require.NoError(t, err)
				keys[hex.EncodeToString(f.members[voterId].PoolKeyHash)] =
					&lcommon.LeiosKey{
						PublicKey:       key.PublicKeyBytes(),
						PossessionProof: proof,
					}
			}
			cfg.KeyProvider = &fakeLeiosKeyProvider{keys: keys}
		},
	)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	require.NoError(t, fixture.mgr.ConfigureVotingKeys(
		[]*VoteSigningKey{fixture.keys[5]},
	))
	var pool lcommon.PoolKeyHash
	copy(pool[:], fixture.members[3].PoolKeyHash)
	status, err := fixture.mgr.ConfigureVoting(pool, fixture.keys[3])
	require.NoError(t, err)
	require.Equal(t, VotingConfigurationEnabled, status)
	assert.Zero(t, fixture.mgr.VotingKeyCount())

	rbHash := lcommon.NewBlake2b256([]byte("configure-over-set-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("configure-over-set-eb"))
	armAnnouncement(t, fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	votes := receiveVotes(t, emittedCh, 1)
	require.Contains(t, votes, uint64(3))
}

func TestVoteManagerPoolBoundKeyStopsVotingWhenRegistrationRotates(
	t *testing.T,
) {
	t.Parallel()

	const seat = uint64(3)
	oldKey := testSigningKey(t, 201)
	oldReg := testLeiosKey(t, 201)
	newReg := testLeiosKey(t, 202)
	firstEpoch, secondEpoch := uint64(5), uint64(6)
	firstSlot := firstEpoch*testSlotsPerEpoch + 77
	secondSlot := secondEpoch*testSlotsPerEpoch + 77

	reg := prometheus.NewRegistry()
	slots := &fakeSlotProvider{slot: firstSlot + 1}
	fixture := newKeySetFixture(
		t,
		slots,
		func(f *managerFixture, cfg *VoteManagerConfig) {
			pool := hex.EncodeToString(f.members[seat].PoolKeyHash)
			cfg.PromRegistry = reg
			cfg.KeyProvider = &epochSwitchedKeyProvider{
				switchAt: CommitteeSnapshotEpoch(secondEpoch),
				before:   map[string]*lcommon.LeiosKey{pool: oldReg},
				after:    map[string]*lcommon.LeiosKey{pool: newReg},
			}
		},
	)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	var pool lcommon.PoolKeyHash
	copy(pool[:], fixture.members[seat].PoolKeyHash)
	require.NoError(t, fixture.mgr.EnableVoting(pool, oldKey))

	rb1 := lcommon.NewBlake2b256([]byte("pool-rotation-rb-1"))
	eb1 := lcommon.NewBlake2b256([]byte("pool-rotation-eb-1"))
	armAnnouncement(t, fixture, firstSlot, rb1, eb1, 1)
	votes := receiveVotes(t, emittedCh, 1)
	requireVoteSignedBy(t, oldKey, rb1, votes[seat])

	// The committee snapshot now carries the new registration, so the
	// configured key would sign a vote no peer accepts.
	slots.setSlot(secondSlot + 1)
	rb2 := lcommon.NewBlake2b256([]byte("pool-rotation-rb-2"))
	eb2 := lcommon.NewBlake2b256([]byte("pool-rotation-eb-2"))
	armAnnouncement(t, fixture, secondSlot, rb2, eb2, 2)
	testutil.RequireNoReceive(
		t, emittedCh, 300*time.Millisecond, "the pool's key was rotated",
	)
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(
			fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
				voteNotEmittedKeyMismatch,
			),
		) == 1
	}, testutil.AsyncWait, 10*time.Millisecond)
}
