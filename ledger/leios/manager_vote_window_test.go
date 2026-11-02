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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeVoteTiming supplies the CIP-164 header and vote periods.
type fakeVoteTiming struct {
	header, vote time.Duration
	err          error
}

func (f fakeVoteTiming) LeiosVotingParameters(
	uint64,
) (time.Duration, time.Duration, error) {
	return f.header, f.vote, f.err
}

// fakeSlotOnset is a slot provider on which every slot begins at onset.
type fakeSlotOnset struct {
	*fakeSlotProvider
	onset time.Time
	err   error
}

func (f fakeSlotOnset) SlotToTime(uint64) (time.Time, error) {
	return f.onset, f.err
}

// voteWindowHarness drives a seated, keyed manager on a fake clock and
// captures armed timers instead of waiting for them.
type voteWindowHarness struct {
	fixture *managerFixture
	onset   time.Time
	clock   time.Time
	armed   []time.Duration
	fire    []func()
	stopped int
}

const (
	voteHeaderPeriod = time.Second
	// voteWindowOpens is the equivocation-detection wait, three header
	// periods after the announcing slot begins.
	voteWindowOpens  = 3 * voteHeaderPeriod
	voteWindowLength = 4 * time.Second
)

// onTimeVoteTiming is a header and vote period pair with a resolvable window.
var onTimeVoteTiming = fakeVoteTiming{
	header: voteHeaderPeriod, vote: voteWindowLength,
}

func newVoteWindowHarness(
	t *testing.T,
	timing fakeVoteTiming,
	onset fakeSlotOnset,
) *voteWindowHarness {
	t.Helper()
	h := &voteWindowHarness{onset: onset.onset}
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	onset.fakeSlotProvider = slots
	h.fixture = newHeaderArmingFixture(
		t,
		slots,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.PromRegistry = prometheus.NewRegistry()
			cfg.VotingParamsProvider = timing
			cfg.SlotProvider = onset
		},
	)
	mgr := h.fixture.mgr
	mgr.now = func() time.Time { return h.clock }
	mgr.scheduleAfter = func(d time.Duration, f func()) func() bool {
		h.armed = append(h.armed, d)
		h.fire = append(h.fire, f)
		return func() bool {
			h.stopped++
			return true
		}
	}
	return h
}

var (
	voteWindowRbHash = lcommon.NewBlake2b256([]byte("announcing-rb"))
	voteWindowEbHash = lcommon.NewBlake2b256([]byte("announced-eb"))
)

// validate records the EB's semantic validation at the harness clock.
func (h *voteWindowHarness) validate() {
	h.fixture.mgr.HandleValidatedEndorserBlock(
		headerArmingRbSlot, voteWindowRbHash, voteWindowEbHash,
	)
}

// observe arms the announcement from a header that arrived at the onset of
// its slot, inside the header period.
func (h *voteWindowHarness) observe() {
	h.fixture.mgr.observeAnnouncement(
		headerArmingRbSlot, voteWindowRbHash, voteWindowEbHash, 0, h.onset,
	)
}

func (h *voteWindowHarness) announce() {
	h.validate()
	h.observe()
}

func (h *voteWindowHarness) voted() bool {
	return len(h.fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}})) == 1
}

func (h *voteWindowHarness) notEmitted(reason string) float64 {
	return promtestutil.ToFloat64(
		h.fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(reason),
	)
}

func TestVoteManagerHoldsVoteUntilWindowOpens(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		onTimeVoteTiming,
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset.Add(time.Second)

	h.announce()
	assert.False(t, h.voted(), "no vote before the window opens")
	require.Equal(t, []time.Duration{2 * time.Second}, h.armed)

	// A repeated observation of the same announcement arms nothing new.
	h.announce()
	require.Len(t, h.armed, 1, "one timer per announcement")

	h.clock = onset.Add(voteWindowOpens)
	h.fire[0]()
	assert.True(t, h.voted(), "the timer casts the vote once the window opens")
}

func TestVoteManagerVotesImmediatelyInsideWindow(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		onTimeVoteTiming,
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset.Add(voteWindowOpens + voteWindowLength - time.Millisecond)

	h.announce()
	assert.True(t, h.voted(), "the instant before the deadline is inside the window")
	assert.Empty(t, h.armed)
}

func TestVoteManagerDropsVoteAfterWindowCloses(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		onTimeVoteTiming,
		fakeSlotOnset{onset: onset},
	)
	// The EB validated inside the window; the announcement is only seen
	// once the window has closed. CIP-164 requires the vote before the
	// deadline, so the deadline itself is already closed.
	h.clock = onset.Add(voteWindowOpens)
	h.validate()
	h.clock = onset.Add(voteWindowOpens + voteWindowLength)
	h.observe()
	assert.False(t, h.voted())
	assert.Empty(t, h.armed, "a closed window arms no timer")
	assert.Equal(t, float64(1), h.notEmitted(voteNotEmittedTooLate))
}

// CIP-164 defines the voting window only through the protocol parameters
// and the slot onset, so an announcement whose window cannot be resolved is
// not voted on rather than voted on without one.
func TestVoteManagerDeclinesVoteWithoutWindow(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	cases := map[string]struct {
		timing fakeVoteTiming
		onset  fakeSlotOnset
	}{
		"timing unavailable": {
			timing: fakeVoteTiming{err: errors.New("no pparams")},
			onset:  fakeSlotOnset{onset: onset},
		},
		"slot onset unavailable": {
			timing: onTimeVoteTiming,
			onset:  fakeSlotOnset{err: errors.New("past the horizon")},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newVoteWindowHarness(t, tc.timing, tc.onset)
			h.clock = onset
			h.announce()
			assert.False(t, h.voted())
			assert.Empty(t, h.armed)
			assert.Equal(t, float64(1),
				h.notEmitted(voteNotEmittedTimingUnavailable))
		})
	}
}

func TestVoteManagerStopCancelsScheduledVotes(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		onTimeVoteTiming,
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset
	h.announce()
	require.Len(t, h.fire, 1)

	require.NoError(t, h.fixture.mgr.Stop())
	assert.Equal(t, 1, h.stopped)

	// A timer that had already fired when Stop ran must not vote.
	h.clock = onset.Add(voteWindowOpens)
	h.fire[0]()
	testutil.WaitForCondition(t, func() bool { return !h.voted() },
		testutil.AsyncWait, "no vote after Stop")
}

// signHook replaces the local signer with one that calls during before it
// signs, and returns the number of signatures attempted. Signing happens outside the state lock, so this is where a slow signature
// lets the manager's state move on under an emission that already passed its
// checks.
func (h *voteWindowHarness) signHook(during func()) *int {
	calls := 0
	h.fixture.mgr.signVote = func(
		key *VoteSigningKey,
		msg []byte,
	) ([]byte, error) {
		calls++
		if during != nil {
			during()
		}
		return SignVote(key, msg)
	}
	return &calls
}

func TestVoteManagerDropsVoteSignedPastDeadline(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		onTimeVoteTiming,
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset.Add(voteWindowOpens + voteWindowLength - time.Millisecond)
	h.signHook(func() {
		h.clock = h.clock.Add(time.Millisecond)
	})

	h.announce()
	assert.False(t, h.voted(), "a signature finished after the window closed")
	assert.Equal(t, float64(1), h.notEmitted(voteNotEmittedTooLate))
	assert.Equal(t, float64(0), h.notEmitted(voteNotEmittedNotInserted))
}

func TestVoteManagerScheduledVoteAcrossStopCastsNothing(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{"stop": false, "stop then start": true}
	for name, restart := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			onset := time.Unix(1_700_000_000, 0)
			h := newVoteWindowHarness(
				t,
				onTimeVoteTiming,
				fakeSlotOnset{onset: onset},
			)
			h.clock = onset
			h.announce()
			require.Len(t, h.fire, 1)

			// The timer has already passed its map check and is signing
			// when the manager stops.
			mgr := h.fixture.mgr
			h.signHook(func() {
				require.NoError(t, mgr.Stop())
				if restart {
					require.NoError(t, mgr.Start(t.Context()))
				}
			})
			h.clock = onset.Add(voteWindowOpens)
			h.fire[0]()
			assert.False(t, h.voted(), "a timer of a stopped lifecycle voted")
			assert.Equal(t, float64(1), h.notEmitted(voteNotEmittedSuperseded))
		})
	}
}

func TestVoteManagerScheduledVoteAfterRollbackSignsNothing(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		onTimeVoteTiming,
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset
	h.announce()
	require.Len(t, h.fire, 1)
	signed := h.signHook(nil)

	h.fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
	})
	h.clock = onset.Add(voteWindowOpens)
	h.fire[0]()
	assert.False(t, h.voted())
	assert.Zero(t, *signed, "a rolled-back announcement was signed")
	assert.Equal(t, float64(1), h.notEmitted(voteNotEmittedSuperseded))
	assert.Equal(t, float64(0), h.notEmitted(voteNotEmittedNotInserted))
}

// A competing header can arrive while the vote is being signed outside the
// state lock, so the commit reapplies the CIP-164 chain-choice rules and
// counts the reason rather than a generic refusal.
func TestVoteManagerCommitRechecksChainChoice(t *testing.T) {
	t.Parallel()
	issuer := lcommon.PoolKeyHash{0x01}
	cases := map[string]struct {
		during func(*VoteManager)
		reason string
	}{
		"newer RB": {
			during: func(m *VoteManager) {
				m.handleChainHeaderObserved(chain.ChainHeaderObservedEvent{
					Slot:   headerArmingRbSlot,
					RbHash: lcommon.NewBlake2b256([]byte("same-slot-rb")),
					Issuer: lcommon.PoolKeyHash{0x02},
				})
			},
			reason: voteNotEmittedNotLatestRb,
		},
		"RB equivocation": {
			during: func(m *VoteManager) {
				for _, name := range []string{"equivocal-a", "equivocal-b"} {
					m.handleChainHeaderObserved(chain.ChainHeaderObservedEvent{
						Slot:   headerArmingRbSlot,
						RbHash: lcommon.NewBlake2b256([]byte(name)),
						Issuer: issuer,
					})
				}
			},
			reason: voteNotEmittedRbEquivocation,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			onset := time.Unix(1_700_000_000, 0)
			h := newVoteWindowHarness(
				t,
				onTimeVoteTiming,
				fakeSlotOnset{onset: onset},
			)
			h.clock = onset.Add(voteWindowOpens)
			signed := h.signHook(func() { tc.during(h.fixture.mgr) })

			h.announce()
			assert.Equal(t, 1, *signed, "the rules held when signing began")
			assert.False(t, h.voted())
			assert.Equal(t, float64(1), h.notEmitted(tc.reason))
			assert.Equal(t, float64(0), h.notEmitted(voteNotEmittedNotInserted))
		})
	}
}
