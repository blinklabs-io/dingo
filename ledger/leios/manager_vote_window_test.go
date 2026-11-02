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

type fakeVoteTiming struct {
	opens, window time.Duration
	err           error
}

func (f fakeVoteTiming) LeiosVoteTiming() (time.Duration, time.Duration, error) {
	return f.opens, f.window, f.err
}

type fakeSlotOnset struct {
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
	voteWindowOpens  = 3 * time.Second
	voteWindowLength = 4 * time.Second
)

func newVoteWindowHarness(
	t *testing.T,
	timing fakeVoteTiming,
	onset fakeSlotOnset,
) *voteWindowHarness {
	t.Helper()
	h := &voteWindowHarness{onset: onset.onset}
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	h.fixture = newHeaderArmingFixture(
		t,
		slots,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.PromRegistry = prometheus.NewRegistry()
			cfg.VoteTimingProvider = timing
			cfg.SlotOnsetProvider = onset
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

func (h *voteWindowHarness) announce() (lcommon.Blake2b256, lcommon.Blake2b256) {
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	h.fixture.mgr.HandleValidatedEndorserBlock(
		headerArmingRbSlot, rbHash, ebHash,
	)
	h.fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)
	return rbHash, ebHash
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
		fakeVoteTiming{opens: voteWindowOpens, window: voteWindowLength},
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
		fakeVoteTiming{opens: voteWindowOpens, window: voteWindowLength},
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset.Add(voteWindowOpens + voteWindowLength)

	h.announce()
	assert.True(t, h.voted(), "the deadline itself is still inside the window")
	assert.Empty(t, h.armed)
}

func TestVoteManagerDropsVoteAfterWindowCloses(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		fakeVoteTiming{opens: voteWindowOpens, window: voteWindowLength},
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset.Add(voteWindowOpens + voteWindowLength + time.Millisecond)

	h.announce()
	assert.False(t, h.voted())
	assert.Empty(t, h.armed, "a closed window arms no timer")
	assert.Equal(t, float64(1), h.notEmitted(voteNotEmittedTooLate))
}

func TestVoteManagerFallsBackToImmediateVoteWithoutWindow(t *testing.T) {
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
			timing: fakeVoteTiming{
				opens: voteWindowOpens, window: voteWindowLength,
			},
			onset: fakeSlotOnset{err: errors.New("past the horizon")},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newVoteWindowHarness(t, tc.timing, tc.onset)
			h.clock = onset
			h.announce()
			assert.True(t, h.voted())
			assert.Empty(t, h.armed)
		})
	}
}

func TestVoteManagerStopCancelsScheduledVotes(t *testing.T) {
	t.Parallel()
	onset := time.Unix(1_700_000_000, 0)
	h := newVoteWindowHarness(
		t,
		fakeVoteTiming{opens: voteWindowOpens, window: voteWindowLength},
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
		fakeVoteTiming{opens: voteWindowOpens, window: voteWindowLength},
		fakeSlotOnset{onset: onset},
	)
	h.clock = onset.Add(voteWindowOpens + voteWindowLength)
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
				fakeVoteTiming{
					opens: voteWindowOpens, window: voteWindowLength,
				},
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
		fakeVoteTiming{opens: voteWindowOpens, window: voteWindowLength},
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
