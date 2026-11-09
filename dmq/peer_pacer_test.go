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

package dmq

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// poolMessage returns a message signed by the pool with the given cold key
// byte.
func poolMessage(t *testing.T, body string, pool byte) ocommon.DmqMessage {
	t.Helper()
	msg := idedMessage(t, body, soonExpiry())
	msg.ColdVerificationKey = bytes.Repeat([]byte{pool}, 32)
	return msg
}

func newPacedPeer(
	t *testing.T,
	interval time.Duration,
	now *time.Time,
) (*PeerConn, *Stack) {
	t.Helper()
	s, _ := newTestStack(t, StackConfig{
		Now:                    func() time.Time { return *now },
		MinPoolMessageInterval: interval,
	})
	p := &PeerConn{
		stack:     s,
		logger:    s.logger,
		cursorKey: "unit",
		known:     newIDSet(8),
		pace:      newPoolPacer(s.minPoolInterval()),
	}
	t.Cleanup(func() { s.pool.RemovePeer(p.cursorKey) })
	return p, s
}

func announcedBodies(t *testing.T, s *Stack, ids []ocommon.MessageIDAndSize) []string {
	t.Helper()
	bodies := make([]string, 0, len(ids))
	for _, item := range ids {
		msg, ok := s.pool.Get(item.MessageID)
		require.True(t, ok)
		bodies = append(bodies, string(msg.Payload.MessageBody))
	}
	return bodies
}

func TestPeerHoldsASecondMessageOfOnePoolUntilTheIntervalPasses(t *testing.T) {
	t.Parallel()
	now := time.Now()
	p, s := newPacedPeer(t, time.Minute, &now)
	for _, m := range []ocommon.DmqMessage{
		poolMessage(t, "a1", 1),
		poolMessage(t, "a2", 1),
	} {
		_, err := s.pool.Add(m)
		require.NoError(t, err)
	}

	require.Equal(t, []string{"a1"}, announcedBodies(t, s, p.nextMessageIDs(10)))
	wait, held := p.pace.nextRelease(now)
	require.True(t, held)
	require.Equal(t, time.Minute+poolPaceSlack, wait)

	now = now.Add(time.Minute)
	require.Empty(
		t, p.nextMessageIDs(10),
		"the interval alone is not enough: the slack is part of the hold",
	)

	now = now.Add(poolPaceSlack)
	require.Equal(t, []string{"a2"}, announcedBodies(t, s, p.nextMessageIDs(10)))
	_, held = p.pace.nextRelease(now)
	require.False(t, held)
}

func TestPeerAnnouncesOtherPoolsPastAHeldMessage(t *testing.T) {
	t.Parallel()
	now := time.Now()
	p, s := newPacedPeer(t, time.Minute, &now)
	for _, m := range []ocommon.DmqMessage{
		poolMessage(t, "a1", 1),
		poolMessage(t, "a2", 1),
		poolMessage(t, "b1", 2),
	} {
		_, err := s.pool.Add(m)
		require.NoError(t, err)
	}

	require.Equal(
		t,
		[]string{"a1", "b1"},
		announcedBodies(t, s, p.nextMessageIDs(10)),
	)
}

func TestPeerReleasesHeldMessagesOfOnePoolOnePerInterval(t *testing.T) {
	t.Parallel()
	now := time.Now()
	p, s := newPacedPeer(t, time.Minute, &now)
	for _, m := range []ocommon.DmqMessage{
		poolMessage(t, "a1", 1),
		poolMessage(t, "a2", 1),
		poolMessage(t, "a3", 1),
	} {
		_, err := s.pool.Add(m)
		require.NoError(t, err)
	}

	require.Equal(t, []string{"a1"}, announcedBodies(t, s, p.nextMessageIDs(10)))
	now = now.Add(time.Minute + poolPaceSlack)
	require.Equal(t, []string{"a2"}, announcedBodies(t, s, p.nextMessageIDs(10)))
	now = now.Add(time.Minute + poolPaceSlack)
	require.Equal(t, []string{"a3"}, announcedBodies(t, s, p.nextMessageIDs(10)))
}

func TestPeerKeepsAHeldMessageThatDoesNotFitTheRequest(t *testing.T) {
	t.Parallel()
	now := time.Now()
	p, s := newPacedPeer(t, time.Minute, &now)
	for _, m := range []ocommon.DmqMessage{
		poolMessage(t, "a1", 1),
		poolMessage(t, "a2", 1),
		poolMessage(t, "b1", 2),
		poolMessage(t, "b2", 2),
	} {
		_, err := s.pool.Add(m)
		require.NoError(t, err)
	}
	require.Equal(
		t, []string{"a1", "b1"}, announcedBodies(t, s, p.nextMessageIDs(10)),
	)

	now = now.Add(time.Minute + poolPaceSlack)
	require.Equal(t, []string{"a2"}, announcedBodies(t, s, p.nextMessageIDs(1)))
	require.Equal(t, []string{"b2"}, announcedBodies(t, s, p.nextMessageIDs(1)))
}

func TestPeerDoesNotSpaceMessagesWhenPacingIsOff(t *testing.T) {
	t.Parallel()
	now := time.Now()
	p, s := newPacedPeer(t, -1, &now)
	for _, m := range []ocommon.DmqMessage{
		poolMessage(t, "a1", 1),
		poolMessage(t, "a2", 1),
	} {
		_, err := s.pool.Add(m)
		require.NoError(t, err)
	}
	require.Nil(t, p.pace)
	require.Equal(
		t, []string{"a1", "a2"}, announcedBodies(t, s, p.nextMessageIDs(10)),
	)
}

func TestStackPacesByDefault(t *testing.T) {
	t.Parallel()
	s := &Stack{}
	require.Equal(t, DefaultMinPoolMessageInterval, s.minPoolInterval())
}

// A blocking request that only has held messages to answer with waits for
// the first to fall due, rather than for an admission that may never come.
func TestPeerAnnouncesAHeldMessageOnABlockingRequestWhenItFallsDue(t *testing.T) {
	t.Parallel()
	const interval = 2 * time.Second
	a, _ := newTestStack(t, StackConfig{MinPoolMessageInterval: interval})
	b, _ := newTestStack(t, StackConfig{})
	// The wait for an admission would outlast the test, so only the held
	// message's own deadline can end it in time.
	a.peerTune.blockingReplyWait = time.Hour
	for _, m := range []ocommon.DmqMessage{
		poolMessage(t, "a1", 1),
		poolMessage(t, "a2", 1),
	} {
		_, err := a.pool.Add(m)
		require.NoError(t, err)
	}

	_, pb := attachPair(t, a, b)
	waitFor(t, "b receives the first message", func() bool {
		return b.pool.Len() == 1
	})
	require.Never(t, func() bool { return b.pool.Len() > 1 },
		300*time.Millisecond, 10*time.Millisecond,
		"the second message of the same pool must wait for the interval")
	waitFor(t, "b receives the second message", func() bool {
		return b.pool.Len() == 2
	})
	require.False(t, closed(pb.Done()))
}

// An expired held message is dropped without spending its pool's interval.
func TestPeerDropsAnExpiredHeldMessageWithoutDelayingItsPool(t *testing.T) {
	t.Parallel()
	now := time.Now()
	p, s := newPacedPeer(t, time.Minute, &now)
	// #nosec G115 -- test fixture timestamp
	soon := uint32(now.Add(90 * time.Second).Unix())
	first := poolMessage(t, "a1", 1)
	short := newTestMessage([]byte("a2"), soon)
	short.ColdVerificationKey = bytes.Repeat([]byte{1}, 32)
	last := poolMessage(t, "a3", 1)
	for _, m := range []ocommon.DmqMessage{first, short, last} {
		_, err := s.pool.Add(m)
		require.NoError(t, err)
	}
	require.Equal(t, []string{"a1"}, announcedBodies(t, s, p.nextMessageIDs(10)))

	// a2 expires while held. Dropping it must not spend the pool's interval,
	// or a3 would wait another minute.
	now = now.Add(95 * time.Second)
	require.Equal(t, []string{"a3"}, announcedBodies(t, s, p.nextMessageIDs(10)))
}

// One pool's backlog is not copied into a connection without bound.
func TestPeerStopsReadingAheadWhenTooManyMessagesAreHeld(t *testing.T) {
	t.Parallel()
	now := time.Now()
	p, s := newPacedPeer(t, time.Minute, &now)
	for i := range maxHeldMessages + 50 {
		_, err := s.pool.Add(poolMessage(t, fmt.Sprintf("a%d", i), 1))
		require.NoError(t, err)
	}
	require.Len(t, p.nextMessageIDs(10), 1)
	require.True(t, p.pace.full())
	require.Len(t, p.pace.held, maxHeldMessages)
}
