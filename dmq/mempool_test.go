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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newTestMessage builds a well-formed DmqMessage with no message ID set, so
// Add computes one from the payload, matching real submissions.
func newTestMessage(body []byte, expiresAt uint32) ocommon.DmqMessage {
	return ocommon.DmqMessage{
		Payload: ocommon.DmqMessagePayload{
			MessageBody: body,
			KESPeriod:   1,
			ExpiresAt:   expiresAt,
		},
		KESSignature: make([]byte, 448),
		OperationalCertificate: ocommon.OperationalCertificate{
			KESVerificationKey: make([]byte, 32),
			IssueNumber:        1,
			KESPeriod:          1,
			ColdSignature:      make([]byte, 64),
		},
		ColdVerificationKey: make([]byte, 32),
	}
}

func futureExpiry(t *testing.T) uint32 {
	t.Helper()
	// #nosec G115 -- test fixture timestamp, far from the uint32 rollover
	return uint32(time.Now().Add(time.Hour).Unix())
}

func TestMessageMempool_AddComputesIDAndRoundTrips(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	msg := newTestMessage([]byte("hello dmq"), futureExpiry(t))

	wantID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)
	require.Len(t, wantID, 32)

	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	got, ok := mp.Get(wantID)
	require.True(t, ok)
	require.Equal(t, wantID, got.ID())
	require.Equal(t, msg.Payload.MessageBody, got.Payload.MessageBody)

	// Round-trip: what the pool holds must encode identically to the same
	// fields marshaled directly, and decoding those bytes must reproduce
	// the same payload and ID.
	msg.SetMessageID(wantID)
	wantEncoded, err := msg.MarshalCBOR()
	require.NoError(t, err)
	gotEncoded, err := got.MarshalCBOR()
	require.NoError(t, err)
	require.Equal(t, wantEncoded, gotEncoded)

	var decoded ocommon.DmqMessage
	_, err = cbor.Decode(gotEncoded, &decoded)
	require.NoError(t, err)
	require.Equal(t, wantID, decoded.ID())
	require.Equal(t, msg.Payload.MessageBody, decoded.Payload.MessageBody)
}

func TestMessageMempool_AddClonesStoredMessage(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	body := []byte("original")
	msg := newTestMessage(body, futureExpiry(t))
	wantID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)

	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	// Mutate the caller's own backing array after Add returns. The pool
	// must own an independent copy, not an alias into it.
	body[0] = 'X'

	got, ok := mp.Get(wantID)
	require.True(t, ok)
	require.Equal(t, []byte("original"), got.Payload.MessageBody)
}

func TestMessageMempool_GetReturnsIndependentCopy(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	msg := newTestMessage([]byte("original"), futureExpiry(t))
	wantID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)

	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	got, ok := mp.Get(wantID)
	require.True(t, ok)
	got.Payload.MessageBody[0] = 'X'

	// A second, independent Get must not observe the mutation above: Get
	// must return a copy the caller cannot use to corrupt retained data.
	got2, ok := mp.Get(wantID)
	require.True(t, ok)
	require.Equal(t, []byte("original"), got2.Payload.MessageBody)
}

func TestMessageMempool_AddDedup(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	msg := newTestMessage([]byte("dup"), futureExpiry(t))

	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	added, err = mp.Add(msg)
	require.NoError(t, err)
	require.False(t, added)

	require.Equal(t, 1, mp.Len())
}

func TestMessageMempool_AddInvalidMessageID(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	msg := newTestMessage([]byte("bad id"), futureExpiry(t))
	msg.SetMessageID([]byte{1, 2, 3})

	added, err := mp.Add(msg)
	require.False(t, added)
	require.ErrorIs(t, err, ErrInvalidMessageID)
	require.Equal(t, 0, mp.Len())
}

func TestMessageMempool_AddRejectsMismatchedMessageID(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	msg := newTestMessage([]byte("mismatched"), futureExpiry(t))
	wrongID := make([]byte, 32)
	wrongID[0] = 0xAB
	msg.SetMessageID(wrongID)

	added, err := mp.Add(msg)
	require.False(t, added)
	require.ErrorIs(t, err, ErrInvalidMessageID)
	require.Equal(t, 0, mp.Len())
}

func TestMessageMempool_AddExpired(t *testing.T) {
	t.Parallel()

	fixedNow := time.Unix(2_000_000_000, 0)
	mp := NewMessageMempool(Config{
		Now: func() time.Time { return fixedNow },
	})
	// #nosec G115 -- fixed test timestamp
	msg := newTestMessage(
		[]byte("expired"),
		uint32(fixedNow.Add(-time.Minute).Unix()),
	)

	added, err := mp.Add(msg)
	require.False(t, added)
	require.ErrorIs(t, err, ErrExpired)
	require.Equal(t, 0, mp.Len())
}

func TestMessageMempool_AddRechecksExpiryUnderLock(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	now := time.Unix(2_000_000_000, 0)
	nowFn := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	mp := NewMessageMempool(Config{Now: nowFn})
	// Simulate the message's expiresAt passing in the window between Add's
	// early (unlocked) expiry check and the write lock that actually admits
	// it -- a race otherwise too narrow to hit deterministically.
	mp.testBeforeLock = func() {
		mu.Lock()
		now = now.Add(2 * time.Minute)
		mu.Unlock()
	}

	mu.Lock()
	// #nosec G115 -- fixed test timestamp
	expiresAt := uint32(now.Add(time.Minute).Unix())
	mu.Unlock()
	msg := newTestMessage([]byte("race"), expiresAt)

	added, err := mp.Add(msg)
	require.False(t, added)
	require.ErrorIs(t, err, ErrExpired)
	require.Equal(t, 0, mp.Len())
}

func TestMessageMempool_AddFullByMaxMessages(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{MaxMessages: 1})
	expiry := futureExpiry(t)

	added, err := mp.Add(newTestMessage([]byte("one"), expiry))
	require.NoError(t, err)
	require.True(t, added)

	added, err = mp.Add(newTestMessage([]byte("two"), expiry))
	require.False(t, added)
	require.ErrorIs(t, err, ErrFull)
	require.Equal(t, 1, mp.Len())
}

func TestMessageMempool_AddFullByCapacity(t *testing.T) {
	t.Parallel()

	msg := newTestMessage([]byte("size probe"), futureExpiry(t))
	encoded, err := msg.MarshalCBOR()
	require.NoError(t, err)

	mp := NewMessageMempool(Config{Capacity: int64(len(encoded)) - 1})

	added, err := mp.Add(msg)
	require.False(t, added)
	require.ErrorIs(t, err, ErrFull)
	require.Equal(t, int64(0), mp.SizeBytes())
}

func TestMessageMempool_AddPublishesEvent(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	mp := NewMessageMempool(Config{EventBus: bus})
	_, addCh := bus.Subscribe(AddMessageEventType)

	msg := newTestMessage([]byte("event"), futureExpiry(t))
	wantID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)

	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	evt := testutil.RequireReceive(
		t, addCh, time.Second, "add event for new message",
	)
	data, ok := evt.Data.(AddMessageEvent)
	require.True(t, ok)
	require.Equal(t, wantID, data.MessageID)
}

func TestMessageMempool_TTLExpirySweepsAndPublishes(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	now := time.Unix(2_000_000_000, 0)
	nowFn := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	mp := NewMessageMempool(Config{
		Now:             nowFn,
		CleanupInterval: 10 * time.Millisecond,
		EventBus:        bus,
	})
	_, removeCh := bus.Subscribe(RemoveMessageEventType)

	mp.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, mp.Stop(ctx))
	})

	mu.Lock()
	// #nosec G115 -- fixed test timestamp
	expiresAt := uint32(now.Add(time.Minute).Unix())
	mu.Unlock()
	msg := newTestMessage([]byte("ttl"), expiresAt)
	wantID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)

	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, mp.Len())

	mu.Lock()
	now = now.Add(2 * time.Minute)
	mu.Unlock()

	testutil.WaitForCondition(t, func() bool {
		return mp.Len() == 0
	}, time.Second, "expired message must be swept from the pool")
	require.Equal(t, int64(0), mp.SizeBytes())

	evt := testutil.RequireReceive(
		t, removeCh, time.Second, "remove event for expired message",
	)
	data, ok := evt.Data.(RemoveMessageEvent)
	require.True(t, ok)
	require.Equal(t, wantID, data.MessageID)
}

func TestMessageMempool_NextForPeerFIFOCursorAdvancement(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	expiry := futureExpiry(t)

	bodies := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	ids := make([][]byte, len(bodies))
	for i, body := range bodies {
		msg := newTestMessage(body, expiry)
		id, err := ocommon.ComputeDmqMessageID(msg.Payload)
		require.NoError(t, err)
		ids[i] = id

		added, err := mp.Add(msg)
		require.NoError(t, err)
		require.True(t, added)
	}

	// A fresh peer walks the log in arrival order.
	for i, wantID := range ids {
		got, ok := mp.NextForPeer("peer-a")
		require.Truef(t, ok, "message %d", i)
		require.Equal(t, wantID, got.ID())
	}
	// The peer has caught up: no more messages until something new arrives.
	_, ok := mp.NextForPeer("peer-a")
	require.False(t, ok)

	// A second peer's cursor is independent and starts from the beginning.
	first, ok := mp.NextForPeer("peer-b")
	require.True(t, ok)
	require.Equal(t, ids[0], first.ID())

	// New arrivals appear only after messages peer-a already consumed.
	msg := newTestMessage([]byte("fourth"), expiry)
	fourthID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)
	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	got, ok := mp.NextForPeer("peer-a")
	require.True(t, ok)
	require.Equal(t, fourthID, got.ID())

	_, ok = mp.NextForPeer("peer-a")
	require.False(t, ok)
}

func TestMessageMempool_RemovePeerResetsCursor(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{})
	expiry := futureExpiry(t)

	msg := newTestMessage([]byte("only"), expiry)
	wantID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)
	added, err := mp.Add(msg)
	require.NoError(t, err)
	require.True(t, added)

	got, ok := mp.NextForPeer("peer-a")
	require.True(t, ok)
	require.Equal(t, wantID, got.ID())
	_, ok = mp.NextForPeer("peer-a")
	require.False(t, ok)

	mp.RemovePeer("peer-a")

	got, ok = mp.NextForPeer("peer-a")
	require.True(t, ok)
	require.Equal(t, wantID, got.ID())
}

func TestMessageMempool_StopIsIdempotentAndBounded(t *testing.T) {
	t.Parallel()

	mp := NewMessageMempool(Config{CleanupInterval: 5 * time.Millisecond})
	mp.Start()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, mp.Stop(ctx))
	require.NoError(t, mp.Stop(ctx))
}
