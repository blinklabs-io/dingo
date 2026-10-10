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

package ouroboros

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// TestClassifyLeiosFetchFailure pins the failure taxonomy the by-point backfill
// reacts to. Previously every one of these outcomes was folded into a
// single undifferentiated error with a single cooldown, so the one class that
// requires the connection to be replaced (a permanently abandoned request slot)
// was instead cooled down and retried for the life of the connection.
func TestClassifyLeiosFetchFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want leiosFetchFailureClass
	}{
		{"success", nil, leiosFetchFailureNone},
		{
			"busy",
			errLeiosBackfillConnBusy,
			leiosFetchFailureBusy,
		},
		{
			"busy wrapped",
			errors.Join(errLeiosBackfillConnBusy),
			leiosFetchFailureBusy,
		},
		{
			"abandoned slot is a dead connection",
			leiosfetch.ErrRequestSlotAbandoned,
			leiosFetchFailureDead,
		},
		{
			"abandoned slot wrapped by the tx fetch",
			// The shape fetchEndorserBlockOnConn actually returns.
			errors.Join(
				errors.New("tx fetch (0/1)"),
				leiosfetch.ErrRequestSlotAbandoned,
			),
			leiosFetchFailureDead,
		},
		{
			"protocol shutting down is a dead connection",
			protocol.ErrProtocolShuttingDown,
			leiosFetchFailureDead,
		},
		{
			"deadline is transient",
			context.DeadlineExceeded,
			leiosFetchFailureTransient,
		},
		{
			"wrong bytes are transient",
			errors.New("leios endorser block cache: point hash mismatch"),
			leiosFetchFailureTransient,
		},
	} {
		require.Equalf(
			t,
			tc.want,
			classifyLeiosFetchFailure(tc.err),
			"class for %s",
			tc.name,
		)
	}
}

// TestLeiosBackfillAttemptBudget verifies the per-connection attempt budget is
// derived from the candidates still to be tried. The multi-peer case keeps the
// per-attempt bound; the single-candidate case -- the normal shape of a
// topology with one Leios relay -- gets the whole remaining budget instead of
// having its only attempt truncated at 30s with nothing to fail over to.
func TestLeiosBackfillAttemptBudget(t *testing.T) {
	t.Parallel()
	require.Equal(
		t,
		2*time.Minute,
		leiosBackfillAttemptBudget(2*time.Minute, 1),
		"the last remaining candidate gets the whole remainder",
	)
	require.Equal(
		t,
		leiosBackfillPerAttemptTimeout,
		leiosBackfillAttemptBudget(2*time.Minute, 4),
		"four candidates split a two-minute budget at the #2819 bound",
	)
	require.Equal(
		t,
		leiosBackfillPerAttemptTimeout,
		leiosBackfillAttemptBudget(2*time.Minute, 16),
		"the per-attempt floor holds when the split would be smaller",
	)
	require.Equal(
		t,
		5*time.Second,
		leiosBackfillAttemptBudget(5*time.Second, 16),
		"a budget below the floor is never overspent",
	)
	require.Zero(t, leiosBackfillAttemptBudget(0, 1))
	require.Zero(t, leiosBackfillAttemptBudget(-time.Second, 3))
}

// TestLeiosBackfillConnOrderPutsDeadConnectionsLast verifies a connection whose
// leios-fetch protocol is dead is tried after every other partition, including
// cooled-down ones: attempting it can only burn the caller's grace period. It is
// ordered last rather than excluded so a misdiagnosis cannot black out backfill.
func TestLeiosBackfillConnOrderPutsDeadConnectionsLast(t *testing.T) {
	t.Parallel()
	now := time.Now()
	dead := namedConnId("dead")
	cooled := namedConnId("cooled")
	fresh := namedConnId("fresh")
	deadGuard := &leiosFetchGuard{}
	cooledGuard := &leiosFetchGuard{}
	freshGuard := &leiosFetchGuard{}
	guards := map[gouroboros.ConnectionId]*leiosFetchGuard{
		dead:   deadGuard,
		cooled: cooledGuard,
		fresh:  freshGuard,
	}
	guardFor := func(id gouroboros.ConnectionId) *leiosFetchGuard {
		return guards[id]
	}
	deadGuard.markProtocolDead()
	cooledGuard.markFetchFailed(now, leiosBackfillConnCooldown)

	order := leiosBackfillConnOrder(
		[]gouroboros.ConnectionId{dead, cooled, fresh},
		0,
		now,
		leiosBackfillAffinityWindow,
		guardFor,
	)
	require.Equal(
		t,
		[]gouroboros.ConnectionId{fresh, cooled, dead},
		order,
	)
}

// leiosCertifiedRecoveryFixture builds a one-transaction endorser block, its
// manifest and the request bitmap that fetches it.
func leiosCertifiedRecoveryFixture(
	t *testing.T,
	seed byte,
	slot uint64,
) (cbor.RawMessage, []byte, ocommon.Point, map[uint16]uint64) {
	t.Helper()
	tx, ref := testLeiosManifestTx(t, seed)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(slot, lcommon.Blake2b256Hash(manifestRaw).Bytes())
	return tx, manifestRaw, point, map[uint16]uint64{0: 1 << 63}
}

// poisonLeiosFetchBlockTxsSlot leaves conn's leios-fetch block-txs request slot
// permanently abandoned, exactly as a by-point attempt whose deadline expires
// before the relay answers does. Do not probe the slot here: the next request's
// ErrRequestSlotAbandoned is what fails the connection, and the production path
// under test must be the caller that observes and classifies it.
func poisonLeiosFetchBlockTxsSlot(
	t *testing.T,
	conn *gouroboros.Connection,
	point ocommon.Point,
	bitmap map[uint16]uint64,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()
	resp, err := conn.LeiosFetch().Client.BlockTxsRequest(ctx, point, bitmap)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, resp)
}

// TestFetchEndorserBlockByPointRecyclesDeadConnectionAndFailsOver is the
// unavailable-certified-EB recovery path.
//
// A connection whose leios-fetch request slot is permanently abandoned can
// never answer again, so a cooldown only re-tries a corpse: the connection has
// to be replaced. This asserts the fetch (a) fails over to a healthy peer and
// makes the endorser block available to the ledger provider, (b) diagnoses the
// dead connection and asks the connection manager to recycle it so peer
// governance dials a replacement, and (c) does not publish another recycle
// request after that connection has been removed.
func TestFetchEndorserBlockByPointRecyclesDeadConnectionAndFailsOver(
	t *testing.T,
) {
	t.Parallel()

	tx, manifestRaw, point, bitmap := leiosCertifiedRecoveryFixture(
		t,
		0x52,
		376038,
	)
	// A second, still-incomplete endorser block, so the duplicate-recycle
	// assertion below drives a real second fetch after the dead connection is
	// removed. A re-fetch of the first block would return from the complete-cache
	// fast path and prove only that a cache hit publishes nothing.
	_, manifestRaw2, point2, bitmap2 := leiosCertifiedRecoveryFixture(
		t, 0x62, 376138,
	)

	deadConn, deadDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	healthyConn, healthyDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlockTxsFull(
						point,
						bitmap,
						[]cbor.RawMessage{tx},
					),
				},
			},
			// The second endorser block: this peer cannot serve it, and the
			// leios-fetch protocol has no absence reply, so it accepts the
			// request and never answers.
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(deadConn, false, "dead"))
	require.True(t, cm.AddConnection(healthyConn, false, "healthy"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	recycled := make(chan ledger.ConnectionRecycleRequestedEvent, 4)
	bus.SubscribeFunc(
		ledger.ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
			if !ok {
				return
			}
			recycled <- e
		},
	)

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EventBus:    bus,
		EnableLeios: true,
	})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			manifestRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point2,
			manifestRaw2,
			nil,
			leiosStoreAuthoritative,
		),
	)
	poisonLeiosFetchBlockTxsSlot(t, deadConn, point, bitmap)
	// Synchronize with the mock accepting the unanswered request. The slot is
	// now abandoned, but the connection remains live until the production fetch
	// below diagnoses it.
	requireLeiosFetchConversationDone(t, deadDone)

	// Make the dead connection the first candidate, so the fetch cannot succeed
	// by simply preferring the healthy peer.
	o.leiosFetchGuardFor(deadConn.Id()).markFetchOK()
	require.NoError(
		t,
		o.FetchEndorserBlockByPoint(
			context.Background(),
			point.Slot,
			point.Hash,
		),
	)

	ledgerTxs, ok := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, ok, "ledger provider still reports the EB unavailable")
	require.Equal(t, []cbor.RawMessage{tx}, ledgerTxs)

	evt := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"no recycle request for the dead leios-fetch connection",
	)
	require.Equal(t, deadConn.Id(), evt.ConnectionId)
	require.Equal(t, "leios_fetch_request_slot_abandoned", evt.Reason)
	require.True(
		t,
		o.leiosFetchGuardFor(deadConn.Id()).isProtocolDead(),
		"dead connection was not diagnosed",
	)
	require.False(
		t,
		o.leiosFetchGuardFor(healthyConn.Id()).isProtocolDead(),
		"healthy connection was wrongly diagnosed as dead",
	)
	require.Eventually(
		t,
		func() bool { return cm.GetConnectionById(deadConn.Id()) == nil },
		2*time.Second,
		time.Millisecond,
		"diagnosed connection was not removed",
	)

	// The remaining peer cannot serve the second, still-incomplete endorser
	// block. It never answers, so its request slot is abandoned and the
	// connection is diagnosed dead and recycled like deadConn.
	poisonLeiosFetchBlockTxsSlot(t, healthyConn, point2, bitmap2)

	err := o.FetchEndorserBlockByPoint(
		context.Background(),
		point2.Slot,
		point2.Hash,
	)
	require.Error(t, err, "the second endorser block must not be servable")
	require.ErrorIs(t, err, leiosfetch.ErrRequestSlotAbandoned)

	evt2 := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"no recycle request for the second dead leios-fetch connection",
	)
	require.Equal(t, healthyConn.Id(), evt2.ConnectionId)
	require.Equal(t, "leios_fetch_request_slot_abandoned", evt2.Reason)

	// A third fetch must not raise another recycle request after peer
	// governance removes both diagnosed connections: the first endorser
	// block is already cached, so this is the already-diagnosed-connection
	// invariant on a fetch that never touches a connection at all.
	require.NoError(
		t,
		o.FetchEndorserBlockByPoint(context.Background(), point.Slot, point.Hash),
	)
	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"recycle request repeated for an already-diagnosed connection",
	)

	requireLeiosFetchConversationDone(t, healthyDone)
}

// TestFetchEndorserBlockByPointFailsWhenSolePeerCannotAnswer covers the
// terminal case: the only connected peer cannot serve this endorser block.
// The leios-fetch protocol has no absence reply, so that peer never answers,
// which on the wire is indistinguishable from a stalled connection. Its
// request slot is abandoned, so it is diagnosed dead and recycled, and the
// fetch fails with that error rather than succeeding or hanging.
func TestFetchEndorserBlockByPointFailsWhenSolePeerCannotAnswer(t *testing.T) {
	t.Parallel()

	_, manifestRaw, point, bitmap := leiosCertifiedRecoveryFixture(
		t, 0x53, 376039,
	)

	deadConn, deadDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(deadConn, false, "dead"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	recycled := make(chan ledger.ConnectionRecycleRequestedEvent, 4)
	bus.SubscribeFunc(
		ledger.ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			if e, ok := evt.Data.(ledger.ConnectionRecycleRequestedEvent); ok {
				recycled <- e
			}
		},
	)

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EventBus:    bus,
		EnableLeios: true,
	})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			manifestRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)
	poisonLeiosFetchBlockTxsSlot(t, deadConn, point, bitmap)
	requireLeiosFetchConversationDone(t, deadDone)

	err := o.FetchEndorserBlockByPoint(
		context.Background(),
		point.Slot,
		point.Hash,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, leiosfetch.ErrRequestSlotAbandoned)
	require.NotErrorIs(t, err, ledger.ErrEndorserBlockFetchNoPeer)

	evt := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"no recycle request for the sole dead leios-fetch connection",
	)
	require.Equal(t, deadConn.Id(), evt.ConnectionId)
	require.True(
		t,
		o.leiosFetchGuardFor(deadConn.Id()).isProtocolDead(),
		"sole unanswerable connection was not diagnosed dead",
	)
}

// TestFetchEndorserBlockByPointReportsNoPeer verifies a fetch with no
// leios-fetch connection reports ledger.ErrEndorserBlockFetchNoPeer, the cause
// the ledger pipeline keeps out of its deterministic-halt count (dingo#5026).
func TestFetchEndorserBlockByPointReportsNoPeer(t *testing.T) {
	t.Parallel()

	_, _, point, _ := leiosCertifiedRecoveryFixture(t, 0x54, 376040)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})

	err := o.FetchEndorserBlockByPoint(
		t.Context(),
		point.Slot,
		point.Hash,
	)
	require.ErrorIs(t, err, ledger.ErrEndorserBlockFetchNoPeer)
}

func TestFetchEndorserBlockByPointReportsNoPeerAfterDisconnect(t *testing.T) {
	t.Parallel()

	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})
	staleConnId := gouroboros.ConnectionId{}

	err := o.fetchEndorserBlockByPointWithConnections(
		t.Context(),
		376040,
		[]byte("eb-hash"),
		[]gouroboros.ConnectionId{staleConnId},
		func(gouroboros.ConnectionId) *gouroboros.Connection {
			return nil
		},
	)
	require.ErrorIs(t, err, ledger.ErrEndorserBlockFetchNoPeer)
}

// TestFetchEndorserBlockByPointHonoursCallerBudget verifies the by-point fetch
// does not outlive the context the ledger hands it. Block application waits for
// this fetch, so a fetch that ignored the budget would hold the apply loop past
// the window the caller reserved for it.
//
// The peer accepts the transaction request and never answers, so the fetch is
// parked inside the leios-fetch client when the caller's context is cancelled:
// this exercises cancellation of an in-flight request, not the pre-flight
// checks. The caller supplies no deadline, so a request context that did not
// derive from the caller would fall back to the two-minute total budget and
// park there.
func TestFetchEndorserBlockByPointHonoursCallerBudget(t *testing.T) {
	t.Parallel()

	_, manifestRaw, point, _ := leiosCertifiedRecoveryFixture(t, 0x54, 376040)

	stalledConn, _ := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			// Accepted and never answered.
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlock(cbor.RawMessage(manifestRaw)),
				},
			},
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(stalledConn, false, "stalled"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- o.FetchEndorserBlockByPoint(ctx, point.Slot, point.Hash)
	}()
	// The manifest lands in the cache before the transaction request is sent,
	// so its presence means the fetch has reached the request that stalls.
	testutil.WaitForCondition(
		t,
		func() bool {
			data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
			return ok && !data.completeTxCache()
		},
		2*time.Second,
		"by-point fetch never reached the stalled transaction request",
	)
	cancel()

	err := testutil.RequireReceive(
		t,
		done,
		5*time.Second,
		"by-point fetch ignored the cancelled caller context",
	)
	require.ErrorIs(t, err, context.Canceled)
	guard := o.leiosFetchGuardFor(stalledConn.Id())
	require.Zero(t, guard.consecutiveFailures.Load())
	require.False(t, guard.inCooldown(time.Now()))
}

// TestFetchEndorserBlockByPointDeadlineDoesNotCoolDownPeer verifies that a
// caller deadline is not mistaken for a peer failure. Ledger apply uses
// deadline-bounded contexts, so this must be covered separately from explicit
// cancellation: context.DeadlineExceeded is not context.Canceled.
func TestFetchEndorserBlockByPointDeadlineDoesNotCoolDownPeer(t *testing.T) {
	t.Parallel()

	_, manifestRaw, point, _ := leiosCertifiedRecoveryFixture(t, 0x56, 376042)
	stalledConn, _ := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlock(cbor.RawMessage(manifestRaw)),
				},
			},
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(stalledConn, false, "stalled"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- o.FetchEndorserBlockByPoint(ctx, point.Slot, point.Hash)
	}()
	testutil.WaitForCondition(
		t,
		func() bool {
			data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
			return ok && !data.completeTxCache()
		},
		2*time.Second,
		"by-point fetch never reached the stalled transaction request",
	)
	testutil.RequireReceive(
		t,
		ctx.Done(),
		3*time.Second,
		"caller deadline did not expire",
	)

	err := testutil.RequireReceive(
		t,
		done,
		5*time.Second,
		"by-point fetch ignored the caller deadline",
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	guard := o.leiosFetchGuardFor(stalledConn.Id())
	require.Zero(t, guard.consecutiveFailures.Load())
	require.False(t, guard.inCooldown(time.Now()))
}

// TestLeiosFetchRequestContextReusesParentAtEqualDeadline pins the boundary
// that made TestFetchEndorserBlockByPointDeadlineDoesNotCoolDownPeer flaky
// under load (observed on Windows CI): the last (or only) backfill candidate
// gets the caller's whole remaining budget, so its per-attempt deadline is
// computed to equal the caller's own context deadline exactly. context.
// WithDeadline only reuses a parent's own cancellation timer when the
// parent's deadline is *strictly* earlier than the requested one
// (cur.Before(d)); an equal deadline does not qualify, so without this
// boundary check leiosFetchRequestContext would arm a second, independent
// timer racing the caller's own. Under scheduling contention that second
// timer can fire fractionally before the caller's, so
// fetchEndorserBlockOnConn's ctx.Err() check (which reads the caller's
// context specifically) observes it as not-yet-expired and misattributes the
// caller's own deadline to the peer, incrementing consecutiveFailures on a
// connection that did nothing wrong.
//
// This does not race real timers to prove the point: whether an independent
// timer got armed is not black-box observable without either racing it or
// reflecting into context's unexported types, so the decision that prevents
// it is pinned directly instead.
func TestLeiosFetchRequestContextReusesParentAtEqualDeadline(t *testing.T) {
	t.Parallel()
	now := time.Now()
	for _, tc := range []struct {
		name              string
		parentDeadline    time.Time
		hasParentDeadline bool
		deadline          time.Time
		wantReusesParent  bool
	}{
		{
			name:              "no parent deadline always gets its own timer",
			hasParentDeadline: false,
			deadline:          now.Add(time.Second),
			wantReusesParent:  false,
		},
		{
			// The exact scenario that made the flake possible: the last/only
			// backfill candidate's attempt deadline equals the caller's own.
			name:              "equal deadline reuses parent",
			parentDeadline:    now,
			hasParentDeadline: true,
			deadline:          now,
			wantReusesParent:  true,
		},
		{
			name:              "parent deadline strictly earlier reuses parent",
			parentDeadline:    now.Add(-time.Second),
			hasParentDeadline: true,
			deadline:          now,
			wantReusesParent:  true,
		},
		{
			// A genuinely truncated multi-candidate attempt must still get
			// its own independent timer, preserving backfill failover:
			// ctx.Err() being nil for this attempt's
			// failure is correct, not a bug.
			name:              "deadline strictly earlier than parent needs its own timer",
			parentDeadline:    now,
			hasParentDeadline: true,
			deadline:          now.Add(-time.Second),
			wantReusesParent:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t,
				tc.wantReusesParent,
				leiosFetchRequestContextReusesParent(
					tc.parentDeadline,
					tc.hasParentDeadline,
					tc.deadline,
				),
			)
		})
	}
}

// leiosFetchRequestContextTestDeadline overrides Deadline() on top of an
// otherwise plain context, so a test can report an arbitrary parent deadline
// to leiosFetchRequestContext without wiring a real timer to it. Done/Err/
// Value are promoted from the embedded context untouched, so this parent can
// only ever become cancelled by an explicit call against the embedded
// context -- never by the reported deadline elapsing. That isolates
// leiosFetchRequestContext's own timer-arming decision: whatever it returns
// is the only thing in play that could expire on its own at the requested
// deadline.
type leiosFetchRequestContextTestDeadline struct {
	context.Context
	deadline time.Time
}

func (d leiosFetchRequestContextTestDeadline) Deadline() (time.Time, bool) {
	return d.deadline, true
}

// TestLeiosFetchRequestContextDoesNotArmIndependentTimerAtEqualOrEarlierParentDeadline
// calls the production leiosFetchRequestContext directly -- not just the
// leiosFetchRequestContextReusesParent predicate -- and proves by observation
// that it does not arm a second timer when the parent's deadline is equal to
// or earlier than the requested one.
//
// The "equal parent and requested deadline" case is the discriminating one:
// if leiosFetchRequestContext is changed to call
// context.WithDeadline(parent, deadline) unconditionally (reintroducing the
// equal-deadline bug), that subtest fails. The "parent deadline earlier" case
// pins the same contract but does not discriminate that particular mutation,
// because Go's own context.WithDeadline already takes the parent-reuse shortcut
// itself when the parent's deadline is *strictly* earlier (cur.Before(d)); it
// only misses it at cur == d, which is exactly the boundary this fix closes.
// Both subtests are kept because both are part of the contract
// leiosFetchRequestContextReusesParent states.
//
// context.WithDeadline and context.WithCancel report identical Deadline() and
// Cause() values once a context has actually been cancelled, so nothing
// observable after the fact distinguishes them (see leiosFetchRequestContext's
// doc comment), and racing the two real timers against each other is exactly
// the scenario that cannot be forced deterministically. This
// test sidesteps the race instead of trying to win it: the parent passed to
// leiosFetchRequestContext reports a deadline via
// leiosFetchRequestContextTestDeadline but has no timer of its own (it is a
// plain context.WithCancel, cancelled only by this test). Under the correct
// context.WithCancel(parent) branch, the returned request context inherits
// that same property -- no expiry mechanism of its own -- and cannot become
// Done before this test cancels the parent, however far the clock advances.
// Under the context.WithDeadline(parent, deadline) mutation, the returned
// context arms its own timer at deadline regardless of the parent's state, and
// synctest's fake clock reaching that instant cancels it deterministically
// (nothing else in the bubble is running to race against).
//
// Early parent cancellation does not discriminate the two branches (an
// independent timerCtx derived from the parent is cancelled by an early
// parent cancel too, since it is still registered as the parent's child) so
// this test does not rely on that; it relies on the parent surviving past the
// requested deadline uncancelled instead.
func TestLeiosFetchRequestContextDoesNotArmIndependentTimerAtEqualOrEarlierParentDeadline(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		parentDeadline time.Duration // relative to the bubble's fake start time
		deadline       time.Duration
	}{
		{
			// The exact boundary: the last/only backfill
			// candidate's attempt deadline equals the caller's own.
			name:           "equal parent and requested deadline",
			parentDeadline: 5 * time.Second,
			deadline:       5 * time.Second,
		},
		{
			// parent's own deadline is earlier than the requested one, so
			// parent already bounds the attempt without a second timer.
			name:           "parent deadline earlier than requested deadline",
			parentDeadline: 5 * time.Second,
			deadline:       8 * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				base, baseCancel := context.WithCancel(context.Background())
				defer baseCancel()
				parent := leiosFetchRequestContextTestDeadline{
					Context:  base,
					deadline: start.Add(tc.parentDeadline),
				}
				reqCtx, cancel := leiosFetchRequestContext(
					parent,
					start.Add(tc.deadline),
				)
				defer cancel()

				// Advance the fake clock past the requested deadline without
				// ever cancelling base. A correct context.WithCancel(parent)
				// result has no expiry of its own and must still be open.
				time.Sleep(tc.deadline + time.Second)
				require.NoError(
					t,
					reqCtx.Err(),
					"request context expired on its own at the requested "+
						"deadline even though its parent was never "+
						"cancelled -- an independent timer was armed",
				)

				// Confirm reqCtx is genuinely wired to parent (not simply
				// leaked/unreachable): cancelling parent now must still
				// cancel it.
				baseCancel()
				synctest.Wait()
				require.ErrorIs(t, reqCtx.Err(), context.Canceled)
			})
		})
	}
}

// TestFetchEndorserBlockByPointBusyCandidateIsNotCooledDown verifies that a
// candidate whose fetch guard is held by another fetch is skipped without
// counting as a failed attempt, while the other candidate's dead connection
// still fails the fetch.
func TestFetchEndorserBlockByPointBusyCandidateIsNotCooledDown(
	t *testing.T,
) {
	t.Parallel()

	_, manifestRaw, point, bitmap := leiosCertifiedRecoveryFixture(
		t, 0x56, 376042,
	)

	deadConn, deadDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	// Never asked anything: its fetch guard is held for the whole call.
	busyConn, busyDone := newLeiosFetchConversation(t, leiosFetchHandshake())
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(deadConn, false, "dead"))
	require.True(t, cm.AddConnection(busyConn, false, "busy"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			manifestRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)
	poisonLeiosFetchBlockTxsSlot(t, deadConn, point, bitmap)
	requireLeiosFetchConversationDone(t, deadDone)

	busyGuard := o.leiosFetchGuardFor(busyConn.Id())
	busyGuard.mu.Lock()

	err := o.FetchEndorserBlockByPoint(
		context.Background(),
		point.Slot,
		point.Hash,
	)
	busyGuard.mu.Unlock()
	require.Error(t, err)
	require.ErrorIs(t, err, leiosfetch.ErrRequestSlotAbandoned)
	require.False(
		t,
		busyGuard.inCooldown(time.Now()),
		"a busy connection is not a failed attempt",
	)
	requireLeiosFetchConversationDone(t, busyDone)
}

// TestFetchEndorserBlockByPointRotatesPastPeerThatNeverAnswers drives the
// not-served case through the production fetch. A peer that cannot serve an
// endorser block accepts the request and never answers, because the
// leios-fetch protocol has no absence reply. The first fetch must return
// within its caller's budget rather than hang on that peer; the next sweep,
// as the ledger's fetchRequired retry makes, must diagnose the connection dead
// from its abandoned request slot, recycle it, and obtain the endorser block
// from the peer that holds it.
func TestFetchEndorserBlockByPointRotatesPastPeerThatNeverAnswers(
	t *testing.T,
) {
	t.Parallel()

	tx, manifestRaw, point, bitmap := leiosCertifiedRecoveryFixture(
		t,
		0x58,
		376044,
	)
	silentConn, silentDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	servingConn, servingDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlockTxsFull(
						point,
						bitmap,
						[]cbor.RawMessage{tx},
					),
				},
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(silentConn, false, "silent"))
	require.True(t, cm.AddConnection(servingConn, false, "serving"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	recycled := make(chan ledger.ConnectionRecycleRequestedEvent, 4)
	bus.SubscribeFunc(
		ledger.ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
			if !ok {
				return
			}
			recycled <- e
		},
	)

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EventBus:    bus,
		EnableLeios: true,
	})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			manifestRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)
	// Recent success orders the silent peer first, so neither fetch can pass
	// by simply preferring the serving one.
	o.leiosFetchGuardFor(silentConn.Id()).markFetchOK()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		500*time.Millisecond,
	)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- o.FetchEndorserBlockByPoint(ctx, point.Slot, point.Hash)
	}()
	err := testutil.RequireReceive(
		t,
		done,
		5*time.Second,
		"by-point fetch hung on a peer that never answers",
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	requireLeiosFetchConversationDone(t, silentDone)
	_, ok := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.False(t, ok, "no peer served the endorser block yet")

	require.NoError(
		t,
		o.FetchEndorserBlockByPoint(
			context.Background(),
			point.Slot,
			point.Hash,
		),
	)
	ledgerTxs, ok := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, ok, "fetch did not rotate to the serving peer")
	require.Equal(t, []cbor.RawMessage{tx}, ledgerTxs)
	evt := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"no recycle request for the connection that never answered",
	)
	require.Equal(t, silentConn.Id(), evt.ConnectionId)
	require.Equal(t, "leios_fetch_request_slot_abandoned", evt.Reason)
	require.True(
		t,
		o.leiosFetchGuardFor(silentConn.Id()).isProtocolDead(),
		"connection that never answered was not diagnosed dead",
	)
	require.False(
		t,
		o.leiosFetchGuardFor(servingConn.Id()).isProtocolDead(),
		"serving connection was wrongly diagnosed dead",
	)
	requireLeiosFetchConversationDone(t, servingDone)
}
