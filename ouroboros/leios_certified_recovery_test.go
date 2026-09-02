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
// reacts to. Before dingo #3552 every one of these outcomes was folded into a
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
			"typed block decline",
			leiosfetch.ErrBlockNotFound,
			leiosFetchFailureDeclined,
		},
		{
			// MsgNoBlockTxs is not a definitive "I do not hold this endorser
			// block": dingo's own leios-fetch server answers it for a manifest
			// it holds with a still-incomplete transaction cache, so ordinary
			// in-progress diffusion arrives here looking like absence.
			"typed block-txs decline is not a definitive decline",
			leiosfetch.ErrBlockTxsNotFound,
			leiosFetchFailureTxsUnavailable,
		},
		{
			"block-txs decline wrapped by the tx fetch",
			errors.Join(
				errors.New("tx fetch (0/1)"),
				leiosfetch.ErrBlockTxsNotFound,
			),
			leiosFetchFailureTxsUnavailable,
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
// issue #2819 bound; the single-candidate case -- the normal shape of a topology
// with one Leios relay -- gets the whole remaining budget instead of having its
// only attempt truncated at 30s with nothing to fail over to.
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
	guards := map[gouroboros.ConnectionId]*leiosFetchGuard{
		dead:   {},
		cooled: {},
		fresh:  {},
	}
	guardFor := func(id gouroboros.ConnectionId) *leiosFetchGuard {
		return guards[id]
	}
	guards[dead].markProtocolDead()
	guards[cooled].markFetchFailed(now, leiosBackfillConnCooldown)

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
// before the relay answers does. Every later request on that connection then
// returns ErrRequestSlotAbandoned, for the life of the connection: this is the
// state that made a from-genesis sync wedge on an unavailable certified endorser
// block (dingo #3552).
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
	// Confirm the slot really is stuck, so the test below cannot pass for any
	// other reason.
	resp, err = conn.LeiosFetch().Client.BlockTxsRequest(
		context.Background(),
		point,
		bitmap,
	)
	require.ErrorIs(t, err, leiosfetch.ErrRequestSlotAbandoned)
	require.Nil(t, resp)
}

// TestFetchEndorserBlockByPointRecyclesDeadConnectionAndFailsOver is the
// unavailable-certified-EB recovery path for dingo #3552.
//
// A connection whose leios-fetch request slot is permanently abandoned can
// never answer again, so a cooldown only re-tries a corpse: the connection has
// to be replaced. This asserts the fetch (a) fails over to a healthy peer and
// makes the endorser block available to the ledger provider, (b) diagnoses the
// dead connection and asks the connection manager to recycle it so peer
// governance dials a replacement, and (c) publishes exactly one recycle request
// per connection however many fetches hit it.
func TestFetchEndorserBlockByPointRecyclesDeadConnectionAndFailsOver(
	t *testing.T,
) {
	tx, manifestRaw, point, bitmap := leiosCertifiedRecoveryFixture(t, 0x52, 376038)
	// A second, still-incomplete endorser block, so the duplicate-recycle
	// assertion below drives a real second fetch onto the dead connection. A
	// re-fetch of the first block would return from the complete-cache fast
	// path and prove only that a cache hit publishes nothing.
	_, manifestRaw2, point2, _ := leiosCertifiedRecoveryFixture(t, 0x62, 376138)

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
			// The second endorser block: this peer answers that it cannot
			// serve the transactions, so the fetch moves on to the dead
			// connection and takes the recycle path a second time.
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgNoBlockTxs(),
				},
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
	require.NoError(t, o.storeLeiosEndorserBlock(point, manifestRaw, nil))
	require.NoError(t, o.storeLeiosEndorserBlock(point2, manifestRaw2, nil))
	poisonLeiosFetchBlockTxsSlot(t, deadConn, point, bitmap)

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

	slot, ledgerTxs, ok := o.EndorserBlockTxsByHash(point.Hash)
	require.True(t, ok, "ledger provider still reports the EB unavailable")
	require.Equal(t, point.Slot, slot)
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

	// A second dead fetch must not raise a second recycle request for the same
	// connection. This one is for a different, still-incomplete endorser block,
	// so it cannot short-circuit on the cache: the healthy peer declines its
	// transactions and the dead connection is attempted again, returning
	// ErrRequestSlotAbandoned from its poisoned slot.
	err := o.FetchEndorserBlockByPoint(
		context.Background(),
		point2.Slot,
		point2.Hash,
	)
	require.Error(t, err, "the second endorser block must not be servable")
	require.ErrorIs(t, err, leiosfetch.ErrRequestSlotAbandoned)
	require.NotErrorIs(
		t,
		err,
		errLeiosEndorserBlockDeclinedByAllPeers,
		"a dead connection is not a peer declining to hold the block",
	)
	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"recycle request repeated for an already-diagnosed connection",
	)

	requireLeiosFetchConversationDone(t, deadDone)
	requireLeiosFetchConversationDone(t, healthyDone)
}

// TestFetchEndorserBlockByPointDeclinedByEveryPeer covers the terminal case:
// every connected peer answers the by-point request with a typed decline, so no
// peer holds this endorser block. That is not a broken connection and must not
// recycle it or install the long stalled-peer cooldown -- but the ledger has to
// be told, because it is the difference between "our peers are broken" and "the
// certified endorser block is not obtainable from anyone we are connected to".
func TestFetchEndorserBlockByPointDeclinedByEveryPeer(t *testing.T) {
	_, _, point, _ := leiosCertifiedRecoveryFixture(t, 0x53, 376039)

	// MsgNoBlock, not MsgNoBlockTxs: only a declined manifest request is a
	// definitive "I do not hold this endorser block". The manifest is
	// deliberately not pre-cached so the fetch issues the block request.
	decliningConn, decliningDone := newLeiosFetchConversation(
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
					leiosfetch.NewMsgNoBlock(),
				},
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(decliningConn, false, "declining"))
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

	err := o.FetchEndorserBlockByPoint(
		context.Background(),
		point.Slot,
		point.Hash,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errLeiosEndorserBlockDeclinedByAllPeers)
	require.ErrorIs(t, err, leiosfetch.ErrBlockNotFound)

	g := o.leiosFetchGuardFor(decliningConn.Id())
	require.False(
		t,
		g.isProtocolDead(),
		"a peer that declines correctly is not a dead connection",
	)
	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"a declining peer must not be recycled",
	)
	// The decline cooldown is short, so this peer stays a candidate for every
	// other endorser block instead of being sidelined for the stalled-peer
	// cooldown.
	require.True(t, g.inCooldown(time.Now()))
	require.False(
		t,
		g.inCooldown(
			time.Now().Add(leiosBackfillConnDeclineCooldown+time.Second),
		),
		"a typed decline must not install the stalled-peer cooldown",
	)

	requireLeiosFetchConversationDone(t, decliningDone)
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
			data, ok := o.lookupLeiosEndorserBlock(point.Hash)
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
}

// TestFetchEndorserBlockByPointTxsUnavailableIsNotAnAllPeerDecline covers the
// diffusion case: a peer that answers MsgNoBlockTxs may hold the manifest with
// a still-incomplete transaction cache (dingo's own leios-fetch server answers
// exactly that way), so reporting it as "no connected peer holds this endorser
// block" would make the all-declined diagnostic fire during ordinary catch-up.
func TestFetchEndorserBlockByPointTxsUnavailableIsNotAnAllPeerDecline(
	t *testing.T,
) {
	_, manifestRaw, point, _ := leiosCertifiedRecoveryFixture(t, 0x55, 376041)

	conn, connDone := newLeiosFetchConversation(
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
					leiosfetch.NewMsgNoBlockTxs(),
				},
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(conn, false, "diffusing"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})
	require.NoError(t, o.storeLeiosEndorserBlock(point, manifestRaw, nil))

	err := o.FetchEndorserBlockByPoint(
		context.Background(),
		point.Slot,
		point.Hash,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, leiosfetch.ErrBlockTxsNotFound)
	require.NotErrorIs(
		t,
		err,
		errLeiosEndorserBlockDeclinedByAllPeers,
		"MsgNoBlockTxs is indistinguishable from in-progress diffusion and "+
			"must not be reported as no peer holding the endorser block",
	)
	requireLeiosFetchConversationDone(t, connDone)
}

// TestFetchEndorserBlockByPointBusyCandidateSuppressesDeclineVerdict verifies
// the all-declined verdict is withheld when a candidate never answered the
// query. Operators act on that error as "no connected peer holds this endorser
// block"; a peer that was busy serving another fetch is no evidence of that.
func TestFetchEndorserBlockByPointBusyCandidateSuppressesDeclineVerdict(
	t *testing.T,
) {
	_, _, point, _ := leiosCertifiedRecoveryFixture(t, 0x56, 376042)

	decliningConn, decliningDone := newLeiosFetchConversation(
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
					leiosfetch.NewMsgNoBlock(),
				},
			},
		),
	)
	// Never asked anything: its fetch guard is held for the whole call.
	busyConn, busyDone := newLeiosFetchConversation(t, leiosFetchHandshake())
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(decliningConn, false, "declining"))
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
	busyGuard := o.leiosFetchGuardFor(busyConn.Id())
	busyGuard.mu.Lock()

	err := o.FetchEndorserBlockByPoint(
		context.Background(),
		point.Slot,
		point.Hash,
	)
	busyGuard.mu.Unlock()
	require.Error(t, err)
	require.NotErrorIs(
		t,
		err,
		errLeiosEndorserBlockDeclinedByAllPeers,
		"a busy candidate never answered, so not every peer declined",
	)
	require.False(
		t,
		busyGuard.inCooldown(time.Now()),
		"a busy connection is not a failed attempt",
	)
	requireLeiosFetchConversationDone(t, decliningDone)
	requireLeiosFetchConversationDone(t, busyDone)
}

// TestFetchEndorserBlockByPointDeclineDoesNotEscalateCooldown verifies repeated
// typed declines keep the short fixed decline cooldown. A peer that answers
// promptly and correctly that it does not hold an endorser block is healthy: if
// each decline escalated the cooldown like a stall does, a small peer set would
// be sidelined for leiosBackfillConnCooldownMax and every later endorser block
// would lose candidates that are working fine.
func TestFetchEndorserBlockByPointDeclineDoesNotEscalateCooldown(
	t *testing.T,
) {
	const declines = 3
	conversation := leiosFetchHandshake()
	points := make([]ocommon.Point, 0, declines)
	for i := range declines {
		//nolint:gosec // small test fixture seed
		_, _, point, _ := leiosCertifiedRecoveryFixture(
			t,
			byte(0x70+i),
			376050+uint64(i),
		)
		points = append(points, point)
		conversation = append(
			conversation,
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgNoBlock(),
				},
			},
		)
	}
	conn, connDone := newLeiosFetchConversation(t, conversation)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(conn, false, "declining"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})
	for _, point := range points {
		err := o.FetchEndorserBlockByPoint(
			context.Background(),
			point.Slot,
			point.Hash,
		)
		require.ErrorIs(t, err, leiosfetch.ErrBlockNotFound)
	}

	g := o.leiosFetchGuardFor(conn.Id())
	require.True(t, g.inCooldown(time.Now()))
	require.False(
		t,
		g.inCooldown(
			time.Now().Add(leiosBackfillConnDeclineCooldown + time.Second),
		),
		"repeated typed declines escalated a healthy peer's cooldown",
	)
	requireLeiosFetchConversationDone(t, connDone)
}
