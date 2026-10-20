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
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// holdFixture is an Ouroboros with a real ledger and chain, a tracked
// ingress-eligible chainsync client, and subscribers for the events a held
// node must not emit.
type holdFixture struct {
	*blockfetchRangeFixture
	conn     ouroboros.ConnectionId
	ledgerCh <-chan event.Event
	awaitCh  <-chan event.Event
	tipCh    <-chan event.Event
}

func newHoldFixture(t *testing.T) *holdFixture {
	t.Helper()
	f := newBlockfetchRangeFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	f.o.eventBus = bus
	f.o.config.ChainsyncIngressEligible = func(ouroboros.ConnectionId) bool {
		return true
	}
	f.o.chainsyncState = dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6020", "10.0.0.50:3001")
	require.True(t, f.o.chainsyncState.AddClientConnId(conn))
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, awaitCh := bus.Subscribe(ledger.ChainsyncAwaitReplyEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	return &holdFixture{
		blockfetchRangeFixture: f,
		conn:                   conn,
		ledgerCh:               ledgerCh,
		awaitCh:                awaitCh,
		tipCh:                  tipCh,
	}
}

func (f *holdFixture) rollForwardOn(
	conn ouroboros.ConnectionId,
	tip ochainsync.Tip,
) error {
	tipBlock := f.blocks[len(f.blocks)-1]
	return f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		newTestBlockHeader(tipBlock.Slot+10, tipBlock.BlockNumber+1, 0xb1),
		tip,
	)
}

// rollForward delivers a header extending the chain and requires that it
// reached the ledger path (no stop).
func (f *holdFixture) rollForward(t *testing.T, tip ochainsync.Tip) {
	t.Helper()
	require.NoError(t, f.rollForwardOn(f.conn, tip))
}

// rollForwardHeld delivers the same header and requires that it was dropped
// and the client told to stop requesting headers.
func (f *holdFixture) rollForwardHeld(t *testing.T, tip ochainsync.Tip) {
	t.Helper()
	require.ErrorIs(
		t,
		f.rollForwardOn(f.conn, tip),
		ochainsync.ErrStopSyncProcess,
	)
}

// While network ingest is held a peer's headers reach neither the ledger nor
// chain selection, so the chain does not move, yet the tip the peer reports is
// still tracked for the catch-up decision. The control after release shows
// the same delivery does reach the ledger.
func TestHoldNetworkIngestDropsHeaders(t *testing.T) {
	t.Parallel()
	f := newHoldFixture(t)
	ls := f.o.ledgerState
	chainTip := ls.PrimaryChainTip()
	peerTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(9000, []byte{0xee}),
		BlockNumber: 900,
	}

	_, ok := f.o.chainsyncState.PeerTipSlot()
	require.False(t, ok, "no peer has reported a tip yet")

	release := ls.HoldNetworkIngest()
	f.rollForwardHeld(t, peerTip)
	// A raw header is not even decoded while held: undecodable bytes are
	// accepted rather than failing the connection.
	laterTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(9100, []byte{0xef}),
		BlockNumber: 910,
	}
	require.ErrorIs(t, f.o.chainsyncClientRollForwardRaw(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		6,
		[]byte{0xff},
		laterTip,
	), ochainsync.ErrStopSyncProcess)
	require.ErrorIs(t, f.o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		chainTip.Point,
		laterTip,
	), ochainsync.ErrStopSyncProcess)

	require.NoError(t, f.o.chainsyncClientAwaitReply(
		ochainsync.CallbackContext{ConnectionId: f.conn},
	))

	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a held header or rollback must not reach the ledger",
	)
	testutil.RequireNoReceive(
		t,
		f.awaitCh,
		10*time.Millisecond,
		"a held await-reply must not reach the ledger",
	)
	testutil.RequireNoReceive(
		t,
		f.tipCh,
		10*time.Millisecond,
		"a held header must not reach chain selection",
	)
	assert.Equal(t, chainTip, ls.PrimaryChainTip(), "the chain must not move")
	slot, ok := f.o.chainsyncState.PeerTipSlot()
	require.True(t, ok)
	assert.Equal(
		t,
		uint64(9100),
		slot,
		"the tip each held message reports is tracked",
	)

	release()
	f.rollForward(t, peerTip)
	testutil.RequireReceive(
		t,
		f.ledgerCh,
		testutil.AsyncWait,
		"after release the same header reaches the ledger",
	)
	require.NoError(t, f.o.chainsyncClientAwaitReply(
		ochainsync.CallbackContext{ConnectionId: f.conn},
	))
	testutil.RequireReceive(
		t,
		f.awaitCh,
		testutil.AsyncWait,
		"after release an await-reply reaches the ledger",
	)
}

// Inbound blockfetch keeps serving committed history while ingest is held.
func TestHoldServesPeers(t *testing.T) {
	t.Parallel()
	f := newBlockfetchRangeFixture(t)
	release := f.o.ledgerState.HoldNetworkIngest()
	defer release()

	f.requestRange(t, f.point(0), f.point(2))

	assert.Equal(
		t,
		[]byte{
			blockfetch.MessageTypeStartBatch,
			blockfetch.MessageTypeBlock,
			blockfetch.MessageTypeBlock,
			blockfetch.MessageTypeBlock,
			blockfetch.MessageTypeBatchDone,
		},
		f.readMessageTypes(t, 5),
		"a held node must still serve every committed block in the range",
	)
}

// holdResyncFixture is a tracked chainsync client on a connection registered
// with a real connection manager, so a resync's close is observable.
type holdResyncFixture struct {
	o        *Ouroboros
	cm       *connmanager.ConnectionManager
	connId   ouroboros.ConnectionId
	observer ouroboros.ConnectionId
}

func newHoldResyncFixture(t *testing.T) *holdResyncFixture {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	chainManager, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, chainManager.SetLedger(
		testSecurityParamLedger{securityParam: 2160},
	))
	first := chain.RawBlock{
		Slot: 10, Hash: bytes.Repeat([]byte{0x01}, 32), Type: 1,
		Cbor: []byte{0x80},
	}
	second := chain.RawBlock{
		Slot: 20, Hash: bytes.Repeat([]byte{0x02}, 32), BlockNumber: 1,
		Type: 1, PrevHash: first.Hash, Cbor: []byte{0x80},
	}
	require.NoError(t, chainManager.PrimaryChain().AddRawBlocks(
		context.Background(),
		[]chain.RawBlock{first, second},
	))
	// The ledger publishes the release on the bus the resync subscriber
	// listens to, as the node wires them.
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		EventBus:     bus,
		Logger:       logger,
	})
	require.NoError(t, err)

	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{EventBus: bus, Logger: logger},
	)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = cm.Stop(stopCtx)
	})
	// Created after the slow database setup: the mock connection tears
	// itself down once its keepalive conversation runs out, so the test's
	// window must start here.
	conn := newChainsyncResyncTestConnection(t, "10.0.0.51:3001")
	t.Cleanup(func() { _ = conn.Close() })
	require.True(t, cm.AddConnection(conn, false, "10.0.0.51:3001"))
	observer := newChainsyncResyncTestConnection(t, "10.0.0.53:3001")
	t.Cleanup(func() { _ = observer.Close() })
	require.True(t, cm.AddConnection(observer, true, "10.0.0.53:3001"))
	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(conn.Id()))
	require.True(t, state.TryAddObservedClientConnId(observer.Id()))
	// The admitted cursor stays where it was when the hold was taken, behind
	// the tip external batches moved the chain to, so rewinding cursors
	// ahead of the tip selects nothing.
	firstPoint := ocommon.NewPoint(first.Slot, first.Hash)
	state.UpdateClientTip(
		conn.Id(),
		firstPoint,
		ochainsync.Tip{Point: firstPoint},
	)
	o := newOuroboros(OuroborosConfig{
		EventBus:    bus,
		Logger:      logger,
		ConnManager: cm,
	})
	o.eventBus = bus
	o.chainsyncState = state
	o.ledgerState = ls
	o.SubscribeChainsyncResync(t.Context())
	return &holdResyncFixture{
		o:        o,
		cm:       cm,
		connId:   conn.Id(),
		observer: observer.Id(),
	}
}

func (f *holdResyncFixture) publishResync(reason string) {
	f.o.eventBus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: f.connId,
				Reason:       reason,
			},
		),
	)
}

// While held, a resync from a timer armed before the hold or from any other
// source must not close a peer: the release re-intersects every client
// anyway. The release itself closes every tracked client so each reconnects
// from the current chain tip.
func TestHoldResyncReleaseClosesTrackedClients(t *testing.T) {
	t.Parallel()
	f := newHoldResyncFixture(t)

	release := f.o.ledgerState.HoldNetworkIngest()
	f.publishResync(event.ChainsyncResyncReasonBehindLocalTip)
	require.Never(
		t,
		func() bool { return f.cm.GetConnectionById(f.connId) == nil },
		200*time.Millisecond,
		10*time.Millisecond,
		"a held node must keep its peers connected",
	)

	release()
	// Bounded below the mock connection's own keepalive teardown.
	testutil.WaitForCondition(
		t,
		func() bool { return f.cm.GetConnectionById(f.connId) == nil },
		10*time.Second,
		"the release must re-intersect every tracked client",
	)
	// An observer's cursor and chain-selection view froze with the hold
	// too, and it may be promoted later, so it is closed as well.
	testutil.WaitForCondition(
		t,
		func() bool { return f.cm.GetConnectionById(f.observer) == nil },
		10*time.Second,
		"the release must close observability-only clients too",
	)
}

// A connection that existed during the hold keeps being dropped after the
// release until it closes: its cursor ran ahead with its headers discarded,
// and the closing resync is asynchronous. A connection made after the
// release is followed normally.
func TestHoldReleaseKeepsDroppingHeldConnections(t *testing.T) {
	t.Parallel()
	f := newHoldFixture(t)
	peerTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(9000, []byte{0xee}),
		BlockNumber: 900,
	}
	f.o.chainsyncState.SetNetworkIngestHeld(true)
	release := f.o.ledgerState.HoldNetworkIngest()
	release()
	f.o.chainsyncState.SetNetworkIngestHeld(false)

	f.rollForwardHeld(t, peerTip)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a connection from the hold must stay dropped until it closes",
	)

	fresh := newTestConnId("127.0.0.1:6020", "10.0.0.52:3001")
	require.True(t, f.o.chainsyncState.AddClientConnId(fresh))
	require.NoError(t, f.rollForwardOn(fresh, peerTip))
	testutil.RequireReceive(
		t,
		f.ledgerCh,
		testutil.AsyncWait,
		"a connection made after the release is followed",
	)
}

// A hold taken while a header waits at the Limit on Eagerness or in
// future-header admission drops that header when the wait returns.
func TestHoldTakenDuringWaitDropsHeader(t *testing.T) {
	t.Parallel()
	peerTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(9000, []byte{0xee}),
		BlockNumber: 900,
	}
	for _, test := range []struct {
		name string
		wire func(*holdFixture, func())
	}{
		{"eagerness", func(f *holdFixture, hold func()) {
			f.o.config.ChainsyncAwaitEagerness = func(
				context.Context,
				ouroboros.ConnectionId,
				uint64,
				func() ochainsync.Tip,
			) error {
				hold()
				return nil
			}
		}},
		{"admission", func(f *holdFixture, hold func()) {
			f.o.chainsyncHeaderAdmission = func(
				context.Context,
				ledger.ChainsyncEvent,
			) (bool, error) {
				hold()
				return true, nil
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHoldFixture(t)
			var release func()
			test.wire(f, func() {
				release = f.o.ledgerState.HoldNetworkIngest()
			})
			f.rollForwardHeld(t, peerTip)
			require.NotNil(t, release)
			release()
			testutil.RequireNoReceive(
				t,
				f.ledgerCh,
				100*time.Millisecond,
				"a header whose wait spanned the hold must be dropped",
			)
		})
	}
}

// heldChainsyncPeer is the peer side of a real chainsync client over a muxer,
// serving an endless chain whose tip the test moves.
type heldChainsyncPeer struct {
	peer   *muxerServerPeer
	client *ochainsync.Client
}

func newHeldChainsyncPeer(
	t *testing.T,
	f *holdFixture,
	pipelineLimit int,
) *heldChainsyncPeer {
	t.Helper()
	opts, peer := newMuxerServerPeer(t)
	opts.Mode = protocol.ProtocolModeNodeToNode
	opts.Role = protocol.ProtocolRoleClient
	opts.ConnectionId = f.conn
	cfg := ochainsync.NewConfig(
		ochainsync.WithPipelineLimit(pipelineLimit),
		ochainsync.WithRollForwardRawFunc(f.o.chainsyncClientRollForwardRaw),
		ochainsync.WithRollBackwardFunc(f.o.chainsyncClientRollBackward),
		ochainsync.WithAwaitReplyFunc(f.o.chainsyncClientAwaitReply),
	)
	client := ochainsync.NewClient(opts, &cfg)
	client.Start()
	peer.muxer.Start()
	t.Cleanup(func() { _ = client.Stop() })
	return &heldChainsyncPeer{peer: peer, client: client}
}

// next reads the client's next message and returns its type and, for a
// FindIntersect, its points.
func (p *heldChainsyncPeer) next(t *testing.T) (uint, []ocommon.Point) {
	t.Helper()
	_, raw := p.peer.readMessage(t, 10*time.Second)
	var fields []cbor.RawMessage
	_, err := cbor.Decode(raw, &fields)
	require.NoError(t, err)
	require.NotEmpty(t, fields)
	var msgType uint
	_, err = cbor.Decode(fields[0], &msgType)
	require.NoError(t, err)
	if msgType != ochainsync.MessageTypeFindIntersect {
		return msgType, nil
	}
	msg, err := ochainsync.NewMsgFromCborNtN(msgType, raw)
	require.NoError(t, err)
	find, ok := msg.(*ochainsync.MsgFindIntersect)
	require.True(t, ok)
	return msgType, find.Points
}

func heldTestTip(slot uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32)),
		BlockNumber: slot / 10,
	}
}

// A held client stops requesting headers once it has reported a tip, so a
// peer with a long chain sees a bounded number of RequestNext rather than the
// whole gap, and the peer's moving tip still reaches PeerTipSlot through the
// refresher's tip-only FindIntersect within one refresh interval.
func TestHoldStopsRequestingHeaders(t *testing.T) {
	t.Parallel()
	f := newHoldFixture(t)
	const pipelineLimit = 10
	p := newHeldChainsyncPeer(t, f, pipelineLimit)
	f.o.heldTips.interval = 20 * time.Millisecond
	f.o.heldTips.clientFor = func(ouroboros.ConnectionId) heldTipClient {
		return p.client
	}
	release := f.o.ledgerState.HoldNetworkIngest()
	defer release()

	go func() { _ = p.client.Sync([]ocommon.Point{ocommon.NewPointOrigin()}) }()
	msgType, _ := p.next(t)
	require.Equal(t, uint(ochainsync.MessageTypeFindIntersect), msgType)
	tip := heldTestTip(5000)
	sendChainsyncResponse(t, p.peer,
		ochainsync.NewMsgIntersectFound(ocommon.NewPointOrigin(), tip))

	requestNext := 0
	refreshes := 0
	for refreshes < 3 {
		msgType, points := p.next(t)
		switch msgType {
		case ochainsync.MessageTypeRequestNext:
			requestNext++
			require.LessOrEqual(
				t,
				requestNext,
				1+pipelineLimit,
				"a held client must not keep requesting headers",
			)
			if requestNext == 1 {
				sendChainsyncResponse(t, p.peer, ochainsync.NewMsgRollBackward(
					ocommon.NewPointOrigin(),
					tip,
				))
				continue
			}
			rollForward, err := ochainsync.NewMsgRollForwardNtN(
				gledger.BlockHeaderTypeBabbage,
				0,
				[]byte{0x82, 0x01, 0x02},
				tip,
			)
			require.NoError(t, err)
			sendChainsyncResponse(t, p.peer, rollForward)
		case ochainsync.MessageTypeFindIntersect:
			require.Empty(t, points, "a refresh asks only for the tip")
			refreshes++
			tip = heldTestTip(tip.Point.Slot + 100)
			sendChainsyncResponse(t, p.peer,
				ochainsync.NewMsgIntersectNotFound(tip))
			want := tip.Point.Slot
			testutil.WaitForCondition(t, func() bool {
				slot, ok := f.o.chainsyncState.PeerTipSlot()
				return ok && slot == want
			}, 2*time.Second, "PeerTipSlot must follow the refreshed tip")
		default:
			t.Fatalf("unexpected chainsync message type %d", msgType)
		}
	}
	assert.Equal(t, 1, requestNext,
		"a client held from the start sends only its first RequestNext")
}
