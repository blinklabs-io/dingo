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
	"maps"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func TestEffectiveChainsyncBlockTimeoutUsesProtocolMaxAsFloor(t *testing.T) {
	require.Equal(
		t,
		ochainsync.MustReplyTimeoutMax,
		effectiveChainsyncBlockTimeout(0),
	)
	require.Equal(
		t,
		ochainsync.MustReplyTimeoutMax,
		effectiveChainsyncBlockTimeout(time.Minute),
	)
	require.Equal(
		t,
		10*time.Minute,
		effectiveChainsyncBlockTimeout(10*time.Minute),
	)
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type testBlockHeader struct {
	hash        gledger.Blake2b256
	prevHash    gledger.Blake2b256
	blockNumber uint64
	slotNumber  uint64
	bodySize    uint64
	bodyHash    gledger.Blake2b256
}

// testBlock is the smallest block implementation needed to wake a server-side
// ChainIterator and drive the async RollForward path.
type testBlock struct {
	*testBlockHeader
	blockType int
	cbor      []byte
}

func (h *testBlockHeader) Hash() gledger.Blake2b256 {
	return h.hash
}

func (h *testBlockHeader) PrevHash() gledger.Blake2b256 {
	return h.prevHash
}

func (h *testBlockHeader) BlockNumber() uint64 {
	return h.blockNumber
}

func (h *testBlockHeader) SlotNumber() uint64 {
	return h.slotNumber
}

func (h *testBlockHeader) IssuerVkey() gledger.IssuerVkey {
	return gledger.IssuerVkey{}
}

func (h *testBlockHeader) BlockBodySize() uint64 {
	return h.bodySize
}

func (h *testBlockHeader) Era() gledger.Era {
	return gledger.Era{}
}

func (h *testBlockHeader) Cbor() []byte {
	return nil
}

func (h *testBlockHeader) BlockBodyHash() gledger.Blake2b256 {
	return h.bodyHash
}

func (b *testBlock) Header() gledger.BlockHeader {
	return b.testBlockHeader
}

func (b *testBlock) Type() int {
	return b.blockType
}

func (b *testBlock) Transactions() []gledger.Transaction {
	return nil
}

func (b *testBlock) Utxorpc() (*utxorpc.Block, error) {
	return nil, nil
}

func (b *testBlock) Cbor() []byte {
	return b.cbor
}

func newTestBlockHeader(slot, block uint64, hashByte byte) gledger.BlockHeader {
	var hash gledger.Blake2b256
	hash[0] = hashByte
	return &testBlockHeader{
		hash:        hash,
		blockNumber: block,
		slotNumber:  slot,
	}
}

func newTestConnId(local, remote string) ouroboros.ConnectionId {
	localAddr, err := net.ResolveTCPAddr("tcp", local)
	if err != nil {
		panic(err)
	}
	remoteAddr, err := net.ResolveTCPAddr("tcp", remote)
	if err != nil {
		panic(err)
	}
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

type testSecurityParamLedger struct {
	securityParam int
}

func (l testSecurityParamLedger) SecurityParam() int {
	return l.securityParam
}

func newTestLedgerState(t *testing.T) *ledger.LedgerState {
	t.Helper()

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2160}))

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	return ls
}

func snapshotChainsyncNtNTimeouts() map[string]struct {
	timeout        time.Duration
	hasTimeoutFunc bool
} {
	snapshot := make(map[string]struct {
		timeout        time.Duration
		hasTimeoutFunc bool
	})
	for state, entry := range ochainsync.StateMapNtN {
		switch state.Name {
		case "CanAwait", "MustReply":
			snapshot[state.Name] = struct {
				timeout        time.Duration
				hasTimeoutFunc bool
			}{
				timeout:        entry.Timeout,
				hasTimeoutFunc: entry.TimeoutFunc != nil,
			}
		}
	}
	return snapshot
}

func TestNewOuroborosDoesNotMutateChainsyncNtNTimeouts(t *testing.T) {
	originalStateMap := ochainsync.StateMapNtN.Copy()
	t.Cleanup(func() {
		clear(ochainsync.StateMapNtN)
		maps.Copy(ochainsync.StateMapNtN, originalStateMap)
	})

	before := snapshotChainsyncNtNTimeouts()

	_ = NewOuroboros(OuroborosConfig{
		ChainsyncBlockTimeout: 10 * time.Minute,
	})
	require.Equal(t, before, snapshotChainsyncNtNTimeouts())

	_ = NewOuroboros(OuroborosConfig{
		ChainsyncBlockTimeout: 20 * time.Minute,
	})
	require.Equal(t, before, snapshotChainsyncNtNTimeouts())
}

func TestChainsyncConnOptsUseConfiguredBlockTimeout(t *testing.T) {
	const blockTimeout = 20 * time.Minute

	o := NewOuroboros(OuroborosConfig{
		ChainsyncBlockTimeout: blockTimeout,
	})

	clientCfg := ochainsync.NewConfig(o.chainsyncClientConnOpts()...)
	serverCfg := ochainsync.NewConfig(o.chainsyncServerConnOpts()...)

	require.Equal(t, blockTimeout, clientCfg.BlockTimeout)
	require.Equal(t, blockTimeout, serverCfg.BlockTimeout)
}

type chainsyncAsyncSendFailureHarness struct {
	o           *Ouroboros
	conn        *ouroboros.Connection
	server      *ochainsync.Server
	ledgerState *ledger.LedgerState
	closedCh    <-chan event.Event
}

// newChainsyncAsyncSendFailureHarness creates a real in-memory Ouroboros
// connection registered with connmanager, so async send errors must flow
// through conn.ErrorChan() to produce connmanager.conn_closed.
func newChainsyncAsyncSendFailureHarness(
	t *testing.T,
) chainsyncAsyncSendFailureHarness {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)

	_, closedCh := bus.Subscribe(connmanager.ConnectionClosedEventType)
	ledgerState := newTestLedgerState(t)
	chainsyncState := dchainsync.NewState(bus, ledgerState)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			EventBus: bus,
			Logger:   logger,
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = connManager.Stop(stopCtx)
	})
	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() {
		_ = serverPipe.Close()
		_ = clientPipe.Close()
	})

	serverConnCh := make(chan *ouroboros.Connection, 1)
	serverErrCh := make(chan error, 1)
	go func() {
		conn, err := ouroboros.New(
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithDelayProtocolStart(true),
			ouroboros.WithLogger(logger),
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		serverConnCh <- conn
	}()
	clientConn, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNetworkMagic(42),
		ouroboros.WithDelayProtocolStart(true),
		ouroboros.WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })
	var conn *ouroboros.Connection
	select {
	case err := <-serverErrCh:
		t.Fatalf("server connection setup failed: %v", err)
	case conn = <-serverConnCh:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server connection setup")
	}
	t.Cleanup(func() { _ = conn.Close() })

	require.True(
		t,
		connManager.AddConnection(conn, false, conn.Id().RemoteAddr.String()),
	)
	t.Cleanup(func() {
		sendChainsyncTestConnError(conn.ErrorChan(), context.Canceled)
	})

	server := conn.ChainSync().Server
	server.Start()
	t.Cleanup(server.Stop)

	o := NewOuroboros(OuroborosConfig{
		ConnManager: connManager,
		EventBus:    bus,
		Logger:      logger,
	})
	o.LedgerState = ledgerState
	o.ChainsyncState = chainsyncState

	return chainsyncAsyncSendFailureHarness{
		o:           o,
		conn:        conn,
		server:      server,
		ledgerState: ledgerState,
		closedCh:    closedCh,
	}
}

func sendChainsyncTestConnError(errCh chan error, err error) {
	defer func() {
		_ = recover()
	}()
	select {
	case errCh <- err:
	default:
	}
}

// requireChainsyncClosedEvent verifies that the async send failure reached
// connmanager's lifecycle path instead of being logged and dropped.
func requireChainsyncClosedEvent(
	t *testing.T,
	h chainsyncAsyncSendFailureHarness,
	msg string,
) {
	t.Helper()
	evt := testutil.RequireReceive(
		t,
		h.closedCh,
		5*time.Second,
		msg,
	)
	closed, ok := evt.Data.(connmanager.ConnectionClosedEvent)
	require.True(t, ok)
	require.Equal(t, h.conn.Id(), closed.ConnectionId)
	require.Error(t, closed.Error)
}

// requestNextIntoAsyncAwait performs the initial rollback handshake and then
// parks the server in AwaitReply, which is the async path covered by H6.
func requestNextIntoAsyncAwait(
	t *testing.T,
	h chainsyncAsyncSendFailureHarness,
) {
	t.Helper()
	ctx := ochainsync.CallbackContext{
		ConnectionId: h.conn.Id(),
		Server:       h.server,
	}
	require.NoError(t, h.o.chainsyncServerRequestNext(ctx))
	require.NoError(t, h.o.chainsyncServerRequestNext(ctx))
}

// TestChainsyncServerRequestNext_AsyncRollForwardErrorClosesConnection
// reproduces H6 for the async RollForward path: once AwaitReply has returned,
// a later send failure must still close through connmanager lifecycle handling.
func TestChainsyncServerRequestNext_AsyncRollForwardErrorClosesConnection(
	t *testing.T,
) {
	h := newChainsyncAsyncSendFailureHarness(t)
	requestNextIntoAsyncAwait(t, h)

	// Stop the protocol before waking the iterator so the goroutine's
	// RollForward send fails after chainsyncServerRequestNext has returned.
	h.server.Stop()
	block := &testBlock{
		testBlockHeader: &testBlockHeader{
			hash:        gledger.Blake2b256{0x01},
			blockNumber: 1,
			slotNumber:  1,
		},
		blockType: 1,
		cbor:      []byte{0x80},
	}
	require.NoError(t, h.ledgerState.Chain().AddBlock(block, nil))

	// The failure must be observable as normal connection lifecycle handling.
	requireChainsyncClosedEvent(
		t,
		h,
		"async RollForward send failure should close the connection",
	)
}

// TestChainsyncServerRequestNext_AsyncRollBackwardErrorClosesConnection
// reproduces H6 for the async RollBackward path: rollback send failures after
// AwaitReply must not leave the downstream peer connection silently open.
func TestChainsyncServerRequestNext_AsyncRollBackwardErrorClosesConnection(
	t *testing.T,
) {
	h := newChainsyncAsyncSendFailureHarness(t)
	block := &testBlock{
		testBlockHeader: &testBlockHeader{
			hash:        gledger.Blake2b256{0x01},
			blockNumber: 1,
			slotNumber:  1,
		},
		blockType: 1,
		cbor:      []byte{0x80},
	}
	require.NoError(t, h.ledgerState.Chain().AddBlock(block, nil))
	requestNextIntoAsyncAwait(t, h)
	ctx := ochainsync.CallbackContext{
		ConnectionId: h.conn.Id(),
		Server:       h.server,
	}
	require.NoError(t, h.o.chainsyncServerRequestNext(ctx))

	// Stop the protocol before rolling back so the goroutine's RollBackward
	// send fails after chainsyncServerRequestNext has returned.
	h.server.Stop()
	require.NoError(t, h.ledgerState.Chain().Rollback(ocommon.NewPointOrigin()))

	// The failure must be observable as normal connection lifecycle handling.
	requireChainsyncClosedEvent(
		t,
		h,
		"async RollBackward send failure should close the connection",
	)
}

func TestNormalizeIntersectPoints(t *testing.T) {
	points := []ocommon.Point{
		ocommon.NewPoint(20, []byte("b")),
		ocommon.NewPoint(30, []byte("c")),
		ocommon.NewPoint(20, []byte("b")),
		ocommon.NewPointOrigin(),
		ocommon.NewPointOrigin(),
	}

	normalized := normalizeIntersectPoints(points)

	require.Equal(
		t,
		[]ocommon.Point{
			ocommon.NewPoint(20, []byte("b")),
			ocommon.NewPoint(30, []byte("c")),
			ocommon.NewPointOrigin(),
		},
		normalized,
	)
}

func TestChainsyncClientRollForwardReplaysDuplicateFromSelectedPeerSeenElsewhere(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	state.SetClientConnId(connA)

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	evt1 := <-ch
	data1, ok := evt1.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, connB, data1.ConnectionId)

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt2 := <-ch:
		data2, ok := evt2.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connA, data2.ConnectionId)
	case <-time.After(time.Second):
		t.Fatal(
			"expected selected peer to replay duplicate header first seen elsewhere",
		)
	}
}

func TestChainsyncClientRollForwardReplaysDuplicateFromEquivalentSelectedPeerSeenElsewhere(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connADup := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connADup))
	require.True(t, state.AddClientConnId(connB))
	state.SetClientConnId(connA)

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	evt1 := <-ch
	data1, ok := evt1.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, connB, data1.ConnectionId)

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connADup},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt2 := <-ch:
		data2, ok := evt2.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connADup, data2.ConnectionId)
	case <-time.After(time.Second):
		t.Fatal(
			"expected equivalent selected peer to replay duplicate header first seen elsewhere",
		)
	}
}

func TestChainsyncClientRollForwardDropsDuplicateFromSameSelectedPeer(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	require.True(t, state.AddClientConnId(connA))
	state.SetClientConnId(connA)

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	evt1 := <-ch
	data1, ok := evt1.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, connA, data1.ConnectionId)

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt2 := <-ch:
		t.Fatalf(
			"expected same-connection duplicate to be dropped, got event: %#v",
			evt2,
		)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestChainsyncClientRollForward_IneligiblePeerDoesNotPoisonDedup(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connEligible := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connIneligible := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connEligible))
	require.True(t, state.AddClientConnId(connIneligible))

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(connId ouroboros.ConnectionId) bool {
			return connId == connEligible
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	header := newTestBlockHeader(42, 7, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(42, header.Hash().Bytes()),
		BlockNumber: 7,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connIneligible},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt := <-ledgerCh:
		t.Fatalf("unexpected ledger event from ineligible peer: %#v", evt)
	default:
	}

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connEligible},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connEligible, data.ConnectionId)
		require.Equal(t, tip.Point.Slot, data.Point.Slot)
	case <-time.After(2 * time.Second):
		t.Fatal("expected eligible peer header to feed the ledger")
	}
}

func TestRegisterTrackedChainsyncClient_ObservabilityOnlyDoesNotConsumePool(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connObserved := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	connEligible := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	state := dchainsync.NewStateWithConfig(bus, nil, dchainsync.Config{
		MaxClients:   1,
		StallTimeout: time.Minute,
	})
	o := NewOuroboros(OuroborosConfig{EventBus: bus})
	o.ChainsyncState = state

	require.True(t, o.registerTrackedChainsyncClient(connObserved, false, true))
	observabilityOnly, exists := state.ClientObservabilityOnly(connObserved)
	require.True(t, exists)
	require.True(t, observabilityOnly)
	outbound, exists := state.ClientStartedAsOutbound(connObserved)
	require.True(t, exists)
	require.True(t, outbound)
	require.False(t, o.isInboundChainsyncClient(connObserved))
	require.Equal(t, 0, state.ClientConnCount())

	require.True(t, o.registerTrackedChainsyncClient(connEligible, true, true))
	require.Equal(t, 1, state.ClientConnCount())

	active := state.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connEligible, *active)
}

func TestRegisterTrackedChainsyncClient_PromotedObservedKeepsDirection(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connId := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewStateWithConfig(bus, nil, dchainsync.Config{
		MaxClients:   1,
		StallTimeout: time.Minute,
	})
	o := NewOuroboros(OuroborosConfig{EventBus: bus})
	o.ChainsyncState = state

	require.True(t, o.registerTrackedChainsyncClient(connId, false, true))
	observabilityOnly, exists := state.ClientObservabilityOnly(connId)
	require.True(t, exists)
	require.True(t, observabilityOnly)
	require.False(t, o.isInboundChainsyncClient(connId))

	require.True(t, o.registerTrackedChainsyncClient(connId, true, true))
	observabilityOnly, exists = state.ClientObservabilityOnly(connId)
	require.True(t, exists)
	require.False(t, observabilityOnly)
	outbound, exists := state.ClientStartedAsOutbound(connId)
	require.True(t, exists)
	require.True(t, outbound)
	require.False(t, o.isInboundChainsyncClient(connId))
}

func TestHandlePeerEligibilityChangedEvent_DemotesObservedIngress(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	state.SetClientConnId(connA)
	state.UpdateClientTip(
		connA,
		ocommon.NewPoint(200, []byte("ha")),
		ochainsync.Tip{Point: ocommon.NewPoint(200, []byte("ha"))},
	)
	state.UpdateClientTip(
		connB,
		ocommon.NewPoint(100, []byte("hb")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("hb"))},
	)

	o := NewOuroboros(OuroborosConfig{EventBus: bus})
	o.ChainsyncState = state
	o.HandlePeerEligibilityChangedEvent(event.NewEvent(
		peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{
			ConnectionId: connA,
			Eligible:     false,
		},
	))

	observabilityOnly, exists := state.ClientObservabilityOnly(connA)
	require.True(t, exists)
	require.True(t, observabilityOnly)

	active := state.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connB, *active)
}

func TestChainsyncClientRollForward_UntrackedPeerDoesNotPublishToLedger(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connId := newTestConnId("127.0.0.1:6000", "3.3.3.3:3001")
	state := dchainsync.NewState(bus, nil)
	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	header := newTestBlockHeader(42, 7, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(42, header.Hash().Bytes()),
		BlockNumber: 7,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connId},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		t.Fatalf("unexpected ledger event from untracked peer: %#v", evt)
	default:
	}
}

func TestSubscribeChainsyncResyncRewindsClientsWithoutRecycle(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	rollbackPoint := ocommon.NewPoint(90, []byte("rollback"))
	point := ocommon.NewPoint(100, []byte("hdr"))
	tip := ochainsync.Tip{Point: point}

	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	state.UpdateClientTip(
		connA,
		ocommon.NewPoint(120, []byte("ahead")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(120, []byte("ahead")),
		},
	)
	state.UpdateClientTip(connB, point, tip)
	require.True(
		t,
		state.HeaderPreviouslySeenFromOtherConn(connA, point),
	)

	o := NewOuroboros(OuroborosConfig{EventBus: bus})
	o.ChainsyncState = state
	o.EventBus = bus

	_, recycleCh := bus.Subscribe(
		connmanager.ConnectionRecycleRequestedEventType,
	)
	ctx := t.Context()
	o.SubscribeChainsyncResync(ctx)

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				Reason: event.ChainsyncResyncReasonLocalLedgerRollback,
				Point:  rollbackPoint,
			},
		),
	)

	select {
	case evt := <-recycleCh:
		t.Fatalf("unexpected recycle request: %#v", evt)
	case <-time.After(100 * time.Millisecond):
	}

	require.False(
		t,
		state.HeaderPreviouslySeenFromOtherConn(connA, point),
	)
	tc := state.GetTrackedClient(connA)
	require.NotNil(t, tc)
	require.Equal(t, rollbackPoint, tc.Cursor)
}

func TestSubscribeChainsyncResyncDoesNotRecycleOnLocalRollbackWithoutPeerHistory(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	rollbackPoint := ocommon.NewPoint(90, []byte("rollback"))

	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	// Keep the tracked cursor at the rollback point so
	// RewindTrackedClientsTo returns no connections. The local rollback
	// still needs to resynchronize the live tracked session.
	state.UpdateClientTip(
		connA,
		rollbackPoint,
		ochainsync.Tip{Point: rollbackPoint},
	)
	o := NewOuroboros(OuroborosConfig{EventBus: bus})
	o.ChainsyncState = state
	o.EventBus = bus
	o.LedgerState = newTestLedgerState(t)

	_, recycleCh := bus.Subscribe(
		connmanager.ConnectionRecycleRequestedEventType,
	)
	ctx := t.Context()
	o.SubscribeChainsyncResync(ctx)

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				Reason: event.ChainsyncResyncReasonLocalLedgerRollback,
				Point:  rollbackPoint,
			},
		),
	)

	// The fallback path should not request peer-governance recycling here.
	// Recovery may close the connection for a fresh reconnect instead.
	select {
	case evt := <-recycleCh:
		t.Fatalf("unexpected recycle request: %#v", evt)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSubscribeChainsyncResyncClosesConnectionForFreshSyncReasons(
	t *testing.T,
) {
	reasons := []string{
		event.ChainsyncResyncReasonLocalTipPlateau,
		event.ChainsyncResyncReasonPostPlateauRealign,
		event.ChainsyncResyncReasonRollbackNotFound,
		event.ChainsyncResyncReasonPersistentFork,
		event.ChainsyncResyncReasonRollbackExceedsK,
		event.ChainsyncResyncReasonForkResolutionExceedsK,
		event.ChainsyncResyncReasonRollbackLoop,
	}
	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			logBuf := &lockedBuffer{}
			logger := slog.New(
				slog.NewJSONHandler(
					logBuf,
					&slog.HandlerOptions{Level: slog.LevelDebug},
				),
			)
			bus := event.NewEventBus(nil, logger)
			defer bus.Close()

			connManager := connmanager.NewConnectionManager(
				connmanager.ConnectionManagerConfig{
					EventBus: bus,
					Logger:   logger,
				},
			)
			t.Cleanup(func() {
				stopCtx, stopCancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer stopCancel()
				_ = connManager.Stop(stopCtx)
			})

			mockConn := ouroboros_mock.NewConnection(
				ouroboros_mock.ProtocolRoleClient,
				ouroboros_mock.ConversationKeepAlive,
			)
			oConn, err := ouroboros.New(
				ouroboros.WithConnection(mockConn),
				ouroboros.WithNetworkMagic(
					ouroboros_mock.MockNetworkMagic,
				),
				ouroboros.WithNodeToNode(true),
				ouroboros.WithKeepAlive(true),
				ouroboros.WithKeepAliveConfig(
					keepalive.NewConfig(
						keepalive.WithCookie(
							ouroboros_mock.MockKeepAliveCookie,
						),
						keepalive.WithPeriod(30*time.Second),
						keepalive.WithTimeout(15*time.Second),
					),
				),
			)
			require.NoError(t, err)
			connManager.AddConnection(oConn, false, "127.0.0.1:1234")

			o := NewOuroboros(OuroborosConfig{
				EventBus: bus,
				Logger:   logger,
			})
			o.EventBus = bus
			o.ConnManager = connManager

			ctx := t.Context()
			o.SubscribeChainsyncResync(ctx)

			connId := oConn.Id()
			bus.Publish(
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: connId,
						Reason:       reason,
					},
				),
			)

			require.Eventually(
				t,
				func() bool {
					return connManager.GetConnectionById(connId) == nil
				},
				2*time.Second,
				20*time.Millisecond,
			)
			require.Eventually(
				t,
				func() bool {
					logs := logBuf.String()
					return strings.Contains(
						logs,
						`"msg":"closing connection for fresh chainsync"`,
					) && strings.Contains(
						logs,
						`"reason":"`+reason+`"`,
					)
				},
				2*time.Second,
				20*time.Millisecond,
			)
			require.NotContains(
				t,
				logBuf.String(),
				`"msg":"restarting chainsync client"`,
			)
		})
	}
}

func TestSubscribeChainsyncResyncDeniesDivergentPeer(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.EventBus = bus
	o.PeerGov = peerGov
	o.SubscribeChainsyncResync(t.Context())

	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", "10.0.0.1:3001")
	require.NoError(t, err)
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: connId,
				Reason:       event.ChainsyncResyncReasonRollbackExceedsK,
			},
		),
	)

	require.Eventually(
		t,
		func() bool {
			return peerGov.IsDenied(remoteAddr.String())
		},
		2*time.Second,
		20*time.Millisecond,
	)
}

func TestSubscribeChainsyncResyncDoesNotDenyRollbackLoop(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.EventBus = bus
	o.PeerGov = peerGov
	o.SubscribeChainsyncResync(t.Context())

	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", "10.0.0.1:3001")
	require.NoError(t, err)
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: connId,
				Reason:       event.ChainsyncResyncReasonRollbackLoop,
			},
		),
	)

	require.Never(
		t,
		func() bool {
			return peerGov.IsDenied(remoteAddr.String())
		},
		200*time.Millisecond,
		20*time.Millisecond,
	)
}

func TestHeaderPreviouslySeenFromOtherConnTreatsEquivalentConnIdsAsSame(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connADup := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	point := ocommon.NewPoint(100, []byte("hdr"))
	tip := ochainsync.Tip{Point: point}

	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	state.UpdateClientTip(connA, point, tip)

	require.False(
		t,
		state.HeaderPreviouslySeenFromOtherConn(connADup, point),
	)
}

// TestChainsyncClientRollForward_InboundUpstreamPublishesWhenEligible
// exercises a full-duplex inbound connection from a configured upstream peer
// (one that ChainsyncIngressEligible recognises as eligible). Even though the
// chainsync client is registered inbound (startedAsOutbound=false), headers
// should flow into the ledger and a PeerTipUpdateEvent should be emitted.
// This covers the single-relay block producer scenario where the relay wins
// the dial race after a crash.
func TestChainsyncClientRollForward_InboundUpstreamPublishesWhenEligible(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	state := dchainsync.NewState(bus, nil)

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(connId ouroboros.ConnectionId) bool {
			return connId == connInbound
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	// Register as inbound + ingress-eligible to model a full-duplex inbound
	// from a trusted upstream peer.
	require.True(t, o.registerTrackedChainsyncClient(connInbound, true, false))
	observabilityOnly, exists := state.ClientObservabilityOnly(connInbound)
	require.True(t, exists)
	require.False(
		t,
		observabilityOnly,
		"eligible inbound should not be observability-only",
	)
	require.True(t, o.isInboundChainsyncClient(connInbound))

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connInbound},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, tip.Point.Slot, data.Point.Slot)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"expected eligible inbound header to feed the ledger; " +
				"single-relay producer would stay stuck at tip otherwise",
		)
	}

	select {
	case evt := <-tipCh:
		data, ok := evt.Data.(chainselection.PeerTipUpdateEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, tip.Point.Slot, data.Tip.Point.Slot)
	case <-time.After(2 * time.Second):
		t.Fatal("expected PeerTipUpdateEvent for eligible inbound peer")
	}
}

// TestChainsyncClientRollForward_InboundIneligiblePeerStaysObservabilityOnly
// verifies the fix preserves the protection added in #1699: when peergov
// reports the peer as ineligible (e.g. a random downstream client pulling
// data from us), its headers must not feed the ledger even though chainsync
// is running against it.
func TestChainsyncClientRollForward_InboundIneligiblePeerStaysObservabilityOnly(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return false
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	require.True(t, o.registerTrackedChainsyncClient(connInbound, false, false))
	observabilityOnly, exists := state.ClientObservabilityOnly(connInbound)
	require.True(t, exists)
	require.True(t, observabilityOnly)

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connInbound},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		t.Fatalf(
			"unexpected ledger event from ineligible inbound peer: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case evt := <-tipCh:
		t.Fatalf(
			"unexpected PeerTipUpdateEvent from ineligible inbound peer: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestShouldPublishChainsyncToLedger_InboundFailsClosedWithNilCallback
// verifies that when no ChainsyncIngressEligible policy is wired, an inbound
// full-duplex chainsync client is not treated as ingress-eligible. Outbound
// chainsync retains its legacy default of eligible so the fix does not
// regress existing callers that don't pass a policy. Regression guard for
// the review feedback on issue #1982.
func TestShouldPublishChainsyncToLedger_InboundFailsClosedWithNilCallback(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connOutbound := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)

	o := NewOuroboros(OuroborosConfig{EventBus: bus})
	o.ChainsyncState = state
	o.EventBus = bus
	require.Nil(t, o.config.ChainsyncIngressEligible)

	require.True(t, o.registerTrackedChainsyncClient(connOutbound, true, true))
	require.True(t, o.registerTrackedChainsyncClient(connInbound, false, false))

	require.True(
		t,
		o.shouldPublishChainsyncToLedger(connOutbound),
		"outbound default must remain eligible when no policy is wired",
	)
	require.False(
		t,
		o.shouldPublishChainsyncToLedger(connInbound),
		"inbound default must be observability-only when no policy is wired",
	)

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)

	require.NoError(
		t,
		o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: connInbound},
			0,
			header,
			tip,
		),
	)

	select {
	case evt := <-ledgerCh:
		t.Fatalf(
			"inbound peer with nil policy must not feed ledger: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case evt := <-tipCh:
		t.Fatalf(
			"inbound peer with nil policy must not emit PeerTipUpdateEvent: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}

	observabilityOnly, exists := state.ClientObservabilityOnly(connInbound)
	require.True(t, exists)
	require.True(
		t,
		observabilityOnly,
		"reconcile must not upgrade inbound under nil policy",
	)
}

// TestChainsyncClientRollBackward_InboundUpstreamProcessesRollback verifies
// that rollbacks received on an eligible inbound chainsync client are
// forwarded to the ledger. Without the fix, isInboundChainsyncClient
// short-circuits before reconcileChainsyncIngressAdmission and rollbacks are
// silently dropped, so the node can't react to chain reorganisations reported
// by a configured upstream when the relay dialed first.
func TestChainsyncClientRollBackward_InboundUpstreamProcessesRollback(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	state := dchainsync.NewState(bus, nil)

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

	require.True(t, o.registerTrackedChainsyncClient(connInbound, true, false))

	_, rollbackCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, chainSelectionRollbackCh := bus.Subscribe(
		chainselection.PeerRollbackEventType,
	)
	rollbackPoint := ocommon.NewPoint(90, []byte("rollback"))
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(95, []byte("tip")),
		BlockNumber: 5,
	}

	err := o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: connInbound},
		rollbackPoint,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-rollbackCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, rollbackPoint.Slot, data.Point.Slot)
		require.True(t, data.Rollback)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"expected rollback event from eligible inbound peer",
		)
	}

	select {
	case evt := <-chainSelectionRollbackCh:
		data, ok := evt.Data.(chainselection.PeerRollbackEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, rollbackPoint.Slot, data.Point.Slot)
		require.Equal(t, tip.BlockNumber, data.Tip.BlockNumber)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"expected chainselection rollback event from eligible inbound peer",
		)
	}
}

// newFindIntersectTestOuroboros builds an Ouroboros wired with a fresh,
// empty LedgerState (tip at origin) and ChainsyncState. With the chain at
// origin, GetIntersectPoint returns the origin point for any in-bounds point
// list, so a successful FindIntersect proves the cap did not reject the
// request.
func newFindIntersectTestOuroboros(t *testing.T) *Ouroboros {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	ledgerState := newTestLedgerState(t)
	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.LedgerState = ledgerState
	o.ChainsyncState = dchainsync.NewState(bus, ledgerState)
	return o
}

func makeFindIntersectPoints(n int) []ocommon.Point {
	points := make([]ocommon.Point, n)
	for i := range points {
		points[i] = ocommon.NewPoint(
			uint64(i+1),
			[]byte{byte(i), byte(i >> 8)},
		)
	}
	return points
}

func TestChainsyncServerFindIntersect_AtLimitAccepted(t *testing.T) {
	o := newFindIntersectTestOuroboros(t)
	connId := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints)

	_, _, err := o.chainsyncServerFindIntersect(
		ochainsync.CallbackContext{ConnectionId: connId},
		points,
	)
	// An empty chain intersects every in-bounds request at origin, so a
	// point list at the limit must be accepted (no error).
	require.NoError(t, err)
}

func TestChainsyncServerFindIntersect_OverLimitRejected(t *testing.T) {
	o := newFindIntersectTestOuroboros(t)
	connId := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints + 1)

	_, _, err := o.chainsyncServerFindIntersect(
		ochainsync.CallbackContext{ConnectionId: connId},
		points,
	)
	// Over-limit lists are rejected before any intersection lookup. On an
	// empty chain the lookup would otherwise return origin, so receiving
	// ErrIntersectNotFound here proves the cap short-circuited the request.
	require.ErrorIs(t, err, ochainsync.ErrIntersectNotFound)
}

func TestChainsyncServerFindIntersect_NormalPointListAccepted(t *testing.T) {
	o := newFindIntersectTestOuroboros(t)
	connId := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	// A typical client sends at most chainsyncIntersectPointCount points.
	points := makeFindIntersectPoints(chainsyncIntersectPointCount)

	_, _, err := o.chainsyncServerFindIntersect(
		ochainsync.CallbackContext{ConnectionId: connId},
		points,
	)
	require.NoError(t, err)
}

// Both Mithril-boundary rejection reasons must close the connection for a
// fresh intersect AND deny the peer for a cooldown. Without the deny, a
// peer whose chain is refused at the trust boundary is redialed roughly
// every backoff interval and rejected ~600ms later, forever.
func TestChainsyncResyncMithrilReasonsDenyPeerAndRequireFreshConnection(
	t *testing.T,
) {
	tests := []struct {
		reason         string
		wantFresh      bool
		wantDeniesPeer bool
	}{
		{
			reason:         event.ChainsyncResyncReasonRollbackExceedsMithril,
			wantFresh:      true,
			wantDeniesPeer: true,
		},
		{
			reason:         event.ChainsyncResyncReasonPeerTipBehindMithril,
			wantFresh:      true,
			wantDeniesPeer: true,
		},
		// Existing behavior pins
		{
			reason:         event.ChainsyncResyncReasonRollbackExceedsK,
			wantFresh:      true,
			wantDeniesPeer: true,
		},
		{
			reason:         event.ChainsyncResyncReasonLocalTipPlateau,
			wantFresh:      true,
			wantDeniesPeer: false,
		},
	}
	for _, tt := range tests {
		if got := chainsyncResyncRequiresFreshConnection(tt.reason); got != tt.wantFresh {
			t.Errorf(
				"chainsyncResyncRequiresFreshConnection(%q) = %v, want %v",
				tt.reason, got, tt.wantFresh,
			)
		}
		if got := chainsyncResyncDeniesPeer(tt.reason); got != tt.wantDeniesPeer {
			t.Errorf(
				"chainsyncResyncDeniesPeer(%q) = %v, want %v",
				tt.reason, got, tt.wantDeniesPeer,
			)
		}
	}
}
