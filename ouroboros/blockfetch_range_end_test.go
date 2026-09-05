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
	"crypto/sha256"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockfetchRangeFixture wires Dingo's real blockfetch range server to a real
// chain, over a real muxer and protocol server, so MsgRequestRange traffic
// reaches blockfetchServerRequestRange exactly as a peer's would and every
// message the server emits is read back off the wire.
//
// A connection is registered with a real ConnManager, and the protocol options
// are given that connection's ID, so the callback's own GetConnectionById
// lookup resolves and the async streaming half of the callback actually runs.
type blockfetchRangeFixture struct {
	o      *Ouroboros
	peer   *muxerServerPeer
	blocks []chain.RawBlock
	logBuf *syncLogBuffer
}

// requestRange sends a MsgRequestRange as a peer would.
func (f *blockfetchRangeFixture) requestRange(
	t *testing.T,
	start ocommon.Point,
	end ocommon.Point,
) {
	t.Helper()
	f.peer.send(
		t,
		blockfetch.ProtocolId,
		blockfetch.NewMsgRequestRange(start, end),
	)
}

// point returns the chain point of the fixture block at idx.
func (f *blockfetchRangeFixture) point(idx int) ocommon.Point {
	return ocommon.NewPoint(f.blocks[idx].Slot, f.blocks[idx].Hash)
}

// readMessageTypes reads count response segments and returns the message type
// byte of each. Every blockfetch server message is a CBOR array whose first
// element is the message type, so the second payload byte identifies it.
func (f *blockfetchRangeFixture) readMessageTypes(
	t *testing.T,
	count int,
) []byte {
	t.Helper()
	types := make([]byte, 0, count)
	for range count {
		segment := f.peer.readResponse(t, 5*time.Second)
		require.Equal(t, blockfetch.ProtocolId, segment.GetProtocolId())
		require.GreaterOrEqual(t, len(segment.Payload), 2)
		types = append(types, segment.Payload[1])
	}
	return types
}

func newBlockfetchRangeFixture(t *testing.T) *blockfetchRangeFixture {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2160}),
	)

	blocks := make([]chain.RawBlock, 0, 3)
	var prevHash []byte
	for i := range 3 {
		sum := sha256.Sum256([]byte{byte(i)})
		hash := append([]byte(nil), sum[:]...)
		blocks = append(blocks, chain.RawBlock{
			Slot:        uint64(i+1) * 10,
			Hash:        hash,
			BlockNumber: uint64(i),
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(blocks))

	logBuf := &syncLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{EventBus: bus, Logger: logger},
	)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		require.NoError(t, connManager.Stop(stopCtx))
	})
	conn, err := ouroboros.New(
		ouroboros.WithConnection(
			ouroboros_mock.NewConnection(
				ouroboros_mock.ProtocolRoleClient,
				ouroboros_mock.ConversationKeepAlive,
			),
		),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithKeepAliveConfig(keepalive.NewConfig(
			keepalive.WithCookie(ouroboros_mock.MockKeepAliveCookie),
			keepalive.WithPeriod(30*time.Second),
			keepalive.WithTimeout(15*time.Second),
		)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.True(t, connManager.AddConnection(conn, false, "127.0.0.1:3001"))

	o := newOuroboros(OuroborosConfig{
		Logger:      logger,
		EventBus:    bus,
		ConnManager: connManager,
	})
	o.ledgerState = ls

	opts, peer := newMuxerServerPeer(t)
	// Report the registered connection's ID, the way a real muxer on that
	// connection would, so the callback resolves its peer through ConnManager.
	opts.ConnectionId = conn.Id()
	cfg, err := blockfetch.NewConfig(o.blockfetchServerConnOpts()...)
	require.NoError(t, err)
	server := blockfetch.NewServer(opts, &cfg)
	peer.start(t, server)

	return &blockfetchRangeFixture{
		o:      o,
		peer:   peer,
		blocks: blocks,
		logBuf: logBuf,
	}
}

// TestBlockfetchServerRequestRange_EndPointNotInChain is issue #397: only the
// end *slot* was validated, so a peer could name an end point the server does
// not hold -- here the right slot under a hash from another chain -- and the
// range server would happily stream its own blocks up to that slot number as
// though it had served the requested range. The end point must be resolved the
// way the start point is, and answered with NoBlocks when it does not resolve.
func TestBlockfetchServerRequestRange_EndPointNotInChain(t *testing.T) {
	f := newBlockfetchRangeFixture(t)

	start := f.point(0)
	wrongHash := sha256.Sum256([]byte("not-our-block"))
	end := ocommon.NewPoint(
		f.blocks[1].Slot,
		append([]byte(nil), wrongHash[:]...),
	)

	f.requestRange(t, start, end)

	assert.Equal(
		t,
		[]byte{blockfetch.MessageTypeNoBlocks},
		f.readMessageTypes(t, 1),
		"an end point we do not hold must be answered with NoBlocks, "+
			"not served as a slot-bounded prefix of our own chain",
	)
}

// A hash we do hold is not enough either: the end point must be at the slot it
// claims. A point pairing block 2's hash with block 1's slot is not a point on
// our chain.
func TestBlockfetchServerRequestRange_EndPointSlotHashMismatch(t *testing.T) {
	f := newBlockfetchRangeFixture(t)

	start := f.point(0)
	end := ocommon.NewPoint(f.blocks[1].Slot, f.blocks[2].Hash)

	f.requestRange(t, start, end)

	assert.Equal(
		t,
		[]byte{blockfetch.MessageTypeNoBlocks},
		f.readMessageTypes(t, 1),
		"a slot/hash pair that is not a point on our chain must be rejected",
	)
}

// The end-point rejection must feed the same stuck-peer valve every other
// invalid-range rejection in blockfetchServerRequestRange feeds, rather than
// answering NoBlocks forever to a peer that never moves on.
func TestBlockfetchServerRequestRange_RepeatedBadEndPointReachesCloseThreshold(
	t *testing.T,
) {
	const closeWarnMsg = "closing stuck peer after repeated missing-point requests"

	f := newBlockfetchRangeFixture(t)
	start := f.point(0)
	wrongHash := sha256.Sum256([]byte("not-our-block"))
	end := ocommon.NewPoint(
		f.blocks[1].Slot,
		append([]byte(nil), wrongHash[:]...),
	)

	for i := 1; i <= blockfetchMaxConsecutiveNoBlocks; i++ {
		f.logBuf.Reset()
		f.requestRange(t, start, end)
		assert.Equal(
			t,
			[]byte{blockfetch.MessageTypeNoBlocks},
			f.readMessageTypes(t, 1),
			"request %d should be answered with NoBlocks",
			i,
		)
		if i < blockfetchMaxConsecutiveNoBlocks {
			assert.False(
				t,
				strings.Contains(f.logBuf.String(), closeWarnMsg),
				"request %d should not yet reach the close threshold",
				i,
			)
		} else {
			testutil.WaitForCondition(
				t,
				func() bool {
					return strings.Contains(f.logBuf.String(), closeWarnMsg)
				},
				2*time.Second,
				"expected close-eligible WARN once the threshold is reached",
			)
		}
	}
}

// The control for the fix: a range whose start and end are both points on our
// chain must still be served in full -- StartBatch, every block in the range,
// BatchDone -- and must not be diverted into the NoBlocks path.
func TestBlockfetchServerRequestRange_InChainRangeStillServedInFull(
	t *testing.T,
) {
	f := newBlockfetchRangeFixture(t)

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
		"an in-chain range must still stream every block it covers",
	)
}

// A single-block range whose start and end are the same in-chain point is the
// narrowest case the end-point resolution must not reject.
func TestBlockfetchServerRequestRange_SingleBlockInChainRangeStillServed(
	t *testing.T,
) {
	f := newBlockfetchRangeFixture(t)

	f.requestRange(t, f.point(1), f.point(1))

	assert.Equal(
		t,
		[]byte{
			blockfetch.MessageTypeStartBatch,
			blockfetch.MessageTypeBlock,
			blockfetch.MessageTypeBatchDone,
		},
		f.readMessageTypes(t, 3),
		"a single-block in-chain range must still be served",
	)
}
