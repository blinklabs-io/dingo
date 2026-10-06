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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockfetchRangeAdmission_PerConnAndGlobalBounds(t *testing.T) {
	t.Parallel()

	a := newBlockfetchRangeAdmission(2, 3)
	c1, c2 := testConnIdWithPort(4001), testConnIdWithPort(4002)

	r1, res := a.reserve(c1)
	require.Equal(t, blockfetchRangeAdmitted, res)
	_, res = a.reserve(c1)
	require.Equal(t, blockfetchRangeAdmitted, res)
	_, res = a.reserve(c1)
	assert.Equal(t, blockfetchRangeConnSaturated, res)

	_, res = a.reserve(c2)
	require.Equal(t, blockfetchRangeAdmitted, res)
	_, res = a.reserve(testConnIdWithPort(4003))
	assert.Equal(t, blockfetchRangeGlobalSaturated, res)

	r1()
	r1() // idempotent
	total, conn := a.counts(c1)
	assert.Equal(t, 2, total)
	assert.Equal(t, 1, conn)
	_, res = a.reserve(testConnIdWithPort(4003))
	assert.Equal(t, blockfetchRangeAdmitted, res)
}

// A connection-closed event must not free reservations still held by sender
// goroutines: their iterators are live until each sender exits, so freeing the
// slot early lets repeated disconnects exceed the global bound. The event is
// also keyed only by ConnectionId, so a delayed one must not clear the
// reservations of a replacement connection that reuses the same ID.
func TestBlockfetchRangeAdmission_ConnClosedEventKeepsLiveSendersCharged(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	peer := testConnIdWithPort(4001)
	oldSender, res := o.blockfetchRangeAdmission.reserve(peer)
	require.Equal(t, blockfetchRangeAdmitted, res)
	replacement, res := o.blockfetchRangeAdmission.reserve(peer)
	require.Equal(t, blockfetchRangeAdmitted, res)

	o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: peer},
	))

	total, conn := o.blockfetchRangeAdmission.counts(peer)
	assert.Equal(t, 2, total, "global count while senders are live")
	assert.Equal(t, 2, conn, "per-connection count while senders are live")

	oldSender()
	total, conn = o.blockfetchRangeAdmission.counts(peer)
	assert.Equal(t, 1, total, "replacement still charged globally")
	assert.Equal(t, 1, conn, "replacement still charged per connection")

	replacement()
	total, conn = o.blockfetchRangeAdmission.counts(peer)
	assert.Zero(t, total)
	assert.Zero(t, conn)
}

func TestNewOuroboros_BlockfetchAdmissionDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	def := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	assert.Equal(t, blockfetchMaxRangesPerConnDefault,
		def.blockfetchRangeAdmission.maxPerConn)
	assert.Equal(t, blockfetchMaxRangesGlobalDefault,
		def.blockfetchRangeAdmission.maxGlobal)

	custom := newOuroboros(OuroborosConfig{
		Logger:                     logger,
		EventBus:                   event.NewEventBus(nil, logger),
		BlockfetchMaxRangesPerConn: 2,
		BlockfetchMaxRangesGlobal:  7,
	})
	assert.Equal(t, 2, custom.blockfetchRangeAdmission.maxPerConn)
	assert.Equal(t, 7, custom.blockfetchRangeAdmission.maxGlobal)
}

// A saturated server must answer NoBlocks without touching the ledger: the
// ledger is nil here, so reaching GetChainFromPoint (iterator creation) would
// nil-dereference instead of producing the NoBlocks reply.
func TestBlockfetchServerRequestRange_SaturationRejectedBeforeIterator(
	t *testing.T,
) {
	t.Parallel()

	for name, tc := range map[string]struct {
		perConn, global int
		holderIsPeer    bool
	}{
		"global": {perConn: 4, global: 1},
		"conn":   {perConn: 1, global: 4, holderIsPeer: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			o := newOuroboros(OuroborosConfig{
				Logger:                     logger,
				EventBus:                   event.NewEventBus(nil, logger),
				BlockfetchMaxRangesPerConn: tc.perConn,
				BlockfetchMaxRangesGlobal:  tc.global,
			})
			o.ledgerState = nil
			opts, peer := newMuxerServerPeer(t)
			cfg, err := blockfetch.NewConfig(o.blockfetchServerConnOpts()...)
			require.NoError(t, err)
			peer.start(t, blockfetch.NewServer(opts, &cfg))

			holder := testConnIdWithPort(9999)
			if tc.holderIsPeer {
				holder = opts.ConnectionId
			}
			_, res := o.blockfetchRangeAdmission.reserve(holder)
			require.Equal(t, blockfetchRangeAdmitted, res)

			start := ocommon.NewPoint(10, make([]byte, 32))
			end := ocommon.NewPoint(20, make([]byte, 32))
			peer.send(t, blockfetch.ProtocolId,
				blockfetch.NewMsgRequestRange(start, end))
			protocolID, msg := peer.readMessage(t, testutil.AsyncWait)
			require.Equal(t, blockfetch.ProtocolId, protocolID)
			assert.Equal(t, byte(blockfetch.MessageTypeNoBlocks), msg[1])
		})
	}
}

func TestBlockfetchServerRequestRange_ReservationLifecycle(t *testing.T) {
	t.Parallel()

	f := newBlockfetchRangeFixture(t)
	counts := func() (int, int) {
		return f.o.blockfetchRangeAdmission.counts(f.connID)
	}

	// Error path: a start point we do not hold is rejected after the
	// reservation was taken and must give it back.
	bogus := ocommon.NewPoint(5, make([]byte, 32))
	f.requestRange(t, bogus, f.point(2))
	require.Equal(t,
		[]byte{blockfetch.MessageTypeNoBlocks}, f.readMessageTypes(t, 1))
	testutil.WaitForCondition(t, func() bool {
		total, conn := counts()
		return total == 0 && conn == 0
	}, testutil.AsyncWait, "reservation held after rejected range")

	// Completion path: a served range releases once the sender finishes.
	f.requestRange(t, f.point(0), f.point(2))
	assert.Equal(t, []byte{
		blockfetch.MessageTypeStartBatch,
		blockfetch.MessageTypeBlock,
		blockfetch.MessageTypeBlock,
		blockfetch.MessageTypeBlock,
		blockfetch.MessageTypeBatchDone,
	}, f.readMessageTypes(t, 5))
	testutil.WaitForCondition(t, func() bool {
		total, conn := counts()
		return total == 0 && conn == 0
	}, testutil.AsyncWait, "reservation held after completed range")
}

// A peer that disconnects mid-range must not leave its reservation held. The
// fixture never delivers ConnectionClosedEvent, so only the sender goroutine's
// own exit can release the slot here.
func TestBlockfetchServerRequestRange_PeerDisconnectMidRangeReleases(
	t *testing.T,
) {
	t.Parallel()

	f := newBlockfetchRangeFixture(t)
	f.requestRange(t, f.point(0), f.point(2))
	// StartBatch proves the sender goroutine owns the reservation. The rest
	// of the batch is left unread, so the sender is blocked mid-range.
	require.Equal(t,
		[]byte{blockfetch.MessageTypeStartBatch}, f.readMessageTypes(t, 1))
	total, conn := f.o.blockfetchRangeAdmission.counts(f.connID)
	require.Equal(t, 1, total)
	require.Equal(t, 1, conn)

	require.NoError(t, f.peer.peerConn.Close())
	testutil.WaitForCondition(t, func() bool {
		total, conn := f.o.blockfetchRangeAdmission.counts(f.connID)
		return total == 0 && conn == 0
	}, testutil.AsyncWait, "reservation held after peer disconnect mid-range")
}

// With the only slot held, a request is rejected; once released the same
// request streams, so the rejection was caused by admission and not the range.
func TestBlockfetchServerRequestRange_AdmitsAfterRelease(t *testing.T) {
	t.Parallel()

	f := newBlockfetchRangeFixture(t)
	f.o.blockfetchRangeAdmission = newBlockfetchRangeAdmission(1, 1)
	release, res := f.o.blockfetchRangeAdmission.reserve(f.connID)
	require.Equal(t, blockfetchRangeAdmitted, res)

	f.requestRange(t, f.point(0), f.point(2))
	require.Equal(t,
		[]byte{blockfetch.MessageTypeNoBlocks}, f.readMessageTypes(t, 1))

	release()
	f.requestRange(t, f.point(0), f.point(2))
	assert.Equal(t, byte(blockfetch.MessageTypeStartBatch),
		f.readMessageTypes(t, 1)[0])
}

// counts returns the process-wide and per-connection in-flight range counts.
func (a *blockfetchRangeAdmission) counts(
	connId ouroboros.ConnectionId,
) (total int, conn int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if cr := a.conns[connIdKey(connId)]; cr != nil {
		conn = cr.active
	}
	return a.total, conn
}
