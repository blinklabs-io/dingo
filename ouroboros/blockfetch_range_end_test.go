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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
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
	connID ouroboros.ConnectionId
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
	slots := make([]uint64, 3)
	for i := range slots {
		slots[i] = uint64(i+1) * 10
	}
	return newBlockfetchRangeFixtureWithSlots(t, slots)
}

// newBlockfetchRangeFixtureWithSlots is newBlockfetchRangeFixture generalized
// to caller-chosen slots, so a test can reproduce a sparse or
// low-active-slot-coefficient custom network where consecutive real blocks
// span far more slots than mainnet's stability window (#4354).
func newBlockfetchRangeFixtureWithSlots(
	t *testing.T,
	slots []uint64,
) *blockfetchRangeFixture {
	t.Helper()
	return newBlockfetchRangeFixtureWithSlotsAndConfig(t, slots, nil)
}

// newBlockfetchRangeFixtureWithSlotsAndConfig is
// newBlockfetchRangeFixtureWithSlots generalized to an optional
// *cardano.CardanoNodeConfig, so a test can control what
// o.ledgerState.SecurityParam() (LedgerState.SecurityParam, distinct from
// the ChainManager-level testSecurityParamLedger below) returns. A nil
// config leaves LedgerState without a CardanoNodeConfig, so SecurityParam()
// falls back to blockfetchBatchSlotThresholdDefault.
func newBlockfetchRangeFixtureWithSlotsAndConfig(
	t *testing.T,
	slots []uint64,
	cardanoConfig *cardano.CardanoNodeConfig,
) *blockfetchRangeFixture {
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

	blocks := make([]chain.RawBlock, 0, len(slots))
	var prevHash []byte
	for i, slot := range slots {
		sum := sha256.Sum256([]byte{byte(i)})
		hash := append([]byte(nil), sum[:]...)
		blocks = append(blocks, chain.RawBlock{
			Slot:        slot,
			Hash:        hash,
			BlockNumber: uint64(i),
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(blocks))

	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: cardanoConfig,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
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
		connID: conn.Id(),
	}
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
	f := newBlockfetchRangeFixture(t)
	start := f.point(0)
	wrongHash := sha256.Sum256([]byte("not-our-block"))
	end := ocommon.NewPoint(
		f.blocks[1].Slot,
		append([]byte(nil), wrongHash[:]...),
	)

	for i := 1; i <= blockfetchMaxConsecutiveNoBlocks; i++ {
		f.requestRange(t, start, end)
		assert.Equal(
			t,
			[]byte{blockfetch.MessageTypeNoBlocks},
			f.readMessageTypes(t, 1),
			"request %d should be answered with NoBlocks",
			i,
		)
	}
	// The valve must close the registered transport at the threshold. The
	// connection manager removes it when the real protocol connection closes.
	testutil.WaitForCondition(t, func() bool {
		return f.o.connManager.GetConnectionById(f.connID) == nil
	}, 10*time.Second, "threshold must close the peer")
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

// TestBlockfetchServerRequestRange_SparseNetworkRangeServedOverWire is issue
// #4354, end to end: a real MsgRequestRange whose endpoint slots differ by
// more than 129600 (the old, now-removed MaxBlockFetchRange) must still be
// served in full when every requested block is a real point on the chain --
// this is what a sparse or low-active-slot-coefficient custom network
// produces when a client batches consecutive blocks for BlockFetch. Both
// endpoints are validated against the chain by blockfetchServerRequestRange
// before this ever reaches the block-count bound in blockfetchServerSendBatch,
// so this also exercises that endpoint-validation path with a genuinely
// oversized slot span.
func TestBlockfetchServerRequestRange_SparseNetworkRangeServedOverWire(
	t *testing.T,
) {
	const slotGap = 70000 // 2 gaps * 70000 > 129600 across 3 blocks
	slots := []uint64{slotGap, 2 * slotGap, 3 * slotGap}
	f := newBlockfetchRangeFixtureWithSlots(t, slots)
	require.Greater(
		t,
		f.blocks[2].Slot-f.blocks[0].Slot,
		uint64(129600),
		"test setup must exercise a slot span larger than the old fixed limit",
	)

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
		"a sparse-network range spanning more than 129600 slots must still be served in full, not answered with NoBlocks",
	)
}

// smallSecurityParamCardanoConfig builds a minimal Byron+Shelley genesis
// config whose security parameter K is k, so
// LedgerState.SecurityParam() -- and so maxBlockFetchBlocksForSecurityParam
// -- is small and test-controlled instead of falling back to
// blockfetchBatchSlotThresholdDefault (50000, far too large to build a
// real-chain test around).
func smallSecurityParamCardanoConfig(
	t *testing.T,
	k int,
) *cardano.CardanoNodeConfig {
	t.Helper()
	byronGenesisJSON, err := cardano.EmbeddedConfigFS.ReadFile(
		"mainnet/byron-genesis.json",
	)
	require.NoError(t, err)
	var byronGenesis map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(byronGenesisJSON, &byronGenesis))
	var protocolConsts map[string]json.RawMessage
	require.NoError(
		t,
		json.Unmarshal(byronGenesis["protocolConsts"], &protocolConsts),
	)
	protocolConsts["k"], err = json.Marshal(k)
	require.NoError(t, err)
	byronGenesis["protocolConsts"], err = json.Marshal(protocolConsts)
	require.NoError(t, err)
	byronGenesisJSON, err = json.Marshal(byronGenesis)
	require.NoError(t, err)
	shelleyGenesisJSON := fmt.Sprintf(
		`{"activeSlotsCoeff": 0.05, "securityParam": %d, "systemStart": "2022-10-25T00:00:00Z"}`,
		k,
	)
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(string(byronGenesisJSON))),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)
	return cfg
}

// TestBlockfetchServerRequestRange_OversizedRangeRejectedWithNoBlocks is
// review comment feedback on #4354's original fix: enforcing the block-count
// bound only in blockfetchServerSendBatch, after StartBatch, made an
// over-cap range unrecoverable for an honest peer -- the transport drops
// with no protocol-level signal, and retrying the identical range repeats
// the same drop forever. Both endpoints are already resolved against the
// chain before StartBatch, so the bound belongs on the NoBlocks path with
// the other invalid-range rejections instead. This drives a real
// MsgRequestRange whose block count exceeds the (test-configured, small-K)
// cap over the actual wire and asserts a clean single NoBlocks answer, not
// a dropped connection.
func TestBlockfetchServerRequestRange_OversizedRangeRejectedWithNoBlocks(
	t *testing.T,
) {
	// k=1 forces blockfetchMaxBlocksFloor (not 3*k) to be the binding
	// bound, keeping the fixture's block count in the low thousands rather
	// than needing a k large enough for 3*k itself to matter.
	maxBlocks := maxBlockFetchBlocksForSecurityParam(1)
	blockCount := maxBlocks + 1
	slots := make([]uint64, blockCount)
	for i := range slots {
		slots[i] = uint64(i + 1)
	}
	f := newBlockfetchRangeFixtureWithSlotsAndConfig(
		t,
		slots,
		smallSecurityParamCardanoConfig(t, 1),
	)

	f.requestRange(t, f.point(0), f.point(blockCount-1))

	assert.Equal(
		t,
		[]byte{blockfetch.MessageTypeNoBlocks},
		f.readMessageTypes(t, 1),
		"a range whose block count exceeds the cap must be answered with a clean NoBlocks, not a dropped connection",
	)
}

// The oversized-range rejection must feed the same stuck-peer valve every
// other invalid-range rejection in blockfetchServerRequestRange feeds,
// mirroring TestBlockfetchServerRequestRange_RepeatedBadEndPointReachesCloseThreshold.
func TestBlockfetchServerRequestRange_RepeatedOversizedRangeReachesCloseThreshold(
	t *testing.T,
) {
	maxBlocks := maxBlockFetchBlocksForSecurityParam(1)
	blockCount := maxBlocks + 1
	slots := make([]uint64, blockCount)
	for i := range slots {
		slots[i] = uint64(i + 1)
	}
	f := newBlockfetchRangeFixtureWithSlotsAndConfig(
		t,
		slots,
		smallSecurityParamCardanoConfig(t, 1),
	)
	start := f.point(0)
	end := f.point(blockCount - 1)

	for i := 1; i <= blockfetchMaxConsecutiveNoBlocks; i++ {
		f.requestRange(t, start, end)
		assert.Equal(
			t,
			[]byte{blockfetch.MessageTypeNoBlocks},
			f.readMessageTypes(t, 1),
			"request %d should be answered with NoBlocks",
			i,
		)
	}
	testutil.WaitForCondition(t, func() bool {
		return f.o.connManager.GetConnectionById(f.connID) == nil
	}, 10*time.Second, "threshold must close the peer")
}
