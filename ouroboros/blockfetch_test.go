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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ouroboros_conn "github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// auxTxBody returns a transaction body carrying auxiliary_data_hash (field 7)
// equal to the blake2b-256 of aux, so a decode failure cannot be a hash
// mismatch. The same minimal body decodes in every Shelley-family era;
// Shelley requires the ttl.
func auxTxBody(aux []byte) []byte {
	hash := lcommon.Blake2b256Hash(aux)
	// {0: [], 1: [], 2: 0, 3: 1000, 7: h'<hash>'}
	body := []byte{
		0xa5, 0x00, 0x80, 0x01, 0x80, 0x02, 0x00, 0x03, 0x19, 0x03, 0xe8,
		0x07, 0x58, 0x20,
	}
	return append(body, hash.Bytes()...)
}

// auxBlockCase is one block body: txCount transactions, each carrying the
// auxiliary data at its index in auxByIndex when present. auxByIndex may name
// an index past the transaction list.
type auxBlockCase struct {
	name       string
	txCount    int
	auxByIndex map[uint][]byte
	wantErr    string
}

func buildAuxBlock(t *testing.T, blockType uint, tc auxBlockCase) []byte {
	t.Helper()
	metadataMap := []byte{0xa1, 0x01, 0x01}
	bodies := []byte{0x80 + byte(tc.txCount)}
	witnesses := []byte{0x80 + byte(tc.txCount)}
	for i := range tc.txCount {
		aux := tc.auxByIndex[uint(i)]
		if aux == nil {
			aux = metadataMap
		}
		bodies = append(bodies, auxTxBody(aux)...)
		witnesses = append(witnesses, 0xa0)
	}
	auxMap := []byte{0xa0 + byte(len(tc.auxByIndex))}
	for index := range uint(4) {
		if aux, ok := tc.auxByIndex[index]; ok {
			auxMap = append(auxMap, byte(index))
			auxMap = append(auxMap, aux...)
		}
	}
	components := [][]byte{bodies, witnesses, auxMap}
	if blockType >= gledger.BlockTypeAlonzo {
		components = append(components, []byte{0x80})
	}
	return testutil.BuildBlockBytesFromComponents(
		t, blockType, 100, 1, components...,
	)
}

// TestDecodeBlockAuxiliaryDataRules drives raw Shelley-through-Conway block
// bytes through every dingo entry point that decodes a block body from the
// wire or from storage, and asserts on the auxiliary-data rule that rejected
// the block rather than on rejection alone, since an unrelated failure would
// also reject these blocks. A block that fails to decode never reaches ledger
// application.
func TestDecodeBlockAuxiliaryDataRules(t *testing.T) {
	t.Parallel()

	metadataMap := []byte{0xa1, 0x01, 0x01}                // {1: 1}
	shelleyMaArray := []byte{0x82, 0xa1, 0x01, 0x01, 0x80} // [{1: 1}, []]
	shelleyMaOneElement := []byte{0x81, 0xa0}              // [{}]
	// 259({0: {1: 1}})
	taggedMetadata := []byte{0xd9, 0x01, 0x03, 0xa1, 0x00, 0xa1, 0x01, 0x01}
	// 259({0: {1: 1}, 6: []})
	taggedUnknownField := []byte{
		0xd9, 0x01, 0x03, 0xa2, 0x00, 0xa1, 0x01, 0x01, 0x06, 0x80,
	}
	// taggedPlutus returns 259({key: []}); key 2 carries Plutus V1 scripts
	// and key 5 Plutus V4.
	taggedPlutus := func(key byte) []byte {
		return []byte{0xd9, 0x01, 0x03, 0xa1, key, 0x80}
	}
	const (
		arrayRejected  = "Shelley-MA auxiliary-data arrays are not supported in this era"
		taggedRejected = "tagged auxiliary-data maps are not supported in this era"
	)
	plutusRejected := func(version int) string {
		return "auxiliary scripts for Plutus V" +
			string(rune('0'+version)) + " are not supported in this era"
	}

	indexCases := []auxBlockCase{
		{name: "empty auxiliary-data map", txCount: 1},
		{
			name:       "last valid index",
			txCount:    2,
			auxByIndex: map[uint][]byte{1: metadataMap},
		},
		{
			name:       "first invalid index",
			txCount:    2,
			auxByIndex: map[uint][]byte{2: metadataMap},
			wantErr:    "outside transaction list length 2",
		},
		{
			name:       "index with no transactions",
			txCount:    0,
			auxByIndex: map[uint][]byte{0: metadataMap},
			wantErr:    "outside transaction list length 0",
		},
		{
			name:       "metadata map",
			txCount:    1,
			auxByIndex: map[uint][]byte{0: metadataMap},
		},
	}
	aux := func(name string, data []byte, wantErr string) auxBlockCase {
		return auxBlockCase{
			name:       name,
			txCount:    1,
			auxByIndex: map[uint][]byte{0: data},
			wantErr:    wantErr,
		}
	}
	// Each era accepts the formats of earlier eras and rejects those of
	// later ones, and a tagged map admits only the Plutus languages the era
	// defines.
	eras := []struct {
		name      string
		blockType uint
		cases     []auxBlockCase
	}{
		{
			name:      "shelley",
			blockType: gledger.BlockTypeShelley,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, arrayRejected),
				aux("tagged map", taggedMetadata, taggedRejected),
			},
		},
		{
			name:      "allegra",
			blockType: gledger.BlockTypeAllegra,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("shelley-ma array with one element", shelleyMaOneElement,
					"must have 2 elements"),
				aux("tagged map", taggedMetadata, taggedRejected),
			},
		},
		{
			name:      "mary",
			blockType: gledger.BlockTypeMary,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("tagged map", taggedMetadata, taggedRejected),
			},
		},
		{
			name:      "alonzo",
			blockType: gledger.BlockTypeAlonzo,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("tagged map", taggedMetadata, ""),
				aux("tagged map unknown field", taggedUnknownField,
					"unknown auxiliary-data field 6"),
				aux("plutus v1 scripts", taggedPlutus(2), ""),
				aux("plutus v2 scripts", taggedPlutus(3), plutusRejected(2)),
			},
		},
		{
			name:      "babbage",
			blockType: gledger.BlockTypeBabbage,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("tagged map", taggedMetadata, ""),
				aux("tagged map unknown field", taggedUnknownField,
					"unknown auxiliary-data field 6"),
				aux("plutus v2 scripts", taggedPlutus(3), ""),
				aux("plutus v3 scripts", taggedPlutus(4), plutusRejected(3)),
			},
		},
		{
			name:      "conway",
			blockType: gledger.BlockTypeConway,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("shelley-ma array with one element", shelleyMaOneElement,
					"must have 2 elements"),
				aux("tagged map", taggedMetadata, ""),
				aux("tagged map unknown field", taggedUnknownField,
					"unknown auxiliary-data field 6"),
				aux("plutus v3 scripts", taggedPlutus(4), ""),
				aux("plutus v4 scripts", taggedPlutus(5), plutusRejected(4)),
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	decoders := []struct {
		name   string
		conway bool
		decode func(blockType uint, raw []byte) (gledger.Block, error)
	}{
		{
			name: "blockfetch",
			decode: newOuroboros(OuroborosConfig{
				Logger:       logger,
				NetworkMagic: ouroboros.NetworkMainnet.NetworkMagic,
			}).decodeBlockfetchBlock,
		},
		{
			name:   "musashi blockfetch",
			conway: true,
			decode: newOuroboros(OuroborosConfig{
				Logger:       logger,
				NetworkMagic: ouroboros.NetworkCardanoMusashi.NetworkMagic,
			}).decodeBlockfetchBlock,
		},
		{
			// The ledger decodes stored block CBOR through this entry
			// before applying it.
			name: "stored block",
			decode: func(blockType uint, raw []byte) (gledger.Block, error) {
				return models.DecodeBlockCbor(blockType, raw)
			},
		},
	}

	for _, era := range eras {
		for _, tc := range append(append([]auxBlockCase{}, indexCases...), era.cases...) {
			for _, decoder := range decoders {
				if decoder.conway && era.blockType != gledger.BlockTypeConway {
					continue
				}
				t.Run(era.name+"/"+tc.name+"/"+decoder.name, func(t *testing.T) {
					t.Parallel()
					raw := buildAuxBlock(t, era.blockType, tc)
					block, err := decoder.decode(era.blockType, raw)
					if tc.wantErr != "" {
						require.ErrorContains(t, err, tc.wantErr)
						return
					}
					require.NoError(t, err)
					require.Len(t, block.Transactions(), tc.txCount)
				})
			}
		}
	}
}

func TestBlockfetchServerSendBatch_ExactEndpointContract(t *testing.T) {
	endBlock := testBlockfetchIteratorBlock(200)
	boundaryBlock := testBlockfetchIteratorBlock(200)
	boundaryBlock.Block.Type = gledger.BlockTypeByronEbb
	boundaryBlock.Point.Hash = []byte{0xeb}
	tests := []struct {
		name       string
		steps      []blockfetchIteratorStep
		wantBlocks int
		wantDone   bool
	}{
		{
			name: "complete range",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{result: endBlock},
			},
			wantBlocks: 2,
			wantDone:   true,
		},
		{
			name: "earlier boundary block at endpoint slot",
			steps: []blockfetchIteratorStep{
				{result: boundaryBlock},
				{result: endBlock},
			},
			wantBlocks: 2,
			wantDone:   true,
		},
		{
			name: "rollback after partial delivery",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{result: &chain.ChainIteratorResult{Rollback: true}},
			},
			wantBlocks: 1,
			wantDone:   true,
		},
		{
			name: "nil result after partial delivery",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{},
			},
			wantBlocks: 1,
			wantDone:   true,
		},
		{
			name: "overshoots endpoint",
			steps: []blockfetchIteratorStep{
				{result: testBlockfetchIteratorBlock(100)},
				{result: testBlockfetchIteratorBlock(201)},
			},
			wantBlocks: 1,
			wantDone:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			bus := event.NewEventBus(nil, logger)
			t.Cleanup(bus.Close)
			node := newOuroboros(OuroborosConfig{
				Logger:   logger,
				EventBus: bus,
			})
			iter := &stubBlockfetchIterator{steps: test.steps}
			server := &stubBlockfetchBatchServer{}
			conn := &stubBlockfetchConnection{errChan: make(chan error)}
			err := node.blockfetchServerSendBatch(
				testConnId().String(),
				test.steps[0].result.Point,
				endBlock.Point,
				iter,
				server,
				conn,
				testMaxBlocksUnbounded,
			)
			if test.wantDone {
				require.NoError(t, err)
				require.Equal(t, 1, server.batchDoneCalls)
				require.Zero(t, conn.closeCalls)
			} else {
				require.Error(t, err)
				require.Zero(t, server.batchDoneCalls)
				require.Equal(t, 1, conn.closeCalls)
			}
			require.Equal(t, test.wantBlocks, server.blockCalls)
			require.Equal(t, 1, iter.cancelCalls)
		})
	}
}

// capturingRangeRequester records the RangeRequest it is handed.
type capturingRangeRequester struct {
	mu   sync.Mutex
	reqs []blockfetch.RangeRequest
}

func (f *capturingRangeRequester) RequestRange(
	_ context.Context,
	req blockfetch.RangeRequest,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return uint64(len(f.reqs)), nil
}

func TestBlockfetchClientRequestRangeSendsExpectedBytes(t *testing.T) {
	t.Parallel()

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	for _, tc := range []struct {
		name string
		want uint64
	}{
		{name: "estimate", want: 123456},
		{name: "no estimate", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &capturingRangeRequester{}
			o := newOuroboros(OuroborosConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			})
			o.blockfetchConnClient = func(
				ouroboros.ConnectionId,
			) (blockfetchRangeRequester, error) {
				return fake, nil
			}
			var gotStart, gotEnd ocommon.Point
			o.blockfetchRangeBytes = func(s, e ocommon.Point) uint64 {
				gotStart, gotEnd = s, e
				return tc.want
			}

			_, err := o.BlockfetchClientRequestRange(testConnId(), start, end)
			require.NoError(t, err)

			require.Len(t, fake.reqs, 1)
			require.Equal(t, tc.want, fake.reqs[0].ExpectedBytes)
			require.Equal(t, start, gotStart)
			require.Equal(t, end, gotEnd)
		})
	}
}

// TestBlockfetchClientRequestRangeUsesLedgerEstimateByDefault covers the
// production default of the blockfetchRangeBytes seam: an Ouroboros built
// with a LedgerState sends the ledger's queued-header estimate without any
// test override.
func TestBlockfetchClientRequestRangeUsesLedgerEstimateByDefault(
	t *testing.T,
) {
	t.Parallel()

	ls := newTestLedgerState(t)
	blocks, err := testfixtures.GenerateConwayChainWithTransactions(3)
	require.NoError(t, err)
	for _, b := range blocks {
		require.NoError(t, ls.Chain().AddBlockHeader(b.Header()))
	}
	start := ocommon.NewPoint(blocks[0].SlotNumber(), blocks[0].Hash().Bytes())
	end := ocommon.NewPoint(blocks[2].SlotNumber(), blocks[2].Hash().Bytes())
	want := ls.BlockfetchRangeExpectedBytes(start, end)
	require.NotZero(t, want)

	fake := &capturingRangeRequester{}
	o := newOuroboros(OuroborosConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: ls,
	})
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return fake, nil
	}

	_, err = o.BlockfetchClientRequestRange(testConnId(), start, end)
	require.NoError(t, err)
	require.Len(t, fake.reqs, 1)
	require.Equal(t, want, fake.reqs[0].ExpectedBytes)
}

// TestBlockfetchClientWiringKeepsRangesPipelinedUnderRealEstimates drives
// dingo's real client wiring with range requests sized at the ledger's
// per-range cap, the size real estimates produce on a heavy region. With no
// configured budget gouroboros' 100 x 88 KiB default admits only one such
// range at a time; the configured budget must keep several in flight.
func TestBlockfetchClientWiringKeepsRangesPipelinedUnderRealEstimates(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := newBlockfetchPeerWithOpts(t, o.blockfetchClientConnOpts()...)

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))
	request := func() chan error {
		done := make(chan error, 1)
		go func() {
			_, err := peer.client.RequestRange(
				context.Background(),
				blockfetch.RangeRequest{
					Start:         start,
					End:           end,
					ExpectedBytes: ledger.BlockfetchMaxRangeBytes,
				},
			)
			done <- err
		}()
		return done
	}

	const pipelined = 3
	for i := range pipelined {
		select {
		case err := <-request():
			require.NoError(t, err, "request %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf(
				"range %d of %d blocked on the in-flight byte budget",
				i+1,
				pipelined,
			)
		}
	}

	// The budget is a bound, not absent: nothing answers on the wire here,
	// so one more range must wait for capacity.
	testutil.RequireNoReceive(
		t,
		request(),
		200*time.Millisecond,
		"a range beyond the in-flight budget was admitted",
	)
}

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
// span far more slots than mainnet's stability window.
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

// TestBlockfetchServerRequestRange_SparseNetworkRangeServedOverWire covers a
// sparse network range end to end: a real MsgRequestRange whose endpoint slots differ by
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
	shelleyGenesisJSON := fmt.Sprintf(
		`{"activeSlotsCoeff": 0.05, "securityParam": %d, "systemStart": "2022-10-25T00:00:00Z"}`,
		k,
	)
	cfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"mainnet/config.json",
	)
	require.NoError(t, err)
	cfg.ByronGenesis().ProtocolConsts.K = k
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)
	return cfg
}

// TestBlockfetchServerRequestRange_OversizedRangeRejectedWithNoBlocks is
// review comment feedback on original fix: enforcing the block-count
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

// fakeBlockfetchRangeRequester is a test double for blockfetchRangeRequester,
// standing in for a live *blockfetch.Client. It lets
// BlockfetchClientRequestRange's own dispatch and blockFetchStarts
// bookkeeping be exercised without a live connection registered in
// connManager: the production code path (blockfetchConnClientLive) requires
// exactly that, which no existing test stands up for this function.
type fakeBlockfetchRangeRequester struct {
	mu     sync.Mutex
	nextID uint64
}

func (f *fakeBlockfetchRangeRequester) RequestRange(
	_ context.Context,
	_ blockfetch.RangeRequest,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	return f.nextID, nil
}

// TestBlockfetchClientRequestRangeOverlappingRequestsDoNotClobberStartTimes
// dispatches two requests on the same connId with different request IDs and
// asserts blockFetchStarts keeps a distinct entry for each.
//
// gouroboros' nextRequestId is scoped per connection, so requestId alone is
// not globally unique, and connId alone is exactly today's clobber bug: prior
// to RequestRange replacing GetBlockRange, BlockfetchClientRequestRange kept
// blockFetchStarts keyed only by connId, so
// `o.blockFetchStarts[connId] = time.Now()` unconditionally overwrote
// whatever entry a still-outstanding request on the same connection had
// already recorded. Pipelining means more than one request can be
// outstanding on one connection at once, so the second dispatch's start time
// would silently replace the first's, corrupting that request's own latency
// accounting when it eventually resolves.
func TestBlockfetchClientRequestRangeOverlappingRequestsDoNotClobberStartTimes(
	t *testing.T,
) {
	t.Parallel()

	connId := testConnId()
	fake := &fakeBlockfetchRangeRequester{}
	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return fake, nil
	}

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	firstId, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.NoError(t, err)
	secondId, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.NoError(t, err)
	require.NotEqual(
		t,
		firstId,
		secondId,
		"the fake requester must hand out distinct request IDs for this to prove anything",
	)

	o.blockFetchMutex.Lock()
	_, firstOk := o.blockFetchStarts[blockFetchKey{
		connId:    connId,
		requestId: firstId,
	}]
	_, secondOk := o.blockFetchStarts[blockFetchKey{
		connId:    connId,
		requestId: secondId,
	}]
	entryCount := len(o.blockFetchStarts)
	o.blockFetchMutex.Unlock()

	assert.True(
		t,
		firstOk,
		"the first request's start time must survive the second request's dispatch",
	)
	assert.True(
		t,
		secondOk,
		"the second request's start time must be recorded",
	)
	assert.Equal(
		t,
		2,
		entryCount,
		"both requests must have their own blockFetchStarts entry",
	)
}

// TestBlockfetchClientRequestRangeDepthTwoNeverBlocksOnCapacity dispatches two
// pipelined RequestRange calls back to back over dingo's real client wiring
// (WithRequestPipelining enabled, WithMaxInFlightBytes left at gouroboros'
// default), and asserts neither blocks waiting for in-flight-byte admission.
// A depth-2 pipeline's two default-sized reservations should be nowhere near
// the ~8.8MB default budget (100 * 88KiB) -- this pins that as an observed
// fact about the wiring, rather than trusting the arithmetic never changes
// underneath it.
func TestBlockfetchClientRequestRangeDepthTwoNeverBlocksOnCapacity(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := newBlockfetchPeerWithOpts(t, o.blockfetchClientConnOpts()...)

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	for i := range 2 {
		done := make(chan error, 1)
		go func() {
			_, err := peer.client.RequestRange(
				context.Background(),
				blockfetch.RangeRequest{Start: start, End: end},
			)
			done <- err
		}()
		select {
		case err := <-done:
			require.NoError(t, err, "request %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf(
				"RequestRange %d blocked on in-flight capacity at depth 2",
				i,
			)
		}
	}
}

// TestBlockfetchClientRequestRangeUnblocksOnStopWhileWaitingForCapacity
// verifies the safety argument BlockfetchClientRequestRange's use of
// context.Background() rests on: sendRequestRange's internal waits (the
// in-flight byte budget here) select on the connection's own protocol
// shutdown channel in addition to the caller's context, so a request parked
// waiting for capacity still unblocks when the protocol stops, even though
// this caller's own context is never canceled directly.
//
// This drives gouroboros' *blockfetch.Client directly (not through
// BlockfetchClientRequestRange), since it is gouroboros' own contract being
// verified, not dingo's wrapper of it.
func TestBlockfetchClientRequestRangeUnblocksOnStopWhileWaitingForCapacity(
	t *testing.T,
) {
	t.Parallel()

	peer := newBlockfetchPeerWithOpts(
		t,
		blockfetch.WithRequestPipelining(true),
		blockfetch.WithRangeDoneFunc(
			func(blockfetch.CallbackContext, error) error { return nil },
		),
		// A tiny budget forces the second request below to genuinely wait:
		// the first reservation is never released (its RangeDoneFunc never
		// resolves it -- nothing answers on the wire in this test), so the
		// queue never empties and the second call cannot bypass the check
		// through the "empty queue always admits" rule.
		blockfetch.WithMaxInFlightBytes(1),
	)

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	_, err := peer.client.RequestRange(context.Background(), blockfetch.RangeRequest{
		Start:         start,
		End:           end,
		ExpectedBytes: 1,
	})
	require.NoError(t, err)

	blocked := make(chan error, 1)
	go func() {
		_, err := peer.client.RequestRange(
			context.Background(),
			blockfetch.RangeRequest{Start: start, End: end, ExpectedBytes: 1},
		)
		blocked <- err
	}()

	// There is no exported hook for "the second call has reached the
	// capacity wait" (gouroboros' own beforeInFlightWait test seam is
	// unexported), so this is a bounded assertion that it has not returned
	// yet, not a proof it is parked in the wait specifically -- the
	// no-hook gap is real, and the remainder of the test still proves the
	// call unblocks specifically because of Stop(), not because it was
	// about to return anyway (see the timing below).
	testutil.RequireNoReceive(
		t,
		blocked,
		100*time.Millisecond,
		"second RequestRange returned before Stop() gave it a reason to",
	)

	// Client.Stop()'s own doc comment allows a delivery error here: this
	// client parked its RequestRange mid-batch, so ClientDone is not
	// pipelinable in Busy/Streaming (StateMap's PipelinedMessageTypes carries
	// only MessageTypeRequestRange there) and cannot go out until agency
	// returns -- which nothing in this test ever grants. Stop() still fully
	// tears down the protocol either way.
	stopErr := peer.client.Stop()
	if stopErr != nil {
		require.ErrorIs(t, stopErr, context.DeadlineExceeded)
	}

	select {
	case err := <-blocked:
		require.ErrorIs(t, err, protocol.ErrProtocolShuttingDown)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"RequestRange blocked on in-flight capacity did not unblock after Stop()",
		)
	}
}

// terminalBeforeIdRequester resolves each request terminally, through the
// RangeDoneFunc path, before handing its ID back to the dispatcher. That is
// the ordering RequestRange permits but a live peer only rarely produces:
// the request is on the wire when RequestRange returns, so the protocol's
// receive goroutine can run blockfetchClientRangeDone to completion while the
// dispatcher is still on its way to recording the start time.
type terminalBeforeIdRequester struct {
	o      *Ouroboros
	connId ouroboros.ConnectionId
	nextID uint64
}

func (f *terminalBeforeIdRequester) RequestRange(
	_ context.Context,
	_ blockfetch.RangeRequest,
) (uint64, error) {
	f.nextID++
	if err := f.o.blockfetchClientRangeDone(
		blockfetch.CallbackContext{
			ConnectionId: f.connId,
			RequestId:    f.nextID,
		},
		nil,
	); err != nil {
		return 0, err
	}
	return f.nextID, nil
}

// TestBlockfetchClientRequestRangeTerminalBeforeIdLeavesNoStartEntry asserts
// that a request whose terminal callback lands first records no start time.
// blockfetchClientRangeDone is a request's only deleter and runs exactly once,
// so an entry inserted after it has already fired is never removed: it sits in
// blockFetchStarts until the connection is torn down, and a peer that answers
// every request that quickly grows the map for the life of the connection.
func TestBlockfetchClientRequestRangeTerminalBeforeIdLeavesNoStartEntry(
	t *testing.T,
) {
	t.Parallel()

	connId := testConnId()
	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	fake := &terminalBeforeIdRequester{o: o, connId: connId}
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return fake, nil
	}

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	const requests = 4
	for range requests {
		_, err := o.BlockfetchClientRequestRange(connId, start, end)
		require.NoError(t, err)
	}

	o.blockFetchMutex.Lock()
	startCount := len(o.blockFetchStarts)
	earlyCount := len(o.blockFetchDoneEarly)
	o.blockFetchMutex.Unlock()

	assert.Equal(
		t,
		0,
		startCount,
		"a request already reported terminal must leave no start-time entry",
	)
	assert.Equal(
		t,
		0,
		earlyCount,
		"each dispatch must consume its own terminal marker",
	)
}

// TestBlockfetchClientRequestRangeTerminalAfterIdStillTimes is the control
// for the test above: in the ordinary ordering the start time is recorded and
// the terminal callback removes it, leaving both maps empty by a different
// route. Without it, a fix that simply stopped recording start times would
// satisfy the race test while destroying the timing this map exists for.
func TestBlockfetchClientRequestRangeTerminalAfterIdStillTimes(t *testing.T) {
	t.Parallel()

	connId := testConnId()
	fake := &fakeBlockfetchRangeRequester{}
	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return fake, nil
	}

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	requestId, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.NoError(t, err)

	o.blockFetchMutex.Lock()
	_, recorded := o.blockFetchStarts[blockFetchKey{
		connId:    connId,
		requestId: requestId,
	}]
	o.blockFetchMutex.Unlock()
	require.True(
		t,
		recorded,
		"the ordinary ordering must still record a start time",
	)

	require.NoError(t, o.blockfetchClientRangeDone(
		blockfetch.CallbackContext{
			ConnectionId: connId,
			RequestId:    requestId,
		},
		nil,
	))

	o.blockFetchMutex.Lock()
	startCount := len(o.blockFetchStarts)
	earlyCount := len(o.blockFetchDoneEarly)
	o.blockFetchMutex.Unlock()

	assert.Equal(t, 0, startCount, "the terminal callback must clear the entry")
	assert.Equal(
		t,
		0,
		earlyCount,
		"a terminal callback that found its entry must leave no marker",
	)
}

// TestBlockfetchClientBlockRawObservesDecodeStageOnCacheMissOnly is the
// regression test for the decode-stage histogram wired into
// blockfetchClientBlockRaw: the decode callback given to the shared cache is
// only invoked on a cache miss, so dingo_blockfetch_stage_duration_seconds
// must record exactly one "decode" sample for two deliveries of identical
// bytes, not two -- the second delivery is served from cache and does no
// decode work of its own.
func TestBlockfetchClientBlockRawObservesDecodeStageOnCacheMissOnly(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})
	blockType, raw := conwayBlockFixtureBytes(t)
	ctx := blockfetch.CallbackContext{}

	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))
	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))

	metric := &dto.Metric{}
	require.NoError(
		t,
		o.blockfetchMetrics.stageDecode.(prometheus.Histogram).Write(metric),
	)
	assert.Equal(
		t,
		uint64(1),
		metric.GetHistogram().GetSampleCount(),
		"only the cache-miss delivery should record a decode observation",
	)
}

// TestBlockfetchClientBlockRawNoopDecodeStageWhenMetricsDisabled confirms the
// decode timing wrapper does not panic when no PromRegistry is configured,
// matching the nil-safe pattern used by the other blockfetch/protocol
// metrics in this package.
func TestBlockfetchClientBlockRawNoopDecodeStageWhenMetricsDisabled(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{}) // no PromRegistry -> no metrics
	blockType, raw := conwayBlockFixtureBytes(t)
	ctx := blockfetch.CallbackContext{}

	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))
	assert.Nil(t, o.blockfetchMetrics)
}

// TestBlockfetchStageDurationBucketsCoverTailStalls pins
// dingo_blockfetch_stage_duration_seconds to the same range as
// dingo_ledger_block_stage_duration_seconds (see
// ledger.TestBlockStageDurationBucketsCoverTailStalls). The two histograms are
// documented as sharing one bucket range so they stay comparable across the
// same block's stages, so this must widen in lockstep.
func TestBlockfetchStageDurationBucketsCoverTailStalls(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})

	metric := &dto.Metric{}
	require.NoError(
		t,
		o.blockfetchMetrics.stageDecode.(prometheus.Histogram).Write(metric),
	)

	buckets := metric.GetHistogram().GetBucket()
	require.NotEmpty(
		t,
		buckets,
		"histogram must have at least one finite bucket boundary",
	)
	largest := buckets[len(buckets)-1].GetUpperBound()
	assert.GreaterOrEqual(
		t,
		largest,
		318.0,
		"largest finite bucket boundary (%vs) must match "+
			"dingo_ledger_block_stage_duration_seconds's widened range",
		largest,
	)
}

// syncLogBuffer is a mutex-guarded log sink. The blockfetch server processes
// requests on its own recvLoop goroutine, which logs concurrently with the
// test goroutine reading those logs back -- a plain bytes.Buffer would race.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncLogBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncLogBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *syncLogBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Reset()
}

// testConnId creates a ConnectionId with valid net.Addr values for testing.
func testConnId() ouroboros_conn.ConnectionId {
	return ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
}

type stubBlockfetchBatchServer struct {
	startBatchCalls int
	blockCalls      int
	batchDoneCalls  int
	startBatchErr   error
	blockErr        error
	batchDoneErr    error
}

func (s *stubBlockfetchBatchServer) StartBatch() error {
	s.startBatchCalls++
	return s.startBatchErr
}

func (s *stubBlockfetchBatchServer) Block(_ uint, _ []byte) error {
	s.blockCalls++
	return s.blockErr
}

func (s *stubBlockfetchBatchServer) BatchDone() error {
	s.batchDoneCalls++
	return s.batchDoneErr
}

type stubBlockfetchDrainBatchServer struct {
	stubBlockfetchBatchServer
	drainCalls    int
	drainResults  []bool
	drainTimeouts []time.Duration
}

func (s *stubBlockfetchDrainBatchServer) WaitSendQueueDrained(
	timeout time.Duration,
) bool {
	s.drainCalls++
	s.drainTimeouts = append(s.drainTimeouts, timeout)
	if len(s.drainResults) == 0 {
		return true
	}
	result := s.drainResults[0]
	s.drainResults = s.drainResults[1:]
	return result
}

type blockfetchIteratorStep struct {
	result *chain.ChainIteratorResult
	err    error
}

type stubBlockfetchIterator struct {
	steps       []blockfetchIteratorStep
	nextCalls   int
	cancelCalls int
}

func (i *stubBlockfetchIterator) Next(
	bool,
) (*chain.ChainIteratorResult, error) {
	if i.nextCalls >= len(i.steps) {
		return nil, chain.ErrIteratorChainTip
	}
	step := i.steps[i.nextCalls]
	i.nextCalls++
	return step.result, step.err
}

func (i *stubBlockfetchIterator) Cancel() {
	i.cancelCalls++
}

type stubBlockfetchConnection struct {
	errChan    chan error
	closeCalls int
	closeErr   error
}

func (c *stubBlockfetchConnection) ErrorChan() chan error {
	return c.errChan
}

func (c *stubBlockfetchConnection) Close() error {
	c.closeCalls++
	return c.closeErr
}

// testMaxBlocksUnbounded is passed to blockfetchServerSendBatch by tests
// that exercise something other than the block-count cap itself (iterator
// errors, rollback, send-drain backpressure, chain-tip exhaustion): large
// enough that none of their handful of blocks ever approaches it.
const testMaxBlocksUnbounded = 1 << 30

func testBlockfetchIteratorBlock(slot uint64) *chain.ChainIteratorResult {
	return &chain.ChainIteratorResult{
		Point: ocommon.NewPoint(slot, []byte{byte(slot)}),
		Block: models.Block{
			Slot: slot,
			Type: 1,
			Cbor: []byte{byte(slot), byte(slot + 1)},
		},
	}
}

func TestBlockfetchServerRequestRange_StartAfterEnd(t *testing.T) {
	t.Parallel()

	// When start slot > end slot, blockfetchServerRequestRange should
	// log a warning and attempt to send NoBlocks. Since we don't have a
	// real protocol server wired up, the NoBlocks call will panic on the
	// nil server. We use assert.Panics to catch that, and then verify the
	// warning was logged BEFORE the NoBlocks call, proving the range check
	// was reached (not GetChainFromPoint, which would be a different panic).
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	// LedgerState is intentionally nil - if the range check works,
	// we never reach GetChainFromPoint and avoid a nil dereference on
	// LedgerState.

	start := ocommon.NewPoint(100, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(50, make([]byte, lcommon.Blake2b256Size))
	ctx := blockfetch.CallbackContext{
		ConnectionId: testConnId(),
		// Server is nil, so NoBlocks() will panic after the log.
	}

	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(ctx, start, end)
	}, "expected panic from nil Server.NoBlocks()")

	// Verify the warning was logged before the panic
	logOutput := logBuf.String()
	assert.Contains(
		t,
		logOutput,
		"start after end",
		"expected log message about start after end",
	)
}

func TestBlockfetchServerRequestRange_EqualPoints(t *testing.T) {
	t.Parallel()

	// When start == end (same slot), this is a valid single-block range
	// and should NOT trigger the "start after end" check.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	// LedgerState is nil, so GetChainFromPoint will panic.
	// This verifies the range check does NOT reject equal slots.
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(100, []byte{0x01})

	// We expect a panic from nil LedgerState (not from range validation),
	// which proves that equal slots pass the range check.
	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(
			blockfetch.CallbackContext{
				ConnectionId: testConnId(),
			},
			start,
			end,
		)
	}, "equal slot range should pass validation and reach LedgerState call")
}

// TestBlockfetchServerRequestRange_SparseNetworkSlotSpanNotRejected covers that
// a slot span far larger than mainnet's stability window (129600) must not be
// rejected at the request-validation stage, since a sparse or
// low-active-slot-coefficient custom network can have a valid run of
// consecutive blocks spanning far more slots than that. Only actual block
// count, scaled to the network's security parameter, bounds the response.
// LedgerState is nil, so the call panics either way -- assert.Panics alone
// cannot tell "rejected by a slot-range check" (which panics inside the nil
// ctx.Server.NoBlocks()) apart from "reached GetChainFromPoint" (which panics
// on the nil LedgerState). Every early-rejection branch in
// blockfetchServerRequestRange logs before it calls NoBlocks, so an empty log
// buffer at the point of the panic is what actually proves no rejection branch
// ran; asserting on specific log wording would pass again if a slot-range check
// returned with different wording.
func TestBlockfetchServerRequestRange_SparseNetworkSlotSpanNotRejected(
	t *testing.T,
) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	start := ocommon.NewPoint(0, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(
		50_000_000_000, // far beyond 129600, the old MaxBlockFetchRange
		make([]byte, lcommon.Blake2b256Size),
	)
	ctx := blockfetch.CallbackContext{
		ConnectionId: testConnId(),
	}

	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(ctx, start, end)
	}, "a large slot span must reach the nil LedgerState call (GetChainFromPoint), not be rejected by a slot-range check")

	assert.Empty(
		t,
		logBuf.String(),
		"a large slot span must not log or take any early-rejection branch before reaching GetChainFromPoint",
	)
}

func TestBlockfetchServerSendBatch_ClosesConnectionOnIteratorError(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{err: errors.New("iterator exploded")},
		},
	}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
		testMaxBlocksUnbounded,
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iterator failed")
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 0, server.batchDoneCalls)
	assert.Equal(t, 1, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
}

func TestBlockfetchServerSendBatch_BatchDoneAtChainTip(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
		testMaxBlocksUnbounded,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 1, server.batchDoneCalls)
	assert.Equal(t, 0, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
}

func TestBlockfetchServerSendBatch_RollbackEndsBatchWithoutServingBlock(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	// The chain iterator surfaces a rollback as a sentinel result with
	// Rollback=true and a zero-value Block. Serving that zero block streams a
	// [0, null] block that a fetching peer decodes as a nil-header Byron EBB
	// and crashes dereferencing it in SlotNumber(). The server must end the
	// batch instead of serving the sentinel.
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{result: &chain.ChainIteratorResult{
				Point:    ocommon.NewPoint(150, []byte{0x03}),
				Rollback: true,
			}},
		},
	}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
		testMaxBlocksUnbounded,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	// The rollback sentinel must NOT be streamed as a block.
	assert.Equal(t, 0, server.blockCalls,
		"rollback sentinel must not be streamed as a block")
	// Blockfetch has no rollback message, so end the batch cleanly and let the
	// client re-request against its updated chain.
	assert.Equal(t, 1, server.batchDoneCalls)
	assert.Equal(t, 0, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
}

// sparseBlockfetchSteps builds n consecutive iterator steps starting at
// startSlot, spaced far enough apart that a run of a few thousand steps
// spans more than 129600 slots (the old, now-removed MaxBlockFetchRange).
// This reproduces a sparse or low-active-slot-coefficient custom network
// where real consecutive blocks span far more slots than mainnet's
// stability window.
func sparseBlockfetchSteps(
	n int,
	startSlot uint64,
) []blockfetchIteratorStep {
	const slotGap = 300 // a few thousand steps * slotGap > 129600
	steps := make([]blockfetchIteratorStep, n)
	for i := range n {
		steps[i] = blockfetchIteratorStep{
			result: testBlockfetchIteratorBlock(startSlot + uint64(i)*slotGap),
		}
	}
	return steps
}

func TestBlockfetchServerSendBatch_ServesSparseRangeUpToMaxBlocks(
	t *testing.T,
) {
	t.Parallel()

	// Valid blocks whose endpoint slots differ by more than 129600
	// (the old MaxBlockFetchRange) must be served in full rather than
	// rejected, since resource usage is now bounded by block count. This
	// exercises blockfetchServerSendBatch's own backstop bound directly
	// (blockfetchServerRequestRange's up-front NoBlocks rejection is
	// covered separately in blockfetch_test.go, over a real
	// chain), so maxBlocks is passed explicitly rather than derived from a
	// security parameter.
	const testMaxBlocks = 5000
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	const startSlot = uint64(1000)
	steps := sparseBlockfetchSteps(testMaxBlocks, startSlot)
	iter := &stubBlockfetchIterator{steps: steps}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{errChan: make(chan error)}
	start := ocommon.NewPoint(startSlot, []byte{0x01})
	end := steps[len(steps)-1].result.Point
	require.Greater(
		t,
		end.Slot-start.Slot,
		uint64(129600),
		"test setup must exercise a slot span larger than the old fixed limit",
	)

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
		testMaxBlocks,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(
		t,
		testMaxBlocks,
		server.blockCalls,
		"all valid blocks in a sparse range up to the cap must be served",
	)
	assert.Equal(t, 1, server.batchDoneCalls)
	assert.Equal(
		t,
		0,
		conn.closeCalls,
		"serving exactly maxBlocks must not close the connection",
	)
}

func TestBlockfetchServerSendBatch_ClosesConnectionWhenBlockCountExceedsMax(
	t *testing.T,
) {
	t.Parallel()

	// A range that would serve more than maxBlocks blocks is still bounded:
	// the server must stop and close the connection instead of streaming an
	// unbounded response. This is the backstop's own boundary
	// (blockfetchServerRequestRange rejects this case with a clean NoBlocks
	// before StartBatch when it can; see
	// TestBlockfetchServerRequestRange_OversizedRangeRejectedWithNoBlocks in
	// blockfetch_test.go), so maxBlocks is passed explicitly.
	const testMaxBlocks = 5000
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	const startSlot = uint64(1000)
	steps := sparseBlockfetchSteps(testMaxBlocks+1, startSlot)
	iter := &stubBlockfetchIterator{steps: steps}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{errChan: make(chan error)}
	start := ocommon.NewPoint(startSlot, []byte{0x01})
	end := steps[len(steps)-1].result.Point

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
		testMaxBlocks,
	)

	assert.Error(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(
		t,
		testMaxBlocks,
		server.blockCalls,
		"the block that would exceed the cap must not be served",
	)
	assert.Equal(t, 0, server.batchDoneCalls)
	assert.Equal(t, 1, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
}

// TestMaxBlockFetchBlocksForSecurityParam pins
// maxBlockFetchBlocksForSecurityParam's two behaviors: a small or
// unconfigured security parameter K must not cap below
// blockfetchMaxBlocksFloor (or Dingo-to-Dingo catch-up on a small-K network
// would ride the same connection-closing edge the range fix removed), and a K
// large enough to matter must scale the cap linearly with K rather than staying
// fixed.
func TestMaxBlockFetchBlocksForSecurityParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		k    int
		want int
	}{
		{"negative k floors like zero", -1, blockfetchMaxBlocksFloor},
		{"zero k uses the floor", 0, blockfetchMaxBlocksFloor},
		{"small k stays at the floor", 100, blockfetchMaxBlocksFloor},
		{"mainnet-shaped k scales past the floor", 2160, 3 * 2160},
		{"large custom-network k scales linearly", 100_000, 3 * 100_000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				tc.want,
				maxBlockFetchBlocksForSecurityParam(tc.k),
			)
		})
	}
}

// TestBlockfetchMaxBlocksFloorHasHeadroomOverChainsyncBatchSize pins the
// coupling blockfetchMaxBlocksFloor's doc comment only asserts in prose:
// the floor must stay comfortably above ledger.BlockfetchBatchSize, the
// largest range Dingo's own chainsync client ever requests, or a small- or
// zero-K network starts riding the same connection-closing edge the range
// fix removed. Without this, a future change to either constant could silently
// erode or eliminate that margin.
func TestBlockfetchMaxBlocksFloorHasHeadroomOverChainsyncBatchSize(
	t *testing.T,
) {
	t.Parallel()

	require.GreaterOrEqual(
		t,
		blockfetchMaxBlocksFloor,
		10*ledger.BlockfetchBatchSize,
		"blockfetchMaxBlocksFloor must keep at least an order of magnitude of headroom over ledger.BlockfetchBatchSize",
	)
}

func TestBlockfetchServerSendBatch_WaitsForSendDrainBetweenMessages(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{result: testBlockfetchIteratorBlock(100)},
			{result: testBlockfetchIteratorBlock(101)},
		},
	}
	server := &stubBlockfetchDrainBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(101, []byte{101})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
		testMaxBlocksUnbounded,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 2, server.blockCalls)
	assert.Equal(t, 1, server.batchDoneCalls)
	assert.Equal(t, 0, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
	assert.Equal(t, 3, server.drainCalls)
	for _, timeout := range server.drainTimeouts {
		assert.Equal(t, blockfetchServerSendDrainTimeout, timeout)
	}
}

func TestBlockfetchServerSendBatch_ClosesConnectionWhenSendDrainStalls(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{result: testBlockfetchIteratorBlock(100)},
			{result: testBlockfetchIteratorBlock(101)},
		},
	}
	server := &stubBlockfetchDrainBatchServer{
		drainResults: []bool{true, false},
	}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(101, []byte{101})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
		testMaxBlocksUnbounded,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "send queue did not drain after Block")
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 1, server.blockCalls)
	assert.Equal(t, 0, server.batchDoneCalls)
	assert.Equal(t, 1, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
	assert.Equal(t, 2, server.drainCalls)
}

func TestReportBlockfetchServerAsyncError_ClosesConnection(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	conn := &stubBlockfetchConnection{errChan: make(chan error)}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})
	o.reportBlockfetchServerAsyncError(
		conn,
		testConnId().String(),
		start,
		end,
		errors.New("async blockfetch failure"),
	)

	assert.Equal(t, 1, conn.closeCalls)
}

func TestReportBlockfetchServerAsyncError_ReportsCloseErrorWithoutPanic(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	conn := &stubBlockfetchConnection{
		errChan:  make(chan error),
		closeErr: errors.New("close failed"),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	assert.NotPanics(t, func() {
		o.reportBlockfetchServerAsyncError(
			conn,
			testConnId().String(),
			start,
			end,
			errors.New("closed channel test"),
		)
	})
	assert.Equal(t, 1, conn.closeCalls)
}

// TestBlockfetchRecordNoBlocks_BelowThreshold verifies repeated NoBlocks stay
// below the close threshold until the configured limit is reached.
func TestBlockfetchRecordNoBlocks_BelowThreshold(t *testing.T) {
	t.Parallel()

	// Returns false for each of the first four identical NoBlocks requests.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(
			t,
			o.blockfetchRecordNoBlocks(connId, start),
			"should not trigger before threshold",
		)
	}
}

// TestBlockfetchRecordNoBlocks_ReachesThreshold verifies the stuck-peer
// detector triggers on the configured consecutive NoBlocks threshold.
func TestBlockfetchRecordNoBlocks_ReachesThreshold(t *testing.T) {
	t.Parallel()

	// Returns true on the fifth consecutive NoBlocks for the same start point.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		o.blockfetchRecordNoBlocks(connId, start)
	}
	assert.True(
		t,
		o.blockfetchRecordNoBlocks(connId, start),
		"should trigger on 5th consecutive request",
	)
}

// TestBlockfetchRecordNoBlocks_ProgressResetsCounter verifies valid progress
// clears prior NoBlocks counts for the connection.
func TestBlockfetchRecordNoBlocks_ProgressResetsCounter(t *testing.T) {
	t.Parallel()

	// Valid blockfetch progress clears prior NoBlocks counts for the connection.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(t, o.blockfetchRecordNoBlocks(connId, start))
	}

	o.blockfetchResetNoBlocks(connId)

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(
			t,
			o.blockfetchRecordNoBlocks(connId, start),
			"counter should reset after progress",
		)
	}
	assert.True(
		t,
		o.blockfetchRecordNoBlocks(connId, start),
		"should need another full sequence after progress",
	)
}

// TestBlockfetchRecordNoBlocks_IndependentPoints verifies changing start
// points resets the consecutive NoBlocks count on the same connection.
func TestBlockfetchRecordNoBlocks_IndependentPoints(t *testing.T) {
	t.Parallel()

	// Only consecutive NoBlocks for the same start point should accumulate.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	startA := ocommon.NewPoint(100, []byte{0x01})
	startB := ocommon.NewPoint(200, []byte{0x02})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(t, o.blockfetchRecordNoBlocks(connId, startA))
	}
	assert.False(
		t,
		o.blockfetchRecordNoBlocks(connId, startB),
		"different start point should not inherit count",
	)
	assert.False(
		t,
		o.blockfetchRecordNoBlocks(connId, startA),
		"interleaved start point should reset consecutive count",
	)
}

// TestBlockfetchRecordNoBlocks_IndependentConns verifies NoBlocks counts are
// tracked separately for each connection.
func TestBlockfetchRecordNoBlocks_IndependentConns(t *testing.T) {
	t.Parallel()

	// Tracks counters independently per connection for the same start point.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connA := ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	connB := ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3003},
	}
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		o.blockfetchRecordNoBlocks(connA, start)
	}
	// Different connId at the same point must have its own independent counter
	assert.False(
		t,
		o.blockfetchRecordNoBlocks(connB, start),
		"different connId should not inherit count",
	)
}

// TestBlockfetchRecordNoBlocks_CleanupResetsCounter verifies connection-close
// cleanup clears stuck-peer state before a reconnect starts fresh.
func TestBlockfetchRecordNoBlocks_CleanupResetsCounter(t *testing.T) {
	t.Parallel()

	// Resets the counter after connection close so the peer gets a fresh count on reconnect.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	// Drive counter to threshold
	for range blockfetchMaxConsecutiveNoBlocks {
		o.blockfetchRecordNoBlocks(connId, start)
	}

	// Simulate connection close cleanup
	o.blockFetchMutex.Lock()
	delete(o.blockfetchNoBlocksCounts, connId)
	o.blockFetchMutex.Unlock()

	// Counter should be reset — needs another full sequence to trigger
	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(
			t,
			o.blockfetchRecordNoBlocks(connId, start),
			"counter should reset after cleanup",
		)
	}
	assert.True(
		t,
		o.blockfetchRecordNoBlocks(connId, start),
		"should trigger again after reset",
	)
}

// newBlockfetchServerPeer builds a muxerServerPeer driving Dingo's real
// blockfetch server config (blockfetchServerConnOpts, instrumentation
// wrappers included), so requests reach blockfetchServerRequestRange exactly
// as a real peer's would and NoBlocks() actually goes out on the wire
// instead of panicking on a nil CallbackContext.Server the way the
// direct-call tests above do.
func newBlockfetchServerPeer(t *testing.T, o *Ouroboros) *muxerServerPeer {
	t.Helper()
	opts, peer := newMuxerServerPeer(t)
	cfg, err := blockfetch.NewConfig(o.blockfetchServerConnOpts()...)
	require.NoError(t, err)
	server := blockfetch.NewServer(opts, &cfg)
	peer.start(t, server)
	return peer
}

// TestBlockfetchServerRequestRange_RepeatedInvertedRangeReachesCloseThreshold
// covers that an inverted range (start after end) sent NoBlocks without
// calling blockfetchRecordNoBlocksAndMaybeClose, the same valve oversized and
// missing-point rejections use, so a peer repeating an inverted request never
// counted toward blockfetchMaxConsecutiveNoBlocks and was never closed.
//
// This drives real MsgRequestRange traffic over a real blockfetch.Server/
// muxer pair -- unlike TestBlockfetchServerRequestRange_StartAfterEnd above,
// which only proves the check is reached before its NoBlocks call panics on a
// nil Server -- so the shared valve's close-eligible WARN log fires for real
// once the configured threshold is reached. o.connManager is nil, so this
// only proves the inverted-range branch now feeds the valve and the valve
// reaches its close-eligible state; it does not assert an actual connection
// close, which blockfetchRecordNoBlocksAndMaybeClose only attempts when
// connManager is non-nil. closeBlockfetchConnection's Close() call is already
// covered generically by TestBlockfetchServerSendBatch_ClosesConnectionWhenSendDrainStalls
// and TestReportBlockfetchServerAsyncError_ClosesConnection above.
func TestBlockfetchServerRequestRange_RepeatedInvertedRangeReachesCloseThreshold(
	t *testing.T,
) {
	t.Parallel()

	const closeWarnMsg = "closing stuck peer after repeated inverted range requests"

	logBuf := &syncLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	peer := newBlockfetchServerPeer(t, o)

	start := ocommon.NewPoint(100, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(50, make([]byte, lcommon.Blake2b256Size))

	for i := 1; i <= blockfetchMaxConsecutiveNoBlocks; i++ {
		logBuf.Reset()
		peer.send(
			t,
			blockfetch.ProtocolId,
			blockfetch.NewMsgRequestRange(start, end),
		)

		segment := peer.readResponse(t, 5*time.Second)
		assert.Equal(t, blockfetch.ProtocolId, segment.GetProtocolId())
		assert.Equal(
			t,
			[]byte{0x81, blockfetch.MessageTypeNoBlocks},
			segment.Payload,
			"request %d should be answered with NoBlocks",
			i,
		)

		if i < blockfetchMaxConsecutiveNoBlocks {
			// Below threshold, blockfetchRecordNoBlocksAndMaybeClose logs
			// nothing at all -- the only log line for this request is the
			// "start after end" one written before NoBlocks() was even
			// enqueued, which readResponse above already happened-before.
			assert.False(
				t,
				strings.Contains(logBuf.String(), closeWarnMsg),
				"request %d should not yet reach the close threshold",
				i,
			)
		} else {
			// At threshold, the close-eligible WARN is logged after NoBlocks()
			// is enqueued, on the server's own goroutine, with no ordering
			// guarantee relative to the wire bytes readResponse observed --
			// poll instead of asserting immediately.
			testutil.WaitForCondition(
				t,
				func() bool {
					return strings.Contains(logBuf.String(), closeWarnMsg)
				},
				2*time.Second,
				"expected close-eligible WARN once the threshold is reached",
			)
		}
	}
}

// TestBlockfetchServerRequestRange_ValidRangeNotRejected is the control for
// the fix above: a valid (non-inverted, in-limit) range reaches
// GetChainFromPoint instead of either range-rejection path. LedgerState is nil,
// so the expected panic proves validation completed before that call.
func TestBlockfetchServerRequestRange_ValidRangeNotRejected(
	t *testing.T,
) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	start := ocommon.NewPoint(100, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(150, make([]byte, lcommon.Blake2b256Size))
	ctx := blockfetch.CallbackContext{ConnectionId: testConnId()}

	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(ctx, start, end)
	}, "valid range should reach LedgerState call, not get rejected")

	logOutput := logBuf.String()
	assert.NotContains(t, logOutput, "start after end")
	assert.NotContains(t, logOutput, "inverted range")
}

func BenchmarkBlockfetchClientBlockMetrics(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	o := newOuroboros(OuroborosConfig{
		Logger:       logger,
		EventBus:     eventBus,
		PromRegistry: prometheus.NewRegistry(),
	})

	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}
	iterator, err := immDb.BlocksFromPoint(ocommon.NewPoint(0, nil))
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	const blockCount = 100
	blocks := make([]gledger.Block, 0, blockCount)
	for len(blocks) < blockCount {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			break
		}
		decoded, err := gledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			continue
		}
		blocks = append(blocks, decoded)
	}
	if len(blocks) == 0 {
		b.Skip("no decoded blocks available")
	}

	connId := testConnId()
	ctx := blockfetch.CallbackContext{ConnectionId: connId}
	key := blockFetchKey{connId: connId, requestId: ctx.RequestId}
	o.blockFetchMutex.Lock()
	o.blockFetchStarts[key] = time.Now().Add(-50 * time.Millisecond)
	o.blockFetchMutex.Unlock()

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Reset fetch start each iteration so delaySeconds is
		// consistent across all iterations.
		o.blockFetchMutex.Lock()
		o.blockFetchStarts[key] = time.Now().Add(-50 * time.Millisecond)
		o.blockFetchMutex.Unlock()

		block := blocks[i%len(blocks)]
		if err := o.blockfetchClientBlock(ctx, uint(block.Type()), block); err != nil {
			b.Fatal(err)
		}
	}
}
