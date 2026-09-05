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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	gconnection "github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// musashiNetworkMagic is the network magic of the Musashi prototype network.
const musashiNetworkMagic = 164

// Fixtures captured from leios-node.play.dev.cardano.org:3001 (magic 164) on
// 2026-09-02. Musashi changes both its chain-sync header wire type and its
// block-fetch block wire type from 7 to 8 at block 4329 (slot 86407), and the
// CBOR layout changes with the tag:
//
//	blocks 0..4328    wire type 7  block: 5 components (Conway layout)
//	                               header: 2 elements, 12-field header body
//	blocks 4329..tip  wire type 8  block: 2 components (Dijkstra layout)
//	                               header: 2 elements, 12-field header body
//
// Both header forms are Dijkstra headers; only the block layout differs. The
// type-7 block is the one gouroboros' strict Conway decoder rejects, which is
// what stops a from-genesis sync at origin (#3798, #3761).
//
// The type-8 pair is the transition block itself (4329), captured with its own
// header so the header and block dispatch paths can be asserted against the
// same block. database/models/testdata/musashi_dijkstra_block.hex is the same
// block *layout* but a different block (28091) and has no paired header, so it
// cannot serve this test.
const (
	musashiType7BlockFixture  = "testdata/musashi_type7_leios_conway_block.hex"
	musashiType7HeaderFixture = "testdata/musashi_type7_leios_header.hex"
	musashiType8BlockFixture  = "testdata/musashi_type8_dijkstra_block.hex"
	musashiType8HeaderFixture = "testdata/musashi_type8_dijkstra_header.hex"
)

func readHexFixture(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	raw, err := hex.DecodeString(
		strings.TrimSpace(strings.ReplaceAll(string(data), "\n", "")),
	)
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	return raw
}

func newMusashiOuroboros(t *testing.T, eventBus *event.EventBus) *Ouroboros {
	t.Helper()
	return newOuroboros(OuroborosConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		NetworkMagic: musashiNetworkMagic,
		EventBus:     eventBus,
	})
}

// TestDecodeBlockfetchBlockMusashiWireTypes drives the production block-fetch
// decode dispatch with the real bytes for both Musashi block wire types, and
// asserts each decodes to the hash chain-sync computed for the same block.
func TestDecodeBlockfetchBlockMusashiWireTypes(t *testing.T) {
	o := newMusashiOuroboros(t, nil)
	for _, tc := range []struct {
		name       string
		blockType  uint
		blockPath  string
		headerPath string
	}{
		{
			name:       "type7_conway_layout",
			blockType:  gledger.BlockTypeConway,
			blockPath:  musashiType7BlockFixture,
			headerPath: musashiType7HeaderFixture,
		},
		{
			name:       "type8_dijkstra_layout",
			blockType:  gledger.BlockTypeDijkstra,
			blockPath:  musashiType8BlockFixture,
			headerPath: musashiType8HeaderFixture,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockRaw := readHexFixture(t, tc.blockPath)
			headerRaw := readHexFixture(t, tc.headerPath)

			block, err := o.decodeBlockfetchBlock(tc.blockType, blockRaw)
			require.NoError(t, err)
			require.NotNil(t, block)

			// The chain-sync header path must reach the same block identity
			// for the same wire type. #3761 was exactly the two paths
			// disagreeing; assert agreement rather than assuming it.
			header, err := o.decodeChainsyncHeader(tc.blockType, headerRaw)
			require.NoError(t, err)
			require.NotNil(t, header)

			require.Equal(
				t,
				header.Hash().String(),
				block.Hash().String(),
				"chain-sync header and block-fetch block must agree on hash",
			)
			require.Equal(t, header.SlotNumber(), block.SlotNumber())
			// The decoded block must keep its verbatim wire bytes so it is
			// stored and re-served unchanged.
			require.Equal(t, blockRaw, block.Cbor())
		})
	}
}

// TestDecodeBlockfetchBlockRejectsMalformed proves the Musashi dispatch still
// fails on input it cannot read, rather than silently accepting it.
func TestDecodeBlockfetchBlockRejectsMalformed(t *testing.T) {
	o := newMusashiOuroboros(t, nil)
	valid := readHexFixture(t, musashiType7BlockFixture)
	for _, tc := range []struct {
		name      string
		blockType uint
		raw       []byte
	}{
		{"truncated_type7", gledger.BlockTypeConway, valid[:len(valid)/2]},
		{"truncated_type8", gledger.BlockTypeDijkstra, valid[:len(valid)/2]},
		{"not_a_block_type7", gledger.BlockTypeConway, []byte{0x01, 0x02}},
		{"not_a_block_type8", gledger.BlockTypeDijkstra, []byte{0x01, 0x02}},
		{"empty_type7", gledger.BlockTypeConway, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, err := o.decodeBlockfetchBlock(tc.blockType, tc.raw)
			require.Error(t, err)
			require.Nil(t, block)
		})
	}
}

// TestDecodeBlockfetchBlockLeavesOtherNetworksStrict proves the Musashi
// fallback is network-scoped: the same Leios-extended bytes must be refused on
// a real Conway network, where accepting them would weaken the decoder every
// mainnet block relies on.
func TestDecodeBlockfetchBlockLeavesOtherNetworksStrict(t *testing.T) {
	o := newOuroboros(OuroborosConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		NetworkMagic: 764824073, // mainnet
	})
	block, err := o.decodeBlockfetchBlock(
		gledger.BlockTypeConway,
		readHexFixture(t, musashiType7BlockFixture),
	)
	require.Error(t, err)
	require.Nil(t, block)
}

// musashiBlockfetchPeer drives Dingo's real block-fetch client config
// (blockfetchClientConnOpts, instrumentation wrappers included) over a real
// muxer, and speaks the server half of the protocol by hand. The assertions
// are therefore about what the production dispatch does with bytes off the
// wire, not about what a decoder returns when called directly.
type musashiBlockfetchPeer struct {
	client   *oblockfetch.Client
	peerConn net.Conn
	errChan  chan error
}

func newMusashiBlockfetchPeer(
	t *testing.T,
	o *Ouroboros,
) *musashiBlockfetchPeer {
	t.Helper()
	return newBlockfetchPeerWithOpts(t, o.blockfetchClientConnOpts()...)
}

// newBlockfetchPeerWithOpts is newMusashiBlockfetchPeer with the client
// configuration supplied directly, so the capability probe can drive the same
// real client with a bare raw callback instead of Dingo's dispatch.
func newBlockfetchPeerWithOpts(
	t *testing.T,
	opts ...oblockfetch.BlockFetchOptionFunc,
) *musashiBlockfetchPeer {
	t.Helper()
	clientConn, peerConn := net.Pipe()
	m := muxer.New(clientConn)
	errChan := make(chan error, 4)
	cfg := blockfetchConfig(opts...)
	client := oblockfetch.NewClient(
		protocol.ProtocolOptions{
			ConnectionId: gconnection.ConnectionId{
				LocalAddr:  clientConn.LocalAddr(),
				RemoteAddr: clientConn.RemoteAddr(),
			},
			ErrorChan: errChan,
			Muxer:     m,
			Mode:      protocol.ProtocolModeNodeToNode,
			Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
		&cfg,
	)
	client.Start()
	m.Start()
	t.Cleanup(func() {
		_ = client.Stop()
		m.Stop()
		_ = clientConn.Close()
		_ = peerConn.Close()
	})
	return &musashiBlockfetchPeer{
		client:   client,
		peerConn: peerConn,
		errChan:  errChan,
	}
}

// sendErr transmits msg as the server half of the protocol, returning an
// error rather than asserting. Every caller runs on its own goroutine, where
// the connection may be torn down underneath it, so an I/O error is an
// observation and not a test failure -- and require.NoError there would call
// FailNow off the test goroutine, which testing forbids. There is deliberately
// no assertion-based variant of this helper for a caller to reach for.
func (p *musashiBlockfetchPeer) sendErr(msg protocol.Message) error {
	data, err := cbor.Encode(msg)
	if err != nil {
		return err
	}
	segment := muxer.NewSegment(oblockfetch.ProtocolId, data, true)
	if segment == nil {
		return errors.New("nil muxer segment")
	}
	buf := &bytes.Buffer{}
	if err := binary.Write(
		buf,
		binary.BigEndian,
		segment.SegmentHeader,
	); err != nil {
		return err
	}
	if _, err := buf.Write(segment.Payload); err != nil {
		return err
	}
	if err := p.peerConn.SetWriteDeadline(
		time.Now().Add(5 * time.Second),
	); err != nil {
		return err
	}
	_, err = p.peerConn.Write(buf.Bytes())
	return err
}

// readRequestRangeErr consumes the MsgRequestRange the client sends, so the
// server half only replies to a request the client actually made. Returns an
// error rather than asserting; see sendErr.
func (p *musashiBlockfetchPeer) readRequestRangeErr() error {
	if err := p.peerConn.SetReadDeadline(
		time.Now().Add(5 * time.Second),
	); err != nil {
		return err
	}
	header := muxer.SegmentHeader{}
	if err := binary.Read(
		p.peerConn,
		binary.BigEndian,
		&header,
	); err != nil {
		return err
	}
	payload := make([]byte, header.PayloadLength)
	if _, err := io.ReadFull(p.peerConn, payload); err != nil {
		return err
	}
	_, err := oblockfetch.NewMsgFromCbor(
		oblockfetch.MessageTypeRequestRange,
		payload,
	)
	return err
}

// rawDeliveryOfUnrepresentableBlockSupported reports whether the linked
// gouroboros delivers a block its typed decoder cannot represent to
// BlockRawFunc rather than failing the request (gouroboros #2186).
//
// This is a behavioral probe, not a version comparison: it drives the real
// block-fetch client with a bare recording raw callback -- not Dingo's
// dispatch -- and reports whether the bytes arrive. A backport, a fork, or a
// later refactor that keeps the behavior therefore reads as supported, and one
// that loses it reads as unsupported, which a version string cannot do.
//
// The probe asks only "does gouroboros hand undecodable bytes to the raw
// callback"; the tests that consult it assert what Dingo then decodes and
// publishes, so the probe does not stand in for their assertions.
func rawDeliveryOfUnrepresentableBlockSupported(t *testing.T) bool {
	t.Helper()
	blockRaw := readHexFixture(t, musashiType7BlockFixture)
	headerRaw := readHexFixture(t, musashiType7HeaderFixture)
	// Assert the premise the probe rests on: these bytes are exactly the case
	// the raw callback exists for -- a payload the typed decoder for the wire
	// tag cannot represent at all.
	_, strictErr := gledger.NewBlockFromCbor(
		gledger.BlockTypeConway,
		blockRaw,
	)
	require.Error(
		t,
		strictErr,
		"probe premise: the type-7 fixture must fail the strict Conway decode",
	)

	header, err := gledger.NewBlockHeaderFromCbor(
		gledger.BlockTypeDijkstra,
		headerRaw,
	)
	require.NoError(t, err)
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	delivered := make(chan []byte, 1)
	peer := newBlockfetchPeerWithOpts(
		t,
		oblockfetch.WithBlockRawFunc(
			func(
				_ oblockfetch.CallbackContext,
				_ uint,
				raw []byte,
			) error {
				select {
				case delivered <- raw:
				default:
				}
				return nil
			},
		),
		oblockfetch.WithBatchDoneFunc(
			func(_ oblockfetch.CallbackContext) error { return nil },
		),
	)

	wrapped, err := cbor.Encode(oblockfetch.WrappedBlock{
		Type:     gledger.BlockTypeConway,
		RawBlock: cbor.RawMessage(blockRaw),
	})
	require.NoError(t, err)

	// The server half runs on its own goroutine because net.Pipe is
	// synchronous, and it never touches t: when the capability is absent the
	// client fails the request and closes the pipe under it, so its writes
	// are expected to fail. serverDone is awaited before returning so the
	// goroutine cannot outlive the probe.
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		if err := peer.readRequestRangeErr(); err != nil {
			return
		}
		for _, msg := range []protocol.Message{
			oblockfetch.NewMsgStartBatch(),
			oblockfetch.NewMsgBlock(wrapped),
			oblockfetch.NewMsgBatchDone(),
		} {
			if err := peer.sendErr(msg); err != nil {
				return
			}
		}
	}()
	defer func() { <-serverDone }()
	if err := peer.client.GetBlockRange(point, point); err != nil {
		return false
	}
	select {
	case raw := <-delivered:
		return bytes.Equal(raw, blockRaw)
	case <-peer.errChan:
		return false
	case <-time.After(10 * time.Second):
		return false
	}
}

// requireRawDeliverySupport skips when the linked gouroboros still fails the
// request before BlockRawFunc. The fix is in gouroboros v0.202.6, after the
// v0.202.5 this module pins, so the skip is a real state of the module graph
// rather than a test defect: verified skipping on v0.202.5 and passing on
// v0.202.6. Dingo's own dispatch stays covered without
// it: TestDecodeBlockfetchBlockMusashiWireTypes asserts the type-7 and type-8
// decode unconditionally.
func requireRawDeliverySupport(t *testing.T) {
	t.Helper()
	if rawDeliveryOfUnrepresentableBlockSupported(t) {
		return
	}
	t.Skip(
		"linked gouroboros fails the block-fetch request before BlockRawFunc; " +
			"needs a release carrying gouroboros #2186",
	)
}

// runMusashiBlockfetchClientDelivery drives Dingo's real block-fetch client
// config over a real muxer with one Musashi block, and returns the block the
// production dispatch published on the event bus.
func runMusashiBlockfetchClientDelivery(
	t *testing.T,
	blockType uint,
	blockPath string,
	headerPath string,
) (gledger.Block, gledger.BlockHeader, []byte) {
	t.Helper()
	blockRaw := readHexFixture(t, blockPath)
	headerRaw := readHexFixture(t, headerPath)

	eventBus := event.NewEventBus(nil, nil)
	blockKey, blockCh := eventBus.Subscribe(ledger.BlockfetchEventType)
	t.Cleanup(func() {
		eventBus.Unsubscribe(ledger.BlockfetchEventType, blockKey)
	})

	o := newMusashiOuroboros(t, eventBus)
	peer := newMusashiBlockfetchPeer(t, o)

	// The point the client asks for is the one chain-sync derived from the
	// paired header, so range correlation in the client is exercised with
	// real values rather than with the block's own decode output.
	header, err := o.decodeChainsyncHeader(blockType, headerRaw)
	require.NoError(t, err)
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	wrapped, err := cbor.Encode(oblockfetch.WrappedBlock{
		Type:     blockType,
		RawBlock: cbor.RawMessage(blockRaw),
	})
	require.NoError(t, err)

	// The server half never touches t. testing requires FailNow to run on
	// the test goroutine, and every exit below other than the happy path
	// ends the test while a write may still be pending here: the client
	// failing the request closes the pipe under this goroutine, so its I/O
	// is expected to fail. Report through a channel and log it instead.
	serverDone := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		defer close(serverDone)
		if err := peer.readRequestRangeErr(); err != nil {
			serverErr <- err
			return
		}
		for _, msg := range []protocol.Message{
			oblockfetch.NewMsgStartBatch(),
			oblockfetch.NewMsgBlock(wrapped),
			oblockfetch.NewMsgBatchDone(),
		} {
			if err := peer.sendErr(msg); err != nil {
				serverErr <- err
				return
			}
		}
	}()
	// Drained on every path, t.Fatal's Goexit included, so the goroutine
	// cannot outlive the test. Both helpers set deadlines, so this cannot
	// block indefinitely.
	defer func() {
		<-serverDone
		select {
		case err := <-serverErr:
			t.Logf("block-fetch test peer stopped: %v", err)
		default:
		}
	}()

	require.NoError(t, peer.client.GetBlockRange(point, point))

	select {
	case evt := <-blockCh:
		bfEvt, ok := evt.Data.(ledger.BlockfetchEvent)
		require.True(t, ok)
		require.NotNil(
			t,
			bfEvt.Block,
			"block-fetch delivered a batch-done before the block",
		)
		return bfEvt.Block, header, blockRaw
	case err := <-peer.errChan:
		t.Fatalf("block-fetch client failed the request: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the block-fetch block event")
	}
	return nil, nil, nil
}

// TestBlockfetchClientDeliversMusashiType7Block is the regression for the
// symptom reported in #3798.
//
// A Musashi block below the type-8 transition arrives tagged as block type 7
// in a five-component Conway layout with a twelve-field Leios header body.
// gouroboros' block-fetch client decoded every block with
// ledger.NewBlockFromCbor before delivering it, so the strict Conway decoder
// rejected these bytes and failed the request -- tearing down the connection
// before Dingo's WithBlockRawFunc callback, and therefore
// decodeBlockfetchBlock's Musashi fallback, ever ran. Every from-genesis sync
// stalled at origin with no selectable peer, with the error #3798 quotes.
//
// Direct decoder tests could not catch this: models.DecodeConwayBlock decodes
// these bytes correctly and always did. The failure was in the dispatch that
// never reached it, so this test drives the real client.
func TestBlockfetchClientDeliversMusashiType7Block(t *testing.T) {
	requireRawDeliverySupport(t)
	block, header, blockRaw := runMusashiBlockfetchClientDelivery(
		t,
		gledger.BlockTypeConway,
		musashiType7BlockFixture,
		musashiType7HeaderFixture,
	)
	require.Equal(t, header.Hash().String(), block.Hash().String())
	require.Equal(t, header.SlotNumber(), block.SlotNumber())
	require.Equal(t, blockRaw, block.Cbor())
}

// TestBlockfetchClientDeliversMusashiType8Block covers the type-8 input #3798
// names, through the same production dispatch.
//
// It needs no raw-delivery support and is not skipped, which is the point:
// gouroboros dispatches block type 8 to its Dijkstra decoder, that decoder
// accepts the Musashi twelve-field Leios header body, so the typed decode
// succeeds and BlockRawFunc is reached even on a gouroboros without #2186.
// Type 8 was never gated out of the Musashi fallback in a way that mattered --
// the strict decoder for type 8 is the Dijkstra decoder. The error #3798
// quotes names conway.tmpConwayBlock, which only the type-7 route can produce.
func TestBlockfetchClientDeliversMusashiType8Block(t *testing.T) {
	block, header, blockRaw := runMusashiBlockfetchClientDelivery(
		t,
		gledger.BlockTypeDijkstra,
		musashiType8BlockFixture,
		musashiType8HeaderFixture,
	)
	require.Equal(t, header.Hash().String(), block.Hash().String())
	require.Equal(t, header.SlotNumber(), block.SlotNumber())
	require.Equal(t, blockRaw, block.Cbor())
}

// TestDecodeBlockfetchBlockKeepsGenuineConwayBlocks is the negative case for
// the Musashi type-7 route: a standard ten-field-header Conway block must
// still decode, as Conway, both on Musashi and on a real Conway network. The
// Musashi fallback only runs after the strict decode fails, so a genuine
// Conway block must never reach it.
func TestDecodeBlockfetchBlockKeepsGenuineConwayBlocks(t *testing.T) {
	blockRaw := testutil.BuildDecodableConwayBlockBytes(t, 42, 7)
	for _, tc := range []struct {
		name  string
		magic uint32
	}{
		{"musashi", musashiNetworkMagic},
		{"mainnet", 764824073},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newOuroboros(OuroborosConfig{
				Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
				NetworkMagic: tc.magic,
			})
			block, err := o.decodeBlockfetchBlock(
				gledger.BlockTypeConway,
				blockRaw,
			)
			require.NoError(t, err)
			require.NotNil(t, block)
			require.EqualValues(t, conway.EraIdConway, block.Era().Id)
			require.Equal(t, uint64(42), block.SlotNumber())
			require.Equal(t, blockRaw, block.Cbor())
		})
	}
}

// TestDecodeBlockfetchBlockType8NeedsNoMusashiScope pins the fact #3798 got
// wrong: block type 8 decodes through gouroboros' own dispatch, so it needs no
// network-scoped fallback and behaves identically on Musashi and on a network
// that has never seen a Musashi block. Widening the Musashi gate to type 8, or
// widening models.hasDijkstraLeiosShape, would change nothing here except to
// loosen a decoder for no observed input.
func TestDecodeBlockfetchBlockType8NeedsNoMusashiScope(t *testing.T) {
	blockRaw := readHexFixture(t, musashiType8BlockFixture)
	var hashes []string
	for _, magic := range []uint32{musashiNetworkMagic, 764824073} {
		o := newOuroboros(OuroborosConfig{
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
			NetworkMagic: magic,
		})
		block, err := o.decodeBlockfetchBlock(
			gledger.BlockTypeDijkstra,
			blockRaw,
		)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.EqualValues(t, dijkstra.EraIdDijkstra, block.Era().Id)
		hashes = append(hashes, block.Hash().String())
	}
	require.Equal(t, hashes[0], hashes[1])
}

// TestMusashiDispatchEraAgreement records the era each production dispatch
// path assigns to the same Musashi block, for both wire types. #3761 was the
// header and block paths disagreeing, so the mapping is asserted rather than
// assumed.
//
// The hashes agree for both types. The eras do not for type 7: its
// five-component Conway envelope is only representable as a Conway block, so
// models.DecodeConwayBlock returns Conway era, while the header decodes
// through the Dijkstra header decoder and reports Dijkstra. The block declares
// protocol version 12 (Dijkstra), so the header is the accurate one. This is a
// gouroboros representability limit, not a dispatch choice Dingo can make
// differently, and it is pinned here so a change to either path is visible.
//
// The disagreement is latent rather than a live defect, and is tracked in
// #3828: the ledger does not gate on a block-derived era. ls.currentEra comes
// from protocol-version pparams and is passed into ledgerProcessBlock, so on
// Musashi at protocol version 12 those gates see Dijkstra whatever the block
// decodes as, and Leios endorser-block application and the Dijkstra
// transaction-validation bypasses are unaffected. The trap is a future gate
// keying on the block's own era, which would disagree with them.
func TestMusashiDispatchEraAgreement(t *testing.T) {
	o := newMusashiOuroboros(t, nil)
	for _, tc := range []struct {
		name         string
		blockType    uint
		blockPath    string
		headerPath   string
		wantBlockEra uint
	}{
		{
			name:         "type7_conway_layout",
			blockType:    gledger.BlockTypeConway,
			blockPath:    musashiType7BlockFixture,
			headerPath:   musashiType7HeaderFixture,
			wantBlockEra: conway.EraIdConway,
		},
		{
			name:         "type8_dijkstra_layout",
			blockType:    gledger.BlockTypeDijkstra,
			blockPath:    musashiType8BlockFixture,
			headerPath:   musashiType8HeaderFixture,
			wantBlockEra: dijkstra.EraIdDijkstra,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block, err := o.decodeBlockfetchBlock(
				tc.blockType,
				readHexFixture(t, tc.blockPath),
			)
			require.NoError(t, err)
			header, err := o.decodeChainsyncHeader(
				tc.blockType,
				readHexFixture(t, tc.headerPath),
			)
			require.NoError(t, err)
			// Both Musashi wire types carry the same twelve-field Leios
			// header body, so the header path reports Dijkstra for both.
			require.EqualValues(t, dijkstra.EraIdDijkstra, header.Era().Id)
			require.EqualValues(t, tc.wantBlockEra, block.Era().Id)
			require.Equal(t, header.Hash().String(), block.Hash().String())
		})
	}
}
