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
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	gconnection "github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
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
	clientConn, peerConn := net.Pipe()
	m := muxer.New(clientConn)
	errChan := make(chan error, 4)
	cfg := blockfetchConfig(o.blockfetchClientConnOpts()...)
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

func (p *musashiBlockfetchPeer) send(t *testing.T, msg protocol.Message) {
	t.Helper()
	data, err := cbor.Encode(msg)
	require.NoError(t, err)
	segment := muxer.NewSegment(oblockfetch.ProtocolId, data, true)
	require.NotNil(t, segment)
	buf := &bytes.Buffer{}
	require.NoError(
		t,
		binary.Write(buf, binary.BigEndian, segment.SegmentHeader),
	)
	_, err = buf.Write(segment.Payload)
	require.NoError(t, err)
	require.NoError(
		t,
		p.peerConn.SetWriteDeadline(time.Now().Add(5*time.Second)),
	)
	_, err = p.peerConn.Write(buf.Bytes())
	require.NoError(t, err)
}

// readRequestRange consumes the MsgRequestRange the client sends, so the
// server half only replies to a request the client actually made.
func (p *musashiBlockfetchPeer) readRequestRange(t *testing.T) {
	t.Helper()
	require.NoError(
		t,
		p.peerConn.SetReadDeadline(time.Now().Add(5*time.Second)),
	)
	header := muxer.SegmentHeader{}
	require.NoError(t, binary.Read(p.peerConn, binary.BigEndian, &header))
	payload := make([]byte, header.PayloadLength)
	_, err := io.ReadFull(p.peerConn, payload)
	require.NoError(t, err)
	msg, err := oblockfetch.NewMsgFromCbor(
		oblockfetch.MessageTypeRequestRange,
		payload,
	)
	require.NoError(t, err)
	require.IsType(t, &oblockfetch.MsgRequestRange{}, msg)
}

// TestBlockfetchClientDeliversMusashiType7Block is the regression for #3798.
//
// A Musashi block below the type-8 transition arrives tagged as block type 7
// in a five-component Conway layout with a twelve-field Leios header body.
// gouroboros' block-fetch client decodes every block with
// ledger.NewBlockFromCbor before delivering it, so the strict Conway decoder
// rejected these bytes and failed the request -- tearing down the connection
// before Dingo's WithBlockRawFunc callback, and therefore
// decodeBlockfetchBlock's Musashi fallback, ever ran. Every from-genesis sync
// stalled at origin with no selectable peer.
//
// Direct decoder tests could not catch this: models.DecodeConwayBlock decodes
// these bytes correctly and always did. The failure was in the dispatch that
// never reached it, so this test drives the real client.
func TestBlockfetchClientDeliversMusashiType7Block(t *testing.T) {
	blockRaw := readHexFixture(t, musashiType7BlockFixture)
	headerRaw := readHexFixture(t, musashiType7HeaderFixture)

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
	header, err := o.decodeChainsyncHeader(gledger.BlockTypeConway, headerRaw)
	require.NoError(t, err)
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	wrapped, err := cbor.Encode(oblockfetch.WrappedBlock{
		Type:     gledger.BlockTypeConway,
		RawBlock: cbor.RawMessage(blockRaw),
	})
	require.NoError(t, err)

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		peer.readRequestRange(t)
		peer.send(t, oblockfetch.NewMsgStartBatch())
		peer.send(t, oblockfetch.NewMsgBlock(wrapped))
		peer.send(t, oblockfetch.NewMsgBatchDone())
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
		require.Equal(t, header.Hash().String(), bfEvt.Block.Hash().String())
		require.Equal(t, header.SlotNumber(), bfEvt.Block.SlotNumber())
		require.Equal(t, blockRaw, bfEvt.Block.Cbor())
	case err := <-peer.errChan:
		t.Fatalf("block-fetch client failed the request: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the block-fetch block event")
	}
	<-serverDone
}
