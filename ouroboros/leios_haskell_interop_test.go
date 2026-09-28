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
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// The helpers in this file reproduce, byte for byte, how the Haskell Leios
// reference node encodes an endorser-block manifest and a MsgLeiosBlockTxs
// response. They follow ouroboros-consensus commit 1820edf5e (leios-prototype
// branch):
//
//   - LeiosDemoTypes: encodeLeiosEb, hashLeiosEb, hashLeiosTx, encodeLeiosTx,
//     encodeLeiosPoint (HASH = Blake2b_256)
//   - LeiosDemoOnlyTestFetch: encodeLeiosFetch (MsgLeiosBlockTxs),
//     encodeBitmaps, decodeBitmaps
//   - LeiosDemoLogic: msgLeiosBlockTxsRequest, which echoes the request point
//     and bitmaps and rejects zero bitmaps and non-ascending offsets
//
// They are written independently of gouroboros' encoders so that the test
// pins interoperability with the reference node rather than Dingo's agreement
// with itself.

// haskellCborUint is cborg's canonical (minimal-length) unsigned integer
// encoding, as used by encodeWord, encodeWord16, encodeWord32 and
// encodeWord64, for the given CBOR major type.
func haskellCborUint(major byte, v uint64) []byte {
	m := major << 5
	switch {
	case v < 24:
		return []byte{m | byte(v)}
	case v <= 0xff:
		return []byte{m | 24, byte(v)}
	case v <= 0xffff:
		b := []byte{m | 25, 0, 0}
		binary.BigEndian.PutUint16(b[1:], uint16(v))
		return b
	case v <= 0xffffffff:
		b := []byte{m | 26, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(b[1:], uint32(v))
		return b
	default:
		b := []byte{m | 27, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint64(b[1:], v)
		return b
	}
}

// haskellCborBytes is cborg's encodeBytes: a definite-length byte string.
func haskellCborBytes(b []byte) []byte {
	return append(haskellCborUint(2, uint64(len(b))), b...)
}

// haskellEncodeLeiosEb mirrors encodeLeiosEb: a definite-length map from tx
// hash (bytes) to tx byte size (word32), in EB order. Each tx hash is
// hashLeiosTx, Blake2b-256 over the complete serialized transaction (not the
// transaction body / tx id).
func haskellEncodeLeiosEb(txs []cbor.RawMessage) []byte {
	out := haskellCborUint(5, uint64(len(txs)))
	for _, tx := range txs {
		h := lcommon.Blake2b256Hash(tx)
		out = append(out, haskellCborBytes(h.Bytes())...)
		out = append(out, haskellCborUint(0, uint64(len(tx)))...)
	}
	return out
}

// haskellEncodeBlockTxs mirrors encodeLeiosFetch for
// MsgLeiosBlockTxs p bitmaps txs as served by msgLeiosBlockTxsRequest:
// listLen 4, word 3, encodeLeiosPoint (listLen 2, slot, hash bytes),
// encodeBitmaps (an indefinite-length map of word16 offset -> word64 bitmap,
// in request order), then listLen n of encodeLeiosTx (cbor-in-cbor: the
// stored tx bytes wrapped in a byte string). Transactions are emitted in
// bitmap order, where the most significant bit of each 64-bit bitmap is the
// first transaction of that window.
func haskellEncodeBlockTxs(
	point ocommon.Point,
	bitmaps [][2]uint64,
	txs []cbor.RawMessage,
) ([]byte, error) {
	out := []byte{0x84}
	out = append(out, haskellCborUint(0, 3)...)
	out = append(out, 0x82)
	out = append(out, haskellCborUint(0, point.Slot)...)
	out = append(out, haskellCborBytes(point.Hash)...)
	out = append(out, 0xbf)
	var served []cbor.RawMessage
	for _, bm := range bitmaps {
		out = append(out, haskellCborUint(0, bm[0])...)
		out = append(out, haskellCborUint(0, bm[1])...)
		for i := range 64 {
			if bm[1]&(uint64(1)<<(63-i)) == 0 {
				continue
			}
			idx := bm[0]*64 + uint64(i)
			if idx >= uint64(len(txs)) {
				return nil, fmt.Errorf(
					"bitmap offset %d requests tx %d of %d",
					bm[0], idx, len(txs),
				)
			}
			served = append(served, txs[idx])
		}
	}
	out = append(out, 0xff)
	out = append(out, haskellCborUint(4, uint64(len(served)))...)
	for _, tx := range served {
		out = append(out, haskellCborBytes(tx)...)
	}
	return out, nil
}

// haskellDecodeRequestBitmaps decodes the bitmaps of a Dingo
// MsgLeiosBlockTxsRequest the way the reference node does: decodeBitmaps
// requires an indefinite-length map, and msgLeiosBlockTxsRequest rejects a zero
// bitmap and offsets that are not strictly ascending. It returns the
// (offset, bitmap) windows in wire order.
func haskellDecodeRequestBitmaps(req protocol.Message) ([][2]uint64, error) {
	raw, err := cbor.Encode(req)
	if err != nil {
		return nil, err
	}
	var elems []cbor.RawMessage
	if _, err := cbor.Decode(raw, &elems); err != nil {
		return nil, err
	}
	if len(elems) != 3 {
		return nil, fmt.Errorf("request has %d elements, want 3", len(elems))
	}
	bm := []byte(elems[2])
	if len(bm) < 2 || bm[0] != 0xbf || bm[len(bm)-1] != 0xff {
		return nil, errors.New("request bitmaps are not an indefinite-length map")
	}
	body := bm[1 : len(bm)-1]
	var out [][2]uint64
	for len(body) > 0 {
		var offset, bitmap uint64
		n, err := cbor.Decode(body, &offset)
		if err != nil {
			return nil, err
		}
		body = body[n:]
		n, err = cbor.Decode(body, &bitmap)
		if err != nil {
			return nil, err
		}
		body = body[n:]
		if offset > 0xffff {
			return nil, fmt.Errorf("offset %d is not a word16", offset)
		}
		if bitmap == 0 {
			return nil, errors.New("a bitmap is zero")
		}
		if len(out) > 0 && offset <= out[len(out)-1][0] {
			return nil, errors.New("offsets not strictly ascending")
		}
		out = append(out, [2]uint64{offset, bitmap})
	}
	return out, nil
}

// haskellBlockTxsPeer answers BlockTxsRequest the way the Haskell reference
// node does, returning the response through gouroboros' wire decoder.
type haskellBlockTxsPeer struct {
	txs      []cbor.RawMessage
	requests int
}

func (p *haskellBlockTxsPeer) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	p.requests++
	windows, err := haskellDecodeRequestBitmaps(
		leiosfetch.NewMsgBlockTxsRequest(point, bitmaps),
	)
	if err != nil {
		return nil, fmt.Errorf("reference node rejects request: %w", err)
	}
	raw, err := haskellEncodeBlockTxs(point, windows, p.txs)
	if err != nil {
		return nil, err
	}
	return leiosfetch.NewMsgFromCbor(leiosfetch.MessageTypeBlockTxs, raw)
}

// TestLeiosFetchHaskellEncodedBlockTxsIsConsumed is an interoperability
// regression for the Haskell reference node's leios-fetch encoding (#3623).
// The endorser block has 70 transactions, so the fetch spans two 64-tx bitmap
// windows. Its manifest, point hash and MsgLeiosBlockTxs response are encoded
// the way the reference node encodes them. Dingo must send a request the
// reference node accepts, decode the response, and bind every returned
// transaction to the manifest. Manifest references hash the full transaction
// CBOR, so validating by transaction-body hash rejects every tx
// ("endorser tx 0 hash mismatch").
func TestLeiosFetchHaskellEncodedBlockTxsIsConsumed(t *testing.T) {
	t.Parallel()

	const txCount = 70
	txs := make([]cbor.RawMessage, txCount)
	for i := range txCount {
		txs[i] = testDijkstraTx(t, byte(i))
	}
	manifestRaw := haskellEncodeLeiosEb(txs)
	// hashLeiosEb: the EB hash is Blake2b-256 of the encoded manifest.
	point := ocommon.NewPoint(
		3623,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)

	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, txCount)

	o := &Ouroboros{}
	peer := &haskellBlockTxsPeer{txs: txs}
	got, err := o.fetchLeiosEbTxsBatched(peer, point, txCount, manifestRaw)
	require.NoError(t, err)
	require.Len(t, got, txCount)
	require.NotZero(t, peer.requests)
	require.NoError(t, validateLeiosEndorserBlockTxs(manifestRaw, got))
}
