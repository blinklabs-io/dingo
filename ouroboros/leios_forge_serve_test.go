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
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// TestServeForgedEndorserBlockTxs verifies that a locally-forged endorser
// block's transaction bodies are stored alongside its manifest so the
// leios-fetch server can serve them, in manifest order, honoring the requested
// bitmap. Each served transaction is the on-the-wire byte-string wrap of the
// forged body, so it decodes back to the original body on the requesting side.
func TestServeForgedEndorserBlockTxs(t *testing.T) {
	o := &Ouroboros{leiosEBLog: newLeiosForgedEBLog()}

	// Three forged transaction bodies (arbitrary raw CBOR is fine; they are
	// only stored and served, not decoded as transactions here).
	bodies := [][]byte{
		{0x83, 0x01, 0x02, 0x03},
		{0x84, 0x0a, 0x0b, 0x0c, 0x0d},
		{0x82, 0xf6},
	}
	// Build the manifest in the same order as the bodies.
	refs := make([]lcommon.LeiosTransactionReference, len(bodies))
	for i, b := range bodies {
		var h [32]byte
		h[0] = byte(i + 1)
		refs[i] = lcommon.LeiosTransactionReference{
			TransactionHash: lcommon.NewBlake2b256(h[:]),
			TransactionSize: uint16(len(b)), //nolint:gosec // small test bodies
		}
	}
	ebCbor, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: refs,
	}.MarshalCBOR()
	require.NoError(t, err)
	ebHash := lcommon.Blake2b256Hash(ebCbor).Bytes()
	const slot = uint64(42)

	// Forge + broadcast: this stores the EB with its transaction bodies.
	require.NoError(t, o.BroadcastEndorserBlock(slot, ebHash, ebCbor, bodies))
	point := ocommon.Point{Slot: slot, Hash: ebHash}

	// Full request: all three transactions. MSB-first, so offsets 0..2 are the
	// top three bits 63,62,61 (see leiosWindowNeededMask).
	resp, err := o.leiosfetchServerBlockTxsRequest(
		oleiosfetch.CallbackContext{},
		point,
		map[uint16]uint64{0: uint64(0b111) << 61},
	)
	require.NoError(t, err)
	msg, ok := resp.(*oleiosfetch.MsgBlockTxs)
	require.True(t, ok)
	require.Len(t, msg.TxsRaw, len(bodies))
	for i, raw := range msg.TxsRaw {
		var inner []byte
		_, derr := cbor.Decode(raw, &inner)
		require.NoErrorf(t, derr, "served tx %d should be a CBOR byte string", i)
		require.Equalf(
			t,
			bodies[i],
			inner,
			"served tx %d should round-trip to the forged body",
			i,
		)
	}

	// Partial request: only transaction index 1 (MSB-first: bit 62).
	resp2, err := o.leiosfetchServerBlockTxsRequest(
		oleiosfetch.CallbackContext{},
		point,
		map[uint16]uint64{0: 1 << 62},
	)
	require.NoError(t, err)
	msg2, ok := resp2.(*oleiosfetch.MsgBlockTxs)
	require.True(t, ok)
	require.Len(t, msg2.TxsRaw, 1)
	var inner1 []byte
	_, err = cbor.Decode(msg2.TxsRaw[0], &inner1)
	require.NoError(t, err)
	require.Equal(t, bodies[1], inner1)
}
