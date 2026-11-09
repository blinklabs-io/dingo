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
	"testing"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// oversizedRationalPayload is an array holding a tag-30 rational whose
// denominator is a 40 KB bignum, the shape of a pool margin that makes the
// ledger decoder spend quadratic CPU.
func oversizedRationalPayload() []byte {
	out := []byte{0x81, 0xd8, 0x1e, 0x82, 0x01, 0xc2, 0x59, 0x9c, 0x40}
	return append(out, bytes.Repeat([]byte{0x01}, 40000)...)
}

// A fetched block is decoded before its body is compared with the header's
// body hash, so every decoder branch must refuse the payload up front.
func TestBlockfetchRefusesOversizedRationals(t *testing.T) {
	t.Parallel()
	payload := oversizedRationalPayload()
	o := newMusashiOuroboros(t, nil)
	for _, blockType := range []uint{
		gledger.BlockTypeConway,
		gledger.BlockTypeDijkstra,
		gledger.BlockTypeBabbage,
	} {
		_, err := o.decodeBlockfetchBlock(blockType, payload)
		require.ErrorIs(t, err, safedecode.ErrOversizedRational)
	}
}

// conwayPoolRegistrationTx builds a Conway transaction whose only certificate
// is a legacy 10-field pool registration carrying margin, the field the issue
// describes.
func conwayPoolRegistrationTx(t *testing.T, margin []byte) []byte {
	t.Helper()
	cert := []any{
		uint64(3),
		bytes.Repeat([]byte{0x01}, 28),
		bytes.Repeat([]byte{0x02}, 32),
		uint64(500_000_000),
		uint64(340_000_000),
		cbor.RawMessage(margin),
		append([]byte{0xe0}, bytes.Repeat([]byte{0x03}, 28)...),
		[]any{bytes.Repeat([]byte{0x04}, 28)},
		[]any{},
		nil,
	}
	body := map[uint64]any{
		0: []any{[]any{bytes.Repeat([]byte{0x05}, 32), uint64(0)}},
		1: []any{},
		2: uint64(200_000),
		4: []any{cert},
	}
	tx, err := cbor.Encode([]any{body, map[uint64]any{}, true, nil})
	require.NoError(t, err)
	return tx
}

// A real pool registration with an ordinary margin must still decode, and one
// with an oversized margin must be refused by the production decoders rather
// than by the ledger decoder's range check.
func TestPoolRegistrationMarginThroughPeerDecoders(t *testing.T) {
	t.Parallel()
	normal := conwayPoolRegistrationTx(
		t,
		[]byte{0xd8, 0x1e, 0x82, 0x01, 0x14}, // 1/20
	)
	tx, err := decodeTxsubmissionTx(gledger.TxTypeConway, normal)
	require.NoError(t, err)
	require.Len(t, tx.Certificates(), 1)

	oversized := conwayPoolRegistrationTx(
		t,
		rationalCbor(1, bytes.Repeat([]byte{0x01}, 40000)),
	)
	_, err = decodeTxsubmissionTx(gledger.TxTypeConway, oversized)
	require.ErrorIs(t, err, safedecode.ErrOversizedRational)
}

// rationalCbor encodes numerator over a bignum denominator of the given
// magnitude as a tag-30 rational.
func rationalCbor(numerator byte, magnitude []byte) []byte {
	out := []byte{0xd8, 0x1e, 0x82, numerator, 0xc2, 0x59,
		byte(len(magnitude) >> 8), byte(len(magnitude))}
	return append(out, magnitude...)
}
