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
	"github.com/blinklabs-io/dingo/internal/txsize"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// oversizedRationalPayload is an array holding a tag-30 rational whose
// denominator is a 40 KB bignum, the shape of a pool margin that makes the
// ledger decoder spend quadratic CPU.
func oversizedRationalPayload() []byte {
	out := []byte{0x81, 0xd8, 0x1e, 0x82, 0x01, 0xc2, 0x59, 0x9c, 0x40}
	return append(out, bytes.Repeat([]byte{0x01}, 40000)...)
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

// withLimits returns an Ouroboros whose decode limits are fixed, standing in
// for what a ledger at some protocol version and MaxTxSize would report.
func withLimits(t *testing.T, limits peerDecodeLimits) *Ouroboros {
	t.Helper()
	o := newMusashiOuroboros(t, nil)
	o.decodeLimits = func(uint) peerDecodeLimits { return limits }
	return o
}

// from protocol version 12 the ledger requires each rational component to fit a
// Word64 as written, so peer bytes that break it are refused before the
// quadratic decode, on both peer decode paths.
func TestPeerDecodersRefuseWideRationalsFromVersion12(t *testing.T) {
	t.Parallel()
	o := withLimits(t, peerDecodeLimits{rationalWidth: safedecode.Word64Bytes})

	t.Run("txsubmission", func(t *testing.T) {
		t.Parallel()
		tx := conwayPoolRegistrationTx(t, rationalCbor(1, bytes.Repeat([]byte{1}, 40000)))
		_, err := o.decodeTxsubmissionTx(gledger.TxTypeConway, tx)
		require.ErrorIs(t, err, safedecode.ErrOversizedRational)
	})

	t.Run("blockfetch", func(t *testing.T) {
		t.Parallel()
		for _, blockType := range []uint{
			gledger.BlockTypeConway,
			gledger.BlockTypeDijkstra,
			gledger.BlockTypeBabbage,
		} {
			_, err := o.decodeBlockfetchBlock(blockType, oversizedRationalPayload())
			require.ErrorIs(t, err, safedecode.ErrOversizedRational)
		}
	})
}

// Below protocol version 12 the ledger bounds the reduced rational, so a
// margin of 2^64/2^65 (nine-byte components, reduced 1/2) is a valid
// transaction and must still decode.
func TestPeerDecodersAcceptWideRationalsBeforeVersion12(t *testing.T) {
	t.Parallel()
	o := withLimits(t, peerDecodeLimits{})
	two64 := append([]byte{0xc2, 0x49, 0x01}, make([]byte, 8)...)
	two65 := append([]byte{0xc2, 0x49, 0x02}, make([]byte, 8)...)
	margin := append(append([]byte{0xd8, 0x1e, 0x82}, two64...), two65...)

	tx, err := o.decodeTxsubmissionTx(
		gledger.TxTypeConway,
		conwayPoolRegistrationTx(t, margin),
	)
	require.NoError(t, err)
	require.Len(t, tx.Certificates(), 1)

	_, err = o.decodeBlockfetchBlock(gledger.TxTypeConway, oversizedRationalPayload())
	require.NotErrorIs(t, err, safedecode.ErrOversizedRational)
}

// A transaction larger than the ledger's MaxTxSize is invalid whatever it
// holds, and is refused from its length before any decode cost is paid.
func TestTxsubmissionRefusesTxOverMaxTxSizeBeforeDecoding(t *testing.T) {
	t.Parallel()
	tx := conwayPoolRegistrationTx(t, []byte{0xd8, 0x1e, 0x82, 0x01, 0x14})

	size := uint(txsize.FromCBOR(gledger.TxTypeConway, tx))
	o := withLimits(t, peerDecodeLimits{maxTxSize: size - 1})
	_, err := o.decodeTxsubmissionTx(gledger.TxTypeConway, tx)
	var sizeErr shelley.MaxTxSizeUtxoError
	require.ErrorAs(t, err, &sizeErr)

	o = withLimits(t, peerDecodeLimits{maxTxSize: size})
	decoded, err := o.decodeTxsubmissionTx(gledger.TxTypeConway, tx)
	require.NoError(t, err)
	require.Len(t, decoded.Certificates(), 1)
}

// rationalCbor encodes numerator over a bignum denominator of the given
// magnitude as a tag-30 rational.
func rationalCbor(numerator byte, magnitude []byte) []byte {
	out := []byte{0xd8, 0x1e, 0x82, numerator, 0xc2, 0x59,
		byte(len(magnitude) >> 8), byte(len(magnitude))}
	return append(out, magnitude...)
}

func TestRationalWidthForVersion(t *testing.T) {
	t.Parallel()
	require.Zero(t, rationalWidthForVersion(10))
	require.Zero(t, rationalWidthForVersion(11))
	require.Equal(t, safedecode.Word64Bytes, rationalWidthForVersion(12))
	require.Equal(t, safedecode.Word64Bytes, rationalWidthForVersion(13))
}
