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

package safedecode_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// bignum encodes an unsigned bignum (tag 2) with an n-byte magnitude.
func bignum(n int) []byte {
	out := []byte{0xc2}
	switch {
	case n < 24:
		out = append(out, 0x40|byte(n))
	case n < 256:
		out = append(out, 0x58, byte(n))
	default:
		out = append(out, 0x59, byte(n>>8), byte(n))
	}
	return append(out, bytes.Repeat([]byte{0x01}, n)...)
}

func rational(numerator, denominator []byte) []byte {
	out := []byte{0xd8, 0x1e, 0x82}
	out = append(out, numerator...)
	return append(out, denominator...)
}

// wrapped encodes item as a tag-24 byte string.
func wrapped(item []byte) []byte {
	out := []byte{0xd8, 0x18, 0x59, byte(len(item) >> 8), byte(len(item))}
	return append(out, item...)
}

// negBignum is bignum with the negative tag (3).
func negBignum(n int) []byte {
	out := bignum(n)
	out[0] = 0xc3
	return out
}

// chunked encodes item as an indefinite-length tag-24 byte string split into
// two chunks at split, so an item inside it straddles a chunk boundary.
func chunked(item []byte, split int) []byte {
	out := []byte{0xd8, 0x18, 0x5f, 0x58, byte(split)}
	out = append(out, item[:split]...)
	out = append(out, 0x58, byte(len(item)-split))
	out = append(out, item[split:]...)
	return append(out, 0xff)
}

// nest wraps item the way a certificate sits in a transaction: inside a map
// value inside an array.
func nest(item []byte) []byte {
	out := []byte{0x82, 0x01, 0xa1, 0x04}
	return append(out, item...)
}

func TestRejectOversizedRationals(t *testing.T) {
	t.Parallel()
	small := []byte{0x01}
	// A Word64 is eight bytes wide.
	atLimit := bignum(8)
	pastLimit := bignum(9)
	padded := append([]byte{0xc2, 0x58, 0x30}, make([]byte, 40)...)
	padded = append(padded, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)
	padded[2] = byte(len(padded) - 3)

	tests := []struct {
		name    string
		data    []byte
		oversze bool
	}{
		{"small rational", rational(small, []byte{0x05}), false},
		{"numerator at the limit", rational(atLimit, small), false},
		{"leading zero padding is not magnitude", rational(padded, small), false},
		{"oversized numerator", rational(pastLimit, small), true},
		{"oversized denominator", rational(small, pastLimit), true},
		{"nested oversized", nest(rational(small, pastLimit)), true},
		{
			"indefinite-length bignum",
			rational(
				append(append([]byte{0xc2, 0x5f, 0x58, 0x20}, bytes.Repeat([]byte{1}, 32)...),
					append([]byte{0x58, 0x20}, append(bytes.Repeat([]byte{1}, 32), 0xff)...)...),
				small,
			),
			true,
		},
		{"oversized negative bignum", rational(negBignum(9), small), true},
		{"negative bignum at the limit", rational(small, negBignum(8)), false},
		{"non-minimal array head", append([]byte{0xd8, 0x1e, 0x98, 0x02, 0x01}, pastLimit...), true},
		{"indefinite array", append(append([]byte{0xd8, 0x1e, 0x9f, 0x01}, pastLimit...), 0xff), true},
		{"tag stack before the array", append([]byte{0xd8, 0x1e, 0xd9, 0xd9, 0xf7, 0x82, 0x01}, pastLimit...), true},
		{"array of three is left to the decoder", append([]byte{0xd8, 0x1e, 0x83, 0x01, 0x01}, pastLimit...), false},
		{"oversized rational in an indefinite tag-24", nest(chunked(rational(small, pastLimit), 5)), true},
		{"small rational in an indefinite tag-24", nest(chunked(rational(small, []byte{0x05}), 2)), false},
		{"oversized byte string outside a rational", nest(append([]byte{0x59, 0x01, 0x00}, make([]byte, 256)...)), false},
		{"oversized bignum outside a rational", nest(bignum(200)), false},
		{"small rational inside a tag-24 byte string", nest(wrapped(rational(small, []byte{0x05}))), false},
		{"oversized rational inside a tag-24 byte string", nest(wrapped(rational(small, pastLimit))), true},
		{"tag-24 wrapper with malformed content", nest(wrapped([]byte{0x82, 0x01})), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := safedecode.RejectOversizedRationals(tt.data)
			if tt.oversze {
				require.ErrorIs(t, err, safedecode.ErrOversizedRational)
				return
			}
			require.NoError(t, err)
		})
	}
}

// The scan rejects only what it identifies; input it cannot walk is the
// decoder's to report.
func TestRejectOversizedRationalsLeavesMalformedInputToDecoder(t *testing.T) {
	t.Parallel()
	valid := nest(rational([]byte{0x01}, bignum(200)))
	deep := bytes.Repeat([]byte{0x81}, 1000)
	for name, data := range map[string][]byte{
		"empty":                   nil,
		"truncated mid-bignum":    valid[:len(valid)-10],
		"reserved head":           {0x82, 0x01, 0x1c},
		"array longer than input": {0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		"nesting past the bound":  append(deep, rational([]byte{1}, bignum(200))...),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, safedecode.RejectOversizedRationals(data))
		})
	}
}

// The entry points that decode peer or API bytes must refuse the input before
// the era decoder runs; a panic or a decode error here would mean it ran.
func TestDecodeEntryPointsRefuseOversizedRationals(t *testing.T) {
	t.Parallel()
	data := nest(rational([]byte{0x01}, bignum(200)))

	_, err := safedecode.Transaction(ledger.TxTypeConway, data)
	require.ErrorIs(t, err, safedecode.ErrOversizedRational)

	_, err = safedecode.DijkstraBlockTransaction(data)
	require.ErrorIs(t, err, safedecode.ErrOversizedRational)
}
