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

package ledgerstate

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func cborCat(parts ...string) []byte {
	var b []byte
	for _, p := range parts {
		d, err := hex.DecodeString(p)
		if err != nil {
			panic(err)
		}
		b = append(b, d...)
	}
	return b
}

// TestLedgerTxOutMatchesTheLedgerSerialization pins the bytes cardano-ledger
// emits for a Conway UTxO entry, from stored forms that differ from it.
func TestLedgerTxOutMatchesTheLedgerSerialization(t *testing.T) {
	t.Parallel()
	addr := "581d61" + hex.EncodeToString(bytes.Repeat([]byte{0x11}, 28))
	dh := "5820" + hex.EncodeToString(bytes.Repeat([]byte{0x22}, 32))
	p1 := "581c" + hex.EncodeToString(bytes.Repeat([]byte{0x01}, 28))
	p2 := "581c" + hex.EncodeToString(bytes.Repeat([]byte{0x02}, 28))
	keyHash := "581c" + hex.EncodeToString(bytes.Repeat([]byte{0x33}, 28))
	payment := hex.EncodeToString(bytes.Repeat([]byte{0x44}, 28))

	// 24 single-byte asset names, stored in descending order.
	var manyIn, manyWant []string
	for i := 23; i >= 0; i-- {
		manyIn = append(manyIn, hex.EncodeToString([]byte{0x41, byte(i), 0x01}))
	}
	for i := range 24 {
		manyWant = append(manyWant, hex.EncodeToString([]byte{0x41, byte(i), 0x01}))
	}

	tests := []struct {
		name   string
		stored []byte
		want   []byte
	}{
		{
			name:   "map without datum or script becomes the 2-element array",
			stored: cborCat("a2", "00", addr, "01", "05"),
			want:   cborCat("82", addr, "05"),
		},
		{
			name:   "map with a datum hash becomes the 3-element array",
			stored: cborCat("a3", "01", "05", "00", addr, "02", "8200", dh),
			want:   cborCat("83", addr, "05", dh),
		},
		{
			name:   "empty multiasset is a bare coin",
			stored: cborCat("82", addr, "8205a0"),
			want:   cborCat("82", addr, "05"),
		},
		{
			name: "policies and asset names in bytewise order",
			stored: cborCat("82", addr, "8205", "a2",
				p2, "a2", "41ff01", "42000002",
				p1, "a1", "410103"),
			want: cborCat("82", addr, "8205", "a2",
				p1, "a1", "410103",
				p2, "a2", "42000002", "41ff01"),
		},
		{
			name: "more than 23 asset names is an indefinite map",
			stored: cborCat(append([]string{
				"82", addr, "8205", "a1", p1, "b818",
			}, manyIn...)...),
			want: cborCat(append(append([]string{
				"82", addr, "8205", "a1", p1, "bf",
			}, manyWant...), "ff")...),
		},
		{
			name: "inline datum keeps the map with ascending keys",
			stored: cborCat("a3", "02", "8201d81843d87980",
				"00", addr, "01", "05"),
			want: cborCat("a3", "00", addr, "01", "05",
				"02", "8201d81843d87980"),
		},
		{
			name:   "plutus script reference",
			stored: cborCat("a3", "03", "d818468202434e4d01", "00", addr, "01", "05"),
			want:   cborCat("a3", "00", addr, "01", "05", "03", "d818468202434e4d01"),
		},
		{
			name: "native script reference with a datum hash",
			stored: cborCat("a4", "03", "d8185822", "8200", "8200", keyHash,
				"02", "8200", dh, "01", "05", "00", addr),
			want: cborCat("a4", "00", addr, "01", "05", "02", "8200", dh,
				"03", "d8185822", "8200", "8200", keyHash),
		},
		{
			// Slot 2^32 does not fit the pointer's 32-bit slot, so
			// mkPtrNormalized zeroes all three components.
			name: "out-of-range pointer is normalized",
			stored: cborCat("82", "582441", payment,
				"9080808000", "01", "02", "05"),
			want: cborCat("82", "582041", payment, "000000", "05"),
		},
		{
			name:   "in-range pointer is kept",
			stored: cborCat("82", "582041", payment, "050102", "05"),
			want:   cborCat("82", "582041", payment, "050102", "05"),
		},
		{
			name:   "bytes after the address payload are cropped",
			stored: cborCat("82", "581f61", payment, "dead", "05"),
			want:   cborCat("82", "581d61", payment, "05"),
		},
		{
			name:   "byron address is kept as stored",
			stored: cborCat("82", "4582d8184100", "05"),
			want:   cborCat("82", "4582d8184100", "05"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, err := decodeSCLSTxOut(tc.stored)
			require.NoError(t, err)
			got, err := ledgerTxOut(out)
			require.NoError(t, err)
			require.Equal(t,
				hex.EncodeToString(tc.want),
				hex.EncodeToString(got),
			)
		})
	}
}
