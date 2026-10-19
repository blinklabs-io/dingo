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

package database

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func TestLeiosTxViewsMatchRawDecoder(t *testing.T) {
	t.Parallel()
	for _, raw := range [][]byte{
		{0x80}, {0xf6}, {0x82, 1, 2}, {0x9f, 1, 0x81, 2, 0xff},
		{0x82, 0xd8, 24, 0x41, 1, 0xf6}, {0x81, 1, 2},
		{0x81}, {0x81, 0x82, 1}, {0x81, 0xff}, {0xa1, 1, 2},
		{0x81, 0xa2, 1, 2, 1, 3},
	} {
		var want []cbor.RawMessage
		_, oldErr := cbor.Decode(raw, &want)
		got, err := decodeLeiosEBTxs(raw)
		if oldErr != nil {
			require.Error(t, err, "old decoder rejects %x", raw)
			continue
		}
		require.NoError(t, err, "old decoder accepts %x", raw)
		require.Equal(t, want, got, "raw bytes differ for %x", raw)
	}
}

func TestLeiosTxViewsAppendCannotOverwriteNextTransaction(t *testing.T) {
	t.Parallel()
	txs, err := decodeLeiosEBTxs([]byte{0x82, 1, 2})
	require.NoError(t, err)
	appended := append(txs[0], 99)
	require.Equal(t, cbor.RawMessage{1, 99}, appended)
	require.Equal(t, cbor.RawMessage{2}, txs[1])
}

func TestGetLeiosEBTxsReadsOwnTheirPayload(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	hash := bytes.Repeat([]byte{42}, 32)
	want := []cbor.RawMessage{{0x81, 1}, {0x81, 2}}
	require.NoError(t, db.SetLeiosEBTxs(7, hash, want))
	first, err := db.GetLeiosEBTxs(hash, 7)
	require.NoError(t, err)
	second, err := db.GetLeiosEBTxs(hash, 7)
	require.NoError(t, err)
	first[0][1] = 99
	require.Equal(t, want, second)
	third, err := db.GetLeiosEBTxs(hash, 7)
	require.NoError(t, err)
	require.Equal(t, want, third)
}

func BenchmarkLeiosClosureDecode(b *testing.B) {
	txs := make([]cbor.RawMessage, 2857)
	for i := range txs {
		txs[i] = append([]byte{0x59, 1, 0x5b}, make([]byte, 347)...)
	}
	raw, err := cbor.Encode(txs)
	if err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"raw", "views"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var got []cbor.RawMessage
				var err error
				if name == "raw" {
					_, err = cbor.Decode(raw, &got)
				} else {
					got, err = decodeLeiosEBTxs(raw)
				}
				if err != nil || len(got) != len(txs) {
					b.Fatalf("decoded %d transactions: %v", len(got), err)
				}
			}
		})
	}
}
