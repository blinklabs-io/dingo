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

package forging

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// blockTxFromMempool independently re-orders a four-element mempool
// transaction, [body, witnesses, is_valid, auxiliary_data], into the
// block_transaction order, [body, witnesses, auxiliary_data, is_valid].
func blockTxFromMempool(t *testing.T, mempoolTx []byte) []byte {
	t.Helper()
	var parts []cbor.RawMessage
	_, err := cbor.Decode(mempoolTx, &parts)
	require.NoError(t, err)
	require.Len(t, parts, 4)
	out, err := cbor.Encode([]cbor.RawMessage{
		parts[0], parts[1], parts[3], parts[2],
	})
	require.NoError(t, err)
	return out
}

func TestLeiosBlockTransactionCbor(t *testing.T) {
	body := cbor.RawMessage{0xa0}
	witnesses := cbor.RawMessage{0xa0}
	aux := cbor.RawMessage{0xa1, 0x00, 0x41, 0x01}
	encode := func(parts ...cbor.RawMessage) []byte {
		out, err := cbor.Encode(parts)
		require.NoError(t, err)
		return out
	}
	wantTrue := encode(body, witnesses, aux, cbor.RawMessage{0xf5})
	wantNullAux := encode(
		body,
		witnesses,
		cbor.RawMessage{0xf6},
		cbor.RawMessage{0xf5},
	)

	for name, tc := range map[string]struct {
		in      []byte
		want    []byte
		wantErr string
	}{
		"mempool order": {
			in:   encode(body, witnesses, cbor.RawMessage{0xf5}, aux),
			want: wantTrue,
		},
		"without is_valid": {
			in:   encode(body, witnesses, aux),
			want: wantTrue,
		},
		"null auxiliary data": {
			in:   encode(body, witnesses, cbor.RawMessage{0xf6}),
			want: wantNullAux,
		},
		"already block order": {
			in:   wantTrue,
			want: wantTrue,
		},
		"block order marked invalid": {
			in: encode(body, witnesses, aux, cbor.RawMessage{0xf4}),
			want: encode(
				body, witnesses, aux, cbor.RawMessage{0xf4},
			),
		},
		"mempool order marked invalid": {
			in:      encode(body, witnesses, cbor.RawMessage{0xf4}, aux),
			wantErr: "marked invalid",
		},
		"no validity flag": {
			in:      encode(body, witnesses, aux, aux),
			wantErr: "no is_valid",
		},
		"wrong arity": {
			in:      encode(body, witnesses),
			wantErr: "expected 3 or 4",
		},
		"not an array": {
			in:      []byte{0x01},
			wantErr: "decode transaction",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := leiosBlockTransactionCbor(tc.in)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// The manifest reference and the served body must both be the
// block_transaction bytes, not the mempool encoding the transaction arrived in.
func TestBuildLeiosEBBindsBlockTransactionBytes(t *testing.T) {
	mempoolTx := makeMinimalTxCbor(t, 0x07, 0)
	ebCbor, _, bodies, err := buildLeiosEB(
		[]MempoolTransaction{{
			Hash: strings.Repeat("07", 32),
			Cbor: mempoolTx,
		}},
		leiosEBCaps{},
	)
	require.NoError(t, err)
	want := blockTxFromMempool(t, mempoolTx)
	require.Equal(t, [][]byte{want}, bodies)
	require.NotEqual(t, mempoolTx, bodies[0])

	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(ebCbor)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, 1)
	require.Equal(
		t,
		lcommon.Blake2b256Hash(want),
		eb.TransactionReferences[0].TransactionHash,
	)
	require.Equal(
		t,
		uint32(len(want)),
		eb.TransactionReferences[0].TransactionSize,
	)
}

// A transaction that cannot be expressed as a block_transaction is left out of
// the manifest and the bodies together.
func TestBuildLeiosEBDropsUnconvertibleTransaction(t *testing.T) {
	good := makeMinimalTxCbor(t, 0x08, 0)
	_, _, bodies, err := buildLeiosEB(
		[]MempoolTransaction{
			{Hash: strings.Repeat("09", 32), Cbor: []byte{0x01, 0x02}},
			{Hash: strings.Repeat("08", 32), Cbor: good},
		},
		leiosEBCaps{},
	)
	require.NoError(t, err)
	require.Equal(t, [][]byte{blockTxFromMempool(t, good)}, bodies)
}
