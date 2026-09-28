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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// The Preview transaction at slot 123854321 spends output
// 08fe669995f61a1a0187c8f1358f3e292f06d9c18c0c17b9dfe314853db0ec0a#0.
// Its witness supplies d87980. The reconstructed MemPack fixtures preserve
// that output's address, amount and datum hash; the tag-5 fixture also adds
// a reference script to cover its raw SafeHash representation.
const previewDatumSpendingTxHex = "84a900d901028182582008fe669995f61a1a0187c8f1358f3e292f06d9c18c0c" +
	"17b9dfe314853db0ec0a00018182581d60c2efb2a030abe0cbe1d9557035e568" +
	"ffb856fda9ab9676a98627a56a1a001be509021a00029f77031a0761dee80b58" +
	"2094486a49e7af7ae95ee7139d6da9ea0dc0b9bc5a810cfbd516eaf28176a86d" +
	"820dd901028182582052b936a016c2e84b4cbdaf8a3bf50cbb7a8a518e22097e" +
	"68d2a7646af8972de4000f001082581d60c2efb2a030abe0cbe1d9557035e568" +
	"ffb856fda9ab9676a98627a56a1a34e74085111a0003ef33a400d90102818258" +
	"204b71601fb48305f588d6c2e510b2a9bd974aeaa4dcc87a14980c6b7c0a579c" +
	"4558403d61cd1faddd1e943e668de1976abbc2ba4333b300ee6d03682ce53c85" +
	"e46d3a037984d1810d9115a9191f9e735fbfe5c2034164709a6fed4a483992a7" +
	"9b650703d90102814e4d0100003322222005120012001104d9010281d8798005" +
	"a182000082d8798082190d481a0007d0c8f5f6"

const previewCompactDatumHashHex = "01391067f33146617a5e61936081db3b2117cbf59bd2123748f58ac967865689" +
	"5789b05dc94c007bd5fdf766585c8d96c7d8352238941d2c641d7200fa890092" +
	"3918e403bf43c34b4ef6b48eb2ee04babed17320d8d1b9ff9ad086e86f44ec"

const previewWordDatumHashHex = "0301895789b05dc94c007bd5fdf766585c8d96c7d8352238941d2c641d72615e" +
	"7a614631f367cb17213bdb8160938af5483712d29bf500000000568667c900fa" +
	"8900c343bf03e418399204eeb28eb4f64e4bb9d1d82073d1bebaec446fe886d0" +
	"9aff"

const previewReferenceDatumHashHex = "05391067f33146617a5e61936081db3b2117cbf59bd2123748f58ac967865689" +
	"5789b05dc94c007bd5fdf766585c8d96c7d8352238941d2c641d7200fa890001" +
	"923918e403bf43c34b4ef6b48eb2ee04babed17320d8d1b9ff9ad086e86f44ec" +
	"00208200581c1111111111111111111111111111111111111111111111111111" +
	"1111"

func TestMempackDatumHashEncoding(t *testing.T) {
	t.Parallel()
	rawTx, err := hex.DecodeString(previewDatumSpendingTxHex)
	require.NoError(t, err)
	tx, err := ledger.NewTransactionFromCbor(ledger.TxTypeConway, rawTx)
	require.NoError(t, err)
	require.Len(t, tx.Inputs(), 1)
	datums := tx.Witnesses().PlutusData()
	require.Len(t, datums, 1)
	want := datums[0].Hash()
	require.Equal(
		t,
		"923918e403bf43c34b4ef6b48eb2ee04babed17320d8d1b9ff9ad086e86f44ec",
		want.String(),
	)
	for _, test := range []struct{ name, packed string }{
		{"raw SafeHash", previewCompactDatumHashHex},
		{"DataHash32 words", previewWordDatumHashHex},
		{"reference script SafeHash", previewReferenceDatumHashHex},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			packed, err := hex.DecodeString(test.packed)
			require.NoError(t, err)
			key, err := cbor.Encode(
				[]any{tx.Inputs()[0].Id().Bytes(), uint32(0)},
			)
			require.NoError(t, err)
			value, err := cbor.Encode(packed)
			require.NoError(t, err)
			rawMap := append(append([]byte{0xa1}, key...), value...)
			var parsed []ParsedUTxO
			count, err := ParseUTxOsStreaming(
				rawMap,
				func(batch []ParsedUTxO) error {
					parsed = append(parsed, batch...)
					return nil
				},
			)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			require.Len(t, parsed, 1)
			model := UTxOToModel(&parsed[0], 123811182)
			output, err := model.Decode()
			require.NoError(t, err)
			state := &mockledger.MockLedgerState{
				UtxoByIdCallback: func(input lcommon.TransactionInput) (lcommon.Utxo, error) {
					if input.String() != tx.Inputs()[0].String() {
						return lcommon.Utxo{}, fmt.Errorf(
							"unexpected input: %s",
							input,
						)
					}
					return lcommon.Utxo{Id: input, Output: output}, nil
				},
			}
			require.NoError(
				t,
				lcommon.ValidateRequiredSpendingDatums(tx, state),
			)
			require.Equal(t, want.Bytes(), model.DatumHash)
			require.NotNil(t, output.DatumHash())
			require.Equal(t, want, *output.DatumHash())
			require.Equal(t, uint64(2000000), output.Amount().Uint64())
			require.Equal(
				t,
				"addr_test1zpnlxv2xv9a9ucvnvzqakwepzl9ltx7jzgm53av2e9ncv45f27ymqhwffsq8h40a7an9shydjmrasdfz8z2p6tryr4eqzk3mmk",
				output.Address().String(),
			)
		})
	}
}

func TestMempackDataHash32Truncated(t *testing.T) {
	t.Parallel()
	packed, err := hex.DecodeString(previewWordDatumHashHex)
	require.NoError(t, err)
	for missing := 1; missing <= 32; missing++ {
		_, err := decodeMempackTxOut(packed[:len(packed)-missing])
		require.ErrorContains(t, err, "reading DataHash32")
	}
}
