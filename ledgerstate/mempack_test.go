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
	"fmt"
	"os"
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

// TestDecodeMempackTxOutTag4 tests decoding a real MemPack TxOut
// with tag 4 (TxOutCompactDatum) from a preview network snapshot.
func TestDecodeMempackTxOutTag4(t *testing.T) {
	t.Parallel()

	// Real TxOut from preview snapshot at offset 0x28 in tvar file.
	// Tag 4 = TxOutCompactDatum: CompactAddr + Value + BinaryData
	// Note: the original hex extracted from the tvar file had an
	// extra trailing 0x80 byte (start of the next CBOR entry).
	// That byte is not part of this TxOut.
	txOutHex := "041d70bbaf8ca900440d2bf53afe3697d041392cb3cac4bc" +
		"509ada17b8f8da01d8b1600148010000000000000" +
		"00c0028" +
		"00e3865ada8b457dcf10524e600158d6a1a15a8f3d51c117" +
		"f4fbbf6b816cfb0d04d2c8cd2e094e06e1416d9516de43f2" +
		"f297d7c2070a190774667720662258" +
		"20cc54e3d1fa5cccde13ea321d7170d8f1a4c7946d2ee838" +
		"9a542d1d673e714242"

	txOutBytes, err := hex.DecodeString(txOutHex)
	require.NoError(t, err)

	decoded, err := decodeMempackTxOut(txOutBytes)
	require.NoError(t, err, "decodeMempackTxOut failed")

	// Tag 4: should have address, value, and datum
	require.NotEmpty(t, decoded.Address, "address should not be empty")
	require.Equal(t, 29, len(decoded.Address),
		"enterprise address should be 29 bytes")
	require.Equal(t, byte(0x70), decoded.Address[0],
		"should be enterprise script address (testnet)")
	outputCbor, err := encodeMempackTxOut(decoded)
	require.NoError(t, err)
	output, err := ledger.NewTransactionOutputFromCbor(outputCbor)
	require.NoError(t, err)
	gotAddress, err := output.Address().Bytes()
	require.NoError(t, err)
	require.Equal(t, decoded.Address, gotAddress)
	require.Equal(t, decoded.Lovelace, output.Amount().Uint64())
	require.NotNil(t, output.Datum(), "inline datum must survive CBOR reconstruction")

	// With corrected VarLen (big-endian 7-bit), the coin bytes
	// d8 b1 60 decode as:
	//   (0x58 << 14) | (0x31 << 7) | 0x60 = 1448160
	require.Equal(t, uint64(1448160), decoded.Lovelace,
		"coin should be 1448160 lovelace (~1.45 ADA)")

	t.Logf("Address: %x", decoded.Address)
	t.Logf("Lovelace: %d", decoded.Lovelace)
	t.Logf("Assets: %d", len(decoded.Assets))
	if decoded.Datum != nil {
		t.Logf("Datum: %d bytes", len(decoded.Datum))
	}
}

// TestDecodeMempackTxOutTag4MultiAsset tests decoding entry 1 from
// the tvar file which has tag 4 with a multi-asset value.
func TestDecodeMempackTxOutTag4MultiAsset(t *testing.T) {
	t.Parallel()

	// Entry 1 from tvar: tag 4, 1 native asset
	txOutHex := "041d704ab17afc9a19a4f06b6fe229f9501e727d3968bff0" +
		"3acb1a8f86acf501f5cb2a012c5904852e00000000" +
		"0c002800" +
		"7c833f1eb9b70c2e700d028e0ee28d421edad2af42220" +
		"61be525382d555344438144d8799fd8799f581c7c833f" +
		"1eb9b70c2e700d028e0ee28d421edad2af4222061be52" +
		"5382d4455534443ffd8799f4040ffd8799f581c7c833f" +
		"1eb9b70c2e700d028e0ee28d421edad2af4222061be52" +
		"5382d514144415f555344435f4944454e54495459ff19" +
		"03e31b0011460f3a1407cd1b002386f26fc10000581cb" +
		"fd59e8bb9d6448f58f46c34379ad913989ecfd6413d3d" +
		"a40d016d72d8799f581c473cae93b0651c74e06e5506c" +
		"f55f1821514c6bbfc7e42556e2bae31ff1a2e850459" +
		"1a002f131bff"

	txOutBytes, err := hex.DecodeString(txOutHex)
	require.NoError(t, err)

	decoded, err := decodeMempackTxOut(txOutBytes)
	require.NoError(t, err, "decodeMempackTxOut failed")

	require.NotEmpty(t, decoded.Address)
	require.Equal(t, 29, len(decoded.Address))
	require.Equal(t, byte(0x70), decoded.Address[0],
		"enterprise script address (testnet)")

	// Coin VarLen bytes: f5 cb 2a
	// (0x75 << 14) | (0x4b << 7) | 0x2a = 1926570
	require.Equal(t, uint64(1926570), decoded.Lovelace,
		"coin should be 1926570 lovelace (~1.93 ADA)")

	// Should have 1 native asset
	require.Equal(t, 1, len(decoded.Assets),
		"should have exactly 1 asset")
	outputCbor, err := encodeMempackTxOut(decoded)
	require.NoError(t, err)
	output, err := ledger.NewTransactionOutputFromCbor(outputCbor)
	require.NoError(t, err)
	require.Equal(t, decoded.Lovelace, output.Amount().Uint64())
	require.Len(t, output.Assets().Policies(), 1)

	t.Logf("Address: %x", decoded.Address)
	t.Logf("Lovelace: %d", decoded.Lovelace)
	t.Logf("Assets: %d", len(decoded.Assets))
	if len(decoded.Assets) > 0 {
		a := decoded.Assets[0]
		t.Logf("Asset[0] PolicyId: %x", a.PolicyId)
		t.Logf("Asset[0] Name: %s (%x)", a.Name, a.Name)
		t.Logf("Asset[0] Amount: %d", a.Amount)
	}
	if decoded.Datum != nil {
		t.Logf("Datum: %d bytes", len(decoded.Datum))
	}
}

// TestDecodeMempackTxOutTag2 tests decoding entry 2 from the tvar
// file which uses tag 2 (AddrHash28 ADA-only).
func TestDecodeMempackTxOutTag2(t *testing.T) {
	t.Parallel()

	// Entry 2 from tvar: tag 2 = TxOut_AddrHash28_AdaOnly
	txOutHex := "02015691d68ad87582fc89b9ac43fd0227cfa4108efb79" +
		"1b9987b290a9ba85b06c5a4edd9c1b857a1b55106ee4" +
		"0191bb091025984a9d01000000f68597d600c99f00"

	txOutBytes, err := hex.DecodeString(txOutHex)
	require.NoError(t, err)

	decoded, err := decodeMempackTxOut(txOutBytes)
	require.NoError(t, err, "decodeMempackTxOut failed")

	// Tag 2 should produce a base address (57 bytes)
	require.Equal(t, 57, len(decoded.Address),
		"base address should be 57 bytes")

	// Lovelace should be reasonable (> 0, < 100B ADA)
	require.Greater(t, decoded.Lovelace, uint64(0),
		"lovelace should be > 0")
	require.Less(t, decoded.Lovelace, uint64(100_000_000_000_000),
		"lovelace should be < 100T (sanity)")

	// No assets (ADA-only)
	require.Empty(t, decoded.Assets, "should have no assets")
	require.Nil(t, decoded.DatumHash, "should have no datum hash")

	t.Logf("Address: %x", decoded.Address)
	t.Logf("Header byte: 0x%02x", decoded.Address[0])
	t.Logf("Lovelace: %d (~%.2f ADA)",
		decoded.Lovelace,
		float64(decoded.Lovelace)/1_000_000)
}

func TestEncodeMempackTxOutReferenceScript(t *testing.T) {
	t.Parallel()

	address := buildShelleyAddr(
		7, 1, bytes.Repeat([]byte{0x11}, 28), nil,
	)
	nativeScript, err := cbor.Encode([]any{
		uint64(0), bytes.Repeat([]byte{0x22}, 28),
	})
	require.NoError(t, err)

	// Tag 5 stores CompactAddr, ADA-only CompactValue, no datum, then
	// AlonzoScript's native-script tag and length-prefixed CBOR.
	mempack := []byte{babbageTxOutCompactRefScript, byte(len(address))}
	mempack = append(mempack, address...)
	mempack = append(mempack, 0, 1, 0, alonzoScriptNative, byte(len(nativeScript)))
	mempack = append(mempack, nativeScript...)

	decoded, err := decodeMempackTxOut(mempack)
	require.NoError(t, err)
	require.Equal(t, uint8(alonzoScriptNative), decoded.ScriptRefType)
	require.Equal(t, nativeScript, decoded.ScriptRef)
	outputCbor, err := encodeMempackTxOut(decoded)
	require.NoError(t, err)
	output, err := ledger.NewTransactionOutputFromCbor(outputCbor)
	require.NoError(t, err)
	require.NotNil(t, output.ScriptRef())

	plutusMempack := []byte{babbageTxOutCompactRefScript, byte(len(address))}
	plutusMempack = append(plutusMempack, address...)
	plutusMempack = append(plutusMempack, 0, 1, 0, alonzoScriptPlutus, 0, 2, 0x41, 0x00)
	plutusOutput, err := decodeMempackTxOut(plutusMempack)
	require.NoError(t, err)
	require.Equal(t, uint8(1), plutusOutput.ScriptRefType,
		"Plutus V1's zero version tag must not be mistaken for a native script")
	encodedPlutus, err := encodeMempackTxOut(plutusOutput)
	require.NoError(t, err)
	parsedPlutus, err := ledger.NewTransactionOutputFromCbor(encodedPlutus)
	require.NoError(t, err)
	require.NotNil(t, parsedPlutus.ScriptRef())
}

func TestVarLenDecoding(t *testing.T) {
	t.Parallel()

	// MemPack VarLen is big-endian 7-bit continuation:
	// MSB=1 means more bytes, MSB=0 means last byte.
	// acc = (acc << 7) | (byte & 0x7f) for each byte.
	tests := []struct {
		name     string
		input    []byte
		expected int
	}{
		{"zero", []byte{0x00}, 0},
		{"one", []byte{0x01}, 1},
		{"29", []byte{0x1d}, 29},
		{"127", []byte{0x7f}, 127},
		// 128 = 0b10000000 -> 7-bit groups from MSB: 0000001 0000000
		// bytes: 0x81 0x00
		{"128", []byte{0x81, 0x00}, 128},
		// 300 = 0b100101100 -> 7-bit groups from MSB: 0000010 0101100
		// bytes: 0x82 0x2c
		{"300", []byte{0x82, 0x2c}, 300},
		// 1448160 = 0x161660
		// In binary: 1_0110000_1100010_1100000 (21 bits)
		// 7-bit groups from MSB: 1011000 0110001 1100000
		// With continuation: 0xd8 (1_1011000) 0xb1 (1_0110001) 0x60 (0_1100000)
		{"1448160", []byte{0xd8, 0xb1, 0x60}, 1448160},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newMempackReader(tt.input)
			got, err := r.readVarLen()
			require.NoError(t, err)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestIsMempackFormat(t *testing.T) {
	t.Parallel()

	require.True(t, isMempackFormat([]byte{0x00, 0x01}))
	require.True(t, isMempackFormat([]byte{0x04, 0x1d}))
	require.True(t, isMempackFormat([]byte{0x05, 0x1d}))
	require.False(t, isMempackFormat([]byte{0x82})) // CBOR array
	require.False(t, isMempackFormat([]byte{0xa2})) // CBOR map
	require.False(t, isMempackFormat([]byte{0xbf})) // CBOR indef map
	require.False(t, isMempackFormat([]byte{}))     // empty
}

// TestParseTvarFileIfAvailable tests parsing a real tvar file from a
// Mithril snapshot, if one is available at the expected path.
func TestParseTvarFileIfAvailable(t *testing.T) {
	t.Parallel()

	tvarPath := os.Getenv("DINGO_TVAR_PATH")
	if tvarPath == "" {
		t.Skip(
			"DINGO_TVAR_PATH not set, skipping " +
				"(set to a UTxO-HD tvar file path)",
		)
	}
	if _, err := os.Stat(tvarPath); err != nil {
		t.Skipf("tvar file not accessible at %s: %v", tvarPath, err)
	}

	var totalEntries int
	var sampleEntry *ParsedUTxO
	count, err := ParseUTxOsFromFile(
		tvarPath,
		func(batch []ParsedUTxO) error {
			totalEntries += len(batch)
			if sampleEntry == nil && len(batch) > 0 {
				e := batch[0]
				sampleEntry = &e
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Greater(t, count, 0, "should have parsed some UTxOs")

	t.Logf("Parsed %d UTxO entries", count)
	t.Logf("Total entries via callback: %d", totalEntries)
	if sampleEntry != nil {
		t.Logf("Sample TxHash: %x", sampleEntry.TxHash)
		t.Logf("Sample Address: %x", sampleEntry.Address)
		t.Logf("Sample Amount: %d lovelace", sampleEntry.Amount)
	}
}
