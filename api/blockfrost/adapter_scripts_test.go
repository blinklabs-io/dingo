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

package blockfrost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

func insertScript(
	t *testing.T,
	raw *sql.DB,
	hash []byte,
	content []byte,
	scriptType uint8,
) {
	t.Helper()
	_, err := raw.Exec(
		`INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, 1, ?)`,
		hash,
		content,
		scriptType,
	)
	require.NoError(t, err)
}

func nativeScriptFixture(t *testing.T) (lcommon.NativeScript, []byte, string) {
	t.Helper()
	h1 := bytes.Repeat([]byte{0x11}, 28)
	h2 := bytes.Repeat([]byte{0x22}, 28)
	raw, err := cbor.Encode([]any{1, []any{
		[]any{0, h1},
		[]any{4, uint64(100)},
		[]any{5, uint64(200)},
		[]any{3, 2, []any{[]any{0, h1}, []any{0, h2}}},
		[]any{2, []any{[]any{0, h2}}},
	}})
	require.NoError(t, err)
	var script lcommon.NativeScript
	_, err = cbor.Decode(raw, &script)
	require.NoError(t, err)
	hex1, hex2 := hex.EncodeToString(h1), hex.EncodeToString(h2)
	want := `{"type":"all","scripts":[
{"type":"sig","keyHash":"` + hex1 + `"},
{"type":"after","slot":100},
{"type":"before","slot":200},
{"type":"atLeast","required":2,"scripts":[
 {"type":"sig","keyHash":"` + hex1 + `"},{"type":"sig","keyHash":"` + hex2 + `"}]},
{"type":"any","scripts":[{"type":"sig","keyHash":"` + hex2 + `"}]}]}`
	return script, raw, want
}

func TestNodeAdapterScripts(t *testing.T) {
	t.Parallel()
	adapter, raw, _ := newDBBackedAdapter(t)
	ctx := context.Background()
	native, nativeRaw, wantJSON := nativeScriptFixture(t)
	v1, v2 := fill32(0x01)[:28], fill32(0x02)[:28]
	v3, v4 := fill32(0x03)[:28], fill32(0x04)[:28]
	plutus := []byte{0x45, 0x01, 0x02, 0x03, 0x04, 0x05}
	insertScript(t, raw, native.Hash().Bytes(), nativeRaw, 0)
	insertScript(t, raw, v1, plutus, 1)
	insertScript(t, raw, v2, plutus, 2)
	insertScript(t, raw, v3, plutus, 3)
	insertScript(t, raw, v4, plutus, 4)

	hashes, total, err := adapter.Scripts(
		ctx, PaginationParams{Count: 2, Page: 2, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 5, total)
	require.Equal(
		t,
		[]string{hex.EncodeToString(v1), hex.EncodeToString(v2)}[:0],
		hashes[:0],
	)
	require.Equal(
		t,
		[]string{hex.EncodeToString(v2), hex.EncodeToString(v3)},
		hashes,
	)
	hashes, _, err = adapter.Scripts(
		ctx, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Equal(t, []string{hex.EncodeToString(v4)}, hashes)
	hashes, total, err = adapter.Scripts(
		ctx, PaginationParams{Count: 10, Page: 9, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 5, total)
	require.Empty(t, hashes)

	for hash, want := range map[string]string{
		hex.EncodeToString(v1): "plutusV1",
		hex.EncodeToString(v2): "plutusV2",
		hex.EncodeToString(v3): "plutusV3",
		hex.EncodeToString(v4): "plutusV4",
		native.Hash().String(): "timelock",
	} {
		hashBytes, err := hex.DecodeString(hash)
		require.NoError(t, err)
		info, err := adapter.Script(ctx, hashBytes)
		require.NoError(t, err)
		require.Equal(t, hash, info.Hash)
		require.Equal(t, want, info.Type)
		if want == "timelock" {
			require.Nil(t, info.SerialisedSize)
		} else {
			require.NotNil(t, info.SerialisedSize)
			require.Equal(t, len(plutus), *info.SerialisedSize)
		}
	}
	_, err = adapter.Script(ctx, bytes.Repeat([]byte{0xee}, 28))
	require.ErrorIs(t, err, ErrScriptNotFound)

	jsonValue, err := adapter.ScriptJSON(ctx, native.Hash().Bytes())
	require.NoError(t, err)
	require.JSONEq(t, wantJSON, string(jsonValue))
	jsonValue, err = adapter.ScriptJSON(ctx, v2)
	require.NoError(t, err)
	require.Nil(t, jsonValue, "a Plutus script has no JSON form")
	_, err = adapter.ScriptJSON(ctx, bytes.Repeat([]byte{0xee}, 28))
	require.ErrorIs(t, err, ErrScriptNotFound)

	cborHex, err := adapter.ScriptCBOR(ctx, v3)
	require.NoError(t, err)
	require.NotNil(t, cborHex)
	require.Equal(t, hex.EncodeToString(plutus), *cborHex)
	cborHex, err = adapter.ScriptCBOR(ctx, native.Hash().Bytes())
	require.NoError(t, err)
	require.Nil(t, cborHex, "a native script has no Plutus CBOR")
	_, err = adapter.ScriptCBOR(ctx, bytes.Repeat([]byte{0xee}, 28))
	require.ErrorIs(t, err, ErrScriptNotFound)
}

func TestNativeScriptJSONRejectsUnknownConstructor(t *testing.T) {
	t.Parallel()
	_, err := nativeScriptJSON([]byte{0x80})
	require.Error(t, err)
}

func TestNodeAdapterDatum(t *testing.T) {
	t.Parallel()
	adapter, raw, _ := newDBBackedAdapter(t)
	datum := data.NewConstr(0, data.NewInteger(big.NewInt(42)))
	datumCbor, err := data.Encode(datum)
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(datumCbor)
	_, err = raw.Exec(
		`INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 1)`,
		hash.Bytes(), datumCbor,
	)
	require.NoError(t, err)

	ctx := context.Background()
	jsonValue, err := adapter.DatumJSON(ctx, hash.Bytes())
	require.NoError(t, err)
	require.JSONEq(
		t,
		`{"constructor":0,"fields":[{"int":42}]}`,
		string(jsonValue),
	)
	cborHex, err := adapter.DatumCBOR(ctx, hash.Bytes())
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(datumCbor), cborHex)

	missing := bytes.Repeat([]byte{0xee}, 32)
	_, err = adapter.DatumJSON(ctx, missing)
	require.ErrorIs(t, err, ErrDatumNotFound)
	_, err = adapter.DatumCBOR(ctx, missing)
	require.ErrorIs(t, err, ErrDatumNotFound)
}

type fakeRedeemerSource struct {
	byTx  map[string][]TransactionRedeemerInfo
	calls map[string]int
	err   error
}

func (f *fakeRedeemerSource) TransactionRedeemers(
	_ context.Context,
	hash []byte,
) ([]TransactionRedeemerInfo, error) {
	key := hex.EncodeToString(hash)
	f.calls[key]++
	return f.byTx[key], f.err
}

func TestBuildScriptRedeemerInfos(t *testing.T) {
	t.Parallel()
	txA, txB := bytes.Repeat([]byte{0xa1}, 32), bytes.Repeat([]byte{0xb2}, 32)
	datum := "dd"
	source := &fakeRedeemerSource{
		calls: map[string]int{},
		byTx: map[string][]TransactionRedeemerInfo{
			hex.EncodeToString(txA): {
				{
					TxIndex:          0,
					Purpose:          "spend",
					RedeemerDataHash: "r0",
					DatumHash:        &datum,
					UnitMem:          "1",
					UnitSteps:        "2",
					Fee:              "3",
				},
				{
					TxIndex:          1,
					Purpose:          "spend",
					RedeemerDataHash: "r1",
					UnitMem:          "4",
					UnitSteps:        "5",
					Fee:              "6",
				},
				{
					TxIndex:          0,
					Purpose:          "mint",
					RedeemerDataHash: "r2",
					UnitMem:          "7",
					UnitSteps:        "8",
					Fee:              "9",
				},
			},
			hex.EncodeToString(txB): {
				{
					TxIndex:          2,
					Purpose:          "reward",
					RedeemerDataHash: "r3",
					UnitMem:          "0",
					UnitSteps:        "0",
					Fee:              "0",
				},
			},
		},
	}
	refs := []models.ScriptRedeemerRef{
		{TxHash: txA, Tag: uint8(lcommon.RedeemerTagSpend), Index: 1},
		{TxHash: txA, Tag: uint8(lcommon.RedeemerTagMint), Index: 0},
		{TxHash: txB, Tag: uint8(lcommon.RedeemerTagReward), Index: 2},
		{TxHash: txA, Tag: uint8(lcommon.RedeemerTagSpend), Index: 0},
	}
	got, err := buildScriptRedeemerInfos(context.Background(), refs, source)
	require.NoError(t, err)
	require.Equal(t, []ScriptRedeemerInfo{
		{
			TxHash:           hex.EncodeToString(txA),
			TxIndex:          1,
			Purpose:          "spend",
			RedeemerDataHash: "r1",
			UnitMem:          "4",
			UnitSteps:        "5",
			Fee:              "6",
		},
		{
			TxHash:           hex.EncodeToString(txA),
			TxIndex:          0,
			Purpose:          "mint",
			RedeemerDataHash: "r2",
			UnitMem:          "7",
			UnitSteps:        "8",
			Fee:              "9",
		},
		{
			TxHash:           hex.EncodeToString(txB),
			TxIndex:          2,
			Purpose:          "reward",
			RedeemerDataHash: "r3",
			UnitMem:          "0",
			UnitSteps:        "0",
			Fee:              "0",
		},
		{
			TxHash:           hex.EncodeToString(txA),
			TxIndex:          0,
			Purpose:          "spend",
			RedeemerDataHash: "r0",
			DatumHash:        &datum,
			UnitMem:          "1",
			UnitSteps:        "2",
			Fee:              "3",
		},
	}, got)
	require.Equal(t, 1, source.calls[hex.EncodeToString(txA)],
		"each transaction is resolved once however many redeemers it holds")

	// A redeemer the index names but the transaction does not carry means the
	// index and the transaction disagree; it must not be skipped silently.
	_, err = buildScriptRedeemerInfos(
		context.Background(),
		[]models.ScriptRedeemerRef{
			{TxHash: txB, Tag: uint8(lcommon.RedeemerTagSpend), Index: 9},
		},
		source,
	)
	require.Error(t, err)

	source.err = errors.New("boom")
	_, err = buildScriptRedeemerInfos(context.Background(), refs, source)
	require.ErrorContains(t, err, "boom")
}

func TestNodeAdapterScriptUTXOs(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newSlotTimeDBBackedAdapter(t)
	script := bytes.Repeat([]byte{0x91}, lcommon.AddressHashSize)
	addr, _, _ := testKeyAddress(t, 0x92, 0)
	policy := bytes.Repeat([]byte{0x93}, lcommon.AddressHashSize)
	datumHash := bytes.Repeat([]byte{0xd1}, 32)

	firstTx := seedAssetTransaction(t, raw, db, fill32(0x01), 1, 10, 0)
	secondTx := seedAssetTransaction(t, raw, db, fill32(0x02), 2, 20, 0)
	spentTx := seedAssetTransaction(t, raw, db, fill32(0x03), 3, 30, 0)
	otherTx := seedAssetTransaction(t, raw, db, fill32(0x04), 4, 40, 0)
	spender := &models.Transaction{Hash: fill32(0x05), Slot: 50, Valid: true}
	insertAdapterTransaction(t, raw, spender)
	for i, tx := range []*models.Transaction{firstTx, secondTx, spentTx, otherTx} {
		utxo := &models.Utxo{
			TransactionID: &tx.ID,
			TxId:          tx.Hash,
			OutputIdx:     0,
			PaymentKey:    addr.PaymentKeyHash().Bytes(),
			AddedSlot:     tx.Slot,
			Amount:        types.Uint64(1_000_000 * uint64(i+1)),
		}
		if tx == firstTx {
			utxo.Assets = []models.Asset{
				{PolicyId: policy, Name: []byte("T"), Amount: 2},
			}
		}
		insertAdapterUtxo(t, raw, utxo)
		storePointerOutputCbor(t, db, tx.Hash, 0, addr, uint64(utxo.Amount))
		if tx == firstTx {
			// insertAdapterUtxo has no datum hash column.
			_, err := raw.Exec(
				`UPDATE utxo SET datum_hash = ? WHERE tx_id = ?`,
				datumHash, tx.Hash,
			)
			require.NoError(t, err)
		}
		var utxoID int64
		require.NoError(t, raw.QueryRow(
			`SELECT id FROM utxo WHERE tx_id = ?`, tx.Hash,
		).Scan(&utxoID))
		holds := script
		if tx == otherTx {
			holds = bytes.Repeat([]byte{0x99}, lcommon.AddressHashSize)
		}
		_, err := raw.Exec(
			`INSERT INTO utxo_reference_script (utxo_id, script_hash) VALUES (?, ?)`,
			utxoID,
			holds,
		)
		require.NoError(t, err)
		if tx == spentTx {
			_, err := raw.Exec(
				`UPDATE utxo SET spent_at_tx_id = ?, deleted_slot = 51 WHERE id = ?`,
				spender.Hash,
				utxoID,
			)
			require.NoError(t, err)
		}
	}

	ctx := context.Background()
	rows, total, err := adapter.ScriptUTXOs(
		ctx, script,
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 2)
	require.Equal(t, hex.EncodeToString(firstTx.Hash), rows[0].TxHash)
	require.Equal(t, addr.String(), rows[0].Address)
	require.Equal(t, uint32(0), rows[0].OutputIndex)
	require.Equal(t, hex.EncodeToString(firstTx.BlockHash), rows[0].Block)
	require.Equal(t, hex.EncodeToString(script), rows[0].ReferenceScriptHash)
	require.NotNil(t, rows[0].DataHash)
	require.Equal(t, hex.EncodeToString(datumHash), *rows[0].DataHash)
	require.Nil(t, rows[0].InlineDatum)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "1000000"},
		{
			Unit: hex.EncodeToString(
				policy,
			) + hex.EncodeToString(
				[]byte("T"),
			),
			Quantity: "2",
		},
	}, rows[0].Amount)
	require.Equal(t, hex.EncodeToString(secondTx.Hash), rows[1].TxHash)
	require.Nil(t, rows[1].DataHash)

	rows, total, err = adapter.ScriptUTXOs(
		ctx, script,
		PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(secondTx.Hash), rows[0].TxHash)

	rows, total, err = adapter.ScriptUTXOs(
		ctx, script,
		PaginationParams{Count: 10, Page: 4, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Empty(t, rows)

	// A known script nothing holds lists empty; an unknown one is not found.
	known := bytes.Repeat([]byte{0xed}, 28)
	insertScript(t, raw, known, []byte{0x41, 0x00}, 2)
	rows, total, err = adapter.ScriptUTXOs(
		ctx, known,
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, rows)
	_, _, err = adapter.ScriptUTXOs(
		ctx, bytes.Repeat([]byte{0xee}, 28),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, ErrScriptNotFound)
}

// A spend redeemer's index points into the transaction's inputs sorted by
// (transaction id, output index), not into their order in the body.
func TestTransactionRedeemerMetadataUsesSortedInputs(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	scriptHash := bytes.Repeat([]byte{0x9a}, lcommon.AddressHashSize)
	scriptAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash,
		nil,
	)
	require.NoError(t, err)
	keyAddr, _, _ := testKeyAddress(t, 0x9b, 0)
	spent := fill32(0x9c)
	storePointerOutputCbor(t, db, spent, 0, scriptAddr, 1_000_000)
	storePointerOutputCbor(t, db, spent, 1, keyAddr, 1_000_000)

	scriptInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(spent).WithIndex(0).Build()
	require.NoError(t, err)
	keyInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(spent).WithIndex(1).Build()
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(keyAddr.String()).WithLovelace(1_000_000).Build()
	require.NoError(t, err)
	decoded, err := mockledger.NewTransactionBuilder().
		WithId(fill32(0x9d)).
		WithInputs(keyInput, scriptInput).
		WithOutputs(output).
		Build()
	require.NoError(t, err)
	row := &models.Transaction{
		Inputs: []models.Utxo{
			{TxId: spent, OutputIdx: 1},
			{TxId: spent, OutputIdx: 0},
		},
		Redeemers: []models.Redeemer{
			{Tag: uint8(lcommon.RedeemerTagSpend), Index: 0},
		},
	}

	metadata, err := adapter.transactionRedeemerMetadata(
		fill32(0x9d), row, decoded, lcommon.ProtocolVersionConway,
	)
	require.NoError(t, err)
	key := lcommon.RedeemerKey{Tag: lcommon.RedeemerTagSpend, Index: 0}
	require.Contains(t, metadata, key)
	require.Equal(t, hex.EncodeToString(scriptHash), metadata[key].ScriptHash)
}
