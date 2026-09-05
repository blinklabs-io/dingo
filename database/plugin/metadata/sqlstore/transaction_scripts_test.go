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

package sqlstore

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestStoreTransactionIndexedScriptsIncludesReferenceScripts(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	script := lcommon.PlutusV2Script{0x41, 0x00}
	scriptRefCBOR, err := cbor.Encode(&lcommon.ScriptRef{
		Type:   lcommon.ScriptRefTypePlutusV2,
		Script: script,
	})
	require.NoError(t, err)
	produced, err := mockledger.NewUtxoBuilder().
		WithTxId(bytes.Repeat([]byte{0x11}, 32)).
		WithIndex(0).
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		WithScriptRef(scriptRefCBOR).
		Build()
	require.NoError(t, err)
	tx, err := mockledger.NewTransactionBuilder().
		WithId(bytes.Repeat([]byte{0x22}, 32)).
		WithInputs(produced.Id).
		WithOutputs(produced.Output).
		Build()
	require.NoError(t, err)

	const slot = uint64(42)
	require.NoError(t, storeTransactionIndexedScripts(
		t.Context(),
		store.writeDB,
		tx,
		slot,
	))
	got, err := store.GetScript(script.Hash(), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, script.Hash().Bytes(), got.Hash)
	require.Equal(t, script.RawScriptBytes(), got.Content)
	require.Equal(t, uint8(lcommon.ScriptRefTypePlutusV2), got.Type)
	require.Equal(t, slot, got.CreatedSlot)
}

func TestStoreTransactionWitnessesIncludesPlutusV4(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	script := lcommon.PlutusV4Script{0x41, 0x00}
	result, err := store.writeDB.ExecContext(
		t.Context(),
		`INSERT INTO "transaction" (hash) VALUES (?)`,
		bytes.Repeat([]byte{0x33}, 32),
	)
	require.NoError(t, err)
	transactionID, err := result.LastInsertId()
	require.NoError(t, err)
	tx := &dijkstra.DijkstraTransaction{
		TxIsValid: true,
		WitnessSet: dijkstra.DijkstraTransactionWitnessSet{
			WsPlutusV4Scripts: cbor.NewSetType(
				[]lcommon.PlutusV4Script{script},
				false,
			),
		},
	}

	const slot = uint64(43)
	require.NoError(t, storeTransactionWitnesses(
		t.Context(),
		store.writeDB,
		transactionID,
		tx,
		slot,
	))
	var witnessHash []byte
	var witnessType int64
	require.NoError(t, store.writeDB.QueryRowContext(
		t.Context(),
		`SELECT script_hash, type
FROM witness_scripts
WHERE transaction_id = ?`,
		transactionID,
	).Scan(&witnessHash, &witnessType))
	require.Equal(t, script.Hash().Bytes(), witnessHash)
	require.Equal(t, int64(lcommon.ScriptRefTypePlutusV4), witnessType)
	got, err := store.GetScript(script.Hash(), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, script.RawScriptBytes(), got.Content)
	require.Equal(t, uint8(lcommon.ScriptRefTypePlutusV4), got.Type)
	require.Equal(t, slot, got.CreatedSlot)
}

func TestStoreTransactionWitnessesIncludesDijkstraSubTransactions(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	sharedScript := lcommon.PlutusV4Script{0x41, 0x00}
	nestedScript := lcommon.PlutusV4Script{0x41, 0x01}
	sharedDatumRaw := []byte{0xd8, 0x79, 0x80}
	nestedDatumRaw := []byte{0xd8, 0x79, 0x81, 0x01}
	var sharedDatum lcommon.Datum
	require.NoError(t, sharedDatum.UnmarshalCBOR(sharedDatumRaw))
	var nestedDatum lcommon.Datum
	require.NoError(t, nestedDatum.UnmarshalCBOR(nestedDatumRaw))
	result, err := store.writeDB.ExecContext(
		t.Context(),
		`INSERT INTO "transaction" (hash) VALUES (?)`,
		bytes.Repeat([]byte{0x34}, 32),
	)
	require.NoError(t, err)
	transactionID, err := result.LastInsertId()
	require.NoError(t, err)
	tx := &dijkstra.DijkstraTransaction{
		TxIsValid: true,
		Body: dijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]dijkstra.DijkstraSubTransaction{
					{
						WitnessSet: dijkstra.DijkstraTransactionWitnessSet{
							WsPlutusV4Scripts: cbor.NewSetType(
								[]lcommon.PlutusV4Script{
									sharedScript,
									nestedScript,
								},
								false,
							),
							WsPlutusData: cbor.NewSetType(
								[]lcommon.Datum{
									sharedDatum,
									nestedDatum,
								},
								false,
							),
						},
					},
				},
				false,
			),
		},
		WitnessSet: dijkstra.DijkstraTransactionWitnessSet{
			WsPlutusV4Scripts: cbor.NewSetType(
				[]lcommon.PlutusV4Script{sharedScript},
				false,
			),
			WsPlutusData: cbor.NewSetType(
				[]lcommon.Datum{sharedDatum},
				false,
			),
		},
	}

	const slot = uint64(45)
	require.NoError(t, storeTransactionWitnesses(
		t.Context(),
		store.writeDB,
		transactionID,
		tx,
		slot,
	))
	var witnessScriptCount int
	require.NoError(t, store.writeDB.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*) FROM witness_scripts WHERE transaction_id = ?`,
		transactionID,
	).Scan(&witnessScriptCount))
	require.Equal(t, 2, witnessScriptCount)
	var plutusDataCount int
	require.NoError(t, store.writeDB.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*) FROM plutus_data WHERE transaction_id = ?`,
		transactionID,
	).Scan(&plutusDataCount))
	require.Equal(t, 2, plutusDataCount)

	gotScript, err := store.GetScript(nestedScript.Hash(), nil)
	require.NoError(t, err)
	require.NotNil(t, gotScript)
	require.Equal(t, nestedScript.RawScriptBytes(), gotScript.Content)
	require.Equal(t, uint8(lcommon.ScriptRefTypePlutusV4), gotScript.Type)

	require.NoError(t, storeTransactionDatumIndex(
		t.Context(),
		store.writeDB,
		tx,
		slot,
	))
	gotDatum, err := store.GetDatum(
		lcommon.Blake2b256Hash(nestedDatumRaw),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotDatum)
	require.Equal(t, nestedDatumRaw, gotDatum.RawDatum)
}

func TestStoreTransactionIndexesDijkstraSubTransactionOutputs(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	script := lcommon.PlutusV4Script{0x41, 0x02}
	scriptRefCBOR, err := cbor.Encode(&lcommon.ScriptRef{
		Type:   lcommon.ScriptRefTypePlutusV4,
		Script: script,
	})
	require.NoError(t, err)
	datumRaw := []byte{0xd8, 0x79, 0x81, 0x02}
	produced, err := mockledger.NewUtxoBuilder().
		WithTxId(bytes.Repeat([]byte{0x35}, 32)).
		WithIndex(0).
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		WithDatum(datumRaw).
		WithScriptRef(scriptRefCBOR).
		Build()
	require.NoError(t, err)
	tx := &dijkstra.DijkstraTransaction{
		TxIsValid: true,
		Body: dijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]dijkstra.DijkstraSubTransaction{
					{
						Body: dijkstra.DijkstraSubTransactionBody{
							TxOutputs: []dijkstra.DijkstraTransactionOutput{
								{Output: produced.Output},
							},
						},
					},
				},
				false,
			),
		},
	}

	const slot = uint64(46)
	require.NoError(t, storeTransactionIndexedScripts(
		t.Context(),
		store.writeDB,
		tx,
		slot,
	))
	gotScript, err := store.GetScript(script.Hash(), nil)
	require.NoError(t, err)
	require.NotNil(t, gotScript)
	require.Equal(t, script.RawScriptBytes(), gotScript.Content)
	require.Equal(t, uint8(lcommon.ScriptRefTypePlutusV4), gotScript.Type)

	require.NoError(t, storeTransactionDatumIndex(
		t.Context(),
		store.writeDB,
		tx,
		slot,
	))
	gotDatum, err := store.GetDatum(lcommon.Blake2b256Hash(datumRaw), nil)
	require.NoError(t, err)
	require.NotNil(t, gotDatum)
	require.Equal(t, datumRaw, gotDatum.RawDatum)
}

func TestStoreTransactionDatumIndexIncludesInvalidWitnesses(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	raw := []byte{0xd8, 0x79, 0x80}
	var datum lcommon.Datum
	require.NoError(t, datum.UnmarshalCBOR(raw))
	tx := &dijkstra.DijkstraTransaction{
		TxIsValid: false,
		WitnessSet: dijkstra.DijkstraTransactionWitnessSet{
			WsPlutusData: cbor.NewSetType(
				[]lcommon.Datum{datum},
				false,
			),
		},
	}

	const slot = uint64(44)
	require.NoError(t, storeTransactionDatumIndex(
		t.Context(),
		store.writeDB,
		tx,
		slot,
	))
	got, err := store.GetDatum(lcommon.Blake2b256Hash(raw), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, raw, got.RawDatum)
	require.Equal(t, slot, got.AddedSlot)
}
