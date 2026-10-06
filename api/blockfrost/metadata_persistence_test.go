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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// TestPersistedMetadataEndpointsDeterministicAcrossKeyOrders stores
// transactions through the API-mode persistence path, so the label index is
// written by the same code that indexes chain data, and checks that every
// metadata endpoint gives the same outcome for each key order on every call.
func TestPersistedMetadataEndpointsDeterministicAcrossKeyOrders(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeAPI,
	})
	require.NoError(t, err)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)

	// Label 721 maps integer 1 and text "1", which collide in JSON; label 1
	// is the representable text "a". The indefinite-length form re-encodes
	// differently, so it shows whether the stored label CBOR is the original.
	const (
		collideIntFirst  = "a20163696e7461316474657874"
		collideTextFirst = "a2613164746578740163696e74"
		collideIndef     = "bf0163696e7461316474657874ff"
	)
	txs := []struct {
		hashByte byte
		metadata string
		label721 string
	}{
		{0x61, "a21902d1" + collideIntFirst + "016161", collideIntFirst},
		{0x62, "a2016161" + "1902d1" + collideIntFirst, collideIntFirst},
		{0x63, "a21902d1" + collideTextFirst + "016161", collideTextFirst},
		{0x64, "a2016161" + "1902d1" + collideTextFirst, collideTextFirst},
		{0x65, "a2016161" + "1902d1" + collideIndef, collideIndef},
	}
	hashes := make([]string, len(txs))
	for i, mt := range txs {
		metadataCbor, err := hex.DecodeString(mt.metadata)
		require.NoError(t, err)
		hash := bytes.Repeat([]byte{mt.hashByte}, 32)
		hashes[i] = hex.EncodeToString(hash)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		input, err := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{mt.hashByte + 0x10}, 32),
			0,
		)
		require.NoError(t, err)
		txBuilder := mockledger.NewTransactionBuilder()
		txBuilder.WithId(hash)
		txBuilder.WithInputs(input)
		txBuilder.WithOutputs(output)
		txBuilder.WithType(gledger.TxTypeConway)
		txBuilder.WithValid(true)
		txBuilder.WithMetadata(metadataCbor)
		tx, err := txBuilder.Build()
		require.NoError(t, err)
		require.NoError(t, db.SetTransactionMetadataOnly(
			tx,
			ocommon.Point{
				Slot: uint64(100 + i),
				Hash: bytes.Repeat([]byte{0xb0 + byte(i)}, 32),
			},
			0,
			nil,
			nil,
		))
	}

	for pass := range 8 {
		for i, mt := range txs {
			hash := bytes.Repeat([]byte{mt.hashByte}, 32)

			jsonLabels, err := adapter.TransactionMetadata(hash)
			require.NoError(t, err, "pass %d tx %d", pass, i)
			require.Len(t, jsonLabels, 2)
			require.Equal(t, "1", jsonLabels[0].Label)
			require.Equal(t, `"a"`, string(jsonLabels[0].JSONMetadata))
			require.Equal(t, "721", jsonLabels[1].Label)
			require.Empty(
				t,
				jsonLabels[1].JSONMetadata,
				"pass %d tx %d",
				pass,
				i,
			)

			cborLabels, err := adapter.TransactionMetadataCBOR(hash)
			require.NoError(t, err)
			require.Len(t, cborLabels, 2)
			require.Equal(t, "6161", cborLabels[0].CBORMetadata)
			require.Equal(t, mt.label721, cborLabels[1].CBORMetadata)
		}

		params := PaginationParams{
			Count: 10,
			Page:  1,
			Order: PaginationOrderAsc,
		}
		for _, label := range []uint64{1, 721} {
			jsonRows, total, err := adapter.MetadataTransactions(label, params)
			require.NoError(t, err)
			require.Equal(t, len(txs), total, "label %d", label)
			require.Len(t, jsonRows, len(txs))
			cborRows, cborTotal, err := adapter.MetadataTransactionsCBOR(
				label,
				params,
			)
			require.NoError(t, err)
			require.Equal(t, len(txs), cborTotal, "label %d", label)
			require.Len(t, cborRows, len(txs))
			for i, mt := range txs {
				require.Equal(t, hashes[i], jsonRows[i].TxHash)
				require.Equal(t, hashes[i], cborRows[i].TxHash)
				if label == 1 {
					require.Equal(t, `"a"`, string(jsonRows[i].JSONMetadata))
					require.Equal(t, "6161", cborRows[i].Metadata)
				} else {
					require.Nil(t, jsonRows[i].JSONMetadata, "pass %d row %d", pass, i)
					require.Equal(t, mt.label721, cborRows[i].Metadata)
				}
			}
		}

		var seenJSON, seenCBOR []string
		for page := 1; page <= len(txs); page++ {
			pageParams := PaginationParams{
				Count: 1,
				Page:  page,
				Order: PaginationOrderAsc,
			}
			rows, _, err := adapter.MetadataTransactions(721, pageParams)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			seenJSON = append(seenJSON, rows[0].TxHash)
			cborRows, _, err := adapter.MetadataTransactionsCBOR(
				721,
				pageParams,
			)
			require.NoError(t, err)
			require.Len(t, cborRows, 1)
			seenCBOR = append(seenCBOR, cborRows[0].TxHash)
		}
		require.Equal(t, hashes, seenJSON)
		require.Equal(t, hashes, seenCBOR)
	}
}
