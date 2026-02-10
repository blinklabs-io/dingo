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

package ledger

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestGenesisUtxoStorageAndRetrieval(t *testing.T) {
	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "genesis_utxo_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	// Create database
	dbConfig := &database.Config{
		DataDir:        tmpDir,
		Logger:         logger,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}
	db, err := database.New(dbConfig)
	require.NoError(t, err)
	defer db.Close()

	// Load cardano config from embedded preview network
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigPreviewNetworkFS,
	)
	require.NoError(t, err)

	// Get genesis UTxOs
	byronGenesis := nodeCfg.ByronGenesis()
	require.NotNil(t, byronGenesis)

	byronGenesisUtxos, err := byronGenesis.GenesisUtxos()
	require.NoError(t, err)
	t.Logf("Found %d Byron genesis UTxOs", len(byronGenesisUtxos))

	if len(byronGenesisUtxos) == 0 {
		t.Skip("No Byron genesis UTxOs to test")
	}

	// First, encode the genesis outputs to CBOR (like createGenesisBlock does)
	encodedUtxos := make([]lcommon.Utxo, len(byronGenesisUtxos))
	for i, utxo := range byronGenesisUtxos {
		// Encode the output to CBOR
		cborData, err := cbor.Encode(utxo.Output)
		require.NoError(t, err, "Failed to encode output %d", i)

		// Create a new Utxo with CBOR-encoded output
		switch output := utxo.Output.(type) {
		case byron.ByronTransactionOutput:
			newOutput := output
			(&newOutput).SetCbor(cborData)
			encodedUtxos[i] = lcommon.Utxo{
				Id:     utxo.Id,
				Output: newOutput,
			}
		default:
			t.Fatalf("Unexpected output type: %T", utxo.Output)
		}
		t.Logf("Encoded UTxO %x#%d: %d bytes",
			utxo.Id.Id().Bytes()[:8], utxo.Id.Index(), len(cborData))
	}

	// Get the Byron genesis hash to use as the synthetic block hash
	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)

	// Create a transaction to store genesis UTxOs
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Build and store genesis block CBOR
		utxoOffsets := make(map[database.UtxoRef]database.CborOffset)

		// Build synthetic genesis block CBOR (simplified version)
		// First, encode each UTxO to get its CBOR
		var blockCbor []byte

		// Track offsets as we build the block
		for _, utxo := range encodedUtxos {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			// Get the output CBOR
			outputCbor := utxo.Output.Cbor()
			if len(outputCbor) == 0 {
				return fmt.Errorf(
					"UTxO %x#%d still has no CBOR after encoding",
					txId[:8], outputIdx,
				)
			}

			var txHashArray [32]byte
			copy(txHashArray[:], txId)

			ref := database.UtxoRef{
				TxId:      txHashArray,
				OutputIdx: outputIdx,
			}

			// Track offset within block (simplified: just concatenate)
			offset := uint32(len(blockCbor))
			blockCbor = append(blockCbor, outputCbor...)

			utxoOffsets[ref] = database.CborOffset{
				BlockSlot:  0,
				BlockHash:  genesisHash,
				ByteOffset: offset,
				ByteLength: uint32(len(outputCbor)),
			}

			t.Logf("UTxO %x#%d: offset=%d, length=%d",
				txId[:8], outputIdx, offset, len(outputCbor))
		}

		// Store the genesis block CBOR
		t.Logf("Storing genesis block CBOR: %d bytes", len(blockCbor))
		if err := db.SetGenesisCbor(0, genesisHash[:], blockCbor, txn); err != nil {
			return err
		}

		// Now store each genesis transaction
		for _, utxo := range encodedUtxos {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			var txHashArray [32]byte
			copy(txHashArray[:], txId)

			ref := database.UtxoRef{
				TxId:      txHashArray,
				OutputIdx: outputIdx,
			}

			offset, ok := utxoOffsets[ref]
			if !ok {
				return fmt.Errorf(
					"no offset for UTxO %x#%d after building block",
					txId[:8], outputIdx,
				)
			}

			// Store the offset
			offsetData := database.EncodeUtxoOffset(&offset)
			t.Logf(
				"Storing UTxO offset: %s",
				hex.EncodeToString(offsetData[:20]),
			)

			blob := db.Blob()
			if blob == nil {
				return fmt.Errorf("blob store is nil")
			}
			blobTxn := txn.Blob()
			if blobTxn == nil {
				return fmt.Errorf("blob transaction is nil")
			}

			if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)

	// Now try to retrieve each genesis UTxO
	t.Log("Retrieving genesis UTxOs...")
	for _, utxo := range byronGenesisUtxos {
		txId := utxo.Id.Id().Bytes()
		outputIdx := utxo.Id.Index()

		// Try to get the UTxO from blob store
		readTxn := db.Transaction(false)
		blob := db.Blob()
		require.NotNil(t, blob)
		blobTxn := readTxn.Blob()
		require.NotNil(t, blobTxn)

		data, err := blob.GetUtxo(blobTxn, txId, outputIdx)
		if err != nil {
			t.Errorf("Failed to get UTxO %s#%d from blob: %v",
				hex.EncodeToString(txId[:8]), outputIdx, err)
			readTxn.Rollback() //nolint:errcheck
			continue
		}

		t.Logf(
			"Retrieved data for %x#%d: %d bytes, first bytes: %s",
			txId[:8],
			outputIdx,
			len(data),
			hex.EncodeToString(data[:min(20, len(data))]),
		)

		// Check if it's an offset
		if database.IsUtxoOffsetStorage(data) {
			t.Logf("Data is offset storage (has DOFF magic)")

			// Decode the offset
			offset, err := database.DecodeUtxoOffset(data)
			require.NoError(
				t,
				err,
				"Failed to decode offset for %x#%d",
				txId[:8],
				outputIdx,
			)

			t.Logf(
				"Offset: slot=%d, hash=%x, offset=%d, length=%d",
				offset.BlockSlot,
				offset.BlockHash[:8],
				offset.ByteOffset,
				offset.ByteLength,
			)

			// Try to get the block
			blockCbor, _, err := blob.GetBlock(
				blobTxn,
				offset.BlockSlot,
				offset.BlockHash[:],
			)
			if err != nil {
				t.Errorf(
					"Failed to get block for offset: slot=%d, hash=%x, error=%v",
					offset.BlockSlot,
					offset.BlockHash[:8],
					err,
				)
				readTxn.Rollback() //nolint:errcheck
				continue
			}

			t.Logf("Got block: %d bytes", len(blockCbor))

			// Extract the UTxO CBOR
			end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
			if end > uint64(len(blockCbor)) {
				t.Errorf(
					"Offset out of bounds: offset=%d, length=%d, block_size=%d",
					offset.ByteOffset,
					offset.ByteLength,
					len(blockCbor),
				)
			} else {
				utxoCbor := blockCbor[offset.ByteOffset:end]
				t.Logf("Extracted UTxO CBOR: %d bytes, first bytes: %s",
					len(utxoCbor), hex.EncodeToString(utxoCbor[:min(20, len(utxoCbor))]))
			}
		} else {
			t.Logf("Data is raw CBOR (legacy format)")
		}

		readTxn.Rollback() //nolint:errcheck
	}
}
