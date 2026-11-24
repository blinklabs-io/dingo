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

package dingo

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// historyExpiryLiveSource stands for the ledger's live UTxO set, which holds
// the ledger's outputs itself.
type historyExpiryLiveSource struct{}

func (historyExpiryLiveSource) LiveUtxoCbor(
	types.Txn, []byte, uint32,
) ([]byte, bool, error) {
	return nil, false, nil
}

func (historyExpiryLiveSource) LiveUtxoStake(
	types.Txn, uint8, []byte,
) (uint64, error) {
	return 0, nil
}

func (historyExpiryLiveSource) LivePointerStakes(
	types.Txn,
) ([]types.PointerStake, error) {
	return nil, nil
}

func (historyExpiryLiveSource) EachLiveUtxoStake(
	types.Txn, func(uint8, []byte, uint64) error,
) error {
	return nil
}

// An IndexerDB output is an offset into a block of the ledger's block store.
// History expiry of that block leaves the output's CBOR readable from the
// IndexerDB, spent or not, as an API node kept it before the IndexerDB.
func TestHistoryExpiryKeepsIndexerOutputsReadable(t *testing.T) {
	t.Parallel()
	ledgerDB, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	require.NoError(t, ledgerDB.SetLiveUtxoSource(historyExpiryLiveSource{}))
	idx, err := dbtest.NewIndexerDatabase(t, ledgerDB, dbtest.IndexerOptions{})
	require.NoError(t, err)

	const slot uint64 = 100
	payload := []byte("indexer-output-cbor")
	blockHash := bytes.Repeat([]byte{0xb7}, 32)
	require.NoError(t, ledgerDB.BlockCreate(models.Block{
		Slot: slot, Hash: blockHash, Cbor: payload, Type: 1,
	}, nil))
	txID := bytes.Repeat([]byte{0xa7}, 32)
	var hashArr [32]byte
	copy(hashArr[:], blockHash)
	offset := database.EncodeUtxoOffset(&database.CborOffset{
		BlockSlot:  slot,
		BlockHash:  hashArr,
		ByteLength: uint32(len(payload)),
	})
	require.NoError(t, idx.BlobTxn(true).Do(func(txn *database.Txn) error {
		return idx.Blob().SetUtxo(txn.Blob(), txID, 0, offset)
	}))
	require.NoError(t, idx.MetadataTxn(t.Context(), true).Do(
		func(txn *database.Txn) error {
			return idx.Metadata().CreateUtxo(txn.Metadata(), &models.Utxo{
				TxId:        txID,
				AddedSlot:   slot,
				DeletedSlot: slot + 50,
				Amount:      types.Uint64(1),
				Address:     []byte("indexer-address"),
			})
		},
	))

	n := &Node{db: ledgerDB, indexerDB: idx}
	pruner := n.newHistoryExpiryPrunerConfig()
	require.NoError(t, pruner.BeforeExpire(t.Context(), slot, blockHash))
	_, err = ledgerDB.PruneBlock(t.Context(), slot, blockHash)
	require.NoError(t, err)

	txn := idx.BlobTxn(false)
	defer txn.Release()
	got, err := idx.Blob().GetUtxo(txn.Blob(), txID, 0)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
