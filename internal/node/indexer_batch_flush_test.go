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

package node

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// stakedPayment is a one-output Conway transaction paying amount to address.
// A non-empty label0674 text becomes its metadata under label 674; the
// fixture encoder drops a metadatum set on the struct, so the auxiliary data
// is spliced into the encoded transaction.
func stakedPayment(
	t *testing.T,
	seed byte,
	address common.Address,
	amount uint64,
	label0674 string,
) *conway.ConwayTransaction {
	t.Helper()
	inputHash := common.Blake2b256{}
	inputHash[0] = seed
	tx := conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{{TxId: inputHash}},
			),
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAddress: address,
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: amount,
				},
			}},
			TxFee: 1,
		},
		TxIsValid: true,
	}
	if label0674 == "" {
		return &tx
	}
	encoded, err := cbor.Encode(&tx)
	require.NoError(t, err)
	// The encoding ends with the null auxiliary data field, 0xf6.
	require.Equal(t, byte(0xf6), encoded[len(encoded)-1])
	aux, err := cbor.Encode(map[uint64]string{674: label0674})
	require.NoError(t, err)
	withAux := append(slices.Clone(encoded[:len(encoded)-1]), aux...)
	var decoded conway.ConwayTransaction
	require.NoError(t, decoded.UnmarshalCBOR(withAux))
	require.NotNil(t, decoded.Metadata())
	return &decoded
}

// indexTransactionBlocks writes one block per transaction to the ledger's
// database and runs the IndexerDB writer over them in a single batch.
func indexTransactionBlocks(
	t *testing.T,
	txs []*conway.ConwayTransaction,
) *database.Database {
	t.Helper()
	ctx := context.Background()
	db := newIndexerTestDB(t)
	consensus := consensusOf(t, db)

	prev := common.Blake2b256{}
	var last models.Block
	for i, tx := range txs {
		blk, err := fixtures.NewBlockBuilder(ledger.GetEraById(6)).
			WithBlockNumber(uint64(i + 1)).
			WithSlot(uint64(100 + i)).
			WithPreviousHash(prev).
			WithTransactions(tx).
			Build()
		require.NoError(t, err)
		last = models.Block{
			Slot: blk.SlotNumber(), Hash: blk.Hash().Bytes(),
			PrevHash: blk.PrevHash().Bytes(), Number: blk.BlockNumber(),
			Cbor: blk.Cbor(), Type: uint(blk.Type()),
		}
		require.NoError(t, consensus.BlockCreate(last, nil))
		prev = blk.Hash()
	}
	require.NoError(t, consensus.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))

	store, err := indexerdb.NewStore(db, db, 10)
	require.NoError(t, err)
	bf := NewBackfill(db, nil, slog.Default())
	bf.SetDelegatorInactivityEnabled(false)
	w := indexer.NewWriter(
		store, indexer.NewDatabaseSource(consensus), bf, slog.Default(),
		len(txs)+1,
	)
	require.NoError(t, w.CatchUp(ctx))
	return db
}

// The writer queues each block's live-stake changes and writes them when it
// flushes the batch, so a credential paid by several blocks of one batch must
// come out with the sum of them.
func TestIndexerWriterFlushesCoalescedStakeDeltas(t *testing.T) {
	t.Parallel()
	stakingKey := common.NewBlake2b224([]byte("indexer-flush-stake-key"))
	paymentKey := common.NewBlake2b224([]byte("indexer-flush-payment-key"))
	address, err := common.NewAddressFromParts(
		common.AddressTypeKeyKey,
		common.AddressNetworkTestnet,
		paymentKey.Bytes(),
		stakingKey.Bytes(),
	)
	require.NoError(t, err)
	amounts := []uint64{3_000_000, 2_000_000, 4_000_000}
	var txs []*conway.ConwayTransaction
	for i, amount := range amounts {
		txs = append(txs, stakedPayment(t, byte(i+1), address, amount, ""))
	}

	db := indexTransactionBlocks(t, txs)

	raw := openRaw(t, filepath.Join(db.Config().DataDir, "metadata.sqlite"))
	var utxoStake sql.NullString
	err = raw.QueryRow(
		`SELECT utxo_stake FROM reward_live_stake WHERE staking_key = ?`,
		stakingKey.Bytes(),
	).Scan(&utxoStake)
	require.NoError(t, err, "no live stake row for the paid credential")
	require.True(t, utxoStake.Valid)
	require.Equal(t, "9000000", utxoStake.String)
}

// The writer's transaction metadata rows wait in the batch until it is
// flushed; the flush must write one row per label of each transaction.
func TestIndexerWriterFlushesTransactionMetadataLabels(t *testing.T) {
	t.Parallel()
	address, err := common.NewAddressFromParts(
		common.AddressTypeKeyNone,
		common.AddressNetworkTestnet,
		common.NewBlake2b224([]byte("indexer-flush-metadata")).Bytes(),
		nil,
	)
	require.NoError(t, err)
	txs := []*conway.ConwayTransaction{
		stakedPayment(t, 1, address, 1_000_000, "first"),
		stakedPayment(t, 2, address, 1_000_000, ""),
		stakedPayment(t, 3, address, 1_000_000, "third"),
	}

	db := indexTransactionBlocks(t, txs)

	got, err := db.GetTransactionsByMetadataLabel(
		context.Background(), 674, 10, 0, false, nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, txs[0].Hash().Bytes(), got[0].Hash)
	require.Equal(t, txs[2].Hash().Bytes(), got[1].Hash)
}
