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
	"context"
	"database/sql"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// seedChain stores count generated Conway blocks with transactions at heights
// 1..count and returns them. Block ids follow the store's 1-based index.
func seedChain(
	t *testing.T,
	db *database.Database,
	count int,
) []gledger.Block {
	t.Helper()
	chain, err := testfixtures.GenerateConwayChainWithTransactions(count)
	require.NoError(t, err)
	for _, block := range chain {
		require.NoError(t, db.BlockCreate(models.Block{
			Hash:     block.Hash().Bytes(),
			PrevHash: block.PrevHash().Bytes(),
			Slot:     block.SlotNumber(),
			Number:   block.BlockNumber(),
			ID:       block.BlockNumber() + database.BlockInitialIndex,
			Type:     uint(block.Type()),
			Cbor:     block.Cbor(),
		}, nil))
	}
	return chain
}

func heightsOf(blocks []models.Block) []uint64 {
	ret := make([]uint64, 0, len(blocks))
	for _, block := range blocks {
		ret = append(ret, block.Number)
	}
	return ret
}

func TestNodeAdapterNeighborBlocks(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	chain := seedChain(t, db, 5)
	hashOf := func(height int) string { return chain[height-1].Hash().String() }
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		id      string
		forward bool
		count   int
		page    int
		want    []uint64
	}{
		{"next by hash", hashOf(2), true, 2, 1, []uint64{3, 4}},
		{"next by height", "2", true, 2, 1, []uint64{3, 4}},
		{"next second page", "2", true, 2, 2, []uint64{5}},
		{"next past tip", "2", true, 2, 3, []uint64{}},
		{"next of tip", "5", true, 5, 1, []uint64{}},
		{"previous by hash", hashOf(4), false, 2, 1, []uint64{3, 2}},
		{"previous by height", "4", false, 2, 1, []uint64{3, 2}},
		{"previous second page", "4", false, 2, 2, []uint64{1}},
		{"previous of first", "1", false, 5, 1, []uint64{}},
		{"previous page past start", "4", false, 2, 5, []uint64{}},
		{"default count", "1", true, 100, 1, []uint64{2, 3, 4, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			blocks, err := adapter.neighborBlocks(
				ctx,
				tc.id,
				PaginationParams{Count: tc.count, Page: tc.page},
				tc.forward,
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, heightsOf(blocks))
		})
	}

	t.Run("invalid id", func(t *testing.T) {
		t.Parallel()
		_, err := adapter.neighborBlocks(
			ctx, "not-a-block", PaginationParams{Count: 1, Page: 1}, true,
		)
		require.ErrorIs(t, err, ErrInvalidBlockID)
	})
	t.Run("unknown block", func(t *testing.T) {
		t.Parallel()
		_, err := adapter.neighborBlocks(
			ctx, "99", PaginationParams{Count: 1, Page: 1}, true,
		)
		require.ErrorIs(t, err, ErrBlockNotFound)
		_, err = adapter.neighborBlocks(
			ctx,
			hex.EncodeToString(fill32(0xee)),
			PaginationParams{Count: 1, Page: 1},
			false,
		)
		require.ErrorIs(t, err, ErrBlockNotFound)
	})
	t.Run("page offset overflow is empty", func(t *testing.T) {
		t.Parallel()
		blocks, err := adapter.neighborBlocks(
			ctx,
			"2",
			PaginationParams{Count: 100, Page: MaxPaginationPage},
			true,
		)
		require.NoError(t, err)
		require.Empty(t, blocks)
	})
}

func TestNodeAdapterBlockAtSlot(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	chain := seedChain(t, db, 3)
	ctx := context.Background()

	block, err := adapter.blockAtSlot(ctx, chain[1].SlotNumber())
	require.NoError(t, err)
	require.Equal(t, chain[1].Hash().Bytes(), block.Hash)

	_, err = adapter.blockAtSlot(ctx, chain[1].SlotNumber()+1)
	require.ErrorIs(t, err, ErrBlockNotFound)
}

func TestNodeAdapterBlockAtEpochSlot(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	chain := seedChain(t, db, 3)
	ctx := context.Background()
	// Epoch 4 starts at the second block's slot minus 7 and is 100 slots long.
	start := chain[1].SlotNumber() - 7
	require.NoError(t, db.SetEpoch(
		start, 4, nil, nil, nil, nil, 6, 1000, 100, nil,
	))

	block, err := adapter.blockAtEpochSlot(ctx, 4, 7)
	require.NoError(t, err)
	require.Equal(t, chain[1].Hash().Bytes(), block.Hash)

	_, err = adapter.blockAtEpochSlot(ctx, 4, 8)
	require.ErrorIs(t, err, ErrBlockNotFound, "empty slot")
	_, err = adapter.blockAtEpochSlot(ctx, 4, 100)
	require.ErrorIs(t, err, ErrBlockNotFound, "slot past the epoch end")
	_, err = adapter.blockAtEpochSlot(ctx, 9, 0)
	require.ErrorIs(t, err, ErrBlockNotFound, "unknown epoch")
}

func TestNodeAdapterBlockTransactions(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	chain := seedChain(t, db, 3)
	ctx := context.Background()
	for _, block := range chain {
		txs := block.Transactions()
		require.NotEmpty(t, txs)
		id := strconv.FormatUint(block.BlockNumber(), 10)

		hashes, err := adapter.BlockTransactionHashes(ctx, id)
		require.NoError(t, err)
		require.Len(t, hashes, len(txs))
		cbors, err := adapter.BlockTransactionCBOR(ctx, block.Hash().String())
		require.NoError(t, err)
		require.Len(t, cbors, len(txs))
		for i, tx := range txs {
			require.Equal(t, tx.Hash().String(), hashes[i])
			require.Equal(t, tx.Hash().String(), cbors[i].TxHash)
			require.Equal(t, hex.EncodeToString(tx.Cbor()), cbors[i].CBOR)
		}
	}
	_, err := adapter.BlockTransactionHashes(ctx, "99")
	require.ErrorIs(t, err, ErrBlockNotFound)
	_, err = adapter.BlockTransactionCBOR(ctx, "zz")
	require.ErrorIs(t, err, ErrInvalidBlockID)
}

// insertSpentAdapterUtxo stores a spent output so a transaction row resolves
// it as an input through spent_at_tx_id.
func insertSpentAdapterUtxo(
	t *testing.T,
	raw *sql.DB,
	utxo *models.Utxo,
	spentBy []byte,
) {
	t.Helper()
	insertAdapterUtxo(t, raw, utxo)
	_, err := raw.Exec(
		`UPDATE utxo SET spent_at_tx_id = ?, deleted_slot = ? WHERE tx_id = ? AND output_idx = ?`,
		spentBy, utxo.AddedSlot+1, utxo.TxId, utxo.OutputIdx,
	)
	require.NoError(t, err)
}

func TestNodeAdapterBlockAddresses(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	chain := seedChain(t, db, 2)
	ctx := context.Background()
	block := chain[0]
	txs := block.Transactions()
	require.NotEmpty(t, txs)

	// Register the block's transactions and give the first one a spent input.
	payment := fill32(0x31)[:28]
	staking := fill32(0x32)[:28]
	inputAddr, err := gledger.NewAddressFromParts(
		gledger.AddressTypeKeyKey,
		adapter.networkID(),
		payment,
		staking,
	)
	require.NoError(t, err)
	for i, tx := range txs {
		insertAdapterTransaction(t, raw, &models.Transaction{
			Hash:       tx.Hash().Bytes(),
			BlockHash:  block.Hash().Bytes(),
			BlockIndex: uint32(i),
			Slot:       block.SlotNumber(),
			Valid:      tx.IsValid(),
		})
	}
	insertSpentAdapterUtxo(t, raw, &models.Utxo{
		TxId:          fill32(0x41),
		OutputIdx:     0,
		PaymentKey:    payment,
		StakingKey:    staking,
		CredentialTag: 0,
		AddedSlot:     1,
	}, txs[0].Hash().Bytes())

	got, err := adapter.BlockAddresses(ctx, block.Hash().String())
	require.NoError(t, err)

	// Expected: every output address of the block plus the resolved input
	// address, each with the hashes of the transactions touching it, sorted
	// by address.
	want := map[string][]string{}
	add := func(address, hash string) {
		for _, existing := range want[address] {
			if existing == hash {
				return
			}
		}
		want[address] = append(want[address], hash)
	}
	for _, tx := range txs {
		for _, output := range tx.Outputs() {
			add(output.Address().String(), tx.Hash().String())
		}
	}
	add(inputAddr.String(), txs[0].Hash().String())
	require.Len(t, got, len(want))
	for i, row := range got {
		require.Equal(t, want[row.Address], row.TxHashes, row.Address)
		if i > 0 {
			require.Less(t, got[i-1].Address, row.Address)
		}
	}
	require.Contains(t, want, inputAddr.String())

	_, err = adapter.BlockAddresses(ctx, "99")
	require.ErrorIs(t, err, ErrBlockNotFound)
}

func TestTransactionAddressEffects(t *testing.T) {
	t.Parallel()
	const (
		outAddr = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"
		colAddr = "addr1vx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzers66hrl8"
	)
	build := func(valid bool) lcommon.Transaction {
		regular, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(outAddr).WithLovelace(5_000_000).Build()
		require.NoError(t, err)
		collateral, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(colAddr).WithLovelace(2_000_000).Build()
		require.NoError(t, err)
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(fill32(0x62)).WithIndex(0).Build()
		require.NoError(t, err)
		tx, err := mockledger.NewTransactionBuilder().
			WithId(fill32(0x61)).
			WithInputs(input).
			WithOutputs(regular).
			WithValid(valid).
			Build()
		require.NoError(t, err)
		mock, ok := tx.(*mockledger.MockTransaction)
		require.True(t, ok)
		return mock.WithCollateralReturn(collateral)
	}
	row := models.Transaction{
		Inputs:     []models.Utxo{{TxId: fill32(0x01)}},
		Collateral: []models.Utxo{{TxId: fill32(0x02)}},
	}

	outputs, spent := transactionAddressEffects(build(true), row)
	require.Equal(t, []string{outAddr}, outputs)
	require.Equal(t, row.Inputs, spent)

	outputs, spent = transactionAddressEffects(build(false), row)
	require.Equal(t, []string{colAddr}, outputs)
	require.Equal(t, row.Collateral, spent)
}
