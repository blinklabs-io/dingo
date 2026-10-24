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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/mempool"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type fakeMempool struct {
	mu           sync.Mutex
	txs          []mempool.MempoolTransaction
	lastMaxItems int
	// byAddress is the address index the pool would answer from.
	byAddress map[string][]string
	// boundedReads and addressReads count full-pool snapshots and index
	// lookups.
	boundedReads int
	addressReads int
}

func (f *fakeMempool) TransactionHashesByAddress(address string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addressReads++
	return append([]string{}, f.byAddress[address]...)
}

func (f *fakeMempool) GetTransaction(
	hash string,
) (mempool.MempoolTransaction, bool) {
	for _, tx := range f.txs {
		if tx.Hash == hash {
			return tx, true
		}
	}
	return mempool.MempoolTransaction{}, false
}

func (f *fakeMempool) TransactionsBounded(
	maxItems int,
	_ int64,
) ([]mempool.MempoolTransaction, int) {
	f.mu.Lock()
	f.lastMaxItems = maxItems
	f.boundedReads++
	f.mu.Unlock()
	if maxItems > 0 && maxItems < len(f.txs) {
		return f.txs[:maxItems], len(f.txs)
	}
	return f.txs, len(f.txs)
}

// mempoolFixture returns count generated Conway transactions as mempool
// entries, with the address each one pays and the transaction id its input
// spends.
func mempoolFixture(
	t *testing.T,
	count int,
) ([]mempool.MempoolTransaction, []string, [][]byte) {
	t.Helper()
	chain, err := testfixtures.GenerateConwayChainWithTransactions(count)
	require.NoError(t, err)
	var (
		txs       []mempool.MempoolTransaction
		addresses []string
		inputIDs  [][]byte
	)
	for _, block := range chain {
		for _, tx := range block.Transactions() {
			txs = append(txs, mempool.MempoolTransaction{
				Hash: tx.Hash().String(),
				Cbor: tx.Cbor(),
				Type: uint(tx.Type()),
			})
			addresses = append(addresses, tx.Outputs()[0].Address().String())
			inputIDs = append(inputIDs, tx.Inputs()[0].Id().Bytes())
		}
	}
	require.Len(t, txs, count)
	return txs, addresses, inputIDs
}

func hashesOf(txs []mempool.MempoolTransaction) []string {
	out := make([]string, 0, len(txs))
	for _, tx := range txs {
		out = append(out, tx.Hash)
	}
	return out
}

func TestNodeAdapterMempoolTransactions(t *testing.T) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	txs, _, _ := mempoolFixture(t, 5)
	pool := &fakeMempool{txs: txs}
	adapter.mempool = pool
	ctx := context.Background()
	all := hashesOf(txs)

	got, total, err := adapter.MempoolTransactions(
		ctx, PaginationParams{Count: 2, Page: 2, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 5, total)
	require.Equal(t, all[2:4], got)
	pool.mu.Lock()
	require.Equal(t, 4, pool.lastMaxItems,
		"an ascending page reads only the prefix it needs")
	pool.mu.Unlock()

	got, total, err = adapter.MempoolTransactions(
		ctx, PaginationParams{Count: 2, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Equal(t, 5, total)
	require.Equal(t, []string{all[4], all[3]}, got)
	got, _, err = adapter.MempoolTransactions(
		ctx, PaginationParams{Count: 2, Page: 3, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Equal(t, []string{all[0]}, got)

	got, total, err = adapter.MempoolTransactions(
		ctx, PaginationParams{Count: 2, Page: 9, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 5, total)
	require.Empty(t, got)
	got, total, err = adapter.MempoolTransactions(
		ctx,
		PaginationParams{
			Count: 100,
			Page:  MaxPaginationPage,
			Order: PaginationOrderAsc,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 5, total)
	require.Empty(t, got)

	adapter.mempool = nil
	_, _, err = adapter.MempoolTransactions(
		ctx, PaginationParams{Count: 2, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, ErrMempoolUnavailable)
}

func TestNodeAdapterMempoolTransaction(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	txs, addresses, inputIDs := mempoolFixture(t, 2)
	adapter.mempool = &fakeMempool{txs: txs}
	ctx := context.Background()

	// The second transaction's input resolves to a confirmed output at a
	// known address.
	spender, _, _ := testKeyAddress(t, 0xf1, 0)
	seedMempoolInput(t, raw, db, inputIDs[1], spender)

	hash, err := hex.DecodeString(txs[1].Hash)
	require.NoError(t, err)
	got, err := adapter.MempoolTransaction(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, txs[1].Hash, got.Tx.Hash)
	require.Equal(t, len(txs[1].Cbor), got.Tx.Size)
	require.Equal(t, "1", got.Tx.Fees)
	require.Equal(t, "0", got.Tx.Deposit)
	require.True(t, got.Tx.ValidContract)
	require.Equal(t, 2, got.Tx.UtxoCount, "one input and one output")
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "1000001"},
	}, got.Tx.OutputAmount)
	require.Nil(t, got.Tx.InvalidBefore)
	require.Zero(t, got.Tx.RedeemerCount)

	require.Len(t, got.Inputs, 1)
	require.Equal(t, hex.EncodeToString(inputIDs[1]), got.Inputs[0].TxHash)
	require.Equal(t, uint32(0), got.Inputs[0].OutputIndex)
	require.Equal(t, spender.String(), got.Inputs[0].Address)
	require.False(t, got.Inputs[0].Collateral)
	require.Nil(t, got.Inputs[0].Reference)
	require.Len(t, got.Outputs, 1)
	require.Equal(t, addresses[1], got.Outputs[0].Address)
	require.Equal(t, uint32(0), got.Outputs[0].OutputIndex)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "1000001"},
	}, got.Outputs[0].Amount)
	require.False(t, got.Outputs[0].Collateral)
	require.Empty(t, got.Redeemers)

	_, err = adapter.MempoolTransaction(ctx, make([]byte, 32))
	require.ErrorIs(t, err, ErrTransactionNotFound)

	garbage, err := hex.DecodeString(txs[0].Hash)
	require.NoError(t, err)
	adapter.mempool = &fakeMempool{txs: []mempool.MempoolTransaction{
		{Hash: txs[0].Hash, Cbor: []byte{0xff}, Type: 6},
	}}
	_, err = adapter.MempoolTransaction(ctx, garbage)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrTransactionNotFound)

	adapter.mempool = nil
	_, err = adapter.MempoolTransaction(ctx, hash)
	require.ErrorIs(t, err, ErrMempoolUnavailable)
}

func TestNodeAdapterMempoolAddressTransactions(t *testing.T) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	txs, addresses, _ := mempoolFixture(t, 3)
	hot, _, _ := testKeyAddress(t, 0xf2, 0)
	pool := &fakeMempool{
		txs: txs,
		byAddress: map[string][]string{
			addresses[1]: {txs[1].Hash},
			hot.String(): {txs[0].Hash, txs[2].Hash},
		},
	}
	adapter.mempool = pool
	adapter.mempoolAddresses = pool
	ctx := context.Background()
	asc := PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc}

	got, total, err := adapter.MempoolAddressTransactions(
		ctx, addresses[1], asc,
	)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Equal(t, []string{txs[1].Hash}, got)

	got, total, err = adapter.MempoolAddressTransactions(
		ctx, hot.String(), asc,
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Equal(t, []string{txs[0].Hash, txs[2].Hash}, got)

	got, total, err = adapter.MempoolAddressTransactions(
		ctx, hot.String(),
		PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total, "the total counts every match, not the page")
	require.Equal(t, []string{txs[2].Hash}, got)

	got, total, err = adapter.MempoolAddressTransactions(
		ctx, hot.String(),
		PaginationParams{Count: 1, Page: 2, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Equal(t, []string{txs[2].Hash}, got)

	_, _, err = adapter.MempoolAddressTransactions(ctx, "nonsense", asc)
	require.ErrorIs(t, err, ErrInvalidAddress)
	other, _, _ := testKeyAddress(t, 0xf3, 0)
	adapter.mempoolAddresses = nil
	_, _, err = adapter.MempoolAddressTransactions(ctx, other.String(), asc)
	require.ErrorIs(t, err, ErrMempoolUnavailable)
}

// TestNodeAdapterMempoolAddressTransactionsReadsOnlyTheIndex proves an
// address nothing matches costs one index lookup: no snapshot of the pool,
// so no decoding of its transactions, and no confirmed-output read.
func TestNodeAdapterMempoolAddressTransactionsReadsOnlyTheIndex(
	t *testing.T,
) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	txs, _, _ := mempoolFixture(t, 3)
	pool := &fakeMempool{txs: txs}
	adapter.mempool = pool
	adapter.mempoolAddresses = pool
	other, _, _ := testKeyAddress(t, 0xf3, 0)

	got, total, err := adapter.MempoolAddressTransactions(
		context.Background(),
		other.String(),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, got)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	require.Equal(t, 1, pool.addressReads)
	require.Zero(t, pool.boundedReads, "the pool must not be snapshotted")
}

// seedMempoolInput stores the confirmed output a mempool transaction spends.
func seedMempoolInput(
	t *testing.T,
	raw *sql.DB,
	db *database.Database,
	txID []byte,
	owner lcommon.Address,
) {
	t.Helper()
	insertAdapterTransaction(t, raw, &models.Transaction{
		Hash: txID, Slot: 1, Valid: true,
		Outputs: []models.Utxo{{
			TxId: txID, OutputIdx: 0,
			PaymentKey: owner.PaymentKeyHash().Bytes(),
			AddedSlot:  1, Amount: types.Uint64(5_000_000),
		}},
	})
	storePointerOutputCbor(t, db, txID, 0, owner, 5_000_000)
}
