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
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func plainOverlayChain(
	t testing.TB,
	n int,
) (lcommon.Transaction, []lcommon.Transaction) {
	t.Helper()
	payment := make([]byte, 28)
	payment[0] = 0x31
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	output := func() lcommon.TransactionOutput {
		out, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(address.String()).
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		return out
	}
	seed := mockledger.NewTransactionBuilder()
	seed.WithId(overlayTxID(0x51, 0))
	seed.WithOutputs(output())
	seed.WithValid(true)
	transactions := make([]lcommon.Transaction, 0, n)
	prior := seed.Hash().Bytes()
	for i := range n {
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(overlayTxID(0x52, i))
		tx.WithInputs(overlayTxInput(t, prior, 0))
		tx.WithOutputs(output())
		tx.WithValid(true)
		tx.WithMetadata([]byte{0xa1, 0x01, 0x02})
		tx.WithWitnesses(mockledger.NewMockTransactionWitnessSet().WithVkeyWitnesses(lcommon.VkeyWitness{Vkey: []byte{byte(i), 0x01}, Signature: []byte{0x02, byte(i)}}))
		transactions = append(transactions, tx)
		prior = tx.Hash().Bytes()
	}
	return seed, transactions
}

func applyPlainOverlayChain(
	t testing.TB,
	store *Store,
	transactions []lcommon.Transaction,
	enabled, barrier bool,
) []string {
	t.Helper()
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	if enabled {
		got, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
		require.NoError(t, err)
		require.True(t, got)
	}
	var views []string
	for i, tx := range transactions {
		contextSlot := uint64(99)
		require.NoError(
			t,
			store.SetTransactionLeiosClosureBatched(
				tx,
				ocommon.Point{Slot: 100, Hash: overlayTxID(0x53, 0)},
				uint32(i),
				nil,
				true,
				&contextSlot,
				acc,
				txn,
				0,
			),
		)
		if barrier {
			model, err := store.GetUtxo(tx.Hash().Bytes(), 0, txn)
			require.NoError(t, err)
			require.NotNil(t, model)
			model.PaymentKey[0] = 1
			again, err := store.GetUtxo(tx.Hash().Bytes(), 0, txn)
			require.NoError(t, err)
			require.Equal(t, byte(0x31), again.PaymentKey[0])
			input := tx.Inputs()[0]
			spent, err := store.GetUtxo(input.Id().Bytes(), input.Index(), txn)
			require.NoError(t, err)
			require.Nil(t, spent)
			historical, err := store.GetUtxoIncludingSpent(
				input.Id().Bytes(),
				input.Index(),
				txn,
			)
			require.NoError(t, err)
			require.NotNil(t, historical)
			require.Equal(t, tx.Hash().Bytes(), []byte(historical.SpentAtTxId))
			for _, value := range []any{again, historical} {
				encoded, err := json.Marshal(value)
				require.NoError(t, err)
				views = append(views, string(encoded))
			}
			if i == 1 {
				db, ctx, err := store.dbFromTxn(txn)
				require.NoError(t, err)
				var outputRows int
				require.NoError(
					t,
					db.QueryRowContext(ctx, `SELECT COUNT(*) FROM utxo`).
						Scan(&outputRows),
				)
				require.Equal(t, i+2, outputRows)
			}
		}
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
	return views
}

func TestLeiosRetainedUtxoOverlayMatchesPerRowHistory(t *testing.T) {
	t.Parallel()
	seed, transactions := plainOverlayChain(t, 8)
	states := make([]string, 2)
	metadataViews := make([][]string, 2)
	for i, enabled := range []bool{false, true} {
		store := newForeignKeySQLiteStore(t)
		store.storageMode = types.StorageModeAPI
		require.NoError(
			t,
			store.SetTransaction(
				seed,
				ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
				0,
				nil,
				true,
				nil,
				0,
			),
		)
		metadataViews[i] = applyPlainOverlayChain(t, store, transactions, enabled, true)
		states[i] = retainedOverlayAPIState(t, store)
		for _, table := range []string{"transaction", "leios_transaction_context", "address_transaction"} {
			var count int
			require.NoError(
				t,
				store.writeDB.QueryRow(`SELECT COUNT(*) FROM "`+table+`"`).
					Scan(&count),
			)
			expected := len(transactions)
			if table == "transaction" || table == "address_transaction" {
				expected++
			}
			require.Equal(t, expected, count)
		}
	}
	require.Equal(t, states[0], states[1])
	require.Equal(t, metadataViews[0], metadataViews[1])
}

func TestLeiosRetainedUtxoOverlayRollsBack(t *testing.T) {
	t.Parallel()
	seed, transactions := plainOverlayChain(t, 4)
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	require.NoError(
		t,
		store.SetTransaction(
			seed,
			ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
			0,
			nil,
			true,
			nil,
			0,
		),
	)
	before := overlayStoreState(t, store)
	txn := store.Transaction(context.Background())
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	got, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.True(t, got)
	for i, tx := range transactions {
		require.NoError(
			t,
			store.SetTransactionLeiosClosureBatched(
				tx,
				ocommon.Point{Slot: 100, Hash: overlayTxID(0x53, 0)},
				uint32(i),
				nil,
				true,
				nil,
				acc,
				txn,
				0,
			),
		)
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Rollback())
	require.Equal(t, before, overlayStoreState(t, store))
}

func TestLeiosRetainedUtxoOverlayFallsBackForSpentInput(t *testing.T) {
	t.Parallel()
	seed, transactions := plainOverlayChain(t, 4)
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	require.NoError(
		t,
		store.SetTransaction(
			seed,
			ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
			0,
			nil,
			true,
			nil,
			0,
		),
	)
	spender := mockledger.NewTransactionBuilder()
	spender.WithId(overlayTxID(0x55, 0))
	spender.WithInputs(transactions[0].Inputs()...)
	spender.WithOutputs(transactions[0].Outputs()...)
	spender.WithValid(true)
	require.NoError(t, store.SetTransactionLeiosClosure(spender, ocommon.Point{Slot: 50, Hash: overlayTxID(0x54, 0)}, 0, nil, true, nil, 0))

	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.False(t, enabled)
	require.Nil(t, acc.(*transactionBatchAccumulator).overlay)
}

func BenchmarkLeiosRetainedUtxoOverlay(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprint(enabled), func(b *testing.B) {
			seed, transactions := plainOverlayChain(b, 1000)
			store := newForeignKeySQLiteStore(b)
			store.storageMode = types.StorageModeAPI
			require.NoError(
				b,
				store.SetTransaction(
					seed,
					ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
					0,
					nil,
					true,
					nil,
					0,
				),
			)
			b.ReportAllocs()
			for b.Loop() {
				txn := store.Transaction(context.Background())
				acc := store.NewLeiosClosureBatchAccumulator()
				if enabled {
					got, err := store.EnableLeiosClosureUtxoOverlay(
						acc,
						txn,
						transactions,
					)
					require.NoError(b, err)
					require.True(b, got)
				}
				for i, tx := range transactions {
					require.NoError(
						b,
						store.SetTransactionLeiosClosureBatched(
							tx,
							ocommon.Point{
								Slot: 100,
								Hash: overlayTxID(0x53, 0),
							},
							uint32(i),
							nil,
							true,
							nil,
							acc,
							txn,
							0,
						),
					)
				}
				require.NoError(b, store.FlushBatch(acc, txn))
				require.NoError(b, txn.Rollback())
				acc.Reset()
			}
		})
	}
}

func retainedOverlayAPIState(t testing.TB, store *Store) string {
	t.Helper()
	var result strings.Builder
	result.WriteString(overlayStoreState(t, store))
	for _, table := range []string{"transaction", "leios_transaction_context", "address_transaction", "key_witness", "transaction_metadata_label"} {
		rows, err := store.writeDB.Query(`SELECT * FROM "` + table + `"`)
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		var lines []string
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			require.NoError(t, rows.Scan(pointers...))
			encoded, err := json.Marshal(values)
			require.NoError(t, err)
			lines = append(lines, string(encoded))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sort.Strings(lines)
		fmt.Fprintf(&result, "%s: %v\n", table, lines)
	}
	return result.String()
}

func TestLeiosRetainedUtxoOverlayRejectsExternalMutation(t *testing.T) {
	t.Parallel()
	seed, transactions := plainOverlayChain(t, 4)
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	require.NoError(t, store.SetTransaction(seed, ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()}, 0, nil, true, nil, 0))
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.True(t, enabled)
	require.NoError(t, store.SetTransactionLeiosClosureBatched(transactions[0], ocommon.Point{Slot: 100, Hash: overlayTxID(0x53, 0)}, 0, nil, true, nil, acc, txn, 0))
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `DELETE FROM utxo WHERE tx_id = ?`, seed.Hash().Bytes())
	require.ErrorContains(t, err, "external mutation")
	_, err = store.GetUtxo(transactions[0].Hash().Bytes(), 0, txn)
	require.ErrorContains(t, err, "external mutation")
	require.ErrorContains(t, store.FlushBatch(acc, txn), "external mutation")
}

func TestLeiosRetainedUtxoOverlayRejectsPointerInput(t *testing.T) {
	t.Parallel()
	seed, transactions := plainOverlayChain(t, 4)
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	require.NoError(t, store.SetTransaction(seed, ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()}, 0, nil, true, nil, 0))
	_, err := store.writeDB.Exec(`INSERT INTO utxo_pointer(utxo_id, ptr_slot, ptr_tx_index, ptr_cert_index) SELECT id, 1, 0, 0 FROM utxo WHERE tx_id = ?`, seed.Hash().Bytes())
	require.NoError(t, err)
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.False(t, enabled)
}

func TestLeiosRetainedUtxoOverlayClosureBound(t *testing.T) {
	t.Parallel()
	seed, transactions := plainOverlayChain(t, maxLeiosUtxoOverlayTransactions+1)
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	require.NoError(t, store.SetTransaction(seed, ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()}, 0, nil, true, nil, 0))
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.False(t, enabled)
	enabled, err = store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions[:maxLeiosUtxoOverlayTransactions])
	require.NoError(t, err)
	require.True(t, enabled)
}

func TestLeiosRetainedUtxoOverlayOutputByteBound(t *testing.T) {
	t.Parallel()
	seed, transactions := plainOverlayChain(t, 2)
	store := newForeignKeySQLiteStore(t)
	store.storageMode = types.StorageModeAPI
	require.NoError(t, store.SetTransaction(seed, ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()}, 0, nil, true, nil, 0))
	txn := store.Transaction(context.Background())
	defer func() { _ = txn.Rollback() }()
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	secondSize, err := lcommon.TransactionOutputCborSize(transactions[1].Outputs()[0])
	require.NoError(t, err)
	output := transactions[0].Outputs()[0].(interface{ SetCbor([]byte) })
	output.SetCbor(make([]byte, maxLeiosUtxoOverlayOutputBytes))
	enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.False(t, enabled)
	output.SetCbor(make([]byte, maxLeiosUtxoOverlayOutputBytes-int(secondSize)))
	enabled, err = store.EnableLeiosClosureUtxoOverlay(acc, txn, transactions)
	require.NoError(t, err)
	require.True(t, enabled)
}
